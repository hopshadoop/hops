/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Support for interacting with various CGroup subsystems. Thread-safe.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsHandlerImpl extends BaseCGroupsHandler implements CGroupsHandler {
  private static final Logger cgroup1Logger = LoggerFactory.getLogger(CGroupsHandlerImpl.class);
  private static final String MTAB_FILE = "/proc/mounts";

  private String mtabFile;
  private final boolean enableCGroupMount;
  private Map<CGroupController, String> controllerPaths;
  private Map<String, Set<String>> parsedMtab;
  private final ReadWriteLock rwLock;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  /**
   * Create cgroup handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @param mtab mount file location
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor, String mtab)
      throws ResourceHandlerException {
    super(conf);
    this.enableCGroupMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.controllerPaths = new HashMap<>();
    this.parsedMtab = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    mtabFile = mtab;
    init();
  }

  /**
   * Create cgroup handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor) throws ResourceHandlerException {
    this(conf, privilegedOperationExecutor, MTAB_FILE);
  }

  private void init() throws ResourceHandlerException {
    initializeControllerPaths();
  }

  @Override
  String getProcessesFilename() {
    return "tasks";
  }

  @Override
  public String getControllerPath(CGroupController controller) {
    try {
      rwLock.readLock().lock();
      return controllerPaths.get(controller);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return Paths.get(getControllerPath(controller), cGroupPrefix, cGroupId).toString();
  }


  private void initializeControllerPaths() throws ResourceHandlerException {
    // Cluster admins may have some subsystems mounted in specific locations
    // We'll attempt to figure out mount points. We do this even if we plan
    // to mount cgroups into our own tree to control the path permissions or
    // to mount subsystems that are not mounted previously.
    // The subsystems for new and existing mount points have to match, and
    // the same hierarchy will be mounted at each mount point with the same
    // subsystem set.

    Map<String, Set<String>> newMtab = null;
    Map<CGroupController, String> cPaths;
    try {
      if (this.cGroupMountPath != null && !this.enableCGroupMount) {
        newMtab = ResourceHandlerModule.
            parseConfiguredCGroupPath(this.cGroupMountPath);
      }

      if (newMtab == null) {
        // parse mtab
        newMtab = parseMtab(mtabFile);
      }

      // find cgroup controller paths
      cPaths = initializeControllerPathsFromMtab(newMtab);
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: " + e);
      throw new ResourceHandlerException(
          "Failed to initialize controller paths!");
    }

    // we want to do a bulk update without the paths changing concurrently
    try {
      rwLock.writeLock().lock();
      controllerPaths = cPaths;
      parsedMtab = newMtab;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  static Map<CGroupController, String> initializeControllerPathsFromMtab(
      Map<String, Set<String>> parsedMtab)
      throws ResourceHandlerException {
    Map<CGroupController, String> ret = new HashMap<>();

    for (CGroupController controller : CGroupController.values()) {
      String subsystemName = controller.getName();
      String controllerPath = findControllerInMtab(subsystemName, parsedMtab);

      if (controllerPath != null) {
        ret.put(controller, controllerPath);
      }
    }
    return ret;
  }

  /* We are looking for entries of the form:
   * none /cgroup/path/mem cgroup rw,memory 0 0
   *
   * Use a simple pattern that splits on the five spaces, and
   * grabs the 2, 3, and 4th fields.
   */

  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

  /*
   * Returns a map: path -> mount options
   * for mounts with type "cgroup". Cgroup controllers will
   * appear in the list of options for a path.
   */
  @VisibleForTesting
  static Map<String, Set<String>> parseMtab(String mtab)
      throws IOException {
    Map<String, Set<String>> ret = new HashMap<>();
    BufferedReader in = null;
    // cgroup2 has a single hierarchy tree so we don't need to parse the different controllers from mtab
    Set<String> validCgroups =
        CGroupsHandler.CGroupController.getValidCGroups(CGroupController.V1_CGROUP_FILTER);

    try {
      FileInputStream fis = new FileInputStream(new File(mtab));
      in = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);

          if (type.equals(CGROUPS_FSTYPE)) {
            Set<String> cgroupList =
                new HashSet<>(Arrays.asList(options.split(",")));
            // Collect the valid subsystem names
            cgroupList.retainAll(validCgroups);
            ret.put(path, cgroupList);
          } else if (type.equals(CGROUPS2_FSTYPE)) {
            ret.put(path, new HashSet<>(Arrays.asList(CGROUPS2_FSTYPE)));
          }
        }
      }
    } catch (IOException e) {
      if (Shell.LINUX) {
        throw new IOException("Error while reading " + mtab, e);
      } else {
        // Ignore the error, if we are running on an os other than Linux
        cgroup1Logger.warn("Error while reading " + mtab, e);
      }
    } finally {
      IOUtils.cleanupWithLogger(cgroup1Logger, in);
    }

    return ret;
  }

  static int cgroupsVersion() throws IOException {
    return cgroupsVersion(MTAB_FILE);
  }

  static int cgroupsVersion(String mtabFile) throws IOException {
    Map<String, Set<String>> parsedMtab = parseMtab(mtabFile);
    for (Set<String> v : parsedMtab.values()) {
      if (v.contains(CGROUPS2_FSTYPE) && parsedMtab.size() == 1) {
        return 2;
      }
    }
    return 1;
  }

  /**
   * Find the hierarchy of the subsystem.
   * The kernel ensures that a subsystem can only be part of a single hierarchy.
   * The subsystem can be part of multiple mount points, if they belong to the
   * same hierarchy.
   * @param controller subsystem like cpu, cpuset, etc...
   * @param entries map of paths to mount options
   * @return the first mount path that has the requested subsystem
   */
  @VisibleForTesting
  static String findControllerInMtab(String controller,
      Map<String, Set<String>> entries) {
    for (Map.Entry<String, Set<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller)) {
        if (new File(e.getKey()).canRead()) {
          return e.getKey();
        } else {
          cgroup1Logger.warn(String.format(
              "Skipping inaccessible cgroup mount point %s", e.getKey()));
        }
      }
    }

    return null;
  }

  private void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    if (cGroupMountPath == null) {
      throw new ResourceHandlerException(
          String.format("Cgroups mount path not specified in %s.",
              YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH));
    }
    String existingMountPath = getControllerPath(controller);
    String requestedMountPath =
        new File(cGroupMountPath, controller.getName()).getAbsolutePath();

    if (existingMountPath == null ||
        !requestedMountPath.equals(existingMountPath)) {
      try {
        //lock out other readers/writers till we are done
        rwLock.writeLock().lock();

        // If the controller was already mounted we have to mount it
        // with the same options to clone the mount point otherwise
        // the operation will fail
        String mountOptions;
        if (existingMountPath != null) {
          mountOptions = Joiner.on(',')
              .join(parsedMtab.get(existingMountPath));
        } else {
          mountOptions = controller.getName();
        }

        String cGroupKV =
            mountOptions + "=" + requestedMountPath;
        PrivilegedOperation.OperationType opType = PrivilegedOperation
            .OperationType.MOUNT_CGROUPS;
        PrivilegedOperation op = new PrivilegedOperation(opType);

        op.appendArgs(cGroupPrefix, cGroupKV);
        LOG.info("Mounting controller " + controller.getName() + " at " +
              requestedMountPath);
        privilegedOperationExecutor.executePrivilegedOperation(op, false);

        //if privileged operation succeeds, update controller paths
        controllerPaths.put(controller, requestedMountPath);
      } catch (PrivilegedOperationException e) {
        LOG.error("Failed to mount controller: " + controller.getName());
        throw new ResourceHandlerException("Failed to mount controller: "
            + controller.getName());
      } finally {
        rwLock.writeLock().unlock();
      }
    } else {
      LOG.info("CGroup controller already mounted at: " + existingMountPath);
    }
  }

  /**
   * Mount cgroup or use existing mount point based on configuration.
   * @param controller - the controller being initialized
   * @throws ResourceHandlerException yarn hierarchy cannot be created or
   *   accessed for any reason
   */
  @Override
  public void initializeCGroupController(CGroupController controller) throws
      ResourceHandlerException {
    if (enableCGroupMount) {
      // We have a controller that needs to be mounted
      mountCGroupController(controller);
    }

    // We are working with a pre-mounted contoller
    // Make sure that YARN cgroup hierarchy path exists
    initializePreMountedCGroupController(controller);
  }

  /**
   * This function is called when the administrator opted
   * to use a pre-mounted cgroup controller.
   * There are two options.
   * 1. YARN hierarchy already exists. We verify, whether we have write access
   * in this case.
   * 2. YARN hierarchy does not exist, yet. We create it in this case.
   * @param controller the controller being initialized
   * @throws ResourceHandlerException yarn hierarchy cannot be created or
   *   accessed for any reason
   */
  private void initializePreMountedCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    // Check permissions to cgroup hierarchy and
    // create YARN cgroup if it does not exist, yet
    String controllerPath = getControllerPath(controller);

    if (controllerPath == null) {
      throw new ResourceHandlerException(
          String.format("Controller %s not mounted."
                  + " You either need to mount it with %s"
                  + " or mount cgroups before launching Yarn",
              controller.getName(), YarnConfiguration.
                  NM_LINUX_CONTAINER_CGROUPS_MOUNT));
    }

    File rootHierarchy = new File(controllerPath);
    File yarnHierarchy = new File(rootHierarchy, cGroupPrefix);
    String subsystemName = controller.getName();

    LOG.info("Initializing mounted controller " + controller.getName() + " " +
        "at " + yarnHierarchy);

    if (!rootHierarchy.exists()) {
      throw new ResourceHandlerException(getErrorWithDetails(
              "Cgroups mount point does not exist or not accessible",
              subsystemName,
              rootHierarchy.getAbsolutePath()
          ));
    } else if (!yarnHierarchy.exists()) {
      LOG.info("Yarn control group does not exist. Creating " +
          yarnHierarchy.getAbsolutePath());
      try {
        if (!yarnHierarchy.mkdir()) {
          // Unexpected: we just checked that it was missing
          throw new ResourceHandlerException(getErrorWithDetails(
                  "Unexpected: Cannot create yarn cgroup",
                  subsystemName,
                  yarnHierarchy.getAbsolutePath()
              ));
        }
      } catch (SecurityException e) {
        throw new ResourceHandlerException(getErrorWithDetails(
                "No permissions to create yarn cgroup",
                subsystemName,
                yarnHierarchy.getAbsolutePath()
            ), e);
      }
    } else if (!FileUtil.canWrite(yarnHierarchy)) {
      throw new ResourceHandlerException(getErrorWithDetails(
              "Yarn control group not writable",
              subsystemName,
              yarnHierarchy.getAbsolutePath()
          ));
    }
  }

  /**
   * Creates an actionable error message for mtab parsing.
   * @param errorMessage message to use
   * @param subsystemName cgroup subsystem
   * @param yarnCgroupPath cgroup path that failed
   * @return a string builder that can be appended by the caller
   */
  private String getErrorWithDetails(
      String errorMessage,
      String subsystemName,
      String yarnCgroupPath) {
    return String.format("%s Subsystem:%s Mount points:%s User:%s Path:%s ",
        errorMessage, subsystemName, mtabFile, System.getProperty("user.name"),
        yarnCgroupPath);
  }
}