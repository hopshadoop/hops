/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class BaseCGroupsHandler implements CGroupsHandler {

  final Logger LOG = LoggerFactory.getLogger(getClass());
  final long deleteCGroupTimeout;
  final long deleteCGroupDelay;
  final String cGroupPrefix;
  final String cGroupMountPath;
  final Clock clock;

  static final String CGROUPS_FSTYPE = "cgroup";
  static final String CGROUPS2_FSTYPE = "cgroup2";

  BaseCGroupsHandler(Configuration conf) {
    this.deleteCGroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT) +
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) + 1000;
    this.deleteCGroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    this.cGroupPrefix = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn")
        .replaceAll("^/", "").replaceAll("$/", "");
    this.cGroupMountPath = conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
    this.clock = new MonotonicClock();
  }

  @Override
  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    String path = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("createCgroup: " + path);
    }

    File cGroupFile = new File(path);
    if (!cGroupFile.exists() && !cGroupFile.mkdir()) {
      throw new ResourceHandlerException("Failed to create cgroup at " + path);
    }

    return path;
  }

  @Override
  public void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);
    PrintWriter pw = null;

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          String.format("updateCGroupParam for path: %s with value %s",
              cGroupParamPath, value));
    }

    try {
      File file = new File(cGroupParamPath);
      Writer w = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new ResourceHandlerException(
          String.format("Unable to write to %s with value: %s",
              cGroupParamPath, value), e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if (hasError) {
          throw new ResourceHandlerException(
              String.format("PrintWriter unable to write to %s with value: %s",
                  cGroupParamPath, value));
        }
        if (pw.checkError()) {
          throw new ResourceHandlerException(
              String.format("Error while closing cgroup file %s",
                  cGroupParamPath));
        }
      }
    }
  }

  @Override
  public String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException {
    String cGroupParamPath =
        param.equals(getProcessesFilename()) ?
            getPathForCGroup(controller, cGroupId)
                + org.apache.hadoop.fs.Path.SEPARATOR + param :
            getPathForCGroupParam(controller, cGroupId, param);

    try {
      byte[] contents = Files.readAllBytes(Paths.get(cGroupParamPath));
      return new String(contents, StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new ResourceHandlerException(
          "Unable to read from " + cGroupParamPath);
    }
  }

  @Override
  public String getCGroupMountPath() {
    return cGroupMountPath;
  }

  @Override
  public String getRelativePathForCGroup(String cGroupId) {
    return cGroupPrefix + Path.SEPARATOR + cGroupId;
  }

  @Override
  public String getPathForCGroupParam(CGroupController controller, String cGroupId, String param) {
    return Paths.get(getPathForCGroup(controller, cGroupId), controller.getName() + "." + param).toString();
  }

  @Override
  public String getPathForCGroupTasks(CGroupController controller, String cGroupId) {
    return Paths.get(getPathForCGroup(controller, cGroupId), getProcessesFilename()).toString();
  }

  @Override
  public void deleteCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    boolean deleted = false;
    String cGroupPath = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteCGroup: " + cGroupPath);
    }

    long start = clock.getTime();

    do {
      try {
        deleted = checkAndDeleteCgroup(new File(cGroupPath));
        if (!deleted) {
          Thread.sleep(deleteCGroupDelay);
        }
      } catch (InterruptedException ex) {
        // NOP
      }
    } while (!deleted && (clock.getTime() - start) < deleteCGroupTimeout);

    if (!deleted) {
      LOG.warn(String.format("Unable to delete  %s, tried to delete for %d ms",
          cGroupPath, deleteCGroupTimeout));
    }
  }

  /**
   * If tasks file is empty, delete the cgroup.
   *
   * @param cgf object referring to the cgroup to be deleted
   * @return Boolean indicating whether cgroup was deleted
   */
  boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    // FileInputStream in = null;
    if ( cgf.exists() ) {
      String procsFile = Paths.get(cgf.toString(), getProcessesFilename()).toString();
      try (FileInputStream in = new FileInputStream(procsFile)) {
        if (in.read() == -1) {
          /*
           * "tasks" file is empty, sleep a bit more and then try to delete the
           * cgroup. Some versions of linux will occasionally panic due to a race
           * condition in this area, hence the paranoia.
           */
          Thread.sleep(deleteCGroupDelay);
          deleted = cgf.delete();
          if (!deleted) {
            LOG.warn("Failed attempt to delete cgroup: " + cgf);
          }
        } else{
          logLineFromTasksFile(cgf);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    } else {
      LOG.info("Parent Cgroups directory {} does not exist. Skipping "
          + "deletion", cgf.getPath());
      deleted = true;
    }
    return deleted;
  }

  /*
   * Utility routine to print first line from cgroup tasks file
   */
  void logLineFromTasksFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      String procsFile = Paths.get(cgf.toString(), getProcessesFilename()).toString();
      try (BufferedReader inl =
               new BufferedReader(new InputStreamReader(new FileInputStream(procsFile), StandardCharsets.UTF_8))) {
        str = inl.readLine();
        if (str != null) {
          LOG.debug("First line in cgroup tasks file: " + cgf + " " + str);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    }
  }

  abstract String getProcessesFilename();
}
