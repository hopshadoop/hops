/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CertificateLocalizationCtx;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DelegatingLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/** Container execution for Linux. Provides linux-specific localization
 * mechanisms, resource management via cgroups and can switch between multiple
 * container runtimes - e.g Standard "Process Tree", Docker etc
 */

public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(LinuxContainerExecutor.class);

  private String nonsecureLocalUser;
  private Pattern nonsecureLocalUserPattern;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;
  private LinuxContainerRuntime linuxContainerRuntime;

  public LinuxContainerExecutor() {
  }

  // created primarily for testing
  public LinuxContainerExecutor(LinuxContainerRuntime linuxContainerRuntime) {
    this.linuxContainerRuntime = linuxContainerRuntime;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    resourcesHandler = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
            DefaultLCEResourcesHandler.class, LCEResourcesHandler.class), conf);
    resourcesHandler.setConf(conf);
    resourcesHandler.setExecutablePath(PrivilegedOperationExecutor.getContainerExecutorExecutablePath(conf));

    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY)
        != null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = conf
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY,
              YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }
    nonsecureLocalUser = conf.get(
        YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY,
        YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
    nonsecureLocalUserPattern = Pattern.compile(
        conf.get(YarnConfiguration.NM_NONSECURE_MODE_USER_PATTERN_KEY,
            YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_USER_PATTERN));
    containerLimitUsers = conf.getBoolean(
        YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS,
        YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LIMIT_USERS);
    if (!containerLimitUsers) {
      LOG.warn(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS +
          ": impersonation without authentication enabled");
    }
  }

  void verifyUsernamePattern(String user) {
    if (!UserGroupInformation.isSecurityEnabled() &&
        !nonsecureLocalUserPattern.matcher(user).matches()) {
      throw new IllegalArgumentException("Invalid user name '" + user + "'," +
          " it must match '" + nonsecureLocalUserPattern.pattern() + "'");
    }
  }

  String getRunAsUser(String user) {
    if (UserGroupInformation.isSecurityEnabled() ||
        !containerLimitUsers) {
      return user;
    } else {
      return nonsecureLocalUser;
    }
  }

  protected String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
        ? defaultPath
        : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        defaultPath);
  }

  protected void addSchedPriorityCommand(List<String> command) {
    if (containerSchedPriorityIsSet) {
      command.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment)));
    }
  }

  protected PrivilegedOperationExecutor getPrivilegedOperationExecutor() {
    return PrivilegedOperationExecutor.getInstance(getConf());
  }

  @Override
  public void init() throws IOException {
    Configuration conf = super.getConf();

    // Send command to executor which will just start up,
    // verify configuration/permissions and exit
    try {
      PrivilegedOperation checkSetupOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.CHECK_SETUP);
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(checkSetupOp,
          false);
    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container executor initialization is : "
          + exitCode, e);

      throw new IOException("Linux container executor not configured properly"
          + " (error=" + exitCode + ")", e);
    }

    try {
      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf);
      if (resourceHandlerChain != null) {
        resourceHandlerChain.bootstrap(conf);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Failed to bootstrap configured resource subsystems! ", e);
      throw new IOException(
          "Failed to bootstrap configured resource subsystems!");
    }

    try {
      if (linuxContainerRuntime == null) {
        LinuxContainerRuntime runtime = new DelegatingLinuxContainerRuntime();

        runtime.initialize(conf);
        this.linuxContainerRuntime = runtime;
      }
    } catch (ContainerExecutionException e) {
      throw new IOException("Failed to initialize linux container runtime(s)!");
    }
    resourcesHandler.initializeHierarchy(conf);
    resourcesHandler.init(this);
  }

  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    String userFolder = ctx.getUserFolder();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);
    PrivilegedOperation initializeContainerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.INITIALIZE_CONTAINER);
    List<String> prefixCommands = new ArrayList<>();

    addSchedPriorityCommand(prefixCommands);

    initializeContainerOp.appendArgs(
        runAsUser,
        userFolder,
        Integer.toString(
            PrivilegedOperation.RunAsUserCommand.INITIALIZE_CONTAINER
                .getValue()),
        appId,
        nmPrivateContainerTokensPath.toUri().getPath().toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            localDirs),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            logDirs));

    File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");
    initializeContainerOp.appendArgs(jvm.toString());
    initializeContainerOp.appendArgs("-classpath");
    initializeContainerOp.appendArgs(System.getProperty("java.class.path"));
    String javaLibPath = System.getProperty("java.library.path");
    if (javaLibPath != null) {
      initializeContainerOp.appendArgs("-Djava.library.path=" + javaLibPath);
    }

    initializeContainerOp.appendArgs(ContainerLocalizer.getJavaOpts(getConf()));

    List<String> localizerArgs = new ArrayList<>();

    buildMainArgs(localizerArgs, user, appId, locId, nmAddr, localDirs, userFolder);
    initializeContainerOp.appendArgs(localizerArgs);

    Map<String, String> environment = null;

     // If SSL is enabled, populate the environment with the location of the certificates
    if (getConf().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      CertificateLocalization certificateLocalization = CertificateLocalizationCtx.getInstance()
          .getCertificateLocalization();
      String certFolder = certificateLocalization.getMaterialLocation(user, appId).getCertFolder().toString();

      environment = new HashMap<>();
      environment.put(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, certFolder);
    }

    try {
      Configuration conf = super.getConf();
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
          initializeContainerOp, null, environment, false, true);

    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container " + locId + " startLocalizer is : "
          + exitCode, e);

      throw new IOException("Application " + appId + " initialization failed" +
          " (exitCode=" + exitCode + ") with output: " + e.getOutput(), e);
    }
  }
  
  @Override
  public void recoverDeviceControlSystem(ContainerId containerId) {
    resourcesHandler.recoverDeviceControlSystem(containerId);
  }
  
  @VisibleForTesting
  public void buildMainArgs(List<String> command, String user, String appId,
      String locId, InetSocketAddress nmAddr, List<String> localDirs, String userFolder) {
    ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr,
      localDirs, userFolder);
  }

  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String userFolder = ctx.getUserFolder();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();
    Map<Path, List<String>> localizedResources = ctx.getLocalizedResources();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();

    resourcesHandler.preExecute(containerId,
            container.getResource());
    String resourcesOptions = resourcesHandler.getResourcesOption(
            containerId);
    String tcCommandFile = null;
    
    LOG.info("The resourcesOptions " + resourcesOptions);

    try {
      if (resourceHandlerChain != null) {
        List<PrivilegedOperation> ops = resourceHandlerChain
            .preStart(container);

        if (ops != null) {
          List<PrivilegedOperation> resourceOps = new ArrayList<>();

          resourceOps.add(new PrivilegedOperation(
              PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
                  resourcesOptions));

          for (PrivilegedOperation op : ops) {
            switch (op.getOperationType()) {
            case ADD_PID_TO_CGROUP:
              resourceOps.add(op);
              break;
            case TC_MODIFY_STATE:
              tcCommandFile = op.getArguments().get(0);
              break;
            default:
              LOG.warn("PrivilegedOperation type unsupported in launch: "
                  + op.getOperationType());
            }
          }

          if (resourceOps.size() > 1) {
            //squash resource operations
            try {
              PrivilegedOperation operation = PrivilegedOperationExecutor
                  .squashCGroupOperations(resourceOps);
              resourcesOptions = operation.getArguments().get(0);
            } catch (PrivilegedOperationException e) {
              LOG.error("Failed to squash cgroup operations!", e);
              throw new ResourceHandlerException(
                  "Failed to squash cgroup operations!");
            }
          }
        }
      }
    } catch (ResourceHandlerException e) {
      LOG.error("ResourceHandlerChain.preStart() failed!", e);
      throw new IOException("ResourceHandlerChain.preStart() failed!", e);
    }

    try {
      Path pidFilePath = getPidFilePath(containerId);
      if (pidFilePath != null) {
        List<String> prefixCommands = new ArrayList<>();
        ContainerRuntimeContext.Builder builder = new ContainerRuntimeContext
            .Builder(container);

        addSchedPriorityCommand(prefixCommands);
        if (prefixCommands.size() > 0) {
          builder.setExecutionAttribute(CONTAINER_LAUNCH_PREFIX_COMMANDS,
              prefixCommands);
        }

        builder.setExecutionAttribute(LOCALIZED_RESOURCES, localizedResources)
            .setExecutionAttribute(RUN_AS_USER, runAsUser)
            .setExecutionAttribute(USER, userFolder)
            .setExecutionAttribute(APPID, appId)
            .setExecutionAttribute(CONTAINER_ID_STR, containerIdStr)
            .setExecutionAttribute(CONTAINER_WORK_DIR, containerWorkDir)
            .setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH,
                nmPrivateContainerScriptPath)
            .setExecutionAttribute(NM_PRIVATE_TOKENS_PATH, nmPrivateTokensPath)
            .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
            .setExecutionAttribute(LOCAL_DIRS, localDirs)
            .setExecutionAttribute(LOG_DIRS, logDirs)
            .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);

        if (tcCommandFile != null) {
          builder.setExecutionAttribute(TC_COMMAND_FILE, tcCommandFile);
        }

        linuxContainerRuntime.launchContainer(builder.build());
      } else {
        LOG.info(
            "Container was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (ContainerExecutionException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // output
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: "
            + containerId + " and exit code: " + exitCode, e);

        StringBuilder builder = new StringBuilder();
        builder.append("Exception from container-launch.\n");
        builder.append("Container id: " + containerId + "\n");
        builder.append("Exit code: " + exitCode + "\n");
        if (!Optional.fromNullable(e.getErrorOutput()).or("").isEmpty()) {
          builder.append("Exception message: " + e.getErrorOutput() + "\n");
        }
        builder.append("Stack trace: "
            + StringUtils.stringifyException(e) + "\n");
        String output = e.getOutput();
        if (output != null && !e.getOutput().isEmpty()) {
          builder.append("Shell output: " + output + "\n");
        }
        String diagnostics = builder.toString();
        logOutput(diagnostics);
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      resourcesHandler.postExecute(containerId);

      try {
        if (resourceHandlerChain != null) {
          resourceHandlerChain.postComplete(containerId);
        }
      } catch (ResourceHandlerException e) {
        LOG.warn("ResourceHandlerChain.postComplete failed for " +
            "containerId: " + containerId + ". Exception: " + e);
      }
    }

    return 0;
  }

  @Override
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    ContainerId containerId = ctx.getContainerId();

    try {
      //Resource handler chain needs to reacquire container state
      //as well
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.reacquireContainer(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.reacquireContainer failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }

      return super.reacquireContainer(ctx);
    } finally {
      resourcesHandler.postExecute(containerId);
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.postComplete(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.postComplete failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }
    }
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    ContainerRuntimeContext runtimeContext = new ContainerRuntimeContext
        .Builder(container)
        .setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, pid)
        .setExecutionAttribute(SIGNAL, signal)
        .build();

    try {
      linuxContainerRuntime.signalContainer(runtimeContext);
    } catch (ContainerExecutionException e) {
      int retCode = e.getExitCode();
      if (retCode == PrivilegedOperation.ResultCode.INVALID_CONTAINER_PID
          .getValue()) {
        return false;
      }
      LOG.warn("Error in signalling container " + pid + " with " + signal
          + "; exit = " + retCode, e);
      logOutput(e.getOutput());
      throw new IOException("Problem signalling container " + pid + " with "
          + signal + "; output: " + e.getOutput() + " and exitCode: "
          + retCode, e);
    }
    return true;
  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx) {
    String user = ctx.getUser();
    Path dir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    verifyUsernamePattern(user);

    String runAsUser = getRunAsUser(user);
    String dirString = dir == null ? "" : dir.toUri().getPath();

    PrivilegedOperation deleteAsUserOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.DELETE_AS_USER, (String) null);

    deleteAsUserOp.appendArgs(
        runAsUser,
        user,
        Integer.toString(PrivilegedOperation.
            RunAsUserCommand.DELETE_AS_USER.getValue()),
        dirString);

    List<String> pathsToDelete = new ArrayList<String>();
    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + dir);
      pathsToDelete.add(dirString);
    } else {
      for (Path baseDir : baseDirs) {
        Path del = dir == null ? baseDir : new Path(baseDir, dir);
        LOG.info("Deleting path : " + del);
        pathsToDelete.add(del.toString());
        deleteAsUserOp.appendArgs(baseDir.toUri().getPath());
      }
    }

    try {
      Configuration conf = super.getConf();
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(deleteAsUserOp,
          false);
    }   catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.error("DeleteAsUser for " + StringUtils.join(" ", pathsToDelete)
          + " returned with exit code: " + exitCode, e);
    }
  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Container container = ctx.getContainer();

    // Send a test signal to the process as the user to see if it's alive
    return signalContainer(new ContainerSignalContext.Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .setSignal(Signal.NULL)
        .build());
  }

  public void mountCgroups(List<String> cgroupKVs, String hierarchy)
      throws IOException {
    try {
      PrivilegedOperation mountCGroupsOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.MOUNT_CGROUPS, hierarchy);
      Configuration conf = super.getConf();

      mountCGroupsOp.appendArgs(cgroupKVs);
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(mountCGroupsOp,
          false);
    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exception in LinuxContainerExecutor mountCgroups ", e);

      throw new IOException("Problem mounting cgroups " + cgroupKVs +
          "; exit code = " + exitCode + " and output: " + e.getOutput(),
          e);
    }
  }
}
