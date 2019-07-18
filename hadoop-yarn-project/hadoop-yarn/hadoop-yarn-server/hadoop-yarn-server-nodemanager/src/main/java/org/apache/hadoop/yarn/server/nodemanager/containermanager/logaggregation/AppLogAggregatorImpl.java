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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationDFSException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import org.apache.hadoop.fs.CommonConfigurationKeys;


public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Logger LOG =
       LoggerFactory.getLogger(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final Path remoteNodeLogFileForApp;
  private final Path remoteNodeTmpLogFileForApp;

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final FileContext lfs;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final NodeId nodeId;
  private final LogAggregationFileControllerContext logControllerContext;

  // These variables are only for testing
  private final AtomicBoolean waiting = new AtomicBoolean(false);
  private int logAggregationTimes = 0;
  private int cleanupOldLogTimes = 0;

  private boolean renameTemporaryLogFileFailed = false;

  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();
  private final ContainerLogAggregationPolicy logAggPolicy;

  private final LogAggregationFileController logAggregationFileController;


  /**
   * The value recovered from state store to determine the age of application
   * log files if log retention is enabled. Files older than retention policy
   * will not be uploaded but scheduled for cleaning up. -1 if not recovered.
   */
  private final long recoveredLogInitedTime;

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval, Configuration sslConf) throws IOException, InterruptedException {
    this(dispatcher, deletionService, conf, appId, userUgi, nodeId,
        dirsHandler, remoteNodeLogFileForApp, appAcls,
        logAggregationContext, context, lfs, rollingMonitorInterval, -1, null, sslConf);
  }

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      long recoveredLogInitedTime, Configuration sslConf) throws IOException, InterruptedException {
    this(dispatcher, deletionService, conf, appId, userUgi, nodeId,
        dirsHandler, remoteNodeLogFileForApp, appAcls,
        logAggregationContext, context, lfs, rollingMonitorInterval,
        recoveredLogInitedTime, null, sslConf);
  }

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      long recoveredLogInitedTime,
      LogAggregationFileController logAggregationFileController, Configuration sslConf) 
      throws FileNotFoundException, InterruptedException {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = appId.toString();
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.lfs = lfs;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.nodeId = nodeId;
    this.logAggPolicy = getLogAggPolicy(conf);
    this.recoveredLogInitedTime = recoveredLogInitedTime;
    if (logAggregationFileController == null) {
      // by default, use T-File Controller
      this.logAggregationFileController = new LogAggregationTFileController();
      this.logAggregationFileController.initialize(conf, "TFile");
      this.logAggregationFileController.verifyAndCreateRemoteLogDir(conf, sslConf);
      String  keyStoreLocation = null;
      if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
          CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
        keyStoreLocation = context.getCertificateLocalizationService()
            .getX509MaterialLocation(this.userUgi.getShortUserName(), appId.toString()).getKeyStoreLocation().
            toString();
      }
      this.logAggregationFileController.createAppDir(
          this.userUgi.getShortUserName(), appId, userUgi,
          conf, keyStoreLocation);
      this.remoteNodeLogFileForApp = this.logAggregationFileController
          .getRemoteNodeLogFileForApp(appId,
              this.userUgi.getShortUserName(), nodeId);
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    } else {
      this.logAggregationFileController = logAggregationFileController;
      this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    }
    boolean logAggregationInRolling =
        rollingMonitorInterval <= 0 || this.logAggregationContext == null
            || this.logAggregationContext.getRolledLogsIncludePattern() == null
            || this.logAggregationContext.getRolledLogsIncludePattern()
                .isEmpty() ? false : true;
    logControllerContext = new LogAggregationFileControllerContext(
            this.remoteNodeLogFileForApp,
            this.remoteNodeTmpLogFileForApp,
            logAggregationInRolling,
            rollingMonitorInterval,
            this.appId, this.appAcls, this.nodeId, this.userUgi);
  }

  private ContainerLogAggregationPolicy getLogAggPolicy(Configuration conf) {
    ContainerLogAggregationPolicy policy = getLogAggPolicyInstance(conf);
    String params = getLogAggPolicyParameters(conf);
    if (params != null) {
      policy.parseParameters(params);
    }
    return policy;
  }

  // Use the policy class specified in LogAggregationContext if available.
  // Otherwise use the cluster-wide default policy class.
  private ContainerLogAggregationPolicy getLogAggPolicyInstance(
      Configuration conf) {
    Class<? extends ContainerLogAggregationPolicy> policyClass = null;
    if (this.logAggregationContext != null) {
      String className =
          this.logAggregationContext.getLogAggregationPolicyClassName();
      if (className != null) {
        try {
          Class<?> policyFromContext = conf.getClassByName(className);
          if (ContainerLogAggregationPolicy.class.isAssignableFrom(
              policyFromContext)) {
            policyClass = policyFromContext.asSubclass(
                ContainerLogAggregationPolicy.class);
          } else {
            LOG.warn(this.appId + " specified invalid log aggregation policy " +
                className);
          }
        } catch (ClassNotFoundException cnfe) {
          // We don't fail the app if the policy class isn't valid.
          LOG.warn(this.appId + " specified invalid log aggregation policy " +
              className);
        }
      }
    }
    if (policyClass == null) {
      policyClass = conf.getClass(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS,
          AllContainerLogAggregationPolicy.class,
              ContainerLogAggregationPolicy.class);
    } else {
      LOG.info(this.appId + " specifies ContainerLogAggregationPolicy of "
          + policyClass);
    }
    return ReflectionUtils.newInstance(policyClass, conf);
  }

  // Use the policy parameters specified in LogAggregationContext if available.
  // Otherwise use the cluster-wide default policy parameters.
  private String getLogAggPolicyParameters(Configuration conf) {
    String params = null;
    if (this.logAggregationContext != null) {
      params = this.logAggregationContext.getLogAggregationPolicyParameters();
    }
    if (params == null) {
      params = conf.get(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS_PARAMETERS);
    }
    return params;
  }

  private void uploadLogsForContainers(boolean appFinished)
      throws LogAggregationDFSException {
    if (this.logAggregationDisabled) {
      return;
    }

    addCredentials();

    // Create a set of Containers whose logs will be uploaded in this cycle.
    // It includes:
    // a) all containers in pendingContainers: those containers are finished
    //    and satisfy the ContainerLogAggregationPolicy.
    // b) some set of running containers: For all the Running containers,
    //    we use exitCode of 0 to find those which satisfy the
    //    ContainerLogAggregationPolicy.
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);
    Set<ContainerId> finishedContainers =
        new HashSet<ContainerId>(pendingContainerInThisCycle);
    if (this.context.getApplications().get(this.appId) != null) {
      for (Container container : this.context.getApplications()
        .get(this.appId).getContainers().values()) {
        ContainerType containerType =
            container.getContainerTokenIdentifier().getContainerType();
        if (shouldUploadLogs(new ContainerLogContext(
            container.getContainerId(), containerType, 0))) {
          pendingContainerInThisCycle.add(container.getContainerId());
        }
      }
    }

    if (pendingContainerInThisCycle.isEmpty()) {
      sendLogAggregationReport(true, "", appFinished);
      return;
    }

    logAggregationTimes++;
    String diagnosticMessage = "";
    boolean logAggregationSucceedInThisCycle = true;
    DeletionTask deletionTask = null;
    try {
      try {
        logAggregationFileController.initializeWriter(logControllerContext);
      } catch (IOException e1) {
        logAggregationSucceedInThisCycle = false;
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ", e1);
        return;
      }

      boolean uploadedLogsInThisCycle = false;
      for (ContainerId container : pendingContainerInThisCycle) {
        ContainerLogAggregator aggregator = null;
        if (containerLogAggregators.containsKey(container)) {
          aggregator = containerLogAggregators.get(container);
        } else {
          aggregator = new ContainerLogAggregator(container);
          containerLogAggregators.put(container, aggregator);
        }
        Set<Path> uploadedFilePathsInThisCycle =
            aggregator.doContainerLogAggregation(logAggregationFileController,
            appFinished, finishedContainers.contains(container));
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
          List<Path> uploadedFilePathsInThisCycleList = new ArrayList<>();
          uploadedFilePathsInThisCycleList.addAll(uploadedFilePathsInThisCycle);
          deletionTask = new FileDeletionTask(delService,
              this.userUgi.getShortUserName(), null,
              uploadedFilePathsInThisCycleList);
        }

        // This container is finished, and all its logs have been uploaded,
        // remove it from containerLogAggregators.
        if (finishedContainers.contains(container)) {
          containerLogAggregators.remove(container);
        }
      }

      logControllerContext.setUploadedLogsInThisCycle(uploadedLogsInThisCycle);
      logControllerContext.setLogUploadTimeStamp(System.currentTimeMillis());
      logControllerContext.increLogAggregationTimes();
      try {
        this.logAggregationFileController.postWrite(logControllerContext);
        diagnosticMessage = "Log uploaded successfully for Application: "
            + appId + " in NodeManager: "
            + LogAggregationUtils.getNodeString(nodeId) + " at "
            + Times.format(logControllerContext.getLogUploadTimeStamp())
            + "\n";
      } catch (Exception e) {
        diagnosticMessage = e.getMessage();
        renameTemporaryLogFileFailed = true;
        logAggregationSucceedInThisCycle = false;
      }
    } finally {
      LogAggregationDFSException exc = null;
      try {
        this.logAggregationFileController.closeWriter();
      } catch (LogAggregationDFSException e) {
        diagnosticMessage = e.getMessage();
        renameTemporaryLogFileFailed = true;
        logAggregationSucceedInThisCycle = false;
        exc = e;
      }
      if (logAggregationSucceedInThisCycle && deletionTask != null) {
        delService.delete(deletionTask);
      }
      sendLogAggregationReport(logAggregationSucceedInThisCycle,
          diagnosticMessage, appFinished);
      if (exc != null) {
        throw exc;
      }
    }
  }

  private void addCredentials() {
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials systemCredentials =
          context.getSystemCredentialsForApps().get(appId);
      if (systemCredentials != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding new framework-token for " + appId
              + " for log-aggregation: " + systemCredentials.getAllTokens()
              + "; userUgi=" + userUgi);
        }
        // this will replace old token
        userUgi.addCredentials(systemCredentials);
      }
    }
  }

  private void sendLogAggregationReport(
      boolean logAggregationSucceedInThisCycle, String diagnosticMessage,
      boolean appFinished) {
    LogAggregationStatus logAggregationStatus =
        logAggregationSucceedInThisCycle
            ? LogAggregationStatus.RUNNING
            : LogAggregationStatus.RUNNING_WITH_FAILURE;
    sendLogAggregationReportInternal(logAggregationStatus, diagnosticMessage,
        false);
    if (appFinished) {
      // If the app is finished, one extra final report with log aggregation
      // status SUCCEEDED/FAILED will be sent to RM to inform the RM
      // that the log aggregation in this NM is completed.
      LogAggregationStatus finalLogAggregationStatus =
          renameTemporaryLogFileFailed || !logAggregationSucceedInThisCycle
              ? LogAggregationStatus.FAILED
              : LogAggregationStatus.SUCCEEDED;
      sendLogAggregationReportInternal(finalLogAggregationStatus, "", true);
    }
  }

  private void sendLogAggregationReportInternal(
      LogAggregationStatus logAggregationStatus, String diagnosticMessage,
      boolean finalized) {
    LogAggregationReport report =
        Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setDiagnosticMessage(diagnosticMessage);
    report.setLogAggregationStatus(logAggregationStatus);
    this.context.getLogAggregationStatusForApps().add(report);
    this.context.getNMLogAggregationStatusTracker().updateLogAggregationStatus(
        appId, logAggregationStatus, System.currentTimeMillis(),
        diagnosticMessage, finalized);
  }

  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } catch (LogAggregationDFSException e) {
      // if the log aggregation could not be performed due to DFS issues
      // let's not clean up the log files, since that can result in
      // loss of logs
      LOG.error("Error occurred while aggregating the log for the application "
          + appId, e);
    } catch (Exception e) {
      // do post clean up of log directories on any other exception
      LOG.error("Error occurred while aggregating the log for the application "
          + appId, e);
      doAppLogAggregationPostCleanUp();
    } finally {
      if (!this.appAggregationFinished.get() && !this.aborted.get()) {
        LOG.warn("Log aggregation did not complete for application " + appId);
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(this.appId,
                ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
      }
      this.appAggregationFinished.set(true);
    }
  }

  private void doAppLogAggregation() throws LogAggregationDFSException {
    while (!this.appFinishing.get() && !this.aborted.get()) {
      synchronized(this) {
        try {
          waiting.set(true);
          if (logControllerContext.isLogAggregationInRolling()) {
            wait(logControllerContext.getRollingMonitorInterval() * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            uploadLogsForContainers(false);
          } else {
            wait(THREAD_SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.warn("PendingContainers queue is interrupted");
          this.appFinishing.set(true);
        } catch (LogAggregationDFSException e) {
          this.appFinishing.set(true);
          throw e;
        }
      }
    }

    if (this.aborted.get()) {
      return;
    }

    try {
      // App is finished, upload the container logs.
      uploadLogsForContainers(true);

      doAppLogAggregationPostCleanUp();
    } catch (LogAggregationDFSException e) {
      LOG.error("Error during log aggregation", e);
    }

    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);
  }

  private void doAppLogAggregationPostCleanUp() {
    // Remove the local app-log-dirs
    List<Path> localAppLogDirs = new ArrayList<Path>();
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      Path logPath = new Path(rootLogDir, applicationId);
      try {
        // check if log dir exists
        lfs.getFileStatus(logPath);
        localAppLogDirs.add(logPath);
      } catch (UnsupportedFileSystemException ue) {
        LOG.warn("Log dir " + rootLogDir + "is an unsupported file system", ue);
        continue;
      } catch (IOException fe) {
        continue;
      }
    }

    if (localAppLogDirs.size() > 0) {
      List<Path> localAppLogDirsList = new ArrayList<>();
      localAppLogDirsList.addAll(localAppLogDirs);
      DeletionTask deletionTask = new FileDeletionTask(delService,
          this.userUgi.getShortUserName(), null, localAppLogDirsList);
      this.delService.delete(deletionTask);
    }
  }

  private Path getRemoteNodeTmpLogFileForApp() {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  private boolean shouldUploadLogs(ContainerLogContext logContext) {
    return logAggPolicy.shouldDoLogAggregation(logContext);
  }

  @Override
  public void startContainerLogAggregation(ContainerLogContext logContext) {
    if (shouldUploadLogs(logContext)) {
      LOG.info("Considering container " + logContext.getContainerId()
          + " for log-aggregation");
      this.pendingContainers.add(logContext.getContainerId());
    }
  }

  @Override
  public synchronized void finishLogAggregation() {
    LOG.info("Application just finished : " + this.applicationId);
    this.appFinishing.set(true);
    this.notifyAll();
  }

  @Override
  public synchronized void abortLogAggregation() {
    LOG.info("Aborting log aggregation for " + this.applicationId);
    this.aborted.set(true);
    this.notifyAll();
  }

  @Override
  public void disableLogAggregation() {
    this.logAggregationDisabled = true;
  }

  @Override
  public void enableLogAggregation() {
    this.logAggregationDisabled = false;
  }

  @Override
  public boolean isAggregationEnabled() {
    return !logAggregationDisabled;
  }

  @Private
  @VisibleForTesting
  // This is only used for testing.
  // This will wake the log aggregation thread that is waiting for
  // rollingMonitorInterval.
  // To use this method, make sure the log aggregation thread is running
  // and waiting for rollingMonitorInterval.
  public synchronized void doLogAggregationOutOfBand() {
    while(!waiting.get()) {
      try {
        wait(200);
      } catch (InterruptedException e) {
        // Do Nothing
      }
    }
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }

  class ContainerLogAggregator {
    private final AggregatedLogFormat.LogRetentionContext retentionContext;
    private final ContainerId containerId;
    private Set<String> uploadedFileMeta = new HashSet<String>();
    public ContainerLogAggregator(ContainerId containerId) {
      this.containerId = containerId;
      this.retentionContext = getRetentionContext();
    }

    private AggregatedLogFormat.LogRetentionContext getRetentionContext() {
      final long logRetentionSecs =
          conf.getLong(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS,
              YarnConfiguration.DEFAULT_LOG_AGGREGATION_RETAIN_SECONDS);
      return new AggregatedLogFormat.LogRetentionContext(
          recoveredLogInitedTime, logRetentionSecs * 1000);
    }

    public Set<Path> doContainerLogAggregation(
        LogAggregationFileController logAggregationFileController,
        boolean appFinished, boolean containerFinished) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirsForRead()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirsForRead(), containerId,
            userUgi.getShortUserName(), logAggregationContext,
            this.uploadedFileMeta,  retentionContext, appFinished,
            containerFinished);
      try {
        logAggregationFileController.write(logKey, logValue);
      } catch (Exception e) {
        LOG.error("Couldn't upload logs for " + containerId
            + ". Skipping this container.", e);
        return new HashSet<Path>();
      }
      this.uploadedFileMeta.addAll(logValue
        .getCurrentUpLoadedFileMeta());
      // if any of the previous uploaded logs have been deleted,
      // we need to remove them from alreadyUploadedLogs
      Iterable<String> mask =
          Iterables.filter(uploadedFileMeta, new Predicate<String>() {
            @Override
            public boolean apply(String next) {
              return logValue.getAllExistingFilesMeta().contains(next);
            }
          });

      this.uploadedFileMeta = Sets.newHashSet(mask);

      // need to return files uploaded or older-than-retention clean up.
      return Sets.union(logValue.getCurrentUpLoadedFilesPath(),
          logValue.getObsoleteRetentionLogFiles());

    }
  }

  // only for test
  @VisibleForTesting
  public UserGroupInformation getUgi() {
    return this.userUgi;
  }

  public UserGroupInformation updateCredentials(Credentials cred) {
    this.userUgi.addCredentials(cred);
    return userUgi;
  }

  @Private
  @VisibleForTesting
  public int getLogAggregationTimes() {
    return this.logAggregationTimes;
  }

  @VisibleForTesting
  int getCleanupOldLogTimes() {
    return this.cleanupOldLogTimes;
  }

  @VisibleForTesting
  public LogAggregationFileController getLogAggregationFileController() {
    return this.logAggregationFileController;
  }

  @VisibleForTesting
  public LogAggregationFileControllerContext
      getLogAggregationFileControllerContext() {
    return this.logControllerContext;
  }
}
