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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LogAggregationService extends AbstractService implements
    LogHandler {

  private static final Log LOG = LogFactory
      .getLog(LogAggregationService.class);

  /*
   * Expected deployment TLD will be 1777, owner=<NMOwner>, group=<NMGroup -
   * Group to which NMOwner belongs> App dirs will be created as 770,
   * owner=<AppOwner>, group=<NMGroup>: so that the owner and <NMOwner> can
   * access / modify the files.
   * <NMGroup> should obviously be a limited access group.
   */
  /**
   * Permissions for the top level directory under which app directories will be
   * created.
   */
  private static final FsPermission TLDIR_PERMISSIONS = FsPermission
      .createImmutable((short) 01777);
  /**
   * Permissions for the Application directory.
   */
  private static final FsPermission APP_DIR_PERMISSIONS = FsPermission
      .createImmutable((short) 0770);

  private final Context context;
  private final DeletionService deletionService;
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  Path remoteRootLogDir;
  String remoteRootLogDirSuffix;
  private NodeId nodeId;
  private Configuration sslConf;

  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  private final ExecutorService threadPool;
  
  public LogAggregationService(Dispatcher dispatcher, Context context,
      DeletionService deletionService, LocalDirsHandlerService dirsHandler) {
    super(LogAggregationService.class.getName());
    this.dispatcher = dispatcher;
    this.context = context;
    this.deletionService = deletionService;
    this.dirsHandler = dirsHandler;
    this.appLogAggregators =
        new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.threadPool = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("LogAggregationService #%d")
          .build());
    sslConf = new Configuration(false);
  }

  protected void serviceInit(Configuration conf) throws Exception {
    this.remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    this.remoteRootLogDirSuffix =
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
          "ssl-server.xml"));
    } else {
      sslConf = null;
    }
    
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // NodeId is only available during start, the following cannot be moved
    // anywhere else.
    this.nodeId = this.context.getNodeId();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    stopAggregators();
    super.serviceStop();
  }
   
  private void stopAggregators() {
    threadPool.shutdown();
    boolean supervised = getConfig().getBoolean(
        YarnConfiguration.NM_RECOVERY_SUPERVISED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
    // if recovery on restart is supported then leave outstanding aggregations
    // to the next restart
    boolean shouldAbort = context.getNMStateStore().canRecover()
        && !context.getDecommissioned() && supervised;
    // politely ask to finish
    for (AppLogAggregator aggregator : appLogAggregators.values()) {
      if (shouldAbort) {
        aggregator.abortLogAggregation();
      } else {
        aggregator.finishLogAggregation();
      }
    }
    while (!threadPool.isTerminated()) { // wait for all threads to finish
      for (ApplicationId appId : appLogAggregators.keySet()) {
        LOG.info("Waiting for aggregation to complete for " + appId);
      }
      try {
        if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
          threadPool.shutdownNow(); // send interrupt to hurry them along
        }
      } catch (InterruptedException e) {
        LOG.warn("Aggregation stop interrupted!");
        break;
      }
    }
    for (ApplicationId appId : appLogAggregators.keySet()) {
      LOG.warn("Some logs may not have been aggregated for " + appId);
    }
  }

  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return this.remoteRootLogDir.getFileSystem(conf);
  }

  void verifyAndCreateRemoteLogDir(Configuration conf) {
    // Checking the existence of the TLD
    FileSystem remoteFS = null;
    try {
      if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
            CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)
          && sslConf != null) {
        // Setting the keystore file path here is necessary to get the correct
        // cached FileSystem object
        String kstorePath = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY));
        String kstorePass = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY));
        String keyPass = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY));
        String tstorePath = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
        String tstorePass = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
        HopsSSLSocketFactory.configureTlsClient(kstorePath, kstorePass,
            keyPass, tstorePath, tstorePass, conf);
      }
      
      remoteFS = getFileSystem(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Unable to get Remote FileSystem instance", e);
    }
    boolean remoteExists = true;
    try {
      FsPermission perms =
          remoteFS.getFileStatus(this.remoteRootLogDir).getPermission();
      if (!perms.equals(TLDIR_PERMISSIONS)) {
        LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
            + "] already exist, but with incorrect permissions. "
            + "Expected: [" + TLDIR_PERMISSIONS + "], Found: [" + perms
            + "]." + " The cluster may have problems with multiple users.");
      }
    } catch (FileNotFoundException e) {
      remoteExists = false;
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Failed to check permissions for dir ["
              + this.remoteRootLogDir + "]", e);
    }
    if (!remoteExists) {
      LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
          + "] does not exist. Attempting to create it.");
      try {
        Path qualified =
            this.remoteRootLogDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());
        remoteFS.mkdirs(qualified, new FsPermission(TLDIR_PERMISSIONS));
        remoteFS.setPermission(qualified, new FsPermission(TLDIR_PERMISSIONS));

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        String primaryGroupName = null;
        try {
          primaryGroupName = loginUser.getPrimaryGroupName();
        } catch (IOException e) {
          LOG.warn("No primary group found. The remote root log directory" +
              " will be created with the HDFS superuser being its group " +
              "owner. JobHistoryServer may be unable to read the directory.");
        }
        // set owner on the remote directory only if the primary group exists
        if (primaryGroupName != null) {
          remoteFS.setOwner(qualified,
              loginUser.getShortUserName(), primaryGroupName);
        }
      } catch (IOException e) {
        throw new YarnRuntimeException("Failed to create remoteLogDir ["
            + this.remoteRootLogDir + "]", e);
      }
    }
  }

  Path getRemoteNodeLogFileForApp(ApplicationId appId, String user) {
    return LogAggregationUtils.getRemoteNodeLogFileForApp(
        this.remoteRootLogDir, appId, user, this.nodeId,
        this.remoteRootLogDirSuffix);
  }

  Path getRemoteAppLogDir(ApplicationId appId, String user) {
    return LogAggregationUtils.getRemoteAppLogDir(this.remoteRootLogDir, appId,
        user, this.remoteRootLogDirSuffix);
  }

  private void createDir(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    FsPermission dirPerm = new FsPermission(fsPerm);
    fs.mkdirs(path, dirPerm);
    FsPermission umask = FsPermission.getUMask(fs.getConf());
    if (!dirPerm.equals(dirPerm.applyUMask(umask))) {
      fs.setPermission(path, new FsPermission(fsPerm));
    }
  }

  private boolean checkExists(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    boolean exists = true;
    try {
      FileStatus appDirStatus = fs.getFileStatus(path);
      if (!APP_DIR_PERMISSIONS.equals(appDirStatus.getPermission())) {
        fs.setPermission(path, APP_DIR_PERMISSIONS);
      }
    } catch (FileNotFoundException fnfe) {
      exists = false;
    }
    return exists;
  }

  protected void createAppDir(final String user, final ApplicationId appId,
      UserGroupInformation userUgi) {
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          try {
            // TODO: Reuse FS for user?
            // Setting the keystore file path here is necessary to get the correct
            // cached FileSystem object
            Configuration conf = getConfig();
            if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
                CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
              conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY
                      .getValue(),
                  context.getCertificateLocalizationService()
                      .getMaterialLocation(user, appId.toString()).getKeyStoreLocation().toString());
            }
            
            FileSystem remoteFS = getFileSystem(conf);
            
            // Only creating directories if they are missing to avoid
            // unnecessary load on the filesystem from all of the nodes
            Path appDir = LogAggregationUtils.getRemoteAppLogDir(
                LogAggregationService.this.remoteRootLogDir, appId, user,
                LogAggregationService.this.remoteRootLogDirSuffix);
            appDir = appDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());

            if (!checkExists(remoteFS, appDir, APP_DIR_PERMISSIONS)) {
              Path suffixDir = LogAggregationUtils.getRemoteLogSuffixedDir(
                  LogAggregationService.this.remoteRootLogDir, user,
                  LogAggregationService.this.remoteRootLogDirSuffix);
              suffixDir = suffixDir.makeQualified(remoteFS.getUri(),
                  remoteFS.getWorkingDirectory());

              if (!checkExists(remoteFS, suffixDir, APP_DIR_PERMISSIONS)) {
                Path userDir = LogAggregationUtils.getRemoteLogUserDir(
                    LogAggregationService.this.remoteRootLogDir, user);
                userDir = userDir.makeQualified(remoteFS.getUri(),
                    remoteFS.getWorkingDirectory());

                if (!checkExists(remoteFS, userDir, APP_DIR_PERMISSIONS)) {
                  createDir(remoteFS, userDir, APP_DIR_PERMISSIONS);
                }

                createDir(remoteFS, suffixDir, APP_DIR_PERMISSIONS);
              }

              createDir(remoteFS, appDir, APP_DIR_PERMISSIONS);
            }

          } catch (IOException e) {
            LOG.error("Failed to setup application log directory for "
                + appId, e);
            throw e;
          }
          return null;
        }
      });
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, String userFolder) {
    ApplicationEvent eventResponse;
    try {
      verifyAndCreateRemoteLogDir(getConfig());
      initAppAggregator(appId, user, credentials, appAcls,
          logAggregationContext, userFolder);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
    } catch (YarnRuntimeException e) {
      LOG.warn("Application failed to init aggregation", e);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED);
    }
    this.dispatcher.getEventHandler().handle(eventResponse);
  }
  
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }


  protected void initAppAggregator(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, String userFolder) {

    // Get user's FileSystem credentials
    final UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    userUgi.addApplicationId(appId.toString());
    if (credentials != null) {
      userUgi.addCredentials(credentials);
    }

    // New application
    final AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.dispatcher, this.deletionService,
            getConfig(), appId, userUgi, this.nodeId, dirsHandler,
        getRemoteNodeLogFileForApp(appId, user),
        appAcls, logAggregationContext, this.context,
        getLocalFileContext(getConfig()), userFolder);
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnRuntimeException("Duplicate initApp for " + appId);
    }
    // wait until check for existing aggregator to create dirs
    YarnRuntimeException appDirException = null;
    try {
      // Create the app dir
      createAppDir(user, appId, userUgi);
    } catch (Exception e) {
      appLogAggregator.disableLogAggregation();
      if (!(e instanceof YarnRuntimeException)) {
        appDirException = new YarnRuntimeException(e);
      } else {
        appDirException = (YarnRuntimeException)e;
      }
      appLogAggregators.remove(appId);
      closeFileSystems(userUgi);
      throw appDirException;
    }

    // TODO Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    Runnable aggregatorWrapper = new Runnable() {
      public void run() {
        try {
          appLogAggregator.run();
        } finally {
          appLogAggregators.remove(appId);
          closeFileSystems(userUgi);
        }
      }
    };
    this.threadPool.execute(aggregatorWrapper);
  }

  protected void closeFileSystems(final UserGroupInformation userUgi) {
    try {
      FileSystem.closeAllForUGI(userUgi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  // for testing only
  @Private
  int getNumAggregators() {
    return this.appLogAggregators.size();
  }

  private void stopContainer(ContainerId containerId, int exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.
    AppLogAggregator aggregator = this.appLogAggregators.get(
        containerId.getApplicationAttemptId().getApplicationId());
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + containerId
          + ", did it fail to start?");
      return;
    }
    Container container = context.getContainers().get(containerId);
    if (null == container) {
      LOG.warn("Log aggregation cannot be started for " + containerId
          + ", as its an absent container");
      return;
    }
    ContainerType containerType =
        container.getContainerTokenIdentifier().getContainerType();
    aggregator.startContainerLogAggregation(
        new ContainerLogContext(containerId, containerType, exitCode));
  }

  @SuppressWarnings("unchecked")
  private void stopApp(ApplicationId appId) {

    // App is complete. Finish up any containers' pending log aggregation and
    // close the application specific logFile.

    AppLogAggregator aggregator = this.appLogAggregators.get(appId);
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + appId
          + ", did it fail to start?");
      this.dispatcher.getEventHandler().handle(
          new ApplicationEvent(appId,
              ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
      return;
    }
    aggregator.finishLogAggregation();
  }

  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartEvent =
            (LogHandlerAppStartedEvent) event;
        initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
            appStartEvent.getCredentials(),
            appStartEvent.getApplicationAcls(),
            appStartEvent.getLogAggregationContext(),
            appStartEvent.getUserFolder());
        break;
      case CONTAINER_FINISHED:
        LogHandlerContainerFinishedEvent containerFinishEvent =
            (LogHandlerContainerFinishedEvent) event;
        stopContainer(containerFinishEvent.getContainerId(),
            containerFinishEvent.getExitCode());
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        stopApp(appFinishedEvent.getApplicationId());
        break;
      default:
        ; // Ignore
    }

  }

  @VisibleForTesting
  public ConcurrentMap<ApplicationId, AppLogAggregator> getAppLogAggregators() {
    return this.appLogAggregators;
  }

  @VisibleForTesting
  public NodeId getNodeId() {
    return this.nodeId;
  }
}
