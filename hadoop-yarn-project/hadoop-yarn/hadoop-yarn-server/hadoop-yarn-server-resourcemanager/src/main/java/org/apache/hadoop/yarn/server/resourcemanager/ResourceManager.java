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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import io.hops.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.ssl.CertificateLocalizationCtx;
import org.apache.hadoop.security.ssl.RevocationListFetcherService;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppCertificateManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppCertificateManagerEventType;
import org.apache.hadoop.yarn.server.security.CertificateLocalizationService;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilter;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.yarn.server.resourcemanager.quota.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.quota.PriceMultiplicatiorService;
import org.apache.hadoop.yarn.server.resourcemanager.quota.QuotaService;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 *
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService implements Recoverable {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  private static long clusterTimeStamp = System.currentTimeMillis();

  private Lock resourceTrackingServiceStartStopLock = new ReentrantLock(true);
  
  /**
   * "Always On" services. Services that need to run always irrespective of
   * the HA state of the RM.
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;
  private Dispatcher rmDispatcher;
  @VisibleForTesting
  protected AdminService adminService;

  /**
   * "Active" services. Services that need to run only on the Active RM.
   * These services are managed (initialized, started, stopped) by the
   * {@link CompositeService} RMActiveServices.
   *
   * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
   * in Active state.
   */
  protected RMSchedulerServices schedulerServices;
  protected RMSecretManagerService rmSecretManagerService;

  protected ResourceScheduler scheduler;
  protected QuotaService quotaService;
  protected ContainersLogsService containersLogsService;
  protected PriceMultiplicatiorService priceMultiplicatiorService;
  protected CertificateLocalizationService certificateLocalizationService;
  protected RevocationListFetcherService revocationListFetcherService;
  protected ReservationSystem reservationSystem;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  protected RMAppManager rmAppManager;
  protected RMAppCertificateManager rmAppCertificateManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;
  private JvmPauseMonitor pauseMonitor;
  private boolean curatorEnabled = false;
  private CuratorFramework curator;
  private final String zkRootNodePassword =
      Long.toString(new SecureRandom().nextLong());
  private boolean recoveryEnabled;

  @VisibleForTesting
  protected String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  /** End of Active services */

  private Configuration conf;

  private UserGroupInformation rmLoginUGI;
  
  private ResourceTrackingServices resourceTrackingService;
  
  protected GroupMembershipService groupMembershipService;
  
  public ResourceManager() {
    super("ResourceManager");
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  @VisibleForTesting
  protected static void setClusterTimeStamp(long timestamp) {
    clusterTimeStamp = timestamp;
  }

  @VisibleForTesting
  Dispatcher getRmDispatcher() {
    return rmDispatcher;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    this.rmContext = new RMContextImpl();
    
    this.rmContext.setIsDistributed(conf.getBoolean(
            YarnConfiguration.DISTRIBUTED_RM,
            YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
    
    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    // load core-site.xml
    InputStream coreSiteXMLInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    if (coreSiteXMLInputStream != null) {
      this.conf.addResource(coreSiteXMLInputStream,
          YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    }

    // Do refreshUserToGroupsMappings with loaded core-site.xml
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(this.conf)
        .refresh();

    // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
    // Or use RM specific configurations to overwrite the common ones first
    // if they exist
    RMServerUtils.processRMProxyUsersConf(conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

    // load yarn-site.xml
    InputStream yarnSiteXMLInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    if (yarnSiteXMLInputStream != null) {
      this.conf.addResource(yarnSiteXMLInputStream,
          YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    }

    validateConfigs(this.conf);
  
    createAndInitCRLFetcherService();
    
    // Set HA configuration should be done before login
    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
      if (this.rmContext.isDistributed() || (HAUtil.isAutomaticFailoverEnabled(conf)
              && HAUtil.isHopsRMFailoverProxy(conf))) {
        groupMembershipService = createGroupMembershipService();
        addService(groupMembershipService);
        rmContext.setRMGroupMembershipService(groupMembershipService);
      }
    }else if(this.rmContext.isDistributed()){
      groupMembershipService = createGroupMembershipService();
      addService(groupMembershipService);
      rmContext.setRMGroupMembershipService(groupMembershipService);
    }
    // Set UGI and do login
    // If security is enabled, use login user
    // If security is not enabled, use current user
    this.rmLoginUGI = UserGroupInformation.getCurrentUser();
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    // register the handlers for all AlwaysOn services using setupDispatcher().
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    // The order of services below should not be changed as services will be
    // started in same order
    // As elector service needs admin service to be initialized and started,
    // first we add admin service then elector service

    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);

    // elector must be added post adminservice
    if (this.rmContext.isHAEnabled()) {
      // If the RM is configured to use an embedded leader elector,
      // initialize the leader elector.
      if (HAUtil.isAutomaticFailoverEnabled(conf)
          && HAUtil.isAutomaticFailoverEmbedded(conf)) {
        EmbeddedElector elector = createEmbeddedElector();
        addIfService(elector);
        rmContext.setLeaderElectorService(elector);
      }
    }

    rmContext.setYarnConfiguration(conf);
    
    byte[] seed = new byte[16];
    new Random().nextBytes(seed);
    rmContext.setSeed(seed);

    rmContext.setUserFolderHashAlgo(conf.get(YarnConfiguration.USER_FOLDER_ALGO,
        YarnConfiguration.DEFAULT_USER_FOLDER_ALGO));
    createAndInitSchedulerServices();

    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
                      YarnConfiguration.RM_BIND_HOST,
                      WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));

    RMApplicationHistoryWriter rmApplicationHistoryWriter =
        createRMApplicationHistoryWriter();
    addService(rmApplicationHistoryWriter);
    rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    SystemMetricsPublisher systemMetricsPublisher = createSystemMetricsPublisher();
    addService(systemMetricsPublisher);
    rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

    createCertificateLocalizationService();
    
    super.serviceInit(this.conf);
  }


  @SuppressWarnings("deprecation")
  protected EmbeddedElector createEmbeddedElector() throws Exception {
    EmbeddedElector elector;
    curatorEnabled =
        conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
            YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
    if (curatorEnabled) {
      this.curator = createAndStartCurator(conf);
      elector = new CuratorBasedElectorService(rmContext, this);
    } else {
      elector = new ActiveStandbyElectorBasedElectorService(rmContext);
    }
    return elector;
  }

  public CuratorFramework createAndStartCurator(Configuration conf)
      throws Exception {
    String zkHostPort = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    if (zkHostPort == null) {
      throw new YarnRuntimeException(
          YarnConfiguration.RM_ZK_ADDRESS + " is not configured.");
    }
    int numRetries = conf.getInt(YarnConfiguration.RM_ZK_NUM_RETRIES,
        YarnConfiguration.DEFAULT_ZK_RM_NUM_RETRIES);
    int zkSessionTimeout = conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);
    int zkRetryInterval = conf.getInt(YarnConfiguration.RM_ZK_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_ZK_RETRY_INTERVAL_MS);

    // set up zk auths
    List<ZKUtil.ZKAuthInfo> zkAuths = RMZKUtils.getZKAuths(conf);
    List<AuthInfo> authInfos = new ArrayList<>();
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
    }

    if (HAUtil.isHAEnabled(conf) && HAUtil.getConfValueForRMInstance(
        YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf) == null) {
      String zkRootNodeUsername = HAUtil
          .getConfValueForRMInstance(YarnConfiguration.RM_ADDRESS,
              YarnConfiguration.DEFAULT_RM_ADDRESS, conf);
      byte[] defaultFencingAuth =
          (zkRootNodeUsername + ":" + zkRootNodePassword)
              .getBytes(Charset.forName("UTF-8"));
      authInfos.add(new AuthInfo(new DigestAuthenticationProvider().getScheme(),
          defaultFencingAuth));
    }

    CuratorFramework client =  CuratorFrameworkFactory.builder()
        .connectString(zkHostPort)
        .sessionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(numRetries, zkRetryInterval))
        .authorization(authInfos).build();
    client.start();
    return client;
  }

  public CuratorFramework getCurator() {
    return this.curator;
  }

  public String getZkRootNodePassword() {
    return this.zkRootNodePassword;
  }


  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return new QueueACLsManager(scheduler, conf);
  }

  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    rmStore.setResourceManager(this);
    rmContext.setStateStore(rmStore);
  }

  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new SchedulerEventDispatcher(this.scheduler);
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  protected ResourceScheduler createScheduler() {
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
  }

  protected ReservationSystem createReservationSystem() {
    String reservationClassName =
        conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_CLASS,
            AbstractReservationSystem.getDefaultReservationSystem(scheduler));
    if (reservationClassName == null) {
      return null;
    }
    LOG.info("Using ReservationSystem: " + reservationClassName);
    try {
      Class<?> reservationClazz = Class.forName(reservationClassName);
      if (ReservationSystem.class.isAssignableFrom(reservationClazz)) {
        return (ReservationSystem) ReflectionUtils.newInstance(
            reservationClazz, this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + reservationClassName
            + " not instance of " + ReservationSystem.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate ReservationSystem: " + reservationClassName, e);
    }
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
  }
  
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    return new RMNodeLabelsManager();
  }
  
  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
      this.applicationACLsManager, this.conf);
  }
  
  protected RMAppCertificateManager createRMAppCertificateManager() throws Exception {
    return new RMAppCertificateManager(this.rmContext);
  }

  protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
    return new RMApplicationHistoryWriter();
  }

  protected SystemMetricsPublisher createSystemMetricsPublisher() {
    return new SystemMetricsPublisher(); 
  }

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int globalMaxAppAttempts =
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid global max attempts configuration"
          + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
          + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException("Nodemanager expiry interval should be no"
          + " less than heartbeat interval, "
          + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
          + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
          + heartbeatIntvl);
    }
  }

  @Private
  class ResourceTrackingServices extends CompositeService {
    private ContainerAllocationExpirer containerAllocationExpirer;
    private boolean recoveryEnabled;
    private RMActiveServiceContext activeServiceContext;
    private ResourceManager rm;
    private StreamingReceiver streamingReceiver;
    
    public ResourceTrackingServices(ResourceManager rm) {
      super("ResourceTrackingServices");
      LOG.info("create resourceTrackingService");
      this.rm = rm;
    }

    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      LOG.info("init resourceTrackingService");
      conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);
      activeServiceContext = new RMActiveServiceContext();
      rmContext.setActiveServiceContext(activeServiceContext);
      
      rmSecretManagerService = createRMSecretManagerService();
      addService(rmSecretManagerService);

      containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
      addService(containerAllocationExpirer);
      rmContext.setContainerAllocationExpirer(containerAllocationExpirer);

      AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
      addService(amLivelinessMonitor);
      rmContext.setAMLivelinessMonitor(amLivelinessMonitor);

      AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
      addService(amFinishingMonitor);
      rmContext.setAMFinishingMonitor(amFinishingMonitor);
      
      RMNodeLabelsManager nlm = createNodeLabelManager();
      nlm.setRMContext(rmContext);
      addService(nlm);
      rmContext.setNodeLabelManager(nlm);

      RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater =
          createRMDelegatedNodeLabelsUpdater();
      if (delegatedNodeLabelsUpdater != null) {
        addService(delegatedNodeLabelsUpdater);
        rmContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
      }

      recoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
          YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

      RMStateStore rmStore = null;
      if (recoveryEnabled) {
        rmStore = RMStateStoreFactory.getStore(conf);
        boolean isWorkPreservingRecoveryEnabled =
            conf.getBoolean(
              YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
              YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);
        rmContext
            .setWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled);
      } else {
        rmStore = new NullRMStateStore();
      }

      try {
        rmStore.setResourceManager(rm);
        rmStore.init(conf);
        rmStore.setRMDispatcher(rmDispatcher);
      } catch (Exception e) {
        // the Exception from stateStore.init() needs to be handled for
        // HA and we need to give up master status if we got fenced
        LOG.error("Failed to init state store", e);
        throw e;
      }
      rmContext.setStateStore(rmStore);

      // Register event handler for NodesListManager
      nodesListManager = new NodesListManager(rmContext);
      rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
      addService(nodesListManager);
      rmContext.setNodesListManager(nodesListManager);

      // Register event handler for RmNodes
      rmDispatcher.register(
          RMNodeEventType.class, new NodeEventDispatcher(rmContext));

      nmLivelinessMonitor = createNMLivelinessMonitor();
      addService(nmLivelinessMonitor);

      resourceTracker = createResourceTrackerService();
      addService(resourceTracker);
      rmContext.setResourceTrackerService(resourceTracker);

      DefaultMetricsSystem.initialize("ResourceManager");
      JvmMetrics jm = JvmMetrics.initSingleton("ResourceManager", null);
      pauseMonitor = new JvmPauseMonitor(conf);
      jm.setPauseMonitor(pauseMonitor);
      super.serviceInit(conf);
    }
    
    @Override
    protected void serviceStart() throws Exception {
      LOG.info("starting resourceTrackingService");
      RMStateStore rmStore = rmContext.getStateStore();
      // The state store needs to start irrespective of recoveryEnabled as apps
      // need events to move to further states.
      rmStore.start();
      
      pauseMonitor.start();

      if (rmContext.isDistributed()) {
        if (!rmContext.isLeader()) {
          LOG.info("streaming processor is starting for resource tracker");
          RMStorageFactory.kickEventStreamingAPI(false, conf);
          streamingReceiver = new RtStreamingProcessor(rmContext);
          streamingReceiver.start();
        } else {
          LOG.info("streaming processor is starting for scheduler");
          RMStorageFactory.kickEventStreamingAPI(true, conf);
          streamingReceiver = new RmStreamingProcessor(rmContext);
          streamingReceiver.start();
        }
      }
      
      if(recoveryEnabled) {
        try {
          LOG.info("Recovery started");
          rmStore.checkVersion();
          if (rmContext.isWorkPreservingRecoveryEnabled()) {
            rmContext.setEpoch(rmStore.getAndIncrementEpoch());
          }
          RMState state = rmStore.loadState();
          recover(state);
          LOG.info("Recovery ended");
        } catch (Exception e) {
          // the Exception from loadState() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to load/recover state", e);
          throw e;
        }
      }
      super.serviceStart();
      LOG.info("started resourceTrackingService");
    }
    
    @Override
    protected void serviceStop() throws Exception {
      if (streamingReceiver != null) {
        streamingReceiver.stop();
        RMStorageFactory.stopEventStreamingAPI();
      }
      super.serviceStop();
    }
  }
  
  /**
   * RMActiveServices handles all the Active services in the RM.
   */
  @Private
  public class RMSchedulerServices extends CompositeService {

    private DelegationTokenRenewer delegationTokenRenewer;
    private EventHandler<SchedulerEvent> schedulerDispatcher;
    private ApplicationMasterLauncher applicationMasterLauncher;
   

    RMSchedulerServices() {
      super("RMActiveServices");
    }
    
    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      createAndInitResourceTrackingServices();
 
      if (UserGroupInformation.isSecurityEnabled()) {
        delegationTokenRenewer = createDelegationTokenRenewer();
        rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
      }
      
      // Initialize the scheduler
      scheduler = createScheduler();
      scheduler.setRMContext(rmContext);
      addIfService(scheduler);
      rmContext.setScheduler(scheduler);

      schedulerDispatcher = createSchedulerEventDispatcher();
      addIfService(schedulerDispatcher);
      rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

      // Register event handler for RmAppEvents
      rmDispatcher.register(RMAppEventType.class,
          new ApplicationEventDispatcher(rmContext));

      // Register event handler for RmAppAttemptEvents
      rmDispatcher.register(RMAppAttemptEventType.class,
          new ApplicationAttemptEventDispatcher(rmContext));

      // Initialize the Reservation system
      if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
        reservationSystem = createReservationSystem();
        if (reservationSystem != null) {
          reservationSystem.setRMContext(rmContext);
          addIfService(reservationSystem);
          rmContext.setReservationSystem(reservationSystem);
          LOG.info("Initialized Reservation system");
        }
      }

      // Initialize the container logger system
      containersLogsService = new ContainersLogsService(rmContext);
      addIfService(containersLogsService);
      rmContext.setContainersLogsService(containersLogsService);
      
      // Initialize the quota service
      quotaService = new QuotaService();
      addIfService(quotaService);
      rmContext.setQuotaService(quotaService);
      if(conf.getBoolean(YarnConfiguration.QUOTA_VARIABLE_PRICE_ENABLED,
            YarnConfiguration.DEFAULT_QUOTA_VARIABLE_PRICE_ENABLED)){
        priceMultiplicatiorService = new PriceMultiplicatiorService(rmContext);
        addIfService(priceMultiplicatiorService);
      }
      
      // creating monitors that handle preemption
      createPolicyMonitors();

      masterService = createApplicationMasterService();
      addService(masterService) ;
      rmContext.setApplicationMasterService(masterService);

      applicationACLsManager = new ApplicationACLsManager(conf);

      queueACLsManager = createQueueACLsManager(scheduler, conf);

      rmAppManager = createRMAppManager();
      // Register event handler for RMAppManagerEvents
      rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

      rmAppCertificateManager = createRMAppCertificateManager();
      rmDispatcher.register(RMAppCertificateManagerEventType.class, rmAppCertificateManager);
      addService(rmAppCertificateManager);
      
      clientRM = createClientRMService();
      addService(clientRM);
      rmContext.setClientRMService(clientRM);

      applicationMasterLauncher = createAMLauncher();
      rmDispatcher.register(AMLauncherEventType.class,
          applicationMasterLauncher);

      addService(applicationMasterLauncher);
      if (UserGroupInformation.isSecurityEnabled()) {
        addService(delegationTokenRenewer);
        delegationTokenRenewer.setRMContext(rmContext);
      }

      new RMNMInfo(rmContext, scheduler);

      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      resourceTrackingServiceStartStopLock.lock();
      LOG.info("locked resourceTrackingServiceStart");
      try{
        LOG.info("starting SchedulerService");
        resourceTrackingService.start();
        super.serviceStart();
      }finally{
        LOG.info("unlocked resourceTrackingServiceStart");
        resourceTrackingServiceStartStopLock.unlock();
      }
    }

    @Override
    protected void serviceStop() throws Exception {

      super.serviceStop();

      if (pauseMonitor != null) {
        pauseMonitor.stop();
      }

      DefaultMetricsSystem.shutdown();
      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();
        try {
          if (null != store) {
            store.close();
          }
        } catch (Exception e) {
          LOG.error("Error closing store.", e);
        }
      }

    }

    protected void createPolicyMonitors() {
      if (scheduler instanceof PreemptableResourceScheduler
          && conf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS)) {
        LOG.info("Loading policy monitors");
        List<SchedulingEditPolicy> policies = conf.getInstances(
            YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            SchedulingEditPolicy.class);
        if (policies.size() > 0) {
          for (SchedulingEditPolicy policy : policies) {
            LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
            // periodically check whether we need to take action to guarantee
            // constraints
            SchedulingMonitor mon = new SchedulingMonitor(rmContext, policy);
            addService(mon);
          }
        } else {
          LOG.warn("Policy monitors configured (" +
              YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS +
              ") but none specified (" +
              YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + ")");
        }
      }
    }
  }

  @Private
  public static class SchedulerEventDispatcher extends AbstractService
      implements EventHandler<SchedulerEvent> {

    private final ResourceScheduler scheduler;
    private final BlockingQueue<SchedulerEvent> eventQueue =
      new LinkedBlockingQueue<SchedulerEvent>();
    private volatile int lastEventQueueSizeLogged = 0;
    private final Thread eventProcessor;
    private volatile boolean stopped = false;
    private boolean shouldExitOnError = false;

    public SchedulerEventDispatcher(ResourceScheduler scheduler) {
      super(SchedulerEventDispatcher.class.getName());
      this.scheduler = scheduler;
      this.eventProcessor = new Thread(new EventProcessor());
      this.eventProcessor.setName("ResourceManager Event Processor");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      this.shouldExitOnError =
          conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      this.eventProcessor.start();
      super.serviceStart();
    }

    private final class EventProcessor implements Runnable {
      @Override
      public void run() {

        SchedulerEvent event;

        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return; // TODO: Kill RM.
          }

          try {
            scheduler.handle(event);
          } catch (Throwable t) {
            // An error occurred, but we are shutting down anyway.
            // If it was an InterruptedException, the very act of 
            // shutdown could have caused it and is probably harmless.
            if (stopped) {
              LOG.warn("Exception during shutdown: ", t);
              break;
            }
            LOG.fatal("Error in handling event type " + event.getType()
                + " to the scheduler", t);
            if (shouldExitOnError
                && !ShutdownHookManager.get().isShutdownInProgress()) {
              LOG.info("Exiting, bbye..");
              System.exit(-1);
            }
          }
        }
      }
    }

    @Override
    protected void serviceStop() throws Exception {
      this.stopped = true;
      this.eventProcessor.interrupt();
      try {
        this.eventProcessor.join();
      } catch (InterruptedException e) {
        throw new YarnRuntimeException(e);
      }
      super.serviceStop();
    }

    @Override
    public void handle(SchedulerEvent event) {
      try {
        int qSize = eventQueue.size();
        if (qSize != 0 && qSize % 1000 == 0
            && lastEventQueueSizeLogged != qSize) {
          lastEventQueueSizeLogged = qSize;
          LOG.info("Size of scheduler event-queue is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          LOG.info("Very low remaining capacity on scheduler event queue: "
              + remCapacity);
        }
        this.eventQueue.put(event);
      } catch (InterruptedException e) {
        LOG.info("Interrupted. Trying to exit gracefully.");
      }
    }
  }

  @Private
  public static class RMFatalEventDispatcher
      implements EventHandler<RMFatalEvent> {

    @Override
    public void handle(RMFatalEvent event) {
      LOG.fatal("Received a " + RMFatalEvent.class.getName() + " of type " +
          event.getType().name() + ". Cause:\n" + event.getCause());

      ExitUtil.terminate(1, event.getCause());
    }
  }

  public void handleTransitionToStandBy() {
    if (rmContext.isHAEnabled() || rmContext.isDistributed()) {
      try {
        // Transition to standby and reinit active services
        LOG.info("Transitioning RM to Standby mode");
        transitionToStandby(true);
        EmbeddedElector elector = rmContext.getLeaderElectorService();
        if (elector != null) {
          elector.rejoinElection();
        }
      } catch (Exception e) {
        LOG.fatal("Failed to transition RM to Standby mode.", e);
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Private
  public static final class ApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public ApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;

    public ApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      ApplicationId appAttemptId = appAttemptID.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appAttemptId);
      if (rmApp != null) {
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptID);
        if (rmAppAttempt != null) {
          try {
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
        }
      }
    }
  }

  @Private
  public static final class NodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for node " + nodeId, t);
        }
      }
    }
  }
  
  protected void startWepApp() {
    LOG.info("+");
    // Use the customized yarn filter instead of the standard kerberos filter to
    // allow users to authenticate using delegation tokens
    // 4 conditions need to be satisfied -
    // 1. security is enabled
    // 2. http auth type is set to kerberos
    // 3. "yarn.resourcemanager.webapp.use-yarn-filter" override is set to true
    // 4. hadoop.http.filter.initializers container AuthenticationFilterInitializer

    Configuration conf = getConfig();
    LOG.info("+");
    boolean enableCorsFilter =
        conf.getBoolean(YarnConfiguration.RM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ENABLE_CORS_FILTER);
    LOG.info("+");
    boolean useYarnAuthenticationFilter =
        conf.getBoolean(
          YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
          YarnConfiguration.DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER);
    LOG.info("+");
    String authPrefix = "hadoop.http.authentication.";
    String authTypeKey = authPrefix + "type";
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    String actualInitializers = "";
    LOG.info("+");
    Class<?>[] initializersClasses =
        conf.getClasses(filterInitializerConfKey);
LOG.info("+");
    // setup CORS
    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }
LOG.info("+");
    boolean hasHadoopAuthFilterInitializer = false;
    boolean hasRMAuthFilterInitializer = false;
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName().equals(
          AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
        }
        if (initializer.getName().equals(
          RMAuthenticationFilterInitializer.class.getName())) {
          hasRMAuthFilterInitializer = true;
        }
      }
      if (UserGroupInformation.isSecurityEnabled()
          && useYarnAuthenticationFilter
          && hasHadoopAuthFilterInitializer
          && conf.get(authTypeKey, "").equals(
            KerberosAuthenticationHandler.TYPE)) {
        ArrayList<String> target = new ArrayList<String>();
        for (Class<?> filterInitializer : initializersClasses) {
          if (filterInitializer.getName().equals(
            AuthenticationFilterInitializer.class.getName())) {
            if (hasRMAuthFilterInitializer == false) {
              target.add(RMAuthenticationFilterInitializer.class.getName());
            }
            continue;
          }
          target.add(filterInitializer.getName());
        }
        actualInitializers = StringUtils.join(",", target);

        LOG.info("Using RM authentication filter(kerberos/delegation-token)"
            + " for RM webapp authentication");
        RMAuthenticationFilter
          .setDelegationTokenSecretManager(getClientRMService().rmDTSecretManager);
        conf.set(filterInitializerConfKey, actualInitializers);
      }
    }
LOG.info("+");
    // if security is not enabled and the default filter initializer has not 
    // been set, set the initializer to include the
    // RMAuthenticationFilterInitializer which in turn will set up the simple
    // auth filter.

    String initializers = conf.get(filterInitializerConfKey);
    if (!UserGroupInformation.isSecurityEnabled()) {
      if (initializersClasses == null || initializersClasses.length == 0) {
        conf.set(filterInitializerConfKey,
          RMAuthenticationFilterInitializer.class.getName());
        conf.set(authTypeKey, "simple");
      } else if (initializers.equals(StaticUserWebFilter.class.getName())) {
        conf.set(filterInitializerConfKey,
          RMAuthenticationFilterInitializer.class.getName() + ","
              + initializers);
        conf.set(authTypeKey, "simple");
      }
    }
LOG.info("+");
    Builder<ApplicationMasterService> builder = 
        WebApps
            .$for("cluster", ApplicationMasterService.class, masterService,
                "ws")
            .with(conf)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .at(webAppAddress);
    String proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
    if(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
        equals(proxyHostAndPort)) {
      if (HAUtil.isHAEnabled(conf)) {
        fetcher = new AppReportFetcher(conf);
      } else {
        fetcher = new AppReportFetcher(conf, getClientRMService());
      }
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);

    }
    LOG.info("+");
    webApp = builder.start(new RMWebApp(this));
    LOG.info("+");
  }

  /**
   * Helper method to create and init {@link #schedulerServices}. This creates an
   * instance of {@link RMActiveServices} and initializes it.
   */
  protected void createAndInitSchedulerServices() throws Exception {
    schedulerServices = new RMSchedulerServices();
    schedulerServices.init(conf);
  }
  
  void createAndInitResourceTrackingServices() {
    resourceTrackingService = new ResourceTrackingServices(this);
    resourceTrackingService.init(conf);
  }

  /**
   * Helper method to start {@link #schedulerServices}.
   * @throws Exception
   */
  void startSchedulerServices() throws Exception {
    if (schedulerServices != null) {
      clusterTimeStamp = System.currentTimeMillis();
      schedulerServices.start();
    }
  }

  /**
   * Helper method to stop {@link #schedulerServices}.
   * @throws Exception
   */
  void stopSchedulerServices() throws Exception {
    if (schedulerServices != null) {
      schedulerServices.stop();
      schedulerServices = null;
      //do we need any of the following??
//      rmContext.getRMNodes().clear();
//      rmContext.getInactiveRMNodes().clear();
//      rmContext.getRMApps().clear();
//      ClusterMetrics.destroy();
//      QueueMetrics.clearQueueMetrics();    
//
//      if (eventRetriever != null) {
//        LOG.info("NDB Event streaming is stoping now ..");
//        eventRetriever.finish();
//      }
    }
  }

  void reinitialize(boolean initialize) throws Exception {
    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    if (initialize) {
      resetDispatcher();
      createAndInitSchedulerServices();
      if (rmContext.isDistributed()) {
        resourceTrackingService.start();
      }
    }
  }

  @VisibleForTesting
  public boolean areSchedulerServicesRunning() {
    return schedulerServices != null && schedulerServices.isInState(STATE.STARTED);
  }

  public synchronized void transitionToActive() throws Exception {
    resourceTrackingServiceStartStopLock.lock();
    LOG.info("locked resourceTrackingServiceStart");
    try{
    if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state <" + getBindAddress(this.conf).getPort() + ">");
      return;
    }

    LOG.info("Transitioning to active state " + HAUtil.getRMHAId(conf));

    stopSchedulerServices();
    if(resourceTrackingService.isInState(STATE.STARTED)){
      resourceTrackingService.stop();
    }
    reinitialize(false);
    resetDispatcher();
    createAndInitSchedulerServices();
      
    this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          startSchedulerServices();
          return null;
        } catch (Exception e) {
          reinitialize(true);
          throw e;
        }
      }
    });
    
//    if (rmContext.isDistributed()) {
//      eventProcessor.start();
//    }

    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state " + HAUtil.getRMHAId(conf));
    }finally{
      LOG.info("unlocked resourceTrackingServiceStart");
      resourceTrackingServiceStartStopLock.unlock();
    }
  }

  public synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    resourceTrackingServiceStartStopLock.lock();
    LOG.info("locked resourceTrackingServiceStart");
    try{
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state <" + getBindAddress(this.conf).getPort() + ">");
      return;
    }

    LOG.info("Transitioning to standby state " + HAUtil.getRMHAId(conf));
    HAServiceState state = rmContext.getHAServiceState();
      LOG.info("Transitioning to standby state <" + + getBindAddress(this.conf).getPort() + "> " + state.toString());
    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
      stopSchedulerServices();
      resourceTrackingService.stop();
      if (((this.rmContext.isHAEnabled()
              && HAUtil.isAutomaticFailoverEnabled(conf) && HAUtil.isHopsRMFailoverProxy(conf))
              || this.rmContext.isDistributed()) && rmContext.isLeader()) {
        if (groupMembershipService != null) {
          groupMembershipService.relinquishId();
        }
      }
      reinitialize(initialize);
    }
    LOG.info("Transitioned to standby state " + HAUtil.getRMHAId(conf));
    }finally{
      LOG.info("unlocked resourceTrackingServiceStart");
      resourceTrackingServiceStartStopLock.unlock();
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    resourceTrackingServiceStartStopLock.lock();
    LOG.info("locked resourceTrackingServiceStart");
    try{
    startCRLFetcherService();
    if (this.rmContext.isHAEnabled() || this.rmContext.isDistributed()) {
      transitionToStandby(true);
      if ((HAUtil.isAutomaticFailoverEnabled(conf) && HAUtil.isHopsRMFailoverProxy(conf))
              || this.rmContext.isDistributed()) {
        LOG.info("start gms");
        groupMembershipService.start();
      }
    } else {
      transitionToActive();
    }
    LOG.info("start webapp");
    startWepApp();
    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
        false)) {
      int port = webApp.port();
      WebAppUtils.setRMWebAppPort(conf, port);
    }
    if (rmContext.isDistributed()) {
      resourceTrackingService.start();
    }
    LOG.info("start super");
    super.serviceStart();
    }finally{
      LOG.info("unlocked resourceTrackingServiceStart");
      resourceTrackingServiceStartStopLock.unlock();
    }
  }
  
  protected void doSecureLogin() throws IOException {
	InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

    // if security is enable, set rmLoginUGI as UGI of loginUser
    if (UserGroupInformation.isSecurityEnabled()) {
      this.rmLoginUGI = UserGroupInformation.getLoginUser();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    resourceTrackingServiceStartStopLock.lock();
    LOG.info("locked resourceTrackingServiceStart");
    try{
    if (webApp != null) {
      webApp.stop();
    }
    if (fetcher != null) {
      fetcher.stop();
    }
    if (configurationProvider != null) {
      configurationProvider.close();
    }
    if (resourceTrackingService != null) {
      resourceTrackingService.stop();
    }
    super.serviceStop();
    if (curator != null) {
      curator.close();
    }
    transitionToStandby(false);
    stopCRLFetcherService();
    rmContext.setHAServiceState(HAServiceState.STOPPING);
    }finally{
      LOG.info("unlocked resourceTrackingServiceStart");
      resourceTrackingServiceStartStopLock.unlock();
    }
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor,
        this.rmContext.getContainerTokenSecretManager(),
        this.rmContext.getNMTokenSecretManager());
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.queueACLsManager,
        this.rmContext.getRMDelegationTokenSecretManager());
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(this.rmContext, scheduler);
  }

  protected AdminService createAdminService() {
    return new AdminService(this, rmContext);
  }

  protected RMSecretManagerService createRMSecretManagerService() {
    return new RMSecretManagerService(conf, rmContext);
  }

  /**
   * Create RMDelegatedNodeLabelsUpdater based on configuration.
   */
  protected RMDelegatedNodeLabelsUpdater createRMDelegatedNodeLabelsUpdater() {
    if (conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
            YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED)
        && YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(
            conf)) {
      return new RMDelegatedNodeLabelsUpdater(rmContext);
    } else {
      return null;
    }
  }

  protected GroupMembershipService createGroupMembershipService() {
    return new GroupMembershipService(this, rmContext);
  }
  
  private void createAndInitCRLFetcherService() {
    if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      if (conf.getBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY,
          CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_DEFAULT)) {
        LOG.info("Creating CertificateRevocationList Fetcher service");
        revocationListFetcherService = new RevocationListFetcherService();
        revocationListFetcherService.init(conf);
      } else {
        LOG.warn("RPC TLS is enabled but CRL validation is disabled");
      }
    }
  }
  
  private void startCRLFetcherService() {
    if (revocationListFetcherService != null) {
      revocationListFetcherService.start();
    }
  }
  
  private void stopCRLFetcherService() {
    if (revocationListFetcherService != null) {
      revocationListFetcherService.stop();
    }
  }
  
  private void createCertificateLocalizationService() {
    if (conf.getBoolean(CommonConfigurationKeysPublic
        .IPC_SERVER_SSL_ENABLED, CommonConfigurationKeysPublic
        .IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      boolean isHAEnabled = rmContext.isHAEnabled();
      
      certificateLocalizationService = new CertificateLocalizationService(CertificateLocalizationService.ServiceType.RM);
      CertificateLocalizationCtx.getInstance().setCertificateLocalization
          (certificateLocalizationService);
      addService(certificateLocalizationService);
      rmContext.setCertificateLocalizationService(certificateLocalizationService);
    }
  }
  
  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }

  /**
   * return the scheduler.
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return the resource tracking component.
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Private
  public ApplicationACLsManager getApplicationACLsManager() {
    return this.applicationACLsManager;
  }

  @Private
  public QueueACLsManager getQueueACLsManager() {
    return this.queueACLsManager;
  }

  @Private
  WebApp getWebapp() {
    return this.webApp;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover RMdelegationTokenSecretManager
    rmContext.getRMDelegationTokenSecretManager().recover(state);

    // recover AMRMTokenSecretManager
    rmContext.getAMRMTokenSecretManager().recover(state);

    // recover reservations
    if (reservationSystem != null) {
      reservationSystem.recover(state);
    }
    // recover applications
    rmAppManager.recover(state);

    recoverSalt();
    
    setSchedulerRecoveryStartAndWaitTime(state, conf);
  }

  private void recoverSalt() throws IOException{
    byte[] seed = DBUtility.verifySalt(rmContext.getSeed());
    rmContext.setSeed(seed);
  }
  
  public static void main(String argv[]) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      // If -format-state-store, then delete RMStateStore; else startup normally
      if (argv.length >= 1) {
        if (argv[0].equals("-format-state-store")) {
          deleteRMStateStore(conf);
        } else if (argv[0].equals("-remove-application-from-state-store")
            && argv.length == 2) {
          removeApplication(conf, argv[1]);
        } else {
          printUsage(System.err);
        }
      } else {
        ResourceManager resourceManager = new ResourceManager();
        ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(resourceManager),
          SHUTDOWN_HOOK_PRIORITY);
        resourceManager.init(conf);
        resourceManager.start();
      }
    } catch (Throwable t) {
      LOG.fatal("Error starting ResourceManager", t);
      System.exit(-1);
    }
  }

  /**
   * Register the handlers for alwaysOn services
   */
  private Dispatcher setupDispatcher() {
    Dispatcher dispatcher = createDispatcher();
    dispatcher.register(RMFatalEventType.class,
        new ResourceManager.RMFatalEventDispatcher());
    return dispatcher;
  }

  private void resetDispatcher() {
    removeService((Service)rmDispatcher);
    ((Service) rmDispatcher).stop();
    Dispatcher dispatcher = setupDispatcher();
    ((Service)dispatcher).init(this.conf);
    ((Service)dispatcher).start();
    // Need to stop previous rmDispatcher before assigning new dispatcher
    // otherwise causes "AsyncDispatcher event handler" thread leak
    rmDispatcher = dispatcher;
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);
  }

  private void setSchedulerRecoveryStartAndWaitTime(RMState state,
      Configuration conf) {
    if (!state.getApplicationState().isEmpty()) {
      long waitTime =
          conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
      rmContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
    }
  }

  /**
   * Retrieve RM bind address from configuration
   * 
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }

  /**
   * Deletes the RMStateStore
   *
   * @param conf
   * @throws Exception
   */
  private static void deleteRMStateStore(Configuration conf) throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.init(conf);
    rmStore.start();
    try {
      LOG.info("Deleting ResourceManager state store...");
      rmStore.deleteStore();
      LOG.info("State store deleted");
    } finally {
      rmStore.stop();
    }
  }

  private static void removeApplication(Configuration conf, String applicationId)
      throws Exception {
    RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
    rmStore.init(conf);
    rmStore.start();
    try {
      ApplicationId removeAppId = ApplicationId.fromString(applicationId);
      LOG.info("Deleting application " + removeAppId + " from state store");
      rmStore.removeApplication(removeAppId);
      LOG.info("Application is deleted from state store");
    } finally {
      rmStore.stop();
    }
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: yarn resourcemanager [-format-state-store]");
    out.println("                            "
        + "[-remove-application-from-state-store <appId>]" + "\n");
  }
}
