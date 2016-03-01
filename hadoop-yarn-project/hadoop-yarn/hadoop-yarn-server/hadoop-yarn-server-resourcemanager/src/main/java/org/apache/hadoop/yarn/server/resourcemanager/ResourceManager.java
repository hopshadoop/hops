/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import io.hops.common.GlobalThreadPool;
import io.hops.ha.common.TransactionStateManager;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEventTransaction;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAuthenticationHandler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilter;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota.QuotaSchedulerService;

/**
 * The ResourceManager is the main class that is a set of components. "I am the
 * ResourceManager. All your resources belong to us..."
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService implements Recoverable {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  private static long clusterTimeStamp = System.currentTimeMillis();

  /**
   * "Always On" services. Services that need to run always irrespective of the
   * HA state of the RM.
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;//recovered
  private Dispatcher rmDispatcher;
  private TransactionStateManager transactionStateManager;
  @VisibleForTesting
  protected AdminService adminService;
  protected GroupMembershipService groupMembershipService;

  /**
   * "Active" services. Services that need to run only on the Active RM. These
   * services are managed (initialized, started, stopped) by the
   * {@link CompositeService} RMActiveServices.
   * <p/>
   * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
   * in
   * Active state.
   */
  protected RMSchedulerServices schedulerServices;
  protected RMSecretManagerService rmSecretManagerService;

  protected ResourceScheduler scheduler;//recovered
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  protected RMAppManager rmAppManager;//recovered
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  protected ContainersLogsService containersLogsService;
  protected QuotaSchedulerService quotaSchedulerService;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;

  private String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  PendingEventRetrieval eventRetriever;
  private ContainerAllocationExpirer containerAllocationExpirer;
  private boolean recoveryEnabled;
  private DelegationTokenRenewer delegationTokenRenewer;
  private CompositeService resourceTrackingService;
  private List<SchedulingMonitor> schedulingPolicyMonitors;
  /**
   * End of Active services
   */

  private Configuration conf;
  
  private UserGroupInformation rmLoginUGI;

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

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    this.rmContext = new RMContextImpl();

    this.rmContext.setDistributedEnabled(conf.getBoolean(
            YarnConfiguration.DISTRIBUTED_RM,
            YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
    
    transactionStateManager = new TransactionStateManager();
    addIfService(transactionStateManager);
    rmContext.setTransactionStateManager(transactionStateManager);
    
    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    // load core-site.xml
    InputStream coreSiteXMLInputStream = this.configurationProvider
        .getConfigurationInputStream(this.conf,
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    if (coreSiteXMLInputStream != null) {
      this.conf.addResource(coreSiteXMLInputStream);
    }

    // Do refreshUserToGroupsMappings with loaded core-site.xml
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(this.conf)
        .refresh();

    // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

    // load yarn-site.xml
    InputStream yarnSiteXMLInputStream = this.configurationProvider
        .getConfigurationInputStream(this.conf,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    if (yarnSiteXMLInputStream != null) {
      this.conf.addResource(yarnSiteXMLInputStream);
    }

    validateConfigs(this.conf);

    // register the handlers for all AlwaysOn services using setupDispatcher().
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);
    groupMembershipService = createGroupMembershipService();
    if (HAUtil.isAutomaticFailoverEnabled(conf)) {
      addService(groupMembershipService);
    }
    rmContext.setRMGroupMembershipService(groupMembershipService);
    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
    }
    if (HAUtil.isHAEnabled(conf)) {
      webAppAddress = WebAppUtils.getRMHAWebAppURLWithoutScheme(this.conf);
    } else {
      webAppAddress = WebAppUtils.getRMWebAppURLWithoutScheme(this.conf);
    }
    
    this.rmLoginUGI = UserGroupInformation.getCurrentUser();

    createAndInitSchedulerServices();
    
    super.serviceInit(this.conf);
  }

  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return new QueueACLsManager(scheduler, conf);
  }

  boolean rmStoreBlocked=false;
  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    rmStoreBlocked=true;
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
        return (ResourceScheduler) ReflectionUtils
            .newInstance(schedulerClazz, this.conf);
      } else {
        throw new YarnRuntimeException(
            "Class: " + schedulerClassName + " not instance of " +
                ResourceScheduler.class.
                    getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate Scheduler: " + schedulerClassName, e);
    }
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext.getDispatcher(), rmContext);
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher, rmContext);
  }

  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
        this.applicationACLsManager, this.conf);
  }

  protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
    return new RMApplicationHistoryWriter();
  }

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException(
          "Invalid global max attempts configuration" + ", " +
              YarnConfiguration.RM_AM_MAX_ATTEMPTS + "=" +
              globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException(
          "Nodemanager expiry interval should be no" +
              " less than heartbeat interval, " +
              YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl +
              ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "=" +
              heartbeatIntvl);
    }
  }

  
  @Private
  class ResourceTrackingServices extends CompositeService {

    public ResourceTrackingServices() {
      super("ResourceTrackingServices");
      LOG.info("create resourceTrackingService");
    }
    
    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      LOG.info("init resourceTrackingService");
      conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

      if (!rmStoreBlocked) {
        RMStateStore rmStore = null;
        if (recoveryEnabled) {
          rmStore = RMStateStoreFactory.getStore(conf);
        } else {
          rmStore = new NullRMStateStore();
        }

        try {
          rmStore.init(conf);
          rmStore.setRMDispatcher(rmDispatcher);
        } catch (Exception e) {
      // the Exception from stateStore.init() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to init state store", e);
          throw e;
        }
        rmContext.setStateStore(rmStore);
      }
      
      rmSecretManagerService = createRMSecretManagerService();
      this.addService(rmSecretManagerService);

      containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher,
              rmContext);
      this.addService(containerAllocationExpirer);
      rmContext.setContainerAllocationExpirer(containerAllocationExpirer);

      AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
      this.addService(amLivelinessMonitor);
      rmContext.setAMLivelinessMonitor(amLivelinessMonitor);

      AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
      this.addService(amFinishingMonitor);
      rmContext.setAMFinishingMonitor(amFinishingMonitor);

      boolean isRecoveryEnabled = conf.getBoolean(
              YarnConfiguration.RECOVERY_ENABLED,
              YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

      if (isRecoveryEnabled) {
        LOG.info("recovery enabled");
        recoveryEnabled = true;
      } else {
        recoveryEnabled = false;
      }

      if (UserGroupInformation.isSecurityEnabled()) {
        delegationTokenRenewer = createDelegationTokenRenewer();
        rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
      }

      RMApplicationHistoryWriter rmApplicationHistoryWriter
              = createRMApplicationHistoryWriter();
      this.addService(rmApplicationHistoryWriter);
      rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

      // Register event handler for NodesListManager
      nodesListManager = new NodesListManager(rmContext);
      rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
      this.addService(nodesListManager);
      rmContext.setNodesListManager(nodesListManager);

      // Register event handler for RmNodes
      rmDispatcher
              .register(RMNodeEventType.class,
                      new NodeEventDispatcher(rmContext));

      nmLivelinessMonitor = createNMLivelinessMonitor();
      this.addService(nmLivelinessMonitor);

      resourceTracker = createResourceTrackerService();
      this.addService(resourceTracker);
      rmContext.setResourceTrackerService(resourceTracker);

      DefaultMetricsSystem.initialize("ResourceManager");
      JvmMetrics.initSingleton("ResourceManager", null);
      
      super.serviceInit(conf);
    }
    
    @Override
    protected void serviceStart() throws Exception {
      LOG.info("starting resourceTrackingService");
      super.serviceStart();
    }
  }
  
  
  /**
   * RMSchedulerServices handles all the services run by the Scheduler node.
   */
  @Private
  public class RMSchedulerServices extends CompositeService {

    private EventHandler<SchedulerEvent> schedulerDispatcher;
    private ApplicationMasterLauncher applicationMasterLauncher;


    RMSchedulerServices() {
      super("RMActiveServices");
    }

    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      
      createAndInitResourceTrackingServices();
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



      // creating monitors that handle preemption
      schedulingPolicyMonitors = createPolicyMonitors();

      masterService = createApplicationMasterService();
      addService(masterService);
      rmContext.setApplicationMasterService(masterService);

      applicationACLsManager = new ApplicationACLsManager(conf);

      queueACLsManager = createQueueACLsManager(scheduler, conf);

      rmAppManager = createRMAppManager();
      // Register event handler for RMAppManagerEvents
      rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

      clientRM = createClientRMService();
      rmContext.setClientRMService(clientRM);
      addService(clientRM);
      rmContext.setClientRMService(clientRM);

      applicationMasterLauncher = createAMLauncher();
      rmDispatcher
          .register(AMLauncherEventType.class, applicationMasterLauncher);

      addService(applicationMasterLauncher);
      if (UserGroupInformation.isSecurityEnabled()) {
        addService(delegationTokenRenewer);
        delegationTokenRenewer.setRMContext(rmContext);
      }

      new RMNMInfo(rmContext, scheduler);

      super.serviceInit(conf);
    }

    @VisibleForTesting
    public List<SchedulingMonitor> getSchedulingMonitors() {
      return schedulingPolicyMonitors;
    }

    private void startDispatchers(){
      if (schedulerDispatcher instanceof Service) {
        ((Service)schedulerDispatcher).start();
      }
      if(rmDispatcher instanceof Service){
        ((Service)rmDispatcher).start();
      }
    }
    
    @Override
    protected void serviceStart() throws Exception {
      LOG.info("start schedulerServices");
      RMStateStore rmStore = rmContext.getStateStore();
      // The state store needs to start irrespective of recoveryEnabled as apps
      // need events to move to further states.
      rmStore.start();

      if (recoveryEnabled) {
        try {
          rmStore.checkVersion();
          RMState state = rmStore.loadState(rmContext);
          //the dispatchers should be started to recover the RPCs.
          startDispatchers();
          recover(state);
        } catch (Exception e) {
          // the Exception from loadState() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to load/recover state", e);
          throw e;
        }
      }
      resourceTrackingService.start();
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      LOG.info("stop schedulerServices");
      DefaultMetricsSystem.shutdown();

      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();
        try {
          if(!rmStoreBlocked){
            store.close();
          }
        } catch (Exception e) {
          LOG.error("Error closing store.", e);
        }
      }

      super.serviceStop();
    }

    protected List<SchedulingMonitor> createPolicyMonitors() {
      if (scheduler instanceof PreemptableResourceScheduler &&
          conf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS)) {
        LOG.info("Loading policy monitors");
        List<SchedulingEditPolicy> policies =
            conf.getInstances(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
                SchedulingEditPolicy.class);
        if (policies.size() > 0) {
          rmDispatcher.register(ContainerPreemptEventType.class,
              new RMContainerPreemptEventDispatcher(
                  (PreemptableResourceScheduler) scheduler));
          List<SchedulingMonitor> schedulingMonitors = new ArrayList<SchedulingMonitor>();

          for (SchedulingEditPolicy policy : policies) {
            LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
            // periodically check whether we need to take action to guarantee
            // constraints
            SchedulingMonitor mon = new SchedulingMonitor(rmContext, policy);
            schedulingMonitors.add(mon);
            addService(mon);
          }
          return schedulingMonitors;
        } else {
          LOG.warn("Policy monitors configured (" +
              YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS +
              ") but none specified (" +
              YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + ")");
        }
      }
      return null;
    }
  }

  @Private
  public static class SchedulerEventDispatcher extends AbstractService
      implements EventHandler<SchedulerEvent> {

    private final ResourceScheduler scheduler;
    private final BlockingQueue<SchedulerEvent> eventQueue =
        new LinkedBlockingQueue<SchedulerEvent>();
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
            LOG.error("Returning, interrupted : ", e);
            return; // TODO: Kill RM.
          }

          try {
            scheduler.handle(event);
            if (event instanceof AbstractEventTransaction) {
              AbstractEventTransaction eventT =
                  (AbstractEventTransaction) event;
              if (eventT.getTransactionState() != null) {
                eventT.getTransactionState().decCounter(event.getType());
              }
            }
          } catch (Throwable t) {
            // An error occurred, but we are shutting down anyway.
            // If it was an InterruptedException, the very act of 
            // shutdown could have caused it and is probably harmless.
            if (stopped) {
              LOG.warn("Exception during shutdown: ", t);
              break;
            }
            LOG.fatal("Error in handling event type " + event.getType() +
                " to the scheduler", t);
            if (shouldExitOnError &&
                !ShutdownHookManager.get().isShutdownInProgress()) {
              LOG.info("Exiting, bbye..");
              ExitUtil.terminate(-1);
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
        if (qSize != 0 && qSize % 1000 == 0) {
          LOG.info("Size of scheduler event-queue is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          LOG.info("Very low remaining capacity on scheduler event queue: " +
              remCapacity);
        }
        if (event instanceof AbstractEventTransaction) {
          AbstractEventTransaction eventT = (AbstractEventTransaction) event;
          if (eventT.getTransactionState() != null) {
            LOG.debug("HOP :: counter incr:" + event.getType());
            eventT.getTransactionState().incCounter(event.getType());
          }
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

    private final RMContext rmContext;
    private final ResourceManager rm;

    public RMFatalEventDispatcher(RMContext rmContext,
        ResourceManager resourceManager) {
      this.rmContext = rmContext;
      this.rm = resourceManager;
    }

    @Override
    public void handle(RMFatalEvent event) {
      LOG.fatal("Received a " + RMFatalEvent.class.getName() + " of type " +
          event.getType().name() + ". Cause:\n" + event.getCause());

      if (event.getType() == RMFatalEventType.STATE_STORE_FENCED) {
        LOG.info("RMStateStore has been fenced");
        if (rmContext.isHAEnabled()) {
          try {
            // Transition to standby and reinit active services
            LOG.info("Transitioning RM to Standby mode");
            rm.transitionToStandby(true);
            return;
          } catch (Exception e) {
            LOG.fatal("Failed to transition RM to Standby mode.");
          }
        }
      }

      ExitUtil.terminate(1, event.getCause());
    }
  }

  @Private
  public static final class ApplicationEventDispatcher
      implements EventHandler<RMAppEvent> {

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
          LOG.error("Error in handling event type " + event.getType() +
              " for application " + appID, t);
        }
      }
    }
  }

  @Private
  public static final class RMContainerPreemptEventDispatcher
      implements EventHandler<ContainerPreemptEvent> {

    private final PreemptableResourceScheduler scheduler;

    public RMContainerPreemptEventDispatcher(
        PreemptableResourceScheduler scheduler) {
      this.scheduler = scheduler;
    }

    @Override
    public void handle(ContainerPreemptEvent event) {
      ApplicationAttemptId aid = event.getAppId();
      RMContainer container = event.getContainer();
      switch (event.getType()) {
        case DROP_RESERVATION:
          scheduler.dropContainerReservation(container, event.
              getTransactionState());
          break;
        case PREEMPT_CONTAINER:
          scheduler.preemptContainer(aid, container);
          break;
        case KILL_CONTAINER:
          scheduler.killContainer(container, event.getTransactionState());
          break;
      }
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher
      implements EventHandler<RMAppAttemptEvent> {

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
            LOG.error("Error in handling event type " + event.getType() +
                " for applicationAttempt " + appAttemptId, t);
          }
        }
      }
    }
  }

  @Private
  public static final class NodeEventDispatcher
      implements EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      RMNode node = this.rmContext.getActiveRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error(
              "Error in handling event type " + event.getType() + " for node " +
                  nodeId, t);
        }
      }
    }
  }

  protected void startWepApp() {

    // Use the customized yarn filter instead of the standard kerberos filter to
    // allow users to authenticate using delegation tokens
    // 3 conditions need to be satisfied -
    // 1. security is enabled
    // 2. http auth type is set to kerberos
    // 3. "yarn.resourcemanager.webapp.use-yarn-filter" override is set to true
    Configuration conf = getConfig();
    boolean useYarnAuthenticationFilter =
        conf.getBoolean(
          YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
          YarnConfiguration.DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER);
    String authPrefix = "hadoop.http.authentication.";
    String authTypeKey = authPrefix + "type";
    String initializers = conf.get("hadoop.http.filter.initializers");
    if (UserGroupInformation.isSecurityEnabled()
        && useYarnAuthenticationFilter
        && conf.get(authTypeKey, "").equalsIgnoreCase(
          KerberosAuthenticationHandler.TYPE)) {
      LOG.info("Using RM authentication filter(kerberos/delegation-token)"
          + " for RM webapp authentication");
      RMAuthenticationHandler
        .setSecretManager(getClientRMService().rmDTSecretManager);
      String yarnAuthKey =
          authPrefix + RMAuthenticationFilter.AUTH_HANDLER_PROPERTY;
      conf.setStrings(yarnAuthKey, RMAuthenticationHandler.class.getName());

      initializers =
         initializers == null || initializers.isEmpty() ? "" : ","
              + initializers;
      if (!initializers.contains(RMAuthenticationFilterInitializer.class
        .getName())) {
        conf.set("hadoop.http.filter.initializers",
          RMAuthenticationFilterInitializer.class.getName() + initializers);
      }
    }
    // if security is not enabled and the default filter initializer has been
    // set, set the initializer to include the
    // RMAuthenticationFilterInitializer which in turn will set up the simple
    // auth filter.

    if (!UserGroupInformation.isSecurityEnabled()) {
      if (initializers == null || initializers.isEmpty()) {
        conf.set("hadoop.http.filter.initializers",
          RMAuthenticationFilterInitializer.class.getName());
        conf.set(authTypeKey, "simple");
      } else if (initializers.equals(StaticUserWebFilter.class.getName())) {
        conf.set("hadoop.http.filter.initializers",
          RMAuthenticationFilterInitializer.class.getName() + ","
              + initializers);
        conf.set(authTypeKey, "simple");
      }
    }

    Builder<ApplicationMasterService> builder = WebApps
        .$for("cluster", ApplicationMasterService.class, masterService, "ws")
        .with(conf).withHttpSpnegoPrincipalKey(
            YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
        .withHttpSpnegoKeytabKey(
            YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
        .at(webAppAddress);
    String proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
    if (WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
        equals(proxyHostAndPort)) {
      fetcher = new AppReportFetcher(conf, getClientRMService());
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);

    }
    webApp = builder.start(new RMWebApp(this));
  }

  /**
   * Helper method to create and init {@link #activeServices}. This creates an
   * instance of {@link RMSchedulerServices} and initializes it.
   *
   * @throws Exception
   */
  void createAndInitSchedulerServices() throws Exception {
    schedulerServices = new RMSchedulerServices();
    schedulerServices.init(conf);
  }

  void createAndInitResourceTrackingServices() {
    resourceTrackingService = new ResourceTrackingServices();
    resourceTrackingService.init(conf);
  }
  
  /**
   * Helper method to start {@link #activeServices}.
   *
   * @throws Exception
   */
  void startSchedulerServices() throws Exception {
    if (schedulerServices != null) {
      clusterTimeStamp = System.currentTimeMillis();
      schedulerServices.start(); 
      
    }
  }
  
  /**
   * Helper method to stop {@link #activeServices}.
   *
   * @throws Exception
   */
  void stopSchedulerServices() throws Exception {
    if (schedulerServices != null) {
      schedulerServices.stop();
      schedulerServices = null;
      rmContext.getActiveRMNodes().clear();//we should not update the db here 
      rmContext.getInactiveRMNodes().clear();//we should not update the db here 
      rmContext.getRMApps().clear();//we should not update the db here 
      ClusterMetrics.destroy();
      QueueMetrics.clearQueueMetrics();
      //TODO should probably not be there anymore
      if (eventRetriever != null) {
        LOG.info("NDB Event streaming is stoping now ..");
        eventRetriever.finish();
      }
    }
  }

  @VisibleForTesting
  protected boolean areActiveServicesRunning() {
    return schedulerServices != null && schedulerServices.isInState(STATE.STARTED);
  }

  synchronized void transitionToActive() throws Exception {
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }

    LOG.info("Transitioning to active state " + groupMembershipService.
        getHostname());
    
    stopSchedulerServices();
    resourceTrackingService.stop();
    resetDispatcher();
    resetTransactionStateManager();  
    createAndInitSchedulerServices();  
    if (rmContext.isDistributedEnabled()) {
        LOG.info("streaming porcessor is straring for scheduler");
        RMStorageFactory.kickTheNdbEventStreamingAPI(true, conf);
        eventRetriever = new NdbEventStreamingProcessor(rmContext, conf);
    }
    // use rmLoginUGI to startActiveServices.
    // in non-secure model, rmLoginUGI will be current UGI
    // in secure model, rmLoginUGI will be LoginUser UGI
    this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        startSchedulerServices();
        return null;
      }
    });

    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state" + groupMembershipService.
        getHostname());
    //Start PendingEvent retrieval thread
    //Start periodic retrieval of pending scheduler events
    if (rmContext.isDistributedEnabled()) {
      eventRetriever.start();
    }
  }

  @VisibleForTesting
  protected synchronized void transitionToStandby(boolean initialize) throws Exception {
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state " + groupMembershipService.
          getHostname());
      return;
    }

    LOG.debug("Transitioning to standby " + groupMembershipService.
            getHostname());
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.ACTIVE) {
      stopSchedulerServices();
      resourceTrackingService.stop();
      if (groupMembershipService.isLeader()) {
        groupMembershipService.relinquishId();
      }
      if (initialize) {
        resetDispatcher();
        resetTransactionStateManager();
        createAndInitSchedulerServices();
        if(rmContext.isDistributedEnabled()){
          resourceTrackingService.start();
        }
      }
    }
    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    LOG.info("Transitioned to standby state" + groupMembershipService.
        getHostname());
  }

  synchronized void transitionToLeadingRT(){
    //create and start containersLogService
    createAndStartQuotaServices();
  }
  
  synchronized void transitionToNonLeadingRT(){
    //stop containersLogService
    if (containersLogsService != null) {
      containersLogsService.stop();
    }
    if (quotaSchedulerService != null) {
      quotaSchedulerService.stop();
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    if (this.rmContext.isHAEnabled()) {
      LOG.info("HA enabled");
      transitionToStandby(true);
      if (HAUtil.isAutomaticFailoverEnabled(conf)) {
        LOG.info("automatic failover enabled");
        groupMembershipService.start();
      }
    } else {
      LOG.info("HA not enabled");
      transitionToActive();
      createAndStartQuotaServices();
    }

    startWepApp();
    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      int port = webApp.port();
      WebAppUtils.setRMWebAppPort(conf, port);
    }
    if (rmContext.isDistributedEnabled()) {
      resourceTrackingService.start();
    }
    super.serviceStart();
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
    if (containersLogsService !=null){
      containersLogsService.stop();
    }
    if(quotaSchedulerService!=null){
      quotaSchedulerService.stop();
    }
    RMStorageFactory.stopTheNdbEventStreamingAPI();
    super.serviceStop();
    LOG.info("transition to standby serviceStop");
    transitionToStandby(false);
    rmContext.setHAServiceState(HAServiceState.STOPPING);
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

  protected GroupMembershipService createGroupMembershipService() {
    return new GroupMembershipService(this, rmContext);
  }

  protected RMSecretManagerService createRMSecretManagerService() {
    return new RMSecretManagerService(conf, rmContext);
  }
  
  protected void createAndStartQuotaServices() {
    containersLogsService = new ContainersLogsService();
    quotaSchedulerService = new QuotaSchedulerService();
    containersLogsService.init(conf);
    quotaSchedulerService.init(conf);
    rmContext.setContainersLogsService(containersLogsService);
    containersLogsService.start();
    quotaSchedulerService.start();
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }

  /**
   * return the scheduler.
   *
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   *
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
    LOG.info("Recovering");
    // recover RMdelegationTokenSecretManager
    rmContext.getRMDelegationTokenSecretManager().recover(state);
    rmContext.recover(state);

    // recover applications
    rmAppManager.recover(state);

    // recover scheduler
    scheduler.recover(state);

    //recover not finished rpc
    try {
      LOG.info("recover pending events");
      recoverPendingEvents(state);
      LOG.info("recover RPCs");
      recoverRpc(state);
    } catch (YarnException ex) {
      //TODO see what to do with this exceptions
    } catch (IOException ex) {
      //TODO see what to do with this exceptions
    }

    // Recover preempted containers for CapacityPreemptionPolicy
    if (schedulingPolicyMonitors != null) {
      for (SchedulingMonitor monitor : schedulingPolicyMonitors) {
        LOG.info("Trying to recover policy monitor");
        monitor.recover(state);
      }
    }

    LOG.info("Finished recovering");
  }

  private UserGroupInformation creatAMRMTokenUGI(RPC rpc) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(rpc.
        getUserId());

    AMRMTokenIdentifier token = new AMRMTokenIdentifier(ConverterUtils.
        toApplicationAttemptId(rpc.getUserId()));
    ugi.addTokenIdentifier(token);

    return ugi;
  }

  protected void recoverPendingEvents(RMState rmState) throws IOException {
    List<PendingEvent> pendingEvents = rmState.getPendingEvents();
    if (!pendingEvents.isEmpty()) {
      Collections.sort(pendingEvents);
    }
    for (final PendingEvent pendingEvent : pendingEvents) {
      //the rmnode should already have been recovered
      NodeId nodeId = ConverterUtils.toNodeId(pendingEvent.getId().getNodeId());

      RMNode rmNode = rmContext.getActiveRMNodes().get(nodeId);
      LOG.debug("recover pending event for node " + nodeId
              + " of type " + pendingEvent.getType() + "rmNode " + rmNode);
      eventRetriever.triggerEvent(rmNode, pendingEvent, true);
    }
  }
  
  protected void recoverRpc(RMState rmState) throws IOException, YarnException {
    List<RPC> rpcList = rmState.getAppMasterRPCs();
    if(!rpcList.isEmpty()){
      Collections.sort(rpcList);
    }
    Exception lastException = null;
    LOG.debug("recovering rpcs: " + rpcList.size());
    for (final RPC rpc : rpcList) {
      com.google.protobuf.GeneratedMessage proto;
      LOG.debug(
          "recovering rpc: " + rpc.getRPCId() + " of type: " + rpc.getType());
      UserGroupInformation ugi;
      try {
        switch (rpc.getType()) {
          case RegisterApplicationMaster:
            ugi = creatAMRMTokenUGI(rpc);
            ugi.doAs(
                new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {

                  @Override
                  public RegisterApplicationMasterResponse run()
                      throws Exception {
                    com.google.protobuf.GeneratedMessage proto =
                        YarnServiceProtos.RegisterApplicationMasterRequestProto.
                            parseFrom(rpc.getRpc());
                    return masterService.registerApplicationMaster(
                        new RegisterApplicationMasterRequestPBImpl(
                            (YarnServiceProtos.RegisterApplicationMasterRequestProto) proto),
                        rpc.getRPCId());
                  }
                });
            break;
          case FinishApplicationMaster:
            ugi = creatAMRMTokenUGI(rpc);
            ugi.doAs(
                new PrivilegedExceptionAction<FinishApplicationMasterResponse>() {

                  @Override
                  public FinishApplicationMasterResponse run()
                      throws Exception {
                    com.google.protobuf.GeneratedMessage proto =
                        YarnServiceProtos.FinishApplicationMasterRequestProto.
                            parseFrom(rpc.getRpc());
                    return masterService.finishApplicationMaster(
                        new FinishApplicationMasterRequestPBImpl(
                            (YarnServiceProtos.FinishApplicationMasterRequestProto) proto),
                        rpc.getRPCId());
                  }
                });
            break;
          case Allocate:
            ugi = creatAMRMTokenUGI(rpc);
            ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {

                  @Override
                  public AllocateResponse run() throws Exception {
                    com.google.protobuf.GeneratedMessage proto =
                        YarnServiceProtos.AllocateRequestProto.parseFrom(rpc.
                            getRpc());
                    return masterService.allocate(new AllocateRequestPBImpl(
                            (YarnServiceProtos.AllocateRequestProto) proto),
                        rpc.getRPCId());
                  }
                });
            break;
          case SubmitApplication:
            ugi = UserGroupInformation.createRemoteUser(rpc.getUserId());
            ugi.doAs(
                new PrivilegedExceptionAction<SubmitApplicationResponse>() {

                  @Override
                  public SubmitApplicationResponse run() throws Exception {
                    com.google.protobuf.GeneratedMessage proto =
                        YarnServiceProtos.SubmitApplicationRequestProto.
                            parseFrom(rpc.
                                    getRpc());
                    return clientRM.submitApplication(
                        new SubmitApplicationRequestPBImpl(
                            (YarnServiceProtos.SubmitApplicationRequestProto) proto),
                        rpc.getRPCId());
                  }
                });
            break;
          case ForceKillApplication:
            ugi = UserGroupInformation.createRemoteUser(rpc.getUserId());
            ugi.doAs(new PrivilegedExceptionAction<KillApplicationResponse>() {

                  @Override
                  public KillApplicationResponse run() throws Exception {
                    com.google.protobuf.GeneratedMessage proto =
                        YarnServiceProtos.KillApplicationRequestProto.
                            parseFrom(rpc.
                                    getRpc());
                    return clientRM.forceKillApplication(
                        new KillApplicationRequestPBImpl(
                            (YarnServiceProtos.KillApplicationRequestProto) proto),
                        rpc.getRPCId());
                  }
                });
            break;
          case RegisterNM:
            if (!rmContext.isDistributedEnabled() || rmContext.getGroupMembershipService().isAlone()) {
              proto
                      = YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto.
                      parseFrom(rpc.getRpc());
              resourceTracker.registerNodeManager(
                      new RegisterNodeManagerRequestPBImpl(
                              (YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto) proto),
                      rpc.getRPCId());
            }
            break;
          case NodeHeartbeat:
            if (!rmContext.isDistributedEnabled() || rmContext.getGroupMembershipService().isAlone()) {
              proto = YarnServerCommonServiceProtos.NodeHeartbeatRequestProto.
                      parseFrom(rpc.getRpc());
              resourceTracker.nodeHeartbeat(new NodeHeartbeatRequestPBImpl(
                      (YarnServerCommonServiceProtos.NodeHeartbeatRequestProto) proto),
                      rpc.getRPCId());
            }
            break;
          default:
            LOG.error("RPC type does not exist");
            throw new IOException("RPC type does not exist");
        }
      } catch (Exception ex) {
        LOG.error("rpc: " + rpc.getRPCId() +
            " was not recovered due to the following exception ", ex);
        lastException = ex;
      }
    }
    if (lastException != null) {
      if (lastException instanceof YarnException) {
        throw (YarnException) lastException;
      } else {
        throw (IOException) lastException;
      }
    }
  }

  public static void main(String argv[]) {
    Thread.
        setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      if (conf.getBoolean(YarnConfiguration.INITIALIZEDB,
          YarnConfiguration.DEFAULT_INITIALIZEDB)) {
      }
      ResourceManager resourceManager = new ResourceManager();
      ShutdownHookManager.get()
          .addShutdownHook(new CompositeServiceShutdownHook(resourceManager),
              SHUTDOWN_HOOK_PRIORITY);
      resourceManager.init(conf);
      resourceManager.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting ResourceManager", t);
      ExitUtil.terminate(-1);
    }
  }

  /**
   * Register the handlers for alwaysOn services
   */
  private Dispatcher setupDispatcher() {
    Dispatcher dispatcher = createDispatcher();
    dispatcher.register(RMFatalEventType.class,
        new ResourceManager.RMFatalEventDispatcher(this.rmContext, this));
    return dispatcher;
  }

  private void resetDispatcher() {
    Dispatcher dispatcher = setupDispatcher();
    ((Service) dispatcher).init(this.conf);
    ((Service) dispatcher).start();
    removeService((Service) rmDispatcher);
    rmDispatcher = dispatcher;
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);
  }

  private void resetTransactionStateManager(){
    LOG.info("reset transactionStateManager");
    transactionStateManager.stop();
    TransactionStateManager tsm = new TransactionStateManager();
    ((Service) tsm).init(conf);
    ((Service) tsm).start();
    removeService((Service) transactionStateManager);
    transactionStateManager = tsm;
    addIfService(transactionStateManager);
    rmContext.setTransactionStateManager(transactionStateManager);
  }
  
  /**
   * Retrieve RM bind address from configuration
   *
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
  }

}
