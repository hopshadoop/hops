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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppSecurityManager;
import org.apache.hadoop.yarn.server.security.CertificateLocalizationService;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;
import io.hops.util.GroupMembershipService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.quota.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.quota.QuotaService;

public class RMContextImpl implements RMContext {

  private Dispatcher rmDispatcher;

  private boolean isHAEnabled;

  private HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING;

  private AdminService adminService;

  private ConfigurationProvider configurationProvider;

  private RMActiveServiceContext activeServiceContext;

  private Configuration yarnConfiguration;

  private RMApplicationHistoryWriter rmApplicationHistoryWriter;
  private SystemMetricsPublisher systemMetricsPublisher;
  private EmbeddedElector elector;

  private final Object haServiceStateLock = new Object();

  private boolean isDistributed;
  private GroupMembershipService groupMembershipService;
  private CertificateLocalizationService certificateLocalizationService;
  
  private byte[] seed;
  private String userFolderHashAlgo = YarnConfiguration.DEFAULT_USER_FOLDER_ALGO;
  private RMAppSecurityManager rmAppSecurityManager;
  
  /**
   * Default constructor. To be used in conjunction with setter methods for
   * individual fields.
   */
  public RMContextImpl() {
  }

  @VisibleForTesting
  // helper constructor for tests
  public RMContextImpl(Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager appTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
      ResourceScheduler scheduler) {
    this();
    this.setDispatcher(rmDispatcher);
    setActiveServiceContext(new RMActiveServiceContext(rmDispatcher,
        containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
        delegationTokenRenewer, appTokenSecretManager,
        containerTokenSecretManager, nmTokenSecretManager,
        clientToAMTokenSecretManager,
        scheduler));

    ConfigurationProvider provider = new LocalConfigurationProvider();
    setConfigurationProvider(provider);
  }
  
  @VisibleForTesting
  // helper constructor for tests
  public RMContextImpl(Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager appTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this(
      rmDispatcher,
      containerAllocationExpirer,
      amLivelinessMonitor,
      amFinishingMonitor,
      delegationTokenRenewer,
      appTokenSecretManager,
      containerTokenSecretManager,
      nmTokenSecretManager,
      clientToAMTokenSecretManager, null);
  }

  @Override
  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }

  @Override
  public void setLeaderElectorService(EmbeddedElector elector) {
    this.elector = elector;
  }

  @Override
  public EmbeddedElector getLeaderElectorService() {
    return this.elector;
  }

  @Override
  public RMStateStore getStateStore() {
    return activeServiceContext.getStateStore();
  }

  @Override
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return activeServiceContext.getRMApps();
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return activeServiceContext.getRMNodes();
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
    return activeServiceContext.getInactiveRMNodes();
  }

  @Override
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return activeServiceContext.getContainerAllocationExpirer();
  }

  @Override
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return activeServiceContext.getAMLivelinessMonitor();
  }

  @Override
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return activeServiceContext.getAMFinishingMonitor();
  }

  @Override
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return activeServiceContext.getDelegationTokenRenewer();
  }

  @Override
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return activeServiceContext.getAMRMTokenSecretManager();
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return activeServiceContext.getContainerTokenSecretManager();
  }

  @Override
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return activeServiceContext.getNMTokenSecretManager();
  }

  @Override
  public ResourceScheduler getScheduler() {
    return activeServiceContext.getScheduler();
  }

  @Override
  public ContainersLogsService getContainersLogsService() {
    return activeServiceContext.getContainersLogsService();
  }
  
  @Override
  public QuotaService getQuotaService() {
    return activeServiceContext.getQuotaService();
  }
  
  @Override
  public ReservationSystem getReservationSystem() {
    return activeServiceContext.getReservationSystem();
  }

  @Override
  public NodesListManager getNodesListManager() {
    return activeServiceContext.getNodesListManager();
  }

  @Override
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return activeServiceContext.getClientToAMTokenSecretManager();
  }

  @Override
  public AdminService getRMAdminService() {
    return this.adminService;
  }

  @VisibleForTesting
  public void setStateStore(RMStateStore store) {
    activeServiceContext.setStateStore(store);
  }

  @Override
  public ClientRMService getClientRMService() {
    return activeServiceContext.getClientRMService();
  }

  @Override
  public ApplicationMasterService getApplicationMasterService() {
    return activeServiceContext.getApplicationMasterService();
  }

  @Override
  public ResourceTrackerService getResourceTrackerService() {
    return activeServiceContext.getResourceTrackerService();
  }

  void setHAEnabled(boolean isHAEnabled) {
    this.isHAEnabled = isHAEnabled;
  }

  void setHAServiceState(HAServiceState serviceState) {
    synchronized (haServiceStateLock) {
      this.haServiceState = serviceState;
    }
  }

  void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  void setRMAdminService(AdminService adminService) {
    this.adminService = adminService;
  }

  @Override
  public void setClientRMService(ClientRMService clientRMService) {
    activeServiceContext.setClientRMService(clientRMService);
  }

  @Override
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return activeServiceContext.getRMDelegationTokenSecretManager();
  }

  @Override
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    activeServiceContext
        .setRMDelegationTokenSecretManager(delegationTokenSecretManager);
  }

  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    activeServiceContext
        .setContainerAllocationExpirer(containerAllocationExpirer);
  }

  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    activeServiceContext.setAMLivelinessMonitor(amLivelinessMonitor);
  }

  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    activeServiceContext.setAMFinishingMonitor(amFinishingMonitor);
  }

  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    activeServiceContext
        .setContainerTokenSecretManager(containerTokenSecretManager);
  }

  void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
    activeServiceContext.setNMTokenSecretManager(nmTokenSecretManager);
  }

  @VisibleForTesting
  public void setScheduler(ResourceScheduler scheduler) {
    activeServiceContext.setScheduler(scheduler);
  }

  void setContainersLogsService(ContainersLogsService containersLogsService) {
    activeServiceContext.setContainersLogsService(containersLogsService);
  }
  
  void setQuotaService(QuotaService quotaService) {
    activeServiceContext.setQuotaService(quotaService);
  }
  
  void setReservationSystem(ReservationSystem reservationSystem) {
    activeServiceContext.setReservationSystem(reservationSystem);
  }

  void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
    activeServiceContext.setDelegationTokenRenewer(delegationTokenRenewer);
  }

  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    activeServiceContext
        .setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
  }

  void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
    activeServiceContext.setAMRMTokenSecretManager(amRMTokenSecretManager);
  }

  void setNodesListManager(NodesListManager nodesListManager) {
    activeServiceContext.setNodesListManager(nodesListManager);
  }

  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    activeServiceContext.setApplicationMasterService(applicationMasterService);
  }

  void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
    activeServiceContext.setResourceTrackerService(resourceTrackerService);
  }

  @Override
  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  @Override
  public HAServiceState getHAServiceState() {
    synchronized (haServiceStateLock) {
      return haServiceState;
    }
  }

  public void setWorkPreservingRecoveryEnabled(boolean enabled) {
    activeServiceContext.setWorkPreservingRecoveryEnabled(enabled);
  }

  @Override
  public boolean isWorkPreservingRecoveryEnabled() {
    return activeServiceContext.isWorkPreservingRecoveryEnabled();
  }

  @Override
  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return this.rmApplicationHistoryWriter;
  }

  @Override
  public void setSystemMetricsPublisher(
      SystemMetricsPublisher systemMetricsPublisher) {
    this.systemMetricsPublisher = systemMetricsPublisher;
  }

  @Override
  public SystemMetricsPublisher getSystemMetricsPublisher() {
    return this.systemMetricsPublisher;
  }

  @Override
  public void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter) {
    this.rmApplicationHistoryWriter = rmApplicationHistoryWriter;

  }

  @Override
  public ConfigurationProvider getConfigurationProvider() {
    return this.configurationProvider;
  }

  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public long getEpoch() {
    return activeServiceContext.getEpoch();
  }

  void setEpoch(long epoch) {
    activeServiceContext.setEpoch(epoch);
  }

  @Override
  public RMNodeLabelsManager getNodeLabelManager() {
    return activeServiceContext.getNodeLabelManager();
  }

  @Override
  public void setNodeLabelManager(RMNodeLabelsManager mgr) {
    activeServiceContext.setNodeLabelManager(mgr);
  }

  @Override
  public RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater() {
    return activeServiceContext.getRMDelegatedNodeLabelsUpdater();
  }

  @Override
  public void setRMDelegatedNodeLabelsUpdater(
      RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater) {
    activeServiceContext.setRMDelegatedNodeLabelsUpdater(
        delegatedNodeLabelsUpdater);
  }

  public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
    activeServiceContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
  }

  public boolean isSchedulerReadyForAllocatingContainers() {
    return activeServiceContext.isSchedulerReadyForAllocatingContainers();
  }

  @Private
  @VisibleForTesting
  public void setSystemClock(Clock clock) {
    activeServiceContext.setSystemClock(clock);
  }

  public ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
    return activeServiceContext.getSystemCredentialsForApps();
  }

  @Private
  @Unstable
  public RMActiveServiceContext getActiveServiceContext() {
    return activeServiceContext;
  }

  @Private
  @Unstable
  void setActiveServiceContext(RMActiveServiceContext activeServiceContext) {
    this.activeServiceContext = activeServiceContext;
  }

  @Override
  public Configuration getYarnConfiguration() {
    return this.yarnConfiguration;
  }

  public void setYarnConfiguration(Configuration yarnConfiguration) {
    this.yarnConfiguration=yarnConfiguration;
  }

  @Override
  public PlacementManager getQueuePlacementManager() {
    return this.activeServiceContext.getQueuePlacementManager();
  }
  
  @Override
  public void setQueuePlacementManager(PlacementManager placementMgr) {
    this.activeServiceContext.setQueuePlacementManager(placementMgr);
  }

  public String getHAZookeeperConnectionState() {
    if (elector == null) {
      return "Could not find leader elector. Verify both HA and automatic " +
          "failover are enabled.";
    } else {
      return elector.getZookeeperConnectionState();
    }
  }
  
  public void setIsDistributed(boolean isDistributed) {
    this.isDistributed = isDistributed;
  }

  @Override
  public boolean isDistributed() {
    return isDistributed;
  }

  @Override
  public boolean isLeader() {
    if (groupMembershipService == null) {
      return true;
    }
    return groupMembershipService.isLeader();
  }

  public void setRMGroupMembershipService(
          GroupMembershipService groupMembershipService) {
    this.groupMembershipService = groupMembershipService;
  }

  @Override
  public GroupMembershipService getGroupMembershipService() {
    return this.groupMembershipService;
  }
  
  public void setCertificateLocalizationService
      (CertificateLocalizationService certificateLocalizationService) {
    this.certificateLocalizationService = certificateLocalizationService;
  }
  
  @Override
  public CertificateLocalizationService getCertificateLocalizationService() {
    return certificateLocalizationService;
  }

  @Override
  public byte[] getSeed() {
    return seed;
  }

  public void setSeed(byte[] seed) {
    this.seed = seed;
  }

  @Override
  public String getUserFolderHashAlgo() {
    return userFolderHashAlgo;
  }

  public void setUserFolderHashAlgo(String userFolderHashAlgo) {
    this.userFolderHashAlgo = userFolderHashAlgo;
  }
  
  @Override
  public RMAppSecurityManager getRMAppSecurityManager() {
    return rmAppSecurityManager;
  }
  
  @Override
  public void setRMAppSecurityManager(RMAppSecurityManager rmAppSecurityManager) {
    this.rmAppSecurityManager = rmAppSecurityManager;
  }
}
