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
import io.hops.ha.common.TransactionStateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota.QuotaService;

public class RMContextImpl implements RMContext {


  private static final Log LOG = LogFactory.getLog(RMContextImpl.class);
  private TransactionStateManager transactionStateManager;
    
  //ResourceTracker client
  public ResourceTracker client;

  public ResourceTracker getResClient() {
    return client;
  }

  @Override
  public void recover(RMStateStore.RMState state) throws Exception {
    LOG.debug("recover rmcontext");
    //1.Recover nodes map
    Map<NodeId, RMNode> activeNodesRecovered = state.
        recoverRMContextActiveNodes(this);
    this.activesNodes.putAll(activeNodesRecovered);
    for(int i=0;i<activesNodes.size();i++){
       ClusterMetrics.getMetrics().incrNumActiveNodes(); 
    }
    this.resyncAfterRolback.addAll(activeNodesRecovered.keySet());
    for (NodeId nodeId : activesNodes.keySet()) {
      if (resourceTrackerService != null) {
        resourceTrackerService.getNmLivelinessMonitor().register(nodeId);
      }
    }
    nmTokenSecretManager.recover(state);
    containerTokenSecretManager.recover(state);
    //Recover rmNode state
    //2. Recover inactiveNodes map
    this.inactiveNodes.
        putAll(state.getRMContextInactiveNodes(this, state));
      for (RMNode node : inactiveNodes.values()) {
          switch (node.getState()) {
              case DECOMMISSIONED:
                  ClusterMetrics.getMetrics().incrDecommisionedNMs();
                  break;
              case LOST:
                  ClusterMetrics.getMetrics().incrNumLostNMs();
                  break;
              case REBOOTED:
                  ClusterMetrics.getMetrics().incrNumRebootedNMs();
                  break;
              case UNHEALTHY:
                  ClusterMetrics.getMetrics().incrNumUnhealthyNMs();
          }
      }
  }

  private Dispatcher rmDispatcher;
  private final ConcurrentMap<ApplicationId, RMApp> applications =
      new ConcurrentHashMap<ApplicationId, RMApp>();
      //recovered when rmappManager is recovered
  private final ConcurrentSkipListSet<NodeId> resyncAfterRolback =
      new ConcurrentSkipListSet<NodeId>();
  private final ConcurrentMap<NodeId, RMNode> activesNodes =
      new ConcurrentHashMap<NodeId, RMNode>();
      //recovered, pushed and removed everywhere
  private final ConcurrentMap<String, RMNode> inactiveNodes =
      new ConcurrentHashMap<String, RMNode>();
      //recovered, pushed and removed everywhere
  private boolean isHAEnabled; //recovered through configuration file
  private boolean isDistributedEnabled;
  private HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING; //recovered
  private AMLivelinessMonitor amLivelinessMonitor;//recovered
  private AMLivelinessMonitor amFinishingMonitor;//recovered
  private RMStateStore stateStore = null; //recovered
  private ContainerAllocationExpirer containerAllocationExpirer; //recovered
  private DelegationTokenRenewer delegationTokenRenewer;//recovered
  private AMRMTokenSecretManager amRMTokenSecretManager;//recovered
  private RMContainerTokenSecretManager containerTokenSecretManager;//recovered
  private NMTokenSecretManagerInRM nmTokenSecretManager;//recovered
  private ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;
      //recovered
  private AdminService adminService;//recovered
  private GroupMembershipService groupMembershipService;
  private ClientRMService clientRMService;//recovered
  private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;
      //recovered
  private ResourceScheduler scheduler;//recovered
  private NodesListManager nodesListManager;//recovered
  private ResourceTrackerService resourceTrackerService;//recovered
  private ApplicationMasterService applicationMasterService;//recovered
  private RMApplicationHistoryWriter rmApplicationHistoryWriter;//recovered
  private ConfigurationProvider configurationProvider;//recovered
  private ContainersLogsService containersLogsService;
  private QuotaService quotaService;
  
  /**
   * Default constructor. To be used in conjunction with setter methods for
   * individual fields.
     * @param conf
   */
  public RMContextImpl() {}

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
      RMApplicationHistoryWriter rmApplicationHistoryWriter, Configuration conf) {
    this.setDispatcher(rmDispatcher);
    this.setContainerAllocationExpirer(containerAllocationExpirer);
    this.setAMLivelinessMonitor(amLivelinessMonitor);
    this.setAMFinishingMonitor(amFinishingMonitor);
    this.setDelegationTokenRenewer(delegationTokenRenewer);
    this.setAMRMTokenSecretManager(appTokenSecretManager);
    this.setContainerTokenSecretManager(containerTokenSecretManager);
    this.setNMTokenSecretManager(nmTokenSecretManager);
    this.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    this.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);
    
    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }

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
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
      RMApplicationHistoryWriter rmApplicationHistoryWriter,
      Configuration conf, TransactionStateManager transactionStateManager) {
    this.setDispatcher(rmDispatcher);
    this.setContainerAllocationExpirer(containerAllocationExpirer);
    this.setAMLivelinessMonitor(amLivelinessMonitor);
    this.setAMFinishingMonitor(amFinishingMonitor);
    this.setDelegationTokenRenewer(delegationTokenRenewer);
    this.setAMRMTokenSecretManager(appTokenSecretManager);
    this.setTransactionStateManager(transactionStateManager);
    
    if (conf != null) {
      this.setContainerTokenSecretManager(
          new RMContainerTokenSecretManager(conf, this));
      this.setNMTokenSecretManager(new NMTokenSecretManagerInRM(conf, this));
    } else {
      this.setContainerTokenSecretManager(null);
      this.setNMTokenSecretManager(null);
    }
    this.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    this.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }

    ConfigurationProvider provider = new LocalConfigurationProvider();
    setConfigurationProvider(provider);
  }

  @Override
  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }

  @Override
  public RMStateStore getStateStore() {
    return stateStore;
  }

  @Override
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return this.applications;
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getActiveRMNodes() {
    return this.activesNodes;
  }

  @Override
  public ConcurrentSkipListSet<NodeId> getRMNodesToResyncAfterRolback() {
    return this.resyncAfterRolback;
  }

  @Override
  public ConcurrentMap<String, RMNode> getInactiveRMNodes() {
    return this.inactiveNodes;
  }

  @Override
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return this.containerAllocationExpirer;
  }

  @Override
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }

  @Override
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return this.amFinishingMonitor;
  }

  @Override
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return delegationTokenRenewer;
  }

  @Override
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return this.amRMTokenSecretManager;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }

  @Override
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return this.nmTokenSecretManager;
  }

  @Override
  public ResourceScheduler getScheduler() {
    return this.scheduler;
  }

  @Override
  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  @Override
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMTokenSecretManager;
  }

  @Override
  public AdminService getRMAdminService() {
    return this.adminService;
  }

  @Override
  public GroupMembershipService getGroupMembershipService() {
    return this.groupMembershipService;
  }

  @VisibleForTesting
  public void setStateStore(RMStateStore store) {
    stateStore = store;
  }

  @Override
  public ClientRMService getClientRMService() {
    return this.clientRMService;
  }

  @Override
  public ApplicationMasterService getApplicationMasterService() {
    return applicationMasterService;
  }

  @Override
  public ResourceTrackerService getResourceTrackerService() {
    return resourceTrackerService;
  }
  
  @Override
  public ContainersLogsService getContainersLogsService() {
      return containersLogsService;
  }

  @Override
  public QuotaService getQuotaService() {
      return quotaService;
  }
  
  void setHAEnabled(boolean isHAEnabled) {
    this.isHAEnabled = isHAEnabled;
  }

  void setDistributedEnabled(boolean isDistributedEnabled){
    this.isDistributedEnabled = isDistributedEnabled;
  }
  
  void setHAServiceState(HAServiceState haServiceState) {
    synchronized (haServiceState) {
      this.haServiceState = haServiceState;
      LOG.debug(
          "setHAServiceState " + groupMembershipService.getHostname() + " " +
              haServiceState);
    }
  }

  void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  void setTransactionStateManager(TransactionStateManager tsm){
    this.transactionStateManager = tsm;
  }
  
  void setRMAdminService(AdminService adminService) {
    this.adminService = adminService;
  }

  public void setRMGroupMembershipService(
      GroupMembershipService groupMembershipService) {
    this.groupMembershipService = groupMembershipService;
  }

  @Override
  public void setClientRMService(ClientRMService clientRMService) {
    this.clientRMService = clientRMService;
  }

  @Override
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return this.rmDelegationTokenSecretManager;
  }

  @Override
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    this.rmDelegationTokenSecretManager = delegationTokenSecretManager;
  }

  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.containerAllocationExpirer = containerAllocationExpirer;
  }

  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    this.amLivelinessMonitor = amLivelinessMonitor;
  }

  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    this.amFinishingMonitor = amFinishingMonitor;
  }

  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
    this.nmTokenSecretManager = nmTokenSecretManager;
  }

  void setScheduler(ResourceScheduler scheduler) {
    this.scheduler = scheduler;
  }

  void setDelegationTokenRenewer(
      DelegationTokenRenewer delegationTokenRenewer) {
    this.delegationTokenRenewer = delegationTokenRenewer;
  }

  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
  }

  void setAMRMTokenSecretManager(
      AMRMTokenSecretManager amRMTokenSecretManager) {
    this.amRMTokenSecretManager = amRMTokenSecretManager;
  }

  @VisibleForTesting
  public void setNodesListManager(NodesListManager nodesListManager) {
    this.nodesListManager = nodesListManager;
  }

  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    this.applicationMasterService = applicationMasterService;
  }

  void setResourceTrackerService(
      ResourceTrackerService resourceTrackerService) {
    this.resourceTrackerService = resourceTrackerService;
  }
  
  public void setContainersLogsService(
          ContainersLogsService containersLogsService) {
      this.containersLogsService = containersLogsService;
  }

  void setQuotaService(
          QuotaService quotaService) {
      this.quotaService = quotaService;
  }
  
  @Override
  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  @Override
  public boolean isLeadingRT(){
    if(!isHAEnabled){
      return true;
    }
    return groupMembershipService.isLeadingRT();
  }
  
  @Override
  public boolean isDistributedEnabled(){
    return isDistributedEnabled;
  }
  
  @Override
  public HAServiceState getHAServiceState() {
    synchronized (haServiceState) {
      LOG.debug(
          "getHAServiceState " + groupMembershipService.getHostname() + " " +
              haServiceState);
      return haServiceState;
    }
  }

  @Override
  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return rmApplicationHistoryWriter;
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
  public TransactionStateManager getTransactionStateManager() {
    return transactionStateManager;
  }
}
