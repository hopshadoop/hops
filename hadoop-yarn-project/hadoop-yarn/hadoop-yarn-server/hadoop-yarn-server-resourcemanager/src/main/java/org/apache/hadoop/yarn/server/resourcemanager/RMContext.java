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

import io.hops.ha.common.TransactionStateManager;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota.QuotaService;

/**
 * Context of the ResourceManager.
 */
public interface RMContext extends Recoverable {

  Dispatcher getDispatcher();

  boolean isHAEnabled();

  boolean isLeadingRT();
  
  boolean isLeader();
  
  boolean isDistributedEnabled();
  
  HAServiceState getHAServiceState();

  RMStateStore getStateStore();

  ConcurrentMap<ApplicationId, RMApp> getRMApps();
  
  ConcurrentMap<String, RMNode> getInactiveRMNodes();

  ConcurrentMap<NodeId, RMNode> getActiveRMNodes();

  AMLivelinessMonitor getAMLivelinessMonitor();

  AMLivelinessMonitor getAMFinishingMonitor();

  ContainerAllocationExpirer getContainerAllocationExpirer();
  
  DelegationTokenRenewer getDelegationTokenRenewer();

  AMRMTokenSecretManager getAMRMTokenSecretManager();

  RMContainerTokenSecretManager getContainerTokenSecretManager();
  
  NMTokenSecretManagerInRM getNMTokenSecretManager();

  ResourceScheduler getScheduler();

  NodesListManager getNodesListManager();

  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();

  AdminService getRMAdminService();

  GroupMembershipService getGroupMembershipService();
  
  ClientRMService getClientRMService();

  ApplicationMasterService getApplicationMasterService();

  ResourceTrackerService getResourceTrackerService();
  
  ContainersLogsService getContainersLogsService();
  
  QuotaService getQuotaService();
  
  void setClientRMService(ClientRMService clientRMService);

  RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

  void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager);

  RMApplicationHistoryWriter getRMApplicationHistoryWriter();

  void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter);

  ConfigurationProvider getConfigurationProvider();
  
  public TransactionStateManager getTransactionStateManager();
}