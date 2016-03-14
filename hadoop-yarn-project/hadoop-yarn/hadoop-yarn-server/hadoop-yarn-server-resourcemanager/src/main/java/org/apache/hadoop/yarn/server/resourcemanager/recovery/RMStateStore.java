/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import io.hops.ha.common.TransactionState;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.Node;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.QueueMetrics;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMContextActiveNodes;
import io.hops.metadata.yarn.entity.RMContextInactiveNodes;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerAppReservations;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.metadata.yarn.entity.appmasterrpc.AllocateRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.HeartBeatRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppUpdateSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUpdateSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;

@Private
@Unstable
/**
 * Base class to implement storage of ResourceManager state.
 * Takes care of asynchronous notifications and interfacing with YARN objects.
 * Real store implementations need to derive from it and implement blocking
 * store and load methods to actually store and load the state.
 */ public abstract class RMStateStore extends AbstractService {

  // constants for RM App state and RMDTSecretManagerState.
  protected static final String RM_APP_ROOT = "RMAppRoot";
  protected static final String RM_DT_SECRET_MANAGER_ROOT =
      "RMDTSecretManagerRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String RM_TOKEN_SECRET_ROOT = "RMTokenSecretRoot";
  protected static final String RM_TOKEN_SECRET_PREFIX = "RMTokenSecretPrefix_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";
  protected static final String DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX =
      "RMDTSequenceNumber_";
  protected static final String VERSION_NODE = "RMVersionNode";
  
  public static final Log LOG = LogFactory.getLog(RMStateStore.class);

  public RMStateStore() {
    super(RMStateStore.class.getName());
  }

  /**
   * State of an application attempt
   */
  public static class ApplicationAttemptState {

    final ApplicationAttemptId attemptId;
    final org.apache.hadoop.yarn.api.records.Container masterContainer;
    final Credentials appAttemptCredentials;
    long startTime = 0;
    float progress;
    String host;
    int rpcPort;
    Set<NodeId> ranNodes;
    List<org.apache.hadoop.yarn.api.records.ContainerStatus>
        justFinishedContainers;

    // fields set when attempt completes
    RMAppAttemptState state;
    String finalTrackingUrl = "N/A";
    String diagnostics;
    FinalApplicationStatus amUnregisteredFinalStatus;

    public ApplicationAttemptState(ApplicationAttemptId attemptId,
        org.apache.hadoop.yarn.api.records.Container masterContainer,
        Credentials appAttemptCredentials, long startTime) {
      this(attemptId, masterContainer, appAttemptCredentials, startTime, null,
          null, "", null, 0, "N/A", -1, null, null);
    }

    public ApplicationAttemptState(ApplicationAttemptId attemptId,
        org.apache.hadoop.yarn.api.records.Container masterContainer,
        Credentials appAttemptCredentials, long startTime,
        RMAppAttemptState state, String finalTrackingUrl, String diagnostics,
        FinalApplicationStatus amUnregisteredFinalStatus, float progress,
        String host, int rpcPort, Set<NodeId> ranNodes,
        List<org.apache.hadoop.yarn.api.records.ContainerStatus> justFinishedContainers) {
      this.attemptId = attemptId;
      this.masterContainer = masterContainer;
      this.appAttemptCredentials = appAttemptCredentials;
      this.startTime = startTime;
      this.progress = progress;
      this.state = state;
      this.finalTrackingUrl = finalTrackingUrl;
      this.diagnostics = diagnostics == null ? "" : diagnostics;
      this.amUnregisteredFinalStatus = amUnregisteredFinalStatus;
      this.host = host;
      this.rpcPort = rpcPort;
      this.ranNodes = ranNodes;
      this.justFinishedContainers = justFinishedContainers;
    }

    public org.apache.hadoop.yarn.api.records.Container getMasterContainer() {
      return masterContainer;
    }

    public ApplicationAttemptId getAttemptId() {
      return attemptId;
    }

    public Credentials getAppAttemptCredentials() {
      return appAttemptCredentials;
    }

    public RMAppAttemptState getState() {
      return state;
    }

    public String getFinalTrackingUrl() {
      return finalTrackingUrl;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public long getStartTime() {
      return startTime;
    }

    public float getProgress() {
      return progress;
    }

    public String getHost() {
      return host;
    }

    public int getRpcPort() {
      return rpcPort;
    }

    public Set<NodeId> getRanNodes() {
      if (ranNodes == null) {
        ranNodes = new HashSet<NodeId>();
      }
      return ranNodes;
    }

    public List<org.apache.hadoop.yarn.api.records.ContainerStatus> getJustFinishedContainers() {
      return justFinishedContainers;
    }

    public FinalApplicationStatus getFinalApplicationStatus() {
      return amUnregisteredFinalStatus;
    }
  }

  /**
   * State of an application application
   */
  public static class ApplicationState {

    final ApplicationSubmissionContext context;
    final long submitTime;
    final long startTime;
    final String user;
    Map<ApplicationAttemptId, ApplicationAttemptState> attempts =
        new HashMap<ApplicationAttemptId, ApplicationAttemptState>();
    // fields set when application completes.
    RMAppState state;
    String diagnostics;
    long finishTime;
    RMAppState stateBeforeKilling;
    private final List<NodeId> updatedNodes;

    public ApplicationState(long submitTime, long startTime,
        ApplicationSubmissionContext context, String user) {
      this(submitTime, startTime, context, user, null, "", 0, null, null);
    }

    public ApplicationState(long submitTime, long startTime,
        ApplicationSubmissionContext context, String user, RMAppState state,
        String diagnostics, long finishTime, RMAppState stateBeforeKilling,
        List<NodeId> updatedNodes) {
      this.submitTime = submitTime;
      this.startTime = startTime;
      this.context = context;
      this.user = user;
      this.state = state;
      this.diagnostics = diagnostics == null ? "" : diagnostics;
      this.finishTime = finishTime;
      this.stateBeforeKilling = stateBeforeKilling;
      this.updatedNodes = updatedNodes;
    }

    public ApplicationId getAppId() {
      return context.getApplicationId();
    }

    public long getSubmitTime() {
      return submitTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public int getAttemptCount() {
      return attempts.size();
    }

    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      return context;
    }

    public ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId) {
      return attempts.get(attemptId);
    }

    public String getUser() {
      return user;
    }

    public RMAppState getState() {
      return state;
    }

    public RMAppState getStateBeforeKilling() {
      return stateBeforeKilling;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public long getFinishTime() {
      return finishTime;
    }

    public List<NodeId> getUpdatedNodes() {
      return updatedNodes;
    }

  }

  public static enum KeyType {

    CURRENTNMTOKENMASTERKEY,
    NEXTNMTOKENMASTERKEY,
    CURRENTCONTAINERTOKENMASTERKEY,
    NEXTCONTAINERTOKENMASTERKEY
  }

  public static class RMDTSecretManagerState {

    // DTIdentifier -> renewDate
    Map<RMDelegationTokenIdentifier, Long> delegationTokenState =
        new HashMap<RMDelegationTokenIdentifier, Long>();

    Set<DelegationKey> masterKeyState = new HashSet<DelegationKey>();

    int dtSequenceNumber = 0;

    public Map<RMDelegationTokenIdentifier, Long> getTokenState() {
      return delegationTokenState;
    }

    public Set<DelegationKey> getMasterKeyState() {
      return masterKeyState;
    }

    public int getDTSequenceNumber() {
      return dtSequenceNumber;
    }
  }

  /**
   * State of the ResourceManager
   */
  public static class RMState {

    Map<ApplicationId, ApplicationState> appState =
        new HashMap<ApplicationId, ApplicationState>();

    Map<ApplicationAttemptId, AllocateResponse> allocateResponses =
        new HashMap<ApplicationAttemptId, AllocateResponse>();
    String nodeState;

    RMDTSecretManagerState rmSecretManagerState = new RMDTSecretManagerState();

    Map<KeyType, MasterKey> secretMamagerKeys;
    List<RPC> appMasterRPCs;
    Map<Integer, HeartBeatRPC> heartBeatRPCs;
    Map<Integer, AllocateRPC> allocateRPCs;
    List<PendingEvent> pendingEvents;
    Map<String, Map<String, AppSchedulingInfo>> appSchedulingInfos;
    Map<String, SchedulerApplication> schedulerApplications;
    Map<String, FiCaSchedulerNode> fiCaSchedulerNodes;
    Map<String, List<LaunchedContainers>> launchedContainers;
    Map<String, List<ResourceRequest>> resourceRequests;
    Map<String, List<AppSchedulingInfoBlacklist>> blackLists;
    List<QueueMetrics> allQueueMetrics;
    Map<String, List<FiCaSchedulerAppSchedulingOpportunities>> schedulingOpportunities;
    Map<String, List<FiCaSchedulerAppLastScheduledContainer>> lastScheduledContainers;
    Map<String, List<FiCaSchedulerAppReservedContainers>> reservedContainers;
    Map<String, List<SchedulerAppReservations>> reReservations;
    Map<String, NodeHeartbeatResponse> nodeHeartBeatResponses;
    Map<String, Set<ContainerId>> containersToClean;
    Map<String, List<ApplicationId>> finishedApplications;
    Map<String, Map<Integer, Map<Integer, Resource>>> nodesResources;
    Map<String, Set<String>> csLeafQueuesPendingApps;
    Map<String, Container> allContainers;
    Map<String, RMContainer> allRMContainers;
    List<RMContextActiveNodes> allRMContextActiveNodes;
    Map<String, RMNode> allRMNodes;
    Map<String, Node> allNodes;
    List<RMContextInactiveNodes> rmContextInactiveNodes;
    Map<String, Map<Integer, List<UpdatedContainerInfo>>>
        allUpdatedContainerInfos;
    Map<String, ContainerStatus> allContainerStatus;
    Map<String, List<JustLaunchedContainers>> allJustLaunchedContainers;
    Map<String, Boolean> allRMNodeNextHeartbeats;
    
    public Map<ApplicationId, ApplicationState> getApplicationState() {
      return appState;
    }

    public Map<ApplicationAttemptId, AllocateResponse> getAllocateResponses() {
      return allocateResponses;
    }

    public RMDTSecretManagerState getRMDTSecretManagerState() {
      return rmSecretManagerState;
    }

    public String getNodeState() {
      return nodeState;
    }

    public void setNodeState(String nodeState) {
      this.nodeState = nodeState;
    }

    public Map<KeyType, MasterKey> getSecretTokenMamagerKey() {
      return secretMamagerKeys;
    }

    public MasterKey getSecretTokenMamagerKey(KeyType keyType) {
      return secretMamagerKeys.get(keyType);
    }

    public List<RPC> getAppMasterRPCs() throws IOException {
      if (appMasterRPCs != null) {
        return appMasterRPCs;
      } else {
        return Collections.EMPTY_LIST;
      }
    }

    public Map<Integer, HeartBeatRPC> getHeartBeatRPCs(){
      return heartBeatRPCs;
    }
    
    public Map<Integer, AllocateRPC> getAllocateRPCs(){
      return allocateRPCs;
    }
    
    public List<PendingEvent> getPendingEvents() throws IOException {
      if (pendingEvents != null) {
        return pendingEvents;
      } else {
        return Collections.EMPTY_LIST;
      }
    }
    
    public Map<String, AppSchedulingInfo> getAppSchedulingInfo(
            final String appId)
            throws IOException {
      return appSchedulingInfos.get(appId);
    }

    public Map<String, SchedulerApplication> getSchedulerApplications()
        throws IOException {
      return schedulerApplications;
    }

    public Map<String, FiCaSchedulerNode> getAllFiCaSchedulerNodes()
        throws IOException {
      return fiCaSchedulerNodes;
    }

    public List<LaunchedContainers> getLaunchedContainers(final String nodeName)
        throws IOException {
      if (launchedContainers.get(nodeName) != null) {
        return launchedContainers.get(nodeName);
      } else {
        return Collections.EMPTY_LIST;
      }
    }

    public List<FiCaSchedulerAppSchedulingOpportunities> getSchedulingOpportunities(
            final String ficaId) throws IOException {
      return schedulingOpportunities.get(ficaId);
    }
    
    public List<FiCaSchedulerAppReservedContainers> getReservedContainers(
            final String ficaId) throws IOException {
      return reservedContainers.get(ficaId);
    }
    
    public List<FiCaSchedulerAppLastScheduledContainer> getLastScheduledContainers(
            final String ficaId) throws IOException {
      return lastScheduledContainers.get(ficaId);
    }
    
    public List<SchedulerAppReservations> getRereservations(
            final String ficaId) throws IOException {
      return reReservations.get(ficaId);
    }
    
    public List<String> getNewlyAllocatedContainers(
        final String ficaId) throws IOException {
       List<String> newlyAllocatedContainers = new ArrayList<String>();
      for(RMContainer rmc: allRMContainers.values()){
        if(rmc.getApplicationAttemptId().equals(ficaId)){
          if(rmc.getState().equals(RMContainerState.NEW.toString()) ||
                  rmc.getState().equals(RMContainerState.RESERVED.toString()) ||
                  rmc.getState().equals(RMContainerState.ALLOCATED.toString())){
              newlyAllocatedContainers.add(rmc.getContainerId());
          }
        }
      }
      return newlyAllocatedContainers;
    }

    //TODO implement in a more efficient way
    public List<String> getLiveContainers(
        final String ficaId) throws IOException {
      List<String> liveContainers = new ArrayList<String>();
      for(RMContainer rmc: allRMContainers.values()){
        if(rmc.getApplicationAttemptId().equals(ficaId)){
          if(!rmc.getState().equals(RMContainerState.COMPLETED.toString()) &&
                  !rmc.getState().equals(RMContainerState.EXPIRED.toString()) &&
                  !rmc.getState().equals(RMContainerState.KILLED.toString()) &&
                  !rmc.getState().equals(RMContainerState.RELEASED.toString())){
              liveContainers.add(rmc.getContainerId());
          }
        }
      }
      
    return liveContainers;
    }

    public List<ResourceRequest> getResourceRequests(final String id)
        throws IOException {
      return resourceRequests.get(id);
    }

    public List<AppSchedulingInfoBlacklist> getBlackList(final String id)
        throws IOException {
      return blackLists.get(id);
    }
        
    private final Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
        alreadyRecoveredRMContextActiveNodes =
        new HashMap<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>();

    public Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode> recoverRMContextActiveNodes(
        final RMContext rmContext) throws IOException, Exception {
      if (alreadyRecoveredRMContextActiveNodes.isEmpty()) {
        if (allRMContextActiveNodes != null && !allRMContextActiveNodes.
            isEmpty()) {
          for (RMContextActiveNodes hopRMContextNode : allRMContextActiveNodes) {
            NodeId nodeId =
                ConverterUtils.toNodeId(hopRMContextNode.getNodeId());
            //retrieve RMNode in order to create a new FiCaSchedulerNode
            RMNode hopRMNode = allRMNodes.get(hopRMContextNode.getNodeId());
            //Retrieve resource of RMNode
            Resource res =
                getResource(hopRMNode.getNodeId(), Resource.TOTAL_CAPABILITY,
                    Resource.RMNODE);
            //Retrieve and Initialize NodeBase for RMNode
            org.apache.hadoop.net.Node node = null;
            if (hopRMNode.getNodeId() != null) {
              Node hopNode = allNodes.get(hopRMNode.getNodeId());
              node = new NodeBase(hopNode.getName(), hopNode.getLocation());
              if (hopNode.getParent() != null) {
                node.setParent(new NodeBase(hopNode.getParent()));
              }
              node.setLevel(hopNode.getLevel());
            }
            //Get NextHeartbeat
            LOG.debug("nexthb: " + allRMNodeNextHeartbeats
                    + " hopRMContextNode: " + hopRMContextNode + " getNodeId: "
                    + hopRMContextNode.
                    getNodeId());
            boolean nextHeartbeat = false;
            if (allRMNodeNextHeartbeats.get(hopRMContextNode.getNodeId())
                    != null) {
              nextHeartbeat = true;
            }

            LOG.info("HOP :: RMStateStore-node:" + hopRMContextNode.
                    getNodeId() + ", state:" + hopRMNode.getCurrentState()
                    + " resources: " + res);
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmNode =
                new RMNodeImpl(nodeId, rmContext, hopRMNode.getHostName(),
                    hopRMNode.getCommandPort(), hopRMNode.getHttpPort(), node,
                    ResourceOption.newInstance(
                        org.apache.hadoop.yarn.api.records.Resource
                            .newInstance(res.
                                getMemory(), res.getVirtualCores()),
                        hopRMNode.getOvercommittimeout()),
                    hopRMNode.getNodemanagerVersion(),
                    hopRMNode.getHealthReport(),
                    hopRMNode.getLastHealthReportTime(), nextHeartbeat);

            ((RMNodeImpl) rmNode).setState(hopRMNode.getCurrentState());
            rmNode.recover(this);
            alreadyRecoveredRMContextActiveNodes.put(nodeId, rmNode);
            LOG.debug("HOP :: alreadyRecoveredRMContextActiveNodes.put:" +
                nodeId.toString());
          }
        }
      }
      return alreadyRecoveredRMContextActiveNodes;

    }

    private Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
        alreadyRecoveredRMContextInactiveNodes =
        new HashMap<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>();

    public Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode> getRMContextInactiveNodes(
        final RMContext rmContext, final RMState state) throws Exception {
      if (alreadyRecoveredRMContextInactiveNodes.isEmpty()) {
        //Retrieve rmctxnodes table entries
        if (rmContextInactiveNodes != null &&
            !rmContextInactiveNodes.isEmpty()) {
          for (RMContextInactiveNodes key : rmContextInactiveNodes) {
            NodeId nodeId = ConverterUtils.toNodeId(key.getRmnodeid());
            //retrieve RMNode in order to create a new FiCaSchedulerNode
            RMNode hopRMNode = allRMNodes.get(key.getRmnodeid());
            //Retrieve resource of RMNode
            Resource res =
                getResource(hopRMNode.getNodeId(), Resource.TOTAL_CAPABILITY,
                    Resource.RMNODE);
            //Retrieve and Initialize NodeBase for RMNode
            org.apache.hadoop.net.Node node = null;
            if (hopRMNode.getNodeId() != null) {
              Node hopNode = allNodes.get(hopRMNode.getNodeId());
              node = new NodeBase(hopNode.getName(), hopNode.getLocation());
              if (hopNode.getParent() != null) {
                node.setParent(new NodeBase(hopNode.getParent()));
              }
              node.setLevel(hopNode.getLevel());
            }
            boolean nextHeartbeat =
                allRMNodeNextHeartbeats.get(key.getRmnodeid());
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmNode =
                new RMNodeImpl(nodeId, rmContext, hopRMNode.getHostName(),
                    hopRMNode.getCommandPort(), hopRMNode.getHttpPort(), node,
                    ResourceOption.newInstance(
                        org.apache.hadoop.yarn.api.records.Resource
                            .newInstance(res.
                                getMemory(), res.getVirtualCores()),
                        hopRMNode.getOvercommittimeout()),
                    hopRMNode.getNodemanagerVersion(),
                    hopRMNode.getHealthReport(),
                    hopRMNode.getLastHealthReportTime(), nextHeartbeat);
            ((RMNodeImpl) rmNode).setState(hopRMNode.getCurrentState());
            rmNode.recover(this);
            alreadyRecoveredRMContextInactiveNodes.put(rmNode.getNodeID().
                getHost(), rmNode);
          }
        }

      }

      return alreadyRecoveredRMContextInactiveNodes;

    }

    private Map<ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>
        alreadyRecoveredContainerStatus =
        new HashMap<ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>();

    public LinkedList<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> getUpdatedContainerInfo(
        final String rmNodeId, RMNodeImpl rmNode) {
      LinkedList<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
          updatedContainerInfoQueue =
          new LinkedList<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
      if (allUpdatedContainerInfos != null) {
        //Retrieve updatedcontainerinfo entries for the particular RMNode
        Map<Integer, List<UpdatedContainerInfo>> updatedContainerInfos =
            allUpdatedContainerInfos.get(rmNodeId);
        if (updatedContainerInfos != null) {
          List<org.apache.hadoop.yarn.api.records.ContainerStatus>
              newlyLaunched =
              new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();
          List<org.apache.hadoop.yarn.api.records.ContainerStatus> completed =
              new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();

          SortedSet<Integer> keys = new TreeSet<Integer>(updatedContainerInfos.
              keySet());
          for (int updatedContainerInfoId : keys) {
            List<UpdatedContainerInfo> hopUpdatedContainerInfo =
                allUpdatedContainerInfos.get(rmNodeId)
                    .get(updatedContainerInfoId);

            for (UpdatedContainerInfo hopUCI : hopUpdatedContainerInfo) {
              //Retrieve containerstatus entries for the particular updatedcontainerinfo
              org.apache.hadoop.yarn.api.records.ContainerStatus
                  containerStatus = null;
              if (!alreadyRecoveredContainerStatus.containsKey(ConverterUtils.
                  toContainerId(hopUCI.getContainerId()))) {
                ContainerStatus hopConStatus = allContainerStatus.get(hopUCI.
                    getContainerId());

                if (hopConStatus != null) {
                  ContainerId cid = ConverterUtils.toContainerId(hopConStatus.
                      getContainerid());
                  containerStatus =
                      org.apache.hadoop.yarn.api.records.ContainerStatus
                          .newInstance(cid,
                              ContainerState.valueOf(hopConStatus.getState()),
                              hopConStatus.getDiagnostics(),
                              hopConStatus.getExitstatus());
                  alreadyRecoveredContainerStatus.put(cid, containerStatus);
                }
              } else {
                containerStatus =
                    alreadyRecoveredContainerStatus.get(ConverterUtils.
                        toContainerId(hopUCI.getContainerId()));
              }
              if (containerStatus != null) {
                if (containerStatus.getState().toString()
                    .equals(TablesDef.ContainerStatusTableDef.STATE_RUNNING)) {
                  if (!rmNode.getJustLaunchedContainers().containsKey(
                          containerStatus.getContainerId())) {
                    LOG.debug("add newlyLaunchedContainer " + containerStatus.
                            getContainerId());
                    newlyLaunched.add(containerStatus);
                  }
                } else if (containerStatus.getState().toString()
                    .equals(TablesDef.ContainerStatusTableDef.STATE_COMPLETED)) {
                  completed.add(containerStatus);
                }
              }
            }
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo
                uci =
                new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                    newlyLaunched, completed, updatedContainerInfoId);
            updatedContainerInfoQueue.add(uci);
          }
        }
      }
      return updatedContainerInfoQueue;
    }

    public NodeHeartbeatResponse getNodeHeartBeatResponse(
        final String rmNodeId) {
      if (nodeHeartBeatResponses != null) {
        return nodeHeartBeatResponses.get(rmNodeId);
      } else {
        return null;
      }
    }

    public Set<ContainerId> getContainersToClean(final String rmNodeId) {
      if (containersToClean != null &&
          containersToClean.get(rmNodeId) != null) {
        return containersToClean.get(rmNodeId);
      } else {
        return new HashSet<ContainerId>();
      }
    }

    public List<ApplicationId> getFinishedApplications(final String rmNodeId) {
      if (finishedApplications != null &&
          finishedApplications.get(rmNodeId) != null) {
        return finishedApplications.get(rmNodeId);
      } else {
        return new ArrayList<ApplicationId>();
      }
    }

    public Map<ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus> getRMNodeJustLaunchedContainers(
        final String rmNodeId) {
      Map<ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>
          justLaunchedContainers =
          new HashMap<ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>();
      if (allJustLaunchedContainers != null) {
        List<JustLaunchedContainers> hopJustLaunchedContainers =
            allJustLaunchedContainers.get(rmNodeId);
        if (hopJustLaunchedContainers != null) {
          for (JustLaunchedContainers hop : hopJustLaunchedContainers) {
            ContainerId cid =
                ConverterUtils.toContainerId(hop.getContainerId());

            if (alreadyRecoveredContainerStatus.containsKey(cid)) {
              justLaunchedContainers.put(cid, alreadyRecoveredContainerStatus.
                  get(cid));
            } else {
              //Retrieve ContainerStatus
              ContainerStatus hopConStatus = allContainerStatus.get(hop.
                  getContainerId());
              org.apache.hadoop.yarn.api.records.ContainerStatus conStatus =
                  org.apache.hadoop.yarn.api.records.ContainerStatus
                      .newInstance(cid,
                          ContainerState.valueOf(hopConStatus.getState()),
                          hopConStatus.getDiagnostics(),
                          hopConStatus.getExitstatus());
              justLaunchedContainers.put(cid, conStatus);
              alreadyRecoveredContainerStatus.put(cid, conStatus);
            }
          }
        }
      }
      return justLaunchedContainers;
    }

    private final Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer>
        alreadyRecoveredRMContainers =
        new HashMap<String, org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer>();

    public org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer getRMContainer(
        final String id, final RMContext rmContext) throws IOException {
      if (!alreadyRecoveredRMContainers.containsKey(id)) {
        RMContainer hopRMContainer = allRMContainers.get(id);
        //retrieve Container
        org.apache.hadoop.yarn.api.records.Container container =
            getContainer(hopRMContainer.getContainerId());

        //construct RMContainer
        org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer
            rmContainer = new RMContainerImpl(container, container.
            getId().getApplicationAttemptId(),
            ConverterUtils.toNodeId(hopRMContainer.getNodeId()),
            hopRMContainer.getUser(), rmContext, null);
        rmContainer.recover(hopRMContainer);
        alreadyRecoveredRMContainers.put(id, rmContainer);
        return rmContainer;
      } else {
        return alreadyRecoveredRMContainers.get(id);
      }
    }

    private final Map<String, org.apache.hadoop.yarn.api.records.Container>
        alreadyRecoveredContainers =
        new HashMap<String, org.apache.hadoop.yarn.api.records.Container>();

    public org.apache.hadoop.yarn.api.records.Container getContainer(
        final String id) throws IOException {
      if (!alreadyRecoveredContainers.containsKey(id)) {
        Container hopContainer = allContainers.get(id);
        ContainerPBImpl container = null;
        if(hopContainer!=null){
          container = new ContainerPBImpl(
          YarnProtos.ContainerProto.parseFrom(hopContainer.
                getContainerState()));
        }else{
          //TORECOVER find out why we sometime get this
          LOG.error("the container should not be null " + id);
        }
        alreadyRecoveredContainers.put(id, container);
        return container;
      } else {
        return alreadyRecoveredContainers.get(id);
      }
    }

    public SchedulerApplication getSchedulerApplication(final String id)
        throws IOException {
      return schedulerApplications.get(id);
    }

    public List<QueueMetrics> getAllQueueMetrics() throws IOException {
      return allQueueMetrics;
    }

    public Resource getResource(final String id, final int type,
        final int parent) throws IOException {
      if (nodesResources != null && nodesResources.get(id) != null &&
          nodesResources.get(id).get(type) != null) {
        return nodesResources.get(id).get(type).get(parent);
      } else {
        return null;
      }
    }
    
    public Set<String> getCSLeafQueuePendingApps(String path){
      if(csLeafQueuesPendingApps!=null){
        return csLeafQueuesPendingApps.get(path);
      }else{
        return null;
      }
    }
  }

  private Dispatcher rmDispatcher;

  /**
   * Dispatcher used to send state operation completion events to
   * ResourceManager services
   */
  public void setRMDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  AsyncDispatcher dispatcher;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher
        .register(RMStateStoreEventType.class, new ForwardingEventHandler());
    dispatcher.setDrainEventsOnStop();
    initInternal(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dispatcher.start();
    startInternal();
  }

  /**
   * Derived classes initialize themselves using this method.
   */
  protected abstract void initInternal(Configuration conf) throws Exception;

  /**
   * Derived classes start themselves using this method.
   * The base class is started and the event dispatcher is ready to use at
   * this point
   */
  protected abstract void startInternal() throws Exception;

  @Override
  protected void serviceStop() throws Exception {
    closeInternal();
    dispatcher.stop();
  }

  /**
   * Derived classes close themselves using this method.
   * The base class will be closed and the event dispatcher will be shutdown
   * after this
   */
  protected abstract void closeInternal() throws Exception;

  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of state-store is a major upgrade, and any
   * compatible change of state-store is a minor upgrade.
   * 3) If theres's no version, treat it as 1.0.
   * 4) Within a minor upgrade, say 1.1 to 1.2:
   * overwrite the version info and proceed as normal.
   * 5) Within a major upgrade, say 1.2 to 2.0:
   * throw exception and indicate user to use a separate upgrade tool to
   * upgrade RM state.
   */
  public void checkVersion() throws Exception {
    RMStateVersion loadedVersion = loadVersion();
    LOG.info("Loaded RM state version info " + loadedVersion);
    if (loadedVersion != null && loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    // if there is no version info, treat it as 1.0;
    if (loadedVersion == null) {
      loadedVersion = RMStateVersion.newInstance(1, 0);
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing RM state version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new RMStateVersionIncompatibleException(
          "Expecting RM state version " + getCurrentVersion() +
              ", but loading version " + loadedVersion);
    }
  }

  /**
   * Derived class use this method to load the version information from state
   * store.
   */
  protected abstract RMStateVersion loadVersion() throws Exception;

  /**
   * Derived class use this method to store the version information.
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * Get the current version of the underlying state store.
   */
  protected abstract RMStateVersion getCurrentVersion();

  /**
   * Blocking API
   * The derived class must recover state from the store and return a new
   * RMState object populated with that state
   * This must not be called on the dispatcher thread
   */
  public abstract RMState loadState(RMContext rmContext) throws Exception;

  /**
   * Non-Blocking API
   * ResourceManager services use this to store the application's state
   * This does not block the dispatcher threads
   * RMAppStoredEvent will be sent on completion to notify the RMApp
   */
  @SuppressWarnings(
      "unchecked")
  public synchronized void storeNewApplication(RMApp app,
      TransactionState transactionState) {
    ApplicationSubmissionContext context =
        app.getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationState appState = new ApplicationState(app.getSubmitTime(), app.
        getStartTime(), context, app.getUser());
    dispatcher.getEventHandler()
        .handle(new RMStateStoreAppEvent(appState, transactionState));
  }

  @SuppressWarnings(
      "unchecked")
  public synchronized void updateApplicationState(ApplicationState appState,
      TransactionState transactionState) {
    dispatcher.getEventHandler()
        .handle(new RMStateUpdateAppEvent(appState, transactionState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an
   * application.
   */
  protected abstract void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception;

  protected abstract void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception;

  @SuppressWarnings(
      "unchecked")
  /**
   * Non-blocking API
   * ResourceManager services call this to store state on an application
   * attempt
   * This does not block the dispatcher threads
   * RMAppAttemptStoredEvent will be sent on completion to notify the
   * RMAppAttempt
   */ public synchronized void storeNewApplicationAttempt(
      RMAppAttempt appAttempt, TransactionState transactionState) {
    Credentials credentials = getCredentialsFromAppAttempt(appAttempt);

    ApplicationAttemptState attemptState =
        new ApplicationAttemptState(appAttempt.getAppAttemptId(),
            appAttempt.getMasterContainer(), credentials,
            appAttempt.getStartTime());

    dispatcher.getEventHandler().handle(
        new RMStateStoreAppAttemptEvent(attemptState, transactionState));
  }

  @SuppressWarnings(
      "unchecked")
  public synchronized void updateApplicationAttemptState(
      ApplicationAttemptState attemptState, TransactionState transactionState) {
    dispatcher.getEventHandler().handle(
        new RMStateUpdateAppAttemptEvent(attemptState, transactionState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an
   * application attempt
   */
  protected abstract void storeApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception;

  protected abstract void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a delegation token
   * and sequence number
   */
  public synchronized void storeRMDelegationTokenAndSequenceNumber(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber, TransactionState transactionState) {
    try {
      storeRMDelegationTokenAndSequenceNumberState(rmDTIdentifier, renewDate,
          latestSequenceNumber);
    } catch (Exception e) {
      notifyStoreOperationFailed(e, transactionState);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void storeRMDelegationTokenAndSequenceNumberState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a delegation token
   */
  public synchronized void removeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, int sequenceNumber,
      TransactionState transactionState) {
    try {
      removeRMDelegationTokenState(rmDTIdentifier);
    } catch (Exception e) {
      notifyStoreOperationFailed(e, transactionState);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * RMDelegationToken
   */
  protected abstract void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception;

  /**
   * RMDTSecretManager call this to update the state of a delegation token
   * and sequence number
   */
  public synchronized void updateRMDelegationTokenAndSequenceNumber(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber, TransactionState transactionState) {
    try {
      updateRMDelegationTokenAndSequenceNumberInternal(rmDTIdentifier,
          renewDate, latestSequenceNumber);
    } catch (Exception e) {
      notifyStoreOperationFailed(e, transactionState);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to update the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void updateRMDelegationTokenAndSequenceNumberInternal(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a master key
   */
  public synchronized void storeRMDTMasterKey(DelegationKey delegationKey,
      TransactionState transactionState) {
    try {
      storeRMDTMasterKeyState(delegationKey);
    } catch (Exception e) {
      notifyStoreOperationFailed(e, transactionState);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * DelegationToken Master Key
   */
  protected abstract void storeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a master key
   */
  public synchronized void removeRMDTMasterKey(DelegationKey delegationKey,
      TransactionState transactionState) {
    try {
      removeRMDTMasterKeyState(delegationKey);
    } catch (Exception e) {
      notifyStoreOperationFailed(e, transactionState);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * DelegationToken Master Key
   */
  protected abstract void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * NMTokenSecretManagerInRM call this to store the state of a master key
   */
  public synchronized void storeRMTokenSecretManagerMasterKey(MasterKey key,
      KeyType keyType) throws Exception {
    storeRMTokenSecretManagerMasterKeyState(key, keyType);
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * Master Key
   */
  protected abstract void storeRMTokenSecretManagerMasterKeyState(MasterKey key,
      KeyType keyType) throws Exception;

  /**
   * NMTokenSecretManagerInRM call this to remove the state of a master key
   */
  public synchronized void removeRMTokenSecretManagerMasterKey(KeyType keyType)
      throws Exception {
    removeRMTokenSecretManagerMasterKeyState(keyType);
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * DelegationToken Master Key
   */
  protected abstract void removeRMTokenSecretManagerMasterKeyState(
      KeyType keyType) throws Exception;

  /**
   * Non-blocking API
   * ResourceManager services call this to remove an application from the state
   * store
   * This does not block the dispatcher threads
   * There is no notification of completion for this operation.
   */
  @SuppressWarnings(
      "unchecked")
  public synchronized void removeApplication(RMApp app,
      TransactionState transactionState) {
    ApplicationState appState =
        new ApplicationState(app.getSubmitTime(), app.getStartTime(),
            app.getApplicationSubmissionContext(), app.getUser());
    for (RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      Credentials credentials = getCredentialsFromAppAttempt(appAttempt);
      ApplicationAttemptState attemptState =
          new ApplicationAttemptState(appAttempt.getAppAttemptId(),
              appAttempt.getMasterContainer(), credentials,
              appAttempt.getStartTime());
      appState.attempts.put(attemptState.getAttemptId(), attemptState);
    }

    dispatcher.getEventHandler()
        .handle(new RMStateStoreRemoveAppEvent(appState, transactionState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of an
   * application and its attempts
   */
  protected abstract void removeApplicationStateInternal(
      ApplicationState appState) throws Exception;

  // TODO: This should eventually become cluster-Id + "AM_RM_TOKEN_SERVICE". See
  // YARN-1779
  public static final Text AM_RM_TOKEN_SERVICE =
      new Text("AM_RM_TOKEN_SERVICE");

  public static final Text AM_CLIENT_TOKEN_MASTER_KEY_NAME =
      new Text("YARN_CLIENT_TOKEN_MASTER_KEY");

  public Credentials getCredentialsFromAppAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = new Credentials();
    Token<AMRMTokenIdentifier> appToken = appAttempt.getAMRMToken();
    if (appToken != null) {
      credentials.addToken(AM_RM_TOKEN_SERVICE, appToken);
    }
    SecretKey clientTokenMasterKey = appAttempt.getClientTokenMasterKey();
    if (clientTokenMasterKey != null) {
      credentials.addSecretKey(AM_CLIENT_TOKEN_MASTER_KEY_NAME,
          clientTokenMasterKey.getEncoded());
    }
    return credentials;
  }

  // Dispatcher related code
  protected void handleStoreEvent(RMStateStoreEvent event) {
    if (event.getType().equals(RMStateStoreEventType.STORE_APP) ||
        event.getType().equals(RMStateStoreEventType.UPDATE_APP)) {
      ApplicationState appState = null;
      if (event.getType().equals(RMStateStoreEventType.STORE_APP)) {
        appState = ((RMStateStoreAppEvent) event).getAppState();
      } else {
        assert event.getType().equals(RMStateStoreEventType.UPDATE_APP);
        appState = ((RMStateUpdateAppEvent) event).getAppState();
      }

      Exception storedException = null;
      ApplicationStateDataPBImpl appStateData =
          (ApplicationStateDataPBImpl) ApplicationStateDataPBImpl
              .newApplicationStateData(appState.getSubmitTime(),
                  appState.getStartTime(), appState.getUser(),
                  appState.getApplicationSubmissionContext(), appState.
                      getState(), appState.getDiagnostics(),
                  appState.getFinishTime(), appState.getUpdatedNodes());

      ApplicationId appId = appState.getApplicationSubmissionContext().
          getApplicationId();

      LOG.info("Storing info for app: " + appId);
      try {
        if (event.getType().equals(RMStateStoreEventType.STORE_APP)) {
          storeApplicationStateInternal(appId, appStateData);
          notifyDoneStoringApplication(appId, storedException, event.
              getTransactionState());
        } else {
          assert event.getType().equals(RMStateStoreEventType.UPDATE_APP);
          updateApplicationStateInternal(appId, appStateData);
          notifyDoneUpdatingApplication(appId, storedException, event.
              getTransactionState());
        }
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        notifyStoreOperationFailed(e, event.getTransactionState());
      }
    } else if (
        event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT) ||
            event.getType().equals(RMStateStoreEventType.UPDATE_APP_ATTEMPT)) {

      ApplicationAttemptState attemptState = null;
      if (event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT)) {
        attemptState = ((RMStateStoreAppAttemptEvent) event).
            getAppAttemptState();
      } else {
        assert event.getType().equals(RMStateStoreEventType.UPDATE_APP_ATTEMPT);
        attemptState = ((RMStateUpdateAppAttemptEvent) event).
            getAppAttemptState();
      }

      Exception storedException = null;
      Credentials credentials = attemptState.getAppAttemptCredentials();
      ByteBuffer appAttemptTokens = null;
      try {
        if (credentials != null) {
          DataOutputBuffer dob = new DataOutputBuffer();
          credentials.writeTokenStorageToStream(dob);
          appAttemptTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        }
        ApplicationAttemptStateDataPBImpl attemptStateData =
            (ApplicationAttemptStateDataPBImpl) ApplicationAttemptStateDataPBImpl
                .
                    newApplicationAttemptStateData(attemptState.getAttemptId(),
                        attemptState.getMasterContainer(), appAttemptTokens,
                        attemptState.getStartTime(), attemptState.getState(),
                        attemptState.getFinalTrackingUrl(),
                        attemptState.getDiagnostics(),
                        attemptState.getFinalApplicationStatus(),
                        attemptState.getRanNodes(),
                        attemptState.getJustFinishedContainers(),
                        attemptState.getProgress(), attemptState.getHost(),
                        attemptState.getRpcPort());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing info for attempt: " + attemptState.getAttemptId());
        }
        if (event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT)) {
          storeApplicationAttemptStateInternal(attemptState.getAttemptId(),
              attemptStateData);
          notifyDoneStoringApplicationAttempt(attemptState.getAttemptId(),
              storedException, event.getTransactionState());
        } else {
          assert event.getType()
              .equals(RMStateStoreEventType.UPDATE_APP_ATTEMPT);
          updateApplicationAttemptStateInternal(attemptState.getAttemptId(),
              attemptStateData);
          notifyDoneUpdatingApplicationAttempt(attemptState.getAttemptId(),
              storedException, event.getTransactionState());
        }
      } catch (Exception e) {
        LOG.error("Error storing appAttempt: " + attemptState.getAttemptId(),
            e);
        notifyStoreOperationFailed(e, event.getTransactionState());
      }
    } else if (event.getType().equals(RMStateStoreEventType.REMOVE_APP)) {
      ApplicationState appState = ((RMStateStoreRemoveAppEvent) event).
          getAppState();
      ApplicationId appId = appState.getAppId();
      LOG.info("Removing info for app: " + appId);
      try {
        removeApplicationStateInternal(appState);
      } catch (Exception e) {
        LOG.error("Error removing app: " + appId, e);
        notifyStoreOperationFailed(e, event.getTransactionState());
      }
    } else {
      LOG.error("Unknown RMStateStoreEvent type: " + event.getType());
    }
  }

  @SuppressWarnings(
      "unchecked")
  /**
   * This method is called to notify the ResourceManager that the store
   * operation has failed.
   * <p>
   * @param failureCause the exception due to which the operation failed
   */ protected void notifyStoreOperationFailed(Exception failureCause,
      TransactionState transactionState) {
    RMFatalEventType type;
    if (failureCause instanceof StoreFencedException) {
      type = RMFatalEventType.STATE_STORE_FENCED;
    } else {
      type = RMFatalEventType.STATE_STORE_OP_FAILED;
    }
    rmDispatcher.getEventHandler()
        .handle(new RMFatalEvent(type, failureCause, transactionState));
  }

  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application that new application is stored in state store
   * <p>
   * @param appId id of the application that has been saved
   * @param storedException the exception that is thrown when storing the
   * application
   */ private void notifyDoneStoringApplication(ApplicationId appId,
      Exception storedException, TransactionState transactionState) {
    rmDispatcher.getEventHandler().handle(
        new RMAppNewSavedEvent(appId, storedException, transactionState));
  }

  @SuppressWarnings("unchecked")
  private void notifyDoneUpdatingApplication(ApplicationId appId,
      Exception storedException, TransactionState transactionState) {
    rmDispatcher.getEventHandler().handle(
        new RMAppUpdateSavedEvent(appId, storedException, transactionState));
  }

  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application attempt that new attempt is stored in state store
   * <p>
   * @param appAttempt attempt that has been saved
   */ private void notifyDoneStoringApplicationAttempt(
      ApplicationAttemptId attemptId, Exception storedException,
      TransactionState transactionState) {
    rmDispatcher.getEventHandler().handle(
        new RMAppAttemptNewSavedEvent(attemptId, storedException,
            transactionState));
  }

  @SuppressWarnings("unchecked")
  private void notifyDoneUpdatingApplicationAttempt(
      ApplicationAttemptId attemptId, Exception updatedException,
      TransactionState transactionState) {
    rmDispatcher.getEventHandler().handle(
        new RMAppAttemptUpdateSavedEvent(attemptId, updatedException,
            transactionState));

  }

  /**
   * EventHandler implementation which forward events to the FSRMStateStore
   * This hides the EventHandle methods of the store from its public interface
   */
  private final class ForwardingEventHandler
      implements EventHandler<RMStateStoreEvent> {

    @Override
    public void handle(RMStateStoreEvent event) {
      handleStoreEvent(event);
    }
  }
}
