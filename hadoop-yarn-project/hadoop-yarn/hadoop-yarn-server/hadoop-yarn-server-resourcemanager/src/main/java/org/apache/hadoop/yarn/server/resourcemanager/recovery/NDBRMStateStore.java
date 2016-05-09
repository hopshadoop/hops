/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import com.google.protobuf.ByteString;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustFinishedContainer;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.appmasterrpc.HeartBeatRPC;
import io.hops.metadata.yarn.entity.rmstatestore.UpdatedNode;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationKey;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationToken;
import io.hops.metadata.yarn.entity.rmstatestore.RanNode;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RMStateVersionProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.RMStateVersionPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

/**
 * MySQL Cluster implementation of the RMStateStore abstract class.
 */
public class NDBRMStateStore extends RMStateStore {

  //NDB id of version. We only need one row as version is unique.
  public static final int RMSTATEVERSION_ID = 0;
  public static final int SEQNUMBER_ID = 0;
  protected static final RMStateVersion CURRENT_VERSION_INFO = RMStateVersion.
      newInstance(1, 0);
  public static final Log LOG = LogFactory.getLog(NDBRMStateStore.class);

  @Override
  protected void initInternal(Configuration conf) throws Exception {
  }

  @Override
  protected void startInternal() throws Exception {
  }

  @Override
  protected void closeInternal() throws Exception {
  }

  @Override
  protected synchronized RMStateVersion loadVersion() throws Exception {
    byte[] protoFound =
        RMUtilities.getRMStateVersionBinaryLightweight(RMSTATEVERSION_ID);
    RMStateVersion versionFound = null;
    if (protoFound != null) {
      versionFound =
          new RMStateVersionPBImpl(RMStateVersionProto.parseFrom(protoFound));
    }
    return versionFound;
  }

  @Override
  protected synchronized void storeVersion() throws Exception {
    byte[] store = ((RMStateVersionPBImpl) CURRENT_VERSION_INFO).getProto().
        toByteArray();
    RMUtilities.setRMStateVersionLightweight(store);
  }

  /**
   * Helper method for testing.
   *
   * @param version
   * @throws Exception
   */
  protected synchronized void storeVersion(RMStateVersionPBImpl version)
      throws Exception {
    byte[] store = version.getProto().toByteArray();
    RMUtilities.setRMStateVersionLightweight(store);
  }

  @Override
  protected RMStateVersion getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public RMState loadState(final RMContext rmContext) throws Exception {
    final RMState rmState = new RMState();

    LightWeightRequestHandler loadStateHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException, IOException {
                connector.beginTransaction();
                connector.readLock();
                // recover DelegationTokenSecretManager
                loadRMDTSecretManagerState(rmState);
                connector.flush();
                // recover RM applications
                loadRMAppState(rmState);
                connector.flush();
                loadNMTokenSecretMamagerCurrentKey(rmState);
                connector.flush();
                loadAppSchedulingInfos(rmState);
                connector.flush();
                loadAllocateResponses(rmState, rmContext);
                connector.flush();
                loadRPCs(rmState);
                connector.flush();
                loadPendingEvents(rmState);
                connector.flush();
                loadSchedulerApplications(rmState);
                connector.flush();
                loadFiCaSchedulerNodes(rmState);
                connector.flush();
                loadLaunchedContainers(rmState);
                connector.flush();
                loadSchedulingOpportunities(rmState);
                connector.flush();
                loadLastScheduleddContainers(rmState);
                connector.flush();
                loadRereservations(rmState);
                connector.flush();
                loadReservedContainers(rmState);
                connector.flush();
                loadResourceRequests(rmState);
                connector.flush();
                loadBlackLists(rmState);
                connector.flush();
                loadAllQueueMetrics(rmState);
                connector.flush();
                loadNodeHeartBeatResponses(rmState);
                connector.flush();
                loadContainersToClean(rmState);
                connector.flush();
                loadFinishedApplications(rmState);
                connector.flush();
                loadNodesResources(rmState);
                connector.flush();
                loadAllContainers(rmState);
                connector.flush();
                loadAllRMContainers(rmState);
                connector.flush();
                loadAllRMContextActiveNodes(rmState);
                connector.flush();
                loadAllRMNodes(rmState);
                connector.flush();
                loadAllRMNodesNextHeartbeat(rmState);
                connector.flush();
                loadAllNodes(rmState);
                connector.flush();
                loadRMContextInactiveNodes(rmState);
                connector.flush();
                loadAllUpdatedContainerInfos(rmState);
                connector.flush();
                loadAllContainerStatus(rmState);
                connector.flush();
                loadAllJustLaunchedContainers(rmState);
                connector.flush();
                LOG.info("loaded rmState");
                connector.commit();
                return null;
              }
            };
    loadStateHandler.handle();
    return rmState;
  }

  private synchronized void loadRMDTSecretManagerState(RMState rmState) 
          throws IOException {
    loadRMDelegationKeyState(rmState);
    loadRMSequentialNumberState(rmState);
    loadRMDelegationTokenState(rmState);
  }

  @Override
  protected synchronized void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception {
    setApplicationState(appId, appStateData);
  }

  @Override
  protected synchronized void updateApplicationStateInternal(
      ApplicationId appId, ApplicationStateDataPBImpl appStateData)
      throws Exception {
    setApplicationState(appId, appStateData);
  }

  @Override
  protected synchronized void storeApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception {
    setApplicationAttemptState(attemptId, attemptStateData);
  }

  @Override
  protected synchronized void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception {
    setApplicationAttemptState(attemptId, attemptStateData);
  }

  //-------------- RMDTSecretManagerState --------------
  @Override
  protected synchronized void storeRMDelegationTokenAndSequenceNumberState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception {
    RMUtilities.setTokenAndSequenceNumber(rmDTIdentifier, renewDate,
        latestSequenceNumber);
  }

  @Override
  protected synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    int seqNumber = rmDTIdentifier.getSequenceNumber();
    RMUtilities.removeDelegationToken(seqNumber);
  }

  @Override
  protected synchronized void updateRMDelegationTokenAndSequenceNumberInternal(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception {
    RMUtilities.setTokenAndSequenceNumber(rmDTIdentifier, renewDate,
        latestSequenceNumber);
  }

  @Override
  protected synchronized void storeRMDTMasterKeyState(
      org.apache.hadoop.security.token.delegation.DelegationKey delegationKey)
      throws Exception {
    RMUtilities.setRMDTMasterKeyState(delegationKey);
  }

  @Override
  protected synchronized void removeRMDTMasterKeyState(
      org.apache.hadoop.security.token.delegation.DelegationKey delegationKey)
      throws Exception {
    int key = delegationKey.getKeyId();
    LOG.info("HOP :: Remove DelegationKey=" + key);
    RMUtilities.removeRMDTMasterKey(key);
  }

  @Override
  protected synchronized void storeRMTokenSecretManagerMasterKeyState(
      MasterKey key, KeyType keyType) throws IOException {
    RMUtilities.setRMTokenSecretManagerMasterKeyState(key, keyType);
  }

  @Override
  protected synchronized void removeRMTokenSecretManagerMasterKeyState(
      KeyType keyType) throws IOException {
    RMUtilities.removeRMTokenSecretManagerMasterKeyState(keyType);
  }

  @Override
  protected synchronized void removeApplicationStateInternal(
      ApplicationState appState) throws Exception {
    String appId = appState.getAppId().toString();

    //Get ApplicationAttemptIds for this 
    List<String> attemptsToRemove = new ArrayList<String>();
    for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      attemptsToRemove.add(attemptId.toString());
    }
    //Delete applicationstate and attempts from ndb
    RMUtilities.removeApplicationStateAndAttempts(appId, attemptsToRemove);

  }

  /**
   * Retrieve ApplicationIds and their ApplicationAttemptIds from NDB.
   *
   * @param rmState
   * @throws Exception
   */
  private synchronized void loadRMAppState(RMState rmState) 
          throws IOException  {
    //Retrieve all applicationIds from NDB
    List<io.hops.metadata.yarn.entity.rmstatestore.ApplicationState> appStates =
        RMUtilities.getApplicationStates();
    Map<String,List<UpdatedNode>> updatedNodeLists = RMUtilities.
            getAllUpdatedNodes();
    if (appStates != null) {
      for (io.hops.metadata.yarn.entity.rmstatestore.ApplicationState hopAppState : appStates) {
       ApplicationId appId = ConverterUtils.toApplicationId(hopAppState.
            getApplicationid());
                
        List<NodeId> updatedNodes = new ArrayList<NodeId>();
        List<UpdatedNode> unl = updatedNodeLists.get(hopAppState.
                getApplicationid());
        if (unl != null) {
          for (UpdatedNode updatedNode : unl) {
            updatedNodes.add(ConverterUtils.toNodeId(updatedNode.getNodeId()));
          }
        }

        ApplicationStateDataPBImpl appStateData =
            new ApplicationStateDataPBImpl(ApplicationStateDataProto.
                parseFrom(hopAppState.getAppstate()));
        ApplicationState appState = new ApplicationState(appStateData.
            getSubmitTime(), appStateData.getStartTime(),
            appStateData.getApplicationSubmissionContext(),
            appStateData.getUser(), appStateData.getState(),
            appStateData.getDiagnostics(), appStateData.getFinishTime(),
            appStateData.getStateBeforeKilling(),
            updatedNodes);
        LOG.debug("loadRMAppState for app " + appState.getAppId() + " state " +
            appState.getState());
        if (!appId.equals(appState.context.getApplicationId())) {
          throw new YarnRuntimeException(
              "The applicationId string representation is different from the application id");
        }
        rmState.appState.put(appId, appState);
        loadApplicationAttemptState(appState, appId);
      }
    }
  }

  private void loadNMTokenSecretMamagerCurrentKey(RMState rMState)
      throws IOException {
    rMState.secretMamagerKeys = RMUtilities.getSecretMamagerKeys();
  }

  private void loadAllocateResponses(RMState rmState, RMContext rmContext) throws IOException {
    rmState.allocateResponses = RMUtilities.getAllocateResponses(rmContext);
  }
   
  private void loadRPCs(RMState rmState) throws IOException {
    rmState.appMasterRPCs = RMUtilities.getAppMasterRPCs();
    rmState.heartBeatRPCs = RMUtilities.getHeartBeatRPCs();
    rmState.allocateRPCs = RMUtilities.getAllocateRPCs();
  }
  
  private void loadPendingEvents(RMState rmState) throws IOException{
    rmState.pendingEvents = RMUtilities.getAllPendingEvents();
  }
  
  private void loadAppSchedulingInfos(RMState rmState) throws IOException {
    List<AppSchedulingInfo> appSchedulingInfosList =
        RMUtilities.getAppSchedulingInfos();
    rmState.appSchedulingInfos = new HashMap<String, Map<String,AppSchedulingInfo>>();
    for (AppSchedulingInfo info : appSchedulingInfosList) {
      if(rmState.appSchedulingInfos.get(info.getAppId())==null){
        rmState.appSchedulingInfos.put(info.getAppId(), new HashMap<String, AppSchedulingInfo>());
      }
      rmState.appSchedulingInfos.get(info.getAppId()).put(info.getSchedulerAppId(), info);
    }
  }
  
  private void loadSchedulerApplications(RMState rmState) throws IOException {
    rmState.schedulerApplications = RMUtilities.getSchedulerApplications();
  }
  
  private void loadFiCaSchedulerNodes(RMState rmState) throws IOException {
    rmState.fiCaSchedulerNodes = RMUtilities.getAllFiCaSchedulerNodes();
  }
  
  private void loadLaunchedContainers(RMState rmState) throws IOException {
    rmState.launchedContainers = RMUtilities.getAllLaunchedContainers();
  }
    
  private void loadSchedulingOpportunities(RMState rmState) throws IOException {
    rmState.schedulingOpportunities = RMUtilities.getAllSchedulingOpportunities();
  }

  private void loadLastScheduleddContainers(RMState rmState) throws IOException {
    rmState.lastScheduledContainers = RMUtilities.getAllLastScheduledContainers();
  }

  private void loadRereservations(RMState rmState) throws IOException {
    rmState.reReservations = RMUtilities.getAllRereservations();
  }

  private void loadReservedContainers(RMState rmState) throws IOException {
    rmState.reservedContainers = RMUtilities.getAllReservedContainers();
  }
  
  private void loadResourceRequests(RMState rmState) throws IOException {
    rmState.resourceRequests = RMUtilities.getAllResourceRequests();
  }
  
  private void loadBlackLists(RMState rmState) throws IOException {
    rmState.blackLists = RMUtilities.getAllBlackLists();
  }
  
  private void loadAllQueueMetrics(RMState rmState) throws IOException {
    rmState.allQueueMetrics = RMUtilities.getAllQueueMetrics();
  }
  
  private void loadCSLeafQueuesPendingApps(RMState rmState) throws IOException{
    rmState.csLeafQueuesPendingApps = RMUtilities.getCSLeafQueuesPendingApps();
  }
  
  private void loadNodeHeartBeatResponses(RMState rmState) throws IOException {
    Map<String, NodeHBResponse> entryMap = RMUtilities.
        getAllNodeHeartBeatResponse();
    rmState.nodeHeartBeatResponses =
        new HashMap<String, NodeHeartbeatResponse>(entryMap.size());
    for (String key : entryMap.keySet()) {
      rmState.nodeHeartBeatResponses.put(key, new NodeHeartbeatResponsePBImpl(
          YarnServerCommonServiceProtos.NodeHeartbeatResponseProto.
              parseFrom(entryMap.get(key).getResponse())));
    }
  }
  
  private void loadContainersToClean(RMState rmState) throws IOException {
    Map<String, Set<ContainerId>> entryMap = RMUtilities.
        getAllContainersToClean();
    rmState.containersToClean =
        new HashMap<String, Set<org.apache.hadoop.yarn.api.records.ContainerId>>(
            entryMap.
                size());
    for (String key : entryMap.keySet()) {
      Set set = new HashSet<org.apache.hadoop.yarn.api.records.ContainerId>(
          entryMap.get(key).size());
      rmState.containersToClean.put(key, set);
      for (ContainerId hop : entryMap.get(key)) {
        set.add(ConverterUtils.toContainerId(hop.getContainerId()));
      }
    }
  }
  
  private void loadFinishedApplications(RMState rmState) throws IOException {
    Map<String, List<FinishedApplications>> entryMap = RMUtilities.
        getAllFinishedApplications();
    rmState.finishedApplications =
        new HashMap<String, List<ApplicationId>>(entryMap.size());
    for (String key : entryMap.keySet()) {
      List<ApplicationId> list = new ArrayList<ApplicationId>(entryMap.get(key).
          size());
      rmState.finishedApplications.put(key, list);
      for (FinishedApplications hop : entryMap.get(key)) {
        list.add(ConverterUtils.toApplicationId(hop.getApplicationId()));
      }
    }
  }
  
  private void loadNodesResources(RMState rmState) throws IOException {
    rmState.nodesResources = RMUtilities.getAllNodesResources();
  }
  
  private void loadAllContainers(RMState rmState) throws IOException {
    rmState.allContainers = RMUtilities.getAllContainers();
  }
  
  private void loadAllRMContainers(RMState rmState) throws IOException {
    rmState.allRMContainers = RMUtilities.getAllRMContainers();
  }
  
  private void loadAllRMContextActiveNodes(RMState rmState) throws IOException {
    rmState.allRMContextActiveNodes = RMUtilities.getAllRMContextActiveNodes();
  }
  
  private void loadAllRMNodes(RMState rmState) throws IOException {
    rmState.allRMNodes = RMUtilities.getAllRMNodes();
  }

  private void loadAllRMNodesNextHeartbeat(RMState rmState) throws IOException {
    rmState.allRMNodeNextHeartbeats = RMUtilities.getAllNextHeartbeats();
  }

  private void loadAllNodes(RMState rmState) throws IOException {
    rmState.allNodes = RMUtilities.getAllNodes();
  }
  
  private void loadRMContextInactiveNodes(RMState rmState) throws IOException {
    rmState.rmContextInactiveNodes = RMUtilities.getAllRMContextInactiveNodes();
  }
  
  private void loadAllUpdatedContainerInfos(RMState rmState)
      throws IOException {
    rmState.allUpdatedContainerInfos = RMUtilities.
        getAllUpdatedContainerInfos();
  }
  
  private void loadAllContainerStatus(RMState rmState) throws IOException {
    rmState.allContainerStatus = RMUtilities.getAllContainerStatus();
  }
  
  private void loadAllJustLaunchedContainers(RMState rmState)
      throws IOException {
    rmState.allJustLaunchedContainers = RMUtilities.
        getAllJustLaunchedContainers();
  }
  
  private void loadRMDelegationKeyState(RMState rmState) throws IOException {
    //Retrieve all DelegationKeys from NDB
    List<DelegationKey> delKeys = RMUtilities.getDelegationKeys();
    if (delKeys != null) {
      for (DelegationKey hopDelKey : delKeys) {

        ByteArrayInputStream is = new ByteArrayInputStream(hopDelKey.
            getDelegationkey());
        DataInputStream fsIn = new DataInputStream(is);

        try {
          org.apache.hadoop.security.token.delegation.DelegationKey key =
              new org.apache.hadoop.security.token.delegation.DelegationKey();
          key.readFields(fsIn);
          rmState.rmSecretManagerState.masterKeyState.add(key);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Retrieve Sequential Number from NDB.
   *
   * @param rmState
   * @throws Exception
   */
  private void loadRMSequentialNumberState(RMState rmState) throws IOException {
    Integer seqNumber = RMUtilities.getRMSequentialNumber(SEQNUMBER_ID);
    if (seqNumber != null) {
      rmState.rmSecretManagerState.dtSequenceNumber = seqNumber;
    }
  }

  /**
   * Retrieve Delegation Tokens from NDB.
   *
   * @param rmState
   * @throws Exception
   */
  private void loadRMDelegationTokenState(RMState rmState) throws IOException {
    //Retrieve all DelegatioTokenIds from NDB
    List<DelegationToken> delTokens = RMUtilities.getDelegationTokens();
    if (delTokens != null) {
      for (DelegationToken hopDelToken : delTokens) {

        ByteArrayInputStream is = new ByteArrayInputStream(hopDelToken.
            getRmdtidentifier());
        DataInputStream fsIn = new DataInputStream(is);

        try {
          RMDelegationTokenIdentifier identifier =
              new RMDelegationTokenIdentifier();
          identifier.readFields(fsIn);
          long renewDate = fsIn.readLong();
          rmState.rmSecretManagerState.delegationTokenState
              .put(identifier, renewDate);
        } finally {
          is.close();
        }
      }
    }
  }

  private Map<String, List<io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState>>
      allHopApplicationAttemptStates;
  private Map<String, List<RanNode>> ranNodes;
  private Map<String, List<JustFinishedContainer>> justFinishedContainers;
  /**
   * Load ApplicationAttemptId for particular ApplicationState
   *
   * @param appState
   * @param appId
   * @throws Exception
   */
  private void loadApplicationAttemptState(ApplicationState appState,
      ApplicationId appId) throws IOException {
    if (allHopApplicationAttemptStates == null) {
      allHopApplicationAttemptStates = RMUtilities.
          getAllApplicationAttemptStates();
    }
    if(ranNodes == null){
      ranNodes = RMUtilities.getAllRanNodes();
    }
    if(justFinishedContainers==null){
      justFinishedContainers = RMUtilities.getAllJustFinishedContainers();
    }
    
    LOG.debug("loadApplicationAttemptState for app " + appState.getAppId() +
        " state " + appState.getState());
    List<io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState>
        attempts = allHopApplicationAttemptStates.get(appId.toString());
    if (attempts != null) {
      for (io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState attempt : attempts) {
        String attemptIDStr = attempt.getApplicationattemptid();
        if (attemptIDStr
            .startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
          byte[] attemptData = attempt.getApplicationattemptstate();

          ApplicationAttemptId attemptId = ConverterUtils.
              toApplicationAttemptId(attemptIDStr);
          ApplicationAttemptStateDataPBImpl attemptStateData =
              new ApplicationAttemptStateDataPBImpl(
                  ApplicationAttemptStateDataProto.
                      parseFrom(attemptData));
          Credentials credentials = null;
          if (attemptStateData.getAppAttemptTokens() != null) {
            credentials = new Credentials();
            DataInputByteBuffer dibb = new DataInputByteBuffer();
            dibb.reset(attemptStateData.getAppAttemptTokens());
            credentials.readTokenStorageStream(dibb);
          }
          
          Set<NodeId> attemptRanNodes = new HashSet<NodeId>();
          List<RanNode> ranNodeList = ranNodes.get(attemptId.toString());
          if (ranNodeList != null) {
            for (RanNode node : ranNodeList) {
              attemptRanNodes.add(ConverterUtils.toNodeId(node.getNodeId()));
            }
          }
          
          List<ContainerStatus> attemptJustFinishedContainers
                  = new ArrayList<ContainerStatus>();
          List<JustFinishedContainer> justFinishedContainersList
                  = justFinishedContainers.get(attemptId.toString());
          if (justFinishedContainersList != null) {
            for (JustFinishedContainer container : justFinishedContainersList) {
              attemptJustFinishedContainers.add(new ContainerStatusPBImpl(
                      YarnProtos.ContainerStatusProto.
                      parseFrom(container.getContainer())));
            }
          }
          
          ApplicationAttemptState attemptState =
              new ApplicationAttemptState(attemptId,
                  attemptStateData.getMasterContainer(), credentials,
                  attemptStateData.getStartTime(), attemptStateData.getState(),
                  attemptStateData.getFinalTrackingUrl(),
                  attemptStateData.getDiagnostics(),
                  attemptStateData.getFinalApplicationStatus(),
                  attemptStateData.getProgress(), attemptStateData.getHost(),
                  attemptStateData.getRpcPort(), 
                  attemptRanNodes,
                  attemptJustFinishedContainers);

          appState.attempts.put(attemptState.getAttemptId(), attemptState);
        }
      }
      LOG.debug("Done Loading applications from NDB state store");
    }
  }

  /**
   * Used byte store/updateApplicationStateInternal. In NDB store/update is
   * the same thing as we use the savePeristent() clusterj method.
   *
   * @param appId
   * @param appStateData
   * @throws IOException
   */
  private void setApplicationState(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws IOException {
    String appIdString = appId.toString();
    byte[] appStateDataBytes = appStateData.getProto().toByteArray();
    RMUtilities.setApplicationState(appIdString, appStateDataBytes);
  }

  /**
   * Used by store/updateApplicationAttemptStateInternal. In NDB store/update
   * is the same thing as we use the savePeristent() clusterj method.
   *
   * @param attemptId
   * @param attemptStateData
   */
  private void setApplicationAttemptState(ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws IOException {
    String appIdStr = attemptId.getApplicationId().toString();
    String appAttemptIdStr = attemptId.toString();
    byte[] attemptIdByteArray = attemptStateData.getProto().toByteArray();
    RMUtilities
        .setApplicationAttemptId(appIdStr, appAttemptIdStr, attemptIdByteArray);
  }
}
