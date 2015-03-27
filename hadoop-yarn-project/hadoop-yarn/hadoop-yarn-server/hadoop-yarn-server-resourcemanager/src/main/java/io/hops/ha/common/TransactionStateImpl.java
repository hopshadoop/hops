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
package io.hops.ha.common;

import io.hops.common.GlobalThreadPool;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService.AllocateResponseLock;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.LOG;

public class TransactionStateImpl extends TransactionState {

  //Type of TransactionImpl to know which finishRPC to call
  //In future implementation this will be removed as a single finishRPC will exist
  private final TransactionType type;
  private org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode
      rmNodeToUpdate;
  private final Map<String, RMNodeInfo> rmNodeInfos =
      new HashMap<String, RMNodeInfo>();
  private final RMContextInfo rmcontextInfo = new RMContextInfo();
  private final Map<String, FiCaSchedulerNodeInfoToUpdate>
      ficaSchedulerNodeInfoToUpdate =
      new HashMap<String, FiCaSchedulerNodeInfoToUpdate>();
  private final Map<String, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>
      ficaSchedulerNodeInfoToAdd =
      new HashMap<String, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>();
  private final Map<String, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>
      ficaSchedulerNodeInfoToRemove =
      new HashMap<String, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>();
  private final SchedulerApplicationInfo schedulerApplicationInfo =
      new SchedulerApplicationInfo();
  private final FairSchedulerNodeInfo fairschedulerNodeInfo =
      new FairSchedulerNodeInfo();
  private org.apache.hadoop.yarn.api.records.Resource clusterResourceToUpdate;
  private org.apache.hadoop.yarn.api.records.Resource usedResourceToUpdate;
  private final Set<RMAppImpl> applicationsToAdd = new HashSet<RMAppImpl>();
  private final List<ApplicationId> applicationsStateToRemove =
      new ArrayList<ApplicationId>();
  private final HashMap<String, RMAppAttempt> appAttempts =
      new HashMap<String, RMAppAttempt>();
  private final HashMap<ApplicationAttemptId, AllocateResponseLock>
      allocateResponsesToAdd =
      new HashMap<ApplicationAttemptId, AllocateResponseLock>();
  private final List<ApplicationAttemptId> allocateResponsesToRemove =
      new ArrayList<ApplicationAttemptId>();
  private final HashMap<String, RMContainerImpl> rmContainersToUpdate =
      new HashMap<String, RMContainerImpl>();
  
  //PersistedEvent to persist for distributed RT
  private final List<PendingEvent> persistedEventsToAdd =
      new ArrayList<PendingEvent>();
  private RMNodeImpl rmNode = null;
  private final List<PendingEvent> persistedEventsToRemove =
      new ArrayList<PendingEvent>();

  //for debug and evaluation
  String rpcType = null;
  NodeId nodeId = null;

  public TransactionStateImpl(int rcpID, TransactionType type) {
    super(rcpID);
    this.type = type;
  }

  public TransactionStateImpl(int rcpID, TransactionType type, String rpcType,
      NodeId nodeId) {
    super(rcpID);
    this.type = type;
    this.rpcType = rpcType;
    this.nodeId = nodeId;
  }
  
  @Override
  void commit() throws IOException {
    GlobalThreadPool.getExecutorService().execute(new RPCFinisher(this));
  }

  public FairSchedulerNodeInfo getFairschedulerNodeInfo() {
    return fairschedulerNodeInfo;
  }

  public void persistFairSchedulerNodeInfo(FSSchedulerNodeDataAccess FSSNodeDA)
      throws StorageException {
    fairschedulerNodeInfo.persist(FSSNodeDA);
  }

  public SchedulerApplicationInfo getSchedulerApplicationInfo() {
    return schedulerApplicationInfo;
  }

  public void persist() throws IOException {
    persitApplicationToAdd();
    persistApplicationStateToRemove();
    persistAppAttempt();
    persistAllocateResponsesToAdd();
    persistAllocateResponsesToRemove();
    persistRMContainerToUpdate();
    persistClusterResourceToUpdate();
    persistUsedResourceToUpdate();
  }

  public void persistSchedulerApplicationInfo(QueueMetricsDataAccess QMDA)
      throws StorageException {
    if (schedulerApplicationInfo != null) {
      schedulerApplicationInfo.persist(QMDA);
    }
  }

  public FiCaSchedulerNodeInfoToUpdate getFicaSchedulerNodeInfoToUpdate(
      String nodeId) {
    FiCaSchedulerNodeInfoToUpdate nodeInfo =
        ficaSchedulerNodeInfoToUpdate.get(nodeId);
    if (nodeInfo == null) {
      nodeInfo = new FiCaSchedulerNodeInfoToUpdate(nodeId);
      ficaSchedulerNodeInfoToUpdate.put(nodeId, nodeInfo);
    }
    return nodeInfo;
  }
  
  public void addFicaSchedulerNodeInfoToAdd(String nodeId,
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    ficaSchedulerNodeInfoToAdd.put(nodeId, node);
  }
  
  public void addFicaSchedulerNodeInfoToRemove(String nodeId,
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    ficaSchedulerNodeInfoToRemove.put(nodeId, node);
  }
  
  public void addApplicationToAdd(RMAppImpl application) {
    applicationsToAdd.add(application);
  }
  
  private void persitApplicationToAdd() throws IOException {
    if (!applicationsToAdd.isEmpty()) {
      ApplicationStateDataAccess DA =
          (ApplicationStateDataAccess) RMStorageFactory
              .getDataAccess(ApplicationStateDataAccess.class);
      List<ApplicationState> newAppStates = new ArrayList<ApplicationState>();
      for (RMAppImpl app : applicationsToAdd) {
        if (!applicationsStateToRemove.remove(app.getApplicationId())) {
          ApplicationStateDataPBImpl appStateData =
              (ApplicationStateDataPBImpl) ApplicationStateDataPBImpl
                  .newApplicationStateData(app.getSubmitTime(),
                      app.getStartTime(), app.getUser(),
                      app.getApplicationSubmissionContext(), app.
                          getState(), app.getDiagnostics().toString(),
                      app.getFinishTime(), app.getUpdatedNodesId());
          byte[] appStateDataBytes = appStateData.getProto().toByteArray();
          LOG.debug("persist app : " + app.getApplicationSubmissionContext().
              getApplicationId() + " with state " + app.getState());
          ApplicationState hop =
              new ApplicationState(app.getApplicationId().toString(),
                  appStateDataBytes, app.getUser(), app.getName(),
                  app.getState().toString());
          newAppStates.add(hop);
        }
      }
      DA.addAll(newAppStates);
    }
  }

  public void addApplicationStateToRemove(ApplicationId appId) {
    applicationsStateToRemove.add(appId);
  }

  private void persistApplicationStateToRemove() throws StorageException {
    if (!applicationsStateToRemove.isEmpty()) {
      ApplicationStateDataAccess DA =
          (ApplicationStateDataAccess) RMStorageFactory
              .getDataAccess(ApplicationStateDataAccess.class);
      List<ApplicationState> appToRemove = new ArrayList<ApplicationState>();
      for (ApplicationId appId : applicationsStateToRemove) {
        LOG.debug("removing app state " + appId.toString());
        appToRemove.add(new ApplicationState(appId.toString()));
      }
      DA.removeAll(appToRemove);
      //TODO remove appattempts
    }
  }
  
  public void addAppAttempt(RMAppAttempt appAttempt) {
    this.appAttempts.put(appAttempt.getAppAttemptId().toString(), appAttempt);
  }

  private void persistAppAttempt() throws IOException {
    if (!appAttempts.isEmpty()) {
      ApplicationAttemptStateDataAccess DA =
          (ApplicationAttemptStateDataAccess) RMStorageFactory.
              getDataAccess(ApplicationAttemptStateDataAccess.class);
      List<ApplicationAttemptState> toAdd =
          new ArrayList<ApplicationAttemptState>();
      for (String appAttemptIdStr : appAttempts.keySet()) {
        RMAppAttempt appAttempt = appAttempts.get(appAttemptIdStr);
        String appIdStr = appAttempt.getAppAttemptId().getApplicationId().
            toString();

        Credentials credentials = appAttempt.getCredentials();
        ByteBuffer appAttemptTokens = null;

        if (credentials != null) {
          DataOutputBuffer dob = new DataOutputBuffer();
          credentials.writeTokenStorageToStream(dob);
          appAttemptTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        }
        ApplicationAttemptStateDataPBImpl attemptStateData =
            (ApplicationAttemptStateDataPBImpl) ApplicationAttemptStateDataPBImpl
                .
                    newApplicationAttemptStateData(appAttempt.getAppAttemptId(),
                        appAttempt.getMasterContainer(), appAttemptTokens,
                        appAttempt.getStartTime(), appAttempt.
                            getState(), appAttempt.getOriginalTrackingUrl(),
                        appAttempt.getDiagnostics(),
                        appAttempt.getFinalApplicationStatus(),
                        appAttempt.getRanNodes(),
                        appAttempt.getJustFinishedContainers(),
                        appAttempt.getProgress(), appAttempt.getHost(),
                        appAttempt.getRpcPort());

        byte[] attemptIdByteArray = attemptStateData.getProto().toByteArray();
        LOG.debug("adding appAttempt : " + appAttempt.getAppAttemptId() +
            " with state " + appAttempt.getState());
        toAdd.add(new ApplicationAttemptState(appIdStr, appAttemptIdStr,
            attemptIdByteArray, appAttempt.
            getHost(), appAttempt.getRpcPort(), appAttemptTokens, appAttempt.
            getTrackingUrl()));
      }
      DA.addAll(toAdd);
    }
  }
  
  
  public void addAllocateResponse(ApplicationAttemptId id,
      AllocateResponseLock allocateResponse) {
    this.allocateResponsesToAdd.put(id, allocateResponse);
  }


  private void persistAllocateResponsesToAdd() throws IOException {
    if (!allocateResponsesToAdd.isEmpty()) {
      AllocateResponseDataAccess da =
          (AllocateResponseDataAccess) RMStorageFactory
              .getDataAccess(AllocateResponseDataAccess.class);
      List<AllocateResponse> toAdd = new ArrayList<AllocateResponse>();
      for (ApplicationAttemptId id : allocateResponsesToAdd.keySet()) {
        if (!allocateResponsesToRemove.remove(id)) {
          AllocateResponseLock lock = allocateResponsesToAdd.get(id);
          synchronized (lock) {
            AllocateResponsePBImpl lastResponse = (AllocateResponsePBImpl) lock.
                getAllocateResponse();
            if (lastResponse != null) {
              toAdd.add(new AllocateResponse(id.toString(), lastResponse.
                  getProto().toByteArray()));
            }
          }
        }
      }
      da.addAll(toAdd);
    }
  }
  
  public void removeAllocateResponse(ApplicationAttemptId id) {
    this.allocateResponsesToRemove.add(id);
  }

  private void persistAllocateResponsesToRemove() throws IOException {
    if (!allocateResponsesToRemove.isEmpty()) {
      AllocateResponseDataAccess da =
          (AllocateResponseDataAccess) RMStorageFactory
              .getDataAccess(AllocateResponseDataAccess.class);
      List<AllocateResponse> toRemove = new ArrayList<AllocateResponse>();
      for (ApplicationAttemptId id : allocateResponsesToRemove) {
        toRemove.add(new AllocateResponse(id.toString()));
      }
      da.removeAll(toRemove);
    }
  }
  
  public void addRMContainerToUpdate(RMContainerImpl rmContainer) {
    rmContainersToUpdate
        .put(rmContainer.getContainer().getId().toString(), rmContainer);
  }

  private void persistRMContainerToUpdate() throws StorageException {
    if (!rmContainersToUpdate.isEmpty()) {
      RMContainerDataAccess rmcontainerDA =
          (RMContainerDataAccess) RMStorageFactory
              .getDataAccess(RMContainerDataAccess.class);
      ArrayList<RMContainer> rmcontainerToUpdate = new ArrayList<RMContainer>();
      for (String containerId : rmContainersToUpdate.keySet()) {
        RMContainerImpl rmContainer = rmContainersToUpdate.get(containerId);
        rmcontainerToUpdate.add(
            new RMContainer(rmContainer.getContainer().getId().toString(),
                rmContainer.getApplicationAttemptId().toString(),
                rmContainer.getNodeId().toString(), rmContainer.getUser(),
                //              rmContainer.getReservedNode(),
                //              Integer.MIN_VALUE,
                rmContainer.getStartTime(), rmContainer.getFinishTime(),
                rmContainer.getState().toString(),
                rmContainer.getContainerState().toString(),
                rmContainer.getContainerExitStatus()));
      }
      rmcontainerDA.addAll(rmcontainerToUpdate);
    }
  }

  public void persistFicaSchedulerNodeInfo(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    persistFiCaSchedulerNodeToAdd(resourceDA, ficaNodeDA, rmcontainerDA,
        launchedContainersDA);

    for (FiCaSchedulerNodeInfoToUpdate nodeInfo : ficaSchedulerNodeInfoToUpdate
        .values()) {
      nodeInfo
          .persist(resourceDA, ficaNodeDA, rmcontainerDA, launchedContainersDA);
    }
    persistFiCaSchedulerNodeToRemove(resourceDA, ficaNodeDA, rmcontainerDA);
  }

  public RMContextInfo getRMContextInfo() {
    return rmcontextInfo;
  }

  public void persistRmcontextInfo(RMNodeDataAccess rmnodeDA,
      ResourceDataAccess resourceDA, NodeDataAccess nodeDA,
      RMContextInactiveNodesDataAccess rmctxinactivenodesDA)
      throws StorageException {
    rmcontextInfo.persist(rmnodeDA, resourceDA, nodeDA, rmctxinactivenodesDA);
  }


  public void persistRMNodeToUpdate(RMNodeDataAccess rmnodeDA)
      throws StorageException {
    if (rmNodeToUpdate != null) {
      //Persist RMNode

      //getOverCommitTimeout could throw exception, probably due to bug.
      //Check hop documentation for details.
      int overcommittimeout = Integer.MIN_VALUE;
      try {
        overcommittimeout =
            rmNodeToUpdate.getResourceOption().getOverCommitTimeout();
      } catch (Exception e) {
      }
      RMNode hopRMNode = new RMNode(rmNodeToUpdate.getNodeID().toString(),
          rmNodeToUpdate.getHostName(), rmNodeToUpdate.getCommandPort(),
          rmNodeToUpdate.getHttpPort(), rmNodeToUpdate.getNodeAddress(),
          rmNodeToUpdate.getHttpAddress(), rmNodeToUpdate.getHealthReport(),
          rmNodeToUpdate.getLastHealthReportTime(),
          ((RMNodeImpl) rmNodeToUpdate).getCurrentState(),
          rmNodeToUpdate.getNodeManagerVersion(), overcommittimeout,
          ((RMNodeImpl) rmNodeToUpdate).getUpdatedContainerInfoId());

      rmnodeDA.add(hopRMNode);
    }
  }

  public void toUpdateRMNode(
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmnodeToAdd) {
    this.rmNodeToUpdate = rmnodeToAdd;
  }

  public RMNodeInfo getRMNodeInfo(String rmNodeId) {
    RMNodeInfo result = rmNodeInfos.get(rmNodeId);
    if (result == null) {
      result = new RMNodeInfo(rmNodeId);
      rmNodeInfos.put(rmNodeId, result);
    }
    return result;
  }

  public void persistRMNodeInfo(NodeHBResponseDataAccess hbDA,
      ContainerIdToCleanDataAccess cidToCleanDA,
      JustLaunchedContainersDataAccess justLaunchedContainersDA,
      UpdatedContainerInfoDataAccess updatedContainerInfoDA,
      FinishedApplicationsDataAccess faDA, ContainerStatusDataAccess csDA)
      throws StorageException {
    if (rmNodeInfos != null) {
      for (RMNodeInfo rmNodeInfo : rmNodeInfos.values()) {
        rmNodeInfo.persist(hbDA, cidToCleanDA, justLaunchedContainersDA,
            updatedContainerInfoDA, faDA, csDA);
      }
    }
  }
  
  public void updateUsedResource(
      org.apache.hadoop.yarn.api.records.Resource usedResource) {
    this.usedResourceToUpdate = usedResource;
  }
  
  private void persistUsedResourceToUpdate() throws StorageException {
    if (usedResourceToUpdate != null) {
      ResourceDataAccess rDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      rDA.add(new Resource("cluster", Resource.CLUSTER, Resource.USED,
          usedResourceToUpdate.getMemory(),
          usedResourceToUpdate.getVirtualCores()));
    }
  }
  
  public void updateClusterResource(
      org.apache.hadoop.yarn.api.records.Resource clusterResource) {
    this.clusterResourceToUpdate = clusterResource;
  }
  
  private void persistClusterResourceToUpdate() throws StorageException {
    if (clusterResourceToUpdate != null) {
      ResourceDataAccess rDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      rDA.add(new Resource("cluster", Resource.CLUSTER, Resource.AVAILABLE,
          clusterResourceToUpdate.getMemory(),
          clusterResourceToUpdate.getVirtualCores()));
    }
  }
  
  private void persistFiCaSchedulerNodeToRemove(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA) throws StorageException {
    if (!ficaSchedulerNodeInfoToRemove.isEmpty()) {
      ArrayList<FiCaSchedulerNode> toRemoveFiCaSchedulerNodes =
          new ArrayList<FiCaSchedulerNode>();
      ArrayList<Resource> toRemoveResources = new ArrayList<Resource>();
      ArrayList<RMContainer> rmcontainerToRemove = new ArrayList<RMContainer>();
      ArrayList<LaunchedContainers> toRemoveLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String nodeId : ficaSchedulerNodeInfoToRemove.keySet()) {
        LOG.debug("remove ficaschedulernodes " + nodeId);
        toRemoveFiCaSchedulerNodes.add(new FiCaSchedulerNode(nodeId,
            ficaSchedulerNodeInfoToRemove.get(nodeId).getNodeName(),
            ficaSchedulerNodeInfoToRemove.get(nodeId).getNumContainers()));
        //Remove Resources
        //Set memory and virtualcores to zero as we do not need
        //these values during remove anyway.
        toRemoveResources.add(new Resource(nodeId, Resource.TOTAL_CAPABILITY,
            Resource.FICASCHEDULERNODE, 0, 0));
        toRemoveResources.add(
            new Resource(nodeId, Resource.AVAILABLE, Resource.FICASCHEDULERNODE,
                0, 0));
        toRemoveResources.add(
            new Resource(nodeId, Resource.USED, Resource.FICASCHEDULERNODE, 0,
                0));
        // Update FiCaSchedulerNode reservedContainer
        org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer
            container =
            ficaSchedulerNodeInfoToRemove.get(nodeId).getReservedContainer();
        if (container != null) {

          rmcontainerToRemove.add(
              new RMContainer(container.getContainerId().toString(),
                  container.getApplicationAttemptId().toString(),
                  container.getNodeId().toString(), container.getUser(),
                  //                  rmContainer.getReservedNode(),
                  //                  Integer.MIN_VALUE,
                  container.getStartTime(), container.getFinishTime(),
                  container.getState().toString(),
                  ((RMContainerImpl) container).getContainerState().toString(),
                  ((RMContainerImpl) container).getContainerExitStatus()));
        }
      }
      resourceDA.removeAll(toRemoveResources);
      ficaNodeDA.removeAll(toRemoveFiCaSchedulerNodes);
      rmcontainerDA.removeAll(rmcontainerToRemove);
    }
  }

  public void persistFiCaSchedulerNodeToAdd(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    if (!ficaSchedulerNodeInfoToAdd.isEmpty()) {
      ArrayList<FiCaSchedulerNode> toAddFiCaSchedulerNodes =
          new ArrayList<FiCaSchedulerNode>();
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      ArrayList<RMContainer> rmcontainerToAdd = new ArrayList<RMContainer>();
      ArrayList<LaunchedContainers> toAddLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String nodeId : ficaSchedulerNodeInfoToAdd.keySet()) {
        if (ficaSchedulerNodeInfoToRemove.remove(nodeId) == null) {
          
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
              node = ficaSchedulerNodeInfoToAdd.get(nodeId);
          //Nikos: reservedContainer is not used by fifoScheduler..thats why its null
          toAddFiCaSchedulerNodes.add(
              new FiCaSchedulerNode(nodeId, node.getNodeName(),
                  node.getNumContainers()));
          //Add Resources
          if (node.getTotalResource() != null) {
            toAddResources.add(new Resource(nodeId, Resource.TOTAL_CAPABILITY,
                Resource.FICASCHEDULERNODE, node.getTotalResource().getMemory(),
                node.getTotalResource().getVirtualCores()));
          }
          if (node.getAvailableResource() != null) {
            toAddResources.add(new Resource(nodeId, Resource.AVAILABLE,
                Resource.FICASCHEDULERNODE,
                node.getAvailableResource().getMemory(),
                node.getAvailableResource().getVirtualCores()));
          }
          if (node.getUsedResource() != null) {
            toAddResources.add(
                new Resource(nodeId, Resource.USED, Resource.FICASCHEDULERNODE,
                    node.getUsedResource().getMemory(),
                    node.getUsedResource().getVirtualCores()));
          }
          if (node.getReservedContainer() != null) {
            rmcontainerToAdd.add(new RMContainer(
                node.getReservedContainer().getContainerId().toString(),
                node.getReservedContainer().getApplicationAttemptId()
                    .toString(),
                node.getReservedContainer().getNodeId().toString(),
                node.getReservedContainer().getUser(),
                //                  rmContainer.getReservedNode(),
                //                  Integer.MIN_VALUE,
                node.getReservedContainer().getStartTime(),
                node.getReservedContainer().getFinishTime(),
                node.getReservedContainer().getState().toString(),
                ((RMContainerImpl) node.getReservedContainer())
                    .getContainerState().toString(),
                ((RMContainerImpl) node.getReservedContainer())
                    .getContainerExitStatus()));
          }
          //Add launched containers
          if (node.getRunningContainers() != null) {
            for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer : node
                .getRunningContainers()) {
              LOG.debug("adding ha_launchedcontainers " +
                  node.getNodeID().toString());
              toAddLaunchedContainers.add(
                  new LaunchedContainers(node.getNodeID().toString(),
                      rmContainer.getContainerId().toString(),
                      rmContainer.getContainerId().toString()));
            }
          }
        }
      }
      resourceDA.addAll(toAddResources);
      ficaNodeDA.addAll(toAddFiCaSchedulerNodes);
      rmcontainerDA.addAll(rmcontainerToAdd);
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  public void addPendingEventToAdd(String rmnodeId, byte type, byte status) {
    LOG.debug("HOP :: updatePendingEventToAdd");
    PendingEvent pendingEvent = new PendingEvent(rmnodeId, type, status,
        pendingEventId.getAndIncrement());
    this.persistedEventsToAdd.add(pendingEvent);
    LOG.debug("HOP :: updatePendingEventToAdd, pendingEvent:" + pendingEvent);
  }

  public void addPendingEventToAdd(String rmnodeId, byte type, byte status,
      RMNodeImpl rmNode) {
    addPendingEventToAdd(rmnodeId, type, status);
    this.rmNode = rmNode;
  }

  public RMNodeImpl getRMNode() {
    return this.rmNode;
  }

  /**
   * Remove pending event from DB. In this case, the event id is not needed,
   * hence set to MIN.
   * <p/>
   *
   * @param id
   * @param rmnodeId
   * @param type
   * @param status
   */
  public void addPendingEventToRemove(int id, String rmnodeId, byte type,
      byte status) {
    this.persistedEventsToRemove
        .add(new PendingEvent(rmnodeId, type, status, id));
  }

  public void persistPendingEvents(PendingEventDataAccess persistedEventsDA)
      throws StorageException {
    if (rpcType != null && !this.persistedEventsToAdd.isEmpty()) {
      LOG.debug("persisting " + rpcType + " node " + nodeId);
    }
    if (!this.persistedEventsToRemove.isEmpty()) {
      LOG.debug("hb handled " + persistedEventsToRemove.size());
    }
    persistedEventsDA
        .prepare(this.persistedEventsToAdd, this.persistedEventsToRemove);
  }

  private class RPCFinisher implements Runnable {

    private final TransactionStateImpl ts;

    public RPCFinisher(TransactionStateImpl ts) {
      this.ts = ts;
    }

    public void run() {
      RMUtilities.finishRPC(ts, rpcID);
    }
  }
}
