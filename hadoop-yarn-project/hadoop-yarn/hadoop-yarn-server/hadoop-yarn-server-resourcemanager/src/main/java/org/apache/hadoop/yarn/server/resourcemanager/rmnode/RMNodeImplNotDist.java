/*
 * Copyright 2016 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.AllocationExpirationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;

/**
 *
 * @author gautier
 */
public class RMNodeImplNotDist extends RMNodeImpl {

  private static final Log LOG = LogFactory.getLog(RMNodeImplNotDist.class);

  public RMNodeImplNotDist(NodeId nodeId, RMContext context, String hostName,
          int cmPort, int httpPort, Node node, Resource capability,
          String nodeManagerVersion) {
    super(nodeId, context, hostName, cmPort, httpPort, node, capability,
            nodeManagerVersion);
  }

  protected NodeState statusUpdateWhenHealthyTransitionInternal(
          RMNodeImpl rmNode, RMNodeEvent event) {

    RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

    // Switch the last heartbeatresponse.
    rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();

    NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
    rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
    rmNode.setLastHealthReportTime(
        remoteNodeHealthStatus.getLastHealthReportTime());

    rmNode.setAggregatedContainersUtilization(
        statusEvent.getAggregatedContainersUtilization());
    rmNode.setNodeUtilization(statusEvent.getNodeUtilization());
    NodeState initialState = rmNode.getState();
    boolean isNodeDecommissioning = initialState.equals(NodeState.DECOMMISSIONING);
    if (isNodeDecommissioning) {
      List<ApplicationId> keepAliveApps = statusEvent.getKeepAliveAppIds();
      if (rmNode.runningApplications.isEmpty() && (keepAliveApps == null || keepAliveApps.isEmpty())) {
        deactivateNode(rmNode, NodeState.DECOMMISSIONED);
        return NodeState.DECOMMISSIONED;
      }
    }

    if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
      LOG.
          info("Node " + rmNode.nodeId + " reported UNHEALTHY with details: " + remoteNodeHealthStatus.getHealthReport());
      // if a node in decommissioning receives an unhealthy report,
      // it will stay in decommissioning.
      if (isNodeDecommissioning) {
        return NodeState.DECOMMISSIONING;
      } else {
        reportNodeUnusable(rmNode, NodeState.UNHEALTHY);
        return NodeState.UNHEALTHY;
      }
    }

    ((RMNodeImplNotDist) rmNode).handleContainerStatus(statusEvent.getContainers());
    rmNode.handleReportedIncreasedContainers(
        statusEvent.getNMReportedIncreasedContainers());

    List<LogAggregationReport> logAggregationReportsForApps = statusEvent.getLogAggregationReportsForApps();
    if (logAggregationReportsForApps != null
        && !logAggregationReportsForApps.isEmpty()) {
      rmNode.handleLogAggregationStatus(logAggregationReportsForApps);
    }

    if (rmNode.nextHeartBeat) {
      rmNode.nextHeartBeat = false;
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeUpdateSchedulerEvent(rmNode));
    }

    // Update DTRenewer in secure mode to keep these apps alive. Today this is
    // needed for log-aggregation to finish long after the apps are gone.
    if (UserGroupInformation.isSecurityEnabled()) {
      rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
          statusEvent.getKeepAliveAppIds());
    }

    return initialState;
  }

  @Override
  protected void handleContainerStatus(List<ContainerStatus> containerStatuses) {
    // Filter the map to only obtain just launched containers and finished
    // containers.
    List<ContainerStatus> newlyLaunchedContainers
            = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    List<io.hops.metadata.yarn.entity.ContainerStatus> containerToLog
            = new ArrayList<>();
    int numRemoteRunningContainers = 0;
    for (ContainerStatus remoteContainer : containerStatuses) {
      ContainerId containerId = remoteContainer.getContainerId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
                + "cleanup, no further processing");
        continue;
      }
      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();
      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
                + " belongs to an application that is already killed,"
                + " no further processing");
        continue;
      } else if (!runningApplications.contains(containerAppId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Container " + containerId
              + " is the first container get launched for application "
              + containerAppId);
        }
        handleRunningAppOnNode(this, context, containerAppId, nodeId);
      }

      // Process running containers
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        ++numRemoteRunningContainers;
        if (!launchedContainers.contains(containerId)) {
          // Just launched container. RM knows about it the first time.
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
          // Unregister from containerAllocationExpirer.
          containerAllocationExpirer.unregister(
              new AllocationExpirationInfo(containerId));
        }
      } else {
        // A finished container
        launchedContainers.remove(containerId);
        completedContainers.add(remoteContainer);
        // Unregister from containerAllocationExpirer.
        containerAllocationExpirer.unregister(
            new AllocationExpirationInfo(containerId));
      }
      containerToLog.add(new io.hops.metadata.yarn.entity.ContainerStatus(
              remoteContainer.getContainerId().toString(), remoteContainer.
              getState().name(), remoteContainer.getDiagnostics(),
              remoteContainer.getExitStatus(), nodeId.toString()));
    }
    List<ContainerStatus> lostContainers = findLostContainers(
      numRemoteRunningContainers, containerStatuses);
    for (ContainerStatus lostContainer : lostContainers) {
      containerToLog.add(
        new io.hops.metadata.yarn.entity.ContainerStatus(
          lostContainer.getContainerId().toString(), 
          lostContainer.getState().name(), 
          lostContainer.getDiagnostics(), 
          lostContainer.getExitStatus(), nodeId.toString()));
    }
    completedContainers.addAll(lostContainers);
    
    if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {
      nodeUpdateQueue.add(new UpdatedContainerInfo(newlyLaunchedContainers,
              completedContainers));
    }
    if(!containerToLog.isEmpty() && context.getContainersLogsService() != null){
      context.getContainersLogsService().insertEvent(containerToLog);
    }
  }

  @Override
  public void updateNodeHeartbeatResponseForCleanup(
          NodeHeartbeatResponse response) {
    this.writeLock.lock();

    try {
      response.addAllContainersToCleanup(
              new ArrayList<ContainerId>(this.containersToClean));
      response.addAllApplicationsToCleanup(this.finishedApplications);
      response.addContainersToBeRemovedFromNM(
              new ArrayList<ContainerId>(this.containersToBeRemovedFromNM));
      response.addAllContainersToSignal(this.containersToSignal);
      this.containersToClean.clear();
      this.finishedApplications.clear();
      this.containersToSignal.clear();
      this.containersToBeRemovedFromNM.clear();
    } finally {
      this.writeLock.unlock();
    }
  }
  
    @Override
  public void updateNodeHeartbeatResponseForContainersDecreasing(
      NodeHeartbeatResponse response) {
    this.writeLock.lock();
    
    try {
      response.addAllContainersToDecrease(toBeDecreasedContainers.values());
      toBeDecreasedContainers.clear();
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  protected void handleRunningAppOnNode(RMNodeImpl rmNode,
          RMContext context, ApplicationId appId, NodeId nodeId) {
    RMApp app = context.getRMApps().get(appId);

    // if we failed getting app by appId, maybe something wrong happened, just
    // add the app to the finishedApplications list so that the app can be
    // cleaned up on the NM
    if (null == app) {
      LOG.warn("Cannot get RMApp by appId=" + appId
              + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
      return;
    }
    rmNode.runningApplications.add(appId);
    context.getDispatcher().getEventHandler()
            .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }

  @Override
  protected void cleanUpAppTransitionInternal(RMNodeImpl rmNode,
          RMNodeEvent event) {
    ApplicationId appId = ((RMNodeCleanAppEvent) event).getAppId();
    rmNode.finishedApplications.add(appId);
    rmNode.runningApplications.remove(appId);
    rmNode.getAppX509ToUpdate().remove(appId);
  }

  protected void cleanUpContainerTransitionInternal(RMNodeImpl rmNode,
          RMNodeEvent event) {
    List<io.hops.metadata.yarn.entity.ContainerStatus> containerToLog
            = new ArrayList<>();
    RMNodeCleanContainerEvent containerEvent = (RMNodeCleanContainerEvent) event;
    containerToLog.add(new io.hops.metadata.yarn.entity.ContainerStatus(
              containerEvent.getContainerId().toString(), ContainerState.COMPLETE.name(), "killed",
              ContainerExitStatus.ABORTED, containerEvent.getNodeId().toString()));
    context.getContainersLogsService().insertEvent(containerToLog);
    rmNode.containersToClean.add(((RMNodeCleanContainerEvent) event).
            getContainerId());

  }

  @Override
  protected List<UpdatedContainerInfo> pullContainerUpdatesInternal() {
    List<UpdatedContainerInfo> latestContainerInfoList
            = new ArrayList<UpdatedContainerInfo>();
    UpdatedContainerInfo containerInfo;
    while ((containerInfo = nodeUpdateQueue.poll()) != null) {
      latestContainerInfoList.add(containerInfo);
    }
    this.nextHeartBeat = true;
    return latestContainerInfoList;
  }

  @Override
  protected void addNodeTransitionInternal(RMNodeImpl rmNode, RMNodeEvent event) {
    // Inform the scheduler
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      NodeId nodeId = rmNode.nodeId;
      RMNode previousRMNode =
          rmNode.context.getInactiveRMNodes().remove(nodeId);
      if (previousRMNode != null) {
        rmNode.updateMetricsForRejoinedNode(previousRMNode.getState());
      } else {
        NodeId unknownNodeId =
            NodesListManager.createUnknownNodeId(nodeId.getHost());
        previousRMNode =
            rmNode.context.getInactiveRMNodes().remove(unknownNodeId);
        if (previousRMNode != null) {
          ClusterMetrics.getMetrics().decrDecommisionedNMs();
        }
        // Increment activeNodes explicitly because this is a new node.
        ClusterMetrics.getMetrics().incrNumActiveNodes();
        containers = startEvent.getNMContainerStatuses();
        if (containers != null && !containers.isEmpty()) {
          for (NMContainerStatus container : containers) {
            if (container.getContainerState() == ContainerState.RUNNING) {
              rmNode.launchedContainers.add(container.getContainerId());
            }
          }
        }
      }

      if (null != startEvent.getRunningApplications()) {
        for (ApplicationId appId : startEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeAddedSchedulerEvent(rmNode, containers));
      rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_USABLE, rmNode));
  }

  protected NodeState reconnectNodeTransitionInternal(RMNodeImpl rmNode,
          RMNodeEvent event) {
    RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent) event;
    RMNode newNode = reconnectEvent.getReconnectedNode();
    rmNode.nodeManagerVersion = newNode.getNodeManagerVersion();
    List<ApplicationId> runningApps = reconnectEvent.getRunningApplications();
    boolean noRunningApps = (runningApps == null) || (runningApps.size() == 0);

    // No application running on the node, so send node-removal event with 
    // cleaning up old container info.
    if (noRunningApps) {
      if (rmNode.getState() == NodeState.DECOMMISSIONING) {
        // When node in decommissioning, and no running apps on this node,
        // it will return as decommissioned state.
        deactivateNode(rmNode, NodeState.DECOMMISSIONED);
        return NodeState.DECOMMISSIONED;
      }
      rmNode.nodeUpdateQueue.clear();
      rmNode.context.getDispatcher().getEventHandler().handle(
              new NodeRemovedSchedulerEvent(rmNode));

      if (rmNode.getHttpPort() == newNode.getHttpPort()) {
        if (!rmNode.getTotalCapability().equals(
                newNode.getTotalCapability())) {
          rmNode.totalCapability = newNode.getTotalCapability();
        }
        if (rmNode.getState().equals(NodeState.RUNNING)) {
          // Only add old node if old state is RUNNING
          rmNode.context.getDispatcher().getEventHandler().handle(
                  new NodeAddedSchedulerEvent(rmNode));
        }
      } else {
        // Reconnected node differs, so replace old node and start new node
        switch (rmNode.getState()) {
          case RUNNING:
            ClusterMetrics.getMetrics().decrNumActiveNodes();
            break;
          case UNHEALTHY:
            ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
            break;
          default:
            LOG.debug("Unexpected Rmnode state");
        }
        rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
        rmNode.context.getDispatcher().getEventHandler().handle(
                new RMNodeStartedEvent(newNode.getNodeID(), null, null));
      }
    } else {
      rmNode.httpPort = newNode.getHttpPort();
      rmNode.httpAddress = newNode.getHttpAddress();
      boolean isCapabilityChanged = false;
      if (!rmNode.getTotalCapability().equals(
              newNode.getTotalCapability())) {
        rmNode.totalCapability = newNode.getTotalCapability();
        isCapabilityChanged = true;
      }

      handleNMContainerStatus(reconnectEvent.getNMContainerStatuses(), rmNode);

      for (ApplicationId appId : reconnectEvent.getRunningApplications()) {
        rmNode.handleRunningAppOnNode(rmNode, rmNode.context, appId,
                rmNode.nodeId);
      }

      if (isCapabilityChanged
              && rmNode.getState().equals(NodeState.RUNNING)) {
        // Update scheduler node's capacity for reconnect node.
        rmNode.context
                .getDispatcher()
                .getEventHandler()
                .handle(
                        new NodeResourceUpdateSchedulerEvent(rmNode,
                                ResourceOption
                                .newInstance(newNode.getTotalCapability(), -1)));
      }
    }
    return rmNode.getState();
  }

  @Override
  protected void decreaseContainersInt(RMNodeImpl rmNode, RMNodeDecreaseContainerEvent de) {
    for (Container c : de.getToBeDecreasedContainers()) {
      rmNode.toBeDecreasedContainers.put(c.getId(), c);
    }
  }
  
  @Override
  protected void deactivateNode(RMNodeImpl rmNode, final NodeState finalState) {
    if (rmNode.getNodeID().getPort() == -1) {
      rmNode.updateMetricsForDeactivatedNode(rmNode.getState(), finalState);
      return;
    }
    reportNodeUnusable(rmNode, finalState);

    // Deactivate the node
    rmNode.context.getRMNodes().remove(rmNode.nodeId);
    LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
        + finalState);
    rmNode.context.getInactiveRMNodes().put(rmNode.nodeId, rmNode);
    if (rmNode.context.getNodesListManager().isUntrackedNode(rmNode.hostName)) {
      rmNode.setUntrackedTimeStamp(Time.monotonicNow());
    }
  }

  /**
   * Report node is UNUSABLE and update metrics.
   *
   * @param rmNode
   * @param finalState
   */
  public static void reportNodeUnusable(RMNodeImpl rmNode,
      NodeState finalState) {
    // Inform the scheduler
    rmNode.nodeUpdateQueue.clear();
    // If the current state is NodeState.UNHEALTHY
    // Then node is already been removed from the
    // Scheduler
    NodeState initialState = rmNode.getState();
    if (!initialState.equals(NodeState.UNHEALTHY)) {
      rmNode.context.getDispatcher().getEventHandler()
          .handle(new NodeRemovedSchedulerEvent(rmNode));
    }
    rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_UNUSABLE, rmNode));

    //Update the metrics
    rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
  }


  protected NodeState statusUpdateWhenUnHealthyTransitionInternal(
          RMNodeImpl rmNode, RMNodeEvent event) {
    RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

    // Switch the last heartbeatresponse.
    rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();
    NodeHealthStatus remoteNodeHealthStatus = statusEvent.getNodeHealthStatus();
    rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
    rmNode.setLastHealthReportTime(
            remoteNodeHealthStatus.getLastHealthReportTime());
    rmNode.setAggregatedContainersUtilization(
          statusEvent.getAggregatedContainersUtilization());
    rmNode.setNodeUtilization(statusEvent.getNodeUtilization());
    if (remoteNodeHealthStatus.getIsNodeHealthy()) {
      rmNode.context.getDispatcher().getEventHandler().handle(
              new NodeAddedSchedulerEvent(rmNode));
      rmNode.context.getDispatcher().getEventHandler().handle(
              new NodesListManagerEvent(
                      NodesListManagerEventType.NODE_USABLE, rmNode));
      // ??? how about updating metrics before notifying to ensure that
      // notifiers get update metadata because they will very likely query it
      // upon notification
      // Update metrics
      rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
      return NodeState.RUNNING;
    }

    return NodeState.UNHEALTHY;
  }
  
  @Override
  protected void signalContainerInt(RMNodeImpl rmNode, RMNodeEvent event){
      rmNode.containersToSignal.add(((RMNodeSignalContainerEvent) event).getSignalRequest());
  }
  
  public void handle(RMNodeEvent event) {
    LOG.debug("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      NodeState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on Node  " + this.nodeId);
      }
      if (oldState != getState()) {
        LOG.info(nodeId + " Node Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }
}
