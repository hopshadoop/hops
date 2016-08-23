/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import io.hops.util.DBUtility;
import io.hops.util.ToCommitHB;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;

public class RMNodeImplDist extends RMNodeImpl {

  private static final Log LOG = LogFactory.getLog(RMNodeImplDist.class);

  // Used by RT streaming receiver
  public static enum KeyType {
    CURRENTNMTOKENMASTERKEY,
    NEXTNMTOKENMASTERKEY,
    CURRENTCONTAINERTOKENMASTERKEY,
    NEXTCONTAINERTOKENMASTERKEY
  }

  public RMNodeImplDist(NodeId nodeId, RMContext context, String hostName,
          int cmPort, int httpPort, Node node, Resource capability,
          String nodeManagerVersion) {
    super(nodeId, context, hostName, cmPort, httpPort, node, capability,
            nodeManagerVersion);
  }

  public static class StatusUpdateWhenHealthyTransition implements
          MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();

      NodeHealthStatus remoteNodeHealthStatus = statusEvent.
              getNodeHealthStatus();
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      rmNode.setLastHealthReportTime(
              remoteNodeHealthStatus.getLastHealthReportTime());
      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        LOG.info("Node " + rmNode.nodeId + " reported UNHEALTHY with details: "
                + remoteNodeHealthStatus.getHealthReport());
        rmNode.nodeUpdateQueue.clear();
        // Inform the scheduler
        rmNode.context.getDispatcher().getEventHandler().handle(
                new NodeRemovedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
                new NodesListManagerEvent(
                        NodesListManagerEventType.NODE_UNUSABLE, rmNode));
        // Update metrics
        rmNode.updateMetricsForDeactivatedNode(rmNode.getState(),
                NodeState.UNHEALTHY);
        return NodeState.UNHEALTHY;
      }
      ToCommitHB toCommit = new ToCommitHB(rmNode.nodeId.toString());
      ((RMNodeImplDist) rmNode).handleContainerStatus(statusEvent.
              getContainers(), toCommit);

      if (rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        if (rmNode.context.isDistributed() && !rmNode.context.isLeader()) {
          //Add NodeUpdatedSchedulerEvent to TransactionState

          toCommit.addPendingEvent("NODE_UPDATED",
                  "SCHEDULER_FINISHED_PROCESSING");
        } else {
          rmNode.context.getDispatcher().getEventHandler().handle(
                  new NodeUpdateSchedulerEvent(rmNode));
        }
      } else if (rmNode.context.isDistributed() && !rmNode.context.
              isLeader()) {
        //Add NodeUpdatedSchedulerEvent to TransactionState

        toCommit.addPendingEvent("NODE_UPDATED",
                "SCHEDULER_NOT_FINISHED_PROCESSING");
      }

      // Update DTRenewer in secure mode to keep these apps alive. Today this is
      // needed for log-aggregation to finish long after the apps are gone.
      if (UserGroupInformation.isSecurityEnabled()) {
        rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
                statusEvent.getKeepAliveAppIds());
      }
      toCommit.commit();
      return NodeState.RUNNING;
    }
  }

  protected void handleContainerStatus(List<ContainerStatus> containerStatuses,
          ToCommitHB toCommit) {
    // Filter the map to only obtain just launched containers and finished
    // containers.
    List<ContainerStatus> newlyLaunchedContainers
            = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
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
      if (finishedApplications.contains(containerId.getApplicationAttemptId()
              .getApplicationId())) {
        LOG.info("Container " + containerId
                + " belongs to an application that is already killed,"
                + " no further processing");
        continue;
      }

      // Process running containers
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        if (!launchedContainers.contains(containerId)) {
          // Just launched container. RM knows about it the first time.
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
        }
      } else {
        // A finished container
        launchedContainers.remove(containerId);
        completedContainers.add(remoteContainer);
      }
    }
    if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {
      UpdatedContainerInfo uci = new UpdatedContainerInfo(
              newlyLaunchedContainers,
              completedContainers);
      nodeUpdateQueue.add(uci);
      toCommit.addNodeUpdateQueue(uci);
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
      DBUtility.removeContainersToClean(containersToClean, this.nodeId);
      this.containersToClean.clear();
      DBUtility.removeFinishedApplications(finishedApplications, this.nodeId);
      this.finishedApplications.clear();
      this.containersToBeRemovedFromNM.clear();
    } finally {
      this.writeLock.unlock();
    }
  }

  private static void handleRunningAppOnNode(RMNodeImpl rmNode,
          RMContext context, ApplicationId appId, NodeId nodeId) {
    RMApp app = context.getRMApps().get(appId);

    // if we failed getting app by appId, maybe something wrong happened, just
    // add the app to the finishedApplications list so that the app can be
    // cleaned up on the NM
    if (null == app) {
      LOG.warn("Cannot get RMApp by appId=" + appId
              + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      DBUtility.addFinishedApplication(appId);
      return;
    }

    context.getDispatcher().getEventHandler()
            .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }

  public static class CleanUpAppTransition
          implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.finishedApplications.add(((RMNodeCleanAppEvent) event).getAppId());
      DBUtility.addFinishedApplication(((RMNodeCleanAppEvent) event).getAppId(),
              rmNode.getNodeID());
    }
  }

  public static class CleanUpContainerTransition implements
          SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToClean.add(((RMNodeCleanContainerEvent) event).
              getContainerId());
      DBUtility.addContainerToClean(((RMNodeCleanContainerEvent) event).
              getContainerId(), rmNode.getNodeID());
    }
  }

  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> latestContainerInfoList
            = new ArrayList<UpdatedContainerInfo>();
    UpdatedContainerInfo containerInfo;
    while ((containerInfo = nodeUpdateQueue.poll()) != null) {
      latestContainerInfoList.add(containerInfo);
    }
    DBUtility.removeUCI(latestContainerInfoList);
    this.nextHeartBeat = true;
    DBUtility.addNextHB(true);
    return latestContainerInfoList;
  }

  public void setContainersToCleanUp(Set<ContainerId> containersToCleanUp) {
    super.writeLock.lock();

    try {
      super.containersToClean.addAll(containersToCleanUp);
    } finally {
      super.writeLock.unlock();
    }
  }

  public void setAppsToCleanUp(List<ApplicationId> appsToCleanUp) {
    super.writeLock.lock();

    try {
      super.finishedApplications.addAll(appsToCleanUp);
    } finally {
      super.writeLock.unlock();
    }
  }

  public void setNextHeartbeat(boolean nextHeartbeat) {
    super.writeLock.lock();

    try {
      super.nextHeartBeat = nextHeartbeat;
    } finally {
      super.writeLock.unlock();
    }
  }
}
