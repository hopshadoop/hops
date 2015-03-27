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
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class PendingEventRetrieval implements Runnable {

  private static final Log LOG = LogFactory.getLog(PendingEventRetrieval.class);
  protected boolean active = true;
  protected final RMContext rmContext;
  protected final Configuration conf;

  public PendingEventRetrieval(RMContext rmContext, Configuration conf) {
    this.rmContext = rmContext;
    this.conf = conf;
  }

  public void finish() {
    LOG.info("HOP :: Stopping pendingEventRetrieval");
    this.active = false;
  }

  protected RMNode convertHopToRMNode(RMNodeComps hopRMNodeFull)
      throws InvalidProtocolBufferException {
    RMNode rmNode = null;
    if (hopRMNodeFull != null) {
      NodeId nodeId =
          ConverterUtils.toNodeId(hopRMNodeFull.getHopRMNode().getNodeId());
      //Retrieve and Initialize NodeBase for RMNode
      Node node = null;
      if (hopRMNodeFull.getHopRMNode().getNodeId() != null) {
        node = new NodeBase(hopRMNodeFull.getHopNode().getName(), hopRMNodeFull.
            getHopNode().getLocation());
        if (hopRMNodeFull.getHopNode().getParent() != null) {
          node.setParent(new NodeBase(hopRMNodeFull.getHopNode().getParent()));
        }
        node.setLevel(hopRMNodeFull.getHopNode().getLevel());
      }
      //Retrieve nextHeartbeat
      boolean nextHeartbeat = hopRMNodeFull.getHopNextHeartbeat().
          isNextheartbeat();
      //Create Resource
      ResourceOption resourceOption = null;
      if (hopRMNodeFull.getHopResource() != null) {
        resourceOption = ResourceOption
            .newInstance(Resource.newInstance(hopRMNodeFull.getHopResource().
                        getMemory(),
                    hopRMNodeFull.getHopResource().getVirtualCores()),
                hopRMNodeFull.getHopRMNode().getOvercommittimeout());
      }
      //Create RMNode from HopRMNode
      rmNode = new RMNodeImpl(nodeId, rmContext,
          hopRMNodeFull.getHopRMNode().getHostName(),
          hopRMNodeFull.getHopRMNode().getCommandPort(),
          hopRMNodeFull.getHopRMNode().getHttpPort(), node, resourceOption,
          hopRMNodeFull.getHopRMNode().getNodemanagerVersion(),
          hopRMNodeFull.getHopRMNode().getHealthReport(),
          hopRMNodeFull.getHopRMNode().getLastHealthReportTime(), nextHeartbeat,
          conf.getBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED,
              YarnConfiguration.DEFAULT_HOPS_DISTRIBUTED_RT_ENABLED));

      ((RMNodeImpl) rmNode).setState(hopRMNodeFull.getHopRMNode().
          getCurrentState());
      // *** Recover maps/lists of RMNode ***
      //1. Recover JustLaunchedContainers
      List<JustLaunchedContainers> hopJlcList = hopRMNodeFull.
          getHopJustLaunchedContainers();
      if (hopJlcList != null && !hopJlcList.isEmpty()) {
        Map<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>
            justLaunchedContainers =
            new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>();
        for (JustLaunchedContainers hop : hopJlcList) {
          //Create ContainerId
          org.apache.hadoop.yarn.api.records.ContainerId cid =
              ConverterUtils.toContainerId(hop.getContainerId());
          //Find and create ContainerStatus
          ContainerStatus hopContainerStatus = hopRMNodeFull.
              getHopContainersStatus().get(hop.getContainerId());
          org.apache.hadoop.yarn.api.records.ContainerStatus conStatus =
              org.apache.hadoop.yarn.api.records.ContainerStatus
                  .newInstance(cid,
                      ContainerState.valueOf(hopContainerStatus.getState()),
                      hopContainerStatus.getDiagnostics(),
                      hopContainerStatus.getExitstatus());
          justLaunchedContainers.put(cid, conStatus);
        }
        ((RMNodeImpl) rmNode).setJustLaunchedContainers(justLaunchedContainers);
      }
      //2. Return ContainerIdToClean
      List<ContainerId> cidToCleanList = hopRMNodeFull.
          getHopContainerIdsToClean();
      if (cidToCleanList != null && !cidToCleanList.isEmpty()) {
        Set<org.apache.hadoop.yarn.api.records.ContainerId> containersToClean =
            new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
        for (ContainerId hop : cidToCleanList) {
          //Create ContainerId
          containersToClean.add(ConverterUtils.toContainerId(hop.
              getContainerId()));
        }
        ((RMNodeImpl) rmNode).setContainersToClean(containersToClean);
      }
      //3. Finished Applications
      List<FinishedApplications> hopFinishedAppsList = hopRMNodeFull.
          getHopFinishedApplications();
      if (hopFinishedAppsList != null && !hopFinishedAppsList.isEmpty()) {
        List<ApplicationId> finishedApps = new ArrayList<ApplicationId>();
        for (FinishedApplications hop : hopFinishedAppsList) {
          finishedApps.add(ConverterUtils.
              toApplicationId(hop.getApplicationId()));
        }
        ((RMNodeImpl) rmNode).setFinishedApplications(finishedApps);
      }
      //4. UpdadedContainerInfo
      //Retrieve all UpdatedContainerInfo entries for this particular RMNode
      Map<Integer, List<UpdatedContainerInfo>> hopUpdatedContainerInfoMap =
          hopRMNodeFull.getHopUpdatedContainerInfo();
      if (hopUpdatedContainerInfoMap != null && !hopUpdatedContainerInfoMap.
          isEmpty()) {
        ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
            updatedContainerInfoQueue =
            new ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
        for (int uciId : hopUpdatedContainerInfoMap.keySet()) {
          for (UpdatedContainerInfo hopUCI : hopUpdatedContainerInfoMap
              .get(uciId)) {
            List<org.apache.hadoop.yarn.api.records.ContainerStatus>
                newlyAllocated =
                new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();
            List<org.apache.hadoop.yarn.api.records.ContainerStatus> completed =
                new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();
            //Retrieve containerstatus entries for the particular updatedcontainerinfo
            org.apache.hadoop.yarn.api.records.ContainerId cid =
                ConverterUtils.toContainerId(hopUCI.
                    getContainerId());
            ContainerStatus hopContainerStatus = hopRMNodeFull.
                getHopContainersStatus().get(hopUCI.getContainerId());

            org.apache.hadoop.yarn.api.records.ContainerStatus conStatus =
                org.apache.hadoop.yarn.api.records.ContainerStatus
                    .newInstance(cid,
                        ContainerState.valueOf(hopContainerStatus.getState()),
                        hopContainerStatus.getDiagnostics(),
                        hopContainerStatus.getExitstatus());
            //Check ContainerStatus state to add it to appropriate list
            if (conStatus != null) {
              if (conStatus.getState().toString()
                  .equals(TablesDef.ContainerStatusTableDef.STATE_RUNNING)) {
                newlyAllocated.add(conStatus);
              } else if (conStatus.getState().toString()
                  .equals(TablesDef.ContainerStatusTableDef.STATE_COMPLETED)) {
                completed.add(conStatus);
              }
            }
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo
                uci =
                new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                    newlyAllocated, completed,
                    hopUCI.getUpdatedContainerInfoId());
            updatedContainerInfoQueue.add(uci);
            ((RMNodeImpl) rmNode)
                .setUpdatedContainerInfo(updatedContainerInfoQueue);
            //Update uci counter
            ((RMNodeImpl) rmNode).setUpdatedContainerInfoId(hopRMNodeFull.
                getHopRMNode().getUciId());
          }
        }
      }
      //5. Retrieve latestNodeHeartBeatResponse
      NodeHBResponse hopHB = hopRMNodeFull.getHopNodeHBResponse();
      if (hopHB != null && hopHB.getResponse() != null) {
        NodeHeartbeatResponse hb = new NodeHeartbeatResponsePBImpl(
            YarnServerCommonServiceProtos.NodeHeartbeatResponseProto.
                parseFrom(hopHB.getResponse()));
        ((RMNodeImpl) rmNode).setLatestNodeHBResponse(hb);
      }
    }
    return rmNode;
  }
  
  protected void updateRMContext(RMNode rmNode) {
    if (rmNode.getState() == NodeState.DECOMMISSIONED ||
        rmNode.getState() == NodeState.REBOOTED ||
        rmNode.getState() == NodeState.LOST) {
      LOG.debug("HOP :: PendingEventRetrieval rmNode:" + rmNode + ", state-" +
          rmNode.getState());
      rmContext.getInactiveRMNodes().put(rmNode.getNodeID().
              getHost(), rmNode);
      rmContext.getActiveRMNodes().
          remove(rmNode.getNodeID(), rmNode);
    } else {
      LOG.debug("HOP :: PendingEventRetrieval rmNode:" + rmNode + ", state-" +
          rmNode.getState());
      rmContext.getInactiveRMNodes().
          remove(rmNode.getNodeID().getHost(), rmNode);
      rmContext.getActiveRMNodes().put(rmNode.getNodeID(), rmNode);
    }

  }
  
  protected void triggerEvent(RMNode rmNode, PendingEvent pendingEvent) {
    LOG.debug(
        "HOP :: RMNodeWorker:" + rmNode.getNodeID() + " processing event:" +
            pendingEvent);

    TransactionState ts = new TransactionStateImpl(Integer.MIN_VALUE,
        TransactionState.TransactionType.NODE);
    if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_ADDED) {
      LOG.debug(
          "HOP :: PendingEventRetrieval event NodeAdded: " + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED
      ((TransactionStateImpl) ts).addPendingEventToRemove(pendingEvent.getId(),
          rmNode.getNodeID().toString(), TablesDef.PendingEventTableDef.NODE_ADDED,
          TablesDef.PendingEventTableDef.COMPLETED);
      rmContext.getDispatcher().getEventHandler()
          .handle(new NodeAddedSchedulerEvent(rmNode, ts));

    } else if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_REMOVED) {
      LOG.debug(
          "HOP :: PendingEventRetrieval event NodeRemoved: " + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED
      ((TransactionStateImpl) ts).addPendingEventToRemove(pendingEvent.getId(),
          rmNode.getNodeID().toString(), TablesDef.PendingEventTableDef.NODE_REMOVED,
          TablesDef.PendingEventTableDef.COMPLETED);

      rmContext.getDispatcher().getEventHandler()
          .handle(new NodeRemovedSchedulerEvent(rmNode, ts));

    } else if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_UPDATED) {
      LOG.debug(
          "HOP :: PendingEventRetrieval event NodeUpdated: " + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED
      ((TransactionStateImpl) ts).addPendingEventToRemove(pendingEvent.getId(),
          rmNode.getNodeID().toString(), TablesDef.PendingEventTableDef.NODE_UPDATED,
          TablesDef.PendingEventTableDef.COMPLETED);

      rmContext.getDispatcher().getEventHandler()
          .handle(new NodeUpdateSchedulerEvent(rmNode, ts));
    }
    try {
      ts.decCounter("PendingEventRetrieval");
    } catch (IOException ex) {
      LOG.error("HOP :: Error decreasing ts counter", ex);
    }
  }
}
