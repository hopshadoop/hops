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
import io.hops.common.GlobalThreadPool;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.ha.common.transactionStateWrapper;
import io.hops.metadata.yarn.TablesDef.ContainerStatusTableDef;
import io.hops.metadata.yarn.TablesDef.PendingEventTableDef;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.ConverterUtils;
import static org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService.resolve;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

public abstract class PendingEventRetrieval implements Runnable {

  static final Log LOG = LogFactory.getLog(PendingEventRetrieval.class);
  protected boolean active = true;
  protected final RMContext rmContext;
  protected final Configuration conf;
  private int rpcId = -1;

  public PendingEventRetrieval(RMContext rmContext, Configuration conf) {
    this.rmContext = rmContext;
    this.conf = conf;
  }

  public void finish() {
    LOG.info("HOP :: Stopping pendingEventRetrieval");
    this.active = false;
  }

  protected RMNode processHopRMNodeComps(RMNodeComps hopRMNodeFull) throws
          InvalidProtocolBufferException {
    NodeId nodeId;
    RMNode rmNode = null;
    if (hopRMNodeFull != null) {
      nodeId = ConverterUtils.toNodeId(hopRMNodeFull.getPendingEvent().
              getRmnodeId());

      rmNode = rmContext.getActiveRMNodes().get(nodeId);
      // so first time we are receiving , this will happen when node registers
      if (rmNode == null) {
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
        boolean nextHeartbeat = true;
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
                hopRMNodeFull.getHopRMNode().getHttpPort(),
                resolve(hopRMNodeFull.getHopRMNode().getHostName()),
                resourceOption,
                hopRMNodeFull.getHopRMNode().getNodemanagerVersion(),
                hopRMNodeFull.getHopRMNode().getHealthReport(),
                hopRMNodeFull.getHopRMNode().getLastHealthReportTime(),
                nextHeartbeat, conf.getBoolean(
                        YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED,
                        YarnConfiguration.DEFAULT_HOPS_DISTRIBUTED_RT_ENABLED));

        //This is  to force java to put the host in cash 
        //for quick loop-up in scheduler container allocation  
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.
                        getPort());
      }
      // now we update the rmnode

      ((RMNodeImpl) rmNode).setState(hopRMNodeFull.getHopRMNode().
              getCurrentState());
      List<UpdatedContainerInfo> hopUpdatedContainerInfoList
              = hopRMNodeFull.getHopUpdatedContainerInfo();
      if (hopUpdatedContainerInfoList != null && !hopUpdatedContainerInfoList.
              isEmpty()) {
        ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> updatedContainerInfoQueue
                = new ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
        Map<Integer, org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> ucis
                = new HashMap<Integer, org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
        for (UpdatedContainerInfo hopUCI : hopUpdatedContainerInfoList) {
          if (!ucis.containsKey(hopUCI.getUpdatedContainerInfoId())) {
            ucis.put(hopUCI.getUpdatedContainerInfoId(),
                    new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                            new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>(),
                            new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>(),
                            hopUCI.getUpdatedContainerInfoId()));
          }

          org.apache.hadoop.yarn.api.records.ContainerId cid = ConverterUtils.
                  toContainerId(hopUCI.
                          getContainerId());
          ContainerStatus hopContainerStatus = hopRMNodeFull.
                  getHopContainersStatusMap().get(hopUCI.getContainerId());
          org.apache.hadoop.yarn.api.records.ContainerStatus conStatus
                  = org.apache.hadoop.yarn.api.records.ContainerStatus.
                  newInstance(cid,
                          ContainerState.valueOf(hopContainerStatus.getState()),
                          hopContainerStatus.getDiagnostics(),
                          hopContainerStatus.getExitstatus());
          //Check ContainerStatus state to add it to appropriate list
          if (conStatus != null) {
            if (conStatus.getState().toString().equals(
                    ContainerStatusTableDef.STATE_RUNNING)) {
              ucis.get(hopUCI.getUpdatedContainerInfoId()).
                      getNewlyLaunchedContainers().add(conStatus);
            } else if (conStatus.getState().toString().equals(
                    ContainerStatusTableDef.STATE_COMPLETED)) {
              ucis.get(hopUCI.getUpdatedContainerInfoId()).
                      getCompletedContainers().add(conStatus);
            }
          }
        }
        int maxUciId = 0;
        for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci
                : ucis.values()) {
          updatedContainerInfoQueue.add(uci);
          if (uci.getUpdatedContainerInfoId() > maxUciId) {
            maxUciId = uci.getUpdatedContainerInfoId();
          }
        }

        ((RMNodeImpl) rmNode).setUpdatedContainerInfoId(maxUciId);
        ((RMNodeImpl) rmNode).setUpdatedContainerInfo(
                updatedContainerInfoQueue);
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

  protected void triggerEvent(final RMNode rmNode, PendingEvent pendingEvent) {
    LOG.debug(
            "Nodeupdate event_pending event trigger event - rmnode : "
            + rmNode.getNodeID());
    
    TransactionState transactionState = null;
    GlobalThreadPool.getExecutorService().execute(new Runnable() {
      @Override
      public void run() {
        NetUtils.normalizeHostName(rmNode.getHostName());
      }
    });
    
    if (pendingEvent.getType() == PendingEventTableDef.NODE_ADDED) {
      LOG.debug("HOP :: PendingEventRetrieval event NodeAdded: "
              + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED
      transactionState = rmContext.getTransactionStateManager().
              getCurrentTransactionStatePriority(rpcId, "nodeHeartbeat");
      ((TransactionStateImpl) transactionState).
              getRMNodeInfo(rmNode.getNodeID()).addPendingEventToRemove(
                      pendingEvent.getId(),
                      rmNode.getNodeID().toString(),
                      PendingEventTableDef.NODE_ADDED,
                      PendingEventTableDef.COMPLETED);
      rmContext.getDispatcher().getEventHandler().handle(
              new NodeAddedSchedulerEvent(rmNode, transactionState));

    } else if (pendingEvent.getType()
            == PendingEventTableDef.NODE_REMOVED) {
      LOG.debug("HOP :: PendingEventRetrieval event NodeRemoved: "
              + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED
      transactionState = rmContext.getTransactionStateManager().
              getCurrentTransactionStatePriority(rpcId, "nodeHeartbeat");
      ((TransactionStateImpl) transactionState).
              getRMNodeInfo(rmNode.getNodeID()).addPendingEventToRemove(
                      pendingEvent.getId(),
                      rmNode.getNodeID().toString(),
                      PendingEventTableDef.NODE_REMOVED,
                      PendingEventTableDef.COMPLETED);

      rmContext.getDispatcher().getEventHandler()
              .handle(new NodeRemovedSchedulerEvent(rmNode, transactionState));

    } else if (pendingEvent.getType()
            == PendingEventTableDef.NODE_UPDATED) {

            // if scheduler is not finished the previous event , then just update the rmcontext
      // once scheduler finished the event , nextheartbeat will be true and rt will notfiy
      // whether to process or not
      if (pendingEvent.getStatus()
              == PendingEventTableDef.SCHEDULER_FINISHED_PROCESSING) {
        transactionState = rmContext.getTransactionStateManager().
                getCurrentTransactionStatePriority(rpcId, "nodeHeartbeat");
        ((TransactionStateImpl) transactionState).getRMNodeInfo(rmNode.
                getNodeID()).addPendingEventToRemove(
                        pendingEvent.getId(),
                        rmNode.getNodeID().toString(),
                        PendingEventTableDef.NODE_UPDATED,
                        PendingEventTableDef.COMPLETED);
        LOG.debug("Nodeupdate event_Scheduler_finished_processing rmnode : "
                + rmNode.getNodeID());
        ((TransactionStateImpl) transactionState).getRMNodeInfo(rmNode.
                getNodeID()).setPendingEventId(pendingEvent.getId());
        rmContext.getDispatcher().getEventHandler().handle(
                new NodeUpdateSchedulerEvent(rmNode, transactionState));

      } else if (pendingEvent.getStatus()
              == PendingEventTableDef.SCHEDULER_NOT_FINISHED_PROCESSING) {
        LOG.debug("Nodeupdate event_Scheduler_not_finished_processing rmnode : "
                + rmNode.getNodeID());
      }
    }

    try {
      if (transactionState != null) {
        transactionState.decCounter(TransactionState.TransactionType.INIT);
      }
    } catch (IOException ex) {
      LOG.error("HOP :: Error decreasing ts counter", ex);
    }
  }
}
