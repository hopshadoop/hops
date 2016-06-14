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

import io.hops.common.GlobalThreadPool;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.TablesDef.PendingEventTableDef;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.PendingEventID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.net.NetUtils;

public abstract class PendingEventRetrieval {

  static final Log LOG = LogFactory.getLog(PendingEventRetrieval.class);
  protected boolean active = false;
  protected Thread retrivingThread;
  protected final RMContext rmContext;
  protected final Configuration conf;
  private int rpcId = -1;
  private Set<PendingEventID> recovered = new HashSet<PendingEventID>();
  
  public PendingEventRetrieval(RMContext rmContext, Configuration conf) {
    this.rmContext = rmContext;
    this.conf = conf;
  }

  public void finish() {
    LOG.info("HOP :: Stopping pendingEventRetrieval");
    this.active = false;
    retrivingThread.interrupt();
  }
  
  public abstract void start();

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

  public void triggerEvent(final RMNode rmNode, PendingEvent pendingEvent,
          boolean recovery) throws InterruptedException {
    if (recovery) {
      if (!recovered.add(pendingEvent.getId())) {
        return;
      }
    } else if (recovered.contains(pendingEvent.getId())) {
      return;
    }
    
    LOG.debug(
            "Nodeupdate event_pending event trigger event : "
            + pendingEvent.getId().getEventId() + " ; " + pendingEvent.getId().
            getNodeId());
    
    TransactionState transactionState = rmContext.getTransactionStateManager().
            getCurrentTransactionStatePriority(rpcId, "nodeHeartbeat");
    ((TransactionStateImpl) transactionState).
            getRMNodeInfo(rmNode.getNodeID()).addPendingEventToRemove(
                    pendingEvent);
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
      rmContext.getDispatcher().getEventHandler().handle(
              new NodeAddedSchedulerEvent(rmNode, transactionState));

    } else if (pendingEvent.getType()
            == PendingEventTableDef.NODE_REMOVED) {
      LOG.debug("HOP :: PendingEventRetrieval event NodeRemoved: "
              + pendingEvent);
      //Put pendingEvent to remove (for testing we update the status to COMPLETED

      rmContext.getDispatcher().getEventHandler()
              .handle(new NodeRemovedSchedulerEvent(rmNode, transactionState));

    } else if (pendingEvent.getType()
            == PendingEventTableDef.NODE_UPDATED) {

            // if scheduler is not finished the previous event , then just update the rmcontext
      // once scheduler finished the event , nextheartbeat will be true and rt will notfiy
      // whether to process or not
      if (pendingEvent.getStatus()
              == PendingEventTableDef.SCHEDULER_FINISHED_PROCESSING) {
        LOG.debug("Nodeupdate event_Scheduler_finished_processing rmnode : "
                + rmNode.getNodeID() + " pending event: "
                + pendingEvent.getId().getEventId());
        ((TransactionStateImpl) transactionState).getRMNodeInfo(rmNode.
                getNodeID()).
                setPendingEventId(pendingEvent.getId().getEventId());
        rmContext.getDispatcher().getEventHandler().handle(
                new NodeUpdateSchedulerEvent(rmNode, transactionState));

      } else if (pendingEvent.getStatus()
              == PendingEventTableDef.SCHEDULER_NOT_FINISHED_PROCESSING) {
        LOG.debug("Nodeupdate event_Scheduler_not_finished_processing rmnode : "
                + rmNode.getNodeID() + " pending event: "
                + pendingEvent.getId().getEventId());
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
