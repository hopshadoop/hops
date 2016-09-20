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
package io.hops.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.PendingEventID;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.streaming.ContainerStatusEvent;
import io.hops.streaming.DBEvent;
import io.hops.streaming.PendingEventEvent;
import io.hops.streaming.ResourceEvent;
import io.hops.streaming.UpdatedContainerInfoEvent;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RmStreamingProcessor extends StreamingReceiver {

  private final ExecutorService exec;

  public RmStreamingProcessor(RMContext rmContext) {
    super(rmContext, "RM Event retriever");
    setRetrievingRunnable(new RetrievingThread());
    exec = Executors.newCachedThreadPool();
  }

  private void updateRMContext(RMNode rmNode) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("HOP :: PendingEventRetrieval rmNode " + rmNode
              + ", state: " + rmNode.getState());
    }

    if (rmNode.getState() == NodeState.DECOMMISSIONED || rmNode.getState()
            == NodeState.REBOOTED || rmNode.getState() == NodeState.LOST) {

      rmContext.getInactiveRMNodes().put(rmNode.getNodeID().getHost(), rmNode);
      rmContext.getRMNodes().remove(rmNode.getNodeID(), rmNode);
    } else {
      rmContext.getInactiveRMNodes().
              remove(rmNode.getNodeID().getHost(), rmNode);
      rmContext.getRMNodes().put(rmNode.getNodeID(), rmNode);
    }
  }

  private void triggerEvent(final RMNode rmNode, PendingEvent pendingEvent) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("NodeUpdate event_pending event trigger event: " + pendingEvent.
              getId().getEventId() + " : " + pendingEvent.getId().getNodeId());
    }

    // TODO Maybe we should put back Hops Global Thread pool
    exec.submit(new Runnable() {
      @Override
      public void run() {
        NetUtils.normalizeHostName(rmNode.getHostName());
      }
    });

    if (pendingEvent.getType().equals(PendingEvent.Type.NODE_ADDED)) {
      LOG.debug("HOP :: PendingEventRetrieval event NodeAdded: " + pendingEvent);
      rmContext.getDispatcher().getEventHandler().handle(
              new NodeAddedSchedulerEvent(rmNode));
    } else if (pendingEvent.getType().equals(PendingEvent.Type.NODE_REMOVED)) {
      LOG.debug("HOP :: PendingEventRetrieval event NodeRemoved: "
              + pendingEvent);
      rmContext.getDispatcher().getEventHandler().handle(
              new NodeRemovedSchedulerEvent(rmNode));
    } else if (pendingEvent.getType().equals(PendingEvent.Type.NODE_UPDATED)) {
      if (pendingEvent.getStatus().equals(
              PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING)) {
        LOG.debug(
                "HOP :: NodeUpdate event - event_scheduler - finished_processing RMNode: "
                + rmNode.getNodeID() + " pending event: "
                + pendingEvent.getId().getEventId());
        rmContext.getDispatcher().getEventHandler().handle(
                new NodeUpdateSchedulerEvent(rmNode));
      } else if (pendingEvent.getStatus().equals(
              PendingEvent.Status.SCHEDULER_NOT_FINISHED_PROCESSING)) {
        LOG.debug(
                "NodeUpdate event - event_scheduler - NOT_finished_processing RMNode: "
                + rmNode.getNodeID() + " pending event: "
                + pendingEvent.getId().getEventId());
      }
    }
  }

  Map<PendingEventID, RMNodeComps> partialRMNodeComps = new HashMap<>();

  private RMNodeComps getRMNodeComps(PendingEventID id) {
    RMNodeComps comps = partialRMNodeComps.get(id);
    if (comps == null) {
      comps = new RMNodeComps();
      partialRMNodeComps.put(id, comps);
    }
    return comps;
  }

  private class RetrievingThread implements Runnable {

    @Override
    public void run() {
      while (running) {
        try {
          DBEvent event = DBEvent.receivedEvents.take();
          RMNodeComps comps;
          if (event instanceof PendingEventEvent) {
            PendingEvent pendingEvent = ((PendingEventEvent) event).
                    getPendingEvent();
            comps = getRMNodeComps(pendingEvent.getId());
            comps.setPendingEvent(pendingEvent);
          } else if (event instanceof io.hops.streaming.RMNodeEvent) {
            io.hops.metadata.yarn.entity.RMNode rmNode
                    = ((io.hops.streaming.RMNodeEvent) event).getRmNode();
            comps = getRMNodeComps(
                    new PendingEventID(rmNode.getPendingEventId(), rmNode.
                            getNodeId()));
            comps.setRMNode(rmNode);
          } else if (event instanceof ResourceEvent) {
            Resource resource = ((ResourceEvent) event).getResource();
            comps = getRMNodeComps(new PendingEventID(resource.
                    getPendingEventId(), resource.getId()));
            comps.setResource(resource);
          } else if (event instanceof UpdatedContainerInfoEvent) {
            UpdatedContainerInfo uci = ((UpdatedContainerInfoEvent) event).
                    getUpdatedContainerInfo();
            comps = getRMNodeComps(new PendingEventID(uci.getPendingEventId(),
                    uci.getRmnodeid()));
            comps.addUpdatedContainerInfo(uci);
          } else if (event instanceof ContainerStatusEvent) {
            ContainerStatus containerStatus = ((ContainerStatusEvent) event).
                    getContainerStatus();
            comps = getRMNodeComps(new PendingEventID(containerStatus.
                    getPendingEventId(), containerStatus.getRMNodeId()));
            comps.addContainersStatus(containerStatus);
          } else {
            LOG.error("should not receive events of type " + event.getClass().
                    getCanonicalName());
            continue;
          }
          if (comps.isComplet()) {
            partialRMNodeComps.remove(comps.getPendingEvent().getId());
            if (comps != null) {
              if (rmContext.isDistributed()) {

                RMNode rmNode = null;

                try {
                  rmNode = DBUtility.processHopRMNodeCompsForScheduler(comps,
                          rmContext);
                  LOG.debug("HOP :: RetrievingThread RMNode: " + rmNode);

                  if (rmNode != null) {
                    updateRMContext(rmNode);
                    triggerEvent(rmNode, comps.getPendingEvent());
                  }

                  DBUtility.removePendingEvent(comps.getPendingEvent().getId().
                          getNodeId(),
                          comps.getPendingEvent().getType(),
                          comps.getPendingEvent().getStatus(),
                          comps.getPendingEvent().getId().getEventId(),
                          comps.getPendingEvent().getContains());
                } catch (InvalidProtocolBufferException ex) {
                  LOG.error("HOP :: Error retrieving RMNode: " + ex, ex);
                } catch (IOException ex) {
                  LOG.error("HOP :: Error removing from DB: " + ex, ex);
                }
              }
            }
          }
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }
      }

      exec.shutdown();
      LOG.info("HOP :: RM Event retriever interrupted");
    }
  }
}
