package io.hops.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by antonis on 8/23/16.
 */
public class NdbRmStreamingProcessor extends NdbStreamingReceiver {

    private final ExecutorService exec;

    public NdbRmStreamingProcessor(RMContext rmContext) {
        super(rmContext, "RM Event retriever");
        setRetrievingRunnable(new RetrievingThread());
        exec = Executors.newCachedThreadPool();
    }

    public void printHopsRMNodeComps(RMNodeComps hopRMNodeNDBCompObject) {

        //print hoprmnode
        LOG.debug(
                "<EvtProcessor_PRINT_START>-------------------------------------------------------------------");
        if (hopRMNodeNDBCompObject.getHopRMNode() != null) {
            LOG.debug("<EvtProcessor> [rmnode] id : " + hopRMNodeNDBCompObject.
                    getHopRMNode().getNodeId() + "| peinding id : "
                    + hopRMNodeNDBCompObject.getHopRMNode().getPendingEventId());
        }
        //print hopresource
        if (hopRMNodeNDBCompObject.getHopResource() != null) {
            LOG.debug("<EvtProcessor> [resource] id : " + hopRMNodeNDBCompObject.
                    getHopResource().getId() + "| memory : " + hopRMNodeNDBCompObject.
                    getHopResource().getMemory());
        }
        if (hopRMNodeNDBCompObject.getPendingEvent() != null) {
            LOG.debug("<EvtProcessor> [pendingevent] id : " + hopRMNodeNDBCompObject.
                    getPendingEvent().getId().getNodeId() + "| peinding id : "
                    + hopRMNodeNDBCompObject.getPendingEvent().getId() +
                    "| type: " + hopRMNodeNDBCompObject.getPendingEvent().getType() +
                    "| status: " + hopRMNodeNDBCompObject.getPendingEvent().getStatus());
        }
        List<UpdatedContainerInfo> hopUpdatedContainerInfo = hopRMNodeNDBCompObject.
                getHopUpdatedContainerInfo();
        for (UpdatedContainerInfo hopuc : hopUpdatedContainerInfo) {
            LOG.debug("<EvtProcessor> [updatedcontainerinfo] id : " + hopuc.
                    getRmnodeid() + "| container id : " + hopuc.getContainerId());
        }
        List<ContainerStatus> hopContainersStatus = hopRMNodeNDBCompObject.
                getHopContainersStatus();
        for (ContainerStatus hopCS : hopContainersStatus) {
            LOG.debug("<EvtProcessor> [containerstatus] id : " + hopCS.getRMNodeId()
                    + "| container id : " + hopCS.getContainerid()
                    + "| container status : " + hopCS.getExitstatus());
        }
        LOG.debug(
                "<EvtProcessor_PRINT_END>-------------------------------------------------------------------");
    }

    private void updateRMContext(RMNode rmNode) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("HOP :: PendingEventRetrieval rmNode " + rmNode
                    + ", state: " + rmNode.getState());
        }

        if (rmNode.getState() == NodeState.DECOMMISSIONED ||
                rmNode.getState() == NodeState.REBOOTED ||
                rmNode.getState() == NodeState.LOST) {

            rmContext.getInactiveRMNodes().put(rmNode.getNodeID().getHost(), rmNode);
            rmContext.getRMNodes().remove(rmNode.getNodeID(), rmNode);
        } else {
            rmContext.getInactiveRMNodes().remove(rmNode.getNodeID().getHost(), rmNode);
            rmContext.getRMNodes().put(rmNode.getNodeID(), rmNode);
        }
    }

    private void triggerEvent(final RMNode rmNode, PendingEvent pendingEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("NodeUpdate event_pending event trigger event: " +
                    pendingEvent.getId().getEventId() + " : " +
                    pendingEvent.getId().getNodeId());
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
            LOG.debug("HOP :: PendingEventRetrieval event NodeRemoved: " + pendingEvent);
            rmContext.getDispatcher().getEventHandler().handle(
                    new NodeRemovedSchedulerEvent(rmNode));
        } else if (pendingEvent.getType().equals(PendingEvent.Type.NODE_UPDATED)) {
            if (pendingEvent.getStatus().equals(PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING)) {
                LOG.debug("HOP :: NodeUpdate event - event_scheduler - finished_processing RMNode: " +
                        rmNode.getNodeID() + " pending event: " + pendingEvent.getId().getEventId());
                rmContext.getDispatcher().getEventHandler().handle(
                        new NodeUpdateSchedulerEvent(rmNode));
            } else if (pendingEvent.getStatus().equals(PendingEvent.Status.SCHEDULER_NOT_FINISHED_PROCESSING)) {
                LOG.debug("NodeUpdate event - event_scheduler - NOT_finished_processing RMNode: " +
                        rmNode.getNodeID() + " pending event: " + pendingEvent.getId().getEventId());
            }
        }
    }

    private class RetrievingThread implements Runnable {

        long lastTimestamp = 0;
        int numOfEvents = 0;
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    RMNodeComps hopRMNodeCompObj = null;
                    LOG.debug("Size of the events queue: " + NdbRmStreamingReceiver.receivedEvents.size());
                    hopRMNodeCompObj = NdbRmStreamingReceiver.receivedEvents.take();
                    if (hopRMNodeCompObj != null) {
                        if (LOG.isDebugEnabled()) {
                            printHopsRMNodeComps(hopRMNodeCompObj);
                        }
                        if (rmContext.isDistributed()) {
                            RMNode rmNode = null;

                            try {
                                rmNode = DBUtility.processHopRMNodeCompsForScheduler(hopRMNodeCompObj, rmContext);
                                LOG.debug("HOP :: RetrievingThread RMNode: " + rmNode);

                                if (rmNode != null) {
                                    updateRMContext(rmNode);
                                    triggerEvent(rmNode, hopRMNodeCompObj.getPendingEvent());
                                }

                                DBUtility.removePendingEvent(hopRMNodeCompObj.getHopRMNode().getNodeId(),
                                        hopRMNodeCompObj.getPendingEvent().getType(),
                                        hopRMNodeCompObj.getPendingEvent().getStatus(),
                                        hopRMNodeCompObj.getPendingEvent().getId().getEventId());
                                // ContainerStatuses and UpdatedContainerInfo are removed
                                // in RMNodeImplDist#pullContainerUpdates
                                if ((System.currentTimeMillis() - lastTimestamp) >= 1000) {
                                    LOG.error("***<Profiler> Processed " + numOfEvents + " per second");
                                    numOfEvents = 0;
                                    lastTimestamp = System.currentTimeMillis();
                                }
                                numOfEvents++;
                            } catch (InvalidProtocolBufferException ex) {
                                LOG.error("HOP :: Error retrieving RMNode: " + ex, ex);
                            } catch (IOException ex) {
                                LOG.error("HOP :: Error removing from DB: " + ex, ex);
                            }
                        }
                    }
                    // TODO process containers for the ContainerLogs service
                } catch (InterruptedException ex) {
                    LOG.error(ex, ex);
                }
            }

            exec.shutdown();
            LOG.info("HOP :: RM Event retriever interrupted");
        }
    }
}
