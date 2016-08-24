package io.hops.util;

import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.util.List;

/**
 * Created by antonis on 8/23/16.
 */
public class NdbRmStreamingProcessor extends PendingEventRetrieval {

    public NdbRmStreamingProcessor(RMContext rmContext, Configuration conf) {
        super(rmContext, conf);
    }

    @Override
    public void start() {
        if (!active) {
            active = true;
            LOG.info("HOP :: Start retrieving thread");
            retrievingThread = new Thread(new RetrievingThread());
            retrievingThread.setName("event retriever");
            retrievingThread.start();
        } else {
            LOG.error("HOP :: NDB event retriever is already active");
        }
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
        //print hopnode
        if (hopRMNodeNDBCompObject.getHopNode() != null) {
            LOG.debug("<EvtProcessor> [node] id : " + hopRMNodeNDBCompObject.
                    getHopNode().getId() + "| level : " + hopRMNodeNDBCompObject.
                    getHopNode().getLevel());
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

    private class RetrievingThread implements Runnable {

        @Override
        public void run() {
            while (active) {
                try {
                    RMNodeComps hopRMNodeCompObj = null;
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
                            } catch (Exception ex) {
                                LOG.error("HOP :: Error retrieving RMNode: " + ex, ex);
                            }
                        }
                    }
                    // TODO process containers for the ContainerLogs service
                } catch (InterruptedException ex) {
                    LOG.error(ex, ex);
                }
            }
        }
    }
}
