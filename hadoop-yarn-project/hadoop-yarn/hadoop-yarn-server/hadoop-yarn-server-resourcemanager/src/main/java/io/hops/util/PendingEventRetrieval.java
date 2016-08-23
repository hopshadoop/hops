package io.hops.util;

import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.entity.PendingEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by antonis on 8/23/16.
 */
public abstract class PendingEventRetrieval {

    protected static final Log LOG = LogFactory.getLog(PendingEventRetrieval.class);

    protected boolean active = false;
    protected Thread retrievingThread;
    protected final RMContext rmContext;
    protected final Configuration conf;
    protected final ExecutorService exec;

    public PendingEventRetrieval(RMContext rmContext, Configuration conf) {
        this.rmContext = rmContext;
        this.conf = conf;
        exec = Executors.newCachedThreadPool();
    }

    public void finish() {
        LOG.info("HOP :: Stopping PendingEventRetrieval");
        this.active = false;
        retrievingThread.interrupt();
    }

    public abstract void start();

    protected void updateRMContext(RMNode rmNode) {
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

    protected void triggerEvent(final RMNode rmNode, PendingEvent pendingEvent) {
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

        if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_ADDED) {
            LOG.debug("HOP :: PendingEventRetrieval event NodeAdded: " + pendingEvent);
            rmContext.getDispatcher().getEventHandler().handle(
                    new NodeAddedSchedulerEvent(rmNode));
        } else if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_REMOVED) {
            LOG.debug("HOP :: PendingEventRetrieval event NodeRemoved: " + pendingEvent);
            rmContext.getDispatcher().getEventHandler().handle(
                    new NodeRemovedSchedulerEvent(rmNode));
        } else if (pendingEvent.getType() == TablesDef.PendingEventTableDef.NODE_UPDATED) {
            if (pendingEvent.getStatus() == TablesDef.PendingEventTableDef.SCHEDULER_FINISHED_PROCESSING) {
                LOG.debug("HOP :: NodeUpdate event - event_scheduler - finished_processing RMNode: " +
                        rmNode.getNodeID() + " pending event: " + pendingEvent.getId().getEventId());
                rmContext.getDispatcher().getEventHandler().handle(
                        new NodeUpdateSchedulerEvent(rmNode));
            } else if (pendingEvent.getStatus() == TablesDef.PendingEventTableDef.SCHEDULER_NOT_FINISHED_PROCESSING) {
                LOG.debug("NodeUpdate event - event_scheduler - NOT_finished_processing RMNode: " +
                        rmNode.getNodeID() + " pending event: " + pendingEvent.getId().getEventId());
            }
        }
    }
}
