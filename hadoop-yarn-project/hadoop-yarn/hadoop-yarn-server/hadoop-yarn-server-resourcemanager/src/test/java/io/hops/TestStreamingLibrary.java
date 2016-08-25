package io.hops;

import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.NdbRmStreamingProcessor;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 8/24/16.
 */
public class TestStreamingLibrary {

    private static final Log LOG = LogFactory.getLog(TestStreamingLibrary.class);

    private static Configuration conf;
    private static MockRM rm;
    private final int GB = 1024;

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new YarnConfiguration();
        // Set configuration options
        conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH, "target/test-classes/RT_EventAPIConfig.ini");
        conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH, "target/test-classes/RM_EventAPIConfig.ini");
        conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
        RMStorageFactory.setConfiguration(conf);
        YarnAPIStorageFactory.setConfiguration(conf);
    }

    @Before
    public void initTests() throws Exception {
        DBUtility.InitializeDB();
        rm = new MockRM(conf);
        rm.start();
    }

    @After
    public void tearDown() throws Exception {
        rm.stop();
    }

    @Test
    public void testRMReceiveEvents() throws Exception {
        LOG.debug("Register NM1");
        MockNM nm1 = rm.registerNode("host0:1234", 4 * GB, 4);
        LOG.debug("Register NM2");
        MockNM nm2 = rm.registerNode("host1:1234", 6 * GB, 6);

        LOG.debug("Heartbeat NM1");
        nm1.nodeHeartbeat(true);
        LOG.debug("Heartbeat NM2");
        nm2.nodeHeartbeat(true);

        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void testAddNode() throws Exception {

        RMNodeWriter rmNodeWriter = new RMNodeWriter(generateHopRMNode(0));
        rmNodeWriter.handle();
        LOG.debug("Persisted RMNode in DB");

        TimeUnit.SECONDS.sleep(5);
        Assert.assertEquals("There should be one RMNode", 1, rm.getRMContext().getRMNodes().size());
    }

    private FullRMNode generateHopRMNode(int pendingEventId) {


        RMNode rmNode = new RMNodeImplDist(
                NodeId.newInstance("localhost", 1234),
                rm.getRMContext(),
                "localhost",
                1337,
                8080,
                new NodeBase("name", "/location"),
                Resource.newInstance(6 * GB, 6),
                "1.0");

        io.hops.metadata.yarn.entity.RMNode hopRMNode = new io.hops.metadata.yarn.entity.RMNode(
                rmNode.getNodeID().toString(),
                rmNode.getHostName(),
                rmNode.getCommandPort(),
                rmNode.getHttpPort(),
                rmNode.getHealthReport(),
                rmNode.getLastHealthReportTime(),
                rmNode.getState().name(),
                rmNode.getNodeManagerVersion(),
                pendingEventId);

        PendingEvent pendingEvent = new PendingEvent(rmNode.getNodeID().toString(),
                PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING,
                pendingEventId);

        io.hops.metadata.yarn.entity.Resource hopResource = new io.hops.metadata.yarn.entity.Resource(
                rmNode.getNodeID().toString(),
                rmNode.getTotalCapability().getMemory(),
                rmNode.getTotalCapability().getVirtualCores(),
                pendingEventId);

        NextHeartbeat nextHB = new NextHeartbeat(rmNode.getNodeID().toString(),
                true, pendingEventId);

        return new FullRMNode(hopRMNode, pendingEvent, hopResource, nextHB);
    }

    private class FullRMNode {
        private final io.hops.metadata.yarn.entity.RMNode rmNode;
        private final PendingEvent pendingEvent;
        private final io.hops.metadata.yarn.entity.Resource resource;
        private final NextHeartbeat nextHeartbeat;

        public FullRMNode(io.hops.metadata.yarn.entity.RMNode rmNode, PendingEvent pendingEvent,
                io.hops.metadata.yarn.entity.Resource resource, NextHeartbeat nextHeartbeat) {
            this.rmNode = rmNode;
            this.pendingEvent = pendingEvent;
            this.resource = resource;
            this.nextHeartbeat = nextHeartbeat;
        }

        public io.hops.metadata.yarn.entity.RMNode getRmNode() {
            return rmNode;
        }

        public PendingEvent getPendingEvent() {
            return pendingEvent;
        }

        public io.hops.metadata.yarn.entity.Resource getResource() {
            return resource;
        }

        public NextHeartbeat getNextHeartbeat() {
            return nextHeartbeat;
        }
    }

    private class RMNodeWriter extends LightWeightRequestHandler {

        private final FullRMNode toCommit;

        public RMNodeWriter(FullRMNode toCommit) {
            super(YARNOperationType.TEST);
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) RMStorageFactory
                    .getDataAccess(RMNodeDataAccess.class);
            rmNodeDAO.add(toCommit.getRmNode());

            PendingEventDataAccess pendingEventDAO = (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            pendingEventDAO.add(toCommit.getPendingEvent());

            ResourceDataAccess resourceDAO = (ResourceDataAccess) RMStorageFactory
                    .getDataAccess(ResourceDataAccess.class);
            resourceDAO.add(toCommit.getResource());

            NextHeartbeatDataAccess nextHBDAO = (NextHeartbeatDataAccess) RMStorageFactory
                    .getDataAccess(NextHeartbeatDataAccess.class);
            nextHBDAO.update(toCommit.getNextHeartbeat());

            connector.commit();
            return null;
        }
    }
}
