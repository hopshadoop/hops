package io.hops;

import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RmStreamingProcessor;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by antonis on 8/24/16.
 */
public class TestStreamingLibrary {

    private static final Log LOG = LogFactory.getLog(TestStreamingLibrary.class);

    private static Configuration conf;
    private static MockRM rm;
    private final int GB = 1024;
    private int id = 0;

    private int getId() {
        return id++;
    }

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
        //wait for the node to have completely started (can be slow on some machines).
        Thread.sleep(10000);
    }

    @After
    public void tearDown() throws Exception {
        TimeUnit.SECONDS.sleep(2);
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

        TimeUnit.SECONDS.sleep(2);
        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);

        TimeUnit.SECONDS.sleep(4);
    }

    @Test
    public void testAddNode() throws Exception {

        FullRMNode toCommit = generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW);
        RMNodeWriter rmNodeWriter = new RMNodeWriter(toCommit);
        rmNodeWriter.handle();
        LOG.debug("Persisted RMNode in DB");

        TimeUnit.SECONDS.sleep(1);

//        rmNodeWriter = new RMNodeWriter(generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
//                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW));
//        rmNodeWriter.handle();
//        LOG.debug("Persisted dummy RMNode");
//        TimeUnit.SECONDS.sleep(1);

        // Node should be added to RM nodes list
        Assert.assertTrue("Node " + toCommit.getYarnRMNode().getNodeID(), rm.getRMContext().getRMNodes().containsKey(
                toCommit.getYarnRMNode().getNodeID()));

        // Node should be added to scheduler node list
        Assert.assertNotNull(rm.getResourceScheduler().getNodeReport(toCommit.getYarnRMNode().getNodeID()));
        int clusterMemory = rm.getResourceScheduler().getClusterResource().getMemory();
        int clusterVCores = rm.getResourceScheduler().getClusterResource().getVirtualCores();
        int numOfNodes = rm.getResourceScheduler().getNumClusterNodes();

        Assert.assertEquals(toCommit.getYarnRMNode().getTotalCapability().getMemory(), clusterMemory);
        Assert.assertEquals(toCommit.getYarnRMNode().getTotalCapability().getVirtualCores(), clusterVCores);
        Assert.assertEquals(1, numOfNodes);
    }

    @Test
    public void testRemoveNode() throws Exception {
        // First add a new node
        FullRMNode addedNode = generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW);
        RMNodeWriter rmNodeWriter = new RMNodeWriter(addedNode);
        rmNodeWriter.handle();

        TimeUnit.SECONDS.sleep(1);

        // Dummy insert
        FullRMNode dummyNode = generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW);
        rmNodeWriter = new RMNodeWriter(dummyNode);
        rmNodeWriter.handle();

        TimeUnit.SECONDS.sleep(1);

        // Node should be added to RM nodes list
        Assert.assertTrue("Node " + addedNode.getYarnRMNode().getNodeID(), rm.getRMContext().getRMNodes().containsKey(
                addedNode.getYarnRMNode().getNodeID()));

        // Decommission node
        FullRMNode decNode = generateHopRMNode(addedNode.getId(), PendingEvent.Type.NODE_REMOVED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.DECOMMISSIONED);
        rmNodeWriter = new RMNodeWriter(decNode);
        rmNodeWriter.handle();

        TimeUnit.SECONDS.sleep(3);

        Assert.assertNotNull(rm.getRMContext().getInactiveRMNodes().get(
                decNode.getYarnRMNode().getNodeID().getHost()));
        Assert.assertFalse(rm.getRMContext().getRMNodes().containsKey(decNode.getYarnRMNode().getNodeID()));

        int clusterMemory = rm.getResourceScheduler().getClusterResource().getMemory();
        int clusterVCores = rm.getResourceScheduler().getClusterResource().getVirtualCores();
        int numOfNodes = rm.getResourceScheduler().getNumClusterNodes();

        Assert.assertNull(rm.getResourceScheduler().getNodeReport(decNode.getYarnRMNode().getNodeID()));
        Assert.assertEquals(dummyNode.getYarnRMNode().getTotalCapability().getMemory(), clusterMemory);
        Assert.assertEquals(dummyNode.getYarnRMNode().getTotalCapability().getVirtualCores(), clusterVCores);
        Assert.assertEquals(1, numOfNodes);
    }

    @Test
    @Ignore
    public void testUpdateNode() throws Exception {
        // First add a new node
        FullRMNode addedNode = generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW);
        RMNodeWriter rmNodeWriter = new RMNodeWriter(addedNode);
        rmNodeWriter.handle();

        TimeUnit.SECONDS.sleep(1);

        // Dummy insert
        FullRMNode dummyNode = generateHopRMNode(getId(), PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, NodeState.NEW);
        rmNodeWriter = new RMNodeWriter(dummyNode);
        rmNodeWriter.handle();

        TimeUnit.SECONDS.sleep(1);

        // Node should be added to RM nodes list
        Assert.assertTrue("Node " + addedNode.getYarnRMNode().getNodeID(), rm.getRMContext().getRMNodes().containsKey(
                addedNode.getYarnRMNode().getNodeID()));

        // Update Node with newly launched containers
    }

    private FullRMNode generateHopRMNode(int id, PendingEvent.Type type, PendingEvent.Status status,
            NodeState state) {
        int contains =0;
        RMNode rmNode = new RMNodeImplDist(
                NodeId.newInstance("host" + id, 1234),
                rm.getRMContext(),
                "host" + id,
                1337,
                8080,
                new NodeBase("name", "/location"),
                Resource.newInstance(6 * GB, 6),
                "1.0");
        
        if (!NodeState.NEW.equals(state)) {
            ((RMNodeImplDist) rmNode).setState(state.name());
        }

        io.hops.metadata.yarn.entity.RMNode hopRMNode = new io.hops.metadata.yarn.entity.RMNode(
                rmNode.getNodeID().toString(),
                rmNode.getHostName(),
                rmNode.getCommandPort(),
                rmNode.getHttpPort(),
                rmNode.getHealthReport(),
                rmNode.getLastHealthReportTime(),
                rmNode.getState().name(),
                rmNode.getNodeManagerVersion(),
                id);
        contains ++;
        

        io.hops.metadata.yarn.entity.Resource hopResource = new io.hops.metadata.yarn.entity.Resource(
                rmNode.getNodeID().toString(),
                rmNode.getTotalCapability().getMemory(),
                rmNode.getTotalCapability().getVirtualCores(),
                id);
        contains ++;
        
        NextHeartbeat nextHB = new NextHeartbeat(rmNode.getNodeID().toString(),
                true);

        
        PendingEvent pendingEvent = new PendingEvent(rmNode.getNodeID().toString(),
                type,
                status,
                id,contains);
        contains ++;
        
        List<ContainerStatus> containerStatuses = generateContainerStatuses(1, rmNode.getNodeID().toString(),pendingEvent.getId().getEventId());
        contains++;
        pendingEvent.setContains(contains);
        return new FullRMNode(rmNode, hopRMNode, pendingEvent, hopResource, nextHB, id,
                containerStatuses);
    }

    private List<ContainerStatus> generateContainerStatuses(int numOfContainers, String rmNodeId,
            int pendingEventId) {
        List<ContainerStatus> containerStatuses = new ArrayList<>();

        for (int i = 0; i < numOfContainers; ++i) {
            ContainerStatus contStat = new ContainerStatus(
                    rmNodeId + "_" + i,
                    "RUNNING",
                    "HEALTHY",
                    1,
                    rmNodeId,
                    pendingEventId,
                    // This is the UpdatedContainerInfo ID
                    pendingEventId);
            containerStatuses.add(contStat);
        }

        return containerStatuses;
    }

    private class FullRMNode {
        private final int id;
        private final RMNode yarnRMNode;
        private final io.hops.metadata.yarn.entity.RMNode rmNode;
        private final PendingEvent pendingEvent;
        private final io.hops.metadata.yarn.entity.Resource resource;
        private final NextHeartbeat nextHeartbeat;
        private final List<ContainerStatus> containerStatuses;

        public FullRMNode(RMNode yarnRMNode, io.hops.metadata.yarn.entity.RMNode rmNode, PendingEvent pendingEvent,
                io.hops.metadata.yarn.entity.Resource resource, NextHeartbeat nextHeartbeat, int id,
                List<ContainerStatus> containerStatuses) {
            this.yarnRMNode = yarnRMNode;
            this.rmNode = rmNode;
            this.pendingEvent = pendingEvent;
            this.resource = resource;
            this.nextHeartbeat = nextHeartbeat;
            this.id = id;
            this.containerStatuses = containerStatuses;
        }

        public List<ContainerStatus> getContainerStatuses() {
            return containerStatuses;
        }

        public int getId() {
            return id;
        }

        public RMNode getYarnRMNode() {
            return yarnRMNode;
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

            connector.flush();

            PendingEventDataAccess pendingEventDAO = (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            pendingEventDAO.add(toCommit.getPendingEvent());

            ResourceDataAccess resourceDAO = (ResourceDataAccess) RMStorageFactory
                    .getDataAccess(ResourceDataAccess.class);
            resourceDAO.add(toCommit.getResource());

            NextHeartbeatDataAccess nextHBDAO = (NextHeartbeatDataAccess) RMStorageFactory
                    .getDataAccess(NextHeartbeatDataAccess.class);
            nextHBDAO.update(toCommit.getNextHeartbeat());

            ContainerStatusDataAccess contStatDAO = (ContainerStatusDataAccess) RMStorageFactory
                    .getDataAccess(ContainerStatusDataAccess.class);
            contStatDAO.addAll(toCommit.getContainerStatuses());

            connector.commit();
            return null;
        }
    }
}
