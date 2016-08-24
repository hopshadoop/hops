package io.hops;

import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by antonis on 8/24/16.
 */
public class TestStreamingLibrary {

    private static Configuration conf;
    private static MockRM rm;

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new YarnConfiguration();
        // Set configuration options
        conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH, "target/test-classes/RT_EventAPIConfig.ini");
        conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH, "target/test-classes/RM_EventAPIConfig.ini");
        conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
        RMStorageFactory.setConfiguration(conf);
        rm = new MockRM(conf);
        rm.start();
        //RMStorageFactory.kickTheNdbEventStreamingAPI(true, conf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        rm.stop();
    }

    @Test
    public void testRMReceiveEvents() throws Exception {
        RMNode rmNode = new RMNodeImplDist(
                NodeId.newInstance("localhost", 1337),
                rm.getRMContext(),
                "localhost",
                1337,
                8080,
                new NodeBase("name", "/location"),
                Resource.newInstance(1024, 2),
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
                1);

        RMNodeWriter rmNodeWriter = new RMNodeWriter(hopRMNode);
        rmNodeWriter.handle();
    }

    private class RMNodeWriter extends LightWeightRequestHandler {

        private final io.hops.metadata.yarn.entity.RMNode toCommit;

        public RMNodeWriter(io.hops.metadata.yarn.entity.RMNode toCommit) {
            super(YARNOperationType.TEST);
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) RMStorageFactory
                    .getDataAccess(RMNodeDataAccess.class);
            rmNodeDAO.add(toCommit);
            connector.commit();
            return null;
        }
    }
}
