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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestDistributedRTClient {

  private static final Log LOG = LogFactory.getLog(TestDistributedRT.class);
  private Configuration conf;
  /**
   * **********************************************
   */
  //The following fields should be initialized based on cmd line arguments
  private final String RT_ADDRESS = "localhost";
  private final int RT_PORT = 8031;
  private final int NUM_OF_TOTAL_NM = 50;
  private final int NUM_OF_NM_PER_THREAD = 20;
  private final int REGISTRATIONS_PER_NM = 1;

  private final int HEARTBEATS_PER_NM = 10;
  //Thread sleeps for HEARTBEAT_PERIOD before sending heartbeat
  private final int HEARTBEAT_PERIOD = 100;

  private ResourceTracker rt = null;
  private boolean sendRegistrations = true;
  private boolean sendHeartbeats = true;
  /**
   * **********************************************
   */
  //ThreadPool for client to send registrations and heartbeats. Each NM needs 
  //one thread for registration and one for heartbeat.
  private final ExecutorService executor = Executors.newCachedThreadPool();

  //NodeManagers
  private final SortedMap<Integer, NodeId> nmAddresses =
      new TreeMap<Integer, NodeId>();
  //Map to store heartbeat responses
  ConcurrentHashMap<NodeId, List<Boolean>> nextHeartbeats =
      new ConcurrentHashMap<NodeId, List<Boolean>>();

  @Before
  public void setup() {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      //Create NMs
      for (int i = 0; i < NUM_OF_TOTAL_NM; i++) {
        nmAddresses.put(i, NodeId.newInstance("host" + i, i));
      }
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }

  /**
   * @throws YarnException
   * @throws IOException
   * @throws InterruptedException
   */
  //  @Test
  public void client() throws YarnException, IOException, InterruptedException {
    //Set the ResourceTracker address to contact
    conf.setStrings(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, RT_ADDRESS);
    conf.setInt(YarnConfiguration.RM_RESOURCE_TRACKER_PORT, RT_PORT);
    //Create the ResourceTracker client
    rt = (ResourceTracker) RpcClientFactoryPBImpl.get().
        getClient(ResourceTracker.class, 1, NetUtils.getConnectAddress(
                new InetSocketAddress(
                    conf.get(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS),
                    conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_PORT,
                        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT))),
            conf);
    //Assign nodes to worker threads
    //Register nodes
    if (sendRegistrations) {
      for (int i = 0; i < NUM_OF_TOTAL_NM; i += NUM_OF_NM_PER_THREAD) {
        int offset =
            Math.min(i + NUM_OF_NM_PER_THREAD - 1, NUM_OF_TOTAL_NM - 1) + 1;
        //Register nodes first
        executor
            .execute(new RTClientWorker(false, nmAddresses.subMap(i, offset).
                values(), i));
      }
    }
    //Wait for processing to complete
    //TODO: Get Active RMNodes from ndb instead of sleeping
    Thread.sleep(5000);
    //Send heartbeats
    for (int i = 0; i < NUM_OF_TOTAL_NM; i += NUM_OF_NM_PER_THREAD) {
      int offset =
          Math.min(i + NUM_OF_NM_PER_THREAD - 1, NUM_OF_TOTAL_NM - 1) + 1;
      //Send the Heartbeats
      if (sendHeartbeats) {
        executor.execute(new RTClientWorker(true, nmAddresses.subMap(i, offset).
            values(), i));
        LOG.debug("HOP :: Started worker for NMs:" + i + "-" + offset);
      }

    }
    executor.shutdown();
    executor.awaitTermination(600000, TimeUnit.MILLISECONDS);

  }

  /**
   * Registers a node with the RT. If num is greater that 1, multiple requests
   * are sent for the same node and the last response is returned;
   * <p/>
   *
   * @param rt
   * @param host
   * @param port
   * @param num
   * @return
   */
  private void registerClient(ResourceTracker rt, Collection<NodeId> nodeIds) {
    for (NodeId node : nodeIds) {
      for (int i = 0; i < REGISTRATIONS_PER_NM; i++) {
        try {
          RegisterNodeManagerRequest request =
              Records.newRecord(RegisterNodeManagerRequest.class);
          request.setHttpPort(node.getPort());
          request.setNodeId(node);
          Resource resource = Resource.newInstance(5012, 8);
          request.setResource(resource);
          rt.registerNodeManager(request);
        } catch (YarnException ex) {
          LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
        } catch (IOException ex) {
          LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
        }
      }
    }
  }

  /**
   * Sends a specific number of heartbeats to the given ResourceTracker,
   * from the provided node (host, port).
   * <p/>
   *
   * @param rt
   * @param host
   * @param port
   * @param num
   */
  private void heartbeatClient(ResourceTracker rt, Collection<NodeId> nodeIds) {
    int lastHeartbeatId = 0;
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(true);
    healthStatus.setLastHealthReportTime(1);
    MasterKey masterKey = new MasterKeyPBImpl();
    //Send heartbeats from every NM
    for (int i = 0; i < HEARTBEATS_PER_NM; i++) {
      for (NodeId nodeId : nodeIds) {
        try {
          NodeHeartbeatRequest req =
              Records.newRecord(NodeHeartbeatRequest.class);
          NodeStatus status = Records.newRecord(NodeStatus.class);
          req.setLastKnownContainerTokenMasterKey(masterKey);
          req.setLastKnownNMTokenMasterKey(masterKey);
          status.setNodeId(nodeId);
          status.setResponseId(lastHeartbeatId);
          status.setNodeHealthStatus(healthStatus);
          req.setNodeStatus(status);
          NodeHeartbeatResponse resp = rt.nodeHeartbeat(req);
          if (resp != null) {
            lastHeartbeatId = resp.getResponseId();
            if (!nextHeartbeats.containsKey(nodeId)) {
              nextHeartbeats.put(nodeId, new ArrayList<Boolean>());
            }
            nextHeartbeats.get(nodeId).add(resp.getNextheartbeat());
          }
          Thread.sleep(HEARTBEAT_PERIOD);
        } catch (YarnException ex) {
          LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
        } catch (IOException ex) {
          LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
        } catch (InterruptedException ex) {
          LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
        }
      }
    }
  }

  @Ignore
  @Test
  public void testNextHeartbeat() throws Exception {
    RMUtilities.InitializeDB();
    MockRM rm = new MockRM(conf);
    rm.start();
    StringBuilder sb = new StringBuilder();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);

    Thread.sleep(2000);
    for (int i = 0; i < 1; i++) {
      NodeHeartbeatResponse resp = nm1.nodeHeartbeat(true);
      sb.append(resp.getNextheartbeat());
      sb.append("\n");
    }
    LOG.debug("HOP :: testNextHeartbeat results:" + sb.toString());
    rm.stop();
    Thread.sleep(2000);

  }

  @Ignore
  @Test
  public void testGetRMNodeBatch() throws Exception {
    RMUtilities.InitializeDB();
    RMNodeComps hopRMNode = RMUtilities.getRMNodeBatch("host1:1234");
    NodeHBResponse resp = hopRMNode.getHopNodeHBResponse();

  }

  @Ignore
  @Test(timeout = 50000)
  public void testGetRMNodePerformance() throws Exception {
    RMUtilities.InitializeDB();
    MockRM rm = new MockRM(conf);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, false);
    rm.start();
    String nodeId = "host1:1234";
    int numOfRetrievals = 500;
    MockNM nm1 = rm.registerNode(nodeId, 5120);

    nm1.nodeHeartbeat(BuilderUtils.
            newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0), 1,
        ContainerState.RUNNING);

    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(5000);
    float nonBatchTime = 0;
    for (int i = 0; i < numOfRetrievals; i++) {
      long start = System.currentTimeMillis();
      RMUtilities.getRMNode(nodeId, null, conf);
      nonBatchTime += System.currentTimeMillis() - start;
    }
    nonBatchTime = nonBatchTime / numOfRetrievals;
    LOG.debug("HOP :: nonBatchTime=" + nonBatchTime + " ms");
    Thread.sleep(2000);
    float batchTime = 0;
    for (int i = 0; i < numOfRetrievals; i++) {
      long start = System.currentTimeMillis();
      RMUtilities.getRMNodeBatch(nodeId);
      batchTime += System.currentTimeMillis() - start;
    }
    batchTime = batchTime / numOfRetrievals;
    LOG.debug("HOP :: nonBatchTime=" + nonBatchTime + " ms");
    LOG.debug("HOP :: BatchTime=" + batchTime + " ms");
    rm.stop();
    Thread.sleep(2000);
  }
  
  @Ignore
  @Test
  public void testRPCValidation() throws IOException, Exception {
    RMUtilities.InitializeDB();
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD, 5000);
    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    nm1.nodeHeartbeat(false);

    rm.stop();
    try {
      Thread.sleep(5000);
      rm.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Ignore
  @Test
  public void testPendingEvent() throws StorageException, IOException {
    RMUtilities.InitializeDB();
    List<PendingEvent> newPendingEvents = new ArrayList<PendingEvent>();
    newPendingEvents.add(new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_ADDED,
        TablesDef.PendingEventTableDef.NEW, 3));
    newPendingEvents.add(new PendingEvent("c", TablesDef.PendingEventTableDef.NODE_ADDED,
        TablesDef.PendingEventTableDef.NEW, 1));
    newPendingEvents.add(
        new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_UPDATED,
            TablesDef.PendingEventTableDef.NEW, 0));
    newPendingEvents.add(
        new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_UPDATED,
            TablesDef.PendingEventTableDef.NEW, 2));
    RMUtilities.setPendingEvents(newPendingEvents);
  }

  @Ignore
  @Test
  public void testConfiguration() throws IOException {
    RMUtilities.InitializeDB();
    boolean distributedRTEnabled = false;
    Assert.assertEquals(distributedRTEnabled,
        conf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
            YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
    distributedRTEnabled = true;
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    Assert.assertEquals(distributedRTEnabled,
        conf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
            YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
  }

  @Ignore
  @Test
  public void testGetRMNode() throws IOException {
    RMUtilities.InitializeDB();
    RMNode rmNode = RMUtilities.getRMNode("h1:1234", null, conf);
    LOG.info("HOP :: rmNode.uci-" + ((RMNodeImpl) rmNode).getQueue());
  }


  private class RTClientWorker implements Runnable {

    private boolean heartbeat = false;
    private final Collection<NodeId> nodeIds;
    private final int workerStartId;

    public RTClientWorker(boolean heartbeat, Collection<NodeId> nodeId,
        int workerStartId) {
      this.workerStartId = workerStartId;
      this.heartbeat = heartbeat;
      this.nodeIds = nodeId;
    }

    @Override
    public void run() {
      if (!heartbeat) {
        registerClient(rt, nodeIds);
      } else {
        heartbeatClient(rt, nodeIds);
        LOG.debug("HOP :: Worker " + workerStartId + " finished heartbeating");
      }
    }

  }
}
