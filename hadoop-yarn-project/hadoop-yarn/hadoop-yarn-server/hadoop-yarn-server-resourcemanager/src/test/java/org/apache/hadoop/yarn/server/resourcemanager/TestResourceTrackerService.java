/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.DBUtilityTests;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestResourceTrackerService {
  private static final Log LOG = LogFactory.getLog(TestResourceTrackerService.class);
  private final static File TEMP_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "decommision");
  private final File hostFile = new File(TEMP_DIR + File.separator + "hostFile.txt");
  private MockRM rm;

  /**
   * Test RM read NM next heartBeat Interval correctly from Configuration file,
   * and NM get next heartBeat Interval from RM correctly
   */
  @Test (timeout = 50000)
  public void testGetNextHeartBeatInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
    DBUtility.InitializeDB();
    
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat2.getNextHeartBeatInterval());

  }

  /**
   * Decommissioning using a pre-configured include hosts file
   */
  @Test
  public void testDecommissionWithIncludeHosts() throws Exception {

    writeToHostsFile("localhost", "host1", "host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int metricCount = metrics.getNumDecommisionedNMs();

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host1", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkDecommissionedNMCount(rm, ++metricCount);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert
        .assertEquals(1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());

    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("Node is not decommisioned.", NodeAction.SHUTDOWN
        .equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals(metricCount, ClusterMetrics.getMetrics()
      .getNumDecommisionedNMs());
  }

  /**
   * Decommissioning using a pre-configured exclude hosts file
   */
  @Test
  public void testDecommissionWithExcludeHosts() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    rm = new MockRM(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext().getDispatcher();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);

    rm.drainEvents();

    int metricCount = ClusterMetrics.getMetrics().getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host2", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkDecommissionedNMCount(rm, metricCount + 2);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));
    rm.drainEvents();

    writeToHostsFile("");
    rm.getNodesListManager().refreshNodes(conf);

    nm3 = rm.registerNode("localhost:4433", 1024);
    rm.drainEvents();
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    // decommissined node is 1 since 1 node is rejoined after updating exclude
    // file
    checkDecommissionedNMCount(rm, metricCount + 1);
  }

  /**
  * Decommissioning using a post-configured include hosts file
  */
  @Test
  public void testAddNewIncludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkDecommissionedNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been decomissioned.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals("Node should have been decomissioned but is in state" +
        nodeHeartbeat.getNodeAction(),
        NodeAction.SHUTDOWN, nodeHeartbeat.getNodeAction());
  }
  
  /**
   * Decommissioning using a post-configured exclude hosts file
   */
  @Test
  public void testAddNewExcludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkDecommissionedNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been decomissioned.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals("Node should have been decomissioned but is in state" +
        nodeHeartbeat.getNodeAction(),
        NodeAction.SHUTDOWN, nodeHeartbeat.getNodeAction());
  }

  @Test
  public void testNodeRegistrationSuccess() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL,response.getNodeAction());
  }

  @Test
  public void testDistributedNodeRegistrationSuccess() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH, "target/test-classes/RT_EventAPIConfig.ini");
    conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH, "target/test-classes/RM_EventAPIConfig.ini");
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
    
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Thread.sleep(100);
    Assert.assertEquals(NodeAction.NORMAL,response.getNodeAction());
    //check that the rmnode object is of the good type 
    Assert.assertTrue(rm.getRMContext().getRMNodes().get(nodeId) instanceof RMNodeImplDist);
    //check that datas are commited to the database 
    //load
    Assert.assertEquals(1, DBUtility.getAllLoads().get(rm.getRMContext().getGroupMembershipService().getRMId()).getLoad());
    //rmnode
    Map<String, io.hops.metadata.yarn.entity.RMNode> rmNodesInDb= DBUtilityTests.getAllRMNodess();
    Assert.assertEquals(1, rmNodesInDb.size());
    io.hops.metadata.yarn.entity.RMNode rmNode = rmNodesInDb.get(nodeId.toString());
    Assert.assertEquals("RUNNING", rmNode.getCurrentState());
    //resource
    Map<String, io.hops.metadata.yarn.entity.Resource> resourcesInDb = DBUtilityTests.getAllResources();
    Assert.assertEquals(1, resourcesInDb.size());
    io.hops.metadata.yarn.entity.Resource resource = resourcesInDb.get(nodeId.toString());
    Assert.assertEquals(1024, resource.getMemory());
    Assert.assertEquals(1, resource.getVirtualCores());
    Assert.assertEquals(1, resource.getGPUs());
    //pending event
    List<PendingEvent> pendingEventsInDb = DBUtilityTests.getAllPendingEvents();
    Assert.assertEquals(1, pendingEventsInDb.size());
    PendingEvent pendingEvent = pendingEventsInDb.get(0);
    Assert.assertEquals(PendingEvent.Type.NODE_ADDED, pendingEvent.getType());
    Assert.assertEquals(PendingEvent.Status.NEW, pendingEvent.getStatus()); 
  }
  
  @Test
  public void testDistributedNodeHeartBeat() throws Exception {
    testDistributedNodeRegistrationSuccess();
    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    NodeHeartbeatRequest request = Records.newRecord(NodeHeartbeatRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    NodeStatus status = NodeStatus.newInstance(nodeId, 0, new ArrayList<ContainerStatus>(),
            new ArrayList<ApplicationId>(), NodeHealthStatus.
                    newInstance(true, "healthreport", 0));
    request.setNodeStatus(status);
    request.setLastKnownContainerTokenMasterKey(new MasterKeyPBImpl());
    request.setLastKnownNMTokenMasterKey(new MasterKeyPBImpl());
    resourceTrackerService.nodeHeartbeat(request);
    
    request = Records.newRecord(NodeHeartbeatRequest.class);
    List<ContainerStatus> containerStatuses = new ArrayList<>();
    ContainerStatus containerStatus = ContainerStatus.newInstance(ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 1), 1),
            ContainerState.RUNNING, "ras", 0);
    containerStatuses.add(containerStatus);
    status = NodeStatus.newInstance(nodeId, 1, containerStatuses,
            new ArrayList<ApplicationId>(), NodeHealthStatus.
                    newInstance(true, "healthreport", 0));
    request.setNodeStatus(status);
    request.setLastKnownContainerTokenMasterKey(new MasterKeyPBImpl());
    request.setLastKnownNMTokenMasterKey(new MasterKeyPBImpl());
    resourceTrackerService.nodeHeartbeat(request);
    Thread.sleep(100);
    
    List<PendingEvent> pendingEventsInDB = DBUtilityTests.getAllPendingEvents();
    Assert.assertEquals(3, pendingEventsInDB.size());
    for (PendingEvent pendingEvent: pendingEventsInDB){
      if(pendingEvent.getId().getEventId()==2){
        Assert.assertEquals(PendingEvent.Status.SCHEDULER_FINISHED_PROCESSING, pendingEvent.getStatus());
        Assert.assertEquals( PendingEvent.Type.NODE_UPDATED, pendingEvent.getType());
      }
      if(pendingEvent.getId().getEventId()==3){
        Assert.assertEquals(PendingEvent.Status.SCHEDULER_NOT_FINISHED_PROCESSING, pendingEvent.getStatus());
        Assert.assertEquals(PendingEvent.Type.NODE_UPDATED, pendingEvent.getType());
      }
    }
    
    Map<String, Map<Integer, List<UpdatedContainerInfo>>> uciInDB = DBUtilityTests.getAllUCIs();
    Assert.assertEquals(1, uciInDB.size());
    Map<Integer, List<UpdatedContainerInfo>> subMap = uciInDB.get(nodeId.toString());
    Assert.assertEquals(1, subMap.size());
    List<UpdatedContainerInfo> subList = subMap.get(1);
    Assert.assertEquals(1, subList.size());
    UpdatedContainerInfo uci = subList.get(0);
    Assert.assertEquals(3, uci.getPendingEventId());
    Assert.assertEquals(containerStatus.getContainerId().toString(), uci.getContainerId());
    
    Map<String, io.hops.metadata.yarn.entity.ContainerStatus> containersStatusInDB = DBUtilityTests.getAllContainerStatus();
    Assert.assertEquals(1, containersStatusInDB.size());
    io.hops.metadata.yarn.entity.ContainerStatus cs = containersStatusInDB.get(
            containerStatus.getContainerId().toString());
    Assert.assertEquals(containerStatus.getState().toString(), cs.getState());
    Assert.assertEquals(3, cs.getPendingEventId());
  }
   
  @Test
  public void testNodeRegistrationVersionLessThanRM() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,"EqualToRM" );
    rm = new MockRM(conf);
    rm.start();
    String nmVersion = "1.9.9";

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(nmVersion);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert.assertTrue("Diagnostic message did not contain: 'Disallowed NodeManager " +
        "Version "+ nmVersion + ", is less than the minimum version'",
        response.getDiagnosticsMessage().contains("Disallowed NodeManager Version " +
            nmVersion + ", is less than the minimum version "));

  }

  @Test
  public void testNodeRegistrationFailure() throws Exception {
    writeToHostsFile("host1");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();
    
    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert
      .assertEquals(
        "Disallowed NodeManager from  host2, Sending SHUTDOWN signal to the NodeManager.",
        response.getDiagnosticsMessage());
  }

  @Test
  public void testSetRMIdentifierInRegistration() throws Exception {

    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    RegisterNodeManagerResponse response = nm.registerNode();

    // Verify the RMIdentifier is correctly set in RegisterNodeManagerResponse
    Assert.assertEquals(ResourceManager.getClusterTimeStamp(),
      response.getRMIdentifier());
  }

  @Test
  public void testNodeRegistrationWithMinimumAllocations() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "2048");
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "4");
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService
      = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = BuilderUtils.newNodeId("host", 1234);
    req.setNodeId(nodeId);

    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    RegisterNodeManagerResponse response1 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response1.getNodeAction());
    
    capability.setMemory(2048);
    capability.setVirtualCores(1);
    req.setResource(capability);
    RegisterNodeManagerResponse response2 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response2.getNodeAction());
    
    capability.setMemory(1024);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response3 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response3.getNodeAction());
    
    capability.setMemory(2048);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response4 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL,response4.getNodeAction());
  }

  @Test
  public void testReboot() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:1234", 2048);

    int initialMetricCount = ClusterMetrics.getMetrics().getNumRebootedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm2.nodeHeartbeat(
      new HashMap<ApplicationId, List<ContainerStatus>>(), true, -100);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals("Too far behind rm response id:0 nm response id:-100",
      nodeHeartbeat.getDiagnosticsMessage());
    checkRebootedNMCount(rm, ++initialMetricCount);
  }

  private void checkRebootedNMCount(MockRM rm2, int count)
      throws InterruptedException {
    
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumRebootedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The rebooted metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumRebootedNMs());
  }

  @Test
  public void testUnhealthyNodeStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnealthyNMCount(rm, nm1, true, 1);

    // node healthy again
    nm1.nodeHeartbeat(true);
    checkUnealthyNMCount(rm, nm1, false, 0);
  }
  
  private void checkUnealthyNMCount(MockRM rm, MockNM nm1, boolean health,
      int count) throws Exception {
    
    int waitCount = 0;
    while((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertFalse((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health);
    Assert.assertEquals("Unhealthy metrics not incremented", count,
        ClusterMetrics.getMetrics().getUnhealthyNMs());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHandleContainerStatusInvalidCompletions() throws Exception {
    rm = new MockRM(new YarnConfiguration());
    rm.start();

    EventHandler handler =
        spy(rm.getRMContext().getDispatcher().getEventHandler());

    // Case 1: Unmanaged AM
    RMApp app = rm.submitApp(1024, true);

    // Case 1.1: AppAttemptId is null
    NMContainerStatus report =
        NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event) any());

    // Case 1.2: Master container is null
    RMAppAttemptImpl currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event)any());

    // Case 2: Managed AM
    app = rm.submitApp(1024);

    // Case 2.1: AppAttemptId is null
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());

    // Case 2.2: Master container is null
    currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
      ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0),
      ContainerState.COMPLETE, Resource.newInstance(1024, 1),
      "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());
  }

  @Test
  public void testReconnectNode() throws Exception {
    rm = new MockRM() {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new SchedulerEventDispatcher(this.scheduler) {
          @Override
          public void handle(SchedulerEvent event) {
            scheduler.handle(event);
          }
        };
      }
    };
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext().getDispatcher();
    
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 5120);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(false);
    rm.drainEvents();
    checkUnealthyNMCount(rm, nm2, true, 1);
    final int expectedNMs = ClusterMetrics.getMetrics().getNumActiveNMs();
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    // TODO Metrics incorrect in case of the FifoScheduler
    Assert.assertEquals(5120, metrics.getAvailableMB());

    // reconnect of healthy node
    nm1 = rm.registerNode("host1:1234", 5120);
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnealthyNMCount(rm, nm2, true, 1);

    // reconnect of unhealthy node
    nm2 = rm.registerNode("host2:5678", 5120);
    response = nm2.nodeHeartbeat(false);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    rm.drainEvents();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnealthyNMCount(rm, nm2, true, 1);
    
    // unhealthy node changed back to healthy
    nm2 = rm.registerNode("host2:5678", 5120);
    rm.drainEvents();
    response = nm2.nodeHeartbeat(true);
    response = nm2.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertEquals(5120 + 5120, metrics.getAvailableMB());

    // reconnect of node with changed capability
    nm1 = rm.registerNode("host2:5678", 10240);
    rm.drainEvents();
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 10240, metrics.getAvailableMB());

    // reconnect of node with changed capability and running applications
    List<ApplicationId> runningApps = new ArrayList<ApplicationId>();
    runningApps.add(ApplicationId.newInstance(1, 0));
    nm1 = rm.registerNode("host2:5678", 15360, 2, runningApps);
    rm.drainEvents();
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());
    
    // reconnect healthy node changing http port
    nm1 = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    nm1.setHttpPort(3);
    nm1.registerNode();
    rm.drainEvents();
    response = nm1.nodeHeartbeat(true);
    response = nm1.nodeHeartbeat(true);
    rm.drainEvents();
    RMNode rmNode = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    Assert.assertEquals(3, rmNode.getHttpPort());
    Assert.assertEquals(5120, rmNode.getTotalCapability().getMemory());
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());

  }

  @Test(timeout = 30000)
  public void testInitDecommMetric() throws Exception {
    testInitDecommMetricHelper(true);
    testInitDecommMetricHelper(false);
  }

  public void testInitDecommMetricHelper(boolean hasIncludeList)
      throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    File excludeHostFile =
        new File(TEMP_DIR + File.separator + "excludeHostFile.txt");
    writeToHostsFile(excludeHostFile, "host1");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());

    if (hasIncludeList) {
      writeToHostsFile(hostFile, "host1", "host2");
      conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
          hostFile.getAbsolutePath());
    }
    rm.getNodesListManager().refreshNodes(conf);
    rm.drainEvents();
    rm.stop();

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    nm1 = rm1.registerNode("host1:1234", 5120);
    nm2 = rm1.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals("Number of Decommissioned nodes should be 1",
        1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    Assert.assertEquals("The inactiveRMNodes should contain an entry for the" +
        "decommissioned node",
        1, rm1.getRMContext().getInactiveRMNodes().size());
    excludeHostFile =
        new File(TEMP_DIR + File.separator + "excludeHostFile.txt");
    writeToHostsFile(excludeHostFile, "");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    rm1.getNodesListManager().refreshNodes(conf);
    nm1 = rm1.registerNode("host1:1234", 5120);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    rm1.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should have " +
            "decremented to 0",
        0, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    Assert.assertEquals("The active nodes metric should be 2",
        2, ClusterMetrics.getMetrics().getNumActiveNMs());
    Assert.assertEquals("The inactive RMNodes entry should have been removed",
        0, rm1.getRMContext().getInactiveRMNodes().size());
    rm1.drainEvents();
    rm1.stop();
  }

  @Test(timeout = 30000)
  public void testInitDecommMetricNoRegistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    //host3 will not register or heartbeat
    File excludeHostFile =
        new File(TEMP_DIR + File.separator + "excludeHostFile.txt");
    writeToHostsFile(excludeHostFile, "host3", "host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeHostFile.getAbsolutePath());
    writeToHostsFile(hostFile, "host1", "host2");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    rm.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should be 1 ",
        1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    rm.stop();

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    rm1.getNodesListManager().refreshNodes(conf);
    rm1.drainEvents();
    Assert.assertEquals("The decommissioned nodes metric should be 2 ",
        2, ClusterMetrics.getMetrics().getNumDecommisionedNMs());
    rm1.stop();
  }

  private void writeToHostsFile(String... hosts) throws IOException {
   writeToHostsFile(hostFile, hosts);
  }

  private void writeToHostsFile(File file, String... hosts)
      throws IOException {
    if (!file.exists()) {
      TEMP_DIR.mkdirs();
      file.createNewFile();
    }
    FileOutputStream fStream = null;
    try {
      fStream = new FileOutputStream(file);
      for (int i = 0; i < hosts.length; i++) {
        fStream.write(hosts[i].getBytes());
        fStream.write("\n".getBytes());
      }
    } finally {
      if (fStream != null) {
        IOUtils.closeStream(fStream);
        fStream = null;
      }
    }
  }

  private void checkDecommissionedNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumDecommisionedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals(count, ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs());
    Assert.assertEquals("The decommisioned metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumDecommisionedNMs());
  }

  @After
  public void tearDown() {
    if (hostFile != null && hostFile.exists()) {
      hostFile.delete();
    }

    ClusterMetrics.destroy();
    if (rm != null) {
      rm.stop();
    }

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("ClusterMetrics") != null) {
      DefaultMetricsSystem.shutdown();
    }
  }
}
