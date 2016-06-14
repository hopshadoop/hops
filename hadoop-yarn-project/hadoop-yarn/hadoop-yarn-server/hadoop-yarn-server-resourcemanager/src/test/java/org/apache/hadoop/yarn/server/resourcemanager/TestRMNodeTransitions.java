/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionState.TransactionType;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.ha.common.TransactionStateManager;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.api.records.ContainerState;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRMNodeTransitions {

  RMNodeImpl node;
  private RMContext rmContext;
  private YarnScheduler scheduler;
  private SchedulerEventType eventType;
  private List<ContainerStatus> completedContainers =
      new ArrayList<ContainerStatus>();

  private final class TestSchedulerEventDispatcher
      implements EventHandler<SchedulerEvent> {

    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }

  private NodesListManagerEvent nodesListManagerEvent = null;

  private class TestNodeListManagerEventDispatcher
      implements EventHandler<NodesListManagerEvent> {

    @Override
    public void handle(NodesListManagerEvent event) {
      nodesListManagerEvent = event;
    }
  }

  @Before
  public void setUp() throws Exception {
    InlineDispatcher rmDispatcher = new InlineDispatcher();

    YarnConfiguration conf = new YarnConfiguration();
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    RMUtilities.InitializeDB();
    TransactionStateManager tsm = new TransactionStateManager();
    tsm.init(conf);
    tsm.start();
    rmContext = new RMContextImpl(rmDispatcher, null, null, null,
        mock(DelegationTokenRenewer.class), null, null, null, conf, tsm);
    NodesListManager nodesListManager = mock(NodesListManager.class);
    HostsFileReader reader = mock(HostsFileReader.class);
    when(nodesListManager.getHostsReader()).thenReturn(reader);
    ((RMContextImpl) rmContext).setNodesListManager(nodesListManager);
    scheduler = mock(YarnScheduler.class);
    doAnswer(new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            final SchedulerEvent event =
                (SchedulerEvent) (invocation.getArguments()[0]);
            eventType = event.getType();
            if (eventType == SchedulerEventType.NODE_UPDATE) {
              List<UpdatedContainerInfo> lastestContainersInfoList =
                  ((NodeUpdateSchedulerEvent) event).getRMNode()
                      .pullContainerUpdates(event.getTransactionState());
              for (UpdatedContainerInfo lastestContainersInfo : lastestContainersInfoList) {
                completedContainers
                    .addAll(lastestContainersInfo.getCompletedContainers());
              }
            }
            return null;
          }
        }).when(scheduler).handle(any(SchedulerEvent.class));

    rmDispatcher
        .register(SchedulerEventType.class, new TestSchedulerEventDispatcher());

    rmDispatcher.register(NodesListManagerEventType.class,
        new TestNodeListManagerEventDispatcher());

    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    node = new RMNodeImpl(nodeId, rmContext, nodeId.getHost(), 0, 0, null, null,
        null);
    nodesListManagerEvent = null;

  }

  @After
  public void tearDown() throws Exception {
  }

  private RMNodeStatusEvent getMockRMNodeStatusEvent() {
    NodeHeartbeatResponse response = mock(NodeHeartbeatResponse.class);

    NodeHealthStatus healthStatus = mock(NodeHealthStatus.class);
    Boolean yes = new Boolean(true);
    doReturn(yes).when(healthStatus).getIsNodeHealthy();

    RMNodeStatusEvent event = mock(RMNodeStatusEvent.class);
    doReturn(healthStatus).when(event).getNodeHealthStatus();
    doReturn(response).when(event).getLatestResponse();
    doReturn(RMNodeEventType.STATUS_UPDATE).when(event).getType();
    doReturn(new TransactionStateImpl( TransactionType.RM)).when(event)
        .getTransactionState();
    return event;
  }

  @Test(timeout = 5000)
  public void testExpiredContainer() {
    // Start the node
    node.handle(new RMNodeEvent(null, RMNodeEventType.STARTED, null));
    verify(scheduler).handle(any(NodeAddedSchedulerEvent.class));

    // Expire a container
    ContainerId completedContainerId = BuilderUtils.newContainerId(BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0),
        0);
    node.handle(
        new RMNodeCleanContainerEvent(null, completedContainerId, null));
    Assert.assertEquals(1, node.getContainersToCleanUp().size());

    // Now verify that scheduler isn't notified of an expired container
    // by checking number of 'completedContainers' it got in the previous event
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent();
    ContainerStatus containerStatus = mock(ContainerStatus.class);
    doReturn(completedContainerId).when(containerStatus).getContainerId();
    doReturn(Collections.singletonList(containerStatus)).
        when(statusEvent).getContainers();
    node.handle(statusEvent);
    /* Expect the scheduler call handle function 2 times
     * 1. RMNode status from new to Running, handle the add_node event
     * 2. handle the node update event
     */
    verify(scheduler, times(2)).handle(any(NodeUpdateSchedulerEvent.class));
  }

  @Test(timeout = 500000)
  public void testContainerUpdate() throws InterruptedException, IOException {
    //Start the node
    node.handle(new RMNodeEvent(null, RMNodeEventType.STARTED,
        new TransactionStateImpl(TransactionType.RM)));
    //If Distributed RT is enabled, this is the only way to let the scheduler
    //pick up the event, the PendingEvent retrieval does not invoke the
    //Mock Scheduler
    Configuration yarnconf = new YarnConfiguration();
    if (yarnconf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
        YarnConfiguration.DEFAULT_DISTRIBUTED_RM)) {
      scheduler.handle(new NodeAddedSchedulerEvent(node,
          new TransactionStateImpl( TransactionType.RM)));
    }
    NodeId nodeId = BuilderUtils.newNodeId("localhost:1", 1);
    RMNodeImpl node2 =
        new RMNodeImpl(nodeId, rmContext, "test", 0, 0, null, null, null);
    node2.handle(new RMNodeEvent(null, RMNodeEventType.STARTED,
        new TransactionStateImpl(TransactionType.RM)));
    //If Distributed RT is enabled, this is the only way to let the scheduler
    //pick up the event, the PendingEvent retrieval does not invoke the
    //Mock Scheduler
    if (yarnconf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
        YarnConfiguration.DEFAULT_DISTRIBUTED_RM)) {
      scheduler.handle(new NodeAddedSchedulerEvent(node2,
          new TransactionStateImpl( TransactionType.RM)));
    }
    ContainerId completedContainerIdFromNode1 = BuilderUtils.newContainerId(
        BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0),
        0);
    ContainerId completedContainerIdFromNode2_1 = BuilderUtils.newContainerId(
        BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(1, 1), 1),
        1);
    ContainerId completedContainerIdFromNode2_2 = BuilderUtils.newContainerId(
        BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(1, 1), 1),
        2);

    RMNodeStatusEvent statusEventFromNode1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEventFromNode2_1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEventFromNode2_2 = getMockRMNodeStatusEvent();


    ContainerStatus containerStatusFromNode1 = mock(ContainerStatus.class);
    ContainerStatus containerStatusFromNode2_1 = mock(ContainerStatus.class);
    ContainerStatus containerStatusFromNode2_2 = mock(ContainerStatus.class);

    doReturn(completedContainerIdFromNode1).when(containerStatusFromNode1)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode1))
        .when(statusEventFromNode1).getContainers();
    doReturn(ContainerState.COMPLETE).when(containerStatusFromNode1).getState();
    node.handle(statusEventFromNode1);
    //If Distributed RT is enabled, this is the only way to let the scheduler
    //pick up the event, the PendingEvent retrieval does not invoke the
    //Mock Scheduler
    if (yarnconf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
        YarnConfiguration.DEFAULT_DISTRIBUTED_RM)) {
      scheduler.handle(new NodeUpdateSchedulerEvent(node,
          new TransactionStateImpl( TransactionType.RM)));
    }
    //ts.decCounter("test");
    Assert.assertEquals(1, completedContainers.size());
    Assert.assertEquals(completedContainerIdFromNode1,
        completedContainers.get(0).getContainerId());

    completedContainers.clear();

    doReturn(completedContainerIdFromNode2_1).when(containerStatusFromNode2_1)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode2_1))
        .when(statusEventFromNode2_1).getContainers();
    doReturn(ContainerState.COMPLETE).when(containerStatusFromNode2_1).getState();
 
    doReturn(completedContainerIdFromNode2_2).when(containerStatusFromNode2_2)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode2_2))
        .when(statusEventFromNode2_2).getContainers();
     doReturn(ContainerState.COMPLETE).when(containerStatusFromNode2_2).getState();
     
    node2.setNextHeartBeat(false);
    node2.handle(statusEventFromNode2_1);
    //If Distributed RT is enabled, this is the only way to let the scheduler
    //pick up the event, the PendingEvent retrieval does not invoke the
    //Mock Scheduler
    if (yarnconf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
        YarnConfiguration.DEFAULT_DISTRIBUTED_RM)) {
      scheduler.handle(new NodeUpdateSchedulerEvent(node2,
          new TransactionStateImpl( TransactionType.RM)));
    }

    node2.setNextHeartBeat(true);
    node2.handle(statusEventFromNode2_2);
    //If Distributed RT is enabled, this is the only way to let the scheduler
    //pick up the event, the PendingEvent retrieval does not invoke the
    //Mock Scheduler
    if (yarnconf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
        YarnConfiguration.DEFAULT_DISTRIBUTED_RM)) {
      scheduler.handle(new NodeUpdateSchedulerEvent(node2,
          new TransactionStateImpl( TransactionType.RM)));
    }
    //ts2.decCounter("test");
    Assert.assertEquals(2, completedContainers.size());
    Assert.assertEquals(completedContainerIdFromNode2_1,
        completedContainers.get(0).getContainerId());
    Assert.assertEquals(completedContainerIdFromNode2_2,
        completedContainers.get(1).getContainerId());
  }

  @Test(timeout = 500000)
  public void testStatusChange() {
    //Start the node
    node.handle(new RMNodeEvent(null, RMNodeEventType.STARTED,
        new TransactionStateImpl(TransactionType.RM)));
    //Add info to the queue first
    node.setNextHeartBeat(false);

    ContainerId completedContainerId1 = BuilderUtils.newContainerId(BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0),
        0);
    ContainerId completedContainerId2 = BuilderUtils.newContainerId(BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(1, 1), 1),
        1);

    RMNodeStatusEvent statusEvent1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEvent2 = getMockRMNodeStatusEvent();

    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    ContainerStatus containerStatus2 = mock(ContainerStatus.class);

    doReturn(completedContainerId1).when(containerStatus1).getContainerId();
    doReturn(Collections.singletonList(containerStatus1)).when(statusEvent1)
        .getContainers();
    doReturn(ContainerState.COMPLETE).when(containerStatus1).getState();
    doReturn(completedContainerId2).when(containerStatus2).getContainerId();
    doReturn(Collections.singletonList(containerStatus2)).when(statusEvent2)
        .getContainers();
    doReturn(ContainerState.COMPLETE).when(containerStatus2).getState();
    verify(scheduler, times(1)).handle(any(NodeUpdateSchedulerEvent.class));
    node.handle(statusEvent1);
    node.handle(statusEvent2);
    verify(scheduler, times(1)).handle(any(NodeUpdateSchedulerEvent.class));
    Assert.assertEquals(2, node.getQueueSize());
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals(0, node.getQueueSize());
  }

  @Test(timeout = 60000)
  public void testRunningExpire() throws IOException {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE,
        new TransactionStateImpl( TransactionType.RM)));
    Assert
        .assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 1, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test(timeout = 60000)
  public void testUnhealthyExpire() throws IOException {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 1, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy - 1,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test(timeout = 60000)
  public void testUnhealthyExpireForSchedulerRemove() throws IOException {
    RMNodeImpl node = getUnhealthyNode();
    verify(scheduler, times(2)).handle(any(NodeRemovedSchedulerEvent.class));
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE,
        new TransactionStateImpl( TransactionType.RM)));
    verify(scheduler, times(2)).handle(any(NodeRemovedSchedulerEvent.class));
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test(timeout = 60000)
  public void testRunningDecommission() throws IOException {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.DECOMMISSION,
        new TransactionStateImpl( TransactionType.RM)));
    Assert
        .assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned + 1,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test(timeout = 60000)
  public void testUnhealthyDecommission() throws IOException {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.DECOMMISSION,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy - 1,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned + 1,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test(timeout = 60000)
  public void testRunningRebooting() throws IOException {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.REBOOTING,
        new TransactionStateImpl( TransactionType.RM)));
    Assert
        .assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted + 1,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test(timeout = 60000)
  public void testUnhealthyRebooting() throws IOException {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.REBOOTING,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy - 1,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted + 1,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test(timeout = 2000000)
  public void testUpdateHeartbeatResponseForCleanup() throws IOException {
    RMNodeImpl node = getRunningNode();
    NodeId nodeId = node.getNodeID();

    int rpcID = HopYarnAPIUtilities.getRPCID();
    byte[] allNMRequestData = new byte[1];
    allNMRequestData[0] = 0xA;
    try {
      RMUtilities
          .persistAppMasterRPC(rpcID, RPC.Type.RegisterNM, allNMRequestData);
    } catch (IOException ex) {
      Logger.getLogger(TestRMNodeTransitions.class.getName())
          .log(Level.SEVERE, null, ex);
    }
    TransactionState ts = new TransactionStateImpl(TransactionType.RM);

    // Expire a container
    ContainerId completedContainerId = BuilderUtils.newContainerId(BuilderUtils
            .newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0),
        0);
    node.handle(
        new RMNodeCleanContainerEvent(nodeId, completedContainerId, ts));
    ts.decCounter(TransactionState.TransactionType.INIT);
    Assert.assertEquals(1, node.getContainersToCleanUp().size());

    rpcID = HopYarnAPIUtilities.getRPCID();
    allNMRequestData = new byte[1];
    allNMRequestData[0] = 0xA;
    try {
      RMUtilities.persistAppMasterRPC(rpcID, RPC.Type.FinishApplicationMaster,
          allNMRequestData);
    } catch (IOException ex) {
      Logger.getLogger(TestRMNodeTransitions.class.getName())
          .log(Level.SEVERE, null, ex);
    }
    TransactionState ts2 = new TransactionStateImpl(
        TransactionType.RM);//TransactionStateRM.newInstance(rpcID);

    // Finish an application
    ApplicationId finishedAppId = BuilderUtils.newApplicationId(0, 1);
    node.handle(new RMNodeCleanAppEvent(nodeId, finishedAppId, ts2));
    ts2.decCounter(TransactionState.TransactionType.INIT);
    Assert.assertEquals(1, node.getAppsToCleanup().size());
    rpcID = HopYarnAPIUtilities.getRPCID();
    allNMRequestData = new byte[1];
    allNMRequestData[0] = 0xA;
    try {
      RMUtilities.persistAppMasterRPC(rpcID, RPC.Type.FinishApplicationMaster,
          allNMRequestData);
    } catch (IOException ex) {
      Logger.getLogger(TestRMNodeTransitions.class.getName())
          .log(Level.SEVERE, null, ex);
    }
    TransactionState ts3 = new TransactionStateImpl(
        TransactionType.RM);//TransactionStateRM.newInstance(rpcID);

    // Verify status update does not clear containers/apps to cleanup
    // but updating heartbeat response for cleanup does
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent();
    RMNodeStatusEvent se = new RMNodeStatusEvent(statusEvent.getNodeId(),
        statusEvent.getNodeHealthStatus(), statusEvent.getContainers(),
        statusEvent.getKeepAliveAppIds(), statusEvent.getLatestResponse(), ts3);
    node.handle(se);

    ts3.decCounter(TransactionState.TransactionType.INIT);

    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    Assert.assertEquals(1, node.getAppsToCleanup().size());
    NodeHeartbeatResponse hbrsp =
        Records.newRecord(NodeHeartbeatResponse.class);
    node.updateNodeHeartbeatResponseForCleanup(hbrsp, ts);
    Assert.assertEquals(0, node.getContainersToCleanUp().size());
    Assert.assertEquals(0, node.getAppsToCleanup().size());
    Assert.assertEquals(1, hbrsp.getContainersToCleanup().size());
    Assert.assertEquals(completedContainerId,
        hbrsp.getContainersToCleanup().get(0));
    Assert.assertEquals(1, hbrsp.getApplicationsToCleanup().size());
    Assert.assertEquals(finishedAppId, hbrsp.getApplicationsToCleanup().get(0));
  }

  private RMNodeImpl getRunningNode() throws IOException {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    Resource capability = Resource.newInstance(4096, 4);

    int rpcID = HopYarnAPIUtilities.getRPCID();
    byte[] allNMRequestData = new byte[10];
    try {
      RMUtilities
          .persistAppMasterRPC(rpcID, RPC.Type.RegisterNM, allNMRequestData);
    } catch (IOException ex) {
      Logger.getLogger(TestRMNodeTransitions.class.getName())
          .log(Level.SEVERE, null, ex);
    }
    TransactionState ts = new TransactionStateImpl(
        TransactionType.RM);//TransactionStateRM.newInstance(rpcID);

    RMNodeImpl node =
        new RMNodeImpl(nodeId, rmContext, nodeId.getHost(), 0, 0, null,
            ResourceOption.newInstance(capability,
                RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT), null);
    ((TransactionStateImpl) ts).getRMContextInfo()
        .toAddActiveRMNode(nodeId, node, 1);
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.STARTED, ts));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    return node;
  }

  private RMNodeImpl getUnhealthyNode() throws IOException {
    RMNodeImpl node = getRunningNode();
    NodeHealthStatus status =
        NodeHealthStatus.newInstance(false, "sick", System.currentTimeMillis());
    node.handle(new RMNodeStatusEvent(node.getNodeID(), status,
        new ArrayList<ContainerStatus>(), null, null,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals(NodeState.UNHEALTHY, node.getState());
    return node;
  }

  private RMNodeImpl getNewNode() {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    RMNodeImpl node =
        new RMNodeImpl(nodeId, rmContext, nodeId.getHost(), 0, 0, null, null,
            null);
    return node;
  }

  @Test(timeout = 60000)
  public void testAdd() {
    RMNodeImpl node = getNewNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(
        new RMNodeEvent(node.getNodeID(), RMNodeEventType.STARTED, null));
    Assert
        .assertEquals("Active Nodes", initialActive + 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    Assert.assertNotNull(nodesListManagerEvent);
    Assert.assertEquals(NodesListManagerEventType.NODE_USABLE,
        nodesListManagerEvent.getType());
  }

  @Test(timeout = 60000)
  public void testReconnect() throws IOException {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeReconnectEvent(node.getNodeID(), node,
        new TransactionStateImpl( TransactionType.RM)));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes", initialUnhealthy,
        cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes", initialDecommissioned,
        cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes", initialRebooted,
        cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    Assert.assertNotNull(nodesListManagerEvent);
    Assert.assertEquals(NodesListManagerEventType.NODE_USABLE,
        nodesListManagerEvent.getType());
  }
}
