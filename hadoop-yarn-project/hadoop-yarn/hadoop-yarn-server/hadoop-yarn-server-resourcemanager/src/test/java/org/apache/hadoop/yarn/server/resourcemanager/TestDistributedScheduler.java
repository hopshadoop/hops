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
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.util.DBUtility;
import io.hops.util.DBUtilityTests;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.quota.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplNotDist;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author gautier
 */
public class TestDistributedScheduler {
  
  private RMContext rmContext;
  private YarnScheduler scheduler;
  private SchedulerEventType eventType;
  private List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
  
  private final class TestSchedulerEventDispatcher implements
  EventHandler<SchedulerEvent> {
    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }

  private NodesListManagerEvent nodesListManagerEvent = null;
  
  private class TestNodeListManagerEventDispatcher implements
      EventHandler<NodesListManagerEvent> {
    
    @Override
    public void handle(NodesListManagerEvent event) {
      nodesListManagerEvent = event;
    }

  }
  
  @Before
  public void setUp() throws Exception {
    Configuration conf = new YarnConfiguration();
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
    
    InlineDispatcher rmDispatcher = new InlineDispatcher();
    
    rmContext =
        new RMContextImpl(rmDispatcher, null, null, null,
            mock(DelegationTokenRenewer.class), null, null, null, null, null);
    NodesListManager nodesListManager = mock(NodesListManager.class);
    HostsFileReader reader = mock(HostsFileReader.class);
    when(nodesListManager.getHostsReader()).thenReturn(reader);
    ((RMContextImpl) rmContext).setNodesListManager(nodesListManager);
    ContainersLogsService containerLogsService = mock(ContainersLogsService.class);
    ((RMContextImpl)rmContext).setContainersLogsService(containerLogsService);
    scheduler = mock(YarnScheduler.class);
    doAnswer(
        new Answer<Void>() {

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            final SchedulerEvent event = (SchedulerEvent)(invocation.getArguments()[0]);
            eventType = event.getType();
            if (eventType == SchedulerEventType.NODE_UPDATE) {
              List<UpdatedContainerInfo> lastestContainersInfoList = 
                  ((NodeUpdateSchedulerEvent)event).getRMNode().pullContainerUpdates();
              for(UpdatedContainerInfo lastestContainersInfo : lastestContainersInfoList) {
            	  completedContainers.addAll(lastestContainersInfo.getCompletedContainers()); 
              }
            }
            return null;
          }
        }
        ).when(scheduler).handle(any(SchedulerEvent.class));
    
    rmDispatcher.register(SchedulerEventType.class, 
        new TestSchedulerEventDispatcher());
    
    rmDispatcher.register(NodesListManagerEventType.class,
        new TestNodeListManagerEventDispatcher());

    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    nodesListManagerEvent =  null;

  }
  
  @Test(timeout=200000)
  public void testUpdateHeartbeatResponseForCleanup() throws Exception {
    RMNodeImpl node = getRunningNode();
    NodeId nodeId = node.getNodeID();

    // Expire a container
		ContainerId completedContainerId = BuilderUtils.newContainerId(
				BuilderUtils.newApplicationAttemptId(
						BuilderUtils.newApplicationId(0, 0), 0), 0);
    node.handle(new RMNodeCleanContainerEvent(nodeId, completedContainerId));
    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    //check DB
    Assert.assertEquals(1, DBUtilityTests.getAllContainersToCleanUp().size());
    Assert.assertEquals(1, DBUtilityTests.getAllContainersToCleanUp().get(node.getNodeID().toString()).size());
    Assert.assertEquals(1, DBUtilityTests.getAllNextHeartbeat().size());
    
    // Finish an application
    ApplicationId finishedAppId = BuilderUtils.newApplicationId(0, 1);
    node.handle(new RMNodeCleanAppEvent(nodeId, finishedAppId));
    Assert.assertEquals(1, node.getAppsToCleanup().size());

    Thread.sleep(500);
    //check DB
    Assert.assertEquals(1, DBUtilityTests.getAllAppsToCleanup().size());
    Assert.assertEquals(1, DBUtilityTests.getAllAppsToCleanup().get(node.getNodeID().toString()).size());
    
    // Verify status update does not clear containers/apps to cleanup
    // but updating heartbeat response for cleanup does
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent();
    node.handle(statusEvent);
    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    Assert.assertEquals(1, node.getAppsToCleanup().size());
    //Check DB
    Assert.assertEquals(1, DBUtilityTests.getAllContainersToCleanUp().size());
    Assert.assertEquals(1, DBUtilityTests.getAllContainersToCleanUp().get(node.getNodeID().toString()).size());
    Assert.assertEquals(1, DBUtilityTests.getAllAppsToCleanup().size());
    Assert.assertEquals(1, DBUtilityTests.getAllAppsToCleanup().get(node.getNodeID().toString()).size());
    Assert.assertEquals(0, DBUtilityTests.getAllNextHeartbeat().size());
    
    NodeHeartbeatResponse hbrsp = Records.newRecord(NodeHeartbeatResponse.class);
    node.updateNodeHeartbeatResponseForCleanup(hbrsp);
    Assert.assertEquals(0, node.getContainersToCleanUp().size());
    Assert.assertEquals(0, node.getAppsToCleanup().size());
    Assert.assertEquals(1, hbrsp.getContainersToCleanup().size());
    Assert.assertEquals(completedContainerId, hbrsp.getContainersToCleanup().get(0));
    Assert.assertEquals(1, hbrsp.getApplicationsToCleanup().size());
    Assert.assertEquals(finishedAppId, hbrsp.getApplicationsToCleanup().get(0));
    //Check DB
    // DB operations are done asynchronously. Sleep for a while just to be sure
    Thread.sleep(100);
    Assert.assertEquals(0, DBUtilityTests.getAllContainersToCleanUp().size());
    Assert.assertEquals(0, DBUtilityTests.getAllAppsToCleanup().size());
  }
  
  private RMNodeImpl getRunningNode() {
    return getRunningNode(null);
  }

  private RMNodeImpl getRunningNode(String nmVersion) {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    Resource capability = Resource.newInstance(4096, 4);
    RMNodeImpl node = new RMNodeImplDist(nodeId, rmContext,null, 0, 0,
        null, capability, nmVersion);
    node.handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    return node;
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
    return event;
  }
}
