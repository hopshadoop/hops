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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.ha.common.TransactionStateManager;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.GroupMembershipService;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.NodeEventDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public class TestNMExpiry {
  private static final Log LOG = LogFactory.getLog(TestNMExpiry.class);
  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  ResourceTrackerService resourceTrackerService;

  private class TestNmLivelinessMonitor extends NMLivelinessMonitor {
    public TestNmLivelinessMonitor(Dispatcher dispatcher, RMContext rmContext,
            Configuration conf) {
      super(dispatcher, rmContext, conf);
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
      conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 1000);
      super.serviceInit(conf);
    }
  }

  GroupMembershipService groupMembership = null;

  @Before
  public void setUp()
      throws StorageInitializtionException, StorageException, IOException {
    Configuration conf = new Configuration();
    conf = new YarnConfiguration(conf);
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
    // Dispatcher that processes events inline
    Dispatcher dispatcher = new InlineDispatcher();
    TransactionStateManager tsm = new TransactionStateManager();
    tsm.init(conf);
    tsm.start();
    RMContextImpl context =
        new RMContextImpl(dispatcher, null, null, null, null, null, null, null,
            conf, tsm);
    groupMembership = new GroupMembershipService(null, context);
    context.setRMGroupMembershipService(groupMembership);
    dispatcher.register(SchedulerEventType.class,
        new InlineDispatcher.EmptyEventHandler());
    dispatcher
        .register(RMNodeEventType.class, new NodeEventDispatcher(context));
    NMLivelinessMonitor nmLivelinessMonitor =
        new TestNmLivelinessMonitor(dispatcher, context, conf);
    nmLivelinessMonitor.init(conf);
    nmLivelinessMonitor.start();
    NodesListManager nodesListManager = new NodesListManager(context);
    nodesListManager.init(conf);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf, context);
    containerTokenSecretManager.start();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf, context);
    nmTokenSecretManager.start();
    resourceTrackerService =
        new ResourceTrackerService(context, nodesListManager,
            nmLivelinessMonitor, containerTokenSecretManager,
            nmTokenSecretManager);
    
    resourceTrackerService.init(conf);
    resourceTrackerService.start();
    
  }

  @After
  public void tearDown() {
    if (groupMembership != null) {
      groupMembership.stop();
    }
  }
  
  private class ThirdNodeHeartBeatThread extends Thread {
    public void run() {
      int lastResponseID = 0;
      while (!stopT) {
        try {
          org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus =
              recordFactory.newRecordInstance(
                  org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
          nodeStatus.setNodeId(request3.getNodeId());
          nodeStatus.setResponseId(lastResponseID);
          nodeStatus.setNodeHealthStatus(
              recordFactory.newRecordInstance(NodeHealthStatus.class));
          nodeStatus.getNodeHealthStatus().setIsNodeHealthy(true);

          NodeHeartbeatRequest request =
              recordFactory.newRecordInstance(NodeHeartbeatRequest.class);
          request.setNodeStatus(nodeStatus);
          lastResponseID =
              resourceTrackerService.nodeHeartbeat(request).getResponseId();

          Thread.sleep(1000);
        } catch (Exception e) {
          LOG.info("failed to heartbeat ", e);
        }
      }
    }
  }

  boolean stopT = false;
  RegisterNodeManagerRequest request3;

  @Test
  public void testNMExpiry() throws Exception {
    String hostname1 = "localhost1";
    String hostname2 = "localhost2";
    String hostname3 = "localhost3";
    Resource capability = BuilderUtils.newResource(1024, 1);

    RegisterNodeManagerRequest request1 =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
    request1.setNodeId(nodeId1);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);

    RegisterNodeManagerRequest request2 =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId2 = NodeId.newInstance(hostname2, 0);
    request2.setNodeId(nodeId2);
    request2.setHttpPort(0);
    request2.setResource(capability);
    resourceTrackerService.registerNodeManager(request2);
    
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumLostNMs() != 2 &&
        waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals(2, ClusterMetrics.getMetrics().getNumLostNMs());

    request3 =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId3 = NodeId.newInstance(hostname3, 0);
    request3.setNodeId(nodeId3);
    request3.setHttpPort(0);
    request3.setResource(capability);
    resourceTrackerService.registerNodeManager(request3);

    /* test to see if hostanme 3 does not expire */
    stopT = false;
    new ThirdNodeHeartBeatThread().start();
    Assert.assertEquals(2, ClusterMetrics.getMetrics().getNumLostNMs());
    stopT = true;
  }
}
