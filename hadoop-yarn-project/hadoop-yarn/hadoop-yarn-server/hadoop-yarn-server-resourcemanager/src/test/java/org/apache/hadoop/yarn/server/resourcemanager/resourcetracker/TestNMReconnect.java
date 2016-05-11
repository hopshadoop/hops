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
import io.hops.metadata.util.YarnAPIStorageFactory;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.GroupMembershipService;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.NodeEventDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestNMReconnect {
  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private RMNodeEvent rmNodeEvent = null;

  private class TestRMNodeEventDispatcher implements EventHandler<RMNodeEvent> {

    @Override
    public void handle(RMNodeEvent event) {
      rmNodeEvent = event;
    }

  }

  ResourceTrackerService resourceTrackerService;
  GroupMembershipService groupMembership = null;

  @Before
  public void setUp()
      throws StorageInitializtionException, StorageException, IOException {
    Configuration conf = new Configuration();
    // Dispatcher that processes events inline
    Dispatcher dispatcher = new InlineDispatcher();

    dispatcher.register(RMNodeEventType.class, new TestRMNodeEventDispatcher());
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
        new NMLivelinessMonitor(dispatcher, context, conf);
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
    
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMStorageFactory.getConnector().formatStorage();
  }

  @After
  public void teardown() {
    if (groupMembership != null) {
      groupMembership.stop();
    }
  }

  @Test
  public void testReconnect() throws Exception {
    String hostname1 = "localhost1";
    Resource capability = BuilderUtils.newResource(1024, 1);

    RegisterNodeManagerRequest request1 =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
    request1.setNodeId(nodeId1);
    request1.setHttpPort(0);
    request1.setResource(capability);
    resourceTrackerService.registerNodeManager(request1);

    Assert.assertEquals(RMNodeEventType.STARTED, rmNodeEvent.getType());

    rmNodeEvent = null;
    resourceTrackerService.registerNodeManager(request1);
    Assert.assertEquals(RMNodeEventType.RECONNECTED, rmNodeEvent.getType());

    rmNodeEvent = null;
    resourceTrackerService.registerNodeManager(request1);
    capability = BuilderUtils.newResource(1024, 2);
    request1.setResource(capability);
    Assert.assertEquals(RMNodeEventType.RECONNECTED, rmNodeEvent.getType());
  }
}
