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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.GroupMembershipService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NdbRtStreamingProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class TestDistributedNMTokenSecretManagerInRM {

  YarnConfiguration conf = new YarnConfiguration();

  //test that old keys are correctly removed from the database
  @Before
  public void setUp() throws Exception {
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }

  @Test
  public void testNonDistributedStart() {
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setDistributedEnabled(false);

    NMTokenSecretManagerInRM nmTokenSecretManager
            = new NMTokenSecretManagerInRM(conf, rmContext);
    try {
      nmTokenSecretManager.start();
      assertNotNull("did not roll master key", nmTokenSecretManager.
              getCurrentKey());
    } finally {
      nmTokenSecretManager.stop();
    }
  }

  @Test
  public void testDistributedNonLeaderStart() {
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setDistributedEnabled(true);
    rmContext.setHAEnabled(true);
    rmContext.setRMGroupMembershipService(new GroupMembershipService(null,
            rmContext));

    assertFalse("should not be leader", rmContext.isLeader());
    NMTokenSecretManagerInRM nmTokenSecretManager
            = new NMTokenSecretManagerInRM(conf, rmContext);
    try {
      nmTokenSecretManager.start();

      nmTokenSecretManager.getCurrentKey();
      fail("should not have rolled master key");
    } catch (NullPointerException ex) {
      /*
       * get currentKey should raise a nullPointerException as the master key
       * has
       * not be rolled
       */
    } finally {
      nmTokenSecretManager.stop();
    }
  }

  @Test
  public void testDistributedLeaderStart() throws Exception {
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setDistributedEnabled(true);
    rmContext.setHAEnabled(true);
    GroupMembershipService groupMembershipService = new GroupMembershipService(
            null, rmContext);
    groupMembershipService.init(conf);
    NMTokenSecretManagerInRM nmTokenSecretManager
            = new NMTokenSecretManagerInRM(conf, rmContext);
    try {
      groupMembershipService.start();

      rmContext.setRMGroupMembershipService(groupMembershipService);
      rmContext.setStateStore(new NDBRMStateStore());

      assertTrue("should be leader", rmContext.isLeader());
      nmTokenSecretManager.start();
      assertNotNull("did not roll master key", nmTokenSecretManager.
              getCurrentKey());

      RMStateStore.RMState state = rmContext.getStateStore().
              loadState(rmContext);
      assertNotNull("key not persisted to the database",
              state.
              getSecretTokenMamagerKey(
                      RMStateStore.KeyType.CURRENTNMTOKENMASTERKEY));

      NMTokenSecretManagerInRM nmTokenSecretManager2
              = new NMTokenSecretManagerInRM(conf, rmContext);
      nmTokenSecretManager2.recover(state);

      assertEquals(nmTokenSecretManager.getCurrentKey(), nmTokenSecretManager2.
              getCurrentKey());
      assertEquals(nmTokenSecretManager.getNextKey(), nmTokenSecretManager2.
              getNextKey());
    } finally {
      groupMembershipService.stop();
      nmTokenSecretManager.stop();
      DefaultMetricsSystem.shutdown();
    }
  }

  @Test
  public void testNonLeaderKeyReception() throws InterruptedException,
          StorageInitializtionException,
          Exception {
    //create a groupMembershipService that will be leader
    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setDistributedEnabled(true);
    rmContext.setHAEnabled(true);
    GroupMembershipService groupMembershipService = new GroupMembershipService(
            null, rmContext);
    groupMembershipService.init(conf);
    NMTokenSecretManagerInRM nmTokenSecretManager
            = new NMTokenSecretManagerInRM(conf, rmContext);
    //create a resrouce tracker
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    MockRM mockRM = new MockRM(conf);
    mockRM.init(conf);

    try {
      groupMembershipService.start();
      rmContext.setRMGroupMembershipService(groupMembershipService);
      rmContext.setStateStore(new NDBRMStateStore());

      while (!rmContext.isLeader()) {
        Thread.sleep(1000);
      }

      mockRM.start();

      if (mockRM.getRMContext().isDistributedEnabled()
              && !mockRM.getRMContext().
              isLeader()) {
        conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
                "target/test-classes/RT_EventAPIConfig.ini");
        NdbRtStreamingProcessor rtStreamingProcessor
                = new NdbRtStreamingProcessor(mockRM.getRMContext());
        RMStorageFactory.kickTheNdbEventStreamingAPI(false, conf);
        new Thread(rtStreamingProcessor).start();

      }

      //this should be a resource tracker not a scheduler
      assertFalse(mockRM.getRMContext().isLeader());

      //simulate creation of a token on the sheduler
      nmTokenSecretManager.start();

      assertNotNull("did not roll master key", nmTokenSecretManager.
              getCurrentKey());
      Thread.sleep(1000);
      dummyUpdate();
      Thread.sleep(1000);
      RMStateStore.RMState state = rmContext.getStateStore().
              loadState(rmContext);
      assertEquals("key not persisted to the database",
              state.
              getSecretTokenMamagerKey(
                      RMStateStore.KeyType.CURRENTNMTOKENMASTERKEY),
              nmTokenSecretManager.getCurrentKey());

      assertEquals(nmTokenSecretManager.getCurrentKey(), mockRM.getRMContext().
              getNMTokenSecretManager().getCurrentKey());
      assertEquals(nmTokenSecretManager.getNextKey(), mockRM.getRMContext().
              getNMTokenSecretManager().getNextKey());
    } finally {
      groupMembershipService.stop();
      mockRM.stop();
      nmTokenSecretManager.stop();
      DefaultMetricsSystem.shutdown();
    }
  }

  private void dummyUpdate() throws IOException {
    LightWeightRequestHandler bomb = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        RMNodeDataAccess da = (RMNodeDataAccess) RMStorageFactory.
                getDataAccess(RMNodeDataAccess.class);
        List<RMNode> toAdd = new ArrayList<RMNode>();
        RMNode rmNode = new RMNode("nodeid_1", "hostName", 1,
                1, "nodeAddress", "httpAddress", "", 1, "currentState",
                "version", 1, 0);
        toAdd.add(rmNode);

        da.addAll(toAdd);
        connector.flush();
        NextHeartbeat nhb
                = new NextHeartbeat("nodeid_1", true, 0);
        List<NextHeartbeat> toAddNextHeartbeats = new ArrayList<NextHeartbeat>();
        toAddNextHeartbeats.add(nhb);
        NextHeartbeatDataAccess nhbda
                = (NextHeartbeatDataAccess) RMStorageFactory.getDataAccess(
                        NextHeartbeatDataAccess.class);
        nhbda.updateAll(toAddNextHeartbeats);

        connector.commit();
        return null;
      }
    };
    bomb.handle();
  }
}
