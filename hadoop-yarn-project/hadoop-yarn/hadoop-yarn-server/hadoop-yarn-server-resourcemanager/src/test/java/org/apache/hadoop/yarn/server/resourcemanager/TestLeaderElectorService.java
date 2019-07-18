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

import com.google.common.base.Supplier;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestLeaderElectorService {

  private static final String RM1_ADDRESS = "1.1.1.1:1";
  private static final String RM1_NODE_ID = "rm1";

  private static final String RM2_ADDRESS = "0.0.0.0:0";
  private static final String RM2_NODE_ID = "rm2";

  Configuration conf ;
  MockRM rm1;
  MockRM rm2;
  TestingCluster zkCluster;
  @Before
  public void setUp() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.INFO);
    conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);

    conf.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);

    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      conf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      conf.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
    }
    zkCluster = new TestingCluster(3);
    zkCluster.start();
  }

  @After
  public void tearDown() throws Exception {
    if (rm1 != null) {
      rm1.stop();
    }
    if (rm2 !=null) {
      rm2.stop();
    }
  }

  // 1. rm1 active
  // 2. rm2 standby
  // 3. stop rm1
  // 4. rm2 become active
  @Test (timeout = 20000)
  public void testRMShutDownCauseFailover() throws Exception {
    rm1 = startRM("rm1", HAServiceState.ACTIVE);
    rm2 = startRM("rm2", HAServiceState.STANDBY);

    // wait for some time to make sure rm2 will not become active;
    Thread.sleep(5000);
    waitFor(rm2, HAServiceState.STANDBY);

    rm1.stop();
    // rm2 should become active;
    waitFor(rm2, HAServiceState.ACTIVE);
  }

  // 1. rm1 active
  // 2. rm2 standby
  // 3. submit a job to rm1 which triggers state-store failure.
  // 4. rm2 become
  @Test
  public void testStateStoreFailureCauseFailover() throws  Exception {

    conf.set(YarnConfiguration.RM_HA_ID, "rm1");
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      @Override
      public synchronized void storeApplicationStateInternal(ApplicationId
          appId, ApplicationStateData appState) throws Exception{
        throw new Exception("store app failure.");
      }
    };
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore, true);
    rm1.init(conf);
    rm1.start();

    waitFor(rm1, HAServiceState.ACTIVE);

    rm2 = startRM("rm2", HAServiceState.STANDBY);

    // submit an app which will trigger state-store failure.
    rm1.submitApp(200, "app1", "user1", null, "default", false);
    waitFor(rm1, HAServiceState.STANDBY);

    // rm2 should become active;
    waitFor(rm2, HAServiceState.ACTIVE);

    rm2.stop();
    // rm1 will become active again
    waitFor(rm1, HAServiceState.ACTIVE);
  }

  // 1. rm1 active
  // 2. restart zk cluster
  // 3. rm1 will first relinquish leadership and re-acquire leadership
  @Test
  public void testZKClusterDown() throws Exception {
    rm1 = startRM("rm1", HAServiceState.ACTIVE);

    // stop zk cluster
    zkCluster.stop();
    waitFor(rm1, HAServiceState.STANDBY);

    Collection<InstanceSpec> instanceSpecs = zkCluster.getInstances();
    zkCluster = new TestingCluster(instanceSpecs);
    zkCluster.start();
    // rm becomes active again
    waitFor(rm1, HAServiceState.ACTIVE);
  }


  // 1. rm1 fail to become active.
  // 2. rm1 will rejoin leader election and retry the leadership
  @Test
  public void testRMFailToTransitionToActive() throws Exception{
    conf.set(YarnConfiguration.RM_HA_ID, "rm1");
    final AtomicBoolean throwException = new AtomicBoolean(true);
    Thread launchRM = new Thread() {
      @Override
      public void run() {
        rm1 = new MockRM(conf, true) {
          @Override
          public synchronized void transitionToActive() throws Exception {
            if (throwException.get()) {
             throw new Exception("Fail to transition to active");
            } else {
              super.transitionToActive();
            }
          }
        };
        rm1.init(conf);
        rm1.start();
      }
     };
    launchRM.start();
    // wait some time, rm will keep retry the leadership;
    Thread.sleep(5000);
    throwException.set(false);
    waitFor(rm1, HAServiceState.ACTIVE);
  }

  private MockRM startRM(String rmId, HAServiceState state) throws Exception{
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
    MockRM rm = new MockRM(yarnConf, true);
    rm.init(yarnConf);
    rm.start();
    waitFor(rm, state);
    return rm;
  }

  private void waitFor(final MockRM rm,
      final HAServiceState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        try {
          return rm.getAdminService().getServiceStatus().getState()
              .equals(state);
        } catch (IOException e) {
        }
        return false;
      }
    }, 2000, 15000);
  }
}
