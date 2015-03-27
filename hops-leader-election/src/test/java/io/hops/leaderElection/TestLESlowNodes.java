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
package io.hops.leaderElection;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.leaderElection.experiments.LightWeightNameNode;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.LEStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestLESlowNodes {

  private static final Log LOG = LogFactory.getLog(TestLESlowNodes.class);
  List<LightWeightNameNode> nnList;
  private final int DFS_LEADER_CHECK_INTERVAL_IN_MS = 2 * 1000;
  private final int DFS_LEADER_MISSED_HB_THRESHOLD = 2;
  private final String HTTP_ADDRESS = "dummy.address.com:9999";
  private final String RPC_ADDRESS = "repc.server.ip:0000";
  private final String DRIVER_JAR = "";
  private final String DRIVER_CLASS = "io.hops.metadata.ndb.NdbStorageFactory";
  private final String DFS_STORAGE_DRIVER_CONFIG_FILE = "ndb-config.properties";
  private final long TIME_PERIOD_INCREMENT = 200;

  @Before
  public void init()
      throws StorageInitializtionException, StorageException, IOException {
    LogManager.getRootLogger().setLevel(Level.ALL);
    nnList = new ArrayList<LightWeightNameNode>();
    LEStorageFactory.setConfiguration(DRIVER_JAR, DRIVER_CLASS,
        DFS_STORAGE_DRIVER_CONFIG_FILE);
    LEStorageFactory.formatStorage();
    VarsRegister.registerHdfsDefaultValues();
  }

  @After
  public void tearDown() {
    //stop all NN
    LOG.debug("tearDown");
    for (LightWeightNameNode nn : nnList) {
      nn.stop();
    }
  }

  /**
   *
   */
  @Test
  public void testSlowLeader()
      throws IOException, InterruptedException, CloneNotSupportedException {
    LOG.debug("start testSlowLeader");
    //create 10 NN
    for (int i = 0; i < 2; i++) {
      LightWeightNameNode nn =
          new LightWeightNameNode(new HdfsLeDescriptorFactory(),
              DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
              TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
      nnList.add(nn);
    }
    
    // wait for one HB and the first node should have complete list of nodes
    Thread.sleep(DFS_LEADER_CHECK_INTERVAL_IN_MS);
    //verify that the number of active nn is equal to the number of started NN
    List<ActiveNode> activesNNs =
        nnList.get(nnList.size() - 1).getActiveNameNodes().getActiveNodes();
    assertTrue("wrong number of actives NN " + activesNNs.size(),
        activesNNs.size() == nnList.size());
    
    //verify that there is one and only one leader.
    int leaderId = 0;
    int nbLeaders = 0;
    for (int i = 0; i < nnList.size(); i++) {
      if (nnList.get(i).isLeader()) {
        nbLeaders++;
        leaderId = i;
      }
    }
    assertTrue("there is no leader", nbLeaders > 0);
    assertTrue("there is more than one leader", nbLeaders == 1);

    //slowdown leader NN by suspending its thread during 10s 
    int sleepTime =
        DFS_LEADER_CHECK_INTERVAL_IN_MS * (DFS_LEADER_MISSED_HB_THRESHOLD + 2);
    nnList.get(leaderId).getLeaderElectionInstance().pauseFor(sleepTime);
    LOG.info("sleep:" + sleepTime);
    Thread.sleep(sleepTime);

    //verify that there is one and only one leader.
    nbLeaders = 0;
    for (int i = 0; i < nnList.size(); i++) {
      if (nnList.get(i).isLeader()) {
        nbLeaders++;
        LOG.debug("leader is " + nnList.get(i).getLeCurrentId());
      }
    }
    assertTrue("there is no leader", nbLeaders > 0);
    assertTrue("there is more than one leader " + nbLeaders, nbLeaders == 1);

  }

}
