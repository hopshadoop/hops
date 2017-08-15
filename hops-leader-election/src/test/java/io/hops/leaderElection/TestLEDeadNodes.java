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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLEDeadNodes {

  private static final Log LOG = LogFactory.getLog(TestLEDeadNodes.class);
  List<LightWeightNameNode> nnList;
  private final int DFS_LEADER_CHECK_INTERVAL_IN_MS = 3 * 1000;
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
   * Test if the NN are correctly removed from the Active NN list
   */
  @Test
  public void testDeadNodesRemoval()
      throws IOException, InterruptedException, CloneNotSupportedException {
    LOG.debug("start testDeadNodesRemoval");
    List<InetSocketAddress> isaList = new ArrayList<InetSocketAddress>();
    //create 10 NN
    for (int i = 0; i < 10; i++) {
      LightWeightNameNode nn =
          new LightWeightNameNode(new HdfsLeDescriptorFactory(),
              DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
              TIME_PERIOD_INCREMENT, HTTP_ADDRESS, "127.0.0.1:" + (50000 + i));
      nnList.add(nn);
      isaList.add(nn.getNameNodeAddress());
    }
    
    // wait for one HB and the first node should have complete list of nodes
    Thread.sleep(
        DFS_LEADER_CHECK_INTERVAL_IN_MS * DFS_LEADER_MISSED_HB_THRESHOLD);
    //verify that the number of active nn is equal to the number of started NN
    List<ActiveNode> activesNNs =
        nnList.get(0).getActiveNameNodes().getActiveNodes();
    assertTrue("wrong number of active NN " + activesNNs.size(),
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

    //stop the leader
    nnList.get(leaderId).stop();

    Thread.sleep(
        DFS_LEADER_CHECK_INTERVAL_IN_MS * (DFS_LEADER_MISSED_HB_THRESHOLD + 2));

    //verify that there is one and only one leader.
    int newLeaderId = 0;
    nbLeaders = 0;
    for (int i = 0; i < nnList.size(); i++) {
      if (i != leaderId) {
        if (nnList.get(i).isLeader()) {
          nbLeaders++;
          newLeaderId = i;
        }
      }
    }
    
    assertTrue("there is no leader", nbLeaders > 0);
    assertTrue("there is more than one leader", nbLeaders == 1);

    //verify that the stoped leader is not in the active list anymore
    activesNNs = nnList.get(newLeaderId).getActiveNameNodes().getActiveNodes();
    for (ActiveNode ann : activesNNs) {
      assertFalse("previous is stil in active nn",
          ann.getRpcServerAddressForClients().equals(isaList.get(leaderId)));
    }

    //stop NN last alive NN
    int tokill = nnList.size() - 1;
    while (leaderId == tokill || newLeaderId == tokill) {
      tokill--;
    }
    LOG.debug("stopping node: " + nnList.get(tokill).getLeCurrentId());
    nnList.get(tokill).stop();
    Thread.sleep(
        DFS_LEADER_CHECK_INTERVAL_IN_MS * (DFS_LEADER_MISSED_HB_THRESHOLD + 2));

    //verify that the killed NN is not in the active NN list anymore
    activesNNs = nnList.get(newLeaderId).getActiveNameNodes().getActiveNodes();
    assertTrue("wrong nb of active nn " + activesNNs.size(),
        activesNNs.size() == 8);
    for (ActiveNode ann : activesNNs) {
      assertFalse("killed nn is stil in active nn",
          ann.getRpcServerAddressForClients().equals(isaList.get(tokill)));
    }

  }

}
