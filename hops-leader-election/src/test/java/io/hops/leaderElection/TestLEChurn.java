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
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestLEChurn {

  private static final Log LOG = LogFactory.getLog(TestLEChurn.class);
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
   * Test leader election behavior under churn: start 10 NN then randomly stop,
   * restart existing NN and start new ones. Check that there is always at most
   * 1 leader and that the time without leader is never higher than expected.
   */
  @Test
  public void testChurn()
      throws IOException, InterruptedException, CloneNotSupportedException {
    LOG.debug("start testChurn");
    Random rand = new Random(0);
    List<LightWeightNameNode> activNNList =
        new ArrayList<LightWeightNameNode>();
    List<LightWeightNameNode> stopedNNList =
        new ArrayList<LightWeightNameNode>();
    int nbStartedNodes = 0;
    //create 10 NN
    for (int i = 0; i < 10; i++) {
      nbStartedNodes++;
      LightWeightNameNode nn =
          new LightWeightNameNode(new HdfsLeDescriptorFactory(),
              DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
              TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
      nnList.add(nn);
      activNNList.add(nn);
    }
    //verify that there is one and only one leader.
    int nbLeaders = 0;
    for (LightWeightNameNode nn : nnList) {
      if (nn.isLeader()) {
        nbLeaders++;
      }
    }
    assertTrue("there is no leader", nbLeaders > 0);
    assertTrue("there is more than one leader", nbLeaders == 1);


    long startingTime = System.currentTimeMillis();
    String s = "";
    while (System.currentTimeMillis() - startingTime < 5 * 60 * 1000) {
      //stop random number of random NN
      int nbStop = rand.nextInt(activNNList.size() - 1);
      for (int i = 0; i < nbStop; i++) {

        int nnId = rand.nextInt(activNNList.size());
        s = s + activNNList.get(nnId).getLeCurrentId() + "; ";
        LOG.debug("Test : pausing " + activNNList.get(nnId).getLeCurrentId());
        if (activNNList.get(nnId).getLeaderElectionInstance().isPaused()) {
          continue;
        }
        activNNList.get(nnId).getLeaderElectionInstance()
            .pauseFor(Long.MAX_VALUE);
        stopedNNList.add(activNNList.get(nnId));
        activNNList.remove(nnId);
      }
      LOG.debug("suspended " + nbStop + " with ids: " + s);


      Thread.sleep(5000); //after dead locks
      //start random number of new NN
      int nbStart = rand.nextInt(10);
      if (nbStartedNodes + nbStart > 100) {
        nbStart = 100 - nbStartedNodes;
      }
      for (int i = 0; i < nbStart; i++) {
        nbStartedNodes++;
        LightWeightNameNode nn =
            new LightWeightNameNode(new HdfsLeDescriptorFactory(),
                DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
                TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
        nnList.add(nn);
        activNNList.add(nn);
      }
      //restart a random number of stoped NN
      int nbRestart = rand.nextInt(stopedNNList.size());
      s = "";
      for (int i = 0; i < nbRestart; i++) {
        int nnId = rand.nextInt(stopedNNList.size());
        s = s + stopedNNList.get(nnId).getLeCurrentId() + "; ";
        stopedNNList.get(nnId).getLeaderElectionInstance().forceResume();
        activNNList.add(stopedNNList.get(nnId));
        stopedNNList.remove(nnId);
      }
      LOG.debug("restarted nodes with ids: " + s);
      //verify that there is at most one leader.
      //check that the time without leader is not too long
      long startWaitingForLeader = System.currentTimeMillis();
      do {
        nbLeaders = 0;
        ArrayList<Long> leadersID = new ArrayList<Long>();
        for (LightWeightNameNode nn : nnList) {
          if (nn.isLeader()) {
            nbLeaders++;
            leadersID.add(nn.getLeCurrentId());
          }
        }

        if (nbLeaders > 1) {
          s = " ";
          for (long id : leadersID) {
            s = s + id + " ";
          }
          assertTrue(
              "there is more than one leader " + nbLeaders + "leaders: " + s,
              nbLeaders <= 1);
        }
        long timeWithoutLeader =
            System.currentTimeMillis() - startWaitingForLeader;
        assertTrue("the time without leader is too long " + timeWithoutLeader,
            timeWithoutLeader < ((DFS_LEADER_CHECK_INTERVAL_IN_MS *
                (DFS_LEADER_MISSED_HB_THRESHOLD + 5)) +
                500));  // due to lock upgrade the upper time is has no limit
      } while (nbLeaders == 0);
    }
  }
}
