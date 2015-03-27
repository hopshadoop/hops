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
import org.apache.hadoop.conf.Configuration;
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
import static org.junit.Assert.fail;

public class TestLESpeed {

  private static final Log LOG = LogFactory.getLog(TestLESpeed.class);
  Configuration conf = null;
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
      throws StorageInitializtionException, StorageException, IOException,
      ClassNotFoundException {
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
  public void testLeaderElectionSpeed()
      throws IOException, InterruptedException, CloneNotSupportedException {

    //create 10 NN
    Random rand = new Random(System.currentTimeMillis());
    for (int i = 0; i < 50; i++) {
      LightWeightNameNode nn = startAProcess();
      nnList.add(nn);
      Thread.sleep(rand.nextInt(DFS_LEADER_CHECK_INTERVAL_IN_MS *
          (DFS_LEADER_MISSED_HB_THRESHOLD + 1)));
    }

    // kill leader and start new nodes
    // see how long does it take to elect a new leader
    LOG.debug("I am test. started all nodes");
    final long timeToRun = 3 * 60 * 1000;
    long startTime = System.currentTimeMillis();
    long lastLeaderKillTime = 0;
    final long killLeaderAfter = 10 * 1000;
    LightWeightNameNode leader = null;
    while ((System.currentTimeMillis() - startTime) < timeToRun) {
      //verify that there is one and only one leader.
      int leaderCount = 0;
      for (int i = 0; i < nnList.size(); i++) {
        if (nnList.get(i).isLeader()) {
          leaderCount++;
          assertTrue("Wrong number of leaders. Found " + leaderCount,
              leaderCount <= 1);
          
          if (leader != null && lastLeaderKillTime > 0 &&
              leader.getLeCurrentId() != nnList.get(i).getLeCurrentId()) {
            Long failOverTime = System.currentTimeMillis() - lastLeaderKillTime;
            long failOverLowerBound = DFS_LEADER_CHECK_INTERVAL_IN_MS;
            long failOverUpperBound = DFS_LEADER_CHECK_INTERVAL_IN_MS *
                (DFS_LEADER_MISSED_HB_THRESHOLD + 1);
            //assertTrue("Wrong failover time "+failOverTime, failOverTime>failOverLowerBound && failOverTime < failOverUpperBound); no longer applicable
            LOG.debug("I am Test. Old Leader Id " + leader.getLeCurrentId() +
                " new Leader Id " + nnList.get(i).getLeCurrentId() +
                " New leader elected in " + (failOverTime) + " ms.");
          }
          leader = nnList.get(i);
        }
      }
      assertTrue("Wrong number of leaders. Found " + leaderCount,
          leaderCount <= 1);

      if ((System.currentTimeMillis() - lastLeaderKillTime) > killLeaderAfter &&
          leaderCount == 1) {
        LOG.debug("I am test. Starting a new Process ");
        LightWeightNameNode nn = startAProcess();
        nnList.add(nn);
        if (leader.isLeader()) {
          LOG.debug("I am test. Stopping the leader process ");
          leader.getLeaderElectionInstance().pauseFor(Long.MAX_VALUE);
          lastLeaderKillTime = System.currentTimeMillis();
        } else {
          LOG.debug("I am test. Leader has already been evicted id : " +
              leader.getLeCurrentId());
          continue;
        }
      }
    }
  }

  public LightWeightNameNode startAProcess() {
    int tries = 10;
    while (tries >= 0) {
      tries--;
      try {
        return new LightWeightNameNode(new HdfsLeDescriptorFactory(),
            DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
            TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
      } catch (Exception e) {
        LOG.debug(
            "Could not create a process. Retrying ... Exception was  " + e);
        e.printStackTrace();
      }
    }
    fail("Failed to spawn a new process.");
    return null;
  }
}
