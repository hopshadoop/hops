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

import static org.junit.Assert.assertTrue;

public class TestLEStability {

  private static final Log LOG = LogFactory.getLog(TestLEStability.class);
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
  public void testLEStability()
      throws IOException, InterruptedException, CloneNotSupportedException {
    LOG.debug("start testSlowLeader");
    //create 10 NN
    for (int i = 0; i < 50; i++) {

      LightWeightNameNode nn = null;
      while (true) {
        try {
          nn = new LightWeightNameNode(new HdfsLeDescriptorFactory(),
              DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
              TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
        } catch (Exception e) {
          LOG.debug(
              "Could not create a process. Retrying ... Exception was  " + e);
          continue;
        }
        break;
      }

      nnList.add(nn);
      if (i == 0) {
        Thread.sleep(
            DFS_LEADER_CHECK_INTERVAL_IN_MS); // make sure that the first one becomes the leader
      }
    }
    final long firstLeader = nnList.get(0).getLeCurrentId();

    Thread.sleep(DFS_LEADER_CHECK_INTERVAL_IN_MS);

    LOG.debug("I am test. started all nodes");
    long timeToRun = 2 * 60 * 1000;
    long startTime = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startTime) < timeToRun) {
      //verify that there is one and only one leader.
      long leaderId = 0;
      int leaderCount = 0;
      for (int i = 0; i < nnList.size(); i++) {
        if (nnList.get(i).isLeader()) {
          leaderCount++;
          leaderId = nnList.get(i).getLeCurrentId();
        }
      }
      assertTrue("Wrong number of leaders. Found " + leaderCount,
          leaderCount <= 1);
      assertTrue(
          "The leader has changed. May be due to contention the leader was evicted. Old Leader id " +
              firstLeader + " new Leader id " + leaderId,
          leaderId == firstLeader);
    }
  }
}
