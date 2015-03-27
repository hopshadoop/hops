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

public class TestMultipleLEInstances {

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
    VarsRegister.registerHdfsDefaultValues();
    VarsRegister.registerYarnDefaultValues();
    LEStorageFactory.formatStorage();
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
  public void testMultipleLEInstances()
      throws IOException, InterruptedException, CloneNotSupportedException {
    LightWeightNameNode hdfs1 =
        new LightWeightNameNode(new HdfsLeDescriptorFactory(),
            DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
            TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
    nnList.add(hdfs1);
    LightWeightNameNode hdfs2 =
        new LightWeightNameNode(new HdfsLeDescriptorFactory(),
            DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
            TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
    nnList.add(hdfs2);

    LightWeightNameNode yarn1 =
        new LightWeightNameNode(new YarnLeDescriptorFactory(),
            DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
            TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
    nnList.add(yarn1);
    LightWeightNameNode yarn2 =
        new LightWeightNameNode(new YarnLeDescriptorFactory(),
            DFS_LEADER_CHECK_INTERVAL_IN_MS, DFS_LEADER_MISSED_HB_THRESHOLD,
            TIME_PERIOD_INCREMENT, HTTP_ADDRESS, RPC_ADDRESS);
    nnList.add(yarn2);

    Thread.sleep(
        DFS_LEADER_CHECK_INTERVAL_IN_MS * DFS_LEADER_MISSED_HB_THRESHOLD);
    //both namenodes should have same id

    assertTrue("Leader Check Failed ", hdfs1.isLeader() == yarn1.isLeader());
    assertTrue("Both namenodes should have same id ",
        hdfs1.getLeCurrentId() == yarn1.getLeCurrentId());
    assertTrue("Both namenodes should have same id ",
        hdfs2.getLeCurrentId() == yarn2.getLeCurrentId());
    assertTrue("Wrong number of Namenodes ",
        hdfs1.getActiveNameNodes().size() == 2);
    assertTrue("Wrong number of Namenodes ",
        yarn1.getActiveNameNodes().size() == 2);
    assertTrue("Wrong number of Namenodes ",
        hdfs2.getActiveNameNodes().size() == 2);
    assertTrue("Wrong number of Namenodes ",
        yarn2.getActiveNameNodes().size() == 2);
  }
}
