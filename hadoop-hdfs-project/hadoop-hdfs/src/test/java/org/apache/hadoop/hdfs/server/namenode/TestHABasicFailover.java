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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestHABasicFailover extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestHABasicFailover.class);


  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger()
        .setLevel(Level.ALL);
  }

  Configuration conf = new HdfsConfiguration();
  MiniDFSCluster cluster = null;
  int NUM_NAMENODES = 2;
  int NUM_DATANODES = 1;

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  /**
   * Testing basic failover. After starting namenodes NN1, NN2, the namenode
   * that first initializes itself would be elected the leader. We allow NN1
   * to be the leader. We kill NN1. Failover will start and NN2 will detect
   * failure of NN1 and hence would elect itself as the leader Also perform
   * fail-back to NN1 by killing NN2
   */
  @Test(timeout = 900000)
  public void testFailover() throws IOException, TimeoutException {

    final int NN1 = 0, NN2 = 1;
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }

    try {
      // Create cluster with 2 namenodes
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
          .numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      // Give it time for leader to be elected
      long timeout =
          conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
              DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT) +
              conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                  DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;

      /**
       * *********************************
       * testing fail over from NN1 to NN2
       * **********************************
       */
      // Check NN1 is the leader
      LOG.info(
          "NameNode 1 id " + cluster.getNameNode(NN1).getId() + " address " +
              cluster.getNameNode(NN1).getServiceRpcAddress().toString());
      LOG.info(
          "NameNode 2 id " + cluster.getNameNode(NN2).getId() + " address " +
              cluster.getNameNode(NN2).getServiceRpcAddress().toString());

      assertTrue("NN1 is expected to be leader, but is not",
          cluster.getNameNode(NN1).isLeader());

      // performing failover - Kill NN1. This would allow NN2 to be leader
      cluster.shutdownNameNode(NN1);


      // wait for leader to be elected and for Datanodes to also detect the leader
      waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2),
          timeout * 10);

      // Check NN2 is the leader and failover is detected
      assertTrue("NN2 is expected to be the leader, but is not",
          cluster.getNameNode(NN2).isLeader());
      assertTrue("Not all datanodes detected the new leader",
          doesDataNodesRecognizeLeader(cluster.getDataNodes(),
              cluster.getNameNode(NN2)));


      LOG.debug("TestNN going to restart the NN2");
      // restart the newly elected leader and see if it is still the leader
      cluster.restartNameNode(NN2, false);

      cluster.waitActive();
      waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2),
          timeout * 10);
      assertTrue("NN2 is expected to be the leader, but is not",
          cluster.getNameNode(NN2).isLeader());
      assertTrue("Not all datanodes detected the new leader",
          doesDataNodesRecognizeLeader(cluster.getDataNodes(),
              cluster.getNameNode(NN2)));

      /**
       * **************************************
       * testing fail-back after some interval datanode asks for a
       * namenode to return all alive namenodes in the system.
       *
       * datanode starts new threads for new namenodes. if it finds out that some
       * previous namenode is dead then the corresponding service thread
       * is killed.              *
       * A datanodes find out new namenodes by asking existing name nodes
       * in the system. what happen data node is connected to X set of
       * namenodes and they all die suddenly; and after a while Y set of
       * namenodes come online. datanode will have no way of finding out
       * namenodes belonging to set Y
       *
       * there is no fix for it yet. if such thing happens then restart
       * datanode with some correct namenode.              *
       * in the tests such secnaiors are avoided by making sure that
       * datanodes are connected to atleast one name node after killing
       * other namenodes. **************************************
       */
      // Doing a fail back scenario to NN1
      cluster.restartNameNode(
          NN1); // will be restarted in the system with the next highest id while NN2 is still the leader
      cluster.waitActive();

      waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2),
          timeout * 10);

      cluster.shutdownNameNode(NN2);
      cluster.waitActive();

      // waiting for NN1 to elect itself as the leader
      waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN1),
          timeout * 10);
      assertTrue("NN1 is expected to be the leader, but is not",
          cluster.getNameNode(NN1).isLeader());
      assertTrue("Not all datanodes detected the new leader",
          doesDataNodesRecognizeLeader(cluster.getDataNodes(),
              cluster.getNameNode(NN1)));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

  }

  public static boolean doesDataNodesRecognizeLeader(List<DataNode> datanodes,
      NameNode namenode) {
    boolean result = true;
    for (DataNode datanode : datanodes) {
      result = result & (datanode.isConnectedToNN(namenode.getNameNodeAddress())
              || datanode.isConnectedToNN(namenode.getServiceRpcAddress()));
    }
    return result;
  }

  public static void waitLeaderElection(List<DataNode> datanodes, NameNode nn,
      long timeout) throws TimeoutException {
    // wait for the new leader to be elected
    long initTime = System.currentTimeMillis();
    while (!nn.isLeader()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }

      // check for time out
      if (System.currentTimeMillis() - initTime >= timeout) {
        throw new TimeoutException(
            "Namenode was not elected leader. Time out " + timeout);
      }
    }

    // wait for all datanodes to recognize the new leader
    initTime = System.currentTimeMillis();
    while (true) {

      try {
        Thread.sleep(2000); // 2sec
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }

      boolean result = doesDataNodesRecognizeLeader(datanodes, nn);
      if (result) {
        break;
      }
      // check for time out
      if (System.currentTimeMillis() - initTime >= timeout) {
        throw new TimeoutException(
            "Datanodes weren't able to detect newly elected leader");
      }
    }
  }
}