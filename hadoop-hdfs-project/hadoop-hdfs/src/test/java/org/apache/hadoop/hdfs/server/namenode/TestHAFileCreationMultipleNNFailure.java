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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/* We have no control over the order in which the datanodes and clients communicate with the namenode. 
 * Consider a scenario where the client is writing to a file. 
 * All the blocks have reached minimum replication level. 
 * The datanode have not yet reported all the replicas. 
 * The namenode will close the file and mark the blocks that have not yet been fully replicated as under replicated. 
 * Replication monitor starts the replication process for the under-replicated blocks. 
 * 
 * In this test number of datanode is equal to replication level of the file. The replication monitor
 * tris to create new replicas on datanode that already have a copy of the replica (not yet reported to datanode). This results
 * in ReplicaAlreadyExists Exception. 
 * 
 * The default placement policy also throws warnings like Not able to place enough replicas
 * 
 * if too many threads are created then sometimes the data length is not consistent. In such case look for java.io.IOException: Too many open files exception in the log 
 *
 */

@Ignore(value = "The design of this test needs to be reconsidered. " +
    "It fails most of the times because of race conditions.")
public class TestHAFileCreationMultipleNNFailure
    extends junit.framework.TestCase {

  public static final Log LOG =
      LogFactory.getLog(TestHAFileCreationMultipleNNFailure.class);

  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger()
        .setLevel(Level.ALL);
  }

  Configuration conf = new HdfsConfiguration();
  MiniDFSCluster cluster = null;
  FileSystem fs = null;
  int NN1 = 0, NN2 = 1;
  static int NUM_NAMENODES = 10;
  static int NUM_DATANODES = 3;
  // 10 seconds timeout default
  long NNDeathTimeout = 10000;
  boolean writeInSameDir = true;
  boolean killNN = true;
  boolean waitFileisClosed = true;
  int fileCloseWaitTime = 5000;
  int waitReplicationTimeout = 5 * 60 * 1000;
  Path baseDir = new Path("/testsLoad");
  Writer[] writers = new Writer[1];

  private void setupCluster(int replicationFactor) throws IOException {
    // initialize the cluster with minimum 2 namenodes and minimum 6 datanodes
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }

    if (replicationFactor > NUM_DATANODES) {
      NUM_DATANODES = replicationFactor;
    }

    this.conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        15);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
        .format(true).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();

    LOG.debug(
        "NN1 address is " + cluster.getNameNode(NN1).getNameNodeAddress() +
            " ld: " + cluster.getNameNode(NN1).isLeader() + " NN2 address is " +
            cluster.getNameNode(NN2).getNameNodeAddress() + " ld: " +
            cluster.getNameNode(NN2).isLeader());

    fs = cluster.getNewFileSystemInstance(NN1);

    NNDeathTimeout =
        conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
            DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT) * (conf
            .getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
                DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT) + 2);

    // create the directory namespace
    assertTrue(fs.mkdirs(baseDir));

    // create writers
    for (int i = 0; i < writers.length; i++) {
      writers[i] =
          new Writer(fs, new String("file" + i), writeInSameDir, baseDir,
              waitFileisClosed, fileCloseWaitTime);
    }


  }

  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }


  /**
   * Under load perform failover by killing leader NN1 NN2 will be active and
   * loads are now processed by NN2 Load should still continue No corrupt
   * blocks should be reported
   */
  @Test(timeout = 900000)
  public void testFailoverWhenLeaderNNCrashesTest1() {
    // Testing with replication factor of 3
    short repFactor = 3;
    LOG.info(
        "Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " +
            repFactor);
    failoverWhenLeaderNNCrashes(repFactor);
  }

  @Test(timeout = 900000)
  public void testFailoverWhenLeaderNNCrashesTest2() {
    // Testing with replication factor of 3
    short repFactor = 6;
    LOG.info(
        "Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " +
            repFactor);
    failoverWhenLeaderNNCrashes(repFactor);
  }

  private void failoverWhenLeaderNNCrashes(short replicationFactor) {
    try {
      // setup the cluster with required replication factor
      setupCluster(replicationFactor);

      // save leader namenode port to restart with the same port
      int nnport = cluster.getNameNodePort(NN1);

      try {
        // writers start writing to their files
        Writer.startWriters(writers);

        // Give all the threads a chance to create their files and write something to it
        Thread.sleep(10000);

        if (killNN) {
          for (int i = 0; i < NUM_NAMENODES - 1; i++) {
            InetSocketAddress address =
                cluster.getNameNode(i).getNameNodeAddress();
            LOG.debug("TestNN about to shutdown the namenode with address " +
                address);
            cluster.shutdownNameNode(NN1);
            LOG.debug("TestNN KILLED Namenode with address " + address);
            Thread.sleep(10000);
          }
        }


        // the load should still continue without any IO Exception thrown
        LOG.info("TestNN Wait a few seconds. Let them write some more");
        Thread.sleep(10000);

      } finally {
        Writer.stopWriters(writers);
      }
      LOG.debug("TestNN All File Should Have been closed");
      Writer.verifyFile(writers, fs);
      // the block report intervals would inform the namenode of under replicated blocks
      // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
      Writer.waitReplication(fs, writers, replicationFactor,
          waitReplicationTimeout);

      if (true) {
        return;
      }
      // restart the cluster without formatting using same ports and same configurations
      cluster.shutdown();
      cluster =
          new MiniDFSCluster.Builder(conf).nameNodePort(nnport).format(false)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
              .numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      // update the client so that it has the fresh list of namenodes. Black listed namenodes will be removed
      fs = cluster.getNewFileSystemInstance(NN1);

      Writer.verifyFile(writers,
          fs); // throws IOException. Should be caught by parent
    } catch (Exception ex) {
      LOG.error("Received exception: " + ex.getMessage(), ex);
      ex.printStackTrace();
      fail("Exception: " + ex.getMessage());
    } finally {
      shutdown();
    }

  }
}