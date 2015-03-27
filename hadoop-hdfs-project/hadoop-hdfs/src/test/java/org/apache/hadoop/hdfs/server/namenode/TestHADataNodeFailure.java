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
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests the failover during load (read/write operations) when:
 * 1. Failover from Leader to namenode with next lowest Id
 * 2. When any other non-leader namenode crashes
 * 3. When datanodes crash
 * <p/>
 * In each of the test case, the following should be ensured
 * 1. The load should continue to run during failover
 * 2. No corrupt blocks are detected during load
 * 3. Failovers should perform successfully from the Leader namenode to the
 * namenode with the next lowest id
 */
@Ignore(value = "The design of this test needs to be reconsidered. " +
    "It fails most of the times because of race conditions.")
public class TestHADataNodeFailure extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestHADataNodeFailure.class);

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
  static int NUM_NAMENODES = 2;
  static int NUM_DATANODES = 1;
  // 10 seconds timeout default
  long NNDeathTimeout = 10000;
  boolean writeInSameDir = false;
  boolean killNN = true;
  boolean waitFileisClosed = true;
  int fileCloseWaitTime = 5000;
  long waitReplicationTimeout = 5 * 60 * 1000;
  Path baseDir = new Path("/testsLoad");
  Writer[] writers = new Writer[25];

  private void setUp(int replicationFactor) throws IOException {
    // initialize the cluster with minimum 2 namenodes and minimum 6 datanodes
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }

    this.conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        15);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 15000);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
        .format(true).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
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

  @Test(timeout = 900000)
  // Problem with this test case is that, although the new pipeline is setup  correctly
  // when it creates the next pipeline and starts to stream, another datanode crashes!! and then another.. etc.
  // it only retries when it detects an error while streaming to the datanodes in the pipeline
  // but not when adding a new datanode to the pipeline
  //[https://issues.apache.org/jira/browse/HDFS-3436]
  public void testFailoverWhenDNCrashesTest1() {
    // Testing with replication factor of 3
    short repFactor = 3;
    LOG.info(
        "TestNN Running test [testFailoverWhenDNCrashes()] with replication factor " +
            repFactor);
    failoverWhenDNCrashes(repFactor);
  }

  @Test(timeout = 900000)
  public void testFailoverWhenDNCrashesTest2() {
    // Testing with replication factor of 6
    short repFactor = 6;
    LOG.info(
        "Running test [testFailoverWhenDNCrashes()] with replication factor " +
            repFactor);
    failoverWhenDNCrashes(repFactor);
  }

  public void failoverWhenDNCrashes(short replicationFactor) {

    try {

      // reset this value
      int numDatanodesToKill = 0;
      if (replicationFactor == 3) {
        NUM_DATANODES = 5;
        numDatanodesToKill = 2;
      } else if (replicationFactor == 6) {
        NUM_DATANODES = 11;
        numDatanodesToKill = 5;
      }

      // setup the cluster with required replication factor
      setUp(replicationFactor);

      // save leader namenode port to restart with the same port
      int nnport = cluster.getNameNodePort(NN1);

      try {
        // writers start writing to their files
        Writer.startWriters(writers);

        // Give all the threads a chance to create their files and write something to it
        Thread.sleep(10000); // 10 sec

        // kill some datanodes (but make sure that we have atleast 6+)
        // The pipleline should be broken and setup again by the client
        
        for (int i = 0; i < numDatanodesToKill; i++) {
          // we need a way to ensure that atleast one valid replica is available
          int dnIndex = AppendTestUtil.nextInt(numDatanodesToKill);
          LOG.debug("TestNN DataNode Killed " +
              (cluster.getDataNodes().get(dnIndex).getDatanodeId()));
          cluster.stopDataNode(dnIndex);

          // wait for 15 seconds then kill the next datanode
          // New pipeline recovery takes place
          try {
            Thread.sleep(15000);
          } catch (InterruptedException ex) {
          }
        }
        

        // the load should still continue without any IO Exception thrown
        LOG.info("TestNN Wait a few seconds. Let them write some more");
        Thread.sleep(2000);

      } finally {
        // closing files - finalize all the files UC
        Writer.stopWriters(writers);
      }

      // the block report intervals would inform the namenode of under replicated blocks
      // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
      Writer.waitReplication(fs, writers, replicationFactor,
          waitReplicationTimeout);
      
    } catch (Exception ex) {
      LOG.error("Received exception: " + ex.getMessage(), ex);
      ex.printStackTrace();
      fail("Exception: " + ex.getMessage());
    } finally {
      shutdown();
    }

  }
}