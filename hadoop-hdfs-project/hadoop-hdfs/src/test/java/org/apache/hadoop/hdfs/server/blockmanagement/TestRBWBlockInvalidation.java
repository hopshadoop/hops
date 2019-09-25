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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.collect.Lists;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import java.io.Closeable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.TestDNFencing;
import org.apache.hadoop.io.IOUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test when RBW block is removed. Invalidation of the corrupted block happens
 * and then the under replicated block gets replicated to the datanode.
 */
public class TestRBWBlockInvalidation {
  private static final Log LOG = LogFactory.getLog(TestRBWBlockInvalidation.class);

  private static NumberReplicas countReplicas(final FSNamesystem namesystem,
      final ExtendedBlock block) throws IOException {
    return (NumberReplicas) new HopsTransactionalRequestHandler(
        HDFSOperationType.COUNT_NODES) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier =
            INodeUtil.resolveINodeFromBlock(block.getLocalBlock());
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.ER,
                LockFactory.BLK.CR));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return namesystem.getBlockManager().countNodes(block.getLocalBlock());
      }

    }.handle(namesystem);
  }

  /**
   * Test when a block's replica is removed from RBW folder in one of the
   * datanode, namenode should ask to invalidate that corrupted block and
   * schedule replication for one more replica for that under replicated block.
   */
  @Test(timeout = 600000)
  public void testBlockInvalidationWhenRBWReplicaMissedInDN()
      throws IOException, InterruptedException {
    // This test cannot pass on Windows due to file locking enforcement.  It will
    // reject the attempt to delete the block file from the RBW folder.
    assumeTrue(!Path.WINDOWS);
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 300);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FSDataOutputStream out = null;
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/tmp/TestRBWBlockInvalidation", "foo1");
      out = fs.create(testPath, (short) 2);
      out.writeBytes("HDFS-3157: " + testPath);
      out.hsync();
      cluster.startDataNodes(conf, 1, true, null, null, null);
      String bpid = namesystem.getBlockPoolId();
      ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, testPath);
      Block block = blk.getLocalBlock();
      DataNode dn = cluster.getDataNodes().get(0);

      // Delete partial block and its meta information from the RBW folder
      // of first datanode.
      File blockFile = DataNodeTestUtils.getBlockFile(dn, bpid, block);
      File metaFile = DataNodeTestUtils.getMetaFile(dn, bpid, block);
      assertTrue("Could not delete the block file from the RBW folder",
          blockFile.delete());
      assertTrue("Could not delete the block meta file from the RBW folder",
          metaFile.delete());

      out.close();

      int liveReplicas = 0;
      while (true) {
        if ((liveReplicas = countReplicas(namesystem, blk).liveReplicas()) < 2) {
          // This confirms we have a corrupt replica
          LOG.info("Live Replicas after corruption: " + liveReplicas);
          break;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be less than 2 replicas in the liveReplicasMap", 1, liveReplicas);

      while (true) {
        if ((liveReplicas = countReplicas(namesystem, blk).liveReplicas()) > 1) {
          //Wait till the live replica count becomes equal to Replication Factor
          LOG.info("Live Replicas after Rereplication: " + liveReplicas);
          break;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be two live replicas", 2, liveReplicas);

      while (true) {
        Thread.sleep(100);
        if (countReplicas(namesystem, blk).corruptReplicas() == 0) {
          LOG.info("Corrupt Replicas becomes 0");
          break;
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
      cluster.shutdown();
    }
  }
  
  /**
   * Regression test for HDFS-4799, a case where, upon restart, if there
   * were RWR replicas with out-of-date genstamps, the NN could accidentally
   * delete good replicas instead of the bad replicas.
   */
  @Test(timeout = 60000)
  public void testRWRInvalidation() throws Exception {
    Configuration conf = new HdfsConfiguration();

    // Set the deletion policy to be randomized rather than the default.
    // The default is based on disk space, which isn't controllable
    // in the context of the test, whereas a random one is more accurate
    // to what is seen in real clusters (nodes have random amounts of free
    // space)
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        TestDNFencing.RandomDeleterPolicy.class, BlockPlacementPolicy.class); 

    // Speed up the test a bit with faster heartbeats.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    // Test with a bunch of separate files, since otherwise the test may
    // fail just due to "good luck", even if a bug is present.
    List<Path> testPaths = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      testPaths.add(new Path("/test" + i));
    }

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    try {
      List<FSDataOutputStream> streams = Lists.newArrayList();
      try {
        // Open the test files and write some data to each
        for (Path path : testPaths) {
          FSDataOutputStream out = cluster.getFileSystem().create(path, (short) 2);
          streams.add(out);

          out.writeBytes("old gs data\n");
          out.hflush();
        }

        // Shutdown one of the nodes in the pipeline
        DataNodeProperties oldGenstampNode = cluster.stopDataNode(0);

        // Write some more data and flush again. This data will only
        // be in the latter genstamp copy of the blocks.
        for (int i = 0; i < streams.size(); i++) {
          Path path = testPaths.get(i);
          FSDataOutputStream out = streams.get(i);

          out.writeBytes("new gs data\n");
          out.hflush();

          // Set replication so that only one node is necessary for this block,
          // and close it.
          cluster.getFileSystem().setReplication(path, (short) 1);
          out.close();
        }

        // Upon restart, there will be two replicas, one with an old genstamp
        // and one current copy. This test wants to ensure that the old genstamp
        // copy is the one that is deleted.
        LOG.info("=========================== restarting cluster");
        DataNodeProperties otherNode = cluster.stopDataNode(0);
        cluster.restartNameNode(false);

        // Restart the datanode with the corrupt replica first.
        cluster.restartDataNode(oldGenstampNode);
        cluster.waitActive();

        // Then the other node
        cluster.restartDataNode(otherNode);
        cluster.waitActive();

        // Compute and send invalidations, waiting until they're fully processed.
        cluster.getNameNode().getNamesystem().getBlockManager()
            .computeInvalidateWorkForDNs(2);
        cluster.triggerHeartbeats();
        HATestUtil.waitForDNDeletions(cluster);
        cluster.triggerDeletionReports();

        // Make sure we can still read the blocks.
        for (Path path : testPaths) {
          String ret = DFSTestUtil.readFile(cluster.getFileSystem(), path);
          assertEquals("old gs data\n" + "new gs data\n", ret);
        }
      } finally {
        IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      }
    } finally {
      cluster.shutdown();
    }

  }
}
