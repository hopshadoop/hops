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

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  @Test(timeout = 300000)
  public void testBlockInvalidationWhenRBWReplicaMissedInDN()
      throws IOException, InterruptedException {
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
}
