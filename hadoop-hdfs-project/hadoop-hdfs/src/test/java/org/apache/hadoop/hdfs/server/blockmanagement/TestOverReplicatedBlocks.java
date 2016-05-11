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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static io.hops.transaction.lock.LockFactory.BLK;
import static org.apache.hadoop.util.Time.now;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOverReplicatedBlocks {
  /**
   * Test processOverReplicatedBlock can handle corrupt replicas fine.
   * It make sure that it won't treat corrupt replicas as valid ones
   * thus prevents NN deleting valid replicas but keeping
   * corrupt ones.
   */
  @Test
  public void testProcesOverReplicateBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000L);
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        Integer.toString(2));
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      final Path fileName = new Path("/foo1");
      DFSTestUtil.createFile(fs, fileName, 2, (short) 3, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short) 3);
      
      // corrupt the block on datanode 0
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
      assertTrue(TestDatanodeBlockScanner.corruptReplica(block, 0));
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      // remove block scanner log to trigger block scanning
      File scanLog = new File(MiniDFSCluster
          .getFinalizedDir(cluster.getInstanceStorageDir(0, 0),
              cluster.getNamesystem().getBlockPoolId()).getParent().toString() +
          "/../dncp_block_verification.log.prev");
      //wait for one minute for deletion to succeed;
      for (int i = 0; !scanLog.delete(); i++) {
        assertTrue("Could not delete log file in one minute", i < 60);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
      }
      
      // restart the datanode so the corrupt replica will be detected
      cluster.restartDataNode(dnProps);
      DFSTestUtil.waitReplication(fs, fileName, (short) 2);
      
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      final DatanodeID corruptDataNode = DataNodeTestUtils
          .getDNRegistrationForBP(cluster.getDataNodes().get(2), blockPoolId);

      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      final HeartbeatManager hm = bm.getDatanodeManager().getHeartbeatManager();
      synchronized (hm) {
        // set live datanode's remaining space to be 0
        // so they will be chosen to be deleted when over-replication occurs
        String corruptMachineName = corruptDataNode.getXferAddr();
        for (DatanodeDescriptor datanode : hm.getDatanodes()) {
          if (!corruptMachineName.equals(datanode.getXferAddr())) {
            datanode.getStorageInfos()[0].setUtilizationForTesting(100L, 100L, 0, 100L);
            datanode.updateHeartbeat(
                BlockManagerTestUtil.getStorageReportsForDatanode(datanode), 0, 0);
          }
        }

        // decrease the replication factor to 1;
        NameNodeAdapter
            .setReplication(namesystem, fileName.toString(), (short) 1);

        new HopsTransactionalRequestHandler(
            HDFSOperationType.TEST_PROCESS_OVER_REPLICATED_BLOCKS) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException, IOException {
            inodeIdentifier =
                INodeUtil.resolveINodeFromBlock(block.getLocalBlock());
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(
                lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            // corrupt one won't be chosen to be excess one
            // without 4910 the number of live replicas would be 0: block gets lost
            assertEquals(1,
                bm.countNodes(block.getLocalBlock()).liveReplicas());
            return null;
          }
        }.handle(namesystem);

      }
      
    } finally {
      cluster.shutdown();
    }
  }

  static final long SMALL_BLOCK_SIZE =
      DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
  static final long SMALL_FILE_LENGTH = SMALL_BLOCK_SIZE * 4;

  /**
   * The test verifies that replica for deletion is chosen on a node,
   * with the oldest heartbeat, when this heartbeat is larger than the
   * tolerable heartbeat interval.
   * It creates a file with several blocks and replication 4.
   * The last DN is configured to send heartbeats rarely.
   * <p/>
   * Test waits until the tolerable heartbeat interval expires, and reduces
   * replication of the file. All replica deletions should be scheduled for the
   * last node. No replicas will actually be deleted, since last DN doesn't
   * send heartbeats.
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, SMALL_BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      fs = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 300);
      cluster.startDataNodes(conf, 1, true, null, null, null);
      DataNode lastDN = cluster.getDataNodes().get(3);
      DatanodeRegistration dnReg = DataNodeTestUtils
          .getDNRegistrationForBP(lastDN, namesystem.getBlockPoolId());
      String lastDNid = dnReg.getDatanodeUuid();

      final Path fileName = new Path("/foo2");
      DFSTestUtil.createFile(fs, fileName, SMALL_FILE_LENGTH, (short) 4, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short) 4);

      // Wait for tolerable number of heartbeats plus one
      DatanodeDescriptor nodeInfo = null;
      long lastHeartbeat = 0;
      long waitTime = DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT * 1000 *
          (DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT +
              1);
      do {
        nodeInfo = namesystem.getBlockManager().getDatanodeManager()
            .getDatanode(dnReg);
        lastHeartbeat = nodeInfo.getLastUpdate();
      } while (now() - lastHeartbeat < waitTime);
      fs.setReplication(fileName, (short) 3);

      BlockLocation locs[] =
          fs.getFileBlockLocations(fs.getFileStatus(fileName), 0,
              Long.MAX_VALUE);

      // All replicas for deletion should be scheduled on lastDN.
      // And should not actually be deleted, because lastDN does not heartbeat.
      Collection<Block> dnBlocks =
          namesystem.getBlockManager().excessReplicateMap.get(
              lastDNid, namesystem.getBlockManager().getDatanodeManager());
      assertEquals("Replicas on node " + lastDNid + " should have been deleted",
          SMALL_FILE_LENGTH / SMALL_BLOCK_SIZE, dnBlocks.size());
      for (BlockLocation location : locs) {
        assertEquals("Block should still have 4 replicas", 4,
            location.getNames().length);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test over replicated block should get invalidated when decreasing the
   * replication for a partial block.
   */
  @Test
  public void testInvalidateOverReplicatedBlock() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path(MiniDFSCluster.getBaseDirectory(), "/foo1");
      FSDataOutputStream out = fs.create(p, (short) 2);
      out.writeBytes("HDFS-3119: " + p);
      out.hsync();
      fs.setReplication(p, (short) 1);
      out.close();
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, p);
      
      new HopsTransactionalRequestHandler(
          HDFSOperationType.TEST_PROCESS_OVER_REPLICATED_BLOCKS) {
        INodeIdentifier inodeIdentifier;

        @Override
        public void setUp() throws StorageException, IOException {
          inodeIdentifier =
              INodeUtil.resolveINodeFromBlock(block.getLocalBlock());
        }

        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          locks.add(
              lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
              .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR));
        }

        @Override
        public Object performTask() throws StorageException, IOException {
          assertEquals("Expected only one live replica for the block", 1,
              bm.countNodes(block.getLocalBlock()).liveReplicas());
          return null;
        }

      }.handle(namesystem);

    } finally {
      cluster.shutdown();
    }
  }
}
