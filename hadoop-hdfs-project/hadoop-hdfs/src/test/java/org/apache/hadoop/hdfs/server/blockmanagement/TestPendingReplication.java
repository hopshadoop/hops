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
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the internals of PendingReplicationBlocks.java,
 * as well as how PendingReplicationBlocks acts in BlockManager
 */
public class TestPendingReplication {
  final static int TIMEOUT = 3;     // 3 seconds
  private static final int DFS_REPLICATION_INTERVAL = 1;
  // Number of datanodes in the cluster
  private static final int DATANODE_COUNT = 5;

  @Test
  public void testPendingReplication() throws IOException, StorageException {
    HdfsStorageFactory.setConfiguration(new HdfsConfiguration());
    HdfsStorageFactory.formatStorage();

    PendingReplicationBlocks pendingReplications =
        new PendingReplicationBlocks(TIMEOUT * 1000);
    pendingReplications.start();

    //
    // Add 10 blocks to pendingReplications.
    //
    for (int i = 0; i < 10; i++) {
      BlockInfo block = newBlockInfo(new Block(i, i, 0), i);
      increment(pendingReplications, block, i);
    }
    
    assertEquals("Size of pendingReplications ", 10,
        pendingReplications.size());


    //
    // remove one item and reinsert it
    //
    BlockInfo blk = newBlockInfo(new Block(8, 8, 0), 8);
    decrement(pendingReplications, blk);             // removes one replica
    assertEquals("pendingReplications.getNumReplicas ", 7,
        getNumReplicas(pendingReplications, blk));

    for (int i = 0; i < 7; i++) {
      decrement(pendingReplications, blk);           // removes all replicas
    }
    assertTrue(pendingReplications.size() == 9);
    increment(pendingReplications, blk, 8);
    assertTrue(pendingReplications.size() == 10);

    //
    // verify that the number of replicas returned
    // are sane.
    //
    for (int i = 0; i < 10; i++) {
      BlockInfo block = newBlockInfo(new Block(i, i, 0), i);
      int numReplicas = getNumReplicas(pendingReplications, block);
      assertTrue(numReplicas == i);
    }

    //
    // verify that nothing has timed out so far
    //
    assertTrue(pendingReplications.getTimedOutBlocks() == null);

    //
    // Wait for one second and then insert some more items.
    //
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    for (int i = 10; i < 15; i++) {
      BlockInfo block = newBlockInfo(new Block(i, i, 0), i);
      increment(pendingReplications, block, i);
    }
    assertTrue(pendingReplications.size() == 15);

    //
    // Wait for everything to timeout.
    //
    int loop = 0;
    while (pendingReplications.size() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      loop++;
    }
    System.out.println("Had to wait for " + loop +
        " seconds for the lot to timeout");

    //
    // Verify that everything has timed out.
    //
    assertEquals("Size of pendingReplications ", 0, pendingReplications.size());
    long[] timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (long aTimedOut2 : timedOut) {
      assertTrue(aTimedOut2 < 15);
    }
    
    //
    // Verify that the blocks have not been removed from the pending database
    //
    timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (long aTimedOut1 : timedOut) {
      assertTrue(aTimedOut1 < 15);
    }
    //
    // Verify that the blocks have not been removed from the pending database
    //
    timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (long aTimedOut : timedOut) {
      assertTrue(aTimedOut < 15);
    }
    
    pendingReplications.stop();
  }
  
  /**
   * Test if BlockManager can correctly remove corresponding pending records
   * when a file is deleted
   *
   * @throws Exception
   */
  @Test
  public void testPendingAndInvalidate() throws Exception {
    final Configuration CONF = new HdfsConfiguration();
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(CONF).numDataNodes(DATANODE_COUNT).build();
    cluster.waitActive();
    
    FSNamesystem namesystem = cluster.getNamesystem();
    BlockManager bm = namesystem.getBlockManager();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // 1. create a file
      Path filePath = new Path("/tmp.txt");
      DFSTestUtil.createFile(fs, filePath, 1024, (short) 3, 0L);
      
      // 2. disable the heartbeats
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      }
      
      // 3. mark a couple of blocks as corrupt
      LocatedBlock block = NameNodeAdapter
          .getBlockLocations(cluster.getNameNode(), filePath.toString(), 0, 1)
          .get(0);
      bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
          "STORAGE_ID", "TEST");
      bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[1],
          "STORAGE_ID", "TEST");
      BlockManagerTestUtil.computeAllPendingWork(bm);
      BlockManagerTestUtil.updateState(bm);
      assertEquals(bm.getPendingReplicationBlocksCount(), 1L);
      assertEquals(getNumReplicas(bm.pendingReplications,
          (BlockInfo) block.getBlock().getLocalBlock()), 2);
      
      // 4. delete the file
      fs.delete(filePath, true);
      // retry at most 10 times, each time sleep for 1s. Note that 10s is much
      // less than the default pending record timeout (5~10min)
      int retries = 10;
      long pendingNum = bm.getPendingReplicationBlocksCount();
      while (pendingNum != 0 && retries-- > 0) {
        Thread.sleep(1000);  // let NN do the deletion
        BlockManagerTestUtil.updateState(bm);
        pendingNum = bm.getPendingReplicationBlocksCount();
      }
      assertEquals(pendingNum, 0L);
    } finally {
      cluster.shutdown();
    }
  }
  
  
  private void increment(final PendingReplicationBlocks pendingReplications,
      final Block block, final int numReplicas) throws IOException {
    incrementOrDecrementPendingReplications(pendingReplications, block, true,
        numReplicas);
  }

  private void decrement(final PendingReplicationBlocks pendingReplications,
      final Block block) throws IOException {
    incrementOrDecrementPendingReplications(pendingReplications, block, false,
        -1);
  }

  private void incrementOrDecrementPendingReplications(
      final PendingReplicationBlocks pendingReplications, final Block block,
      final boolean inc, final int numReplicas) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST_PENDING_REPLICATION) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.PE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfo blockInfo = EntityManager
            .find(BlockInfo.Finder.ByBlockIdAndINodeId, block.getBlockId());
        if (inc) {
          pendingReplications.increment(blockInfo, numReplicas);
        } else {
          pendingReplications.decrement(blockInfo);
        }
        return null;
      }
    }.handle();
  }

  private int getNumReplicas(final PendingReplicationBlocks pendingReplications,
      final Block block) throws IOException {
    return (Integer) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST_PENDING_REPLICATION) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.PE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfo blockInfo = EntityManager
            .find(BlockInfo.Finder.ByBlockIdAndINodeId, block.getBlockId());
        return pendingReplications.getNumReplicas(blockInfo);
      }
    }.handle();
  }
  
  
  /**
   * Test processPendingReplications
   *
   * @throws Exception
   */
  @Test
  public void testProcessPendingReplications() throws Exception {
    final short REPLICATION_FACTOR = (short) 1;
    
    // start a mini dfs cluster of 2 nodes
    final Configuration conf = new HdfsConfiguration();
    //we do not want the nameNode to run processPendingReplications before 
    //we do it ourself 
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        10);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION_FACTOR)
            .build();
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      final FileSystem fs = cluster.getFileSystem();

      PendingReplicationBlocks pendingReplications = bm.pendingReplications;

      //
      // populate the cluster with 10 one block file
      //
      for (int i = 0; i < 10; i++) {
        final Path FILE_PATH = new Path("/testfile_" + i);
        DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
        DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
        //increase the block replication so that they are under replicated
        fs.setReplication(FILE_PATH, (short) 2);
        final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
        increment(pendingReplications, block.getLocalBlock(), i);
      }

      int test = pendingReplications.size();

      assertEquals("Size of pendingReplications " + test, 10,
          pendingReplications.size());


      //
      // Wait for everything to timeout.
      //
      int loop = 0;
      while (pendingReplications.size() > 0) {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
        loop++;
      }
      System.out.println("Had to wait for " + loop +
          " seconds for the lot to timeout");

      //
      // Verify that everything has timed out.
      //
      assertEquals("Size of pendingReplications ", 0,
          pendingReplications.size());
      long[] timedOut = pendingReplications.getTimedOutBlocks();
      assertTrue(timedOut != null && timedOut.length == 10);

      // run processPendingReplications
      bm.processPendingReplications();

      //
      // Verify that the blocks have been removed from the pendingreplication
      // database
      //
      timedOut = pendingReplications.getTimedOutBlocks();
      assertTrue("blocks removed from pending", timedOut == null);

      //
      // Verify that the blocks have been added to the underReplicated database
      //
      UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
      assertEquals("Size of underReplications ", 10, queues.size());


    } finally {
      cluster.shutdown();
    }
  }

  private BlockInfo newBlockInfo(final Block block, final int inodeId)
      throws IOException {
    final BlockInfo blockInfo = new BlockInfo(block, inodeId);
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        EntityManager.add(blockInfo);
        return null;
      }
    }.handle();
    return blockInfo;
  }
}
