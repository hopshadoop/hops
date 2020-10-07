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
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUnderReplicatedBlocks {
  @Test(timeout = 60000) // 5 min timeout
  public void testSetrepIncWithUnderReplicatedBlocks() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final short REPLICATION_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION_FACTOR + 1)
            .build();
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      // remove one replica from the blocksMap so block becomes under-replicated
      // but the block does not get put into the under-replicated blocks queue
      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      final ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);

      HopsTransactionalRequestHandler handler =
          new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
            INodeIdentifier inodeIdentifier;

            @Override
            public void setUp() throws StorageException {
              Block blk = b.getLocalBlock();
              inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
            }

            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE,
                  inodeIdentifier)).add(
                  lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
                  .add(lf.getBlockRelated(LockFactory.BLK.RE,
                      LockFactory.BLK.IV));
            }

            @Override
            public Object performTask() throws StorageException, IOException {
              DatanodeStorageInfo storage = bm.blocksMap.storageList(b.getLocalBlock()).get(0);
              DatanodeDescriptor dn = storage.getDatanodeDescriptor();
              bm.addToInvalidates(b.getLocalBlock(), dn);

              bm.blocksMap.removeNode(b.getLocalBlock(), dn);
              return dn;
            }
          };
      DatanodeDescriptor dn = (DatanodeDescriptor) handler.handle();
      
      //PATCH https://issues.apache.org/jira/browse/HDFS-4067
      // Compute the invalidate work in NN, and trigger the heartbeat from DN
      BlockManagerTestUtil.computeAllPendingWork(bm);
      DataNodeTestUtils.triggerHeartbeat(cluster.getDataNode(dn.getIpcPort()));
      // Wait to make sure the DataNode receives the deletion request 
      Thread.sleep(5000);
      

      HopsTransactionalRequestHandler handler2 =
          new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
            INodeIdentifier inodeIdentifier;

            @Override
            public void setUp() throws StorageException {
              Block blk = b.getLocalBlock();
              inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
            }

            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE,
                  inodeIdentifier)).add(
                  lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
                  .add(lf.getBlockRelated(LockFactory.BLK.RE,
                      LockFactory.BLK.IV));
            }

            @Override
            public Object performTask() throws StorageException, IOException {
              DatanodeDescriptor dn = (DatanodeDescriptor) getParams()[0];
              // Remove the record from blocksMap
              bm.blocksMap.removeNode(b.getLocalBlock(), dn);
              return null;
            }
          };
      handler2.setParams(dn);
      handler2.handle();
      
      // increment this file's replication factor
      FsShell shell = new FsShell(conf);
      assertEquals(0, shell.run(new String[]{"-setrep", "-w",
          Integer.toString(1 + REPLICATION_FACTOR), FILE_NAME}));
    } finally {
      cluster.shutdown();
    }
    
  }

  /**
   * The test verifies the number of outstanding replication requests for a
   * given DN shouldn't exceed the limit set by configuration property
   * dfs.namenode.replication.max-streams-hard-limit.
   * The test does the followings:
   * 1. Create a mini cluster with 2 DNs. Set large heartbeat interval so that
   *    replication requests won't be picked by any DN right away.
   * 2. Create a file with 10 blocks and replication factor 2. Thus each
   *    of the 2 DNs have one replica of each block.
   * 3. Add a DN to the cluster for later replication.
   * 4. Remove a DN that has data.
   * 5. Ask BlockManager to compute the replication work. This will assign
   *    replication requests to the only DN that has data.
   * 6. Make sure the number of pending replication requests of that DN don't
   *    exceed the limit.
   * @throws Exception
   */
  @Test(timeout=60000) // 1 min timeout
  public void testNumberOfBlocksToBeReplicated() throws Exception {
    Configuration conf = new HdfsConfiguration();

    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);

    // Large value to make sure the pending replication request can stay in
    // DatanodeDescriptor.replicateBlocks before test timeout.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 100);

    // Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
    // chooseUnderReplicatedBlocks at once.
     conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 5);

    int NUM_OF_BLOCKS = 10;
    final short REP_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
            REP_FACTOR).build();
    try {
      // create a file with 10 blocks with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, NUM_OF_BLOCKS, REP_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REP_FACTOR);

      cluster.startDataNodes(conf, 1, true, null, null, null, null);

      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      Iterator<DatanodeStorageInfo> storageInfos = getStorageInfo(b.getLocalBlock(), bm)
          .iterator();
      DatanodeDescriptor firstDn = storageInfos.next().getDatanodeDescriptor();
      DatanodeDescriptor secondDn = storageInfos.next().getDatanodeDescriptor();

      bm.getDatanodeManager().removeDatanode(firstDn,false);

      assertEquals(NUM_OF_BLOCKS, bm.getUnderReplicatedNotMissingBlocks());
      bm.computeDatanodeWork();


      assertTrue("The number of blocks to be replicated should be less than "
          + "or equal to " + bm.replicationStreamsHardLimit,
          secondDn.getNumberOfBlocksToBeReplicated()
          <= bm.replicationStreamsHardLimit);
    } finally {
      cluster.shutdown();
    }

  }
  
  List<DatanodeStorageInfo> getStorageInfo(final Block blk, final BlockManager bm) throws IOException {
    return (List<DatanodeStorageInfo>) new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE,
            inodeIdentifier)).add(
                lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE,
                LockFactory.BLK.IV));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return bm.blocksMap.storageList(blk);
      }
    }.handle();
  }

  /*
   For under replicated blocks the replication work is done in two
   different TXs. In the first TX blocks are selected, and in the second TX
   replication work is computed for the blocks. It is possible
   that after fist TX a selected block is deleted by the user and
   when the Second TX runs for such a block it would throw an
   exception
   */
  @Test
  public void testJira1593() throws Exception {
//    Logger.getRootLogger().setLevel(Level.TRACE);
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000 /*sec*/);
    final short REPLICATION_FACTOR = 2;
    final String FILE_NAME = "testFile";
    final Path FILE_PATH = new Path("/",FILE_NAME);
    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION_FACTOR-1)
                    .build();
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);

      new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
          da.deleteInode(FILE_NAME);

          BlockInfoDataAccess bda =
                  (BlockInfoDataAccess) HdfsStorageFactory.getDataAccess(BlockInfoDataAccess.class);
          bda.deleteBlocksForFile(0);
          return null;
        }
      }.handle();

      BlockInfoContiguous bic =  new BlockInfoContiguous();
      bic.setBlockIdNoPersistance(0);
      bic.setINodeIdNoPersistance(INode.ROOT_INODE_ID+1);
      cluster.getNamesystem().getBlockManager().computeReplicationWorkForBlock
              (bic,0);
      new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          UnderReplicatedBlockDataAccess da = (UnderReplicatedBlockDataAccess)
                  HdfsStorageFactory.getDataAccess(UnderReplicatedBlockDataAccess.class);
          assert da.countAll() == 0;
          return null;
        }
      }.handle();

      Block blk = new Block();
      blk.setBlockIdNoPersistance(123);
      cluster.getNamesystem().getBlockManager().computeReplicationWorkForBlock(blk, 0);
      //no exception

    } finally {
      cluster.shutdown();
    }

  }
}
