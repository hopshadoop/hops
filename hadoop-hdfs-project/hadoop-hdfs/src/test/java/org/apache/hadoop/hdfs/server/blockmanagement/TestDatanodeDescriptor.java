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
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static io.hops.transaction.lock.LockFactory.BLK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  @Test
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<>(MAX_BLOCKS);
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP, Block.NON_EXISTING_BUCKET_ID));
    }
    dd.addBlocksToBeInvalidated(blockList);
    Block[] bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, REMAINING_BLOCKS);
  }
  
  @Test
  public void testBlocksCounter() throws Exception {
    HdfsStorageFactory.setConfiguration(new HdfsConfiguration());
    HdfsStorageFactory.formatStorage();

    BlocksMap blocksMap = new BlocksMap(null);
    HashBuckets.initialize(DFSConfigKeys.DFS_NUM_BUCKETS_DEFAULT);

    DatanodeDescriptor datanode = DFSTestUtil.getLocalDatanodeDescriptor(true);
    assertEquals(0, datanode.numBlocks());
    BlockInfoContiguous blk = new BlockInfoContiguous(new Block(1L), 2);
    BlockInfoContiguous blk1 = new BlockInfoContiguous(new Block(2L), 3);
    DatanodeStorageInfo[] storages = datanode.getStorageInfos();
    assertTrue(storages.length > 0);
    // add first block
    assertTrue(addBlock(blocksMap, storages[0], blk) == AddBlockResult.ADDED);
    assertEquals(1, datanode.numBlocks());
    // remove a non-existent block
    assertFalse(removeBlock(datanode, blk1));
    assertEquals(1, datanode.numBlocks());
    // add an existent block
    assertFalse(addBlock(blocksMap, storages[0], blk) == AddBlockResult.ADDED);
    System.out.println("number of blks are " + datanode.numBlocks());
    assertEquals(1, datanode.numBlocks());
    // add second block
    assertTrue(addBlock(blocksMap, storages[0], blk1) == AddBlockResult.ADDED);
    assertEquals(2, datanode.numBlocks());
    // remove first block
    assertTrue(removeBlock(datanode, blk));
    assertEquals(1, datanode.numBlocks());
    // remove second block
    assertTrue(removeBlock(datanode, blk1));
    assertEquals(0, datanode.numBlocks());
  }
  
  private AddBlockResult addBlock(final BlocksMap blocksMap,
      final DatanodeStorageInfo storage,
      final BlockInfoContiguous blk)
      throws IOException {
    return (AddBlockResult) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        blocksMap.addBlockCollection(blk, new INodeFile(blk.getInodeId(), 
            new PermissionStatus("n", "n", FsPermission.getDefault()), null, (short)1, 0, 0, 1, (byte) 0));
        return storage.addBlock(blk);
      }

    }.handle();
  }

  private boolean removeBlock(final DatanodeDescriptor dn,
      final BlockInfoContiguous blk) throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return dn.removeBlock(blk);
      }

    }.handle();
  }
}
