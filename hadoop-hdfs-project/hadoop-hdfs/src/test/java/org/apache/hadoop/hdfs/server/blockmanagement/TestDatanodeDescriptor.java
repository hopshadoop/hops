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
      blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
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

    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    assertEquals(0, dd.numBlocks());
    BlockInfo blk = new BlockInfo(new Block(1L), INode.NON_EXISTING_ID);
    BlockInfo blk1 = new BlockInfo(new Block(2L), INode.NON_EXISTING_ID);
    // add first block
    assertTrue(addBlock(blocksMap, dd, blk));
    assertEquals(1, dd.numBlocks());
    // remove a non-existent block
    assertFalse(removeBlock(dd, blk1));
    assertEquals(1, dd.numBlocks());
    // add an existent block
    assertFalse(addBlock(blocksMap, dd, blk));
    System.out.println("number of blks are " + dd.numBlocks());
    assertEquals(1, dd.numBlocks());
    // add second block
    assertTrue(addBlock(blocksMap, dd, blk1));
    assertEquals(2, dd.numBlocks());
    // remove first block
    assertTrue(removeBlock(dd, blk));
    assertEquals(1, dd.numBlocks());
    // remove second block
    assertTrue(removeBlock(dd, blk1));
    assertEquals(0, dd.numBlocks());
  }
  
  private boolean addBlock(final BlocksMap blocksMap, final DatanodeDescriptor
      dn, final BlockInfo blk)
      throws IOException {
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
        blocksMap.addBlockCollection(blk, new INodeFile(new PermissionStatus
            ("n", "n", FsPermission.getDefault()), null, (short)1, 0, 0, 1));
        return dn.addBlock(blk);
      }

    }.handle();
  }

  private boolean removeBlock(final DatanodeDescriptor dn, final BlockInfo blk)
      throws IOException {
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
        return dn.removeReplica(blk);
      }

    }.handle();
  }
}
