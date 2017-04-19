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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionCluster;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestUnderReplicatedBlockQueues extends Assert {

  /**
   * Test that adding blocks with different replication counts puts them
   * into different queues
   *
   * @throws Throwable
   *     if something goes wrong
   */
  @Test
  public void testBlockPriorities() throws Throwable {
    HdfsStorageFactory.setConfiguration(new HdfsConfiguration());
    StorageConnector connector = HdfsStorageFactory.getConnector().connectorFor(TransactionCluster.PRIMARY);
    HdfsStorageFactory.formatStorage(connector);
    
    UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
    BlockInfo block1 = add(new BlockInfo(new Block(1), 1));
    BlockInfo block2 = add(new BlockInfo(new Block(2), 2));
    BlockInfo block_very_under_replicated = add(new BlockInfo(new Block(3), 3));
    BlockInfo block_corrupt = add(new BlockInfo(new Block(4), 4));
    
    //add a block with a single entry
    assertAdded(queues, block1, 1, 0, 3);

    assertEquals(1, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.size());
    assertInLevel(queues, block1, UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    //repeated additions fail
    assertFalse(add(queues, block1, 1, 0, 3));

    //add a second block with two replicas
    assertAdded(queues, block2, 2, 0, 3);
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(2, queues.size());
    assertInLevel(queues, block2, UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
    //now try to add a block that is corrupt
    assertAdded(queues, block_corrupt, 0, 0, 3);
    assertEquals(3, queues.size());
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.getCorruptBlockSize());
    assertInLevel(queues, block_corrupt,
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);

    //insert a very under-replicated block
    assertAdded(queues, block_very_under_replicated, 4, 0, 25);
    assertInLevel(queues, block_very_under_replicated,
        UnderReplicatedBlocks.QUEUE_VERY_UNDER_REPLICATED);

  }

  private void assertAdded(UnderReplicatedBlocks queues, BlockInfo block,
      int curReplicas, int decomissionedReplicas, int expectedReplicas)
      throws IOException {
    assertTrue("Failed to add " + block,
        add(queues, block, curReplicas, decomissionedReplicas,
            expectedReplicas));
  }

  /**
   * Determine whether or not a block is in a level without changing the API.
   * Instead get the per-level iterator and run though it looking for a match.
   * If the block is not found, an assertion is thrown.
   * <p/>
   * This is inefficient, but this is only a test case.
   *
   * @param queues
   *     queues to scan
   * @param block
   *     block to look for
   * @param level
   *     level to select
   */
  private void assertInLevel(UnderReplicatedBlocks queues, BlockInfo block,
      int level) {
    UnderReplicatedBlocks.BlockIterator bi = queues.iterator(level);
    while (bi.hasNext()) {
      Block next = bi.next();
      if (block.equals(next)) {
        return;
      }
    }
    fail("Block " + block + " not found in level " + level);
  }
  
  
  private BlockInfo add(final BlockInfo block) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
      }

      @Override
      public Object performTask(StorageConnector connector) throws StorageException, IOException {
        EntityManager.add(new BlockInfo(block));
        return null;
      }
    }.handle();
    return block;
  }

  private boolean add(final UnderReplicatedBlocks queues, final BlockInfo block,
      final int curReplicas, final int decomissionedReplicas,
      final int expectedReplicas) throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(block.getBlockId(),
            new INodeIdentifier(block.getInodeId())))
            .add(lf.getBlockRelated(LockFactory.BLK.UR));
      }

      @Override
      public Object performTask(StorageConnector connector) throws StorageException, IOException {
        return queues
            .add(block, curReplicas, decomissionedReplicas, expectedReplicas);
      }
    }.handle();
  }
}
