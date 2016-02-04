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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * Keeps a Collection for every storage containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  
  private final DatanodeManager datanodeManager;

  InvalidateBlocks(final DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  /**
   * @return the number of blocks to be invalidated .
   */
  long numBlocks() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.GET_NUM_INVALIDATED_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * @param storage
   *     the storage to check
   * @param block
   *     the block to look for
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  boolean contains(final DatanodeStorageInfo storage, final BlockInfo block)
      throws StorageException, TransactionContextException {
    InvalidatedBlock blkFound = findBlock(block.getBlockId(),
        storage, block.getInodeId());
    if (blkFound == null) {
      return false;
    }
    return blkFound.getGenerationStamp() == block.getGenerationStamp();
  }

  /**
   * Add a block to the block collection
   * which will be invalidated on the specified storage.
   */
  void add(final BlockInfo block, final DatanodeStorageInfo storage,
      final boolean log) throws StorageException, TransactionContextException {
    InvalidatedBlock invBlk = new InvalidatedBlock(storage.getSid(), block
        .getBlockId(), block.getGenerationStamp(), block.getNumBytes(), block
        .getInodeId());
    if (add(invBlk)) {
      if (log) {
        NameNode.blockStateChangeLog.info(
            "BLOCK* " + getClass().getSimpleName() + ": add " + block + " to " +
                storage);
      }
    }
  }

  /**
   * Remove a storage from the invalidatesSet
   */
  void remove(final DatanodeStorageInfo storage) throws IOException {
    removeInvBlocks(storage);
  }

  /**
   * Remove the block from the specified storage.
   */
  void remove(final DatanodeStorageInfo storage, final BlockInfo block)
      throws StorageException, TransactionContextException {
    InvalidatedBlock invBlok = findBlock(block.getBlockId(),
        storage, block.getInodeId());
    if (invBlok != null) {
      removeInvalidatedBlockFromDB(
          new InvalidatedBlock(storage.getSid(),
              block.getBlockId(), block.getGenerationStamp(),
              block.getNumBytes(), block.getInodeId()));
    }
  }

  /**
   * @return a list of the storage IDs.
   */
  List<String> getStorageIDs() throws IOException {
    LightWeightRequestHandler getAllInvBlocksHandler =
        new LightWeightRequestHandler(HDFSOperationType.GET_ALL_INV_BLKS) {
          @Override
          public Object performTask() throws StorageException, IOException {
            InvalidateBlockDataAccess da =
                (InvalidateBlockDataAccess) HdfsStorageFactory
                    .getDataAccess(InvalidateBlockDataAccess.class);
            return da.findAllInvalidatedBlocks();
          }
        };
    List<InvalidatedBlock> invBlocks =
        (List<InvalidatedBlock>) getAllInvBlocksHandler.handle();
    HashSet<String> storageIds = new HashSet<String>();
    if (invBlocks != null) {
      for (InvalidatedBlock ib : invBlocks) {
        storageIds
            .add(datanodeManager.getStorage(ib.getStorageId())
                .getDatanodeDescriptor().getDatanodeUuid());
      }
    }
    return new ArrayList<String>(storageIds);
  }

  /**
   * Invalidate work for the storage.
   */
  int invalidateWork(final DatanodeStorageInfo storage) throws IOException {
    final List<Block> toInvalidate = invalidateWork(storage.getSid(), storage);
    if (toInvalidate == null) {
      return 0;
    }

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      NameNode.stateChangeLog.info(
          "BLOCK* " + getClass().getSimpleName() + ": ask " + storage +
              " to delete " + toInvalidate);
    }
    return toInvalidate.size();
  }

  private List<Block> invalidateWork(final int storageId,
      final DatanodeStorageInfo storage) throws IOException {
    final List<InvalidatedBlock> invBlocks =
        findInvBlocksbyStorageId(storageId);
    if (invBlocks == null || invBlocks.isEmpty()) {
      return null;
    }
    // # blocks that can be sent in one message is limited
    final int limit = datanodeManager.blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<Block>(limit);
    final List<InvalidatedBlock> toInvblks = new ArrayList<InvalidatedBlock>();
    final Iterator<InvalidatedBlock> it = invBlocks.iterator();
    for (int count = 0; count < limit && it.hasNext(); count++) {
      InvalidatedBlock invBlock = it.next();
      toInvalidate.add(new Block(invBlock.getBlockId(), invBlock.getNumBytes(),
          invBlock.getGenerationStamp()));
      toInvblks.add(invBlock);
    }
    removeInvBlocks(toInvblks);
    // TODO add method to storage -> functionality is now/was in Datanode
    storage.getDatanodeDescriptor().addBlocksToBeInvalidated(toInvalidate);
    return toInvalidate;
  }
  
  void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_INV_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }
  
  
  void add(final Collection<Block> blocks, final DatanodeStorageInfo storage)
      throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ADD_INV_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        List<InvalidatedBlock> invblks = new ArrayList<InvalidatedBlock>();
        for (Block blk : blocks) {
          invblks.add(new InvalidatedBlock(storage.getSid(), blk.getBlockId(),
              blk.getGenerationStamp(), blk.getNumBytes(),
              INode.NON_EXISTING_ID));
        }
        da.prepare(Collections.EMPTY_LIST, invblks, Collections.EMPTY_LIST);
        return null;
      }
    }.handle();
  }

  private boolean add(InvalidatedBlock invBlk)
      throws StorageException, TransactionContextException {
    InvalidatedBlock found =
        findBlock(invBlk.getBlockId(), invBlk.getStorageId(), invBlk.getInodeId());
    if (found == null) {
      addInvalidatedBlockToDB(invBlk);
      return true;
    }
    return false;
  }
  
  private List<InvalidatedBlock> findInvBlocksbyStorageId(final int sid)
      throws IOException {
    return (List<InvalidatedBlock>) new LightWeightRequestHandler(
        HDFSOperationType.GET_INV_BLKS_BY_STORAGEID) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockByStorageId(sid);
      }
    }.handle();
  }

  private void removeInvBlocks(final List<InvalidatedBlock> blks)
      throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.RM_INV_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        da.prepare(blks, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        return null;
      }
    }.handle();
  }
  
  private void removeInvBlocks(final DatanodeStorageInfo storage) throws
      IOException {
    final int sid = storage.getSid();

    new LightWeightRequestHandler(HDFSOperationType.RM_INV_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        da.removeAllByStorageId(sid);
        return null;
      }
    }.handle();
  }

  /**
   * Same as {@link #findBlock(long, int, int)} but takes a storage as argument.
   */
  private InvalidatedBlock findBlock(long blkId, DatanodeStorageInfo storage,
      int inodeId) throws TransactionContextException, StorageException {
    return findBlock(blkId, storage.getSid(), inodeId);
  }

  private InvalidatedBlock findBlock(long blkId, int storageID, int inodeId)
      throws StorageException, TransactionContextException {
    return (InvalidatedBlock) EntityManager
        .find(InvalidatedBlock.Finder.ByBlockIdStorageIdAndINodeId, blkId,
            storageID, inodeId);
  }
  
  private void addInvalidatedBlockToDB(InvalidatedBlock invBlk)
      throws StorageException, TransactionContextException {
    EntityManager.add(invBlk);
  }
  
  private void removeInvalidatedBlockFromDB(InvalidatedBlock invBlk)
      throws StorageException, TransactionContextException {
    EntityManager.remove(invBlk);
  }
  
  private List<InvalidatedBlock> findAllInvalidatedBlocks()
      throws StorageException, TransactionContextException {
    return (List<InvalidatedBlock>) EntityManager
        .findList(InvalidatedBlock.Finder.All);
  }
}
