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

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.Time;

/**
 * Keeps a Collection for every storage containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {

  private static final Log LOG = LogFactory.getLog(InvalidateBlocks.class);;

    private final int blockInvalidateLimit;

  /**
   * The period of pending time for block invalidation since the NameNode
   * startup
   */
  private final long pendingPeriodInMs;
  /** the startup time */
  private final long startupTime = Time.monotonicNow();
  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    printBlockDeletionTime(BlockManager.LOG);
  }

  private void printBlockDeletionTime(final Log log) {
    log.info(DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY
        + " is set to " + DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    log.info("The block deletion will start around "
        + sdf.format(calendar.getTime()));
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
   * @param dn
   *     the datanode to check
   * @param block
   *     the block to look for
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  boolean contains(final DatanodeStorageInfo dn, final BlockInfo block)
      throws StorageException, TransactionContextException {
    InvalidatedBlock blkFound = findBlock(block.getBlockId(),
        dn.getSid(), block.getInodeId());
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

    InvalidatedBlock invBlk = new InvalidatedBlock(
        storage.getSid(),
        block.getBlockId(),
        block.getGenerationStamp(),
        block.getNumBytes(),
        block.getInodeId());

    if (add(invBlk)) {
      LOG.info("BLOCK* " + getClass().getSimpleName() + ": add " + block + " to " + storage);
    } else {
      LOG.info("failed to add BLOCK* " + getClass().getSimpleName() + ": add " + block + " to " + storage);
    }
  }

  /**
   * Remove a list of storages (typically all storages on a datanode) from the
   * invalidatesSet
   */
  void remove(List<Integer> sids) throws IOException {
    if(sids != null) {
      for (int sid : sids) {
        removeInvBlocks(sid);
      }
    }
  }

  void remove(int sid) throws IOException {
    removeInvBlocks(sid);
  }
  
  /**
   * Remove the block from the specified storage.
   */
  void remove(final DatanodeStorageInfo storageInfo, final BlockInfo block) throws IOException {
    InvalidatedBlock invBlok = findBlock(block.getBlockId(), storageInfo.getSid(), block.getInodeId());
    if (invBlok != null) {
      removeInvalidatedBlockFromDB(block.getBlockId(), storageInfo.getSid());
    }
  }

  /**
   * @return a list of the datanode Uuids.
   */
  List<Integer> getSids() throws IOException {
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
    ArrayList<Integer> sids = new ArrayList<>();
    if (invBlocks != null) {
      for (InvalidatedBlock ib : invBlocks) {
        sids.add(ib.getStorageId());
      }
    }
    return sids;
  }

  /**
   * @return the remianing pending time
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }
  
  /**
   * Invalidate work for the datanode.
   */
  List<Block> invalidateWork(DatanodeDescriptor dn) throws IOException {
    final long delay = getInvalidationDelay();
    if (delay > 0) {
      if (BlockManager.LOG.isDebugEnabled()) {
        BlockManager.LOG
            .debug("Block deletion is delayed during NameNode startup. "
                + "The deletion will start after " + delay + " ms.");
      }
      return null;
    }
    final List<InvalidatedBlock> invBlocks = new ArrayList<InvalidatedBlock>();

    for(DatanodeStorageInfo storage : dn.getStorageInfos()) {
      invBlocks.addAll(findInvBlocksbySid(storage.getSid()));
    }

    if (invBlocks == null || invBlocks.isEmpty()) {
      return null;
    }
    // # blocks that can be sent in one message is limited
    final int limit = blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<>(limit);
    final List<InvalidatedBlock> toInvblks = new ArrayList<>();
    final Iterator<InvalidatedBlock> it = invBlocks.iterator();
    for (int count = 0; count < limit && it.hasNext(); count++) {
      InvalidatedBlock invBlock = it.next();
      toInvalidate.add(new Block(invBlock.getBlockId(), invBlock.getNumBytes(), invBlock.getGenerationStamp()));
      toInvblks.add(invBlock);
    }
    removeInvBlocks(toInvblks);
    dn.addBlocksToBeInvalidated(toInvalidate);
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
        List<InvalidatedBlock> invblks = new ArrayList<>();
        for (Block blk : blocks) {
          invblks.add(new InvalidatedBlock(storage.getSid(), blk.getBlockId(),
              blk.getGenerationStamp(), blk.getNumBytes(), BlockInfo.NON_EXISTING_ID));
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
  
  private List<InvalidatedBlock> findInvBlocksbySid(final int sid)
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
  
  private void removeInvBlocks(final int sid) throws
      IOException {

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

  private InvalidatedBlock findBlock(long blkId, int sid, long
      inodeId) throws StorageException, TransactionContextException {
    return (InvalidatedBlock) EntityManager
        .find(InvalidatedBlock.Finder.ByBlockIdSidAndINodeId, blkId,
            sid, inodeId);
  }
  
  private void addInvalidatedBlockToDB(InvalidatedBlock invBlk)
      throws StorageException, TransactionContextException {
    EntityManager.add(invBlk);
  }

  /*
  Removes (all) replica's of the given block on the given datanode.
   */
  private void removeInvalidatedBlockFromDB(final long blockId, final int sid)
      throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.RM_INV_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        da.removeByBlockIdAndStorageId(blockId, sid);
        return null;
      }
    }.handle();
  }

  private List<InvalidatedBlock> findAllInvalidatedBlocks()
      throws StorageException, TransactionContextException {
    return (List<InvalidatedBlock>) EntityManager
        .findList(InvalidatedBlock.Finder.All);
  }

  public Map<DatanodeInfo, List<Integer>> getDatanodes(DatanodeManager manager)
      throws IOException {
    Map<DatanodeInfo, List<Integer>> nodes = new HashMap<DatanodeInfo, List<Integer>>();
    for(int sid : getSids()) {
      DatanodeInfo node = manager.getDatanodeBySid(sid);
      
      List<Integer> sids = nodes.get(node);
      if(sids==null){
        sids = new ArrayList<>();
        nodes.put(node, sids);
      }
      sids.add(sid);
      
    }

    return nodes;
  }
}
