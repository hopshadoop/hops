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
package io.hops.transaction.context;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.transaction.lock.BlockLock;
import io.hops.transaction.lock.LastTwoBlocksLock;
import io.hops.transaction.lock.Lock;
import io.hops.transaction.lock.SqlBatchedBlocksLock;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

public class BlockInfoContext extends BaseEntityContext<Long, BlockInfoContiguous> {

  private final static int DEFAULT_NUM_BLOCKS_PER_INODE = 10;

  private final Map<Long, List<BlockInfoContiguous>> inodeBlocks =
      new HashMap<>();
  private final List<BlockInfoContiguous> concatRemovedBlks = new ArrayList<>();

  private final BlockInfoDataAccess<BlockInfoContiguous> dataAccess;

  private boolean foundByInode = false;

  public BlockInfoContext(BlockInfoDataAccess<BlockInfoContiguous> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeBlocks.clear();
    concatRemovedBlks.clear();
  }

  @Override
  public void update(BlockInfoContiguous blockInfo) throws TransactionContextException {
    super.update(blockInfo);
    //only called in update not add
    updateInodeBlocks(blockInfo);
    if(isLogTraceEnabled()) {
      log("updated-blockinfo", "bid", blockInfo.getBlockId(), "inodeId",
              blockInfo.getInodeId(), "blk index", blockInfo.getBlockIndex(),
              "cloudBucketID", blockInfo.getCloudBucketID());
    }

  }

  @Override
  public void remove(BlockInfoContiguous blockInfo) throws TransactionContextException {
    super.remove(blockInfo);
    removeBlockFromInodeBlocks(blockInfo);
    if(isLogTraceEnabled()) {
      log("removed-blockinfo", "bid", blockInfo.getBlockId());
    }
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    if (foundByInode && !(tlm.getLock(Lock.Type.Block) instanceof BlockLock)
        && !(tlm.getLock(Lock.Type.Block) instanceof LastTwoBlocksLock)
        && !(tlm.getLock(Lock.Type.Block) instanceof SqlBatchedBlocksLock)) {
      throw new TransactionContextException("You can't call find ByINodeId(s) when taking the lock only on one block");
    }
    Collection<BlockInfoContiguous> removed = new ArrayList<>(getRemoved());
    removed.addAll(concatRemovedBlks);
    dataAccess.prepare(removed, getAdded(), getModified());
  }

  @Override
  public BlockInfoContiguous find(FinderType<BlockInfoContiguous> finder, Object... params)
      throws TransactionContextException, StorageException {
    BlockInfoContiguous.Finder bFinder = (BlockInfoContiguous.Finder) finder;
    switch (bFinder) {
      case ByBlockIdAndINodeId:
        return findById(bFinder, params);
      case ByMaxBlockIndexForINode:
        return findMaxBlk(bFinder, params);
      case ByINodeIdAndIndex:
        return findByInodeIdAndIndex(bFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<BlockInfoContiguous> findList(FinderType<BlockInfoContiguous> finder,
      Object... params) throws TransactionContextException, StorageException {
    BlockInfoContiguous.Finder bFinder = (BlockInfoContiguous.Finder) finder;
    switch (bFinder) {
      case ByINodeId:
        foundByInode = true;
        return findByInodeId(bFinder, params);
      case ByBlockIdsAndINodeIds:
        return findBatch(bFinder, params);
      case ByINodeIds:
        foundByInode = true;
        return findByInodeIds(bFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds =
        (HdfsTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        INodeCandidatePrimaryKey trg_param =
            (INodeCandidatePrimaryKey) params[0];
        List<INodeCandidatePrimaryKey> srcs_param =
            (List<INodeCandidatePrimaryKey>) params[1]; // these are the merged inodes
        List<BlockInfoContiguous> oldBlks = (List<BlockInfoContiguous>) params[2];
        deleteBlocksForConcat(trg_param, srcs_param, oldBlks);
        //new blocks have been added by the concat function
        //we just have to delete the blocks rows that dont make sence
        break;
      case EmptyFile:
        Long inodeId = (Long) params[0];
        List<BlockInfoContiguous> result = Collections.emptyList();
        inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
        break;
    }
  }

  @Override
  Long getKey(BlockInfoContiguous blockInfo) {
    return blockInfo.getBlockId();
  }


  private List<BlockInfoContiguous> findByInodeId(BlockInfoContiguous.Finder bFinder,
      final Object[] params)
      throws TransactionContextException, StorageException {
    List<BlockInfoContiguous> result = null;
    final Long inodeId = (Long) params[0];
    if (inodeBlocks.containsKey(inodeId)) {
      result = inodeBlocks.get(inodeId);
      hit(bFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(bFinder, params);
      result = dataAccess.findByInodeId(inodeId);
      inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
      miss(bFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<BlockInfoContiguous> findBatch(BlockInfoContiguous.Finder bFinder, Object[] params)
      throws TransactionContextException, StorageException {
    List<BlockInfoContiguous> result = null;
    final long[] blockIds = (long[]) params[0];
    final long[] inodeIds = (long[]) params[1];
    aboutToAccessStorage(bFinder, params);
    result = dataAccess.findByIds(blockIds, inodeIds);
    miss(bFinder, result, "BlockIds", Arrays.toString(blockIds), "InodeIds",
        Arrays.toString(inodeIds));
    return syncBlockInfoInstances(result, blockIds);
  }

  private List<BlockInfoContiguous> findByInodeIds(BlockInfoContiguous.Finder bFinder,
      Object[] params) throws TransactionContextException, StorageException {
    List<BlockInfoContiguous> result = null;
    final long[] ids = (long[]) params[0];
    aboutToAccessStorage(bFinder, params);
    result = dataAccess.findByInodeIds(ids);
    for (long id : ids) {
      inodeBlocks.put(id, null);
    }
    miss(bFinder, result, "InodeIds", Arrays.toString(ids));
    return syncBlockInfoInstances(result, true);
  }

  private BlockInfoContiguous findByInodeIdAndIndex(BlockInfoContiguous.Finder bFinder,
      final Object[] params)
      throws TransactionContextException, StorageException {
    List<BlockInfoContiguous> blocks = null;
    BlockInfoContiguous result = null;
    final Long inodeId = (Long) params[0];
    final Integer index = (Integer) params[1];
    if (inodeBlocks.containsKey(inodeId)) {
      blocks = inodeBlocks.get(inodeId);
      for(BlockInfoContiguous bi: blocks){
        if(bi.getBlockIndex()==index){
          result = bi;
          break;
        }
      }
      hit(bFinder, result, "inodeid", inodeId);
    } else {
      throw new TransactionContextException("this function can't be called without owning a lock on the block");
    }
    return result;
  }
  
  private BlockInfoContiguous findById(BlockInfoContiguous.Finder bFinder, final Object[] params)
      throws TransactionContextException, StorageException {
    BlockInfoContiguous result = null;
    long blockId = (Long) params[0];
    Long inodeId = null;
    if (params.length > 1 && params[1] != null) {
      inodeId = (Long) params[1];
    }
    if (contains(blockId)) {
      result = get(blockId);
      hit(bFinder, result, "bid", blockId, "inodeId",
          inodeId != null ? Long.toString(inodeId) : "NULL");
    } else {
      // some test intentionally look for blocks that are not in the DB
      // duing the acquire lock phase if we see that an id does not
      // exist in the db then we should put null in the cache for that id

      if (inodeId == null) {
        throw new IllegalArgumentException(Thread.currentThread().getId()+" InodeId is not set for block "+blockId);
      }
      aboutToAccessStorage(bFinder, params);
      result = dataAccess.findById(blockId, inodeId);
      gotFromDB(blockId, result);
      updateInodeBlocks(result);
      miss(bFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private BlockInfoContiguous findMaxBlk(BlockInfoContiguous.Finder bFinder,
      final Object[] params) {
    final long inodeId = (Long) params[0];
    Collection<BlockInfoContiguous> notRemovedBlks = Collections2
        .filter(filterValuesNotOnState(State.REMOVED),
            new Predicate<BlockInfoContiguous>() {
              @Override
              public boolean apply(BlockInfoContiguous input) {
                return input.getInodeId() == inodeId;
              }
            });
    if(notRemovedBlks.size()>0) {
      BlockInfoContiguous result = Collections.max(notRemovedBlks, BlockInfoContiguous.Order.ByBlockIndex);
      hit(bFinder, result, "inodeId", inodeId);
      return result;
    } else {
      miss(bFinder, (BlockInfoContiguous) null, "inodeId", inodeId);
      return null;
    }
  }


  private List<BlockInfoContiguous> syncBlockInfoInstances(List<BlockInfoContiguous> newBlocks,
      long[] blockIds) {
    List<BlockInfoContiguous> result = syncBlockInfoInstances(newBlocks);
    for (long blockId : blockIds) {
      if (!contains(blockId)) {
        gotFromDB(blockId, null);
      }
    }
    return result;
  }

  private List<BlockInfoContiguous> syncBlockInfoInstances(List<BlockInfoContiguous> newBlocks) {
    return syncBlockInfoInstances(newBlocks, false);
  }

  private List<BlockInfoContiguous> syncBlockInfoInstances(List<BlockInfoContiguous> newBlocks,
      boolean syncInodeBlocks) {
    List<BlockInfoContiguous> finalList = new ArrayList<>();

    for (BlockInfoContiguous blockInfo : newBlocks) {
      if (isRemoved(blockInfo.getBlockId())) {
        continue;
      }

      gotFromDB(blockInfo);
      finalList.add(blockInfo);

      if (syncInodeBlocks) {
        List<BlockInfoContiguous> blockList = inodeBlocks.get(blockInfo.getInodeId());
        if (blockList == null) {
          blockList = new ArrayList<>();
          inodeBlocks.put(blockInfo.getInodeId(), blockList);
        }
        blockList.add(blockInfo);
      }
    }

    return finalList;
  }

  private void updateInodeBlocks(BlockInfoContiguous newBlock) {
    if(newBlock == null)
      return;

    List<BlockInfoContiguous> blockList = inodeBlocks.get(newBlock.getInodeId());

    if (blockList != null) {
      int idx = blockList.indexOf(newBlock);
      if (idx != -1) {
        blockList.set(idx, newBlock);
      } else {
        blockList.add(newBlock);
      }
    } else {
      List<BlockInfoContiguous> list =
          new ArrayList<>(DEFAULT_NUM_BLOCKS_PER_INODE);
      list.add(newBlock);
      inodeBlocks.put(newBlock.getInodeId(), list);
    }
  }

  private void removeBlockFromInodeBlocks(BlockInfoContiguous block)
      throws TransactionContextException {
    List<BlockInfoContiguous> blockList = inodeBlocks.get(block.getInodeId());
    if (blockList != null) {
      if (!blockList.remove(block)) {
        throw new TransactionContextException(
            "Trying to remove a block that does not exist");
      }
    }
  }

  private void checkForSnapshotChange() {
    // when you overwrite a file the dst file blocks are removed
    // removedBlocks list may not be empty
    if (!getAdded().isEmpty() || !getModified().isEmpty()) {//incase of move and
      // rename the
      // blocks should not have been modified in any way
      throw new IllegalStateException(
          "Renaming a file(s) whose blocks are changed. During rename and move no block blocks should have been changed.");
    }
  }

  private void deleteBlocksForConcat(INodeCandidatePrimaryKey trg_param,
      List<INodeCandidatePrimaryKey> deleteINodes, List<BlockInfoContiguous> oldBlks /* blks with old pk*/)
      throws TransactionContextException {

    if (!getRemoved()
        .isEmpty()) {//in case of concat new block_infos rows are added by
      // the concat fn
      throw new IllegalStateException(
          "Concat file(s) whose blocks are changed. During rename and move no block blocks should have been changed.");
    }

    for (BlockInfoContiguous bInfo : oldBlks) {
      INodeCandidatePrimaryKey pk =
          new INodeCandidatePrimaryKey(bInfo.getInodeId());
      if (deleteINodes.contains(pk)) {
        //remove the block
        concatRemovedBlks.add(bInfo);
        if(isLogTraceEnabled()) {
          log("snapshot-maintenance-removed-blockinfo", "bid", bInfo.getBlockId(),
                  "inodeId", bInfo.getInodeId());
        }
      }
    }
  }

}
