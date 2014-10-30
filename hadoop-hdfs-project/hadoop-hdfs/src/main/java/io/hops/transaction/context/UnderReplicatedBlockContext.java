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

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.entity.UnderReplicatedBlock;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class UnderReplicatedBlockContext
    extends BaseReplicaContext<BlockPK, UnderReplicatedBlock> {

  private final UnderReplicatedBlockDataAccess<UnderReplicatedBlock> dataAccess;

  public UnderReplicatedBlockContext(
      UnderReplicatedBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(UnderReplicatedBlock hopUnderReplicatedBlock)
      throws TransactionContextException {
    super.update(hopUnderReplicatedBlock);
    if(isLogTraceEnabled()) {
      log("added-urblock", "bid", hopUnderReplicatedBlock.getBlockId(), "level",
              hopUnderReplicatedBlock.getLevel(), "inodeId",
              hopUnderReplicatedBlock.getInodeId());
    }
  }

  @Override
  public void remove(UnderReplicatedBlock hopUnderReplicatedBlock)
      throws TransactionContextException {
    super.remove(hopUnderReplicatedBlock);
    if(isLogTraceEnabled()) {
      log("removed-urblock", "bid", hopUnderReplicatedBlock.getBlockId(), "level",
              hopUnderReplicatedBlock.getLevel());
    }
  }

  @Override
  public UnderReplicatedBlock find(FinderType<UnderReplicatedBlock> finder,
      Object... params) throws TransactionContextException, StorageException {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(urFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<UnderReplicatedBlock> findList(
      FinderType<UnderReplicatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByINodeId:
        return findByINodeId(urFinder, params);
      case ByINodeIds:
        return findByINodeIds(urFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  UnderReplicatedBlock cloneEntity(
      UnderReplicatedBlock hopUnderReplicatedBlock) {
    return cloneEntity(hopUnderReplicatedBlock,
        hopUnderReplicatedBlock.getInodeId());
  }

  @Override
  UnderReplicatedBlock cloneEntity(UnderReplicatedBlock hopUnderReplicatedBlock,
      long inodeId) {
    return new UnderReplicatedBlock(hopUnderReplicatedBlock.getLevel(),
        hopUnderReplicatedBlock.getBlockId(), inodeId, hopUnderReplicatedBlock.getExpectedReplicas());
  }

  @Override
  BlockPK getKey(UnderReplicatedBlock hopUnderReplicatedBlock) {
    return new BlockPK(hopUnderReplicatedBlock.getBlockId(),
        hopUnderReplicatedBlock.getInodeId());
  }

  private UnderReplicatedBlock findByBlockId(
      UnderReplicatedBlock.Finder urFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];
    UnderReplicatedBlock result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      List<UnderReplicatedBlock> urblks = getByBlock(blockId);
      if (urblks != null) {
        if (urblks.size() > 1) {
          throw new IllegalStateException(
              "you should have only one " + "UnderReplicatedBlock per block");
        }
        if (!urblks.isEmpty()) {
          result = urblks.get(0);
        }
      }
      hit(urFinder, result, "bid", blockId, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(urFinder, params);
      result = dataAccess.findByPk(blockId, inodeId);
      gotFromDB(new BlockPK(blockId, inodeId), result);
      miss(urFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<UnderReplicatedBlock> findByINodeId(
      UnderReplicatedBlock.Finder urFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];
    List<UnderReplicatedBlock> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(urFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(urFinder, params);
      result = dataAccess.findByINodeId(inodeId);
      gotFromDB(new BlockPK(null, inodeId), result);
      miss(urFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<UnderReplicatedBlock> findByINodeIds(
      UnderReplicatedBlock.Finder urFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long[] inodeIds = (long[]) params[0];
    List<UnderReplicatedBlock> result = null;
    aboutToAccessStorage(urFinder, params);
    result = dataAccess.findByINodeIds(inodeIds);
    gotFromDB(BlockPK.getBlockKeys(inodeIds), result);
    miss(urFinder, result, "inodeids", Arrays.toString(inodeIds));
    return result;
  }

}
