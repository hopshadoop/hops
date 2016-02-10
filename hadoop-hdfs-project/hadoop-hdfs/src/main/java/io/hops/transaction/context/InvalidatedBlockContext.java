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
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InvalidatedBlockContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, InvalidatedBlock> {

  private final InvalidateBlockDataAccess<InvalidatedBlock> dataAccess;
  private boolean allInvBlocksRead = false;

  public InvalidatedBlockContext(InvalidateBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(InvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.update(hopInvalidatedBlock);
    log("added-invblock", "bid", hopInvalidatedBlock.getBlockId(), "sid",
        hopInvalidatedBlock.getDatanodeUuid());
  }

  @Override
  public void remove(InvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.remove(hopInvalidatedBlock);
    log("removed-invblock", "bid", hopInvalidatedBlock.getBlockId(), "sid",
        hopInvalidatedBlock.getDatanodeUuid());
  }

  @Override
  public InvalidatedBlock find(FinderType<InvalidatedBlock> finder,
      Object... params) throws TransactionContextException, StorageException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdDatanodeUuidAndINodeId:
        return findByPrimaryKey(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<InvalidatedBlock> findList(
      FinderType<InvalidatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case All:
        return findAll(iFinder);
      case ByDatanodeUuid:
        return findByDatanodeUuid(iFinder, params);
      case ByINodeIds:
        return findByINodeIds(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    allInvBlocksRead = false;
  }

  @Override
  InvalidatedBlock cloneEntity(InvalidatedBlock hopInvalidatedBlock) {
    return cloneEntity(hopInvalidatedBlock, hopInvalidatedBlock.getInodeId());
  }

  @Override
  InvalidatedBlock cloneEntity(InvalidatedBlock hopInvalidatedBlock,
      int inodeId) {
    return new InvalidatedBlock(hopInvalidatedBlock.getDatanodeUuid(),
        hopInvalidatedBlock.getStorageId(), hopInvalidatedBlock.getBlockId(),
        inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(InvalidatedBlock hopInvalidatedBlock) {
    return new BlockPK.ReplicaPK(hopInvalidatedBlock.getBlockId(),
        hopInvalidatedBlock.getInodeId(), hopInvalidatedBlock.getStorageId());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getRemoved().isEmpty();
  }

  private InvalidatedBlock findByPrimaryKey(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final String datanodeUuid = (String) params[1];
    final int inodeId = (Integer) params[2];

    final BlockPK.ReplicaPK.RBPK key = new BlockPK.ReplicaPK.RBPK(blockId, inodeId, datanodeUuid);
    InvalidatedBlock result = null;
    if (contains(key) || containsByBlock(blockId) || containsByINode(inodeId)) {
      result = get(key);
      hit(iFinder, result, "bid", blockId, "uuid", datanodeUuid, "inodeId",
          inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvBlockByPkey(blockId, datanodeUuid, inodeId);
      gotFromDB(key, result);
      miss(iFinder, result, "bid", blockId, "uuid", datanodeUuid, "inodeId",
          inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findByBlockId(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];

    List<InvalidatedBlock> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(iFinder, result, "bid", blockId, "inodeId", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvalidatedBlocksByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId), result);
      miss(iFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findByINodeId(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];

    List<InvalidatedBlock> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(iFinder, result, "inodeId", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.findInvalidatedBlocksByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(iFinder, result, "inodeId", inodeId);
    }
    return result;
  }

  private List<InvalidatedBlock> findAll(InvalidatedBlock.Finder iFinder)
      throws StorageCallPreventedException, StorageException {
    List<InvalidatedBlock> result = null;
    if (allInvBlocksRead) {
      result = new ArrayList<InvalidatedBlock>(getAll());
      hit(iFinder, result);
    } else {
      aboutToAccessStorage(iFinder);
      result = dataAccess.findAllInvalidatedBlocks();
      gotFromDB(result);
      allInvBlocksRead = true;
      miss(iFinder, result);
    }
    return result;
  }

  private List<InvalidatedBlock> findByDatanodeUuid(
      InvalidatedBlock.Finder iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long[] blockIds = (long[]) params[0];
    final int[] inodeIds = (int[]) params[1];
    final String datanodeUuid = (String) params[2];

    aboutToAccessStorage(iFinder, params);
    List<InvalidatedBlock> result = dataAccess.findInvalidatedBlockByDatanodeUuid(datanodeUuid);

    gotFromDB(BlockPK.ReplicaPK.RBPK.getKeys(blockIds, inodeIds, datanodeUuid),
        result);

    miss(iFinder, result, "bids", Arrays.toString(blockIds), "inodeIds", Arrays.toString(inodeIds), "uuid", datanodeUuid);
    return result;
  }

  private List<InvalidatedBlock> findByINodeIds(InvalidatedBlock.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];

    aboutToAccessStorage(iFinder, params);
    List<InvalidatedBlock> result =
        dataAccess.findInvalidatedBlocksByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(iFinder, result, "inodeIds", Arrays.toString(inodeIds));
    return result;
  }
}
