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
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.entity.CorruptReplica;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CorruptReplicaContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, CorruptReplica> {

  CorruptReplicaDataAccess dataAccess;

  public CorruptReplicaContext(CorruptReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(CorruptReplica hopCorruptReplica)
      throws TransactionContextException {
    super.update(hopCorruptReplica);
    if(isLogDebugEnabled()) {
      log("added-corrupt", "bid", hopCorruptReplica.getBlockId(), "sid",
              hopCorruptReplica.getStorageId());
    }
  }

  @Override
  public void remove(CorruptReplica hopCorruptReplica)
      throws TransactionContextException {
    super.remove(hopCorruptReplica);
    if(isLogDebugEnabled()) {
      log("removed-corrupt", "bid", hopCorruptReplica.getBlockId(), "sid",
              hopCorruptReplica.getStorageId());
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded());
  }

  @Override
  public Collection<CorruptReplica> findList(FinderType<CorruptReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;
    switch (cFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(cFinder, params);
      case ByINodeId:
        return findByINodeId(cFinder, params);
      case ByINodeIds:
        return findByINodeIds(cFinder, params);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  CorruptReplica cloneEntity(CorruptReplica hopCorruptReplica) {
    return cloneEntity(hopCorruptReplica, hopCorruptReplica.getInodeId());
  }

  @Override
  CorruptReplica cloneEntity(CorruptReplica hopCorruptReplica, int inodeId) {
    return new CorruptReplica(hopCorruptReplica.getStorageId(),
        hopCorruptReplica.getBlockId(), inodeId, hopCorruptReplica.getReason());
  }

  @Override
  BlockPK.ReplicaPK getKey(CorruptReplica hopCorruptReplica) {
    return new BlockPK.ReplicaPK(hopCorruptReplica.getBlockId(),
        hopCorruptReplica.getInodeId(), hopCorruptReplica.getStorageId());
  }

  private List<CorruptReplica> findByBlockId(CorruptReplica.Finder cFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<CorruptReplica> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(cFinder, result, "bid", blockId, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(cFinder, params);
      result = dataAccess.findByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId), result);
      miss(cFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<CorruptReplica> findByINodeId(CorruptReplica.Finder cFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<CorruptReplica> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(cFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(cFinder, params);
      result = dataAccess.findByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(cFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<CorruptReplica> findByINodeIds(CorruptReplica.Finder cFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    aboutToAccessStorage(cFinder, params);
    List<CorruptReplica> result = dataAccess.findByINodeIds(inodeIds);
    miss(cFinder, result, "inodeids", Arrays.toString(inodeIds));
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    return result;
  }

}
