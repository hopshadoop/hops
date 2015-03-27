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
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.IndexedReplica;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, IndexedReplica> {

  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(IndexedReplica replica)
      throws TransactionContextException {
    super.update(replica);
    log("updated-replica", "bid", replica.getBlockId(), "sid",
        replica.getStorageId(), "index", replica.getIndex());
  }

  @Override
  public void remove(IndexedReplica replica)
      throws TransactionContextException {
    super.remove(replica);
    log("removed-replica", "bid", replica.getBlockId(), "sid",
        replica.getStorageId(), "index", replica.getIndex());
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<IndexedReplica> findList(FinderType<IndexedReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    IndexedReplica.Finder iFinder = (IndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case ByBlockIdsStorageIdsAndINodeIds:
        return findByPrimaryKeys(iFinder, params);
      case ByINodeIds:
        return findyByINodeIds(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public IndexedReplica find(FinderType<IndexedReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    IndexedReplica.Finder iFinder = (IndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndStorageId:
        return findByPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(IndexedReplica hopIndexedReplica) {
    return new BlockPK.ReplicaPK(hopIndexedReplica.getBlockId(),
        hopIndexedReplica.getInodeId(), hopIndexedReplica.getStorageId());
  }

  @Override
  IndexedReplica cloneEntity(IndexedReplica hopIndexedReplica) {
    return cloneEntity(hopIndexedReplica, hopIndexedReplica.getInodeId());
  }

  @Override
  IndexedReplica cloneEntity(IndexedReplica hopIndexedReplica, int inodeId) {
    return new IndexedReplica(hopIndexedReplica.getBlockId(),
        hopIndexedReplica.getStorageId(), inodeId,
        hopIndexedReplica.getIndex());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty();
  }

  private IndexedReplica findByPK(IndexedReplica.Finder iFinder,
      Object[] params) {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    IndexedReplica result = null;
    List<IndexedReplica> replicas = getByBlock(blockId);
    if (replicas != null) {
      for (IndexedReplica replica : replicas) {
        if (replica != null) {
          if (replica.getStorageId() == storageId) {
            result = replica;
            break;
          }
        }
      }
    }
    hit(iFinder, result, "bid", blockId, "sid", storageId);
    return result;
  }

  private List<IndexedReplica> findByBlockId(IndexedReplica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<IndexedReplica> results = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      results = getByBlock(blockId);
      hit(iFinder, results, "bid", blockId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasById(blockId, inodeId);
      gotFromDB(new BlockPK(blockId), results);
      miss(iFinder, results, "bid", blockId);
    }
    return results;
  }

  private List<IndexedReplica> findByINodeId(IndexedReplica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<IndexedReplica> results = null;
    if (containsByINode(inodeId)) {
      results = getByINode(inodeId);
      hit(iFinder, results, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), results);
      miss(iFinder, results, "inodeid", inodeId);
    }
    return results;
  }

  private List<IndexedReplica> findByPrimaryKeys(IndexedReplica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    long[] blockIds = (long[]) params[0];
    int[] inodeIds = (int[]) params[1];
    int sid = (Integer) params[2];
    int[] sids = new int[blockIds.length];
    Arrays.fill(sids, sid);
    List<IndexedReplica> results =
        dataAccess.findReplicasByPKS(blockIds, inodeIds, sids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, sid), results);
    miss(iFinder, results, "blockIds", Arrays.toString(blockIds), "inodeIds",
        Arrays.toString(inodeIds), "sid", sid);
    return results;
  }

  private List<IndexedReplica> findyByINodeIds(IndexedReplica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    int[] ids = (int[]) params[0];
    aboutToAccessStorage(iFinder, params);
    List<IndexedReplica> results = dataAccess.findReplicasByINodeIds(ids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(ids), results);
    miss(iFinder, results, "inodeIds", Arrays.toString(ids));
    return results;
  }
}
