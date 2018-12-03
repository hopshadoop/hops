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
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, Replica> {

  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(Replica replica)
      throws TransactionContextException {
    super.update(replica);
    if(isLogTraceEnabled()) {
      log("updated-replica", "bid", replica.getBlockId(), "sid",
              replica.getStorageId());
    }
  }

  @Override
  public void remove(Replica replica)
      throws TransactionContextException {
    super.remove(replica);
    if(isLogTraceEnabled()) {
      log("removed-replica", "bid", replica.getBlockId(), "sid",
              replica.getStorageId());
    }
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<Replica> findList(FinderType<Replica> finder,
      Object... params) throws TransactionContextException, StorageException {
    Replica.Finder iFinder = (Replica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case ByINodeIds:
        return findyByINodeIds(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Replica find(FinderType<Replica> finder,
      Object... params) throws TransactionContextException, StorageException {
    Replica.Finder iFinder = (Replica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndStorageId:
        return findByPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(Replica hopReplica) {
    return new BlockPK.ReplicaPK(hopReplica.getBlockId(),
        hopReplica.getInodeId(), hopReplica.getStorageId());
  }

  @Override
  Replica cloneEntity(Replica hopReplica) {
    return cloneEntity(hopReplica, hopReplica.getInodeId());
  }

  @Override
  Replica cloneEntity(Replica hopReplica, long inodeId) {
    return new Replica(hopReplica.getStorageId(), hopReplica.getBlockId(),
         inodeId, hopReplica.getBucketId());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty();
  }

  private Replica findByPK(Replica.Finder iFinder,
      Object[] params) {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    Replica result = null;
    List<Replica> replicas = getByBlock(blockId);
    if (replicas != null) {
      for (Replica replica : replicas) {
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

  private List<Replica> findByBlockId(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (Long) params[1];
    List<Replica> results = null;
    if (containsByBlock(blockId) || (containsByINode(inodeId) && storageCallPrevented)) {
      results = getByBlock(blockId);
      hit(iFinder, results, "bid", blockId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasById(blockId, inodeId);
      gotFromDB(new BlockPK(blockId, null), results);
      miss(iFinder, results, "bid", blockId);
    }
    return results;
  }

  private List<Replica> findByINodeId(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];
    List<Replica> results = null;
    if (containsByINode(inodeId)) {
      results = getByINode(inodeId);
      hit(iFinder, results, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findReplicasByINodeId(inodeId);
      gotFromDB(new BlockPK(null, inodeId), results);
      miss(iFinder, results, "inodeid", inodeId);
    }
    return results;
  }

  private List<Replica> findyByINodeIds(Replica.Finder iFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    long[] ids = (long[]) params[0];
    aboutToAccessStorage(iFinder, params);
    List<Replica> results = dataAccess.findReplicasByINodeIds(ids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(ids), results);
    miss(iFinder, results, "inodeIds", Arrays.toString(ids));
    return results;
  }
}
