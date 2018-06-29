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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaUnderConstructionContext
    extends BaseReplicaContext<BlockPK.ReplicaPK, ReplicaUnderConstruction> {

  ReplicaUnderConstructionDataAccess dataAccess;

  public ReplicaUnderConstructionContext(
      ReplicaUnderConstructionDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.update(replica);
    if(isLogDebugEnabled()) {
      log("added-replicauc", "bid", replica.getBlockId(), "sid",
              replica.getStorageId(), "state", replica.getState().name());
    }
  }

  @Override
  public void remove(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.remove(replica);
    if(isLogDebugEnabled()) {
      log("removed-replicauc", "bid", replica.getBlockId(), "sid",
              replica.getStorageId(), "state", replica.getState().name());
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<ReplicaUnderConstruction> findList(
      FinderType<ReplicaUnderConstruction> finder, Object... params)
      throws TransactionContextException, StorageException {
    ReplicaUnderConstruction.Finder rFinder =
        (ReplicaUnderConstruction.Finder) finder;
    switch (rFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(rFinder, params);
      case ByINodeId:
        return findByINodeId(rFinder, params);
      case ByINodeIds:
        return findByINodeIds(rFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(ReplicaUnderConstruction replica) {
    return new BlockPK.ReplicaPK(replica.getBlockId(), replica.getInodeId(),
        replica.getStorageId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction) {
    return cloneEntity(replicaUnderConstruction,
        replicaUnderConstruction.getInodeId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction, int inodeId) {
    return new ReplicaUnderConstruction(
        replicaUnderConstruction.getState(),
        replicaUnderConstruction.getStorageId(),
        replicaUnderConstruction.getBlockId(), inodeId,
        replicaUnderConstruction.getGenerationStamp());
  }

  private List<ReplicaUnderConstruction> findByBlockId(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<ReplicaUnderConstruction> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(rFinder, result, "bid", blockId, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(rFinder, params);
      result =
          dataAccess.findReplicaUnderConstructionByBlockId(blockId, inodeId);
      gotFromDB(new BlockPK(blockId, inodeId), result);
      miss(rFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeId(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final int inodeId = (Integer) params[0];
    List<ReplicaUnderConstruction> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(rFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(rFinder, params);
      result = dataAccess.findReplicaUnderConstructionByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(rFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeIds(
      ReplicaUnderConstruction.Finder rFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    aboutToAccessStorage(rFinder, params);
    List<ReplicaUnderConstruction> result =
        dataAccess.findReplicaUnderConstructionByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(rFinder, result, "inodeids", Arrays.toString(inodeIds));
    return result;
  }

}
