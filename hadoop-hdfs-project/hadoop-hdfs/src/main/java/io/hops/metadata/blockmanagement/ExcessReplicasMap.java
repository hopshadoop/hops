/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.hdfs.entity.ExcessReplica;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

public class ExcessReplicasMap {

  private final DatanodeManager datanodeManager;

  public ExcessReplicasMap(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  public LightWeightLinkedSet<Block> get(String datanodeUuid,
      DatanodeManager manager) throws IOException {
    LightWeightLinkedSet<Block> excessReplicas = new LightWeightLinkedSet<Block>();

    List<Integer> sidsOnDatanode = manager.getSidsOnDatanode(datanodeUuid);
    for(int sid : sidsOnDatanode) {
      excessReplicas.addAll(get(sid));
    }

    return excessReplicas;
  }

  //[M] only needed in TestOverReplicatedBlocks
  public LightWeightLinkedSet<Block> get(int sid) throws IOException {
    Collection<ExcessReplica> excessReplicas = getExcessReplicas(sid);
    if (excessReplicas == null) {
      return null;
    }
    LightWeightLinkedSet<Block> excessBlocks =
        new LightWeightLinkedSet<>();
    for (ExcessReplica er : excessReplicas) {
      //FIXME: [M] might need to get the blockinfo from the db, but for now we don't need it
      excessBlocks.add(new Block(er.getBlockId()));
    }
    return excessBlocks;
  }

  public boolean put(int storageId, BlockInfoContiguous excessBlk)
      throws StorageException, TransactionContextException {
    ExcessReplica er = getExcessReplica(storageId, excessBlk);
    if (er == null) {
      addExcessReplicaToDB(new ExcessReplica(storageId, excessBlk.getBlockId(), excessBlk.getInodeId()));
      return true;
    }
    return false;
  }

  /**
   * Mark a block on a datanode for removal
   */
  public boolean remove(DatanodeDescriptor dn, BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    boolean found = false;

    for(DatanodeStorageInfo storage : dn.getStorageInfos()) {
      ExcessReplica er = getExcessReplica(storage.getSid(), block);
      if (er != null) {
        removeExcessReplicaFromDB(er);
        found = true;
      }
    }

    return found;
  }

  /**
   * Get the datanodeUuids of all datanodes storing excess replicas of this
   * block.
   */
  public Collection<String> get(BlockInfoContiguous blk) throws StorageException, TransactionContextException {
    Collection<ExcessReplica> excessReplicas = getExcessReplicas(blk);
    if (excessReplicas == null) {
      return null;
    }
    TreeSet<String> stIds = new TreeSet<>();
    for (ExcessReplica er : excessReplicas) {
      stIds.add(datanodeManager.getDatanodeBySid(er.getStorageId()).getDatanodeUuid());
    }
    return stIds;
  }

  public boolean contains(DatanodeStorageInfo storageInfo, BlockInfoContiguous blk)
      throws IOException {
    return contains(storageInfo.getSid(), blk);
  }

  public boolean contains(final int sid, final BlockInfoContiguous blk)
      throws IOException {
    return new LightWeightRequestHandler(
        HDFSOperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ExcessReplicaDataAccess da = (ExcessReplicaDataAccess)
            HdfsStorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        return da.findByPK(blk.getBlockId(), sid, blk.getInodeId());
      }
    }.handle() != null;
  }

  public void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_EXCESS_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ExcessReplicaDataAccess da =
            (ExcessReplicaDataAccess) HdfsStorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  private Collection<ExcessReplica> getExcessReplicas(final int sid)
      throws IOException {
    return (Collection<ExcessReplica>) new LightWeightRequestHandler(
        HDFSOperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ExcessReplicaDataAccess da =
            (ExcessReplicaDataAccess) HdfsStorageFactory
                .getDataAccess(ExcessReplicaDataAccess.class);
        return da.findExcessReplicaBySid(sid);
      }
    }.handle();
  }

  private void addExcessReplicaToDB(ExcessReplica er)
      throws StorageException, TransactionContextException {
    EntityManager.add(er);
  }

  private void removeExcessReplicaFromDB(ExcessReplica er)
      throws StorageException, TransactionContextException {
    EntityManager.remove(er);
  }

  private Collection<ExcessReplica> getExcessReplicas(BlockInfoContiguous blk)
      throws StorageException, TransactionContextException {
    return EntityManager
        .findList(ExcessReplica.Finder.ByBlockIdAndINodeId, blk.getBlockId(),
            blk.getInodeId());
  }

  private ExcessReplica getExcessReplica(int sid, BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    return EntityManager.find(ExcessReplica.Finder.ByBlockIdSidAndINodeId,
        block.getBlockId(), sid, block.getInodeId());
  }
  
  public int size() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_CORRUPT_REPLICAS) {
      @Override
      public Object performTask() throws IOException {
        ExcessReplicaDataAccess da =
            (ExcessReplicaDataAccess) HdfsStorageFactory
                .getDataAccess(ExcessReplicaDataAccess.class);
        return da.countAllUniqueBlk();
      }
    }.handle();
  }
}
