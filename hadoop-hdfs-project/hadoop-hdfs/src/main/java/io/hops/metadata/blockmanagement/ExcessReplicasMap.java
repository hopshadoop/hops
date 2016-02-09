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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

public class ExcessReplicasMap {

  private final DatanodeManager datanodeManager;

  public ExcessReplicasMap(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  //[M] only needed in TestOverReplicatedBlocks
  public LightWeightLinkedSet<Block> get(String dnUuid) throws IOException {
    Collection<ExcessReplica> excessReplicas =
        getExcessReplicas(dnUuid);
    if (excessReplicas == null) {
      return null;
    }
    LightWeightLinkedSet<Block> excessBlocks =
        new LightWeightLinkedSet<Block>();
    for (ExcessReplica er : excessReplicas) {
      //FIXME: [M] might need to get the blockinfo from the db, but for now we don't need it
      excessBlocks.add(new Block(er.getBlockId()));
    }
    return excessBlocks;
  }

  public boolean put(String dnUuid, BlockInfo excessBlk)
      throws StorageException, TransactionContextException {
    ExcessReplica er = getExcessReplica(dnUuid, excessBlk);
    if (er == null) {
      addExcessReplicaToDB(new ExcessReplica(dnUuid, excessBlk.getBlockId(), excessBlk.getInodeId()));
      return true;
    }
    return false;
  }

  /**
   * Mark a block on a datanode for removal
   */
  public boolean remove(String dnUuid, BlockInfo block)
      throws StorageException, TransactionContextException {
    ExcessReplica er =
        getExcessReplica(dnUuid, block);
    if (er != null) {
      removeExcessReplicaFromDB(er);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get the datanode Uuids of all datanodes storing excess replicas of this
   * block.
   */
  public Collection<String> get(BlockInfo blk)
      throws StorageException, TransactionContextException {
    Collection<ExcessReplica> excessReplicas = getExcessReplicas(blk);
    if (excessReplicas == null) {
      return null;
    }
    TreeSet<String> stIds = new TreeSet<String>();
    for (ExcessReplica er : excessReplicas) {
      stIds.add(er.getDatanodeUuid());
    }
    return stIds;
  }

  public boolean contains(DatanodeInfo dn, BlockInfo blk)
      throws StorageException, TransactionContextException {
    return contains(dn.getDatanodeUuid(), blk);
  }

  public boolean contains(String dnUuid, BlockInfo blk)
      throws StorageException, TransactionContextException {
    Collection<ExcessReplica> ers = getExcessReplicas(blk);
    if (ers == null) {
      return false;
    }
    return ers.contains(
        new ExcessReplica(dnUuid, blk.getBlockId(), blk.getInodeId()));
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

  private Collection<ExcessReplica> getExcessReplicas(final String uuid)
      throws IOException {
    return (Collection<ExcessReplica>) new LightWeightRequestHandler(
        HDFSOperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ExcessReplicaDataAccess da =
            (ExcessReplicaDataAccess) HdfsStorageFactory
                .getDataAccess(ExcessReplicaDataAccess.class);
        return da.findExcessReplicaByDatanodeUuid(uuid);
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

  private Collection<ExcessReplica> getExcessReplicas(BlockInfo blk)
      throws StorageException, TransactionContextException {
    return EntityManager
        .findList(ExcessReplica.Finder.ByBlockIdAndINodeId, blk.getBlockId(),
            blk.getInodeId());
  }

  private ExcessReplica getExcessReplica(String uuid, BlockInfo block)
      throws StorageException, TransactionContextException {
    return EntityManager.find(ExcessReplica.Finder.ByBlockIdDatanodeUuidAndINodeId,
        block.getBlockId(), uuid, block.getInodeId());
  }
}


///*
// * Copyright (C) 2015 hops.io.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.hops.metadata.blockmanagement;
//
//import io.hops.exception.StorageException;
//import io.hops.exception.TransactionContextException;
//import io.hops.metadata.HdfsStorageFactory;
//import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
//import io.hops.metadata.hdfs.entity.ExcessReplica;
//import io.hops.transaction.EntityManager;
//import io.hops.transaction.handler.HDFSOperationType;
//import io.hops.transaction.handler.LightWeightRequestHandler;
//import org.apache.hadoop.hdfs.protocol.Block;
//import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
//import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
//import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
//import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.TreeSet;
//
//public class ExcessReplicasMap {
//
//  private final DatanodeManager datanodeManager;
//
//  public ExcessReplicasMap(DatanodeManager datanodeManager) {
//    this.datanodeManager = datanodeManager;
//  }
//
//  //[M] only needed in TestOverReplicatedBlocks
//  public LightWeightLinkedSet<Block> get(DatanodeStorageInfo storage) throws IOException {
//    Collection<ExcessReplica> excessReplicas =
//        getExcessReplicas(storage);
//    if (excessReplicas == null) {
//      return null;
//    }
//    LightWeightLinkedSet<Block> excessBlocks =
//        new LightWeightLinkedSet<Block>();
//    for (ExcessReplica er : excessReplicas) {
//      //FIXME: [M] might need to get the blockinfo from the db, but for now we don't need it
//      excessBlocks.add(new Block(er.getBlockId()));
//    }
//    return excessBlocks;
//  }
//
//  public boolean put(DatanodeStorageInfo storage, BlockInfo excessBlk)
//      throws StorageException, TransactionContextException {
//    ExcessReplica er =
//        getExcessReplica(storage, excessBlk);
//    if (er == null) {
//      addExcessReplicaToDB(
//          new ExcessReplica(storage.getSid(), excessBlk.getBlockId(), excessBlk.getInodeId()));
//      return true;
//    }
//    return false;
//  }
//
//  public boolean remove(DatanodeStorageInfo storage, BlockInfo block)
//      throws StorageException, TransactionContextException {
//    ExcessReplica er =
//        getExcessReplica(storage, block);
//    if (er != null) {
//      removeExcessReplicaFromDB(er);
//      return true;
//    } else {
//      return false;
//    }
//  }
//
//  public Collection<String> get(BlockInfo blk)
//      throws StorageException, TransactionContextException {
//    Collection<ExcessReplica> excessReplicas = getExcessReplicas(blk);
//    if (excessReplicas == null) {
//      return null;
//    }
//    TreeSet<String> stIds = new TreeSet<String>();
//    for (ExcessReplica er : excessReplicas) {
//      stIds.add(datanodeManager.getStorage(er.getStorageId())
//          .getDatanodeDescriptor().getDatanodeUuid());
//    }
//    return stIds;
//  }
//
//  public boolean contains(DatanodeStorageInfo storage, BlockInfo blk)
//      throws StorageException, TransactionContextException {
//    Collection<ExcessReplica> ers = getExcessReplicas(blk);
//    if (ers == null) {
//      return false;
//    }
//    return ers.contains(
//        new ExcessReplica(storage.getSid(),
//            blk.getBlockId(), blk.getInodeId()));
//  }
//
//  public void clear() throws IOException {
//    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_EXCESS_BLKS) {
//      @Override
//      public Object performTask() throws StorageException, IOException {
//        ExcessReplicaDataAccess da =
//            (ExcessReplicaDataAccess) HdfsStorageFactory
//                .getDataAccess(ExcessReplicaDataAccess.class);
//        da.removeAll();
//        return null;
//      }
//    }.handle();
//  }
//
//  private Collection<ExcessReplica> getExcessReplicas(final
//      DatanodeStorageInfo storage)
//      throws IOException {
//    final int storageId = storage.getSid();
//    return (Collection<ExcessReplica>) new LightWeightRequestHandler(
//        HDFSOperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
//      @Override
//      public Object performTask() throws StorageException, IOException {
//        ExcessReplicaDataAccess da =
//            (ExcessReplicaDataAccess) HdfsStorageFactory
//                .getDataAccess(ExcessReplicaDataAccess.class);
//        return da.findExcessReplicaByStorageId(storageId);
//      }
//    }.handle();
//  }
//
//  private void addExcessReplicaToDB(ExcessReplica er)
//      throws StorageException, TransactionContextException {
//    EntityManager.add(er);
//  }
//
//  private void removeExcessReplicaFromDB(ExcessReplica er)
//      throws StorageException, TransactionContextException {
//    EntityManager.remove(er);
//  }
//
//  private Collection<ExcessReplica> getExcessReplicas(BlockInfo blk)
//      throws StorageException, TransactionContextException {
//    return EntityManager
//        .findList(ExcessReplica.Finder.ByBlockIdAndINodeId, blk.getBlockId(),
//            blk.getInodeId());
//  }
//
//  private ExcessReplica getExcessReplica(DatanodeStorageInfo storage, BlockInfo
//      block) throws StorageException, TransactionContextException {
//    return EntityManager.find(ExcessReplica.Finder.ByBlockIdStorageIdAndINodeId,
//        block.getBlockId(), storage.getSid(), block.getInodeId());
//  }
//}
