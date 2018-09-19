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
package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.entity.BlockInfo;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.zookeeper.KeeperException;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class BlockInfoDALAdaptor extends
    DalAdaptor<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo, BlockInfo>
    implements
    BlockInfoDataAccess<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> {

  private final BlockInfoDataAccess<BlockInfo> dataAccess;

  public BlockInfoDALAdaptor(BlockInfoDataAccess<BlockInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();
  }

  @Override
  public int countAllCompleteBlocks() throws StorageException {
    return dataAccess.countAllCompleteBlocks();
  }
  
  @Override
  public org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo findById(
      long blockId, long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findById(blockId, inodeId));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findByInodeId(
      long id) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findByInodeId(id));
  }

  
  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findByInodeIds(
      long[] inodeIds) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findByInodeIds(inodeIds));
  }
  
  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findAllBlocks()
      throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findAllBlocks());
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findBlockInfosByStorageId(
      int storageId) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findBlockInfosByStorageId(storageId));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findBlockInfosByStorageId(int storageId,
      long from, int size) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(dataAccess.
        findBlockInfosByStorageId(storageId, from, size));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findBlockInfosBySids(
      List<Integer> sids) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findBlockInfosBySids(sids));
  }

  @Override
  public Set<Long> findINodeIdsByStorageId(int storageId)
      throws StorageException {
    return dataAccess.findINodeIdsByStorageId(storageId);
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> findByIds(
      long[] blockIds, long[] inodeIds) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo>) convertDALtoHDFS(
        dataAccess.findByIds(blockIds, inodeIds));
  }

  @Override
  public boolean existsOnAnyStorage(long inodeId, long blockId, List<Integer> sids) throws
      StorageException {
    return dataAccess.existsOnAnyStorage(inodeId, blockId, sids);
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> removed,
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> newed,
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo> modified)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed),
        convertHDFStoDAL(modified));
  }

  @Override
  public BlockInfo convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo hdfsClass)
      throws StorageException {
    if (hdfsClass != null) {
      BlockInfo hopBlkInfo =
          new BlockInfo(hdfsClass.getBlockId(), hdfsClass.getBlockIndex(),
              hdfsClass.getInodeId(), hdfsClass.getNumBytes(),
              hdfsClass.getGenerationStamp(),
              hdfsClass.getBlockUCState().ordinal(), hdfsClass.getTimestamp());
      if (hdfsClass instanceof BlockInfoUnderConstruction) {
        BlockInfoUnderConstruction ucBlock =
            (BlockInfoUnderConstruction) hdfsClass;
        hopBlkInfo.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
        hopBlkInfo.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
      }
      return hopBlkInfo;
    } else {
      return null;
    }
  }

  @Override
  public org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo convertDALtoHDFS(
      BlockInfo dalClass) throws StorageException {
    if (dalClass != null) {
      Block b = new Block(dalClass.getBlockId(), dalClass.getNumBytes(),
          dalClass.getGenerationStamp());
      org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo blockInfo = null;

      if (dalClass.getBlockUCState() >
          0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
        blockInfo = new BlockInfoUnderConstruction(b, dalClass.getInodeId());
        ((BlockInfoUnderConstruction) blockInfo).setBlockUCStateNoPersistance(
            HdfsServerConstants.BlockUCState.values()[dalClass
                .getBlockUCState()]);
        ((BlockInfoUnderConstruction) blockInfo)
            .setPrimaryNodeIndexNoPersistance(dalClass.getPrimaryNodeIndex());
        ((BlockInfoUnderConstruction) blockInfo)
            .setBlockRecoveryIdNoPersistance(dalClass.getBlockRecoveryId());
      } else if (dalClass.getBlockUCState() ==
          HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
        blockInfo =
            new org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo(b,
                dalClass.getInodeId());
      } else {
        // Unexpected UC Block state
        return null;
      }

      blockInfo.setINodeIdNoPersistance(dalClass.getInodeId());
      blockInfo.setTimestampNoPersistance(dalClass.getTimeStamp());
      blockInfo.setBlockIndexNoPersistance(dalClass.getBlockIndex());

      return blockInfo;
    } else {
      return null;
    }
  }
}
