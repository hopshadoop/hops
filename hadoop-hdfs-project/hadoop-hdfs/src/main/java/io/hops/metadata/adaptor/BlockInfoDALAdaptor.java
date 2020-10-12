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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.zookeeper.KeeperException;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;

public class BlockInfoDALAdaptor extends
    DalAdaptor<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous, BlockInfo>
    implements
    BlockInfoDataAccess<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> {

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
  public org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous findById(
      long blockId, long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findById(blockId, inodeId));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findByInodeId(
      long id) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findByInodeId(id));
  }

  
  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findByInodeIds(
      long[] inodeIds) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findByInodeIds(inodeIds));
  }
  
  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findAllBlocks()
      throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findAllBlocks());
  }

  @Override
  public List<BlockInfoContiguous> findAllBlocks(long startID, long endID) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
            dataAccess.findAllBlocks(startID, endID));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findBlockInfosByStorageId(
      int storageId) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findBlockInfosByStorageId(storageId));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findBlockInfosByStorageId(int storageId,
      long from, int size) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(dataAccess.
        findBlockInfosByStorageId(storageId, from, size));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findBlockInfosBySids(
      List<Integer> sids) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findBlockInfosBySids(sids));
  }

  @Override
  public Set<Long> findINodeIdsByStorageId(int storageId)
      throws StorageException {
    return dataAccess.findINodeIdsByStorageId(storageId);
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> findByIds(
      long[] blockIds, long[] inodeIds) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous>) convertDALtoHDFS(
        dataAccess.findByIds(blockIds, inodeIds));
  }

  @Override
  public boolean existsOnAnyStorage(long inodeId, long blockId, List<Integer> sids) throws
      StorageException {
    return dataAccess.existsOnAnyStorage(inodeId, blockId, sids);
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> removed,
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> newed,
      Collection<org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous> modified)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed),
        convertHDFStoDAL(modified));
  }
  
  //only for testing
  @Override
  public void deleteBlocksForFile(long inodeID) throws StorageException {
    dataAccess.deleteBlocksForFile(inodeID);
  }

  @Override
  public BlockInfo convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous hdfsClass)
      throws StorageException {
    if (hdfsClass != null) {
      BlockInfo hopBlkInfo =
          new BlockInfo(hdfsClass.getBlockId(), hdfsClass.getBlockIndex(),
              hdfsClass.getInodeId(), hdfsClass.getNumBytes(),
              hdfsClass.getGenerationStamp(),
              hdfsClass.getBlockUCState().ordinal(), hdfsClass.getTimestamp());
      if (hdfsClass instanceof BlockInfoContiguousUnderConstruction) {
        BlockInfoContiguousUnderConstruction ucBlock =
            (BlockInfoContiguousUnderConstruction) hdfsClass;
        hopBlkInfo.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
        hopBlkInfo.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
        Block truncateBlock = ucBlock.getTruncateBlock();
        if(truncateBlock!=null){
          hopBlkInfo.setTruncateBlockGenerationStamp(truncateBlock.getGenerationStamp());
          hopBlkInfo.setTruncateBlockNumBytes(truncateBlock.getNumBytes());
        }
      }
      return hopBlkInfo;
    } else {
      return null;
    }
  }

  @Override
  public org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous convertDALtoHDFS(
      BlockInfo dalClass) throws StorageException {
    if (dalClass != null) {
      Block b = new Block(dalClass.getBlockId(), dalClass.getNumBytes(),
          dalClass.getGenerationStamp());
      org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous blockInfo = null;

      if (dalClass.getBlockUCState() >
          0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
        blockInfo = new BlockInfoContiguousUnderConstruction(b, dalClass.getInodeId());
        ((BlockInfoContiguousUnderConstruction) blockInfo).setBlockUCStateNoPersistance(
            HdfsServerConstants.BlockUCState.values()[dalClass
                .getBlockUCState()]);
        ((BlockInfoContiguousUnderConstruction) blockInfo)
            .setPrimaryNodeIndexNoPersistance(dalClass.getPrimaryNodeIndex());
        ((BlockInfoContiguousUnderConstruction) blockInfo)
            .setBlockRecoveryIdNoPersistance(dalClass.getBlockRecoveryId());
        if(dalClass.getTruncateBlockNumBytes()>0){
          Block truncateBlock = new Block(dalClass.getBlockId(), dalClass.getTruncateBlockNumBytes(), dalClass.getTruncateBlockGenerationStamp());
          ((BlockInfoContiguousUnderConstruction) blockInfo).setTruncateBlock(truncateBlock);
        }
      } else if (dalClass.getBlockUCState() ==
          HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
        blockInfo =
            new org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous(b,
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
