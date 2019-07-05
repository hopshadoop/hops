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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;


/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {

  private final DatanodeManager datanodeManager;
  private final static List<DatanodeDescriptor> empty_datanode_list =
      Collections.unmodifiableList(new ArrayList<DatanodeDescriptor>());
  private final static List<DatanodeStorageInfo> empty_storage_list =
      Collections.unmodifiableList(new ArrayList<DatanodeStorageInfo>());

  BlocksMap(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  void close() {
    // Empty blocks once GSet#clear is implemented (HDFS-3940)
  }

  BlockCollection getBlockCollection(Block b)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous info = getStoredBlock(b);
    return (info != null) ? info.getBlockCollection() : null;
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  BlockInfoContiguous addBlockCollection(BlockInfoContiguous b, BlockCollection bc)
      throws StorageException, TransactionContextException {
    b.setBlockCollection(bc);
    return b;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(Block block)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous blockInfo = getStoredBlock(block);
    if (blockInfo == null) {
      return;
    }
    blockInfo.remove();
    blockInfo.removeAllReplicas();
  }
  
  /**
   * Returns the block object it it exists in the map.
   */
  BlockInfoContiguous getStoredBlock(Block b)
      throws StorageException, TransactionContextException {
    // TODO STEFFEN - This is a workaround to prevent NullPointerExceptions for me. Not sure how to actually fix the bug.
    if (b == null) {
      return null;
    }
    if (!(b instanceof BlockInfoContiguous)) {
      return EntityManager
          .find(BlockInfoContiguous.Finder.ByBlockIdAndINodeId, b.getBlockId());
    }
    return (BlockInfoContiguous) b;
  }

  /**
   * Searches for the block in the BlocksMap and
   * returns a list of storages that have this block.
   */
  List<DatanodeStorageInfo> storageList(Block b)
      throws TransactionContextException, StorageException {
    BlockInfoContiguous blockInfo = getStoredBlock(b);
    return storageList(blockInfo);
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the storages the block belongs to.
   */
  List<DatanodeStorageInfo> storageList(BlockInfoContiguous storedBlock)
      throws StorageException, TransactionContextException {
    if (storedBlock == null) {
      return null;
    }

    DatanodeStorageInfo[] desc = storedBlock.getStorages(datanodeManager);

    if (desc == null) {
      return empty_storage_list;
    } else {
      return Arrays.asList(desc);
    }
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to
   * <i>that are of the given {@link DatanodeStorage.State state}</i>.
   *
   * @param state DatanodeStorage state by which to filter the returned Iterable
   */
  List<DatanodeStorageInfo> storageList(Block b, final DatanodeStorage.State state) throws StorageException,
      TransactionContextException {
    BlockInfoContiguous blockInfo = getStoredBlock(b);
    return storageList(blockInfo, state);
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the storages the block belongs to.
   */
  List<DatanodeStorageInfo> storageList(BlockInfoContiguous storedBlock, final DatanodeStorage.State state)
      throws StorageException, TransactionContextException {
    if (storedBlock == null) {
      return null;
    }
    DatanodeStorageInfo[] desc = storedBlock.getStorages(datanodeManager,  state);
    if (desc == null) {
      return empty_storage_list;
    } else {
      return Arrays.asList(desc);
    }
  }

  /**
   * counts number of containing nodes. Better than using iterator.
   */
  int numNodes(Block b) throws StorageException, TransactionContextException {
    BlockInfoContiguous info = getStoredBlock(b);
    if (info == null) {
      return 0;
    } else {
      if(info.isProvidedBlock()){
        return 1;
      } else {
        return info.numNodes(datanodeManager);
      }
    }
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous info = getStoredBlock(b);
    if (info == null) {
      return false;
    }

    // remove block from the data-node list and the node from the block info
    return node.removeBlock(info);
  }

  boolean removeNode(Block b, int sid)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous info = getStoredBlock(b);
    if (info == null) {
      return false;
    }

    // remove block from the data-node list and the node from the block info
    return info.removeReplica(sid)!=null;
  }
  
  int size() throws IOException {
    LightWeightRequestHandler getAllBlocksSizeHander =
        new LightWeightRequestHandler(HDFSOperationType.GET_ALL_BLOCKS_SIZE) {
          @Override
          public Object performTask() throws IOException {
            BlockInfoDataAccess bida = (BlockInfoDataAccess) HdfsStorageFactory
                .getDataAccess(BlockInfoDataAccess.class);
            return bida.countAll();
          }
        };
    return (Integer) getAllBlocksSizeHander.handle();
  }

  int sizeCompleteOnly() throws IOException {
    LightWeightRequestHandler getAllBlocksSizeHander =
        new LightWeightRequestHandler(
            HDFSOperationType.GET_COMPLETE_BLOCKS_TOTAL) {
          @Override
          public Object performTask() throws IOException {
            BlockInfoDataAccess bida = (BlockInfoDataAccess) HdfsStorageFactory
                .getDataAccess(BlockInfoDataAccess.class);
            return bida.countAllCompleteBlocks();
          }
        };
    return (Integer) getAllBlocksSizeHander.handle();
  }

  Iterable<BlockInfoContiguous> getBlocks() throws IOException {
    LightWeightRequestHandler getAllBlocksHander =
        new LightWeightRequestHandler(HDFSOperationType.GET_ALL_BLOCKS) {
          @Override
          public Object performTask() throws IOException {
            BlockInfoDataAccess bida = (BlockInfoDataAccess) HdfsStorageFactory
                .getDataAccess(BlockInfoDataAccess.class);
            return bida.findAllBlocks();
          }
        };
    return (List<BlockInfoContiguous>) getAllBlocksHander.handle();
  }
  
  /**
   * Get the capacity of the HashMap that stores blocks
   */
  int getCapacity() {
    return Integer.MAX_VALUE;
  }

  List<INodeIdentifier> getAllINodeFiles(final long offset, final long count)
      throws IOException {
    return (List<INodeIdentifier>) new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_INODES) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.getAllINodeFiles(offset, count);
      }
    }.handle();
  }
  
  boolean haveFilesWithIdGreaterThan(final long id) throws IOException {
    return (Boolean) new LightWeightRequestHandler(
        HDFSOperationType.HAVE_FILES_WITH_IDS_GREATER_THAN) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.haveFilesWithIdsGreaterThan(id);
      }
    }.handle();
  }
  
  boolean haveFilesWithIdBetween(final long startId, final long endId)
      throws IOException {
    return (Boolean) new LightWeightRequestHandler(
        HDFSOperationType.HAVE_FILES_WITH_IDS_BETWEEN) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.haveFilesWithIdsBetween(startId, endId);
      }
    }.handle();
  }

  long getMaxInodeId() throws IOException {
    return (Long) new LightWeightRequestHandler(
        HDFSOperationType.GET_MAX_INODE_ID) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.getMaxId();
      }
    }.handle();
  }
  
  long getMinFileId() throws IOException {
    return (Long) new LightWeightRequestHandler(
        HDFSOperationType.GET_MIN_FILE_ID) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.getMinFileId();
      }
    }.handle();
  }

  int countAllFiles() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNTALL_FILES) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        return ida.countAllFiles();
      }
    }.handle();
  }
  
  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b) throws StorageException, TransactionContextException {
    return getStorages(getStoredBlock(b));
  }
  
  public List<DatanodeStorageInfo> getStorages(BlockInfoContiguous block)
      throws TransactionContextException, StorageException {
    DatanodeStorageInfo[] array = block.getStorages(datanodeManager);
    if(array!=null){
      return Arrays.asList(array);
    } else {
      return Collections.EMPTY_LIST;
    }
  }
}
