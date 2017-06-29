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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.MutableBlockCollection;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;

import java.io.IOException;
import java.util.List;

/**
 * I-node for file being written.
 */
@InterfaceAudience.Private
public class INodeFileUnderConstruction extends INodeFile
    implements MutableBlockCollection {
  /**
   * Cast INode to INodeFileUnderConstruction.
   */
  public static INodeFileUnderConstruction valueOf(INode inode, String path)
      throws IOException {
    final INodeFile file = INodeFile.valueOf(inode, path);
    if (!file.isUnderConstruction()) {
      throw new IOException("File is not under construction: " + path);
    }
    return (INodeFileUnderConstruction) file;
  }

  private String clientName;         // lease holder
  private final String clientMachine;
  private final DatanodeID clientNode; // if client is a cluster node too.

  private long lastBlockId = -1;
  private long penultimateBlockId = -1;

  public INodeFileUnderConstruction(PermissionStatus permissions,
      short replication, long preferredBlockSize, long modTime,
      String clientName, String clientMachine, DatanodeID clientNode)
      throws IOException {
    super(permissions, null, replication, modTime, modTime, preferredBlockSize);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  public INodeFileUnderConstruction(byte[] name, short blockReplication,
      long modificationTime, long preferredBlockSize, BlockInfo[] blocks,
      PermissionStatus perm, String clientName, String clientMachine,
      DatanodeID clientNode, int inodeId, int pid) throws IOException {
    super(perm, blocks, blockReplication, modificationTime, modificationTime,
        preferredBlockSize);
    setLocalNameNoPersistance(name);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
    this.id = inodeId;
    this.parentId = pid;
    //throw new UnsupportedOperationException("HOP: This constructor should not be used"); // The only reason it is here that it is called in some FSImage Classes that are not deleted. 
  }

  //HOP: used instead of INodeFile.convertToUnderConstruction
  protected INodeFileUnderConstruction(INodeFile file, String clientName,
      String clientMachine, DatanodeID clientNode)
      throws IOException {
    super(file);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
  
  public String getClientName() {
    return clientName;
  }

  void setClientName(String clientName)
      throws StorageException, TransactionContextException {
    this.clientName = clientName;
    save();
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public DatanodeID getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  public boolean isUnderConstruction() {
    return true;
  }

  //
  // converts a INodeFileUnderConstruction into a INodeFile
  // use the modification time as the access time
  //
  INodeFile convertToInodeFile()
      throws IOException {
    if(!isFileStoredInDB()) {
    assert allBlocksComplete() : "Can't finalize inode " + this +
        " since it contains non-complete blocks! Blocks are " + getBlocks();
    }
    INodeFile obj = new INodeFile(this);
    obj.setAccessTime(getModificationTime());
    return obj;
    
  }
  
  /**
   * @return true if all of the blocks in this file are marked as completed.
   */
  private boolean allBlocksComplete()
      throws StorageException, TransactionContextException {
    for (BlockInfo b : getBlocks()) {
      if (!b.isComplete()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeLastBlock(Block oldblock) throws IOException, StorageException {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }
    
    removeBlock(blocks[blocks.length - 1]);
  }

  /**
   * Convert the last block of the file to an under-construction block.
   * Set its locations.
   */
  @Override
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeDescriptor[] targets) throws IOException, StorageException {
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoUnderConstruction ucBlock = lastBlock
        .convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
            targets);
    ucBlock.setBlockCollection(this);
    setBlock(numBlocks() - 1, ucBlock);
    return ucBlock;
  }

  /**
   * Update the length for the last block
   *
   * @param lastBlockLength
   *     The length of the last block reported from client
   * @throws IOException
   */
  void updateLengthOfLastBlock(long lastBlockLength)
      throws IOException, StorageException {
    BlockInfo lastBlock = this.getLastBlock();
    assert (lastBlock != null) :
        "The last block for path " + this.getFullPathName() +
            " is null when updating its length";
    assert (lastBlock instanceof BlockInfoUnderConstruction) :
        "The last block for path " + this.getFullPathName() +
            " is not a BlockInfoUnderConstruction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }


  public void removeBlock(BlockInfo block)
      throws StorageException, TransactionContextException {
    BlockInfo[] blks = getBlocks();
    int index = block.getBlockIndex();

    block.setBlockCollection(null);

    if (index != blks.length) {
      for (int i = index + 1; i < blks.length; i++) {
        blks[i].setBlockIndex(i - 1);
      }
    }
  }

  void setLastBlockId(long lastBlockId){
    this.lastBlockId = lastBlockId;
  }

  void setPenultimateBlockId(long penultimateBlockId){
    this.penultimateBlockId = penultimateBlockId;
  }

  public void updateLastTwoBlocks(Lease lease)
      throws TransactionContextException, StorageException {
    updateLastTwoBlocks(lease, getFullPathName());
  }

  public void updateLastTwoBlocks(Lease lease, String src)
      throws TransactionContextException, StorageException {
    LeasePath lp = lease.getLeasePath(src);
    setLastBlockId(lp.getLastBlockId());
    setPenultimateBlockId(lp.getPenultimateBlockId());
  }

  @Override
  public BlockInfo getLastBlock() throws IOException, StorageException {
    if(lastBlockId < 0) {
      return super.getLastBlock();
    }
    return EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
        lastBlockId, getId());
  }

  @Override
  BlockInfo getPenultimateBlock()
      throws StorageException, TransactionContextException {
    if(penultimateBlockId < 0) {
      return super.getPenultimateBlock();
    }
    return EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
        penultimateBlockId, getId());
  }

  @Override
  public BlockInfo getBlock(int index)
      throws TransactionContextException, StorageException {
    List<BlockInfo> blocks = getBlocksOrderedByIndex();
    if(blocks == null){
      return null;
    }

    for(BlockInfo blk : blocks){
      if(blk.getBlockIndex() == index){
        return blk;
      }
    }

    return null;
  }
}
