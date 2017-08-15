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
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.FileInodeData;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * I-node for closed file.
 */
@InterfaceAudience.Private
public class INodeFile extends INode implements BlockCollection {
  /**
   * Cast INode to INodeFile.
   */
  public static INodeFile valueOf(INode inode, String path) throws IOException {
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + path);
    }
    if (!(inode instanceof INodeFile)) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return (INodeFile) inode;
  }


  private int generationStamp = (int) GenerationStamp.FIRST_VALID_STAMP;
  private long size;
  private boolean isFileStoredInDB = false;
  

  public INodeFile(PermissionStatus permissions, BlockInfo[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize) throws IOException {
    super(permissions, modificationTime, atime);
    this.setReplicationNoPersistance(replication);
    this.setPreferredBlockSizeNoPersistance(preferredBlockSize);
    this.setFileStoredInDBNoPersistence(false); // it is set when the data is stored in the database
  }

  public INodeFile(PermissionStatus permissions, long header,
      long modificationTime, long atime, boolean isFileStoredInDB) throws IOException {
    super(permissions, modificationTime, atime);
    this.setHeaderNoPersistance(header);
    this.isFileStoredInDB = isFileStoredInDB;
  }

  //HOP:
  public INodeFile(INodeFile other)
      throws IOException {
    super(other);
    setReplicationNoPersistance(other.getBlockReplication());
    setPreferredBlockSizeNoPersistance(other.getPreferredBlockSize());
    setGenerationStampNoPersistence(other.getGenerationStamp());
    setSizeNoPersistence(other.getSize());
    setFileStoredInDBNoPersistence(other.isFileStoredInDB());
    setHasBlocksNoPersistance(other.hasBlocks());
    setPartitionIdNoPersistance(other.getPartitionId());
    setHeaderNoPersistance(other.getHeader());
  }

  /**
   * @return the replication factor of the file.
   */
  @Override
  public short getBlockReplication() {
    return getBlockReplication(header);
  }

  /**
   * @return preferred block size (in bytes) of the file.
   */
  @Override
  public long getPreferredBlockSize() {
    return getPreferredBlockSize(header);
  }

  /**
   * @return the blocks of the file.
   */
  @Override
  public BlockInfo[] getBlocks()
      throws StorageException, TransactionContextException {

    if(isFileStoredInDB()){
      FSNamesystem.LOG.debug("Stuffed Inode:  getBlocks(). the file is stored in the database. Returning empty list of blocks");
      return BlockInfo.EMPTY_ARRAY;
    }

    List<BlockInfo> blocks = getBlocksOrderedByIndex();
    if(blocks == null){
      return BlockInfo.EMPTY_ARRAY;
    }

    BlockInfo[] blks = new BlockInfo[blocks.size()];
    return blocks.toArray(blks);
  }

  public void storeFileDataInDB(byte[] data)
      throws StorageException {

    int len = data.length;
    FileInodeData fid;
    DBFileDataAccess fida = null;

    if(len <= FSNamesystem.dbInMemorySmallFileMaxSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
      fid = new FileInodeData(getId(), data, data.length, FileInodeData.Type.InmemoryFile);
    } else {
      if( len <= FSNamesystem.dbOnDiskSmallFileMaxSize()){
        fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
      }else if( len <= FSNamesystem.dbOnDiskMediumFileMaxSize()){
        fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
      }else if( len <= FSNamesystem.dbOnDiskLargeFileMaxSize()){
        fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
      }else{
        StorageException up = new StorageException("The data is too large to be stored in the database. Requested data size is : "+len);
        throw up;
      }
      fid = new FileInodeData(getId(), data, data.length, FileInodeData.Type.OnDiskFile);
    }

    fida.add(fid);
    FSNamesystem.LOG.debug("Stuffed Inode:  the file has been stored in the database ");
  }

  public byte[] getFileDataInDB() throws StorageException {
    HdfsStorageFactory.getConnector().readCommitted();

    //depending on the file size read the file from appropriate table
    FileInodeData fid = null;
    DBFileDataAccess fida = null;
    if(getSize() <= FSNamesystem.dbInMemorySmallFileMaxSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
    }else if (getSize() <= FSNamesystem.dbOnDiskSmallFileMaxSize()){
      fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
    } else if (getSize() <= FSNamesystem.dbOnDiskMediumFileMaxSize()){
      fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
    } else if (getSize() <= FSNamesystem.dbOnDiskLargeFileMaxSize()){
      fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
    } else {
      StorageException up = new StorageException("The data is too large to be stored in the database. Requested data size is : "+getSize());
      throw up;
    }

    if(fida instanceof  LargeOnDiskInodeDataAccess) {
      fid = (FileInodeData) (((LargeOnDiskInodeDataAccess)fida).get(getId(),(int)getSize()));
    }else{
      fid = (FileInodeData) fida.get(getId());
    }
    FSNamesystem.LOG.debug("Stuffed Inode:  Read file data from the database. Data length is :" + fid.getInodeData().length);
    return fid.getInodeData();
  }

  public void deleteFileDataStoredInDB() throws StorageException {
    //depending on the file size delete the file from appropriate table
    FileInodeData fid = null;
    DBFileDataAccess fida = null;
    FileInodeData.Type fileType = null;
    if(getSize() <= FSNamesystem.dbInMemorySmallFileMaxSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
      fileType = FileInodeData.Type.InmemoryFile;
    }else if (getSize() <= FSNamesystem.dbOnDiskSmallFileMaxSize()){
      fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    } else if (getSize() <= FSNamesystem.dbOnDiskMediumFileMaxSize()){
      fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    } else if (getSize() <= FSNamesystem.dbOnDiskLargeFileMaxSize()){
      fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    } else {
      IllegalStateException up = new IllegalStateException("Can not delete file. It is not stored in the database");
      throw up;
    }
    fida.delete(new FileInodeData(getId(),null, (int)getSize(), fileType));
    FSNamesystem.LOG.debug("Stuffed Inode:  File data for Inode Id: "+getId()+" is deleted");
  }
  /**
   * append array of blocks to this.blocks
   */
  List<BlockInfo> appendBlocks(INodeFile[] inodes, int totalAddedBlocks /*HOP not used*/)
      throws StorageException, TransactionContextException {
    List<BlockInfo> oldBlks = new ArrayList<>();
    for (INodeFile srcInode : inodes) {
      for (BlockInfo block : srcInode.getBlocks()) {
        BlockInfo copy = BlockInfo.cloneBlock(block);
        oldBlks.add(copy);
        addBlock(block);
        block.setBlockCollection(this);
      }
    }
    return oldBlks;
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock)
      throws StorageException, TransactionContextException {
    BlockInfo maxBlk = findMaxBlk();
    newblock.setBlockIndex(maxBlk.getBlockIndex() + 1);
  }

  /**
   * Set the block of the file at the given index.
   */
  public void setBlock(int idx, BlockInfo blk)
      throws StorageException, TransactionContextException {
    blk.setBlockIndex(idx);
  }

  @Override
  int collectSubtreeBlocksAndClear(List<Block> v)
      throws StorageException, TransactionContextException {
    parent = null;
    BlockInfo[] blocks = getBlocks();
    if (blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        blk.setBlockCollection(null);
        v.add(blk);
      }
    }

    if(isFileStoredInDB()){
      deleteFileDataStoredInDB();
    }

    return 1;
  }
  
  @Override
  public String getName() throws StorageException, TransactionContextException {
    // Get the full path name of this inode.
    return getFullPathName();
  }


  @Override
  long[] computeContentSummary(long[] summary)
      throws StorageException, TransactionContextException {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /**
   * Compute file size.
   * May or may not include BlockInfoUnderConstruction.
   */
  public long computeFileSize(boolean includesBlockInfoUnderConstruction)
      throws StorageException, TransactionContextException {
    return computeFileSize(includesBlockInfoUnderConstruction, getBlocks());
  }

  static long computeFileSize(boolean includesBlockInfoUnderConstruction,
      BlockInfo[] blocks) throws StorageException {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long bytes = 0;
    
    if(blocks[last] instanceof BlockInfoUnderConstruction){
        if(includesBlockInfoUnderConstruction){
            bytes = blocks[last].getNumBytes();
        }
    }else{
        bytes = blocks[last].getNumBytes();
    }
    for (int i = 0; i < last; i++) {
      bytes += blocks[i].getNumBytes();
    }
    return bytes;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts)
      throws StorageException, TransactionContextException {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed()
      throws StorageException, TransactionContextException {
    if(isFileStoredInDB()){
      return getSize(); // We do not know the replicaton of the database here. For now just return the size of the file
    }else {
      return diskspaceConsumed(getBlocks()); // Compute the size of the file. Takes replication in to account
    }
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    return diskspaceConsumed(blkArr, isUnderConstruction(),
        getPreferredBlockSize(), getBlockReplication());
  }

  static long diskspaceConsumed(Block[] blkArr, boolean underConstruction,
      long preferredBlockSize, short blockReplication) {
    long size = 0;
    if (blkArr == null) {
      return 0;
    }

    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length - 1] != null &&
        underConstruction) {
      size += preferredBlockSize - blkArr[blkArr.length - 1].getNumBytes();
    }
    return size * blockReplication;
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock()
      throws StorageException, TransactionContextException {
    BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() throws IOException, StorageException {
    BlockInfo[] blocks = getBlocks();
    return blocks == null || blocks.length == 0 ? null :
        blocks[blocks.length - 1];
  }

  @Override
  public int numBlocks() throws StorageException, TransactionContextException {
    BlockInfo[] blocks = getBlocks();
    return blocks == null ? 0 : blocks.length;
  }
  


  void setReplication(short replication)
      throws StorageException, TransactionContextException {
    setReplicationNoPersistance(replication);
    save();
  }

  public INodeFileUnderConstruction convertToUnderConstruction(
      String clientName, String clientMachine, DatanodeID clientNode)
      throws IOException {
    INodeFileUnderConstruction ucfile =
        new INodeFileUnderConstruction(this, clientName, clientMachine,
            clientNode);
    BlockInfo lastBlock = getLastBlock();
    BlockInfo penultimateBlock = getPenultimateBlock();
    if(lastBlock != null){
      ucfile.setLastBlockId(lastBlock.getBlockId());
    }
    if(penultimateBlock != null){
      ucfile.setPenultimateBlockId(penultimateBlock.getBlockId());
    }
    save(ucfile);
    return ucfile;
  }
  
  public BlockInfo findMaxBlk()
      throws StorageException, TransactionContextException {
    BlockInfo maxBlk = (BlockInfo) EntityManager
        .find(BlockInfo.Finder.ByMaxBlockIndexForINode, this.getId());
    return maxBlk;
  }
  
  public int getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStampNoPersistence(int generationStamp) {
    this.generationStamp = generationStamp;
  }
  
  public int nextGenerationStamp()
      throws StorageException, TransactionContextException {
    generationStamp++;
    save();
    return generationStamp;
  }

  public long getSize() {
    return size;
  }

  public boolean isFileStoredInDB(){
    return isFileStoredInDB;
  }

  public void setFileStoredInDB(boolean isFileStoredInDB) throws StorageException, TransactionContextException {
    setFileStoredInDBNoPersistence(isFileStoredInDB);
    save();
  }

  public void setFileStoredInDBNoPersistence(boolean isFileStoredInDB){
     this.isFileStoredInDB = isFileStoredInDB;
  }

  public void setSizeNoPersistence(long size) {
    this.size = size;
  }

  public void setSize(long size) throws TransactionContextException, StorageException {
    setSizeNoPersistence(size);
    save();
  }
  public void recomputeFileSize() throws StorageException, TransactionContextException {
    setSizeNoPersistence(this.computeFileSize(true));
    save();
  }

  protected List<BlockInfo> getBlocksOrderedByIndex()
      throws TransactionContextException, StorageException {
    if (getId() == INode.NON_EXISTING_ID) {
      return null;
    }
    List<BlockInfo> blocks = (List<BlockInfo>) EntityManager
        .findList(BlockInfo.Finder.ByINodeId, id);
    if (blocks != null) {
      Collections.sort(blocks, BlockInfo.Order.ByBlockIndex);
      return blocks;
    } else {
      return null;
    }
  }

}
