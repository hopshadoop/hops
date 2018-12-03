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

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.FileInodeData;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * I-node for closed file.
 */
@InterfaceAudience.Private
public class INodeFile extends INodeWithAdditionalFields implements BlockCollection {
  
  /** The same as valueOf(inode, path, false). */
  public static INodeFile valueOf(INode inode, String path
      ) throws FileNotFoundException, StorageException, TransactionContextException {
    return valueOf(inode, path, false);
  }

  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
      throws FileNotFoundException, StorageException, TransactionContextException {
    if (inode == null) {
      if (acceptNull) {
        return null;
      } else {
        throw new FileNotFoundException("File does not exist: " + path);
      }
    }
    if (!inode.isFile()) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return inode.asFile();
}

  private int generationStamp = (int) GenerationStamp.LAST_RESERVED_STAMP;
  private long size = 0;
  private boolean isFileStoredInDB = false;
  
  /**
   * @return true unconditionally.
   */
  @Override
  public final boolean isFile() {
    return true;
  }

  /**
   * @return this object.
   */
  @Override
  public final INodeFile asFile() {
    return this;
  }

  public INodeFile(long id, PermissionStatus permissions, BlockInfo[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize, byte storagePolicyID) throws IOException {
    this(id, permissions, blklist, replication, modificationTime, atime, preferredBlockSize, storagePolicyID, false);
  }

  public INodeFile(long id, PermissionStatus permissions, BlockInfo[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize, byte storagePolicyID, boolean inTree) throws IOException {
    super(id, permissions, modificationTime, atime, inTree);
    header = HeaderFormat.toLong(preferredBlockSize, replication);
    this.setFileStoredInDBNoPersistence(false); // it is set when the data is stored in the database
    this.setBlockStoragePolicyIDNoPersistance(storagePolicyID);
  }
 
  public INodeFile(long id, PermissionStatus permissions, long header,
      long modificationTime, long atime, boolean isFileStoredInDB, byte storagepolicy, boolean inTree) throws
      IOException {
    super(id, permissions, modificationTime, atime, inTree);
    this.header = header;
    this.isFileStoredInDB = isFileStoredInDB;
    this.blockStoragePolicyID = storagepolicy;
  }
  
  //HOP:
  public INodeFile(INodeFile other)
      throws IOException {
    super(other);
    setGenerationStampNoPersistence(other.getGenerationStamp());
    setSizeNoPersistence(other.getSize());
    setFileStoredInDBNoPersistence(other.isFileStoredInDB());
    setPartitionIdNoPersistance(other.getPartitionId());
    this.header = other.getHeader();
    this.features = other.features;
  }
  
  /**
   * If the inode contains a {@link FileUnderConstructionFeature}, return it;
   * otherwise, return null.
   */
  public final FileUnderConstructionFeature getFileUnderConstructionFeature() {
    for (Feature f : features) {
      if (f instanceof FileUnderConstructionFeature) {
        return (FileUnderConstructionFeature) f;
      }
    }
    return null;
  }
  
  /** Is this file under construction? */
  @Override // BlockCollection
  public boolean isUnderConstruction() {
    return getFileUnderConstructionFeature() != null;
  }
  
  /**
   * @return the replication factor of the file.
   */
  @Override
  public short getBlockReplication() {
    return HeaderFormat.getReplication(header);
  }

  @Override
  public byte getStoragePolicyID() throws TransactionContextException, StorageException {
    byte id = getLocalStoragePolicyID();
    if (id == BlockStoragePolicySuite.ID_UNSPECIFIED) {
      return this.getParent() != null ? this.getParent().getStoragePolicyID() : id;
    }
    return id;
  }


  /**
   * @return preferred block size (in bytes) of the file.
   */
  @Override
  public long getPreferredBlockSize() {
    return HeaderFormat.getPreferredBlockSize(header);
  }
  
  @Override
  public BlockInfo getBlock(int index) throws TransactionContextException, StorageException {
    return (BlockInfo) EntityManager.find(BlockInfo.Finder.ByINodeIdAndIndex, id, index);
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
  int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info)
      throws StorageException, TransactionContextException {
    parent = null;
    BlockInfo[] blocks = getBlocks();
    if (blocks != null && info != null) {
      for (BlockInfo blk : blocks) {
        blk.setBlockCollection(null);
        info.addDeleteBlock(blk);
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
  public final ContentSummaryComputationContext  computeContentSummary(ContentSummaryComputationContext summary)
      throws StorageException, TransactionContextException {
    computeContentSummary4Current(summary.getCounts());
    return summary;
  }

  private void computeContentSummary4Current(final Content.Counts counts) throws StorageException,
      TransactionContextException {
    counts.add(Content.LENGTH, computeFileSize());
    counts.add(Content.FILE, 1);
    counts.add(Content.DISKSPACE, diskspaceConsumed());
  }
  
  /**
   * Compute file size of the current file size
   * but not including the last block if it is under construction.
   * @throws io.hops.exception.StorageException
   * @throws io.hops.exception.TransactionContextException
   */
  public final long computeFileSizeNotIncludingLastUcBlock() throws StorageException, TransactionContextException {
    return computeFileSize(false);
  }
  
  public long computeFileSize()
      throws StorageException, TransactionContextException {
    return computeFileSize(true, getBlocks());
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
      // We do not know the replicaton of the database here. However, to be
      // consistent with normal files we will multiply the file size by the
      // replication factor.
      return getSize() * getBlockReplication();
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
    if (isUnderConstruction()) {
      FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
      if (uc != null && uc.getPenultimateBlockId() > -1) {
        return EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
            uc.getPenultimateBlockId(), getId());
      }
    }
    BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() throws IOException, StorageException {
    if (isUnderConstruction()) {
      FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
      if (uc != null && uc.getLastBlockId() > -1) {
        return EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
            uc.getLastBlockId(), getId());
      }
    }
    BlockInfo[] blocks = getBlocks();
    return blocks == null || blocks.length == 0 ? null :
        blocks[blocks.length - 1];
  }

  @Override
  public int numBlocks() throws StorageException, TransactionContextException {
    BlockInfo[] blocks = getBlocks();
    return blocks == null ? 0 : blocks.length;
  }
  

  /** @return the diskspace required for a full block. */
  final long getBlockDiskspace() {
    return getPreferredBlockSize() * getBlockReplication();
  }

  void setReplication(short replication)
      throws StorageException, TransactionContextException {
    header = HeaderFormat.REPLICATION.BITS.combine(replication, header);
    save();
  }
  
  public INodeFile toUnderConstruction(String clientName, String clientMachine)
    throws IOException {
    FileUnderConstructionFeature uc = new FileUnderConstructionFeature(clientName, clientMachine, this);
    addFeature(uc);
    save();
    return this;
  }
  
  /**
   * Convert the file to a complete file, i.e., to remove the Under-Construction
   * feature.
   */
  public INodeFile toCompleteFile(long mtime) throws IOException, StorageException {
    FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
    if (uc != null) {
      if (!isFileStoredInDB()) {
        assert assertAllBlocksComplete() : "Can't finalize inode " + this +
            " since it contains non-complete blocks! Blocks are " + getBlocks();
      }
      removeFeature(uc);
      this.setModificationTime(mtime);
    }
    return this;
  }
  
  /**
   * @return true if all of the blocks in this file are marked as completed.
   */
  private boolean assertAllBlocksComplete()
      throws StorageException, TransactionContextException {
    for (BlockInfo b : getBlocks()) {
      if (!b.isComplete()) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock, DatanodeStorageInfo[] locations)
      throws IOException {
    Preconditions.checkState(isUnderConstruction());
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoUnderConstruction ucBlock = lastBlock
        .convertToBlockUnderConstruction(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
            locations);
    ucBlock.setBlockCollection(this);
    setBlock(numBlocks() - 1, ucBlock);
    return ucBlock;
  }
  
  /**
   * Remove a block from the block list. This block should be
   * the last one on the list
   */
  boolean removeLastBlock(Block oldBlock) throws IOException, StorageException {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return false;
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldBlock)) {
      return false;
    }
    removeBlock(blocks[blocks.length - 1]);
    return true;
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
  
  /* End of Under-Construction Feature */
  
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
    if (!isInTree()) {
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

  @Override
  public INode cloneInode () throws IOException{
    return new INodeFile(this);
  }
}
