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
import static org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite.ID_UNSPECIFIED;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
  private Set<Block> removedBlocks = new HashSet<>();
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

  public INodeFile(long id, PermissionStatus permissions, BlockInfoContiguous[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize, byte storagePolicyID) throws IOException {
    this(id, permissions, blklist, replication, modificationTime, atime, preferredBlockSize, storagePolicyID, false);
  }

  public INodeFile(long id, PermissionStatus permissions, BlockInfoContiguous[] blklist,
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
    if (id == ID_UNSPECIFIED) {
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
  public BlockInfoContiguous getBlock(int index) throws TransactionContextException, StorageException {
    return (BlockInfoContiguous) EntityManager.find(BlockInfoContiguous.Finder.ByINodeIdAndIndex, id, index);
  }
  
  /**
   * @return the blocks of the file.
   */
  @Override
  public BlockInfoContiguous[] getBlocks()
      throws StorageException, TransactionContextException {

    if(isFileStoredInDB()){
      FSNamesystem.LOG.debug("Stuffed Inode:  getBlocks(). the file is stored in the database. Returning empty list of blocks");
      return BlockInfoContiguous.EMPTY_ARRAY;
    }

    List<BlockInfoContiguous> blocks = getBlocksOrderedByIndex();
    if(blocks == null){
      return BlockInfoContiguous.EMPTY_ARRAY;
    }

    BlockInfoContiguous[] blks = new BlockInfoContiguous[blocks.size()];
    return blocks.toArray(blks);
  }

  public void storeFileDataInDB(byte[] data)
      throws StorageException {

    int len = data.length;
    FileInodeData fid;
    DBFileDataAccess fida = null;

    if(len <= FSNamesystem.getDBInMemBucketSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
      fid = new FileInodeData(getId(), data, data.length, FileInodeData.Type.InmemoryFile);
    } else {
      if( len <= FSNamesystem.getDBOnDiskSmallBucketSize()){
        fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
      }else if( len <= FSNamesystem.getDBOnDiskMediumBucketSize()){
        fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
      } else if (len <= FSNamesystem.getMaxSmallFileSize()) {
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
    if(getSize() <= FSNamesystem.getDBInMemBucketSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
    }else if (getSize() <= FSNamesystem.getDBOnDiskSmallBucketSize()){
      fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
    } else if (getSize() <= FSNamesystem.getDBOnDiskMediumBucketSize()){
      fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
    } else {
      fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
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
    if(getSize() <= FSNamesystem.getDBInMemBucketSize()){
      fida = (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
      fileType = FileInodeData.Type.InmemoryFile;
    }else if (getSize() <= FSNamesystem.getDBOnDiskSmallBucketSize()){
      fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    } else if (getSize() <= FSNamesystem.getDBOnDiskMediumBucketSize()){
      fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    } else {
      fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
      fileType = FileInodeData.Type.OnDiskFile;
    }

    fida.delete(new FileInodeData(getId(),null, (int)getSize(), fileType));
    FSNamesystem.LOG.debug("Stuffed Inode:  File data for Inode Id: "+getId()+" is deleted");
  }
  /**
   * append array of blocks to this.blocks
   */
  List<BlockInfoContiguous> concatBlocks(INodeFile[] inodes)
      throws StorageException, TransactionContextException {
    List<BlockInfoContiguous> oldBlks = new ArrayList<>();
    for (INodeFile srcInode : inodes) {
      for (BlockInfoContiguous block : srcInode.getBlocks()) {
        BlockInfoContiguous copy = BlockInfoContiguous.cloneBlock(block);
        oldBlks.add(copy);
        addBlock(block);
        block.setBlockCollection(this);
      }
    }
    recomputeFileSize();
    return oldBlks;
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfoContiguous newblock)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous maxBlk = findMaxBlk();
    newblock.setBlockIndex(maxBlk.getBlockIndex() + 1);
  }

  /**
   * Set the block of the file at the given index.
   */
  public void setBlock(int idx, BlockInfoContiguous blk)
      throws StorageException, TransactionContextException {
    blk.setBlockIndex(idx);
  }

  @Override
  public void destroyAndCollectBlocks(BlockStoragePolicySuite bsps,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
      throws StorageException, TransactionContextException {
    parent = null;
    BlockInfoContiguous[] blocks = getBlocks();
    if (blocks != null && collectedBlocks != null) {
      for (BlockInfoContiguous blk : blocks) {
        blk.setBlockCollection(null);
        collectedBlocks.addDeleteBlock(blk);
      }
    }

    if(isFileStoredInDB()){
      deleteFileDataStoredInDB();
    }
    removedINodes.add(this);
  }

  @Override
  public String getName() throws StorageException, TransactionContextException {
    // Get the full path name of this inode.
    return getFullPathName();
  }

  @Override
  public final ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) throws TransactionContextException, StorageException {
    final ContentCounts counts = summary.getCounts();
    long fileLen = 0;
    fileLen = computeFileSize();
    counts.addContent(Content.FILE, 1);
    counts.addContent(Content.LENGTH, fileLen);
    counts.addContent(Content.DISKSPACE, storagespaceConsumed());

    if (getStoragePolicyID() != ID_UNSPECIFIED){
      BlockStoragePolicy bsp = summary.getBlockStoragePolicySuite().
          getPolicy(getStoragePolicyID());
      List<StorageType> storageTypes = bsp.chooseStorageTypes(HeaderFormat.getReplication(header));
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, fileLen);
      }
    }
    return summary;
  }
  
  /**
   * Compute file size of the current file size
   * but not including the last block if it is under construction.
   * @throws io.hops.exception.StorageException
   * @throws io.hops.exception.TransactionContextException
   */
  public final long computeFileSizeNotIncludingLastUcBlock() throws StorageException, TransactionContextException {
    return computeFileSize(false, false);
  }
  
  public long computeFileSize()
      throws StorageException, TransactionContextException {
    return computeFileSize(true, false);
  }  

  /**
   * Compute file size.
   * May or may not include BlockInfoUnderConstruction.
   */
  public final long computeFileSize(boolean includesLastUcBlock,
      boolean usePreferredBlockSize4LastUcBlock) throws StorageException, TransactionContextException {
    BlockInfoContiguous[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long size = blocks[last].getNumBytes();

    if (blocks[last] instanceof BlockInfoContiguousUnderConstruction) {
      if (!includesLastUcBlock) {
        size = 0;
      } else if (usePreferredBlockSize4LastUcBlock) {
        size = getPreferredBlockSize();
      }
    }
    //sum other blocks
    for (int i = 0; i < last; i++) {
      size += blocks[i].getNumBytes();
    }
    return size;
  }

  @Override
  QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps, byte blockStoragePolicyId, QuotaCounts counts)
      throws StorageException, TransactionContextException {
    long nsDelta = 1;
    final long ssDeltaNoReplication;
    short replication;
    ssDeltaNoReplication = storagespaceConsumedNoReplication();
    replication = getBlockReplication();
    counts.addNameSpace(nsDelta);
    counts.addStorageSpace(ssDeltaNoReplication * replication);
    
    //storage policy is not set for new inodes
    if (blockStoragePolicyId != ID_UNSPECIFIED){
      BlockStoragePolicy bsp = bsps.getPolicy(blockStoragePolicyId);
      List<StorageType> storageTypes = bsp.chooseStorageTypes(replication);
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, ssDeltaNoReplication);
      }
    }

    return counts;
  }

  /**
   * Compute size consumed by all blocks of the current file,
   * including blocks in its snapshots.
   * Use preferred block size for the last block if it is under construction.
   */
  public final long storagespaceConsumed() throws StorageException, TransactionContextException {
    return storagespaceConsumedNoReplication() * getBlockReplication();
  }

  public final long storagespaceConsumedNoReplication() throws StorageException, TransactionContextException {
    if(isFileStoredInDB()){
      // We do not know the replicaton of the database here. However, to be
      // consistent with normal files we will multiply the file size by the
      // replication factor.
      return getSize();
    }else{
      return computeFileSize(true, true);
    }
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfoContiguous getPenultimateBlock()
      throws StorageException, TransactionContextException {
    if (isUnderConstruction()) {
      FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
      if (uc != null && uc.getPenultimateBlockId() > -1) {
        return EntityManager.find(BlockInfoContiguous.Finder.ByBlockIdAndINodeId,
            uc.getPenultimateBlockId(), getId());
      }
    }
    BlockInfoContiguous[] blocks = getBlocks();
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfoContiguous getLastBlock() throws IOException, StorageException {
    if (isUnderConstruction()) {
      FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
      if (uc != null && uc.getLastBlockId() > -1) {
        return EntityManager.find(BlockInfoContiguous.Finder.ByBlockIdAndINodeId,
            uc.getLastBlockId(), getId());
      }
    }
    BlockInfoContiguous[] blocks = getBlocks();
    return blocks == null || blocks.length == 0 ? null :
        blocks[blocks.length - 1];
  }

  @Override
  public int numBlocks() throws StorageException, TransactionContextException {
    BlockInfoContiguous[] blocks = getBlocks();
    return blocks == null ? 0 : blocks.length;
  }
  

  /** @return the storagespace required for a full block. */
  final long getPreferredBlockStoragespace() {
    return getPreferredBlockSize() * getBlockReplication();
  }

  void setFileReplication(short replication)
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
    for (BlockInfoContiguous b : getBlocks()) {
      if (!b.isComplete()) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public BlockInfoContiguousUnderConstruction setLastBlock(
      BlockInfoContiguous lastBlock, DatanodeStorageInfo[] locations)
      throws IOException {
    Preconditions.checkState(isUnderConstruction());
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoContiguousUnderConstruction ucBlock = lastBlock
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
    final BlockInfoContiguous[] blocks = getBlocks();
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
  
  public void removeBlock(BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    BlockInfoContiguous[] blks = getBlocks();
    int index = block.getBlockIndex();
    
    block.setBlockCollection(null);
    
    if (index != blks.length) {
      for (int i = index + 1; i < blks.length; i++) {
        blks[i].setBlockIndex(i - 1);
      }
    }
  }
  
  /* End of Under-Construction Feature */
  
  public BlockInfoContiguous findMaxBlk()
      throws StorageException, TransactionContextException {
    BlockInfoContiguous maxBlk = (BlockInfoContiguous) EntityManager
        .find(BlockInfoContiguous.Finder.ByMaxBlockIndexForINode, this.getId());
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
    setSizeNoPersistence(this.computeFileSize(true, false));
    save();
  }

  protected List<BlockInfoContiguous> getBlocksOrderedByIndex()
      throws TransactionContextException, StorageException {
    if (!isInTree()) {
      return null;
    }
    List<BlockInfoContiguous> blocksInDB = (List<BlockInfoContiguous>) EntityManager
        .findList(BlockInfoContiguous.Finder.ByINodeId, id);
    List<BlockInfoContiguous> blocks = null;
    if (blocksInDB != null) {
      for (BlockInfoContiguous block : blocksInDB) {
        if (!removedBlocks.contains(block)) {
          if(blocks==null){
            blocks = new ArrayList<>();
          }
          blocks.add(block);
        }
      }
    }
    if (blocks != null) {
      Collections.sort(blocks, BlockInfoContiguous.Order.ByBlockIndex);
      return blocks;
    } else {
      return null;
    }
  }

  @Override
  public INode cloneInode () throws IOException{
    return new INodeFile(this);
  }

  /**
   * Remove full blocks at the end file up to newLength
   * @return sum of sizes of the remained blocks
   */
  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) throws StorageException, TransactionContextException {
    final BlockInfoContiguous[] oldBlocks = getBlocks();
    if (oldBlocks == null)
      return 0;
    // find the minimum n such that the size of the first n blocks > max
    int n = 0;
    long size = 0;
    for(; n < oldBlocks.length && max > size; n++) {
      size += oldBlocks[n].getNumBytes();
    }
    if (n >= oldBlocks.length)
      return size;

    // collect the blocks beyond max
    if (collectedBlocks != null) {
      for(; n < oldBlocks.length; n++) {
        BlockInfoContiguous block = oldBlocks[n];
        removedBlocks.add(block);
        collectedBlocks.addDeleteBlock(block);
      }
    }
    return size;
  }

  /**
   * compute the quota usage change for a truncate op
   * @param newLength the length for truncation
   * @return the quota usage delta (not considering replication factor)
   */
  long computeQuotaDeltaForTruncate(final long newLength) throws StorageException, TransactionContextException {
    final BlockInfoContiguous[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return 0;
    }

    int n = 0;
    long size = 0;
    for (; n < blocks.length && newLength > size; n++) {
      size += blocks[n].getNumBytes();
    }
    final boolean onBoundary = size == newLength;

    long truncateSize = 0;
    for (int i = (onBoundary ? n : n - 1); i < blocks.length; i++) {
      truncateSize += blocks[i].getNumBytes();
    }

    return onBoundary ? -truncateSize : (getPreferredBlockSize() - truncateSize);
  }

}
