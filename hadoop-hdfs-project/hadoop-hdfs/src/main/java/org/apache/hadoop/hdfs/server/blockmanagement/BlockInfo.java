/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.metadata.hdfs.entity.ReplicaBase;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Internal class for block metadata. BlockInfo class maintains for a given
 * block the {@link BlockCollection} it is part of and datanodes where the
 * replicas of the block are stored.
 */
@InterfaceAudience.Private
public class BlockInfo extends Block {
  
  public static final BlockInfo[] EMPTY_ARRAY = {};
  private static final List<Replica> EMPTY_REPLICAS_ARRAY =
      new ArrayList<Replica>();

  public static enum Finder implements FinderType<BlockInfo> {

    ByBlockIdAndINodeId,
    ByINodeId,
    ByINodeIds,
    ByMaxBlockIndexForINode,
    ByBlockIdsAndINodeIds;
    
    @Override
    public Class getType() {
      return BlockInfo.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndINodeId:
          return Annotation.PrimaryKey;
        case ByBlockIdsAndINodeIds:
          return Annotation.Batched;
        case ByMaxBlockIndexForINode:
          return Annotation.PrunedIndexScan;
        case ByINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  public static enum Order implements Comparator<BlockInfo> {
    
    ByBlockIndex() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getBlockIndex() == o2.getBlockIndex()) {
          throw new IllegalStateException(
              "A file cannot have 2 blocks with the same index. index = " +
                  o1.getBlockIndex() + " blk1_id = " + o1.getBlockId() +
                  " blk2_id = " + o2.getBlockId());
        }
        if (o1.getBlockIndex() < o2.getBlockIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByBlockId() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getBlockId() == o2.getBlockId()) {
          return 0;
        }
        if (o1.getBlockId() < o2.getBlockId()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByGenerationStamp() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getGenerationStamp() == o2.getGenerationStamp()) {
          throw new IllegalStateException(
              "A file cannot have 2 blocks with the same generation stamp");
        }
        if (o1.getGenerationStamp() < o2.getGenerationStamp()) {
          return -1;
        } else {
          return 1;
        }
      }
    };
    
    @Override
    public abstract int compare(BlockInfo o1, BlockInfo o2);
    
    public Comparator acsending() {
      return this;
    }
    
    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }

  private BlockCollection bc;
  private int blockIndex = -1;
  private long timestamp = 1;
  
  protected int inodeId = INode.NON_EXISTING_ID;
  
  public BlockInfo(Block blk, int inodeId) {
    super(blk);
    this.inodeId = inodeId;
    if (blk instanceof BlockInfo) {
      this.bc = ((BlockInfo) blk).bc;
      this.blockIndex = ((BlockInfo) blk).blockIndex;
      this.timestamp = ((BlockInfo) blk).timestamp;
      if (inodeId != ((BlockInfo) blk).inodeId) {
        throw new IllegalArgumentException("inodeId does not match");
      }
    }
  }
  
  public BlockInfo() {
    this.bc = null;
  }

  /**
   * Copy construction. This is used to convert BlockInfoUnderConstruction
   *
   * @param from
   *     BlockInfo to copy from.
   */
  protected BlockInfo(BlockInfo from) {
    super(from);
    this.bc = from.bc;
    this.blockIndex = from.blockIndex;
    this.timestamp = from.timestamp;
    this.inodeId = from.inodeId;
  }
  
  public BlockCollection getBlockCollection()
      throws StorageException, TransactionContextException {
    //Every time get block collection is called, get it from DB
    //Why? some times it happens that the inode is deleted and copy 
    //of the block is lying around is some secondary data structure ( not block_info )
    //if we call get block collection op of that copy then it should return null

    BlockCollection bc = (BlockCollection) EntityManager
        .find(INodeFile.Finder.ByINodeId, inodeId);
    this.bc = bc;
    if (bc == null) {
      this.inodeId = INode.NON_EXISTING_ID;
    }
    return bc;
  }
  
  public void setBlockCollection(BlockCollection bc)
      throws StorageException, TransactionContextException {
    this.bc = bc;
    if (bc != null) {
      setINodeId(bc.getId());
    }
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes(DatanodeManager datanodeMgr)
      throws StorageException, TransactionContextException {
    return getReplicas(datanodeMgr).size();
  }

  /**
   * Returns the Storages on which the replicas of this block are stored.
   * @param datanodeMgr
   * @return array of storages that store a replica of this block
   */
  public DatanodeStorageInfo[] getStorages(DatanodeManager datanodeMgr)
      throws StorageException, TransactionContextException {
    List<Replica> replicas = getReplicas(datanodeMgr);
    return getStorages(datanodeMgr, replicas);
  }

  /**
   * Returns the storage on the given node which stores this block, or null
   * if it can't find such a storage.
   */
  public DatanodeStorageInfo getStorageOnNode(DatanodeDescriptor node)
      throws TransactionContextException, StorageException {
    // TODO HDP_2.6 this is like a join -> should be done in SQL probably...
    DatanodeStorageInfo[] storagesOnNode = node.getStorageInfos();

    for(DatanodeStorageInfo s : storagesOnNode) {
      if (this.isReplicatedOnStorage(s)) {
        return s;
      }
    }
    return null;
  }

  List<Replica> getReplicasNoCheck()
      throws StorageException, TransactionContextException {
    List<Replica> replicas = (List<Replica>) EntityManager
        .findList(Replica.Finder.ByBlockIdAndINodeId, getBlockId(),
            getInodeId());
    if (replicas == null) {
      replicas = EMPTY_REPLICAS_ARRAY;
    } else {
      Collections.sort(replicas, Replica.Order.ByStorageId);
    }
    return replicas;
  }

  List<Replica> getReplicas(DatanodeManager datanodeMgr)
      throws StorageException, TransactionContextException {
    List<Replica> replicas = getReplicasNoCheck();
    getDatanodes(datanodeMgr, replicas);
    Collections.sort(replicas, Replica.Order.ByStorageId);
    return replicas;
  }

  
  /**
   * Adds new replica for this block.
   * @return the replica stored, or null if it is already stored on this storage
   */
  Replica addReplica(DatanodeStorageInfo storage)
      throws StorageException, TransactionContextException {

    if (isReplicatedOnDatanode(storage.getDatanodeDescriptor())) {
      return null;
    }

    Replica replica = new Replica(storage.getSid(), getBlockId(), getInodeId());
    add(replica);
    return replica;
  }

  public void removeAllReplicas()
      throws StorageException, TransactionContextException {
    for (Replica replica : getReplicasNoCheck()) {
      remove(replica);
    }
  }

  /**
   * removes a replica of this block related to storageId
   *
   * @return
   */
  Replica removeReplica(DatanodeStorageInfo storage)
      throws StorageException, TransactionContextException {
    List<Replica> replicas = getReplicasNoCheck();
    Replica replica = null;
    for (Replica r : replicas) {
      if (r.getStorageId() == storage.getSid()) {
        replica = r;
        remove(r);
        break;
      }
    }
    return replica;
  }

  /**
   * Returns true if this block has a replica on the given datanode.
   * @param dn
   * @return
   */
  boolean isReplicatedOnDatanode(DatanodeDescriptor dn)
      throws StorageException {
    DatanodeStorageInfo[] storages = dn.getStorageInfos();
    List<Integer> sids = new ArrayList<Integer>();
    for(DatanodeStorageInfo s : storages) {
      sids.add(s.getSid());
    }

    BlockInfoDataAccess da =
        (BlockInfoDataAccess) HdfsStorageFactory
            .getDataAccess(BlockInfoDataAccess.class);

    return da.existsOnAnyStorage(getBlockId(), sids);
  }

  boolean isReplicatedOnStorage(DatanodeStorageInfo storage)
      throws StorageException, TransactionContextException {
    Replica replica = EntityManager
        .find(Replica.Finder.ByBlockIdAndStorageId, getBlockId(),
            storage.getSid());

    return replica != null;
  }

  /**
   * BlockInfo represents a block that is not being constructed. In order to
   * start modifying the block, the BlockInfo should be converted to
   * {@link BlockInfoUnderConstruction}.
   *
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   *
   * @return BlockInfoUnderConstruction - an under construction block.
   */
  public BlockInfoUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets)
      throws StorageException, TransactionContextException {
    if (isComplete()) {
      return new BlockInfoUnderConstruction(this, this.getInodeId(), s,
          targets);
    }
    // the block is already under construction
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    return ucBlock;
  }
  
  public int getInodeId() {
    return inodeId;
  }
  
  public void setINodeIdNoPersistance(int id) {
    this.inodeId = id;
  }
  
  public void setINodeId(int id)
      throws StorageException, TransactionContextException {
    setINodeIdNoPersistance(id);
    save();
  }
  
  public int getBlockIndex() {
    return this.blockIndex;
  }
  
  public void setBlockIndexNoPersistance(int bindex) {
    this.blockIndex = bindex;
  }
  
  public void setBlockIndex(int bindex)
      throws StorageException, TransactionContextException {
    setBlockIndexNoPersistance(bindex);
    save();
  }
  
  public long getTimestamp() {
    return this.timestamp;
  }
  
  public void setTimestampNoPersistance(long ts) {
    this.timestamp = ts;
  }
  
  public void setTimestamp(long ts)
      throws StorageException, TransactionContextException {
    setTimestampNoPersistance(ts);
    save();
  }

  /**
   * Returns an array of storages where the replicas are stored
   */
  protected DatanodeStorageInfo[] getStorages(DatanodeManager datanodeMgr,
      List<? extends ReplicaBase> replicas) {
    int numLocations = replicas.size();
    HashSet<DatanodeStorageInfo> set = new HashSet<DatanodeStorageInfo>();
    for (int i = numLocations - 1; i >= 0; i--) {
      DatanodeStorageInfo desc = datanodeMgr.getStorage(replicas.get(i).getStorageId());
      if (desc != null) {
        set.add(desc);
      } else {
        replicas.remove(i);
      }
    }
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[set.size()];
    return set.toArray(storages);
  }

  protected DatanodeDescriptor[] getDatanodes(DatanodeManager datanodeMgr,
      List<? extends ReplicaBase> replicas) {
    int numLocations = replicas.size();
    Set<DatanodeDescriptor> datanodes = new HashSet<DatanodeDescriptor>();
    for (int i = numLocations - 1; i >= 0; i--) {
      DatanodeDescriptor dn = datanodeMgr.getDatanodeBySid(replicas.get(i)
          .getStorageId());
      if (dn != null) {
        datanodes.add(dn);
      } else {
        replicas.remove(i);
      }
    }
    DatanodeDescriptor[] locations = new DatanodeDescriptor[datanodes.size()];
    return datanodes.toArray(locations);
  }

  protected void add(Replica replica)
      throws StorageException, TransactionContextException {
    EntityManager.add(replica);
  }
  
  protected void remove(Replica replica)
      throws StorageException, TransactionContextException {
    EntityManager.remove(replica);
  }
  
  protected void save(Replica replica)
      throws StorageException, TransactionContextException {
    EntityManager.update(replica);
  }
  
  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }
  
  @Override
  public String toString() {
    return "bid= " + getBlockId() + "  State = " + getBlockUCState();
  }

  
  public void setBlockId(long bid)
      throws StorageException, TransactionContextException {
    setBlockIdNoPersistance(bid);
    save();
  }

  public void setNumBytes(long len)
      throws StorageException, TransactionContextException {
    setNumBytesNoPersistance(len);
    save();
  }

  public void setGenerationStamp(long stamp)
      throws StorageException, TransactionContextException {
    setGenerationStampNoPersistance(stamp);
    save();
  }

  public void set(long blkid, long len, long genStamp)
      throws StorageException, TransactionContextException {
    setNoPersistance(blkid, len, genStamp);
    save();
  }
  
  protected void save() throws StorageException, TransactionContextException {
    save(this);
  }

  protected void save(BlockInfo blk)
      throws StorageException, TransactionContextException {
    EntityManager.update(blk);
  }

  protected void remove() throws StorageException, TransactionContextException {
    remove(this);
  }

  protected void remove(BlockInfo blk)
      throws StorageException, TransactionContextException {
    EntityManager.remove(blk);
  }
  
  public static BlockInfo cloneBlock(BlockInfo block) throws StorageException {
    if (block instanceof BlockInfo) {
      return new BlockInfo(((BlockInfo) block),
          ((BlockInfo) block).getInodeId());
    } else if (block instanceof BlockInfoUnderConstruction) {
      return new BlockInfoUnderConstruction((BlockInfoUnderConstruction) block,
          ((BlockInfoUnderConstruction) block).getInodeId());
    } else {
      throw new StorageException("Unable to create a clone of the Block");
    }
  }
}
