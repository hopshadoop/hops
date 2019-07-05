/*
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
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Represents a block that is currently being constructed.<br>
 * This is usually the last block of a file opened for write or append.
 */
public class BlockInfoContiguousUnderConstruction extends BlockInfoContiguous {
  /** Block state. See {@link BlockUCState} */

  private static final List<ReplicaUnderConstruction> EMPTY_REPLICAS_ARRAY =
      Collections.unmodifiableList(new ArrayList<ReplicaUnderConstruction>());
  /**
   * Block state. See {@link BlockUCState}
   */
  private BlockUCState blockUCState;

  /**
   * Index of the primary data node doing the recovery. Useful for log
   * messages.
   */
  private int primaryNodeIndex = -1;
  /**
   * The new generation stamp, which this block will have after the recovery
   * succeeds. Also used as a recovery id to identify the right recovery if any
   * of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * The block source to use in the event of copy-on-write truncate.
   */
  private Block truncateBlock;
  
  /**
   * Create block and set its state to {@link BlockUCState#UNDER_CONSTRUCTION}.
   */
  public BlockInfoContiguousUnderConstruction(Block blk, long inodeId) {
    this(blk, inodeId, BlockUCState.UNDER_CONSTRUCTION);
  }

  /**
   * Create a block that is currently being constructed.
   */
  private BlockInfoContiguousUnderConstruction(Block blk, long inodeId,
      BlockUCState state) {
    super(blk, inodeId);
    assert getBlockUCState() !=
        BlockUCState.COMPLETE : "BlockInfoUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
  }

  /**
   * Create a block that is currently being constructed.
   */
  public BlockInfoContiguousUnderConstruction(Block blk, long inodeId, BlockUCState state,
      DatanodeStorageInfo[] targets)
      throws StorageException, TransactionContextException {
    this(blk, inodeId, state);
    setExpectedLocations(targets);
  }

  /**
   * Convert an under construction block to a complete block.
   *
   * @return BlockInfo - a complete block.
   */
  BlockInfoContiguous convertToCompleteBlock()
      throws StorageException, TransactionContextException {
    assert getBlockUCState() !=
        BlockUCState.COMPLETE : "Trying to convert a COMPLETE block";
    complete();
    return new BlockInfoContiguous(this);
  }

  /**
   * Set expected locations
   */
  public void setExpectedLocations(DatanodeStorageInfo[] targets)
      throws StorageException, TransactionContextException {
    
    for (ReplicaUnderConstruction replicaUnderConstruction : getExpectedReplicas()) {
      EntityManager.remove(replicaUnderConstruction);
    }
    
    for (DatanodeStorageInfo storage : targets) {
      addExpectedReplica(storage, ReplicaState.RBW, this.getGenerationStamp());
    }
  }

  /**
   * Create array of expected replica locations (as has been assigned by
   * chooseTargets()).
   */
  public DatanodeStorageInfo[] getExpectedStorageLocations(DatanodeManager m)
      throws StorageException, TransactionContextException {
    List<ReplicaUnderConstruction> replicas = getExpectedReplicas();
    return super.getStorages(m, replicas);
  }

  /**
   * Get the number of expected locations
   */
  public int getNumExpectedLocations()
      throws StorageException, TransactionContextException {
    return getExpectedReplicas().size();
  }

  /**
   * Return the state of the block under construction.
   *
   * @see BlockUCState
   */
  @Override // BlockInfo
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  public void setBlockUCStateNoPersistance(BlockUCState s) {
    blockUCState = s;
  }

  /**
   * Get block recovery ID
   */
  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /** Get recover block */
  public Block getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(Block recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }
  
  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   */
  public void setGenerationStampAndVerifyReplicas(long genStamp, DatanodeManager datanodeMgr) throws StorageException, TransactionContextException {
    // Set the generation stamp for the block.
    setGenerationStamp(genStamp);
    List<ReplicaUnderConstruction> replicas = getExpectedReplicas();
    if (replicas == null)
      return;

    // Remove the replicas with wrong gen stamp.
    // The replica list is unchanged.
    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        r.getExpectedStorageLocation(datanodeMgr).removeBlock(this);
        NameNode.blockStateChangeLog.info("BLOCK* Removing stale replica "
            + "from location: {} for block {}", r.getStorageId(), r.getBlockId());
      }
    }

  }

  /**
   * Commit block's length and generation stamp as reported by the client. Set
   * block state to {@link BlockUCState#COMMITTED}.
   *
   * @param block
   *     - contains client reported block length and generation
   * @throws IOException
   *     if block ids are inconsistent.
   */
  void commitBlock(Block block, DatanodeManager datanodeMgr) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException(
          "Trying to commit inconsistent block: id = " + block.getBlockId() +
              ", expected id = " + getBlockId());
    }
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(),
            block.getGenerationStamp(), block.getCloudBucketID());
    // Sort out invalid replicas.
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp(), datanodeMgr);
  }

  /**
   * Initialize lease recovery for this block. Find the first alive data-node
   * starting from the previous primary and make it primary.
   */
  public void initializeBlockRecovery(long recoveryId,
      DatanodeManager datanodeMgr)
      throws StorageException, TransactionContextException {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    List<ReplicaUnderConstruction> replicas = getExpectedReplicas();
    setBlockRecoveryId(recoveryId);
    if (replicas.isEmpty()) {
      NameNode.blockStateChangeLog.warn(
          "BLOCK*" + " BlockInfoUnderConstruction.initLeaseRecovery:" +
              " No blocks found, lease removed.");
    }

    boolean allLiveReplicasTriedAsPrimary = true;
    for (int i = 0; i < replicas.size(); i++) {
      // Check if all replicas have been tried or not.
      ReplicaUnderConstruction replica = replicas.get(i);
      DatanodeDescriptor dn = datanodeMgr.getDatanodeBySid(replica.getStorageId());
      if (dn !=null && dn.isAlive) {
        allLiveReplicasTriedAsPrimary = (allLiveReplicasTriedAsPrimary && replicas.get(i).getChosenAsPrimary());
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      // Just set all the replicas to be chosen whether they are alive or not.
      for (int i = 0; i < replicas.size(); i++) {
        replicas.get(i).setChosenAsPrimary(false);
        update(replicas.get(i));
      }
    }
    long mostRecentLastUpdate = 0;
    ReplicaUnderConstruction primary = null;
    DatanodeDescriptor primaryDn = null;
    primaryNodeIndex = -1;
    for (int i = 0; i < replicas.size(); i++) {
      // Skip alive replicas which have been chosen for recovery.
      ReplicaUnderConstruction replica = replicas.get(i);
      DatanodeDescriptor dn = datanodeMgr.getDatanodeBySid(replica.getStorageId());
      if (!(dn!= null && dn.isAlive && !replicas.get(i).getChosenAsPrimary())) {
        continue;
      }
      if (dn.getLastUpdateMonotonic() > mostRecentLastUpdate) {
        primary = replicas.get(i);
        primaryNodeIndex = i;
        primaryDn = dn;
        mostRecentLastUpdate = primaryDn.getLastUpdateMonotonic();
      }
    }
    if (primary != null) {
      primaryDn.addBlockToBeRecovered(this);
      primary.setChosenAsPrimary(true);
      update(primary);
      NameNode.blockStateChangeLog.info(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }
  
  @Override // BlockInfo
  // BlockInfoUnderConstruction participates in maps the same way as BlockInfo
  public int hashCode() {
    return super.hashCode();
  }

  @Override // BlockInfo
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public String toString() {
    return "BlkInfoUnderConstruction " + super.toString();
  }

  private List<ReplicaUnderConstruction> getExpectedReplicas()
      throws StorageException, TransactionContextException {
    List<ReplicaUnderConstruction> replicas =
        (List<ReplicaUnderConstruction>) EntityManager
            .findList(ReplicaUnderConstruction.Finder.ByBlockIdAndINodeId,
                getBlockId(), getInodeId());
    if (replicas != null) {
      Collections.sort(replicas, ReplicaUnderConstruction.Order.ByStorageId);
    } else {
      replicas = EMPTY_REPLICAS_ARRAY;
    }
    return replicas;
  }

  protected void addExpectedReplica(
      DatanodeStorageInfo storage, ReplicaState rState, long genStamp)
      throws StorageException, TransactionContextException {
    addReplicaIfNotPresent(storage, rState, genStamp);
  }
  
  protected void addReplicaIfNotPresent(
      DatanodeStorageInfo storage, ReplicaState rState, long genStamp)
      throws StorageException, TransactionContextException {

    int sid = storage.getSid();

    HashSet<Integer> sidsOnDn = storage.getDatanodeDescriptor().getSidsOnNode();

    for (ReplicaUnderConstruction r : getExpectedReplicas()) {
      if(sidsOnDn.contains(r.getStorageId())) {
        // There is already a replica like this on this DN

        if(r.getStorageId() == sid && r.getState().equals(rState)) {
          // Nothing changed: just return the replica
        } else {
          // Update the sid and state, then return
          r.setStorageId(sid);
          r.setState(rState);
        }
        r.setGenerationStamp(genStamp);
        update(r);
        return;
      }
    }

    // Replica did not exist on this DN yet
    ReplicaUnderConstruction replica =
        new ReplicaUnderConstruction(rState, sid, getBlockId(), getInodeId(),
            HashBuckets.getInstance().getBucketForBlock(this), genStamp);
    update(replica);
  }

  public void setBlockRecoveryIdNoPersistance(long recoveryId) {
    this.blockRecoveryId = recoveryId;
  }

  public void setPrimaryNodeIndexNoPersistance(int nodeIndex) {
    this.primaryNodeIndex = nodeIndex;
  }

  public int getPrimaryNodeIndex() {
    return this.primaryNodeIndex;
  }

  private void complete() throws StorageException, TransactionContextException {
    for (ReplicaUnderConstruction rep : getExpectedReplicas()) {
      EntityManager.remove(rep);
    }
  }

  void setBlockUCState(BlockUCState s)
      throws StorageException, TransactionContextException {
    setBlockUCStateNoPersistance(s);
    save();
  }

  private void setBlockRecoveryId(long recoveryId)
      throws StorageException, TransactionContextException {
    setBlockRecoveryIdNoPersistance(recoveryId);
    save();
  }
}
