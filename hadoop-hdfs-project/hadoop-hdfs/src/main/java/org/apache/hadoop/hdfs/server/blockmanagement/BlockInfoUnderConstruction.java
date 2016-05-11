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
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
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
public class BlockInfoUnderConstruction extends BlockInfo {

  private static final List<ReplicaUnderConstruction> EMPTY_REPLICAS_ARRAY =
      Collections.unmodifiableList(new ArrayList<ReplicaUnderConstruction>());
  /**
   * Block state. See {@link BlockUCState}
   */
  private BlockUCState blockUCState;
  /**
   * A data-node responsible for block recovery.
   */
  private int primaryNodeIndex = -1;
  /**
   * The new generation stamp, which this block will have after the recovery
   * succeeds. Also used as a recovery id to identify the right recovery if any
   * of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * Create block and set its state to {@link BlockUCState#UNDER_CONSTRUCTION}.
   */
  public BlockInfoUnderConstruction(Block blk, int inodeId) {
    this(blk, inodeId, BlockUCState.UNDER_CONSTRUCTION);
  }

  /**
   * Create a block that is currently being constructed.
   */
  private BlockInfoUnderConstruction(Block blk, int inodeId,
      BlockUCState state) {
    super(blk, inodeId);
    assert getBlockUCState() !=
        BlockUCState.COMPLETE : "BlockInfoUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
  }

  /**
   * Create a block that is currently being constructed.
   */
  public BlockInfoUnderConstruction(Block blk, int inodeId, BlockUCState state,
      DatanodeStorageInfo[] targets)
      throws StorageException, TransactionContextException {
    this(blk, inodeId, state);
    setExpectedLocations(targets);
  }

  /**
   * Convert an under construction block to a complete block.
   *
   * @return BlockInfo - a complete block.
   * @throws IOException
   *     if the state of the block (the generation stamp and the
   *     length) has not been committed by the client or it does not have at
   *     least a
   *     minimal number of replicas reported from data-nodes.
   */
  BlockInfo convertToCompleteBlock()
      throws StorageException, TransactionContextException {
    assert getBlockUCState() !=
        BlockUCState.COMPLETE : "Trying to convert a COMPLETE block";
    complete();
    return new BlockInfo(this);
  }

  /**
   * Set expected locations
   */
  public void setExpectedLocations(DatanodeStorageInfo[] targets)
      throws StorageException, TransactionContextException {
    for (DatanodeStorageInfo storage : targets) {
      addExpectedReplica(storage, ReplicaState.RBW);
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

  /**
   * Commit block's length and generation stamp as reported by the client. Set
   * block state to {@link BlockUCState#COMMITTED}.
   *
   * @param block
   *     - contains client reported block length and generation
   * @throws IOException
   *     if block ids are inconsistent.
   */
  void commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException(
          "Trying to commit inconsistent block: id = " + block.getBlockId() +
              ", expected id = " + getBlockId());
    }
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
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

    int previous = primaryNodeIndex;
    for (int i = 1; i <= replicas.size(); i++) {
      int j = (previous + i) % replicas.size();
      ReplicaUnderConstruction replica = replicas.get(j);
      DatanodeDescriptor primary =
          datanodeMgr.getDatanodeBySid(replica.getStorageId());
      if (primary.isAlive) {
        primaryNodeIndex = j;
        primary.addBlockToBeRecovered(this);
        NameNode.blockStateChangeLog
            .info("BLOCK* " + this + " recovery started, primary=" + primary);
        return;
      }
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

  protected ReplicaUnderConstruction addExpectedReplica(
      DatanodeStorageInfo storage, ReplicaState rState)
      throws StorageException, TransactionContextException {

    int sid = storage.getSid();

    HashSet<Integer> sidsOnDn = storage.getDatanodeDescriptor().getSidsOnNode();

    for (ReplicaUnderConstruction r : getExpectedReplicas()) {
      if(sidsOnDn.contains(r.getStorageId())) {
        // There is already a replica like this on this DN

        if(r.getStorageId() == sid && r.getState().equals(rState)) {
          // Nothing changed: just return the replica
          return r;
        } else {
          // Update the sid and state, then return
          r.setStorageId(sid);
          r.setState(rState);
          save();
          return r;
        }
      }
    }

    // Replica did not exist on this DN yet
    ReplicaUnderConstruction replica =
        new ReplicaUnderConstruction(rState, sid, getBlockId(), getInodeId());
    add(replica);
    return replica;
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
    ReplicaUnderConstructionDataAccess da =
        (ReplicaUnderConstructionDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaUnderConstructionDataAccess.class);

    da.removeByBlockIdAndInodeId(getBlockId(), getInodeId());
  }

  public void setBlockUCState(BlockUCState s)
      throws StorageException, TransactionContextException {
    setBlockUCStateNoPersistance(s);
    save();
  }

  public void setBlockRecoveryId(long recoveryId)
      throws StorageException, TransactionContextException {
    setBlockRecoveryIdNoPersistance(recoveryId);
    save();
  }
}
