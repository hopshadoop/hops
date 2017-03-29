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
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
      DatanodeDescriptor[] targets)
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
  public void setExpectedLocations(DatanodeDescriptor[] targets)
      throws StorageException, TransactionContextException {
    for (DatanodeDescriptor dn : targets) {
      addExpectedReplica(dn.getSId(), ReplicaState.RBW);
    }
  }

  /**
   * Create array of expected replica locations (as has been assigned by
   * chooseTargets()).
   */
  public DatanodeDescriptor[] getExpectedLocations(DatanodeManager datanodeMgr)
      throws StorageException, TransactionContextException {
    List<ReplicaUnderConstruction> rpls = getExpectedReplicas();
    return getDatanodes(datanodeMgr, rpls);
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
          datanodeMgr.getDatanode(replica.getStorageId());
      if (primary.isAlive) {
        primaryNodeIndex = j;
        primary.addBlockToBeRecovered(this);
        NameNode.blockStateChangeLog
            .info("BLOCK* " + this + " recovery started, primary=" + primary);
        return;
      }
    }
  }

  void addReplicaIfNotPresent(DatanodeDescriptor dn, Block block,
      ReplicaState rState)
      throws StorageException, TransactionContextException {
    for (ReplicaUnderConstruction r : getExpectedReplicas()) {
      if (r.getStorageId() == dn.getSId()) {
        return;
      }
    }
    addExpectedReplica(dn.getSId(), rState);
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

  @Override
  public void appendStringTo(StringBuilder sb) {
    super.appendStringTo(sb);
    appendUCParts(sb);
  }
  

  private void appendUCParts(StringBuilder sb) {
    //    sb.append("{blockUCState=").append(blockUCState)
    //            .append(", primaryNodeIndex=").append(primaryNodeIndex)
    //            .append(", replicas=[");
    //HOP: FIXME:
    /*Iterator<ReplicaUnderConstruction> iter = replicas.iterator();
     if (iter.hasNext()) {
     iter.next().appendStringTo(sb);
     while (iter.hasNext()) {
     sb.append(", ");
     iter.next().appendStringTo(sb);
     }
     }
     sb.append("]}");*/
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

  private ReplicaUnderConstruction addExpectedReplica(int storageId,
      ReplicaState rState)
      throws StorageException, TransactionContextException {
    if (hasExpectedReplicaIn(storageId)) {
      NameNode.blockStateChangeLog.warn(
          "BLOCK* Trying to store multiple blocks of the file on one DataNode. Returning null");
      return null;
    }
    ReplicaUnderConstruction replica =
        new ReplicaUnderConstruction(rState, storageId, getBlockId(),
            getInodeId());
    add(replica);
    return replica;
  }

  private boolean hasExpectedReplicaIn(int storageId)
      throws StorageException, TransactionContextException {
    for (ReplicaUnderConstruction replica : getExpectedReplicas()) {
      if (replica.getStorageId() == storageId) {
        return true;
      }
    }
    return false;
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

  public void setPrimaryNodeIndex(int nodeIndex)
      throws StorageException, TransactionContextException {
    setPrimaryNodeIndexNoPersistance(nodeIndex);
    save();
  }
}
