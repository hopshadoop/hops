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
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This class extends the DatanodeInfo class with ephemeral information (eg
 * health, capacity, what blocks are associated with the Datanode) that is
 * private to the Namenode, ie this class is not exposed to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDescriptor extends DatanodeInfo {
  
  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  public DecommissioningStatus decommissioningStatus =
      new DecommissioningStatus();
  
  /**
   * Block and targets pair
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeDescriptor[] targets;

    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /**
   * A BlockTargetPair queue.
   */
  private static class BlockQueue<E> {
    private final Queue<E> blockq = new LinkedList<E>();

    /**
     * Size of the queue
     */
    synchronized int size() {
      return blockq.size();
    }

    /**
     * Enqueue
     */
    synchronized boolean offer(E e) {
      return blockq.offer(e);
    }

    /**
     * Dequeue
     */
    synchronized List<E> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<E> results = new ArrayList<E>();
      for (; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * Returns <tt>true</tt> if the queue contains the specified element.
     */
    boolean contains(E e) {
      return blockq.contains(e);
    }

    synchronized void clear() {
      blockq.clear();
    }
  }

  private int sid = -1;

  public boolean isAlive = false;
  public boolean needKeyUpdate = false;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;
  
  /**
   * At startup or at any failover, the DNs in the cluster may
   * have pending block deletions from a previous incarnation
   * of the NameNode. Thus, we consider their block contents
   * stale until we have received a block report. When a DN
   * is considered stale, any replicas on it are transitively
   * considered stale. If any block has at least one stale replica,
   * then no invalidations will be processed for this block.
   * See HDFS-1972.
   */
  private boolean blockContentsStale = true;
  
  // A system administrator can tune the balancer bandwidth parameter
  // (dfs.balance.bandwidthPerSec) dynamically by calling
  // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
  // following 'bandwidth' variable gets updated with the new value for each
  // node. Once the heartbeat command is issued to update the value on the
  // specified datanode, this value will be set back to 0.
  private long bandwidth;

  /**
   * A queue of blocks to be replicated by this datanode
   */
  private BlockQueue<BlockTargetPair> replicateBlocks =
      new BlockQueue<BlockTargetPair>();
  /**
   * A queue of blocks to be recovered by this datanode
   */
  private BlockQueue<BlockInfoUnderConstruction> recoverBlocks =
      new BlockQueue<BlockInfoUnderConstruction>();

  /**
   * A set of blocks to be invalidated by this datanode
   */
  private LightWeightHashSet<Block> invalidateBlocks =
      new LightWeightHashSet<Block>();

  /* Variables for maintaining number of blocks scheduled to be written to
   * this datanode. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600 * 1000; //10min
  private int volumeFailures = 0;
  
  /**
   * Set to false after processing first block report
   */
  private boolean firstBlockReport = true;
  
  /**
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;

  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    this(nodeID, 0L, 0L, 0L, 0L, 0, 0);
  }

  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   * @param networkLocation
   *     location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, String networkLocation) {
    this(nodeID, networkLocation, 0L, 0L, 0L, 0L, 0, 0);
  }
  
  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   * @param capacity
   *     capacity of the data node
   * @param dfsUsed
   *     space used by the data node
   * @param remaining
   *     remaining capacity of the data node
   * @param bpused
   *     space used by the block pool corresponding to this namenode
   * @param xceiverCount
   *     # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, long capacity, long dfsUsed,
      long remaining, long bpused, int xceiverCount, int failedVolumes) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, bpused, xceiverCount,
        failedVolumes);
  }

  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   * @param networkLocation
   *     location of the data node in network
   * @param capacity
   *     capacity of the data node, including space used by non-dfs
   * @param dfsUsed
   *     the used space by dfs datanode
   * @param remaining
   *     remaining capacity of the data node
   * @param bpused
   *     space used by the block pool corresponding to this namenode
   * @param xceiverCount
   *     # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, String networkLocation,
      long capacity, long dfsUsed, long remaining, long bpused,
      int xceiverCount, int failedVolumes) {
    super(nodeID, networkLocation);
    updateHeartbeat(capacity, dfsUsed, remaining, bpused, xceiverCount,
        failedVolumes);
  }

  /**
   * Add datanode to the block.
   * Add block to the head of the list of blocks belonging to the data-node.
   */
  public boolean addBlock(BlockInfo b)
      throws StorageException, TransactionContextException {
    if (b.hasReplicaIn(this)) {
      return false;
    }
    b.addReplica(this, b);
    return true;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node.
   * Remove datanode from the block.
   */
  public boolean removeBlock(BlockInfo b)
      throws StorageException, TransactionContextException {
    if (b.removeReplica(this) != null) {
      return true;
    } else {
      return false;
    }
  }

  public void setSId(int sid) {
    this.sid = sid;
  }
  
  public int getSId() {
    return this.sid;
  }

  public void resetBlocks() {
    setCapacity(0);
    setRemaining(0);
    setBlockPoolUsed(0);
    setDfsUsed(0);
    setXceiverCount(0);
    this.invalidateBlocks.clear();
    this.volumeFailures = 0;
  }
  
  public void clearBlockQueues() {
    synchronized (invalidateBlocks) {
      this.invalidateBlocks.clear();
      this.recoverBlocks.clear();
      this.replicateBlocks.clear();
    }
  }

  public int numBlocks() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_REPLICAS_ON_NODE) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaDataAccess.class);
        return da.countAllReplicasForStorageId(getSId());
      }
    }.handle();
  }

  /**
   * Updates stats from datanode heartbeat.
   */
  public void updateHeartbeat(long capacity, long dfsUsed, long remaining,
      long blockPoolUsed, int xceiverCount, int volFailures) {
    setCapacity(capacity);
    setRemaining(remaining);
    setBlockPoolUsed(blockPoolUsed);
    setDfsUsed(dfsUsed);
    setXceiverCount(xceiverCount);
    setLastUpdate(Time.now());
    this.volumeFailures = volFailures;
    this.heartbeatedSinceFailover = true;
    rollBlocksScheduled(getLastUpdate());
  }

  public Iterator<BlockInfo> getBlockIterator() throws IOException {
    return getAllMachineBlockInfos().iterator();
  }

  private List<BlockInfo> getAllMachineBlockInfos() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_MACHINE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
            .getDataAccess(BlockInfoDataAccess.class);
        HdfsStorageFactory.getConnector().beginTransaction();
        List<BlockInfo> list = da.findBlockInfosByStorageId(getSId());
        HdfsStorageFactory.getConnector().commit();
        return list;
      }
    };
    return (List<BlockInfo>) findBlocksHandler.handle();
  }
  
  public Map<Long,Integer> getAllMachineReplicas() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_MACHINE_BLOCKS_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaDataAccess.class);
        return da.findBlockAndInodeIdsByStorageId(getSId());
      }
    };
    return (Map<Long,Integer>)findBlocksHandler.handle();
  }
  
    public Map<Long,Long> getAllMachineInvalidatedReplicasWithGenStamp() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_MACHINE_BLOCKS_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) HdfsStorageFactory
            .getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockAndGenStampByStorageId(getSId());
      }
    };
    return (Map<Long,Long>)findBlocksHandler.handle();
  }
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert (block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(new BlockTargetPair(block, targets));
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(BlockInfoUnderConstruction block) {
    if (recoverBlocks.contains(block)) {
      // this prevents adding the same block twice to the recovery queue
      BlockManager.LOG.info(block + " is already in the recovery queue");
      return;
    }
    recoverBlocks.offer(block);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert (blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for (Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  public List<BlockTargetPair> getReplicationCommand(int maxTransfers) {
    return replicateBlocks.poll(maxTransfers);
  }

  public BlockInfoUnderConstruction[] getLeaseRecoveryCommand(
      int maxTransfers) {
    List<BlockInfoUnderConstruction> blocks = recoverBlocks.poll(maxTransfers);
    if (blocks == null) {
      return null;
    }
    return blocks.toArray(new BlockInfoUnderConstruction[blocks.size()]);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  public Block[] getInvalidateBlocks(int maxblocks) {
    synchronized (invalidateBlocks) {
      Block[] deleteList = invalidateBlocks
          .pollToArray(new Block[Math.min(invalidateBlocks.size(), maxblocks)]);
      return deleteList.length == 0 ? null : deleteList;
    }
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }
  
  /**
   * Increments counter for number of blocks scheduled.
   */
  public void incBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /**
   * Decrements counter for number of blocks scheduled.
   */
  void decBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    }
    // its ok if both counters are zero.
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if ((now - lastBlocksScheduledRollTime) > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
  }
  
  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to use super equality as datanodes are uniquely identified
    // by DatanodeID
    return (this == obj) || super.equals(obj);
  }

  /**
   * Decommissioning status
   */
  public class DecommissioningStatus {
    private int underReplicatedBlocks;
    private int decommissionOnlyReplicas;
    private int underReplicatedInOpenFiles;
    private long startTime;
    
    synchronized void set(int underRep, int onlyRep, int underConstruction) {
      if (isDecommissionInProgress() == false) {
        return;
      }
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }

    /**
     * @return the number of under-replicated blocks
     */
    public synchronized int getUnderReplicatedBlocks() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedBlocks;
    }

    /**
     * @return the number of decommission-only replicas
     */
    public synchronized int getDecommissionOnlyReplicas() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return decommissionOnlyReplicas;
    }

    /**
     * @return the number of under-replicated blocks in open files
     */
    public synchronized int getUnderReplicatedInOpenFiles() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }

    /**
     * Set start time
     */
    public synchronized void setStartTime(long time) {
      startTime = time;
    }

    /**
     * @return start time
     */
    public synchronized long getStartTime() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return startTime;
    }
  }  // End of class DecommissioningStatus

  /**
   * Set the flag to indicate if this datanode is disallowed from communicating
   * with the namenode.
   */
  public void setDisallowed(boolean flag) {
    disallowed = flag;
  }

  /**
   * Is the datanode disallowed from communicating with the namenode?
   */
  public boolean isDisallowed() {
    return disallowed;
  }

  /**
   * @return number of failed volumes in the datanode.
   */
  public int getVolumeFailures() {
    return volumeFailures;
  }

  /**
   * @param nodeReg
   *     DatanodeID to update registration for.
   */
  @Override
  public void updateRegInfo(DatanodeID nodeReg) {
    super.updateRegInfo(nodeReg);
    firstBlockReport = true; // must re-process IBR after re-registration
  }

  /**
   * @return balancer bandwidth in bytes per second for this datanode
   */
  public long getBalancerBandwidth() {
    return this.bandwidth;
  }

  /**
   * @param bandwidth
   *     balancer bandwidth in bytes per second for this datanode
   */
  public void setBalancerBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  public boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  public void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  public void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    firstBlockReport = false;
  }
  
  boolean isFirstBlockReport() {
    return firstBlockReport;
  }

  @Override
  public String dumpDatanode() {
    StringBuilder sb = new StringBuilder(super.dumpDatanode());
    int repl = replicateBlocks.size();
    if (repl > 0) {
      sb.append(" ").append(repl).append(" blocks to be replicated;");
    }
    int inval = invalidateBlocks.size();
    if (inval > 0) {
      sb.append(" ").append(inval).append(" blocks to be invalidated;");
    }
    int recover = recoverBlocks.size();
    if (recover > 0) {
      sb.append(" ").append(recover).append(" blocks to be recovered;");
    }
    return sb.toString();
  }
}
