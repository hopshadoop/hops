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
import io.hops.metadata.StorageMap;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.StorageDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
  public static final Log LOG = LogFactory.getLog(DatanodeDescriptor.class);
  public static final DatanodeDescriptor[] EMPTY_ARRAY = {};
  
  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  public DecommissioningStatus decommissioningStatus =
      new DecommissioningStatus();

  /**
   * A datanode is a collection of storages. This maps storageID's to their
   * DataNodeStorageInfo
   */
  private final Map<String, DatanodeStorageInfo> storageMap =
      new HashMap<String, DatanodeStorageInfo>();

  /**
   * Block and targets pair
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeStorageInfo[] targets;

    BlockTargetPair(Block block, DatanodeStorageInfo[] targets) {
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

  public boolean isAlive = false;
  public boolean needKeyUpdate = false;
  
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
   * this storage. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600 * 1000; //10min
  private int volumeFailures = 0;
  
  /**
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;

  // HB processing can use it to tell if it is the first HB since DN restarted
  private boolean heartbeatedSinceRegistration = false;

  /**
   * The number of replication work pending before targets are determined
   */
  // TODO HDP_2.6 should we keep this volatile or store in DB? I think
  // volatile is OK
  private int PendingReplicationWithoutTargets = 0;

  /**
   * The mapping of storageIds (int) to DatanodeStorageInfo's
   * Updates here affect the storageMap in the DatanodeManager (so don't
   * change the references)
   */
  private final StorageMap globalStorageMap;

  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   */
  public DatanodeDescriptor(StorageMap storageMap, DatanodeID nodeID) {
    super(nodeID);
    this.globalStorageMap = storageMap;
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0, 0);
  }

  /**
   * DatanodeDescriptor constructor
   *
   * @param nodeID
   *     id of the data node
   * @param networkLocation
   *     location of the data node in network
   */
  public DatanodeDescriptor(StorageMap storageMap, DatanodeID nodeID, String
      networkLocation) {
    super(nodeID, networkLocation);
    this.globalStorageMap = storageMap;
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0, 0);
  }

  /**
   * Add block to the storage. Return true on success.
   */
  public boolean addBlock(String storageID, BlockInfo b)
      throws TransactionContextException, StorageException {
    DatanodeStorageInfo s = getStorageInfo(storageID);
    if(s != null) {
      return s.addBlock(b);
    }
    return false;
  }

  public DatanodeStorageInfo getStorageInfo(String storageID) {
    synchronized (storageMap) {
      return this.storageMap.get(storageID);
    }
  }

  public DatanodeStorageInfo[] getStorageInfos() {
    synchronized (storageMap) {
      final Collection<DatanodeStorageInfo> storages = storageMap.values();
      return storages.toArray(new DatanodeStorageInfo[storages.size()]);
    }
  }

  public StorageReport[] getStorageReports() {
    final DatanodeStorageInfo[] infos = getStorageInfos();
    final StorageReport[] reports = new StorageReport[infos.length];
    for(int i = 0; i < infos.length; i++) {
      reports[i] = infos[i].toStorageReport();
    }
    return reports;
  }

  boolean hasStaleStorages() {
    synchronized (storageMap) {
      for (DatanodeStorageInfo storage : storageMap.values()) {
        if (storage.areBlockContentsStale()) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Remove block from the list of blocks belonging to this data-node.
   * Remove datanode from the block.
   */
  public boolean removeBlock(BlockInfo b)
      throws StorageException, TransactionContextException {
    DatanodeStorageInfo s = b.getStorageOnNode(this);

    if (s != null) {
      return s.removeBlock(b);
    }
    return false;
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
    // TODO: do a count in SQL instead of a for loop (both for
    // atomicity and for efficiency)
    // i.e. SELECT COUNT(*) FROM blocks b, storages s
    //  WHERE s.hostid = <some id> AND s.id = b.storageID

    int blocks = 0;

    for (final DatanodeStorageInfo storage : getStorageInfos()) {
      blocks += storage.numBlocks();
    }

    return blocks;
  }

  public void updateHeartbeat(StorageReport[] reports, int xceiverCount,
      int volFailures) {
    updateHeartbeatState(reports, xceiverCount, volFailures);
    heartbeatedSinceRegistration = true;
  }

  /**
   * process datanode heartbeat or stats initialization.
   */
  public void updateHeartbeatState(StorageReport[] reports, int xceiverCount, int volFailures) {
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalBlockPoolUsed = 0;
    long totalDfsUsed = 0;
    Set<DatanodeStorageInfo> failedStorageInfos = null;

    // Decide if we should check for any missing StorageReport and mark it as
    // failed. There are different scenarios.
    // 1. When DN is running, a storage failed. Given the current DN
    //    implementation doesn't add recovered storage back to its storage list
    //    until DN restart, we can assume volFailures won't decrease
    //    during the current DN registration session.
    //    When volumeFailures == this.volumeFailures, it implies there is no
    //    state change. No need to check for failed storage. This is an
    //    optimization.
    // 2. After DN restarts, volFailures might not increase and it is possible
    //    we still have new failed storage. For example, admins reduce
    //    available storages in configuration. Another corner case
    //    is the failed volumes might change after restart; a) there
    //    is one good storage A, one restored good storage B, so there is
    //    one element in storageReports and that is A. b) A failed. c) Before
    //    DN sends HB to NN to indicate A has failed, DN restarts. d) After DN
    //    restarts, storageReports has one element which is B.
    boolean checkFailedStorages = (volFailures > this.volumeFailures) ||
        !heartbeatedSinceRegistration;

    if (checkFailedStorages) {
      LOG.info("Number of failed storage changes from "
          + this.volumeFailures + " to " + volFailures);
      failedStorageInfos = new HashSet<DatanodeStorageInfo>(
          storageMap.values());
    }

    setXceiverCount(xceiverCount);
    setLastUpdate(Time.now());
    this.volumeFailures = volFailures;
    for (StorageReport report : reports) {
      // TODO do we want to pass a storage, or just the id? If we only pass
      // the ID (which should be enough for a DB lookup), we can reduce the
      // report object to only have a storageId, instead of a full
      // DatanodeStorage object. If so, we could also do an if(storage !=
      // null) check...
      DatanodeStorageInfo storage = updateStorage(report.getStorage());
      if (checkFailedStorages) {
        failedStorageInfos.remove(storage);
      }

      storage.receivedHeartbeat(report);
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalBlockPoolUsed += report.getBlockPoolUsed();
      totalDfsUsed += report.getDfsUsed();
    }
    rollBlocksScheduled(getLastUpdate());

    // Update total metrics for the node.
    setCapacity(totalCapacity);
    setRemaining(totalRemaining);
    setBlockPoolUsed(totalBlockPoolUsed);
    setDfsUsed(totalDfsUsed);
    if (checkFailedStorages) {
      updateFailedStorage(failedStorageInfos);
    }

    if (storageMap.size() != reports.length) {
      pruneStorageMap(reports);
    }
  }

  /**
   * Remove stale storages from storageMap. We must not remove any storages
   * as long as they have associated block replicas.
   */
  private void pruneStorageMap(final StorageReport[] reports) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of storages reported in heartbeat=" + reports.length +
          "; Number of storages in storageMap=" + storageMap.size());
    }

    HashMap<String, DatanodeStorageInfo> excessStorages;

    synchronized (storageMap) {
      // Init excessStorages with all known storages.
      excessStorages = new HashMap<String, DatanodeStorageInfo>(storageMap);

      // Remove storages that the DN reported in the heartbeat.
      for (final StorageReport report : reports) {
        excessStorages.remove(report.getStorage().getStorageID());
      }

      // For each remaining storage, remove it if there are no associated
      // blocks.
      for (final DatanodeStorageInfo storageInfo : excessStorages.values()) {
        try {
          if (storageInfo.numBlocks() == 0) {
            storageMap.remove(storageInfo.getStorageID());
            LOG.info("Removed storage " + storageInfo + " from DataNode" + this);
          } else if (LOG.isDebugEnabled()) {
            // This can occur until all block reports are received.
            LOG.debug("Deferring removal of stale storage " + storageInfo +
                " with " + storageInfo.numBlocks() + " blocks");
          }
        } catch (IOException e) {
          // Skip for a bit
          e.printStackTrace();
        }
      }
    }
  }

  private void updateFailedStorage(
      Set<DatanodeStorageInfo> failedStorageInfos) {
    for (DatanodeStorageInfo storageInfo : failedStorageInfos) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        LOG.info(storageInfo + " failed.");
        storageInfo.setState(DatanodeStorage.State.FAILED);
      }
    }
  }

  public Iterator<DatanodeStorageInfo> getStorageIterator() throws IOException {
    return getAllMachineStorages().iterator();
  }

  private List<DatanodeStorageInfo> getAllMachineStorages() throws IOException {
    final String uuid = getDatanodeUuid();

    LightWeightRequestHandler findStoragesHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        StorageDataAccess storages = (StorageDataAccess) HdfsStorageFactory.getDataAccess(StorageDataAccess.class);

        HdfsStorageFactory.getConnector().beginTransaction();
        List<BlockInfo> list = storages.findByHostUuid(uuid);
        HdfsStorageFactory.getConnector().commit();

        return list;
      }
    };
    return (List<DatanodeStorageInfo>) findStoragesHandler.handle();
  }

  // TODO deal with this...
  public Iterator<BlockInfo> getBlockIterator() throws IOException {
    // We could iterate over all storages, and then over all blocks within
    // each storage?

    // For now, let's just pretend we have a single function to deal with it:
    return getAllMachineBlockInfos().iterator();
  }

  void incrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets++;
  }

  void decrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets--;
  }

  private List<BlockInfo> getAllMachineBlockInfos() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_MACHINE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfoDataAccess blocks = (BlockInfoDataAccess) HdfsStorageFactory.getDataAccess(
            BlockInfoDataAccess.class);
        HdfsStorageFactory.getConnector().beginTransaction();
        final List<Integer> sids = globalStorageMap.getSidsForDatanodeUuid(getDatanodeUuid());
        List<BlockInfo> list = blocks.findBlockInfosBySids(sids);
        HdfsStorageFactory.getConnector().commit();
        return list;
      }
    };
    return (List<BlockInfo>) findBlocksHandler.handle();
  }
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
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

  /**
   * Return the sum of remaining spaces of the specified type. If the remaining
   * space of a storage is less than minSize, it won't be counted toward the
   * sum.
   *
   * @param t The storage type. If null, the type is ignored.
   * @param minSize The minimum free space required.
   * @return the sum of remaining spaces that are bigger than minSize.
   */
  public long getRemaining(StorageType t, long minSize) {
    long remaining = 0;
    for (DatanodeStorageInfo s : getStorageInfos()) {
      if (s.getState() == DatanodeStorage.State.NORMAL &&
          (t == null || s.getStorageType() == t)) {
        long r = s.getRemaining();
        if (r >= minSize) {
          remaining += r;
        }
      }
    }
    return remaining;
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
   * to the given storage type of this datanode.
   */
  public int getBlocksScheduled(StorageType t) {
    return (int)(currApproxBlocksScheduled.get(t)
        + prevApproxBlocksScheduled.get(t));
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return (int)(currApproxBlocksScheduled.sum()
        + prevApproxBlocksScheduled.sum());
  }
  
  /** Increment the number of blocks scheduled. */
  void incrementBlocksScheduled(StorageType t) {
    currApproxBlocksScheduled.add(t, 1);
  }

  /** Decrement the number of blocks scheduled. */
  void decrementBlocksScheduled(StorageType t) {
    if (prevApproxBlocksScheduled.get(t) > 0) {
      prevApproxBlocksScheduled.subtract(t, 1);
    } else if (currApproxBlocksScheduled.get(t) > 0) {
      currApproxBlocksScheduled.subtract(t, 1);
    }
    // its ok if both counters are zero.
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if (now - lastBlocksScheduledRollTime > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled.set(currApproxBlocksScheduled);
      currApproxBlocksScheduled.reset();
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

    // must re-process IBR after re-registration
    for(DatanodeStorageInfo storage : getStorageInfos()) {
      storage.setBlockReportCount(0);
    }
    heartbeatedSinceRegistration = false;
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

  public DatanodeStorageInfo updateStorage(DatanodeStorage s) {
    synchronized (storageMap) {
      DatanodeStorageInfo storage = getStorageInfo(s.getStorageID());
      if (storage == null) {
        storage = new DatanodeStorageInfo(this, s);
        storageMap.put(s.getStorageID(), storage);
      } else if (storage.getState() != s.getState() ||
        storage.getStorageType() != s.getStorageType()) {
        // For backwards compatibility, make sure that the type and
        // state are updated. Some reports from older datanodes do
        // not include these fields so we may have assumed defaults.
        storage.updateFromStorage(s);
      }

      // Also update the list kept by the DatanodeManager
      globalStorageMap.updateStorage(storage);

      return storage;
    }
  }

  public HashSet<Integer> getSidsOnNode() {
    HashSet<Integer> sids = new HashSet<Integer>();
    for(DatanodeStorageInfo s : getStorageInfos()) {
      sids.add(s.getSid());
    }
    return sids;
  }
}
