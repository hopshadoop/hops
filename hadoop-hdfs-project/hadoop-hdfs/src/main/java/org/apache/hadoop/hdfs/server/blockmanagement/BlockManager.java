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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.exception.TransientStorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.blockmanagement.ExcessReplicasMap;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.security.token.block.NameNodeBlockTokenSecretManager;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import io.hops.util.Slicer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.BlockReportBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReportBlockState;
import org.apache.hadoop.hdfs.server.protocol.BlockReportBucket;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.hops.transaction.lock.LockFactory.BLK;
import java.util.concurrent.Callable;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 */
@InterfaceAudience.Private
public class BlockManager {

  static final Log LOG = LogFactory.getLog(BlockManager.class);
  static final Log blockLog = NameNode.blockStateChangeLog;

  /**
   * Default load factor of map
   */
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;

  private final Namesystem namesystem;

  private final DatanodeManager datanodeManager;
  private final HeartbeatManager heartbeatManager;
  private final NameNodeBlockTokenSecretManager blockTokenSecretManager;

  private volatile long pendingReplicationBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long underReplicatedBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L;
  private AtomicLong excessBlocksCount = new AtomicLong(0L);
  private AtomicLong postponedMisreplicatedBlocksCount = new AtomicLong(0L);

  /**
   * Used by metrics
   */
  public long getPendingReplicationBlocksCount() {
    return pendingReplicationBlocksCount;
  }

  /**
   * Used by metrics
   */
  public long getUnderReplicatedBlocksCount() {
    return underReplicatedBlocksCount;
  }

  /**
   * Used by metrics
   */
  public long getCorruptReplicaBlocksCount() {
    return corruptReplicaBlocksCount;
  }

  /**
   * Used by metrics
   */
  public long getScheduledReplicationBlocksCount() {
    return scheduledReplicationBlocksCount;
  }

  /**
   * Used by metrics
   */
  public long getPendingDeletionBlocksCount() throws IOException {
    return invalidateBlocks.numBlocks();
  }

  /**
   * Used by metrics
   */
  public long getExcessBlocksCount() {
    return excessBlocksCount.get();
  }

  /**
   * Used by metrics
   */
  public long getPostponedMisreplicatedBlocksCount() {
    return postponedMisreplicatedBlocksCount.get();
  }

  /**
   * replicationRecheckInterval is how often namenode checks for new replication
   * work
   */
  private final long replicationRecheckInterval;

  /**
   * Mapping: Block -> { BlockCollection, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /**
   * Replication thread.
   */
  final Daemon replicationThread = new Daemon(new ReplicationMonitor());

  /**
   * Store blocks -> datanodedescriptor(s) map of corrupt replicas
   */
  final CorruptReplicasMap corruptReplicas;

  /**
   * Blocks to be invalidated.
   */
  private final InvalidateBlocks invalidateBlocks;

  /**
   * After a failover, over-replicated blocks may not be handled
   * until all of the replicas have done a block report to the
   * new active. This is to make sure that this NameNode has been
   * notified of all block deletions that might have been pending
   * when the failover happened.
   */
  private final Set<Block> postponedMisreplicatedBlocks = Sets.newConcurrentHashSet();

  /**
   * Maps a StorageID to the set of blocks that are "extra" for this
   * DataNode. We'll eventually remove these extras.
   */
  public final ExcessReplicasMap excessReplicateMap;

  /**
   * Store set of Blocks that need to be replicated 1 or more times.
   * We also store pending replication-orders.
   */
  public final UnderReplicatedBlocks neededReplications =
      new UnderReplicatedBlocks();

  @VisibleForTesting
  final PendingReplicationBlocks pendingReplications;

  /**
   * The maximum number of replicas allowed for a block
   */
  public final short maxReplication;
  /**
   * The maximum number of outgoing replication streams a given node should
   * have
   * at one time considering all but the highest priority replications needed.
   */
  int maxReplicationStreams;
  /**
   * The maximum number of outgoing replication streams a given node should
   * have
   * at one time.
   */
  int replicationStreamsHardLimit;
  /**
   * Minimum copies needed or else write is disallowed
   */
  public final short minReplication;
  /**
   * Default number of replicas
   */
  public final int defaultReplication;
  /**
   * value returned by MAX_CORRUPT_FILES_RETURNED
   */
  final int maxCorruptFilesReturned;

  final float blocksInvalidateWorkPct;
  final int blocksReplWorkMultiplier;

  /**
   * variable to enable check for enough racks
   */
  final boolean shouldCheckForEnoughRacks;

  // whether or not to issue block encryption keys.
  final boolean encryptDataTransfer;

  /**
   * for block replicas placement
   */
  private BlockPlacementPolicy blockplacement;

  /**
   * Number of blocks to process at one batch
   */
  private final int processReportBatchSize;
  /**
   * Number of files to process at one batch
   */
  private final int processMisReplicatedBatchSize;
  /**
   * Number of batches to be processed by this namenode at one time
   */
  private final int processMisReplicatedNoOfBatchs;

  public BlockManager(final Namesystem namesystem, final FSClusterStats stats,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    int numBuckets = conf.getInt(DFSConfigKeys.DFS_NUM_BUCKETS_KEY,
        DFSConfigKeys.DFS_NUM_BUCKETS_DEFAULT);
    HashBuckets.initialize(numBuckets);
    
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    corruptReplicas = new CorruptReplicasMap(datanodeManager);
    heartbeatManager = datanodeManager.getHeartbeatManager();
    invalidateBlocks = new InvalidateBlocks(datanodeManager);
    excessReplicateMap = new ExcessReplicasMap(datanodeManager);

    blocksMap = new BlocksMap(datanodeManager);
    blockplacement = BlockPlacementPolicy
        .getInstance(conf, stats, datanodeManager.getNetworkTopology());
    pendingReplications = new PendingReplicationBlocks(conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) *
        1000L);

    blockTokenSecretManager = createBlockTokenSecretManager(conf);

    this.maxCorruptFilesReturned =
        conf.getInt(DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
            DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
        DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0) {
      throw new IOException("Unexpected configuration parameters: " +
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY + " = " + minR +
          " <= 0");
    }
    if (maxR > Short.MAX_VALUE) {
      throw new IOException("Unexpected configuration parameters: " +
          DFSConfigKeys.DFS_REPLICATION_MAX_KEY + " = " + maxR + " > " +
          Short.MAX_VALUE);
    }
    if (minR > maxR) {
      throw new IOException("Unexpected configuration parameters: " +
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY + " = " + minR +
          " > " + DFSConfigKeys.DFS_REPLICATION_MAX_KEY + " = " + maxR);
    }
    this.minReplication = (short) minR;
    this.maxReplication = (short) maxR;

    this.maxReplicationStreams =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.replicationStreamsHardLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.shouldCheckForEnoughRacks =
        conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null ?
            false : true;

    this.blocksInvalidateWorkPct =
        DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

    this.replicationRecheckInterval =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;

    this.encryptDataTransfer =
        conf.getBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY,
            DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);

    this.processReportBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE_DEFAULT);

    this.processMisReplicatedBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE_DEFAULT);

    this.processMisReplicatedNoOfBatchs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS,
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS_DEFAULT);

    LOG.info("defaultReplication         = " + defaultReplication);
    LOG.info("maxReplication             = " + maxReplication);
    LOG.info("minReplication             = " + minReplication);
    LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
    LOG.info("shouldCheckForEnoughRacks  = " + shouldCheckForEnoughRacks);
    LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
    LOG.info("encryptDataTransfer        = " + encryptDataTransfer);
    LOG.info("misReplicatedBatchSize     = " + processMisReplicatedBatchSize);
    LOG.info("misReplicatedNoOfBatchs     = " + processMisReplicatedNoOfBatchs);
  }

  private NameNodeBlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled =
        conf.getBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
            DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

    if (!isEnabled) {
      return null;
    }

    final long updateMin =
        conf.getLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY,
            DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT);
    final long lifetimeMin =
        conf.getLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY,
            DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT);
    final String encryptionAlgorithm =
        conf.get(DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY + "=" +
        updateMin + " min(s), " +
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY + "=" + lifetimeMin +
        " min(s), " + DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY + "=" +
        encryptionAlgorithm);

    return new NameNodeBlockTokenSecretManager(updateMin * 60 * 1000L,
        lifetimeMin * 60 * 1000L, null, encryptionAlgorithm, namesystem);
  }

  public void setBlockPoolId(String blockPoolId) {
    if (isBlockTokenEnabled()) {
      blockTokenSecretManager.setBlockPoolId(blockPoolId);
    }
  }

  /**
   * get the BlockTokenSecretManager
   */
  @VisibleForTesting
  public BlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenSecretManager;
  }

  private boolean isBlockTokenEnabled() {
    return blockTokenSecretManager != null;
  }

  /**
   * Should the access keys be updated?
   */
  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled() ?
        blockTokenSecretManager.updateKeys(updateTime) : false;
  }

  public void activate(Configuration conf) throws IOException {
    pendingReplications.start();
    datanodeManager.activate(conf);
    this.replicationThread.start();
    if (isBlockTokenEnabled()) {
      this.blockTokenSecretManager.generateKeysIfNeeded();
    }
  }

  public void close() {
    try {
      if (replicationThread != null) {
        replicationThread.interrupt();
        replicationThread.join(3000);
      }
    } catch (InterruptedException ie) {
    }
    datanodeManager.close();
    pendingReplications.stop();
    blocksMap.close();
  }

  /**
   * @return the datanodeManager
   */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  /**
   * @return the BlockPlacementPolicy
   */
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    return blockplacement;
  }

  /**
   * Set BlockPlacementPolicy
   */
  public void setBlockPlacementPolicy(BlockPlacementPolicy newpolicy) {
    if (newpolicy == null) {
      throw new HadoopIllegalArgumentException("newpolicy == null");
    }
    this.blockplacement = newpolicy;
  }

  /**
   * Dump the metadata for the given block in a human-readable
   * form.
   */
  private void dumpBlockMeta(Block block, PrintWriter out)
      throws StorageException, TransactionContextException {
    List<DatanodeDescriptor> containingNodes =
        new ArrayList<>();
    List<DatanodeDescriptor> containingLiveReplicasNodes =
        new ArrayList<>();

    NumberReplicas numReplicas = new NumberReplicas();
    // source node returned is not used
    chooseSourceDatanode(block, containingNodes, containingLiveReplicasNodes,
        numReplicas, UnderReplicatedBlocks.LEVEL);
    assert containingLiveReplicasNodes.size() == numReplicas.liveReplicas();
    int usableReplicas =
        numReplicas.liveReplicas() + numReplicas.decommissionedReplicas();

    if (block instanceof BlockInfo) {
      String fileName = ((BlockInfo) block).getBlockCollection().getName();
      out.print(fileName + ": ");
    }
    // l: == live:, d: == decommissioned c: == corrupt e: == excess
    out.print(block + ((usableReplicas > 0) ? "" : " MISSING") +
        " (replicas:" +
        " l: " + numReplicas.liveReplicas() +
        " d: " + numReplicas.decommissionedReplicas() +
        " c: " + numReplicas.corruptReplicas() +
        " e: " + numReplicas.excessReplicas() + ") ");

    Collection<DatanodeDescriptor> corruptNodes =
        corruptReplicas.getNodes(getBlockInfo(block));

    for (DatanodeDescriptor node : blocksMap.nodeList(block)){
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
        state = "(corrupt)";
      } else if (node.isDecommissioned() || node.isDecommissionInProgress()) {
        state = "(decommissioned)";
      }

      if (node.areBlockContentsStale()) {
        state += " (block deletions maybe out of date)";
      }
      out.print(" " + node + state + " : ");
    }
    out.println("");
  }

  /**
   * @return maxReplicationStreams
   */
  public int getMaxReplicationStreams() {
    return maxReplicationStreams;
  }

  /**
   * @param block
   * @return true if the block has minimum replicas
   */
  public boolean checkMinReplication(Block block)
      throws StorageException, TransactionContextException {
    return (countNodes(block).liveReplicas() >= minReplication);
  }

  /**
   * Commit a block of a file
   *
   * @param block
   *     block to be committed
   * @param commitBlock
   *     - contains client reported block length and generation
   * @return true if the block is changed to committed state.
   * @throws IOException
   *     if the block does not have at least a minimal number
   *     of replicas reported from data-nodes.
   */
  private static boolean commitBlock(final BlockInfoUnderConstruction block,
      final Block commitBlock) throws IOException, StorageException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED) {
      return false;
    }
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
        "commitBlock length is less than the stored one " +
            commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    block.commitBlock(commitBlock);
    return true;
  }

  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum replication requirement
   *
   * @param bc
   *     block collection
   * @param commitBlock
   *     - contains client reported block length and generation
   * @return true if the last block is changed to committed state.
   * @throws IOException
   *     if the block does not have at least a minimal number
   *     of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(MutableBlockCollection bc,
      Block commitBlock) throws IOException, StorageException {

    if (commitBlock == null) {
      return false; // not committing, this is a block allocation retry
    }
    BlockInfo lastBlock = bc.getLastBlock();
    if (lastBlock == null) {
      return false; // no blocks in file yet
    }
    if (lastBlock.isComplete()) {
      return false; // already completed (e.g. by syncBlock)
    }

    final boolean b =
        commitBlock((BlockInfoUnderConstruction) lastBlock, commitBlock);
    LOG.debug(
        "commitOrCompleteLastBlock. Commited Block " + lastBlock.getBlockId());
    if (countNodes(lastBlock).liveReplicas() >= minReplication) {
      completeBlock(bc, lastBlock.getBlockIndex(), false);
      LOG.debug("commitOrCompleteLastBlock. Completed Block " +
          lastBlock.getBlockId());
    } else {
      LOG.debug("commitOrCompleteLastBlock. Completed FAILED. Block " +
          lastBlock.getBlockId());
    }
    return b;
  }

  /**
   * Convert a specified block of the file to a complete block.
   *
   * @param bc
   *     file
   * @param blkIndex
   *     block index in the file
   * @throws IOException
   *     if the block does not have at least a minimal number
   *     of replicas reported from data-nodes.
   */
  private BlockInfo completeBlock(final MutableBlockCollection bc,
      final int blkIndex, boolean force) throws IOException, StorageException {
    if (blkIndex < 0) {
      return null;
    }
    BlockInfo curBlock = bc.getBlock(blkIndex);
    if (curBlock.isComplete()) {
      return curBlock;
    }
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes(datanodeManager);
    if (!force && numNodes < minReplication) {
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    }
    if (!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    }
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    // replace penultimate block in file
    bc.setBlock(blkIndex, completeBlock);

    // Since safe-mode only counts complete blocks, and we now have
    // one more complete block, we need to adjust the total up, and
    // also count it as safe, if we have at least the minimum replica
    // count. (We may not have the minimum replica count yet if this is
    // a "forced" completion when a file is getting closed by an
    // OP_CLOSE edit on the standby).
    namesystem.adjustSafeModeBlockTotals(0, 1);
    namesystem.incrementSafeBlockCount(curBlock);

    return completeBlock;
  }

  private BlockInfo completeBlock(final MutableBlockCollection bc,
      final BlockInfo block, boolean force)
      throws IOException, StorageException {
    BlockInfo blk = bc.getBlock(block.getBlockIndex());
    if(blk == block){
      return completeBlock(bc, blk.getBlockIndex(), force);
    }
    return block;
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  public BlockInfo forceCompleteBlock(final MutableBlockCollection bc,
      final BlockInfoUnderConstruction block)
      throws IOException, StorageException {
    block.commitBlock(block);
    return completeBlock(bc, block, true);
  }


  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param bc
   *     file
   * @return the last block locations if the block is partial or null otherwise
   */
  public LocatedBlock convertLastBlockToUnderConstruction(
      MutableBlockCollection bc) throws IOException, StorageException {
    BlockInfo oldBlock = bc.getLastBlock();
    if (oldBlock == null ||
        bc.getPreferredBlockSize() == oldBlock.getNumBytes()) {
      return null;
    }
    assert oldBlock ==
        getStoredBlock(oldBlock) : "last block of the file is not in blocksMap";

    DatanodeDescriptor[] targets = getNodes(oldBlock);

    BlockInfoUnderConstruction ucBlock = bc.setLastBlock(oldBlock, targets);

    // Remove block from replication queue.
    NumberReplicas replicas = countNodes(ucBlock);
    neededReplications.remove(ucBlock, replicas.liveReplicas(),
        replicas.decommissionedReplicas(), getReplication(ucBlock));
    pendingReplications.remove(ucBlock);

    // remove this block from the list of pending blocks to be deleted.
    for (DatanodeDescriptor dd : targets) {
      String datanodeId = dd.getStorageID();
      invalidateBlocks.remove(datanodeId, oldBlock);
    }

    // Adjust safe-mode totals, since under-construction blocks don't
    // count in safe-mode.
    namesystem.adjustSafeModeBlockTotals(
        // decrement safe if we had enough
        targets.length >= minReplication ? -1 : 0,
        // always decrement total blocks
        -1);

    final long fileLength = bc.computeContentSummary().getLength();
    final long pos = fileLength - ucBlock.getNumBytes();
    return createLocatedBlock(ucBlock, pos, AccessMode.WRITE);
  }

  /**
   * Get all valid locations of the block
   */
  private List<String> getValidLocations(BlockInfo block)
      throws StorageException, TransactionContextException {
    ArrayList<String> machineSet =
        new ArrayList<>(blocksMap.numNodes(block));
    for (DatanodeDescriptor node : blocksMap.nodeList(block)){
      String storageID = node.getStorageID();
      // filter invalidate replicas
      if (!invalidateBlocks.contains(storageID, block)) {
        machineSet.add(storageID);
      }
    }
    return machineSet;
  }

  private List<LocatedBlock> createLocatedBlockList(final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException, StorageException {
    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
    {
      return Collections.<LocatedBlock>emptyList();
    }

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff && curBlk < blocks.length &&
        results.size() < nrBlocksToReturn);
    return results;
  }

  private List<LocatedBlock> createPhantomLocatedBlockList(INodeFile file, final byte[] data,
      final AccessMode mode) throws IOException, StorageException {
    List<LocatedBlock> results = new ArrayList<>(1);
    BlockInfo fakeBlk = new BlockInfo();
    fakeBlk.setBlockIdNoPersistance(-file.getId());
    fakeBlk.setINodeIdNoPersistance(-file.getId());
    fakeBlk.setBlockIndexNoPersistance(0);
    fakeBlk.setNumBytesNoPersistance(data.length);
    fakeBlk.setTimestampNoPersistance(file.getModificationTime());

    final ExtendedBlock eb =
        new ExtendedBlock(namesystem.getBlockPoolId(),fakeBlk);
    // create fake DatanodeInfos pointing to NameNodes
    /*DatanodeID phantomDatanodID = new DatanodeID(
        namesystem.getNameNode().getServiceRpcAddress().getAddress().getHostAddress(),
        namesystem.getNameNode().getServiceRpcAddress().getAddress().getCanonicalHostName(),
        namesystem.getBlockPoolId(),
        DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    DatanodeInfo phantomDatanode = new DatanodeInfo(phantomDatanodID);
    phantomDatanode.setPhantomDatanode(true);
    DatanodeInfo[] machines = new DatanodeInfo[1];
    machines[0] = phantomDatanode;
    */

    DatanodeInfo randomDatanode =  datanodeManager.getRandomDN();
    DatanodeInfo[] machines = new DatanodeInfo[1];
    if(randomDatanode != null){
      machines[0] = randomDatanode;
    }
    else{
      DatanodeID phantomDatanodID = new DatanodeID(
              namesystem.getNameNode().getServiceRpcAddress().getAddress().getHostAddress(),
              namesystem.getNameNode().getServiceRpcAddress().getAddress().getCanonicalHostName(),
              namesystem.getBlockPoolId(),
              DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
              DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
              DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
      DatanodeInfo phantomDatanode = new DatanodeInfo(phantomDatanodID);
      machines[0] = phantomDatanode;
    }

    LocatedBlock locatedBlock  = new LocatedBlock(eb, machines, 0, false);
    locatedBlock.setData(data);
    results.add(locatedBlock);
    return results;
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos,
      final BlockTokenSecretManager.AccessMode mode)
      throws IOException, StorageException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
    }
    return lb;
  }

  /**
   * @return a LocatedBlock for the given block
   */
  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos)
      throws IOException, StorageException {
    if (blk instanceof BlockInfoUnderConstruction) {
      if (blk.isComplete()) {
        throw new IOException(
            "blk instanceof BlockInfoUnderConstruction && blk.isComplete()" +
                ", blk=" + blk);
      }
      final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction) blk;
      final DatanodeDescriptor[] locations =
          uc.getExpectedLocations(datanodeManager);
      final ExtendedBlock eb =
          new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return new LocatedBlock(eb, locations, pos, false);
    }

    // get block locations
    final int numCorruptNodes = countNodes(blk).corruptReplicas();
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      LOG.warn("Inconsistent number of corrupt replicas for " + blk +
          " blockMap has " + numCorruptNodes +
          " but corrupt replicas map has " + numCorruptReplicas);
    }

    final int numNodes = blocksMap.numNodes(blk);
    final boolean isCorrupt = numCorruptNodes == numNodes;
    final int numMachines = isCorrupt ? numNodes : numNodes - numCorruptNodes;
    final DatanodeDescriptor[] machines = new DatanodeDescriptor[numMachines];
    int j = 0;
    if (numMachines > 0) {
      for (final DatanodeDescriptor d : blocksMap.nodeList(blk)){
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!isCorrupt && !replicaCorrupt)) {
          machines[j++] = d;
        }
      }
    }
    assert j == machines.length : "isCorrupt: " + isCorrupt +
        " numMachines: " + numMachines +
        " numNodes: " + numNodes +
        " numCorrupt: " + numCorruptNodes +
        " numCorruptRepls: " + numCorruptReplicas;
    final ExtendedBlock eb =
        new ExtendedBlock(namesystem.getBlockPoolId(), blk);
    return new LocatedBlock(eb, machines, pos, isCorrupt);
  }
  /**
   * Create a PhantomLocatedBlocks.
   */
  public LocatedBlocks createPhantomLocatedBlocks(INodeFile file, byte[] data,
      final boolean isFileUnderConstruction,
      final boolean needBlockToken)
      throws IOException, StorageException {
    if (needBlockToken == true) {
      new IOException("Block Tokens are not currently supported for files stored in the database");
    }
    final AccessMode mode = needBlockToken ? AccessMode.READ : null;
    final List<LocatedBlock> locatedblocks =
        createPhantomLocatedBlockList(file, data, mode);

    return new LocatedBlocks(data.length,
        isFileUnderConstruction, locatedblocks, null, false/*last block is not complete*/);
  }

  /**
   * Create a LocatedBlocks.
   */
  public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken)
      throws IOException, StorageException {
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) {
      return new LocatedBlocks(0, isFileUnderConstruction,
          Collections.<LocatedBlock>emptyList(), null, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken ? AccessMode.READ : null;
      final List<LocatedBlock> locatedblocks =
          createLocatedBlockList(blocks, offset, length, Integer.MAX_VALUE,
              mode);

      final BlockInfo last = blocks[blocks.length - 1];
      final long lastPos = last.isComplete() ?
          fileSizeExcludeBlocksUnderConstruction - last.getNumBytes() :
          fileSizeExcludeBlocksUnderConstruction;
      final LocatedBlock lastlb = createLocatedBlock(last, lastPos, mode);
      return new LocatedBlocks(fileSizeExcludeBlocksUnderConstruction,
          isFileUnderConstruction, locatedblocks, lastlb, last.isComplete());
    }
  }

  /**
   * @return current access keys.
   */
  public ExportedBlockKeys getBlockKeys() throws IOException {
    return isBlockTokenEnabled() ? blockTokenSecretManager.exportKeys() :
        ExportedBlockKeys.DUMMY_KEYS;
  }

  /**
   * Generate a block token for the located block.
   */
  public void setBlockToken(final LocatedBlock b,
      final BlockTokenSecretManager.AccessMode mode) throws IOException {
    if (isBlockTokenEnabled()) {
      b.setBlockToken(blockTokenSecretManager
          .generateToken(b.getBlock(), EnumSet.of(mode)));
    }
  }

  void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
      final DatanodeDescriptor nodeinfo) throws IOException {
    // check access key update
    if (isBlockTokenEnabled() && nodeinfo.needKeyUpdate) {
      cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
      nodeinfo.needKeyUpdate = false;
    }
  }

  public DataEncryptionKey generateDataEncryptionKey() throws IOException {
    if (isBlockTokenEnabled() && encryptDataTransfer) {
      return blockTokenSecretManager.generateDataEncryptionKey();
    } else {
      return null;
    }
  }

  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    return replication < minReplication ? minReplication :
        replication > maxReplication ? maxReplication : replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
  public void verifyReplication(String src, short replication,
      String clientName) throws IOException {

    if (replication >= minReplication && replication <= maxReplication) {
      //common case. avoid building 'text'
      return;
    }

    String text = "file " + src +
        ((clientName != null) ? " on client " + clientName : "") + ".\n" +
        "Requested replication " + replication;

    if (replication > maxReplication) {
      throw new IOException(text + " exceeds maximum " + maxReplication);
    }

    if (replication < minReplication) {
      throw new IOException(text + " is less than the required minimum " +
          minReplication);
    }
  }

  /**
   * // used in the namenode protocol
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   *
   * @param datanode
   *     on which blocks are located
   * @param size
   *     total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeID datanode, long size
      // used in the namenode protocol
  ) throws IOException {
    namesystem.checkSuperuserPrivilege();
    return getBlocksWithLocations(datanode, size);
  }

  /**
   * Get all blocks with location information from a datanode.
   */
  private BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size) throws UnregisteredNodeException, IOException {
    final DatanodeDescriptor node = getDatanodeManager().getDatanode(datanode);
    if (node == null) {
      blockLog.warn(
          "BLOCK* getBlocks: " + "Asking for blocks from an unrecorded node " +
              datanode);
      throw new HadoopIllegalArgumentException(
          "Datanode " + datanode + " not found.");
    }

    int numBlocks = node.numBlocks();
    if (numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfo> iter = node.getBlockIterator();
    int startBlock =
        DFSUtil.getRandom().nextInt(numBlocks); // starting from a random block
    // skip blocks
    for (int i = 0; i < startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<>();
    long totalSize = 0;
    BlockInfo curBlock;
    while (totalSize < size && iter.hasNext()) {
      curBlock = iter.next();
      if (!curBlock.isComplete()) {
        continue;
      }
      totalSize += addBlock(curBlock, results);
    }
    if (totalSize < size) {
      iter = node.getBlockIterator(); // start from the beginning
      for (int i = 0; i < startBlock && totalSize < size; i++) {
        curBlock = iter.next();
        if (!curBlock.isComplete()) {
          continue;
        }
        totalSize += addBlock(curBlock, results);
      }
    }

    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
  }


  /**
   * Remove the blocks associated to the given datanode.
   */
  void datanodeRemoved(final DatanodeDescriptor node)
      throws IOException {
    final Iterator<BlockInfo> it = node.getBlockIterator();
    while (it.hasNext()) {
      removeStoredBlockTx(it.next().getBlockId(), node);
    }

    node.resetBlocks();
    invalidateBlocks.remove(node.getStorageID());

    // If the DN hasn't block-reported since the most recent
    // failover, then we may have been holding up on processing
    // over-replicated blocks because of it. But we can now
    // process those blocks.
    if (node.areBlockContentsStale()) {
      rescanPostponedMisreplicatedBlocks();
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block, final DatanodeInfo datanode)
      throws StorageException, TransactionContextException {
    BlockInfo temp = getBlockInfo(block);
    invalidateBlocks.add(temp, datanode, true);
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(Block b)
      throws StorageException, TransactionContextException {
    StringBuilder datanodes = new StringBuilder();
    for (DatanodeDescriptor node : blocksMap.nodeList(b)){
      BlockInfo temp = getBlockInfo(b);
      invalidateBlocks.add(temp, node, false);
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.info("BLOCK* addToInvalidates: " + b + " " + datanodes);
    }
  }

  /**
   * Mark the block belonging to datanode as corrupt
   *
   * @param blk
   *     Block to be marked as corrupt
   * @param dn
   *     Datanode which holds the corrupt replica
   * @param reason
   *     a textual reason why the block should be marked corrupt,
   *     for
   *     logging purposes
   */
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, final String reason) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.FIND_AND_MARK_BLOCKS_AS_CORRUPT) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        Block b = blk.getLocalBlock();
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UR, BLK.UC,
                BLK.IV));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        final BlockInfo storedBlock = getStoredBlock(blk.getLocalBlock());
        if (storedBlock == null) {
          // Check if the replica is in the blockMap, if not
          // ignore the request for now. This could happen when BlockScanner
          // thread of Datanode reports bad block before Block reports are sent
          // by the Datanode on startup
          blockLog
              .info("BLOCK* findAndMarkBlockAsCorrupt: " + blk + " not found");
          return null;
        }
        markBlockAsCorrupt(new BlockToMarkCorrupt(storedBlock, reason), dn);
        return null;
      }
    }.handle(namesystem);
  }

  private void markBlockAsCorrupt(BlockToMarkCorrupt b, DatanodeInfo dn)
      throws IOException, StorageException {
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException(
          "Cannot mark " + b + " as corrupt because datanode " + dn +
              " does not exist");
    }

    BlockCollection bc = b.corrupted.getBlockCollection();
    if (bc == null) {
      blockLog.info("BLOCK markBlockAsCorrupt: " + b +
          " cannot be marked as corrupt as it does not belong to any file");
      addToInvalidates(b.corrupted, node);
      return;
    }

    // Add replica to the data-node if it is not already there
    node.addBlock(b.stored);

    // Add this replica to corruptReplicas Map
    corruptReplicas.addToCorruptReplicasMap(b.corrupted, node, b.reason);
    NumberReplicas numberReplicas = countNodes(b.stored);
    if (numberReplicas.liveReplicas() >= bc.getBlockReplication()) {
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(b, node);
    } else if (namesystem.isPopulatingReplQueues()) {
      // add the block to neededReplication
      updateNeededReplications(b.stored, -1, 0);
    }

    FSNamesystem fsNamesystem = (FSNamesystem) namesystem;
    if (!fsNamesystem.isErasureCodingEnabled()) {
      return;
    }

    if (numberReplicas.liveReplicas() == 0) {
      EncodingStatus status =
          EntityManager.find(EncodingStatus.Finder.ByInodeId, bc.getId());
      if (status != null) {
        if (status.isCorrupt() == false) {
          status.setStatus(EncodingStatus.Status.REPAIR_REQUESTED);
          status.setStatusModificationTime(System.currentTimeMillis());
        }
        status.setLostBlocks(status.getLostBlocks() + 1);
        EntityManager.update(status);
      } else {
        status = EntityManager
            .find(EncodingStatus.Finder.ByParityInodeId, bc.getId());
        if (status != null) {
          if (status.isParityCorrupt() == false) {
            status.setParityStatus(
                EncodingStatus.ParityStatus.REPAIR_REQUESTED);
            status.setParityStatusModificationTime(System.currentTimeMillis());
          }
          status.setLostParityBlocks(status.getLostParityBlocks() + 1);
          EntityManager.update(status);
          LOG.info(
              "markBlockAsCorrupt updated parity status to repair requested");
        }
      }
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn)
      throws IOException, StorageException {
    blockLog.info("BLOCK* invalidateBlock: " + b + " on " + dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException(
          "Cannot invalidate " + b + " because datanode " + dn +
              " does not exist.");
    }

    // Check how many copies we have of the block
    NumberReplicas nr = countNodes(b.stored);
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.info("BLOCK* invalidateBlocks: postponing " +
          "invalidation of " + b + " on " + dn + " because " +
          nr.replicasOnStaleNodes() + " replica(s) are located on nodes " +
          "with potentially out-of-date block reports");
      postponeBlock(b.corrupted);

    } else if (nr.liveReplicas() >= 1) {
      // If we have at least one copy on a live node, then we can delete it.
      addToInvalidates(b.corrupted, dn);
      removeStoredBlock(b.stored, node);
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* invalidateBlocks: " + b + " on " + dn +
            " listed for deletion.");
      }
    } else {
      blockLog.info("BLOCK* invalidateBlocks: " + b + " on " + dn +
          " is the only copy and was not deleted");
    }
  }


  private void postponeBlock(Block blk) {
    if (postponedMisreplicatedBlocks.add(blk)) {
      postponedMisreplicatedBlocksCount.incrementAndGet();
    }
  }


  void updateState() throws IOException {
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /**
   * Return number of under-replicated but not missing blocks
   */
  public int getUnderReplicatedNotMissingBlocks() throws IOException {
    return neededReplications.getUnderReplicatedBlockCount();
  }

  /**
   * Schedule blocks for deletion at datanodes
   *
   * @param nodesToProcess
   *     number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) throws IOException {
    final List<String> nodes = invalidateBlocks.getStorageIDs();
    Collections.shuffle(nodes);

    nodesToProcess = Math.min(nodes.size(), nodesToProcess);

    int blockCnt = 0;
    for (int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++) {
      blockCnt += invalidateWorkForOneNode(nodes.get(nodeCnt));
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to.
   * <p/>
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   *
   * @return number of blocks scheduled for replication during this iteration.
   */
  int computeReplicationWork(int blocksToProcess) throws IOException {
    List<List<Block>> blocksToReplicate =
        neededReplications.chooseUnderReplicatedBlocks(blocksToProcess);

    int scheduledWork = 0;
    for (int priority = 0; priority < blocksToReplicate.size(); priority++) {
      for (Block block : blocksToReplicate.get(priority)) {
        scheduledWork += computeReplicationWorkForBlock(block, priority);
      }
    }
    return scheduledWork;
  }

  /**
   * Replicate a set of blocks
   *
   * @return the number of blocks scheduled for replication
   */
  private int computeReplicationWorkForBlockInternal(Block blk, int priority1)
      throws StorageException, IOException {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes, liveReplicaNodes;
    DatanodeDescriptor srcNode;
    BlockCollection bc = null;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<>();

    synchronized (neededReplications) {
      // block should belong to a file
      bc = blocksMap.getBlockCollection(blk);
      // abandoned block or block reopened for append
      if (bc == null || bc instanceof MutableBlockCollection) {
        neededReplications.remove(getBlockInfo(blk),
            priority1); // remove from neededReplications
        neededReplications.decrementReplicationIndex(priority1);
        return scheduledWork;
      }

      requiredReplication = bc.getBlockReplication();

      // get a source data-node
      containingNodes = new ArrayList<>();
      liveReplicaNodes = new ArrayList<>();
      NumberReplicas numReplicas = new NumberReplicas();
      srcNode = chooseSourceDatanode(blk, containingNodes, liveReplicaNodes,
          numReplicas, priority1);
      if (srcNode == null) { // block can not be replicated from any node
        LOG.debug("Block " + blk + " cannot be repl from any node");
        return scheduledWork;
      }

      assert liveReplicaNodes.size() == numReplicas.liveReplicas();
      // do not schedule more if enough replicas is already pending
      numEffectiveReplicas = numReplicas.liveReplicas() +
          pendingReplications.getNumReplicas(getBlockInfo(blk));

      if (numEffectiveReplicas >= requiredReplication) {
        if ((pendingReplications.getNumReplicas(getBlockInfo(blk)) > 0) ||
            (blockHasEnoughRacks(blk))) {
          neededReplications.remove(getBlockInfo(blk),
              priority1); // remove from neededReplications
          neededReplications.decrementReplicationIndex(priority1);
          blockLog.info("BLOCK* Removing " + blk +
              " from neededReplications as it has enough replicas");
          return scheduledWork;
        }
      }

      if (numReplicas.liveReplicas() < requiredReplication) {
        additionalReplRequired = requiredReplication - numEffectiveReplicas;
      } else {
        additionalReplRequired = 1; // Needed on a new rack
      }
      work.add(new ReplicationWork(blk, bc, srcNode, containingNodes,
          liveReplicaNodes, additionalReplRequired, priority1));
    }
    HashMap<Node, Node> excludedNodes = new HashMap<>();
    for (ReplicationWork rw : work) {
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      excludedNodes.clear();
      for (DatanodeDescriptor dn : rw.containingNodes) {
        excludedNodes.put(dn, dn);
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      // It is costly to extract the filename for which chooseTargets is called,
      // so for now we pass in the block collection itself.
      rw.targets = blockplacement
          .chooseTarget(rw.bc, rw.additionalReplRequired, rw.srcNode,
              rw.liveReplicaNodes, excludedNodes, rw.block.getNumBytes());
    }

    for (ReplicationWork rw : work) {
      DatanodeDescriptor[] targets = rw.targets;
      if (targets == null || targets.length == 0) {
        rw.targets = null;
        continue;
      }

      synchronized (neededReplications) {
        Block block = rw.block;
        int priority = rw.priority;
        // Recheck since global lock was released
        // block should belong to a file
        bc = blocksMap.getBlockCollection(block);
        // abandoned block or block reopened for append
        if (bc == null || bc instanceof MutableBlockCollection) {
          neededReplications.remove(getBlockInfo(block),
              priority); // remove from neededReplications
          rw.targets = null;
          neededReplications.decrementReplicationIndex(priority);
          continue;
        }
        requiredReplication = bc.getBlockReplication();

        // do not schedule more if enough replicas is already pending
        NumberReplicas numReplicas = countNodes(block);
        numEffectiveReplicas = numReplicas.liveReplicas() +
            pendingReplications.getNumReplicas(getBlockInfo(block));

        if (numEffectiveReplicas >= requiredReplication) {
          if ((pendingReplications.getNumReplicas(getBlockInfo(block)) > 0) ||
              (blockHasEnoughRacks(block))) {
            neededReplications.remove(getBlockInfo(block),
                priority); // remove from neededReplications
            neededReplications.decrementReplicationIndex(priority);
            rw.targets = null;
            blockLog.info("BLOCK* Removing " + block +
                " from neededReplications as it has enough replicas");
            continue;
          }
        }

        if ((numReplicas.liveReplicas() >= requiredReplication) &&
            (!blockHasEnoughRacks(block))) {
          if (rw.srcNode.getNetworkLocation()
              .equals(targets[0].getNetworkLocation())) {
            //No use continuing, unless a new rack in this case
            continue;
          }
        }

        // Add block to the to be replicated list
        rw.srcNode.addBlockToBeReplicated(block, targets);
        scheduledWork++;

        for (DatanodeDescriptor dn : targets) {
          dn.incBlocksScheduled();
        }

        // Move the block-replication into a "pending" state.
        // The reason we use 'pending' is so we can retry
        // replications that fail after an appropriate amount of time.
        pendingReplications.increment(getBlockInfo(block), targets.length);
        if (blockLog.isDebugEnabled()) {
          blockLog.debug("BLOCK* block " + block +
              " is moved from neededReplications to pendingReplications");
        }

        // remove from neededReplications
        if (numEffectiveReplicas + targets.length >= requiredReplication) {
          neededReplications.remove(getBlockInfo(block),
              priority); // remove from neededReplications
          neededReplications.decrementReplicationIndex(priority);
        }
      }
    }

    if (blockLog.isInfoEnabled()) {
      // log which blocks have been scheduled for replication
      for (ReplicationWork rw : work) {
        DatanodeDescriptor[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (DatanodeDescriptor target : targets) {
            targetList.append(' ');
            targetList.append(target);
          }
          blockLog.info(
              "BLOCK* ask " + rw.srcNode + " to replicate " + rw.block +
                  " to " + targetList);
        }
      }
    }
    if (blockLog.isDebugEnabled()) {
      blockLog.debug(
          "BLOCK* neededReplications = " + neededReplications.size() +
              " pendingReplications = " + pendingReplications.size());
    }

    return scheduledWork;
  }

  /**
   * Choose target datanodes according to the replication policy.
   *
   * @throws IOException
   *     if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, DatanodeDescriptor,
   * List, boolean, HashMap, long)
   */
  public DatanodeDescriptor[] chooseTarget(final String src,
      final int numOfReplicas, final DatanodeDescriptor client,
      final HashMap<Node, Node> excludedNodes, final long blocksize)
      throws IOException {
    // choose targets for the new block to be allocated.
    final DatanodeDescriptor targets[] = blockplacement
        .chooseTarget(src, numOfReplicas, client,
            new ArrayList<DatanodeDescriptor>(), false, excludedNodes,
            blocksize);
    if (targets.length < minReplication) {
      throw new IOException(
          "File " + src + " could only be replicated to " + targets.length +
              " nodes instead of minReplication (=" + minReplication +
              ").  There are " +
              getDatanodeManager().getNetworkTopology().getNumOfLeaves() +
              " datanode(s) running and " +
              (excludedNodes == null ? "no" : excludedNodes.size()) +
              " node(s) are excluded in this operation.");
    }
    return targets;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   * <p/>
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their
   * replication limits.  However, if the replication is of the highest
   * priority
   * and all nodes have reached their replication limits, we will choose a
   * random node despite the replication limit.
   * <p/>
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   *
   * @param block
   *     Block for which a replication source is needed
   * @param containingNodes
   *     List to be populated with nodes found to contain the
   *     given block
   * @param nodesContainingLiveReplicas
   *     List to be populated with nodes found to
   *     contain live replicas of the given block
   * @param numReplicas
   *     NumberReplicas instance to be initialized with the
   *     counts of live, corrupt, excess, and
   *     decommissioned replicas of the given
   *     block.
   * @param priority
   *     integer representing replication priority of the given
   *     block
   * @return the DatanodeDescriptor of the chosen node from which to replicate
   * the given block
   */
  @VisibleForTesting
  DatanodeDescriptor chooseSourceDatanode(Block block,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeDescriptor> nodesContainingLiveReplicas,
      NumberReplicas numReplicas, int priority)
      throws StorageException, TransactionContextException {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    List<DatanodeDescriptor> datanodes = blocksMap.nodeList(block);
    Collection<DatanodeDescriptor> nodesCorrupt =
        corruptReplicas.getNodes(getBlockInfo(block));
    for(DatanodeDescriptor node : datanodes) {
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned++;
      } else if (excessReplicateMap
          .contains(node.getStorageID(), getBlockInfo(block))) {
        excess++;
      } else {
        nodesContainingLiveReplicas.add(node);
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node)) {
        continue;
      }
      if (priority != UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY &&
          node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams) {
        continue; // already reached replication limit
      }
      if (node.getNumberOfBlocksToBeReplicated() >=
          replicationStreamsHardLimit) {
        continue;
      }
      // the block must not be scheduled for removal on srcNode
      if (excessReplicateMap
          .contains(node.getStorageID(), getBlockInfo(block))) {
        continue;
      }
      // never use already decommissioned nodes
      if (node.isDecommissioned()) {
        continue;
      }
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      if (node.isDecommissionInProgress() || srcNode == null) {
        srcNode = node;
        continue;
      }
      if (srcNode.isDecommissionInProgress()) {
        continue;
      }
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (DFSUtil.getRandom().nextBoolean()) {
        srcNode = node;
      }
    }
    if (numReplicas != null) {
      numReplicas.initialize(live, decommissioned, corrupt, excess, 0);
    }
    return srcNode;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  @VisibleForTesting
  void processPendingReplications() throws IOException {
    long[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      for (long timedOutItem : timedOutItems) {
        processTimedOutPendingBlock(timedOutItem);
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report.
   */
  private static class StatefulBlockInfo {
    final BlockInfoUnderConstruction storedBlock;
    final ReplicaState reportedState;

    StatefulBlockInfo(BlockInfoUnderConstruction storedBlock,
        ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedState = reportedState;
    }
  }

  /**
   * BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
   * list of blocks that should be considered corrupt due to a block report.
   */
  private static class BlockToMarkCorrupt {
    /**
     * The corrupted block in a datanode.
     */
    final BlockInfo corrupted;
    /**
     * The corresponding block stored in the BlockManager.
     */
    final BlockInfo stored;
    /**
     * The reason to mark corrupt.
     */
    final String reason;

    BlockToMarkCorrupt(BlockInfo corrupted, BlockInfo stored, String reason) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      Preconditions.checkNotNull(stored, "stored is null");

      this.corrupted = corrupted;
      this.stored = stored;
      this.reason = reason;
    }

    BlockToMarkCorrupt(BlockInfo stored, String reason) {
      this(stored, stored, reason);
    }

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason) {
      this(new BlockInfo(stored), stored, reason);
      //the corrupted block in datanode has a different generation stamp
      corrupted.setGenerationStampNoPersistance(gs);
    }

    @Override
    public String toString() {
      return corrupted + "(" +
          (corrupted == stored ? "same as stored" : "stored=" + stored) + ")";
    }
  }

  /**
   * The given datanode is reporting all its blocks.
   * Update the (machine-->blocklist) and (block-->machinelist) maps.
   */
  public void processReport(final DatanodeID nodeID, final String poolId,
      final BlockReport newReport) throws IOException {
    final long startTime = Time.now(); //after acquiring write lock
    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      throw new IOException(
          "ProcessReport from dead or unregistered node: " + nodeID);
    }

    // To minimize startup time, we discard any second (or later) block reports
    // that we receive while still in startup phase.
    if (namesystem.isInStartupSafeMode() && !node.isFirstBlockReport()) {
      blockLog.info("BLOCK* processReport: " +
          "discarded non-initial block report from " + nodeID +
          " because namenode still in startup phase");
      return;
    }

    processReport(node, newReport);

    // Now that we have an up-to-date block report, we know that any
    // deletions from a previous NN iteration have been accounted for.
    boolean staleBefore = node.areBlockContentsStale();
    node.receivedBlockReport();
    if (staleBefore && !node.areBlockContentsStale()) {
      LOG.info(
          "BLOCK* processReport: Received first block report from " + node +
              " after becoming active. Its block contents are no longer" +
              " considered stale");
      rescanPostponedMisreplicatedBlocks();
    }

    final long endTime = Time.now();

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addBlockReport((int) (endTime - startTime));
    }
    blockLog.info("BLOCK* processReport: from " + nodeID + ", blocks: " +
        newReport.getNumBlocks() + ", processing time: " +
        (endTime - startTime) + " msecs");
  }

  /**
   * Rescan the list of blocks which were previously postponed.
   */
  private void rescanPostponedMisreplicatedBlocks() throws IOException {
    HopsTransactionalRequestHandler rescanPostponedMisreplicatedBlocksHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.RESCAN_MISREPLICATED_BLOCKS) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            Block b = (Block) getParams()[0];
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Block b = (Block) getParams()[0];
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
                .add(lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
                .add(
                    lf.getBlockRelated(BLK.RE, BLK.IV, BLK.CR, BLK.UR, BLK.ER));
          }

          @Override
          public Object performTask() throws IOException {
            Block b = (Block) getParams()[0];
            Set<Block> toRemoveSet = (Set<Block>) getParams()[1];

            BlockInfo bi = blocksMap.getStoredBlock(b);
            if (bi == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
                    "Postponed mis-replicated block " + b +
                    " no longer found " + "in block map.");
              }
              toRemoveSet.add(b);
              postponedMisreplicatedBlocksCount.decrementAndGet();
              return null;
            }
            MisReplicationResult res = processMisReplicatedBlock(bi);
            if (LOG.isDebugEnabled()) {
              LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
                  "Re-scanned block " + b + ", result is " + res);
            }
            if (res != MisReplicationResult.POSTPONE) {
              toRemoveSet.add(b);
              postponedMisreplicatedBlocksCount.decrementAndGet();
            }
            return null;
          }
        };

    final Set<Block> toRemove = new HashSet<>();
    for (Block postponedMisreplicatedBlock : postponedMisreplicatedBlocks) {
      rescanPostponedMisreplicatedBlocksHandler
          .setParams(postponedMisreplicatedBlock, toRemove);
      rescanPostponedMisreplicatedBlocksHandler.handle(namesystem);
    }
    postponedMisreplicatedBlocks.removeAll(toRemove);
  }

  private void processReport(final DatanodeDescriptor node,
      final BlockReport report) throws IOException {
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    ConcurrentHashMap<BlockInfo, Boolean> mapToAdd = new ConcurrentHashMap<BlockInfo,Boolean>();
    ConcurrentHashMap<Long, Boolean> mapToRemove = new ConcurrentHashMap<Long,Boolean>();
    ConcurrentHashMap<Block, Boolean> mapToInvalidate = new ConcurrentHashMap<Block,Boolean>();
    ConcurrentHashMap<BlockToMarkCorrupt, Boolean> mapToCorrupt = new ConcurrentHashMap<BlockToMarkCorrupt,Boolean>();
    ConcurrentHashMap<StatefulBlockInfo, Boolean> mapToUC = new ConcurrentHashMap<StatefulBlockInfo,Boolean>();
    Collection<BlockInfo> toAdd = Collections.newSetFromMap(mapToAdd);
    Collection<Long> toRemove = Collections.newSetFromMap(mapToRemove);
    Collection<Block> toInvalidate = Collections.newSetFromMap(mapToInvalidate);
    Collection<BlockToMarkCorrupt> toCorrupt = Collections.newSetFromMap(mapToCorrupt);
    Collection<StatefulBlockInfo> toUC = Collections.newSetFromMap(mapToUC);

    final boolean firstBlockReport =
        namesystem.isInStartupSafeMode() && node.isFirstBlockReport();
    reportDiff(node, report, toAdd, toRemove, toInvalidate, toCorrupt, toUC,
        firstBlockReport);

    // Process the blocks on each queue
    for (StatefulBlockInfo b : toUC) {
      if (firstBlockReport) {
        addStoredBlockUnderConstructionImmediateTx(b.storedBlock, node,
            b.reportedState);
      } else {
        addStoredBlockUnderConstructionTx(b.storedBlock, node, b.reportedState);
      }
    }

    for (BlockInfo b : toAdd) {
      if (firstBlockReport) {
        addStoredBlockImmediateTx(b, node);
      } else {
        addStoredBlockTx(b, node, null, true);
      }
    }

    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorruptTx(b, node);
    }

    if (!firstBlockReport) {
      for (Block b : toInvalidate) {
        blockLog.info("BLOCK* processReport: " + b + " on " + node + " size " +
            b.getNumBytes() + " does not belong to any file");
      }
      addToInvalidates(toInvalidate, node);

      for (Long b : toRemove) {
        removeStoredBlockTx(b, node);
      }
    }
  }
  
  private static class HashMatchingResult{
    private final List<Integer> matchingBuckets;
    private final List<Integer> mismatchedBuckets;
    
    HashMatchingResult(List<Integer> matchingBuckets, List<Integer>
        mismatchedBuckets){
      this.matchingBuckets = matchingBuckets;
      this.mismatchedBuckets = mismatchedBuckets;
    }
  }

  private void reportDiff(final DatanodeDescriptor dn,
      final BlockReport newReport, final Collection<BlockInfo> toAdd,
      // add to DatanodeDescriptor
      final Collection<Long> toRemove,
      // remove from DatanodeDescriptor
      final Collection<Block> toInvalidate, // should be removed from DN
      final Collection<BlockToMarkCorrupt> toCorrupt,
      // add to corrupt replicas list
      final Collection<StatefulBlockInfo> toUC, final boolean firstBlockReport)
      throws IOException { // add to under-construction list

    if (newReport == null) {
      return;
    }
  
    HashMatchingResult matchingResult;
    if (dn.isFirstBlockReport()){
      //For some reason, the first block reports can report matching hashes
      //despite being incorrect. I still don't get why..
      List<Integer> allBucketIds = new ArrayList<>();
      for (int i = 0; i < newReport.getBuckets().length; i++){
        allBucketIds.add(i);
      }
      matchingResult = new HashMatchingResult(new ArrayList<Integer>(),
          allBucketIds );
    } else {
      matchingResult = calculateMismatchedHashes(dn,
          newReport);
    }
    
    if(LOG.isDebugEnabled()){
      LOG.debug(String.format("%d/%d reported hashes matched",
          newReport.getHashes().length-matchingResult.mismatchedBuckets.size(),
          newReport.getHashes().length));
    }
    
    final Map<Long,Long> invalidatedReplicas = new HashMap<>();
    for (InvalidatedBlock invBlock : invalidateBlocks.findInvBlocksbyStorageId
        (dn.getSId())){
      invalidatedReplicas.put(invBlock.getBlockId(), invBlock
          .getGenerationStamp());
    }
    
    final Set<Long> aggregatedSafeBlocks = new HashSet<>();
    
    for (final int safeBucket : matchingResult.matchingBuckets){
      for (BlockReportBlock safeBlock : newReport.getBuckets()[safeBucket]
          .getBlocks()){
        //We cannot have matching buckets that contain RBW replicas. We only
        //count finalized replicas on the namenode side.
        assert safeBlock.getState() == BlockReportBlockState.FINALIZED :
            "Expected FINALIZED replica, was: " + safeBlock.getState();
        aggregatedSafeBlocks.add(safeBlock.getBlockId());
      }
    }
    
    final Collection<Callable<Void>> subTasks = new ArrayList<>();
    
    final Map<Long, Integer> mismatchedBlocksAndInodes = dn
        .getAllMachineReplicasInBuckets(matchingResult.mismatchedBuckets);
  
    
    final Set<Long> allMismatchedBlocksOnServer = mismatchedBlocksAndInodes.keySet();
    aggregatedSafeBlocks.addAll(allMismatchedBlocksOnServer);
    
    for (final int bucketId : matchingResult.mismatchedBuckets){
      
      final BlockReportBucket bucket = newReport.getBuckets()[bucketId];
      int numSlices = bucket.getBlocks().length / processReportBatchSize + 1;
      final AtomicInteger sliceNotDoneCounter = new AtomicInteger(numSlices);
      final int[] newBucketHash = new int[1];
      
      try {
        Slicer.slice(bucket.getBlocks().length, processReportBatchSize,
            new Slicer.OperationHandler() {
              @Override
              public void handle(int startIndex, int endIndex)
                  throws Exception {
                //Doesn't copy the array, just creates wrappers.
                final List<BlockReportBlock> slice = Arrays.asList(bucket
                    .getBlocks()).subList(startIndex, endIndex);
                final Callable<Void> subTask = new Callable<Void>() {
                  @Override
                  public Void call() throws Exception {
                    final HopsTransactionalRequestHandler processReportHandler =
                        new HopsTransactionalRequestHandler(
                            HDFSOperationType.PROCESS_REPORT) {
            
                          @Override
                          public void acquireLock(TransactionLocks locks) throws IOException {
                            LockFactory lf = LockFactory.getInstance();
                            List<Long> resolvedBlockIds = new ArrayList<>();
                            List<Integer> inodeIds = new ArrayList<>();
                            List<Long> unResolvedBlockIds = new ArrayList<>();
              
                            List<BlockReportBlock> reportedBlocksSlice =
                                (List<BlockReportBlock>) getParams()[0];
                            for (BlockReportBlock reportedBlock :
                                reportedBlocksSlice) {
                              Integer inodeId = mismatchedBlocksAndInodes.get
                                  (reportedBlock.getBlockId());
                              if (inodeId != null) {
                                resolvedBlockIds.add(reportedBlock.getBlockId());
                                inodeIds.add(inodeId);
                              } else {
                                unResolvedBlockIds.add(reportedBlock.getBlockId());
                              }
                            }
              
                            locks.add(
                                lf.getBlockReportingLocks(Longs.toArray(resolvedBlockIds),
                                    Ints.toArray(inodeIds),
                                    Longs.toArray(unResolvedBlockIds), dn.getSId()))
                                .add(lf.getIndividualHashBucketLock(dn.getSId(), bucketId));
                          }
            
                          @Override
                          public Object performTask() throws IOException {
                            List<BlockReportBlock> reportedBlocks =
                                (List<BlockReportBlock>) getParams()[0];
                            // scan the report and process newly reported blocks
                            long hash = 0; // Our updated hash should only consider
                            // finalized, stored blocks
                            for (BlockReportBlock brb : reportedBlocks) {
                              Block block = new Block();
                              block.setNoPersistance(brb.getBlockId(), brb.getLength(),
                                  brb.getGenerationStamp());
                              BlockInfo storedBlock =
                                  processReportedBlock(dn,
                                      block, fromBlockReportBlockState(brb.getState()),
                                      toAdd,
                                      toInvalidate,
                                      toCorrupt, toUC, aggregatedSafeBlocks,
                                      firstBlockReport,
                                      allMismatchedBlocksOnServer.contains(brb.getBlockId()),
                                      invalidatedReplicas);
                              if (storedBlock != null) {
                                mismatchedBlocksAndInodes.remove(storedBlock.getBlockId());
                                if (brb.getState() == BlockReportBlockState.FINALIZED){
                                  // Only update hash with blocks that should not
                                  // be removed and are finalized. This helps catch excess
                                  // replicas as well.
                                  hash += BlockReport.hashAsFinalized(brb);
                                }
                              }
                
                            }
              
                            //Update hash to match:
                            //long reportedHash = (long) getParams()[1];
                            synchronized (newBucketHash){
                              newBucketHash[0] += hash;
                            }
                            
                            if (sliceNotDoneCounter.decrementAndGet() == 0) {
                              //If we are in the last processed slice
                              HashBucket bucket = HashBuckets.getInstance()
                                  .getBucket(dn.getSId(), bucketId);
                              bucket.setHash(newBucketHash[0]);
                            }
                            return null;
                          }
                        };
                    processReportHandler.setParams(slice);
                    processReportHandler.handle(null);
                    return null;
                  }
                };
                subTasks.add(subTask);
              }
            }
        );
      } catch (Exception e){
        e.printStackTrace();
      }
    }
    
    try {
      ((FSNamesystem) namesystem).getExecutorService().invokeAll(subTasks);
    } catch (Exception e) {
      LOG.error("Exception was thrown during block report processing", e);
    }
    
    toRemove.addAll(allMismatchedBlocksOnServer);
    if (namesystem.isInStartupSafeMode()) {
      aggregatedSafeBlocks.removeAll(toRemove);
      LOG.debug("AGGREGATED SAFE BLOCK #: " + aggregatedSafeBlocks.size() +
          " REPORTED BLOCK #: " + newReport.getNumBlocks());
      namesystem.adjustSafeModeBlocks(aggregatedSafeBlocks);
    }
  }
  
  private ReplicaState fromBlockReportBlockState(
      BlockReportBlockState
          state) {
    switch (state){
      case FINALIZED:
        return ReplicaState.FINALIZED;
      case RBW:
        return ReplicaState.RBW;
      case RWR:
        return ReplicaState.RWR;
      default:
        throw new RuntimeException("Block Report should only contain FINALIZED, RBW " +
            "and RWR replicas. Got: " + state);
    }
  }

  private HashMatchingResult calculateMismatchedHashes(DatanodeDescriptor dn,
      BlockReport report) throws IOException {
    List<HashBucket> allMachineHashes = HashBuckets.getInstance()
        .getBucketsForDatanode(dn);
    List<Integer> matchedBuckets = new ArrayList<>();
    List<Integer> mismatchedBuckets = new ArrayList<>();
    
    for (int i = 0; i < report.getBuckets().length; i++){
      boolean matched = false;
      for (HashBucket bucket : allMachineHashes){
        if (bucket.getBucketId() == i && bucket.getHash() == report
            .getHashes()[i]){
          matched = true;
          break;
        }
      }
      if (matched){
        matchedBuckets.add(i);
      } else {
        mismatchedBuckets.add(i);
      }
    }
    
    return new HashMatchingResult(matchedBuckets, mismatchedBuckets);
  }
  
  /**
   * Process a block replica reported by the data-node.
   * No side effects except adding to the passed-in Collections.
   * <p/>
   * <ol>
   * <li>If the block is not known to the system (not in blocksMap) then the
   * data-node should be notified to invalidate this block.</li>
   * <li>If the reported replica is valid that is has the same generation stamp
   * and length as recorded on the name-node, then the replica location should
   * be added to the name-node.</li>
   * <li>If the reported replica is not valid, then it is marked as corrupt,
   * which triggers replication of the existing valid replicas.
   * Corrupt replicas are removed from the system when the block
   * is fully replicated.</li>
   * <li>If the reported replica is for a block currently marked "under
   * construction" in the NN, then it should be added to the
   * BlockInfoUnderConstruction's list of replicas.</li>
   * </ol>
   *
   * @param dn
   *     descriptor for the datanode that made the report
   * @param block
   *     reported block replica
   * @param reportedState
   *     reported replica state
   * @param toAdd
   *     add to DatanodeDescriptor
   * @param toInvalidate
   *     missing blocks (not in the blocks map)
   *     should be removed from the data-node
   * @param toCorrupt
   *     replicas with unexpected length or generation stamp;
   *     add to corrupt replicas
   * @param toUC
   *     replicas of blocks currently under construction
   * @return the up-to-date stored block, if it should be kept.
   * Otherwise, null.
   */
  private BlockInfo processIncrementallyReportedBlock(final DatanodeDescriptor dn,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfo> toAdd, final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block + " on " + dn + " size " +
          block.getNumBytes() + " replicaState = " + reportedState);
    }
//TODO: CAN WE UPDATE SAFE BLOCK COUNT FROM HERE?
    // find block by blockId
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      blockLog.info("BLOCK* processReport: " + block + " on " + dn + " size " +
          block.getNumBytes() + " does not belong to any file");
      toInvalidate.add(new Block(block));
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();

    // Block is on the NN
    if (LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

    // Ignore replicas already scheduled to be removed from the DN
    if (invalidateBlocks.contains(dn.getStorageID(), getBlockInfo(block))) {
     /*  TODO: following assertion is incorrect, see HDFS-2668
      assert storedBlock.findDatanode(dn) < 0 : "Block " + block
      + " in recentInvalidatesSet should not appear in DN " + dn; */
      return storedBlock;
    }
    

    BlockToMarkCorrupt c =
        checkReplicaCorrupt(block, reportedState, storedBlock, ucState, dn);
    if (c != null) {
      toCorrupt.add(c);
      return storedBlock;
    }


    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo((BlockInfoUnderConstruction) storedBlock,
          reportedState));
      return storedBlock;
    }

    //add replica if appropriate
    if (reportedState == ReplicaState.FINALIZED) {
      if(storedBlock.hasReplicaIn(dn)){
        return storedBlock;
      }

      toAdd.add(storedBlock);
    }
    return storedBlock;
  }


  private BlockInfo processReportedBlock(final
  DatanodeDescriptor dn,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfo> toAdd, final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC, final Set<Long> safeBlocks,
      final boolean firstBlockReport, final boolean replicaAlreadyExists,
      final Map<Long,Long> allMachineInvalidatedBlocks)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block + " on " + dn + " size " +
          block.getNumBytes() + " replicaState = " + reportedState);
    }

    // find block by blockId
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      blockLog.info("BLOCK* processReport: " + block + " on " + dn + " size " +
          block.getNumBytes() + " does not belong to any file");
      toInvalidate.add(new Block(block));
      safeBlocks.remove(block.getBlockId());
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();

    // Block is on the NN
    if (LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

    // TODO: I see that this is done to "cache" the invalidated blocks before
    // processing the report. Can't we move this outside this method instead,
    // and keep the processBlockReport shared between incremental and full
    // reports? RE: no point, we don't have "safe blocks" either in the other
    // version.
    if (!firstBlockReport) {
      // Ignore replicas already scheduled to be removed from the DN
      if (allMachineInvalidatedBlocks.containsKey(block.getBlockId()) &&
          allMachineInvalidatedBlocks.get(block.getBlockId()) == block.getGenerationStamp() ) {
       /*  TODO: following assertion is incorrect, see HDFS-2668
        assert storedBlock.findDatanode(dn) < 0 : "Block " + block
        + " in recentInvalidatesSet should not appear in DN " + dn; */
        return storedBlock;
      }
    }

    BlockToMarkCorrupt c =
        checkReplicaCorrupt(block, reportedState, storedBlock, ucState, dn);
    if (c != null) {
      toCorrupt.add(c);
      safeBlocks.remove(block.getBlockId());
      return storedBlock;
    }


    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo((BlockInfoUnderConstruction) storedBlock,
          reportedState));
      safeBlocks.remove(block.getBlockId());
      return storedBlock;
    }

    //add replica if appropriate
    if (reportedState == ReplicaState.FINALIZED) {
      if(replicaAlreadyExists || storedBlock.hasReplicaIn(dn)){
        return storedBlock;
      }

      toAdd.add(storedBlock);
      safeBlocks.remove(block.getBlockId());
    }
    return storedBlock;
  }

  /**
   * The next two methods test the various cases under which we must conclude
   * the replica is corrupt, or under construction.  These are laid out
   * as switch statements, on the theory that it is easier to understand
   * the combinatorics of reportedState and ucState that way.  It should be
   * at least as efficient as boolean expressions.
   *
   * @return a BlockToMarkCorrupt object, or null if the replica is not corrupt
   */
  private BlockToMarkCorrupt checkReplicaCorrupt(Block reported,
      ReplicaState reportedState, BlockInfo storedBlock, BlockUCState ucState,
      DatanodeDescriptor dn) {
    switch (reportedState) {
      case FINALIZED:
        switch (ucState) {
          case COMPLETE:
          case COMMITTED:
            if (storedBlock.getGenerationStamp() !=
                reported.getGenerationStamp()) {
              final long reportedGS = reported.getGenerationStamp();
              return new BlockToMarkCorrupt(storedBlock, reportedGS,
                  "block is " + ucState + " and reported genstamp " +
                      reportedGS + " does not match genstamp in block map " +
                      storedBlock.getGenerationStamp());
            } else if (storedBlock.getNumBytes() != reported.getNumBytes()) {
              return new BlockToMarkCorrupt(storedBlock,
                  "block is " + ucState + " and reported length " +
                      reported.getNumBytes() + " does not match " +
                      "length in block map " + storedBlock.getNumBytes());
            } else {
              return null; // not corrupt
            }
          default:
            return null;
        }
      case RBW:
      case RWR:
        if (!storedBlock.isComplete()) {
          return null; // not corrupt
        } else if (storedBlock.getGenerationStamp() !=
            reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(storedBlock, reportedGS,
              "reported " + reportedState + " replica with genstamp " +
                  reportedGS +
                  " does not match COMPLETE block's genstamp in block map " +
                  storedBlock.getGenerationStamp());
        } else { // COMPLETE block, same genstamp
          if (reportedState == ReplicaState.RBW) {
            // If it's a RBW report for a COMPLETE block, it may just be that
            // the block report got a little bit delayed after the pipeline
            // closed. So, ignore this report, assuming we will get a
            // FINALIZED replica later. See HDFS-2791
            LOG.info("Received an RBW replica for " + storedBlock +
                " on " + dn + ": ignoring it, since it is " +
                "complete with the same genstamp");
            return null;
          } else {
            return new BlockToMarkCorrupt(storedBlock,
                "reported replica has invalid state " + reportedState);
          }
        }
      case RUR:       // should not be reported
      case TEMPORARY: // should not be reported
      default:
        String msg =
            "Unexpected replica state " + reportedState + " for block: " +
                storedBlock +
                " on " + dn + " size " + storedBlock.getNumBytes();
        // log here at WARN level since this is really a broken HDFS invariant
        LOG.warn(msg);
        return new BlockToMarkCorrupt(storedBlock, msg);
    }
  }

  private boolean isBlockUnderConstruction(BlockInfo storedBlock,
      BlockUCState ucState, ReplicaState reportedState) {
    switch (reportedState) {
      case FINALIZED:
        switch (ucState) {
          case UNDER_CONSTRUCTION:
          case UNDER_RECOVERY:
            return true;
          default:
            return false;
        }
      case RBW:
      case RWR:
        return (!storedBlock.isComplete());
      case RUR:       // should not be reported
      case TEMPORARY: // should not be reported
      default:
        return false;
    }
  }

  private void addStoredBlockUnderConstruction(BlockInfoUnderConstruction block,
      DatanodeDescriptor node, ReplicaState reportedState) throws IOException {
    block.addExpectedReplicaIfNotPresent(node, reportedState);
    if (reportedState == ReplicaState.FINALIZED && !block.hasReplicaIn(node)) {
      addStoredBlock(block, node, null, true);
    }
  }

  /**
   * Faster version of
   * {@link #addStoredBlock(BlockInfo, DatanodeDescriptor, DatanodeDescriptor,
   * boolean)}
   * , intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle underReplication/overReplication, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
   *
   * @throws IOException
   */
  private void addStoredBlockImmediate(BlockInfo storedBlock,
      DatanodeDescriptor node) throws IOException {
    assert (storedBlock != null);
    if (!namesystem.isInStartupSafeMode() ||
        namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, node, null, false);
      return;
    }

    // just add it
    node.addBlock(storedBlock);

    // Now check for completion of blocks and safe block count
    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numCurrentReplica >= minReplication) {
      completeBlock((MutableBlockCollection) storedBlock.getBlockCollection(),
          storedBlock, false);
    } else if (storedBlock.isComplete()) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(storedBlock);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   *
   * @return the block that is stored in blockMap.
   */
  private Block addStoredBlock(BlockInfo block, DatanodeDescriptor node,
      DatanodeDescriptor delNodeHint, boolean logEveryBlock)
      throws IOException {
    assert block != null;
    BlockCollection bc = block.getBlockCollection();
    if (bc == null) {
      // If this block does not belong to a file, then we are done.
      blockLog.info(
          "BLOCK* addStoredBlock: " + block + " on " + node + " size " +
              block.getNumBytes() + " but it does not belong to any file");
      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }
    
    // Block is stored and belongs to a file.
    
    FSNamesystem fsNamesystem = (FSNamesystem) namesystem;
    NumberReplicas numBeforeAdding = null;
    if (fsNamesystem.isErasureCodingEnabled()) {
      numBeforeAdding = countNodes(block);
    }

    // add block to the datanode
    boolean added = node.addBlock(block);
    int curReplicaDelta;
    if (added) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        logAddStoredBlock(block, node);
      }
    } else {
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: " +
          "Redundant addStoredBlock request received for " + block +
          " on " + node + " size " + block.getNumBytes());
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(block);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica =
        numLiveReplicas + pendingReplications.getNumReplicas(block);

    if (block.getBlockUCState() == BlockUCState.COMMITTED &&
        numLiveReplicas >= minReplication) {
      block =
          completeBlock((MutableBlockCollection) bc, block, false);
    } else if (block.isComplete()) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that
      // Is no-op if not in safe mode.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(block);
    }

    // if file is under construction, then done for now
    if (bc instanceof MutableBlockCollection) {
      return block;
    }

    // do not try to handle over/under-replicated blocks during safe mode
    if (!namesystem.isPopulatingReplQueues()) {
      return block;
    }

    // handle underReplication/overReplication
    short fileReplication = bc.getBlockReplication();
    if (!isNeededReplication(block, fileReplication, numCurrentReplica)) {
      neededReplications
          .remove(block, numCurrentReplica, num.decommissionedReplicas(),
              fileReplication);
    } else {
      updateNeededReplications(block, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(block, fileReplication, node,
          delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          block + "blockMap has " + numCorruptNodes +
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(block);
    }

    if (fsNamesystem.isErasureCodingEnabled()) {
      INode iNode = EntityManager.find(INode.Finder.ByINodeIdFTIS, bc.getId());
      if (!iNode.isUnderConstruction() &&
          numBeforeAdding.liveReplicas() == 0 && numLiveReplicas > 0) {
        EncodingStatus status =
            EntityManager.find(EncodingStatus.Finder.ByInodeId, bc.getId());
        if (status != null && status.isCorrupt()) {
          int lostBlockCount = status.getLostBlocks() - 1;
          status.setLostBlocks(lostBlockCount);
          if (lostBlockCount == 0) {
            status.setStatus(EncodingStatus.Status.ENCODED);
            status.setStatusModificationTime(System.currentTimeMillis());
          }
          EntityManager.update(status);
        } else {
          status = EntityManager
              .find(EncodingStatus.Finder.ByParityInodeId, bc.getId());
          if (status == null) {
            LOG.info("addStoredBlock returned null for " + bc.getId());
          } else {
            LOG.info("addStoredBlock found " + bc.getId() + " with status " +
                status);
          }
          if (status != null && status.isParityCorrupt()) {
            int lostParityBlockCount = status.getLostParityBlocks() - 1;
            status.setLostParityBlocks(lostParityBlockCount);
            if (lostParityBlockCount == 0) {
              status.setParityStatus(EncodingStatus.ParityStatus.HEALTHY);
              status.setParityStatusModificationTime(
                  System.currentTimeMillis());
            }
            EntityManager.update(status);
            LOG.info("addStoredBlock found set status to potentially fixed");
          }
        }
      }
    }

    return block;
  }

  private void logAddStoredBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    if (!blockLog.isInfoEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder(500);
    sb.append("BLOCK* addStoredBlock: blockMap updated: ").append(node)
        .append(" is added to ");
    storedBlock.appendStringTo(sb);
    sb.append(" size ").append(storedBlock.getNumBytes());
    blockLog.info(sb);
  }

  /**
   * Invalidate corrupt replicas.
   * <p/>
   * This will remove the replicas from the block's location list,
   * add them to {@link #invalidateBlocks} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p/>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk
   *     Block whose corrupt replicas need to be invalidated
   */
  private void invalidateCorruptReplicas(BlockInfo blk)
      throws StorageException, TransactionContextException {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean gotException = false;
    if (nodes == null) {
      return;
    }
    // make a copy of the array of nodes in order to avoid
    // ConcurrentModificationException, when the block is removed from the node
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        invalidateBlock(new BlockToMarkCorrupt(blk, null), node);
      } catch (IOException e) {
        blockLog.info(
            "invalidateCorruptReplicas " + "error in deleting bad block " +
                blk + " on " + node, e);
        gotException = true;
      }
    }
    // Remove the block from corruptReplicasMap
    if (!gotException) {
      corruptReplicas.removeFromCorruptReplicasMap(blk);
    }
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  public void processMisReplicatedBlocks() throws IOException {
    final long[] nrInvalid = {0}, nrOverReplicated = {0}, nrUnderReplicated =
        {0}, nrPostponed = {0},
        nrUnderConstruction = {0};

    //FIXME [M] we need to have a garbage collection to check for the invalid
    // blocks
    final HopsTransactionalRequestHandler processMisReplicatedBlocksHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.PROCESS_MIS_REPLICATED_BLOCKS_PER_INODE_BATCH) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            List<INodeIdentifier> inodeIdentifiers =
                (List<INodeIdentifier>) getParams()[0];
            locks.add(lf.getBatchedINodesLock(inodeIdentifiers))
                .add(lf.getSqlBatchedBlocksLock()).add(
                lf.getSqlBatchedBlocksRelated(BLK.RE, BLK.IV, BLK.CR, BLK.UR,
                    BLK.ER));

          }

          @Override
          public Object performTask() throws IOException {
            List<INodeIdentifier> inodeIdentifiers =
                (List<INodeIdentifier>) getParams()[0];
            for (INodeIdentifier inodeIdentifier : inodeIdentifiers) {
              INode inode = EntityManager
                  .find(INode.Finder.ByINodeIdFTIS, inodeIdentifier.getInodeId());
              for (BlockInfo block : ((INodeFile) inode).getBlocks()) {
                MisReplicationResult res = processMisReplicatedBlock(block);
                if (LOG.isTraceEnabled()) {
                  LOG.trace("block " + block + ": " + res);
                }
                switch (res) {
                  case UNDER_REPLICATED:
                    nrUnderReplicated[0]++;
                    break;
                  case OVER_REPLICATED:
                    nrOverReplicated[0]++;
                    break;
                  case INVALID:
                    nrInvalid[0]++;
                    break;
                  case POSTPONE:
                    nrPostponed[0]++;
                    postponeBlock(block);
                    break;
                  case UNDER_CONSTRUCTION:
                    nrUnderConstruction[0]++;
                    break;
                  case OK:
                    break;
                  default:
                    throw new AssertionError("Invalid enum value: " + res);
                }
              }
            }
            return null;
          }
        };

    final int filesToProcess =
        processMisReplicatedBatchSize * processMisReplicatedNoOfBatchs;

    if (blocksMap.countAllFiles() != 0) {
      boolean haveMore;

      do {
        long filesToProcessEndIndex;
        long filesToProcessStartIndex;
        do {
          filesToProcessEndIndex =
              HdfsVariables.incrementMisReplicatedIndex(filesToProcess);
          filesToProcessStartIndex = filesToProcessEndIndex - filesToProcess;
          haveMore =
              blocksMap.haveFilesWithIdGreaterThan(filesToProcessEndIndex);
        } while (!blocksMap.haveFilesWithIdBetween(filesToProcessStartIndex,
            filesToProcessEndIndex) && haveMore);

        addToMisReplicatedRangeQueue(filesToProcessStartIndex,
            filesToProcessEndIndex);

        final List<INodeIdentifier> allINodes = blocksMap
            .getAllINodeFiles(filesToProcessStartIndex, filesToProcessEndIndex);
        LOG.info("processMisReplicated read  " + allINodes.size() + "/" +
            filesToProcess + " in the Ids range [" + filesToProcessStartIndex +
            " - " + filesToProcessEndIndex + "]");

        try {
          Slicer.slice(allINodes.size(), processMisReplicatedBatchSize,
              new Slicer.OperationHandler() {
                @Override
                public void handle(int startIndex, int endIndex)
                    throws Exception {
                  List<INodeIdentifier> inodes =
                      allINodes.subList(startIndex, endIndex);
                  processMisReplicatedBlocksHandler.setParams(inodes);
                  processMisReplicatedBlocksHandler.handle(namesystem);
                }
              });
        } catch (Exception ex) {
          throw new IOException(ex);
        }

        removeFromMisReplicatedRangeQueue(filesToProcessStartIndex,
            filesToProcessEndIndex);

      } while (haveMore);
    }
    LOG.info("Total number of blocks            = " + blocksMap.size());
    LOG.info("Number of invalid blocks          = " + nrInvalid[0]);
    LOG.info("Number of under-replicated blocks = " + nrUnderReplicated[0]);
    LOG.info("Number of  over-replicated blocks = " + nrOverReplicated[0] +
        ((nrPostponed[0] > 0) ? (" (" + nrPostponed[0] + " postponed)") : ""));
    LOG.info("Number of blocks being written    = " + nrUnderConstruction[0]);
  }

  private void addToMisReplicatedRangeQueue(final long start, final long end)
      throws IOException {
    new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        MisReplicatedRangeQueueDataAccess da =
            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        da.insert(start, end);
        return null;
      }
    }.handle();
  }

  private void removeFromMisReplicatedRangeQueue(final long start,
      final long end) throws IOException {
    new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        MisReplicatedRangeQueueDataAccess da =
            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        da.remove(start, end);
        return null;
      }
    }.handle();
  }

  private int sizeOfMisReplicatedRangeQueue() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_ALL_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        MisReplicatedRangeQueueDataAccess da =
            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * Process a single possibly misreplicated block. This adds it to the
   * appropriate queues if necessary, and returns a result code indicating
   * what happened with it.
   */
  private MisReplicationResult processMisReplicatedBlock(BlockInfo block)
      throws StorageException, TransactionContextException {
    BlockCollection bc = block.getBlockCollection();
    if (bc == null) {
      // block does not belong to any file
      addToInvalidates(block);
      return MisReplicationResult.INVALID;
    }
    if (!block.isComplete()) {
      // Incomplete blocks are never considered mis-replicated --
      // they'll be reached when they are completed or recovered.
      return MisReplicationResult.UNDER_CONSTRUCTION;
    }
    // calculate current replication
    short expectedReplication = bc.getBlockReplication();
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
    // add to under-replicated queue if need to be
    if (isNeededReplication(block, expectedReplication, numCurrentReplica)) {
      if (neededReplications
          .add(block, numCurrentReplica, num.decommissionedReplicas(),
              expectedReplication)) {
        return MisReplicationResult.UNDER_REPLICATED;
      }
    }

    if (numCurrentReplica > expectedReplication) {
      if (num.replicasOnStaleNodes() > 0) {
        // If any of the replicas of this block are on nodes that are
        // considered "stale", then these replicas may in fact have
        // already been deleted. So, we cannot safely act on the
        // over-replication until a later point in time, when
        // the "stale" nodes have block reported.
        return MisReplicationResult.POSTPONE;
      }

      // over-replicated block
      processOverReplicatedBlock(block, expectedReplication, null, null);
      return MisReplicationResult.OVER_REPLICATED;
    }

    return MisReplicationResult.OK;
  }

  /**
   * Set replication for the blocks.
   */
  public void setReplication(final short oldRepl, final short newRepl,
      final String src, final Block... blocks)
      throws StorageException, TransactionContextException {
    if (newRepl == oldRepl) {
      return;
    }

    // update needReplication priority queues
    for (Block b : blocks) {
      updateNeededReplications(b, 0, newRepl - oldRepl);
    }

    if (oldRepl > newRepl) {
      // old replication > the new one; need to remove copies
      LOG.info("Decreasing replication from " + oldRepl + " to " + newRepl +
          " for " + src);
      for (Block b : blocks) {
        processOverReplicatedBlock(b, newRepl, null, null);
      }
    } else { // replication factor is increased
      LOG.info("Increasing replication from " + oldRepl + " to " + newRepl +
          " for " + src);
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  private void processOverReplicatedBlock(final Block block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint)
      throws StorageException, TransactionContextException {

    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeDescriptor> nonExcess =
        new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes =
        corruptReplicas.getNodes(getBlockInfo(block));
    for (DatanodeDescriptor cur : blocksMap.nodeList(block)){
      if (cur.areBlockContentsStale()) {
        LOG.info("BLOCK* processOverReplicatedBlock: " +
            "Postponing processing of over-replicated " +
            block + " since datanode " + cur +
            " does not yet have up-to-date " +
            "block information.");
        postponeBlock(block);
        return;
      }
      if (!excessReplicateMap
          .contains(cur.getStorageID(), getBlockInfo(block))) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(cur);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, addedNode,
        delNodeHint, blockplacement);
  }


  /**
   * We want "replication" replicates for the block, but we now have too many.
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such
   * that:
   * <p/>
   * srcNodes.size() - dstNodes.size() == replication
   * <p/>
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack.
   * If no such a node is available,
   * then pick a node with least free space
   */
  private void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess,
      Block b, short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint, BlockPlacementPolicy replicator)
      throws StorageException, TransactionContextException {

    // first form a rack to datanodes map and
    BlockCollection bc = getBlockCollection(b);
    final Map<String, List<DatanodeDescriptor>> rackMap =
        new HashMap<>();
    for (final DatanodeDescriptor node : nonExcess) {
      final String rackName = node.getNetworkLocation();
      List<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<>();
        rackMap.put(rackName, datanodeList);
      }
      datanodeList.add(node);
    }

    // split nodes into two sets
    // priSet contains nodes on rack with more than one replica
    // remains contains the remaining nodes
    final List<DatanodeDescriptor> priSet = new ArrayList<>();
    final List<DatanodeDescriptor> remains =
        new ArrayList<>();
    for (List<DatanodeDescriptor> datanodeList : rackMap.values()) {
      if (datanodeList.size() == 1) {
        remains.add(datanodeList.get(0));
      } else {
        priSet.addAll(datanodeList);
      }
    }

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    while (nonExcess.size() - replication > 0) {
      // check if we can delete delNodeHint
      final DatanodeInfo cur;
      if (firstOne && delNodeHint != null && nonExcess.contains(delNodeHint) &&
          (priSet.contains(delNodeHint) ||
              (addedNode != null && !priSet.contains(addedNode)))) {
        cur = delNodeHint;
      } else { // regular excessive replica removal
        cur = replicator
            .chooseReplicaToDelete(bc, b, replication, priSet, remains);
      }
      firstOne = false;

      // adjust rackmap, priSet, and remains
      String rack = cur.getNetworkLocation();
      final List<DatanodeDescriptor> datanodes = rackMap.get(rack);
      datanodes.remove(cur);
      if (datanodes.isEmpty()) {
        rackMap.remove(rack);
      }
      if (priSet.remove(cur)) {
        if (datanodes.size() == 1) {
          priSet.remove(datanodes.get(0));
          remains.add(datanodes.get(0));
        }
      } else {
        remains.remove(cur);
      }

      nonExcess.remove(cur);
      addToExcessReplicate(cur, b);

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeReplica" message.
      //
      // The 'invalidate' list is used to inform the datanode the block
      // should be deleted.  Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //
      addToInvalidates(b, cur);
      blockLog.info("BLOCK* chooseExcessReplicates: " + "(" + cur + ", " + b +
          ") is added to invalidated blocks set");
    }
  }

  private void addToExcessReplicate(DatanodeInfo dn, Block block)
      throws StorageException, TransactionContextException {
    if (excessReplicateMap.put(dn.getStorageID(), getBlockInfo(block))) {
      excessBlocksCount.incrementAndGet();
      if (blockLog.isDebugEnabled()) {
        blockLog.debug(
            "BLOCK* addToExcessReplicate:" + " (" + dn + ", " + block +
                ") is added to excessReplicateMap");
      }
    }
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  private void removeStoredBlock(Block block, DatanodeDescriptor node)
      throws IOException {
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* removeStoredBlock: " + block + " from " + node);
    }

    if (!blocksMap.removeNode(block, node)) {
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* removeStoredBlock: " + block +
            " has already been removed from node " + node);
      }
      return;
    }

    //
    // It's possible that the block was removed because of a datanode
    // failure. If the block is still valid, check if replication is
    // necessary. In that case, put block on a possibly-will-
    // be-replicated list.
    //
    BlockCollection bc = blocksMap.getBlockCollection(block);
    if (bc != null) {
      namesystem.decrementSafeBlockCount(getBlockInfo(block));
      updateNeededReplications(block, -1, 0);
    }

    //
    // We've removed a block from a node, so it's definitely no longer
    // in "excess" there.
    //
    if (excessReplicateMap.remove(node.getStorageID(), getBlockInfo(block))) {
      excessBlocksCount.decrementAndGet();
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* removeStoredBlock: " + block +
            " is removed from excessBlocks");
      }
    }

    // Remove the replica from corruptReplicas
    corruptReplicas.removeFromCorruptReplicasMap(getBlockInfo(block), node);

    FSNamesystem fsNamesystem = (FSNamesystem) namesystem;
    if (fsNamesystem.isErasureCodingEnabled()) {
      BlockInfo blockInfo = getStoredBlock(block);
      EncodingStatus status = EntityManager
          .find(EncodingStatus.Finder.ByInodeId, blockInfo.getInodeId());
      if (status != null) {
        NumberReplicas numberReplicas = countNodes(block);
        if (numberReplicas.liveReplicas() == 0) {
          if (status.isCorrupt() == false) {
            status.setStatus(EncodingStatus.Status.REPAIR_REQUESTED);
            status.setStatusModificationTime(System.currentTimeMillis());
          }
          status.setLostBlocks(status.getLostBlocks() + 1);
          EntityManager.update(status);
        }
      } else {
        status = EntityManager.find(EncodingStatus.Finder.ByParityInodeId,
            blockInfo.getInodeId());
        if (status == null) {
          LOG.info(
              "removeStoredBlock returned null for " + blockInfo.getInodeId());
        } else {
          LOG.info("removeStoredBlock found " + blockInfo.getInodeId() +
              " with status " + status);
        }
        if (status != null) {
          NumberReplicas numberReplicas = countNodes(block);
          if (numberReplicas.liveReplicas() == 0) {
            if (status.isParityCorrupt() == false) {
              status.setParityStatus(
                  EncodingStatus.ParityStatus.REPAIR_REQUESTED);
              status
                  .setParityStatusModificationTime(System.currentTimeMillis());
            }
            status.setLostParityBlocks(status.getLostParityBlocks() + 1);
            EntityManager.update(status);
            LOG.info(
                "removeStoredBlock updated parity status to repair requested");
          } else {
            LOG.info("removeStoredBlock found replicas: " +
                numberReplicas.liveReplicas());
          }
        }
      }
    }
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(final Block block, List<BlockWithLocations> results)
      throws IOException {

    final List<String> machineSet = new ArrayList<>();

    new HopsTransactionalRequestHandler(HDFSOperationType.GET_VALID_BLK_LOCS) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.IV));
      }

      @Override
      public Object performTask() throws IOException {
        BlockInfo temp = getBlockInfo(block);
        final List<String> ms = getValidLocations(temp);
        machineSet.addAll(ms);
        return null;
      }
    }.handle(namesystem);

    if (machineSet.isEmpty()) {
      return 0;
    } else {
      results.add(new BlockWithLocations(block,
          machineSet.toArray(new String[machineSet.size()])));
      return block.getNumBytes();
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  @VisibleForTesting
  void addBlock(DatanodeDescriptor node, Block block, String delHint)
      throws IOException {
    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeManager.getDatanode(delHint);
      if (delHintNode == null) {
        blockLog.warn("BLOCK* blockReceived: " + block +
            " is expected to be removed from an unrecorded node " + delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.decrement(getBlockInfo(block));
    processAndHandleReportedBlock(node, block, ReplicaState.FINALIZED,
        delHintNode);
  }

  private void processAndHandleReportedBlock(DatanodeDescriptor node,
      Block block, ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    // blockReceived reports a finalized block
    Collection<BlockInfo> toAdd = new LinkedList<>();
    Collection<Block> toInvalidate = new LinkedList<>();
    Collection<BlockToMarkCorrupt> toCorrupt =
        new LinkedList<>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<>();
    processIncrementallyReportedBlock(node, block, reportedState, toAdd, toInvalidate,
        toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert
        toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <=
            1 : "The block should be only in one of the lists.";

    for (StatefulBlockInfo b : toUC) {
      addStoredBlockUnderConstruction(b.storedBlock, node, b.reportedState);
    }
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, node, delHintNode, true);
    }
    for (Block b : toInvalidate) {
      blockLog.info("BLOCK* addBlock: block " + b + " on " + node + " size " +
          b.getNumBytes() + " does not belong to any file");
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, node);
    }
  }

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   */
  public void processIncrementalBlockReport(final DatanodeID nodeID,
      final String poolId, final ReceivedDeletedBlockInfo blockInfos[])
      throws IOException {
    final int[] received = {0};
    final int[] deleted = {0};
    final int[] receiving = {0};
    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);

    HopsTransactionalRequestHandler processIncrementalBlockReportHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.BLOCK_RECEIVED_AND_DELETED_INC_BLK_REPORT) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            ReceivedDeletedBlockInfo rdbi =
                (ReceivedDeletedBlockInfo) getParams()[0];
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(rdbi.getBlock());
            LOG.debug("reported block id=" + rdbi.getBlock().getBlockId() +
                " with status: " + rdbi.getStatus().name());
            if (inodeIdentifier == null) {
              LOG.error("Invalid State. deleted blk is not recognized. bid=" +
                  rdbi.getBlock().getBlockId());
            }
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            ReceivedDeletedBlockInfo rdbi =
                (ReceivedDeletedBlockInfo) getParams()[0];
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
                .add(lf.getIndividualBlockLock(rdbi.getBlock().getBlockId(),
                    inodeIdentifier))
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UR));
            if (!rdbi.isDeletedBlock()) {
              locks.add(lf.getBlockRelated(BLK.PE, BLK.UC, BLK.IV));
            }
            if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
                inodeIdentifier != null) {
              locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
                  inodeIdentifier.getInodeId()));
            }
            if (rdbi.getStatus() == ReceivedDeletedBlockInfo.BlockStatus
                .RECEIVED ||
                rdbi.getStatus() == ReceivedDeletedBlockInfo.BlockStatus
                    .DELETED){
              locks.add(lf.getIndividualHashBucketLock(node.getSId(), HashBuckets
                  .getInstance().getBucketForBlock(rdbi.getBlock())));
            }
          }

          @Override
          public Object performTask() throws IOException {
            ReceivedDeletedBlockInfo rdbi =
                (ReceivedDeletedBlockInfo) getParams()[0];
            LOG.debug("BLOCK_RECEIVED_AND_DELETED_INC_BLK_REPORT " +
                rdbi.getStatus() + " bid=" + rdbi.getBlock().getBlockId() +
                " dataNode=" + node.getXferAddr());
            HashBuckets hashBuckets = HashBuckets.getInstance();
            
            switch (rdbi.getStatus()) {
              case CREATING:
                processAndHandleReportedBlock(node, rdbi.getBlock(),
                    ReplicaState.RBW, null);
                received[0]++;
                break;
              case APPENDING:
                processAndHandleReportedBlock(node, rdbi.getBlock(),
                    ReplicaState.RBW, null);
                received[0]++;
                break;
              case RECOVERING_APPEND:
                processAndHandleReportedBlock(node, rdbi.getBlock(),
                    ReplicaState.RBW, null);
                received[0]++;
                break;
              case RECEIVED:
                addBlock(node, rdbi.getBlock(), rdbi.getDelHints());
                received[0]++;
                hashBuckets.applyHash(node.getSId(), ReplicaState.FINALIZED,
                    rdbi.getBlock());
                break;
              case UPDATE_RECOVERED:
                addBlock(node, rdbi.getBlock(), rdbi.getDelHints());
                received[0]++;
                break;
              case DELETED:
                removeStoredBlock(rdbi.getBlock(), node);
                hashBuckets.undoHash(node.getSId(), ReplicaState.FINALIZED,
                    rdbi.getBlock());
                deleted[0]++;
                break;
              default:
                String msg =
                    "Unknown block status code reported by " + nodeID + ": " +
                        rdbi;
                blockLog.warn(msg);
                assert false : msg; // if assertions are enabled, throw.
                break;
            }
            if (blockLog.isDebugEnabled()) {
              blockLog.debug("BLOCK* block " + (rdbi.getStatus()) + ": " +
                  rdbi.getBlock() + " is received from " + nodeID);
            }
            return null;
          }
        };

    try {
      if (node == null || !node.isAlive) {
        blockLog.warn("BLOCK* processIncrementalBlockReport" +
            " is received from dead or unregistered node " + nodeID);
        throw new IOException(
            "Got incremental block report from unregistered or dead node");
      }

      for (ReceivedDeletedBlockInfo rdbi : blockInfos) {
        processIncrementalBlockReportHandler.setParams(rdbi);
        processIncrementalBlockReportHandler.handle(namesystem);
      }
    } finally {
      blockLog.debug(
          "*BLOCK* NameNode.processIncrementalBlockReport: " + "from " +
              nodeID + " receiving: " + receiving[0] + ", " + " received: " +
              received[0] + ", " + " deleted: " + deleted[0]);
    }

  }

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   */
  public NumberReplicas countNodes(Block b)
      throws StorageException, TransactionContextException {
    int decommissioned = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    int stale = 0;
    List<DatanodeDescriptor> nodes = blocksMap.nodeList(b);

    Collection<DatanodeDescriptor> nodesCorrupt =
        corruptReplicas.getNodes(getBlockInfo(b));
    for(DatanodeDescriptor node : nodes) {
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned++;
      } else {
        if (excessReplicateMap.contains(node.getStorageID(), getBlockInfo(b))) {
          excess++;
        } else {
          live++;
        }
      }
      if (node.areBlockContentsStale()) {
        stale++;
      }
    }
    return new NumberReplicas(live, decommissioned, corrupt, excess, stale);
  }

  /**
   * Simpler, faster form of {@link #countNodes(Block)} that only returns the
   * number
   * of live nodes.  If in startup safemode (or its 30-sec extension period),
   * then it gains speed by ignoring issues of excess replicas or nodes
   * that are decommissioned or in process of becoming decommissioned.
   * If not in startup, then it calls {@link #countNodes(Block)} instead.
   *
   * @param b
   *     - the block being tested
   * @return count of live nodes for this block
   */
  int countLiveNodes(BlockInfo b) throws IOException {
    if (!namesystem.isInStartupSafeMode()) {
      return countNodes(b).liveReplicas();
    }
    // else proceed with fast case
    int live = 0;
    List<DatanodeDescriptor> nodes = blocksMap.nodeList(b);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeDescriptor node : nodes) {
      if ((nodesCorrupt == null) || (!nodesCorrupt.contains(node))) {
        live++;
      }
    }
    return live;
  }

  private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode,
      NumberReplicas num) throws StorageException, TransactionContextException {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    BlockCollection bc = blocksMap.getBlockCollection(block);
    List<DatanodeDescriptor> nodes = blocksMap.nodeList(block);
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeDescriptor node : nodes){
      nodeList.append(node);
      nodeList.append(" ");
    }
    LOG.info("Block: " + block + ", Expected Replicas: " + curExpectedReplicas +
        ", live replicas: " + curReplicas + ", corrupt replicas: " +
        num.corruptReplicas() + ", decommissioned replicas: " +
        num.decommissionedReplicas() + ", excess replicas: " +
        num.excessReplicas() + ", Is Open File: " +
        (bc instanceof MutableBlockCollection) +
        ", Datanodes having this block: " + nodeList + ", Current Datanode: " +
        srcNode + ", Is current datanode decommissioning: " +
        srcNode.isDecommissionInProgress());
  }

  /**
   * On stopping decommission, check if the node has excess replicas.
   * If there are any excess replicas, call processOverReplicatedBlock()
   */
  void processOverReplicatedBlocksOnReCommission(
      final DatanodeDescriptor srcNode) throws IOException {
    final int[] numOverReplicated = {0};
    final Iterator<? extends Block> it = srcNode.getBlockIterator();
    HopsTransactionalRequestHandler processBlockHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.PROCESS_OVER_REPLICATED_BLOCKS_ON_RECOMMISSION) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            Block b = (Block) getParams()[0];
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Block block = (Block) getParams()[0];
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
                .add(lf.getIndividualBlockLock(block.getBlockId(),
                    inodeIdentifier)).add(
                lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.UR,
                    BLK.IV));
          }

          @Override
          public Object performTask() throws IOException {
            final Block block = (Block) getParams()[0];
            BlockCollection bc = blocksMap.getBlockCollection(block);
            short expectedReplication = bc.getBlockReplication();
            NumberReplicas num = countNodes(block);
            int numCurrentReplica = num.liveReplicas();
            if (numCurrentReplica > expectedReplication) {
              // over-replicated block
              processOverReplicatedBlock(block, expectedReplication, null,
                  null);
              numOverReplicated[0]++;
            }
            return null;
          }
        };

    while (it.hasNext()) {
      processBlockHandler.setParams(it.next()).handle(namesystem);

    }
    LOG.info(
        "Invalidated " + numOverReplicated[0] + " over-replicated blocks on " +
            srcNode + " during recommissioning");
  }

  /**
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  boolean isReplicationInProgress(final DatanodeDescriptor srcNode)
      throws IOException {
    final boolean[] status = new boolean[]{false};
    final int[] underReplicatedBlocks = new int[]{0};
    final int[] decommissionOnlyReplicas = new int[]{0};
    final int[] underReplicatedInOpenFiles = new int[]{0};

    final Iterator<? extends Block> it = srcNode.getBlockIterator();

    HopsTransactionalRequestHandler checkReplicationHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.CHECK_REPLICATION_IN_PROGRESS) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            Block b = (Block) getParams()[0];
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);

          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Block block = (Block) getParams()[0];
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
                .add(lf.getIndividualBlockLock(block.getBlockId(),
                    inodeIdentifier)).add(
                lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UR, BLK.PE));
          }

          @Override
          public Object performTask() throws IOException {
            final Block block = (Block) getParams()[0];
            BlockCollection bc = blocksMap.getBlockCollection(block);

            if (bc != null) {
              NumberReplicas num = countNodes(block);
              int curReplicas = num.liveReplicas();
              int curExpectedReplicas = getReplication(block);
              if (isNeededReplication(block, curExpectedReplicas,
                  curReplicas)) {
                if (curExpectedReplicas > curReplicas) {
                  //Log info about one block for this node which needs replication
                  if (!status[0]) {
                    status[0] = true;
                    logBlockReplicationInfo(block, srcNode, num);
                  }
                  underReplicatedBlocks[0]++;
                  if ((curReplicas == 0) &&
                      (num.decommissionedReplicas() > 0)) {
                    decommissionOnlyReplicas[0]++;
                  }
                  if (bc instanceof MutableBlockCollection) {
                    underReplicatedInOpenFiles[0]++;
                  }
                }
                if (!neededReplications.contains(getBlockInfo(block)) &&
                    pendingReplications.getNumReplicas(getBlockInfo(block)) ==
                        0) {
                  //
                  // These blocks have been reported from the datanode
                  // after the startDecommission method has been executed. These
                  // blocks were in flight when the decommissioning was started.
                  //
                  neededReplications.add(getBlockInfo(block), curReplicas,
                      num.decommissionedReplicas(), curExpectedReplicas);
                }
              }
            }
            return null;
          }
        };

    while (it.hasNext()) {
      checkReplicationHandler.setParams(it.next());
      checkReplicationHandler.handle(namesystem);
    }
    srcNode.decommissioningStatus
        .set(underReplicatedBlocks[0], decommissionOnlyReplicas[0],
            underReplicatedInOpenFiles[0]);
    return status[0];
  }

  public int getActiveBlockCount() throws IOException {
    return blocksMap.size();
  }

  public DatanodeDescriptor[] getNodes(BlockInfo block)
      throws StorageException, TransactionContextException {
    DatanodeDescriptor[] toReturn =
        new DatanodeDescriptor[block.numNodes(datanodeManager)];
    List<DatanodeDescriptor> nodes = blocksMap.nodeList(block);
    if (nodes != null){
      for (int i = 0; i < nodes.size() ; i++){
        toReturn[i] = nodes.get(i);
      }
    }
    return toReturn;
  }

  public int getTotalBlocks() throws IOException {
    return blocksMap.size();
  }

  public void removeBlock(Block block)
      throws StorageException, TransactionContextException {
    // No need to ACK blocks that are being removed entirely
    // from the namespace, since the removal of the associated
    // file already removes them from the block map below.
    block.setNumBytesNoPersistance(BlockCommand.NO_ACK);
    addToInvalidates(block);
    corruptReplicas.removeFromCorruptReplicasMap(getBlockInfo(block));
    BlockInfo storedBlock = getBlockInfo(block);
    blocksMap.removeBlock(block);
    // Remove the block from pendingReplications
    pendingReplications.remove(storedBlock);
    if (postponedMisreplicatedBlocks.remove(block)) {
      postponedMisreplicatedBlocksCount.decrementAndGet();
    }
  }

  public BlockInfo getStoredBlock(Block block)
      throws StorageException, TransactionContextException {
    return blocksMap.getStoredBlock(block);
  }

  /**
   * updates a block in under replication queue
   */
  private void updateNeededReplications(final Block block,
      final int curReplicasDelta, int expectedReplicasDelta)
      throws StorageException, TransactionContextException {
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    NumberReplicas repl = countNodes(block);
    int curExpectedReplicas = getReplication(block);
    if (isNeededReplication(block, curExpectedReplicas, repl.liveReplicas())) {
      neededReplications.update(getBlockInfo(block), repl.liveReplicas(),
          repl.decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
          expectedReplicasDelta);
    } else {
      int oldReplicas = repl.liveReplicas() - curReplicasDelta;
      int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
      neededReplications.remove(getBlockInfo(block), oldReplicas,
          repl.decommissionedReplicas(), oldExpectedReplicas);
    }
  }

  /**
   * Check replication of the blocks in the collection.
   * If any block is needed replication, insert it into the replication queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an over replicated block.
   */
  public void checkReplication(BlockCollection bc)
      throws StorageException, TransactionContextException {
    final short expected = bc.getBlockReplication();
    for (Block block : bc.getBlocks()) {
      final NumberReplicas n = countNodes(block);
      if (isNeededReplication(block, expected, n.liveReplicas())) {
        neededReplications.add(getBlockInfo(block), n.liveReplicas(),
            n.decommissionedReplicas(), expected);
      } else if (n.liveReplicas() > expected) {
        processOverReplicatedBlock(block, expected, null, null);
      }
    }
  }

  /**
   * @return 0 if the block is not found;
   * otherwise, return the replication factor of the block.
   */
  private int getReplication(Block block)
      throws StorageException, TransactionContextException {
    final BlockCollection bc = blocksMap.getBlockCollection(block);
    return bc == null ? 0 : bc.getBlockReplication();
  }


  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #invalidateBlocks}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(String nodeId) throws IOException {
    // blocks should not be replicated or removed if safe mode is on
    if (namesystem.isInSafeMode()) {
      LOG.debug("In safemode, not computing replication work");
      return 0;
    }
    // get blocks to invalidate for the nodeId
    assert nodeId != null;
    return invalidateBlocks.invalidateWork(nodeId);
  }

  boolean blockHasEnoughRacks(Block b)
      throws StorageException, TransactionContextException {
    if (!this.shouldCheckForEnoughRacks) {
      return true;
    }
    boolean enoughRacks = false;
    Collection<DatanodeDescriptor> corruptNodes =
        corruptReplicas.getNodes(getBlockInfo(b));
    int numExpectedReplicas = getReplication(b);
    String rackName = null;
    for (DatanodeDescriptor cur : blocksMap.nodeList(b)) {
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null) || !corruptNodes.contains(cur)) {
          if (numExpectedReplicas == 1 || (numExpectedReplicas > 1 &&
              !datanodeManager.hasClusterEverBeenMultiRack())) {
            enoughRacks = true;
            break;
          }
          String rackNameNew = cur.getNetworkLocation();
          if (rackName == null) {
            rackName = rackNameNew;
          } else if (!rackName.equals(rackNameNew)) {
            enoughRacks = true;
            break;
          }
        }
      }
    }
    return enoughRacks;
  }

  /**
   * A block needs replication if the number of replicas is less than expected
   * or if it does not have enough racks.
   */
  private boolean isNeededReplication(Block b, int expected, int current)
      throws StorageException, TransactionContextException {
    return current < expected || !blockHasEnoughRacks(b);
  }

  public long getMissingBlocksCount() throws IOException {
    // not locking
    return this.neededReplications.getCorruptBlockSize();
  }

  public BlockInfo addBlockCollection(BlockInfo block, BlockCollection bc)
      throws StorageException, TransactionContextException {
    return blocksMap.addBlockCollection(block, bc);
  }

  public BlockCollection getBlockCollection(Block b)
      throws StorageException, TransactionContextException {
    return blocksMap.getBlockCollection(b);
  }

  /**
   * @return an iterator of the datanodes.
   */
  public List<DatanodeDescriptor> datanodeList(final Block block)
      throws StorageException, TransactionContextException {
    return blocksMap.nodeList(block);
  }

  public int numCorruptReplicas(Block block)
      throws StorageException, TransactionContextException {
    return corruptReplicas.numCorruptReplicas(getBlockInfo(block));
  }

  public void removeBlockFromMap(Block block)
      throws StorageException, TransactionContextException {
    // If block is removed from blocksMap remove it from corruptReplicasMap
    corruptReplicas.removeFromCorruptReplicasMap(getBlockInfo(block));
    blocksMap.removeBlock(block);
  }

  public int getCapacity() {
    return blocksMap.getCapacity();
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks
   *     Number of block ids to return.
   *     0 <= numExpectedBlocks <= 100
   * @param startingBlockId
   *     Block id from which to start. If null, start at
   *     beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   */
  public long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
      Long startingBlockId) throws IOException {
    return corruptReplicas
        .getCorruptReplicaBlockIds(numExpectedBlocks, startingBlockId);
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  public Iterator<Block> getCorruptReplicaBlockIterator() {
    return neededReplications
        .iterator(UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  /**
   * @return the size of UnderReplicatedBlocks
   */
  public int numOfUnderReplicatedBlocks() throws IOException {
    return neededReplications.size();
  }

  /**
   * Periodically calls computeReplicationWork().
   */
  private class ReplicationMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          if (namesystem.isLeader()) {
            computeDatanodeWork();
            processPendingReplications();
          }
          Thread.sleep(replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException.",
              ie);
          break;
        } catch (StorageException e) {
          LOG.warn("ReplicationMonitor thread received StorageException.", e);
          if(e instanceof TransientStorageException){
            continue;
          }

          terminate(1, e);
        } catch (Throwable t) {
          LOG.fatal("ReplicationMonitor thread received Runtime exception. ",
              t);
          terminate(1, t);
        }
      }
    }
  }


  /**
   * Compute block replication and block invalidation work that can be
   * scheduled
   * on data-nodes. The datanode will be informed of this work at the next
   * heartbeat.
   *
   * @return number of blocks scheduled for replication or removal.
   * @throws IOException
   */
  int computeDatanodeWork() throws IOException {
    // Blocks should not be replicated or removed if in safe mode.
    // It's OK to check safe mode here w/o holding lock, in the worst
    // case extra replications will be scheduled, and these will get
    // fixed up later.
    if (namesystem.isInSafeMode()) {
      return 0;
    }

    final int numlive = heartbeatManager.getLiveDatanodeCount();
    final int blocksToProcess = numlive * this.blocksReplWorkMultiplier;
    final int nodesToProcess =
        (int) Math.ceil(numlive * this.blocksInvalidateWorkPct);

    int workFound = this.computeReplicationWork(blocksToProcess);

    // Update counters
    this.updateState();
    this.scheduledReplicationBlocksCount = workFound;
    workFound += this.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  public void clearQueues() throws IOException {
    neededReplications.clear();
    pendingReplications.clear();
    excessReplicateMap.clear();
    invalidateBlocks.clear();
    datanodeManager.clearPendingQueues();
  }

  ;


  private static class ReplicationWork {

    private Block block;
    private BlockCollection bc;

    private DatanodeDescriptor srcNode;
    private List<DatanodeDescriptor> containingNodes;
    private List<DatanodeDescriptor> liveReplicaNodes;
    private int additionalReplRequired;

    private DatanodeDescriptor targets[];
    private int priority;

    public ReplicationWork(Block block, BlockCollection bc,
        DatanodeDescriptor srcNode, List<DatanodeDescriptor> containingNodes,
        List<DatanodeDescriptor> liveReplicaNodes, int additionalReplRequired,
        int priority) {
      this.block = block;
      this.bc = bc;
      this.srcNode = srcNode;
      this.containingNodes = containingNodes;
      this.liveReplicaNodes = liveReplicaNodes;
      this.additionalReplRequired = additionalReplRequired;
      this.priority = priority;
      this.targets = null;
    }
  }

  /**
   * A simple result enum for the result of
   * {@link BlockManager#processMisReplicatedBlock(BlockInfo)}.
   */
  enum MisReplicationResult {
    /**
     * The block should be invalidated since it belongs to a deleted file.
     */
    INVALID,
    /**
     * The block is currently under-replicated.
     */
    UNDER_REPLICATED,
    /**
     * The block is currently over-replicated.
     */
    OVER_REPLICATED,
    /**
     * A decision can't currently be made about this block.
     */
    POSTPONE,
    /**
     * The block is under construction, so should be ignored
     */
    UNDER_CONSTRUCTION,
    /**
     * The block is properly replicated
     */
    OK
  }


  private void removeStoredBlockTx(final Long b, final DatanodeDescriptor node)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_STORED_BLOCK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlockID(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(b, inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UR, BLK.UC));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        BlockInfo block =
            EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId, b);
        removeStoredBlock(block, node);
        return null;
      }
    }.handle(namesystem);
  }

  @VisibleForTesting
  int computeReplicationWorkForBlock(final Block b, final int priority)
      throws IOException {
    return (Integer) new HopsTransactionalRequestHandler(
        HDFSOperationType.COMPUTE_REPLICATION_WORK_FOR_BLOCK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
            .add(lf.getVariableLock(Variable.Finder.ReplicationIndex,
                LockType.WRITE)).add(
            lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.UR, BLK.UC));
      }

      @Override
      public Object performTask() throws IOException {
        return computeReplicationWorkForBlockInternal(b, priority);
      }
    }.handle(namesystem);
  }

  //TODO? this is only called in a test, should we remove it?
  int computeReplicationWorkForBlocks(List<List<Block>> blocksToReplicate)
      throws IOException {
    int scheduledWork = 0;
    for (int priority = 0; priority < blocksToReplicate.size(); priority++) {
      for (Block block : blocksToReplicate.get(priority)) {
        scheduledWork += computeReplicationWorkForBlock(block, priority);
      }
    }
    return scheduledWork;
  }

  public BlockInfo tryToCompleteBlock(final MutableBlockCollection bc,
      final int blkIndex) throws IOException {

    if (blkIndex < 0) {
      return null;
    }
    BlockInfo curBlock = bc.getBlock(blkIndex);
    LOG.debug("tryToCompleteBlock. blkId = " + curBlock.getBlockId());
    if (curBlock.isComplete()) {
      return curBlock;
    }
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes(datanodeManager);
    if (numNodes < minReplication) {
      return null;
    }
    if (ucBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      return null;
    }
    BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
    // replace penultimate block in file
    bc.setBlock(blkIndex, completeBlock);

    // Since safe-mode only counts complete blocks, and we now have
    // one more complete block, we need to adjust the total up, and
    // also count it as safe, if we have at least the minimum replica
    // count. (We may not have the minimum replica count yet if this is
    // a "forced" completion when a file is getting closed by an
    // OP_CLOSE edit on the standby).
    namesystem.adjustSafeModeBlockTotals(0, 1);
    namesystem.incrementSafeBlockCount(curBlock);

    return completeBlock;
  }

  private void processTimedOutPendingBlock(final long timedOutItemId)
      throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.PROCESS_TIMEDOUT_PENDING_BLOCK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlockID(timedOutItemId);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(timedOutItemId, inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.UR));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        BlockInfo timedOutItem = EntityManager
            .find(BlockInfo.Finder.ByBlockIdAndINodeId, timedOutItemId);
        NumberReplicas num = countNodes(timedOutItem);
        if (isNeededReplication(timedOutItem, getReplication(timedOutItem),
            num.liveReplicas())) {
          neededReplications.add(getBlockInfo(timedOutItem), num.liveReplicas(),
              num.decommissionedReplicas(), getReplication(timedOutItem));
        }
        pendingReplications.remove(timedOutItem);
        return null;
      }
    }.handle(namesystem);
  }

  private BlockInfo getBlockInfo(Block b)
      throws StorageException, TransactionContextException {
    BlockInfo binfo = blocksMap.getStoredBlock(b);
    if (binfo == null) {
      LOG.error("ERROR: Dangling Block. bid=" + b.getBlockId() +
          " setting inodeId to be " + INode.NON_EXISTING_ID);
      binfo = new BlockInfo(b, INode.NON_EXISTING_ID);
    }
    return binfo;
  }

  private Block addStoredBlockTx(final BlockInfo block,
      final DatanodeDescriptor node, final DatanodeDescriptor delNodeHint,
      final boolean logEveryBlock) throws IOException {
    return (Block) new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_BLK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getBlockLock(block.getBlockId(), inodeIdentifier)).add(
            lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.IV, BLK.UR));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        return addStoredBlock(block, node, delNodeHint, logEveryBlock);
      }
    }.handle();
  }

  private void addStoredBlockUnderConstructionTx(
      final BlockInfoUnderConstruction block, final DatanodeDescriptor node,
      final ReplicaState reportedState) throws IOException {

    new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_UC_BLK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.ER, BLK.CR, BLK.PE,
                BLK.UR));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        addStoredBlockUnderConstruction(block, node, reportedState);
        return null;
      }
    }.handle();
  }

  private void addToInvalidates(final Collection<Block> blocks,
      final DatanodeDescriptor node) throws IOException {
    invalidateBlocks.add(blocks, node);
  }

  private void markBlockAsCorruptTx(final BlockToMarkCorrupt b,
      final DatanodeInfo dn) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_CORRUPT_BLK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b.corrupted);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(b.corrupted.getBlockId(),
                inodeIdentifier)).add(
            lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UR, BLK.UC, BLK.IV));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        markBlockAsCorrupt(b, dn);
        return null;
      }
    }.handle();
  }

  public int getTotalCompleteBlocks() throws IOException {
    return blocksMap.sizeCompleteOnly();
  }

  private void addStoredBlockUnderConstructionImmediateTx(
      final BlockInfoUnderConstruction block, final DatanodeDescriptor node,
      final ReplicaState reportedState) throws IOException {

    new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_UC_BLK_IMMEDIATE) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.CR, BLK.ER, BLK.PE,
                BLK.UR));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        block.addExpectedReplicaIfNotPresent(node, reportedState);
        //and fall through to next clause
        //add replica if appropriate
        if (reportedState == ReplicaState.FINALIZED) {
          addStoredBlockImmediate(block, node);
        }
        return null;
      }
    }.handle();
  }

  private void addStoredBlockImmediateTx(final BlockInfo block,
      final DatanodeDescriptor node) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_BLK_IMMEDIATE) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.PE, BLK.IV,
                BLK.UR));
        if (((FSNamesystem) namesystem).isErasureCodingEnabled() &&
            inodeIdentifier != null) {
          locks.add(lf.getIndivdualEncodingStatusLock(LockType.WRITE,
              inodeIdentifier.getInodeId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        addStoredBlockImmediate(block, node);
        return null;
      }
    }.handle();
  }

}
