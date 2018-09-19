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
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.blockmanagement.ExcessReplicasMap;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MisReplicatedRange;
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
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
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
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
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
import org.apache.hadoop.hdfs.server.protocol.ReportedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReportBlockState;
import org.apache.hadoop.hdfs.server.protocol.Bucket;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static io.hops.transaction.lock.LockFactory.BLK;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 */
@InterfaceAudience.Private
public class BlockManager {

  static final Log LOG = LogFactory.getLog(BlockManager.class);
  public static final Log blockLog = NameNode.blockStateChangeLog;

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

  // Max number of blocks to log info about during a block report.
  private final long maxNumBlocksToLog;
  
  /**
   * Process replication queues asynchronously to allow namenode safemode exit
   * and failover to be faster. HDFS-5496
   */
  private Daemon replicationQueuesInitializer = null;

  /**
   * Progress of the Replication queues initialisation.
   */
  private double replicationQueuesInitProgress = 0.0;
  
  /**
   * for block replicas placement
   */
  private BlockPlacementPolicy blockplacement;
  private final BlockStoragePolicySuite storagePolicySuite;
  
  /** Check whether name system is running before terminating */
  private boolean checkNSRunning = true;

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
  /**
   * Number of threads procession batches in parallel
   */
  private final int processMisReplicatedNoThreads;
  
  /**
   * Number of files to process at one batch
   */
  private final int removalBatchSize;
  /**
   * Number of threads procession batches in parallel
   */
  private final int removalNoThreads;
  
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
    blockplacement = BlockPlacementPolicy.getInstance(
        conf, stats, datanodeManager.getNetworkTopology(),
        datanodeManager.getHost2DatanodeMap());
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();
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

    this.maxNumBlocksToLog =
        conf.getLong(DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
            DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);
        
    this.processReportBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE_DEFAULT);

    this.processMisReplicatedBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE_DEFAULT);

    this.processMisReplicatedNoOfBatchs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS,
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS_DEFAULT);

    this.processMisReplicatedNoThreads = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS,
        DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS_DEFAULT);
    
    this.removalBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REMOVAL_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_REMOVAL_BATCH_SIZE_DEFAULT);
    this.removalNoThreads = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REMOVAL_NO_OF_THREADS,
        DFSConfigKeys.DFS_NAMENODE_REMOVAL_NO_OF_THREADS_DEFAULT);
    
    
    LOG.info("defaultReplication         = " + defaultReplication);
    LOG.info("maxReplication             = " + maxReplication);
    LOG.info("minReplication             = " + minReplication);
    LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
    LOG.info("shouldCheckForEnoughRacks  = " + shouldCheckForEnoughRacks);
    LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
    LOG.info("encryptDataTransfer        = " + encryptDataTransfer);
    LOG.info("maxNumBlocksToLog          = " + maxNumBlocksToLog);
    LOG.info("misReplicatedBatchSize     = " + processMisReplicatedBatchSize);
    LOG.info("misReplicatedNoOfBatchs    = " + processMisReplicatedNoOfBatchs);   
    LOG.info("misReplicatedNoOfThreads   = " + processMisReplicatedNoThreads);   
    LOG.info("removalBatchSize           = " + removalBatchSize);
    LOG.info("removalNoOfThreads         = " + removalNoThreads);   
  }

  private NameNodeBlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled =
        conf.getBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
            DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

    if (!isEnabled) {
      if (UserGroupInformation.isSecurityEnabled()) {
	      LOG.error("Security is enabled but block access tokens " +
		      "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
		      "aren't enabled. This may cause issues " +
		      "when clients attempt to talk to a DataNode.");
      }
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
  
  public BlockStoragePolicy getDefaultStoragePolicy(){
    return storagePolicySuite.getDefaultPolicy();
  }
  
  public BlockStoragePolicy getStoragePolicy(final String policyName) {
    return storagePolicySuite.getPolicy(policyName);
  }

  public BlockStoragePolicy getStoragePolicy(final byte policyId) {
    return storagePolicySuite.getPolicy(policyId);
  }

  public BlockStoragePolicy[] getStoragePolicies() {
    return storagePolicySuite.getAllPolicies();
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

  /** Allow silent termination of replication monitor for testing */
  @VisibleForTesting
  void enableRMTerminationForTesting() {
    checkNSRunning = false;
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

  @VisibleForTesting
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
      throws IOException {
    List<DatanodeDescriptor> containingNodes =
        new ArrayList<>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
        new ArrayList<DatanodeStorageInfo>();

    NumberReplicas numReplicas = new NumberReplicas();
    // source node returned is not used
    chooseSourceDatanode(block, containingNodes,
        containingLiveReplicasNodes, numReplicas,
        UnderReplicatedBlocks.LEVEL);

    // containingLiveReplicasNodes can include READ_ONLY_SHARED replicas which are
    // not included in the numReplicas.liveReplicas() count
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
        numReplicas.decommissionedReplicas();

    if (block instanceof BlockInfo) {
      BlockCollection bc = ((BlockInfo) block).getBlockCollection();
      String fileName = (bc == null) ? "[orphaned]" : bc.getName();
      out.print(fileName + ": ");
    }
    // l: == live:, d: == decommissioned c: == corrupt e: == excess
    out.print(block + ((usableReplicas > 0)? "" : " MISSING") +
        " (replicas:" +
        " l: " + numReplicas.liveReplicas() +
        " d: " + numReplicas.decommissionedReplicas() +
        " c: " + numReplicas.corruptReplicas() +
        " e: " + numReplicas.excessReplicas() + ") ");

    Collection<DatanodeDescriptor> corruptNodes =
        corruptReplicas.getNodes(getBlockInfo(block));

    for(DatanodeStorageInfo storage: blocksMap.storageList(block)){
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
        state = "(corrupt)";
      } else if (node.isDecommissioned() ||
          node.isDecommissionInProgress()) {
        state = "(decommissioned)";
      }

      if (storage.areBlockContentsStale()) {
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
      throws IOException {
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
      final Block commitBlock, DatanodeManager datanodeMgr) throws IOException, StorageException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED) {
      return false;
    }
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
        "commitBlock length is less than the stored one " +
            commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    block.commitBlock(commitBlock, datanodeMgr);
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
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
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
    
    final boolean b = commitBlock((BlockInfoUnderConstruction) lastBlock, commitBlock, getDatanodeManager());
    LOG.debug("commitOrCompleteLastBlock for block " + lastBlock.getBlockId());

    int numReplicas = countNodes(lastBlock).liveReplicas();
    if (numReplicas >= minReplication) {
      completeBlock(bc, lastBlock.getBlockIndex(), false);
      LOG.debug("commitOrCompleteLastBlock. Completed Block " +
          lastBlock.getBlockId());
    } else {
      LOG.debug("commitOrCompleteLastBlock. Completed FAILED. " +
          "Block " + lastBlock.getBlockId() + ": " +
          "needed " + minReplication + " replicas, but only has " + numReplicas);
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
  private BlockInfo completeBlock(final BlockCollection bc,
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
    namesystem.adjustSafeModeBlockTotals(null, 1);
    namesystem.incrementSafeBlockCount(Math.min(numNodes, minReplication), curBlock);

    return completeBlock;
  }
  
  private BlockInfo completeBlock(final BlockCollection bc,
      final BlockInfo block, boolean force)
      throws IOException, StorageException {
    BlockInfo blk = bc.getBlock(block.getBlockIndex());
    if (blk == block) {
      return completeBlock(bc, blk.getBlockIndex(), force);
    }
    return block;
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  public BlockInfo forceCompleteBlock(final BlockCollection bc,
      final BlockInfoUnderConstruction block)
      throws IOException, StorageException {
    block.commitBlock(block, getDatanodeManager());
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
  public LocatedBlock convertLastBlockToUnderConstruction(BlockCollection bc) throws IOException {
    BlockInfo oldBlock = bc.getLastBlock();
    if (oldBlock == null ||
        bc.getPreferredBlockSize() == oldBlock.getNumBytes()) {
      return null;
    }
    assert oldBlock ==
        getStoredBlock(oldBlock) : "last block of the file is not in blocksMap";

    DatanodeStorageInfo[] targets = getStorages(oldBlock);

    BlockInfoUnderConstruction ucBlock = bc.setLastBlock(oldBlock, targets);

    // Remove block from replication queue.
    NumberReplicas replicas = countNodes(ucBlock);
    neededReplications.remove(ucBlock, replicas.liveReplicas(),
        replicas.decommissionedReplicas(), getReplication(ucBlock));
    pendingReplications.remove(ucBlock);

    // remove this block from the list of pending blocks to be deleted. 
    for (DatanodeStorageInfo target : targets) {
      invalidateBlocks.remove(target, oldBlock);
    }

    // Adjust safe-mode totals, since under-construction blocks don't
    // count in safe-mode.
    List<Block> deltaSafe = new ArrayList<>();
    // decrement safe if we had enough
    if(targets.length >= minReplication){
      deltaSafe.add(oldBlock);
    }
    namesystem.adjustSafeModeBlockTotals(
        deltaSafe,
        // always decrement total blocks
        -1);

    final long fileLength = bc.computeContentSummary().getLength();
    final long pos = fileLength - ucBlock.getNumBytes();
    return createLocatedBlock(ucBlock, pos, AccessMode.WRITE);
  }

  /**
   * Get all valid locations of the block
   */
  private List<DatanodeStorageInfo> getValidLocations(BlockInfo block)
      throws StorageException, TransactionContextException {
    ArrayList<DatanodeStorageInfo> storageSet = new ArrayList<DatanodeStorageInfo>();
    for (DatanodeStorageInfo storage : blocksMap.storageList(block)){
      // filter invalid replicas
      if (!invalidateBlocks.contains(storage, block)) {
        storageSet.add(storage);
      }
    }

    return storageSet;
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
  
  private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      long blkSize = blocks[curBlk].getNumBytes();
      if (curPos + blkSize >= endPos) {
        break;
      }
      curPos += blkSize;
    }
    
    return createLocatedBlock(blocks[curBlk], curPos, mode);
  }

  private List<LocatedBlock> createPhantomLocatedBlockList(INodeFile file, final byte[] data,
      final AccessMode mode) throws IOException, StorageException {
    List<LocatedBlock> results = new ArrayList<>(1);
    BlockInfo fakeBlk = new BlockInfo();
    fakeBlk.setBlockIdNoPersistance(-file.getId());
    fakeBlk.setINodeIdNoPersistance(-file.getId());
    fakeBlk.setBlockIndexNoPersistance(0);
    fakeBlk.setNumBytesNoPersistance(file.getSize());
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
              DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
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
      final DatanodeStorageInfo[] locations = uc.getExpectedStorageLocations(datanodeManager);
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
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numMachines];
    int j = 0;
    if (numMachines > 0) {
      for (final DatanodeStorageInfo storage : blocksMap.storageList(blk)){
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk,
            storage.getDatanodeDescriptor());
        if (isCorrupt || (!isCorrupt && !replicaCorrupt)) {
          storages[j++] = storage;
        }
      }
    }

    assert j == storages.length : "isCorrupt: " + isCorrupt +
        " numStorages: " + numMachines +
        " numNodes: " + numNodes +
        " numCorrupt: " + numCorruptNodes +
        " numCorruptRepls: " + numCorruptReplicas;
    final ExtendedBlock eb =
        new ExtendedBlock(namesystem.getBlockPoolId(), blk);
    return new LocatedBlock(eb, storages, pos, isCorrupt);
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

    return new LocatedBlocks(file.getSize(),
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
      final List<LocatedBlock> locatedblocks = createLocatedBlockList(blocks, offset, length, Integer.MAX_VALUE,
          mode);

      final BlockInfo last = blocks[blocks.length - 1];
      final long lastPos = last.isComplete() ? fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
          : fileSizeExcludeBlocksUnderConstruction;
      final LocatedBlock lastlb = createLocatedBlock(last, lastPos, mode);
      final boolean isComplete = last.isComplete();

      return new LocatedBlocks(fileSizeExcludeBlocksUnderConstruction,
          isFileUnderConstruction, locatedblocks, lastlb, isComplete);
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
      // Use cached UGI if serving RPC calls.
      b.setBlockToken(blockTokenSecretManager.generateToken(
          NameNode.getRemoteUser().getShortUserName(),
          b.getBlock(), EnumSet.of(mode)));
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
   * Check if a block is replicated to at least the minimum replication.
   */
  public boolean isSufficientlyReplicated(BlockInfo b) throws IOException {
    // Compare against the lesser of the minReplication and number of live DNs.
    final int replication =
        Math.min(minReplication, getDatanodeManager().getNumLiveDataNodes());


    return countLiveNodes(b) >= replication;
    //countNodes(b).liveReplicas() >= replication;
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
    Iterator<BlockInfo> iter = node.getBlockIterator(false);
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
      List<Block> toAdd = new ArrayList<>();
      long estimatedSize = 0;
      while(totalSize+ estimatedSize < size && iter.hasNext()){
        curBlock = iter.next();
        if (!curBlock.isComplete()) {
          continue;
        }
        toAdd.add(curBlock);
        estimatedSize += curBlock.getNumBytes();
      }
      totalSize += addBlocks(toAdd, results);
    }
    if (totalSize < size) {
      iter = node.getBlockIterator(false); // start from the beginning
      for (int i = 0; i < startBlock && totalSize < size; ) {
        List<Block> toAdd = new ArrayList<>();
        long estimatedSize = 0;
        while (totalSize + estimatedSize < size && i<startBlock) {
          curBlock = iter.next();
          i++;
          if (!curBlock.isComplete()) {
            continue;
          }
          toAdd.add(curBlock);
          estimatedSize += curBlock.getNumBytes();
        }
        totalSize += addBlocks(toAdd, results);
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
    final Iterator<? extends Block> it = node.getBlockIterator(true);
    final List<Long> allBlockIds = new ArrayList<>();
    while (it.hasNext()) {
      allBlockIds.add(it.next().getBlockId());
    }
    
    removeBlocks(allBlockIds, node);

    node.resetBlocks();
    List<Integer> sids = datanodeManager.getSidsOnDatanode(node.getDatanodeUuid());
    invalidateBlocks.remove(sids);

    // If the DN hasn't block-reported since the most recent
    // failover, then we may have been holding up on processing
    // over-replicated blocks because of it. But we can now
    // process those blocks.
    boolean stale = false;
    for(DatanodeStorageInfo storage : node.getStorageInfos()) {
      if (storage.areBlockContentsStale()) {
        stale = true;
        break;
      }
    }
    if (stale) {
      rescanPostponedMisreplicatedBlocks();
    }
  }

  /** Remove the blocks associated to the given DatanodeStorageInfo. */
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo)
      throws IOException {
    final Iterator<BlockInfo> it = storageInfo.getBlockIterator(true);
    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    final List<Long> allBlockIds = new ArrayList<>();
    while (it.hasNext()) {
      allBlockIds.add(it.next().getBlockId());
    }

    removeBlocks(allBlockIds, node);

    invalidateBlocks.remove(storageInfo.getSid());

    namesystem.checkSafeMode();
  }

    void removeBlocksAssociatedTo(final int sid)
      throws IOException {
    final Iterator<BlockInfo> it = getAllStorageBlockInfos(sid).iterator();
    final List<Long> allBlockIds = new ArrayList<>();
    while (it.hasNext()) {
      allBlockIds.add(it.next().getBlockId());
    }

    removeBlocks(allBlockIds, sid);

    invalidateBlocks.remove(sid);

    namesystem.checkSafeMode();
  }
    
  private List<BlockInfo> getAllStorageBlockInfos(final int sid) throws IOException {

    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
            .getDataAccess(BlockInfoDataAccess.class);
        HdfsStorageFactory.getConnector().beginTransaction();
        List<BlockInfo> list = da.findBlockInfosByStorageId(sid);
        HdfsStorageFactory.getConnector().commit();
        return list;
      }
    };
    return (List<BlockInfo>) findBlocksHandler.handle();
  }
  
  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block, final DatanodeInfo datanode)
      throws StorageException, TransactionContextException,
      UnregisteredNodeException {
    DatanodeDescriptor dn = datanodeManager.getDatanode(datanode);
    DatanodeStorageInfo storage = getBlockInfo(block).getStorageOnNode(dn);
    if(storage!=null){
      addToInvalidates(block, storage);
    }
  }
   
  void addToInvalidates(Block block, DatanodeStorageInfo storage)
      throws TransactionContextException, StorageException {
    BlockInfo temp = getBlockInfo(block);
    invalidateBlocks.add(temp, storage, true);
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(Block b)
      throws StorageException, TransactionContextException {
    StringBuilder datanodes = new StringBuilder();
    BlockInfo block = getBlockInfo(b);

    DatanodeStorageInfo[] storages = getBlockInfo(block).getStorages(datanodeManager, DatanodeStorage.State.NORMAL);
    for(DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      invalidateBlocks.add(block, storage, false);
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.info("BLOCK* addToInvalidates: " + block + " " + datanodes);
    }
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   * @param storageID if known, null otherwise.
   * @param reason a textual reason why the block should be marked corrupt,
   * for logging purposes
   */
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, final String storageID, final String reason) throws
      IOException {

    final DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    final DatanodeStorageInfo storage =
        storageID == null ? null : node.getStorageInfo(storageID);

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


        BlockToMarkCorrupt b = new BlockToMarkCorrupt(storedBlock, blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED);
        markBlockAsCorrupt(b, storage, node);

        return null;
      }
    }.handle(namesystem);
  }

  /**
   *
   * @param b
   * @param storageInfo storage that contains the block, if known. null otherwise.
   * @param node the node that contains the block.
   * @throws IOException
   */
  private void markBlockAsCorrupt(BlockToMarkCorrupt b,
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException, StorageException {

    BlockCollection bc = b.corrupted.getBlockCollection();
    if (bc == null) {
      blockLog.info("BLOCK markBlockAsCorrupt: " + b +
          " cannot be marked as corrupt as it does not belong to any file");
      addToInvalidates(b.corrupted, node);
      return;
    }

    // Lookup which storage we are working on if we didn't know it yet
    if(storageInfo == null) {
      storageInfo = b.corrupted.getStorageOnNode(node);
    }

    // Add replica to the storage if it is not already there
    if (storageInfo != null) {
      storageInfo.addBlock(b.stored);
    }

    // Add this replica to corruptReplicas Map
    corruptReplicas.addToCorruptReplicasMap(b.corrupted, storageInfo, b.reason, b.reasonCode);

    NumberReplicas numberOfReplicas = countNodes(b.stored);
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >= bc
        .getBlockReplication();
    boolean minReplicationSatisfied =
        numberOfReplicas.liveReplicas() >= minReplication;
    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
            bc.getBlockReplication();
    boolean corruptedDuringWrite = minReplicationSatisfied &&
        (b.stored.getGenerationStamp() > b.corrupted.getGenerationStamp());
    // case 1: have enough number of live replicas
    // case 2: corrupted replicas + live replicas > Replication factor
    // case 3: Block is marked corrupt due to failure while writing. In this
    //         case genstamp will be different than that of valid block.
    // In all these cases we can delete the replica.
    // In case of 3, rbw block will be deleted and valid block can be replicated
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(b, node);
    } else if (namesystem.isPopulatingReplQueues()) {
      // add the block to neededReplication
      updateNeededReplications(b.stored, -1, 0);
    }

    // HDFS stops here, but we have to check if Erasure Coding is enabled,
    // and if we need to use it to restore this block. If there are no
    // replicas of this block, we can still restore using the parity blocks:
    FSNamesystem fsNamesystem = (FSNamesystem) namesystem;
    if (!fsNamesystem.isErasureCodingEnabled()) {
      return;
    }

    if (numberOfReplicas.liveReplicas() == 0) {
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
   * @return true if the block was successfully invalidated and no longer
   * present in the BlocksMap
   */
  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn
  ) throws IOException {
//  /**
//   * Invalidates the given block on the given storage.
//   */
//  private void invalidateBlock(BlockToMarkCorrupt b,
//      DatanodeStorageInfo storage) throws IOException, StorageException {
    blockLog.info("BLOCK* invalidateBlock: " + b + " on " + dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate " + b
          + " because datanode " + dn + " does not exist.");
    }

    // Check how many copies we have of the block
    NumberReplicas nr = countNodes(b.stored);
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.info("BLOCK* invalidateBlocks: postponing " +
          "invalidation of " + b + " on " + dn + " because " +
          nr.replicasOnStaleNodes() + " replica(s) are located on nodes " +
          "with potentially out-of-date block reports");
      postponeBlock(b.corrupted);
      return false;
    } else if (nr.liveReplicas() >= 1) {
      // If we have at least one copy on a live node, then we can delete it.
      addToInvalidates(b.corrupted, dn);
      removeStoredBlock(b.stored, node);
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* invalidateBlocks: " + b + " on " + dn +
            " listed for deletion.");
      }
      return true;
    } else {
      blockLog.info("BLOCK* invalidateBlocks: " + b + " on " + dn +
          " is the only copy and was not deleted");
      return false;
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
    final Map<DatanodeInfo, List<Integer>> nodesToSids = invalidateBlocks.getDatanodes(datanodeManager);
    List<Map.Entry<DatanodeInfo, List<Integer>>> nodes = new ArrayList<>(nodesToSids.entrySet());
    Collections.shuffle(nodes);

    nodesToProcess = Math.min(nodes.size(), nodesToProcess);

    int blockCnt = 0;
    for (Map.Entry<DatanodeInfo, List<Integer>> dnInfo : nodes) {

      int blocks = invalidateWorkForOneNode(dnInfo);
      if (blocks > 0) {
        blockCnt += blocks;
        if (--nodesToProcess == 0) {
          break;
        }
      }
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

    return computeReplicationWorkForBlocks(blocksToReplicate);
  }

  /**
   * Replicate a set of blocks
   * Calls {@link #computeReplicationWorkForBlock(Block, int)} for every block.
   *
   * @param blocksToReplicate blocks to be replicated, for each priority
   * @return the number of blocks scheduled for replication
   */
  @VisibleForTesting
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

  /**
   * Replicate a set of blocks
   *
   * @return the number of blocks scheduled for replication
   */
  private int computeReplicationWorkForBlockInternal(Block blk, int priority1)
      throws StorageException, IOException {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    BlockCollection bc = null;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<>();

    synchronized (neededReplications) {
      // block should belong to a file
      bc = blocksMap.getBlockCollection(blk);
      // abandoned block or block reopened for append
      if (bc == null || (bc.isUnderConstruction() && getBlockInfo(blk).equals(bc.getLastBlock()))) {
        // remove from neededReplications
        neededReplications.remove(getBlockInfo(blk), priority1);
        neededReplications.decrementReplicationIndex(priority1);
        return scheduledWork;
      }

      requiredReplication = bc.getBlockReplication();

      // get a source data-node
      containingNodes = new ArrayList<>();
      List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<>();
      NumberReplicas numReplicas = new NumberReplicas();
      srcNode = chooseSourceDatanode(blk, containingNodes, liveReplicaNodes,
          numReplicas, priority1);
      if (srcNode == null) { // block can not be replicated from any storage
        LOG.debug("Block " + blk + " cannot be repl from any storage");
        return scheduledWork;
      }

      // liveReplicaNodes can include READ_ONLY_SHARED replicas which are 
      // not included in the numReplicas.liveReplicas() count
      assert liveReplicaNodes.size() >= numReplicas.liveReplicas();

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
    final Set<Node> excludedNodes = new HashSet<>();
    for (ReplicationWork rw : work) {
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      excludedNodes.clear();
      for (DatanodeDescriptor dn : rw.containingNodes) {
        excludedNodes.add(dn);
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      // It is costly to extract the filename for which chooseTargets is called,
      // so for now we pass in the blk collection itself.

      rw.chooseTargets(blockplacement, storagePolicySuite, excludedNodes);
    }

    for (ReplicationWork rw : work) {
      final DatanodeStorageInfo[] targets = rw.targets;
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
        if (bc == null || (bc instanceof MutableBlockCollection && getBlockInfo(blk).equals(bc.getLastBlock()))) {
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
              .equals(targets[0].getDatanodeDescriptor().getNetworkLocation())) {
            //No use continuing, unless a new rack in this case
            continue;
          }
        }

        // Add block to the to be replicated list
        rw.srcNode.addBlockToBeReplicated(block, targets);
        scheduledWork++;
        DatanodeStorageInfo.incrementBlocksScheduled(targets);

        // Move the block-replication into a "pending" state.
        // The reason we use 'pending' is so we can retry
        // replications that fail after an appropriate amount of time.
        pendingReplications.increment(getBlockInfo(block), DatanodeStorageInfo.toDatanodeDescriptors(targets));
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
        DatanodeStorageInfo[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (DatanodeStorageInfo target : targets) {
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
   * Choose target datanodes for creating a new block.
   *
   * @throws IOException
   *           if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node, List, boolean, Set, long, BlockStoragePolicy)
   */
  public DatanodeStorageInfo[] chooseTarget4NewBlock(final String src,
      final int numOfReplicas, final Node client,
      final Set<Node> excludedNodes,
      final long blocksize,
      final List<String> favoredNodes,
      final byte storagePolicyID) throws IOException {
    List<DatanodeDescriptor> favoredDatanodeDescriptors =
        getDatanodeDescriptors(favoredNodes);

    BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);

    final DatanodeStorageInfo[] targets = blockplacement.chooseTarget(src,
        numOfReplicas, client, excludedNodes, blocksize,
        favoredDatanodeDescriptors, storagePolicy);

    if (targets.length < minReplication) {
      throw new IOException("File " + src
          + " could only be replicated to " + targets.length + " nodes "
          +  "instead of minReplication (=" + minReplication + ").  "
          +  "There are " + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
          + " datanode(s) running and "
          + (excludedNodes == null? "no": excludedNodes.size())
          + " node(s) are excluded in this operation. "
          + (excludedNodes != null ? Arrays.toString(excludedNodes.toArray(new Node[excludedNodes.size()])) : "[]"));
    }
    return targets;
  }

  /** Choose target for WebHDFS redirection. */
  public DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize) {
    return blockplacement.chooseTarget(src, 1, clientnode,
        Collections.<DatanodeStorageInfo>emptyList(), false, excludes,
        blocksize, storagePolicySuite.getDefaultPolicy());
  }

  /** Choose target for getting additional datanodes for an existing pipeline. */
  public DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes,
      Node clientnode,
      List<DatanodeStorageInfo> chosen,
      Set<Node> excludes,
      long blocksize,
      byte storagePolicyID) {
    
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    return blockplacement.chooseTarget(src, numAdditionalNodes, clientnode,
        chosen, true, excludes, blocksize, storagePolicy);
  }
  
  public DatanodeStorageInfo[] chooseTarget4ParityRepair(String src,  int numOfReplicas,
      DatanodeDescriptor clientnode,List<DatanodeStorageInfo> chosen, Set<Node> excludes, long blocksize,
      byte storagePolicyID) {
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    return blockplacement.chooseTarget(src, numOfReplicas, clientnode,
        chosen, false, excludes, blocksize, storagePolicy);
  }


  /**
   * Get list of datanode descriptors for given list of nodes. Nodes are
   * hostaddress:port or just hostaddress.
   */
  List<DatanodeDescriptor> getDatanodeDescriptors(List<String> nodes) {
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<DatanodeDescriptor>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
          datanodeDescriptors.add(node);
        }
      }
    }
    return datanodeDescriptors;
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
   * @param b
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
   * @return the DatanodeStorageInfo of the chosen storage from which to
   * replicate the given block
   */
  @VisibleForTesting
  DatanodeDescriptor chooseSourceDatanode(Block b,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> nodesContainingLiveReplicas,
      NumberReplicas numReplicas, int priority)
      throws IOException {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    final BlockInfo block = getBlockInfo(b);

    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    for(DatanodeStorageInfo storage : block.getStorages(datanodeManager)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0; 
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt += countableReplica;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned+= countableReplica;
      } else if (excessReplicateMap.contains(storage, block)) {
          excess+= countableReplica;
      } else {
        nodesContainingLiveReplicas.add(storage);
        live+= countableReplica;
      }
      if(!containingNodes.contains(node)) {
        containingNodes.add(node);
      }


      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node)) {
        continue;
      }
      if (priority != UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY
          && !node.isDecommissionInProgress()
          && node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams) {
        continue; // already reached replication limit
      }
      if (node.getNumberOfBlocksToBeReplicated() >= replicationStreamsHardLimit) {
        continue;
      }
      // the block must not be scheduled for removal on srcNode
      if (excessReplicateMap.contains(storage, block)) {
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

      // We got this far, current node is a reasonable choice
      if (srcNode == null) {
        srcNode = node;
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
  static class StatefulBlockInfo {
    final BlockInfoUnderConstruction storedBlock;
    final Block reportedBlock;
    final ReplicaState reportedState;

    StatefulBlockInfo(BlockInfoUnderConstruction storedBlock,
        Block reportedBlock, ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedBlock = reportedBlock;
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
    /** The reason code to be stored */
    final Reason reasonCode;

    BlockToMarkCorrupt(BlockInfo corrupted, BlockInfo stored, String reason, Reason reasonCode) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      Preconditions.checkNotNull(stored, "stored is null");

      this.corrupted = corrupted;
      this.stored = stored;
      this.reason = reason;
      this.reasonCode = reasonCode;
    }

    BlockToMarkCorrupt(BlockInfo stored, String reason, Reason reasonCode) {
      this(stored, stored, reason, reasonCode);
    }

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason, Reason reasonCode) {
      this(new BlockInfo(stored), stored, reason, reasonCode);
      //the corrupted block in datanode has a different generation stamp
      corrupted.setGenerationStampNoPersistance(gs);
    }

    @Override
    public String toString() {
      return corrupted + "("
          + (corrupted == stored? "same as stored": "stored=" + stored) + ")";
    }
  }

  /**
   * The given storage is reporting all its blocks.
   * Update the (storage-->block list) and (block-->storage list) maps.
   */
  public boolean processReport(final DatanodeID nodeID,
      final DatanodeStorage storage,
      final BlockReport newReport) throws IOException {
    final long startTime = Time.now(); //after acquiring write lock

    DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      throw new IOException(
          "ProcessReport from dead or unregistered node: " + nodeID);
    }

    DatanodeStorageInfo storageInfo = node.getStorageInfo(storage.getStorageID());
    if (storageInfo == null) {
      // We handle this for backwards compatibility.
      storageInfo = node.updateStorage(storage);
    }

    // To minimize startup time, we discard any second (or later) block reports
    // that we receive while still in startup phase.
    if (namesystem.isInStartupSafeMode() && storageInfo.getBlockReportCount() > 0) {
      blockLog.info("BLOCK* processReport: " +
          "discarded non-initial block report from " + nodeID +
          " because namenode still in startup phase");
      return !node.hasStaleStorages();
    }
    ReportStatistics reportStatistics = null;
    try {
      // Get the storageinfo object that we are updating in this processreport
      reportStatistics = processReport(storageInfo, newReport);

      // Now that we have an up-to-date block report, we know that any
      // deletions from a previous NN iteration have been accounted for.
      boolean staleBefore = storageInfo.areBlockContentsStale();
      storageInfo.receivedBlockReport();
      if (staleBefore && !storageInfo.areBlockContentsStale()) {
        LOG.info(
            "BLOCK* processReport: Received first block report from " + node
            + " after becoming active. Its block contents are no longer" + " considered stale");
        rescanPostponedMisreplicatedBlocks();
      }

      final long endTime = Time.now();

      // Log the block report processing stats from Namenode perspective
      final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
      if (metrics != null) {
        metrics.addBlockReport((int) (endTime - startTime));
      }
      blockLog.info("BLOCK* processReport success: from " + nodeID + " storage: " + storage + ", blocks: " + newReport.
          getNumberOfBlocks() + ", processing time: " + (endTime - startTime) + " ms. " + reportStatistics);
      return !node.hasStaleStorages();
    } catch (Throwable t) {
      final long endTime = Time.now();
      blockLog.error("BLOCK* processReport fail: from " + nodeID + " storage: " + storage + ", blocks: " + newReport.
          getNumberOfBlocks() + ", processing time: " + (endTime - startTime) + " ms. " + reportStatistics, t);
      throw t;
    }
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
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
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

  @VisibleForTesting
  public ReportStatistics processReport(final DatanodeStorageInfo storage,final BlockReport report) throws
      IOException {
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
        namesystem.isInStartupSafeMode() || storage.getBlockReportCount() == 0;
    if (storage.getBlockReportCount() == 0){
      HashBuckets.getInstance().createBucketsForStorage(storage);
    }
    ReportStatistics reportStatistics = reportDiff(storage, report, toAdd, toRemove, toInvalidate, toCorrupt,
        toUC, firstBlockReport);


    // Process the blocks on each queue
    for (StatefulBlockInfo b : toUC) {
      if (firstBlockReport) {
        addStoredBlockUnderConstructionImmediateTx(b.storedBlock, storage, b.reportedState);
      } else {
        addStoredBlockUnderConstructionTx(b, storage);
      }
    }
  
  
    final List<Callable<Object>> addTasks = new ArrayList<>();
    int numBlocksLogged = 0;
    for (final BlockInfo b : toAdd) {
      if (firstBlockReport) {
        addStoredBlockImmediateTx(b, storage, numBlocksLogged < maxNumBlocksToLog);
      } else {
        final boolean logIt =  numBlocksLogged < maxNumBlocksToLog;
        addTasks.add(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            addStoredBlockTx(b, storage, null, logIt);
            return null;
          }
        });
      }
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.info("BLOCK* processReport: logged info for " + maxNumBlocksToLog
          + " of " + numBlocksLogged + " reported.");
    }
    try {
      List<Future<Object>> futures = ((FSNamesystem) namesystem).getSubtreeOperationsExecutor().invokeAll(addTasks);
      //Check for exceptions
      for (Future<Object> maybeException : futures){
        maybeException.get();
      }
    } catch (InterruptedException e) {
      LOG.error(e);
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  
  
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorruptTx(b, storage);
    }

    for (Block b : toInvalidate) {
      blockLog.info("BLOCK* processReport: " + b + " on " + storage + " " +
          storage + " size " + b.getNumBytes() +
          " does not belong to any file");
    }
    addToInvalidates(toInvalidate, storage);

    removeBlocks(toRemove, storage.getDatanodeDescriptor());

    return reportStatistics;
  }
  
  @VisibleForTesting
  public void removeBlocks(Collection<Long> allBlockIds, final DatanodeDescriptor node) throws IOException {
    long[] array = new long[allBlockIds.size()];
    int i = 0;
    for (long blockId : allBlockIds) {
      array[i] = blockId;
      i++;
    }
    final Map<Integer, List<Long>> inodeIdsToBlockMap = INodeUtil.getINodeIdsForBlockIds(array);
    final List<Integer> inodeIds = new ArrayList<>(inodeIdsToBlockMap.keySet());

    try {
      Slicer.slice(inodeIds.size(), removalBatchSize, removalNoThreads,
          new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex)
            throws Exception {
          List<Integer> ids = inodeIds.subList(startIndex, endIndex);
          removeStoredBlocksTx(ids, inodeIdsToBlockMap, node);
        }
      });
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  private void removeBlocks(List<Long> allBlockIds, final int sid) throws IOException {
    long[] array = new long[allBlockIds.size()];
    int i = 0;
    for (long blockId : allBlockIds) {
      array[i] = blockId;
      i++;
    }
    final Map<Integer, List<Long>> inodeIdsToBlockMap = INodeUtil.getINodeIdsForBlockIds(array);
    final List<Integer> inodeIds = new ArrayList<>(inodeIdsToBlockMap.keySet());

    try {
      Slicer.slice(inodeIds.size(), removalBatchSize, removalNoThreads,
          new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex)
            throws Exception {
          List<Integer> ids = inodeIds.subList(startIndex, endIndex);
          removeStoredBlocksTx(ids, inodeIdsToBlockMap, sid);
        }
      });
    } catch (Exception ex) {
      throw new IOException(ex);
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
  
  public class ReportStatistics{
    int numBuckets;
    public int numBucketsMatching;
    int numBlocks;
    int numToRemove;
    int numToInvalidate;
    int numToCorrupt;
    int numToUC;
    int numToAdd;
    int numConsideredSafeIfInSafemode;
  
    @Override
    public String toString() {
      return String.format("(buckets,bucketsMatching,blocks,toRemove,toInvalidate,toCorrupt,toUC,toAdd," +
          "safeBlocksIfSafeMode)=(%d,%d,%d,%d,%d,%d,%d,%d,%d)", numBuckets, numBucketsMatching, numBlocks,
          numToRemove, numToInvalidate, numToCorrupt, numToUC, numToAdd,numConsideredSafeIfInSafemode);
    }
  }

  private ReportStatistics reportDiff(final DatanodeStorageInfo storage,
      final BlockReport newReport,
      final Collection<BlockInfo> toAdd, // add to DatanodeStorageInfo
      final Collection<Long> toRemove, // remove from DatanodeStorageInfo
      final Collection<Block> toInvalidate, // should be removed from Storage
      final Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      final Collection<StatefulBlockInfo> toUC,
      final boolean firstBlockReport)
      throws IOException { // add to under-construction list

    if (newReport == null) {
      return null;
    }
    // Get all invalidated replica's
    final Map<Long,Long> invalidatedReplicas = storage
        .getAllStorageInvalidatedReplicasWithGenStamp();
    
    ReportStatistics stats = new ReportStatistics();
    stats.numBuckets = newReport.getBuckets().length;
    stats.numBlocks = newReport.getNumberOfBlocks();
  
    HashMatchingResult matchingResult = calculateMismatchedHashes(storage, newReport, firstBlockReport);
    stats.numBucketsMatching = matchingResult.matchingBuckets.size();
    
    
    if(LOG.isDebugEnabled()){
      LOG.debug(String.format("%d/%d reported hashes matched",
          newReport.getHashes().length-matchingResult.mismatchedBuckets.size(),
          newReport.getHashes().length));
    }
    
    final Set<Long> aggregatedSafeBlocks = new HashSet<>();
    
    final Map<Long, Integer> mismatchedBlocksAndInodes = storage
            .getAllStorageReplicasInBuckets(matchingResult.mismatchedBuckets);

    final Set<Long> allMismatchedBlocksOnServer = mismatchedBlocksAndInodes.keySet();
    //Safe mode report and first report for storage will have all buckets mismatched.
    aggregatedSafeBlocks.addAll(allMismatchedBlocksOnServer);

    processMisMatchingBuckets(storage, newReport, matchingResult, toAdd,
            toInvalidate,
            toCorrupt, toUC, firstBlockReport,
            mismatchedBlocksAndInodes,
            aggregatedSafeBlocks, allMismatchedBlocksOnServer,
            invalidatedReplicas);

    stats.numToAdd = toAdd.size();
    stats.numToInvalidate = toInvalidate.size();
    stats.numToCorrupt = toCorrupt.size();
    stats.numToUC = toUC.size();
    toRemove.addAll(allMismatchedBlocksOnServer);
    stats.numToRemove = toRemove.size();
    if (namesystem.isInStartupSafeMode()) {
      aggregatedSafeBlocks.removeAll(toRemove);
      LOG.debug("AGGREGATED SAFE BLOCK #: " + aggregatedSafeBlocks.size() +
          " REPORTED BLOCK #: " + newReport.getNumberOfBlocks());
      namesystem.adjustSafeModeBlocks(aggregatedSafeBlocks);
      stats.numConsideredSafeIfInSafemode = aggregatedSafeBlocks.size();
    }
    return stats;
  }

  private void processMisMatchingBuckets(final DatanodeStorageInfo storage,
                                               final BlockReport newReport,
                                               final HashMatchingResult matchingResult,
                                               final Collection<BlockInfo> toAdd,
                                               final Collection<Block> toInvalidate,
                                               final Collection<BlockToMarkCorrupt> toCorrupt,
                                               final Collection<StatefulBlockInfo> toUC, final boolean firstBlockReport,
                                               final Map<Long, Integer> mismatchedBlocksAndInodes,
                                               final Set<Long> aggregatedSafeBlocks,
                                               final Set<Long> allMismatchedBlocksOnServer,
                                               final Map<Long,Long> invalidatedReplicas) throws IOException {

    final Collection<Callable<Void>> subTasks = new ArrayList<>();
    for (final int bucketId : matchingResult.mismatchedBuckets) {
      final Bucket bucket = newReport.getBuckets()[bucketId];
      final List<ReportedBlock> bucketBlocks = Arrays.asList(bucket.getBlocks());
      final Callable<Void> subTask = new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          final HopsTransactionalRequestHandler processReportHandler = processBucketInternal(storage, bucketId, toAdd,
              toInvalidate,
              toCorrupt, toUC, firstBlockReport,
              mismatchedBlocksAndInodes,
              aggregatedSafeBlocks, allMismatchedBlocksOnServer,
              invalidatedReplicas, bucketBlocks);
          processReportHandler.handle();
          return null;
        }
      };
      subTasks.add(subTask); // collect subtasks
    }

    try {
      List<Future<Void>> futures = ((FSNamesystem) namesystem).getSubtreeOperationsExecutor().invokeAll(subTasks);
      for (Future<Void> maybeException : futures){
        maybeException.get();
      }
    } catch (InterruptedException e) {
      LOG.error("Exception was thrown during block report processing", e);
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  }
  
  private HopsTransactionalRequestHandler processBucketInternal(final DatanodeStorageInfo storage,
                                                                final int bucketId,
                                                                final Collection<BlockInfo> toAdd,
                                                                final Collection<Block> toInvalidate,
                                                                final Collection<BlockToMarkCorrupt> toCorrupt,
                                                                final Collection<StatefulBlockInfo> toUC,
                                                                final boolean firstBlockReport,
                                                                final Map<Long, Integer> mismatchedBlocksAndInodes,
                                                                final Set<Long> aggregatedSafeBlocks,
                                                                final Set<Long> allMismatchedBlocksOnServer,
                                                                final Map<Long,Long> invalidatedReplicas,
                                                                final List<ReportedBlock> reportedBlocks ) {

    return new HopsTransactionalRequestHandler(HDFSOperationType.PROCESS_REPORT) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (!reportedBlocks.isEmpty()) {
          List<Long> resolvedBlockIds = new ArrayList<>();
          List<Integer> inodeIds = new ArrayList<>();
          List<Long> unResolvedBlockIds = new ArrayList<>();

          for (ReportedBlock reportedBlock : reportedBlocks) {
            Integer inodeId = mismatchedBlocksAndInodes.get(reportedBlock.getBlockId());
            if (inodeId != null) {
              resolvedBlockIds.add(reportedBlock.getBlockId());
              inodeIds.add(inodeId);
            } else {
              unResolvedBlockIds.add(reportedBlock.getBlockId());
            }
          }

          locks.add(lf.getBlockReportingLocks(Longs.toArray(resolvedBlockIds),
              Ints.toArray(inodeIds),
              Longs.toArray(unResolvedBlockIds), storage.getSid()));
        }
        locks.add(lf.getIndividualHashBucketLock(storage.getSid(), bucketId));
      }

      @Override
      public Object performTask() throws IOException {
        // scan the report and process newly reported blocks
        long hash = 0; // Our updated hash should only consider
        // finalized, stored blocks
        for (ReportedBlock brb : reportedBlocks) {
          Block block = new Block();
          block.setNoPersistance(brb.getBlockId(), brb.getLength(),
                  brb.getGenerationStamp());
          BlockInfo storedBlock =
                  processReportedBlock(storage,
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

        //update bucket hash
        HashBucket bucket = HashBuckets.getInstance()
                .getBucket(storage.getSid(), bucketId);
        bucket.setHash(hash);
        return null;
      }
    };
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

  private HashMatchingResult calculateMismatchedHashes(DatanodeStorageInfo storage,
      BlockReport report, Boolean firstBlockReport) throws IOException {
    List<HashBucket> storedHashes = HashBuckets.getInstance().getBucketsForStorage(storage);
    Map<Integer, HashBucket> storedHashesMap = new HashMap<>();
    for (HashBucket allStorageHash : storedHashes) {
      storedHashesMap.put(allStorageHash.getBucketId(), allStorageHash);
    }

    List<Integer> matchedBuckets = new ArrayList<>();
    List<Integer> mismatchedBuckets = new ArrayList<>();

    for (int i = 0; i < report.getBuckets().length; i++){
      if (!storedHashesMap.containsKey(i)){
        //escape early
        mismatchedBuckets.add(i);
        continue;
      }
      
      long storedHash = storedHashesMap.get(i).getHash();
      
      //First block report, or report in safe mode, should always process complete report.
      if (firstBlockReport) {
        //if the bucket is empty there is nothing to process 
        //except if the namenode think that there should be things in the bucket
        if (report.getBuckets()[i].getBlocks().length == 0 && storedHash == 0) {
          matchedBuckets.add(i);
          continue;
        }
        mismatchedBuckets.add(i);
        continue;
      }

      long reportedHash = report.getHashes()[i];

      if (storedHash == reportedHash){
        matchedBuckets.add(i);
      } else {
        mismatchedBuckets.add(i);
      }
    }

    assert matchedBuckets.size() + mismatchedBuckets.size() == report.getHashes().length;
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
   * @param storage
   *     the storage that made the report
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
  private BlockInfo processIncrementallyReportedBlock(
      final DatanodeStorageInfo storage,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfo> toAdd, final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block + " on " + storage.getStorageID() + " size " +
          block.getNumBytes() + " replicaState = " + reportedState);
    }
    // find block by blockId
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      blockLog.info("BLOCK* processReport: " + block + " on " + storage + " size " +
          block.getNumBytes() + " does not belong to any file");
      toInvalidate.add(new Block(block));
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();

    // Block is on the NN
    if (LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState + " bid=" + storedBlock.getBlockIndex());
    }

    // Ignore replicas already scheduled to be removed from the DN
    if (invalidateBlocks.contains(storage, getBlockInfo(block))) {
     /*  TODO: following assertion is incorrect, see HDFS-2668
      assert storedBlock.findDatanode(dn) < 0 : "Block " + block
      + " in recentInvalidatesSet should not appear in DN " + dn; */
      return storedBlock;
    }
    

    BlockToMarkCorrupt c =
        checkReplicaCorrupt(block, reportedState, storedBlock, ucState, storage);
    if (c != null) {
      toCorrupt.add(c);
      return storedBlock;
    }

    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo((BlockInfoUnderConstruction) storedBlock, block,
          reportedState));
      return storedBlock;
    }

    // Add replica if appropriate. If the replica was previously corrupt
    // but now okay, it might need to be updated.
    if (reportedState == ReplicaState.FINALIZED
        && (!storedBlock.isReplicatedOnStorage(storage) || corruptReplicas.isReplicaCorrupt(storedBlock, storage.
        getDatanodeDescriptor()))) {
      toAdd.add(storedBlock);
    }
    return storedBlock;
  }


  private BlockInfo processReportedBlock(
      final DatanodeStorageInfo storage,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfo> toAdd, final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC, final Set<Long> safeBlocks,
      final boolean firstBlockReport, final boolean replicaAlreadyExists,
      final Map<Long,Long> allMachineInvalidatedBlocks)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block + " on " + storage.getStorageID() + " size " +
          block.getNumBytes() + " replicaState = " + reportedState);
    }

    // find block by blockId
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      blockLog.info("BLOCK* processReport: " + block + " on " + storage.getStorageID() +
          " size " +
          block.getNumBytes() + " does not belong to any file");
      toInvalidate.add(new Block(block));
      safeBlocks.remove(block.getBlockId());
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();

    // Block is on the NN
    if (LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState + " bid=" + storedBlock.getBlockIndex());
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
        checkReplicaCorrupt(block, reportedState, storedBlock, ucState,
            storage);
    if (c != null) {
      toCorrupt.add(c);
      safeBlocks.remove(block.getBlockId());
      return storedBlock;
    }


    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo((BlockInfoUnderConstruction) storedBlock, block,
          reportedState));
      safeBlocks.remove(block.getBlockId());
      return storedBlock;
    }

    //add replica if appropriate
    if (reportedState == ReplicaState.FINALIZED) {
      if(replicaAlreadyExists || storedBlock.isReplicatedOnStorage(storage)){
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
      DatanodeStorageInfo storage) {
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
                      storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
            } else if (storedBlock.getNumBytes() != reported.getNumBytes()) {
              return new BlockToMarkCorrupt(storedBlock,
                  "block is " + ucState + " and reported length " +
                      reported.getNumBytes() + " does not match " +
                      "length in block map " + storedBlock.getNumBytes(), Reason.SIZE_MISMATCH);
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
                  storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        } else { // COMPLETE block, same genstamp
          if (reportedState == ReplicaState.RBW) {
            // If it's a RBW report for a COMPLETE block, it may just be that
            // the block report got a little bit delayed after the pipeline
            // closed. So, ignore this report, assuming we will get a
            // FINALIZED replica later. See HDFS-2791
            LOG.info("Received an RBW replica for " + storedBlock +
                " on " + storage + ": ignoring it, since it is " +
                "complete with the same genstamp");
            return null;
          } else {
            return new BlockToMarkCorrupt(storedBlock,
                "reported replica has invalid state " + reportedState, Reason.INVALID_STATE);
          }
        }
      case RUR:       // should not be reported
      case TEMPORARY: // should not be reported
      default:
        String msg =
            "Unexpected replica state " + reportedState + " for block: " +
                storedBlock + " on " + storage + " size " + storedBlock
                .getNumBytes();
        // log here at WARN level since this is really a broken HDFS invariant
        LOG.warn(msg);
        return new BlockToMarkCorrupt(storedBlock, msg, Reason.INVALID_STATE);
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

  private void addStoredBlockUnderConstruction(StatefulBlockInfo ucBlock,
      DatanodeStorageInfo storage) throws IOException {
    BlockInfoUnderConstruction block = ucBlock.storedBlock;
    block.addReplicaIfNotPresent(storage, ucBlock.reportedState, ucBlock.reportedBlock.getGenerationStamp());
    if (ucBlock.reportedState == ReplicaState.FINALIZED && !block.isReplicatedOnStorage(storage)) {
      addStoredBlock(block, storage, null, true);
    }
  }

  /**
   * Faster version of
   * {@link #addStoredBlock(BlockInfo, DatanodeStorageInfo,
   * DatanodeDescriptor, boolean)}
   * , intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle underReplication/overReplication, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
   *
   * @throws IOException
   */
  private void addStoredBlockImmediate(
      BlockInfo storedBlock,
      DatanodeStorageInfo storage, boolean logEveryBlock) throws IOException {
    assert (storedBlock != null);
    if (!namesystem.isInStartupSafeMode() ||
        namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, storage, null, logEveryBlock);
      return;
    }

    // just add it
    storage.addBlock(storedBlock);

    // Now check for completion of blocks and safe block count
    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numCurrentReplica >= minReplication) {
      completeBlock(storedBlock.getBlockCollection(), storedBlock, false);
    } else if (storedBlock.isComplete()) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   *
   * @return the block that is stored in blockMap.
   */
  private Block addStoredBlock(
      final BlockInfo block,
      DatanodeStorageInfo storage,
      DatanodeDescriptor delNodeHint,
      boolean logEveryBlock) throws IOException {
    assert block != null;
    BlockInfo storedBlock;
    if (block instanceof BlockInfoUnderConstruction) {
      //refresh our copy in case the block got completed in another thread
      storedBlock = blocksMap.getStoredBlock(block);
    } else {
      storedBlock = block;
    }
    if (storedBlock == null || storedBlock.getBlockCollection() == null) {
      // If this block does not belong to anyfile, then we are done.
      blockLog.info(
          "BLOCK* addStoredBlock: " + block + " on " + storage.getStorageID() + " size " +
              block.getNumBytes() + " but it does not belong to any file");
      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }
    assert storedBlock != null : "Block must be stored by now";
    BlockCollection bc = storedBlock.getBlockCollection();
    assert bc != null : "Block must belong to a file";

    FSNamesystem fsNamesystem = (FSNamesystem) namesystem;
    NumberReplicas numBeforeAdding = null;
    if (fsNamesystem.isErasureCodingEnabled()) {
      numBeforeAdding = countNodes(block);
    }

    // add block to the datanode
    boolean added = storage.addBlock(storedBlock);

    int curReplicaDelta;
    if (added) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        logAddStoredBlock(storedBlock, storage);
      }
    } else {
      // if the same block is added again and the replica was corrupt
      // previously because of a wrong gen stamp, remove it from the
      // corrupt block list.
      corruptReplicas.removeFromCorruptReplicasMap(block, storage.getDatanodeDescriptor(),
          Reason.GENSTAMP_MISMATCH);
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: " +
          "Redundant addStoredBlock request received for " + storedBlock +
          " on " + storage.getStorageID() + " size " + storedBlock.getNumBytes());
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica =
        numLiveReplicas + pendingReplications.getNumReplicas(storedBlock);

    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numLiveReplicas >= minReplication) {
      storedBlock =
          completeBlock(bc, storedBlock, false);
    } else if (storedBlock.isComplete() && added) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that
      // Is no-op if not in safe mode.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }

    // if file is under construction, then done for now
    if (bc.isUnderConstruction()) {
      return storedBlock;
    }

    // do not try to handle over/under-replicated blocks during first safe mode
    if (!namesystem.isPopulatingReplQueues()) {
      return storedBlock;
    }

    // handle underReplication/overReplication
    short fileReplication = bc.getBlockReplication();
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      neededReplications
          .remove(storedBlock, numCurrentReplica, num.decommissionedReplicas(),
              fileReplication);
    } else {
      updateNeededReplications(storedBlock, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(storedBlock, fileReplication,
          storage.getDatanodeDescriptor(), delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(storedBlock);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + "blockMap has " + numCorruptNodes +
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(storedBlock);
    }

    if (fsNamesystem.isErasureCodingEnabled()) {
      INode iNode = EntityManager.find(INode.Finder.ByINodeIdFTIS, bc.getId());
      if (iNode.isUnderConstruction() == false &&
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

    return storedBlock;
  }

  private void logAddStoredBlock(BlockInfo storedBlock,
      DatanodeStorageInfo storage) {
    if (!blockLog.isInfoEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder(500);
    sb.append("BLOCK* addStoredBlock: blockMap updated: ")
        .append(storage)
        .append(" is added to ")
        .append(storedBlock)
        .append(" size ")
        .append(storedBlock.getNumBytes())
        .append(" byte");
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
    boolean removedFromBlocksMap = true;

    if (nodes == null) {
      return;
    }
    // make a copy of the array of nodes in order to avoid
    // ConcurrentModificationException, when the block is removed from the node
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        removedFromBlocksMap = invalidateBlock(new BlockToMarkCorrupt(blk, null, Reason.ANY), node);
      } catch (IOException e) {
        blockLog.info("invalidateCorruptReplicas error in deleting bad block " +
                blk + " on " + node, e);
        removedFromBlocksMap = false;
      }
    }
    // Remove the block from corruptReplicasMap
    if (removedFromBlocksMap) {
      corruptReplicas.removeFromCorruptReplicasMap(blk);
    }
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  public void processMisReplicatedBlocks() throws IOException {
    //this normaly reinitialize the block scanning, when should we reinitialize the block scanning and
    //how do we propagate it to all NN?
    stopReplicationInitializer();
    if (namesystem.isLeader()) {
      //it should be ok to reset even if other NN did not restart 
      //at worse we will have blocks in neededReplication that should not be there
      //this would only result in these block getting transiantly over replicated
      HdfsVariables.resetMisReplicatedIndex();
      neededReplications.clear();
    }
    replicationQueuesInitializer = new Daemon() {
      
      @Override
      public void run() {
        try {
          processMisReplicatesAsync();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while processing replication queues.");
        } catch (Exception e) {
          LOG.error("Error while processing replication queues async", e);
        }
      }
    };
    replicationQueuesInitializer.setName("Replication Queue Initializer");
    replicationQueuesInitializer.start();
  }
    
    /*
   * Stop the ongoing initialisation of replication queues
   */
  private void stopReplicationInitializer() {
    if (replicationQueuesInitializer != null) {
      replicationQueuesInitializer.interrupt();
      try {
        replicationQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for replicationQueueInitializer. Returning..");
        return;
      } finally {
        replicationQueuesInitializer = null;
      }
    }
  }
    
//  private void restetMisReplicatesIndex() throws IOException{
//    while(namesystem.isLeader() && sizeOfMisReplicatedRangeQueue()>0){
//      cleanMisReplicatedRangeQueue();
//    }
//  }
  
//  private void lockMisReplicatedRangeQueue(long nnId) throws IOException {
//    new LightWeightRequestHandler(
//        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
//      @Override
//      public Object performTask() throws IOException {
//        HdfsStorageFactory.getConnector().writeLock();
//        MisReplicatedRangeQueueDataAccess da =
//            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
//                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
//        da.insert(nnId, -2, -2);
//        return null;
//      }
//    }.handle();
//  }
//  
//  private void unlockMisReplicatedRangeQueue(long nnId) throws IOException {
//    new LightWeightRequestHandler(
//        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
//      @Override
//      public Object performTask() throws IOException {
//        HdfsStorageFactory.getConnector().writeLock();
//        MisReplicatedRangeQueueDataAccess da =
//            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
//                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
//        da.remove(nnId, -2, -2);
//        return null;
//      }
//    }.handle();
//  }
//  
//  private boolean isMisReplicatedRangeQueueLocked() throws IOException {
//    return (boolean) new LightWeightRequestHandler(
//        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
//      @Override
//      public Object performTask() throws IOException {
//        HdfsStorageFactory.getConnector().writeLock();
//        MisReplicatedRangeQueueDataAccess da = (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
//            .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
//        Long leaderIndex = da.getStartIndex(namesystem.getNameNode().getActiveNameNodes().getLeader().getId());
//        if (leaderIndex < -1) {
//          return true;
//        } else {
//          return false;
//        }
//      }
//    }.handle();
//  }
  
//  private void waitOnMisReplicatedRangeQueueLock() throws InterruptedException, IOException{
//    while(isMisReplicatedRangeQueueLocked()){
//      Thread.sleep(500);
//    }
//  }
  
  private List<MisReplicatedRange> checkMisReplicatedRangeQueue() throws IOException {
    final LightWeightRequestHandler cleanMisReplicatedRangeQueueHandler = new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        HdfsStorageFactory.getConnector().writeLock();
        MisReplicatedRangeQueueDataAccess da = (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
            .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        List<MisReplicatedRange> ranges = da.getAll();
        Set<Long> activeNodes = new HashSet<>();
        for (ActiveNode nn : namesystem.getNameNode().getLeaderElectionInstance().getActiveNamenodes().getActiveNodes()) {
          activeNodes.add(nn.getId());
        }
        List<MisReplicatedRange> toRemove = new ArrayList<>();
        for (MisReplicatedRange range : ranges) {
          if (!activeNodes.contains(range.getNnId())) {
            toRemove.add(range);
          }
        }
        da.remove(toRemove);
        return toRemove;
      }
    };
    List<MisReplicatedRange> toProcess = new ArrayList<>();
    while (namesystem.isLeader() && sizeOfMisReplicatedRangeQueue() > 0) {
      toProcess.addAll((List<MisReplicatedRange>)cleanMisReplicatedRangeQueueHandler.handle());
    }
    return toProcess;
  }
  
//  private boolean waitMisReplicatedRangeQueue() throws IOException {
//    return (boolean) new LightWeightRequestHandler(
//        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
//      @Override
//      public Object performTask() throws IOException {
//        HdfsStorageFactory.getConnector().writeLock();
//        MisReplicatedRangeQueueDataAccess da =
//            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
//                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
//        List<Long> startIndexes= da.getAllStartIndex();
//        
//        for(long startIndex: startIndexes){
//          if(startIndex>=0){
//            return true;
//          }
//        }
//        return false;
//      }
//    }.handle();
//  }
  
  private void processMisReplicatesAsync() throws InterruptedException, IOException {
    final AtomicLong nrInvalid = new AtomicLong(0);
    final AtomicLong nrOverReplicated = new AtomicLong(0);
    final AtomicLong nrUnderReplicated = new AtomicLong(0);
    final AtomicLong nrPostponed = new AtomicLong(0);
    final AtomicLong nrUnderConstruction = new AtomicLong(0);
    long startTimeMisReplicatedScan = Time.now();
    
    
    long totalBlocks = blocksMap.size();
    replicationQueuesInitProgress = 0;
    final AtomicLong totalProcessed = new AtomicLong(0);   
    boolean haveMore;
    final int filesToProcess = processMisReplicatedBatchSize * processMisReplicatedNoOfBatchs;

    addToMisReplicatedRangeQueue(new MisReplicatedRange(namesystem.getNamenodeId(), -1));
    
    while (namesystem.isRunning() && !Thread.currentThread().isInterrupted()) {
      long filesToProcessEndIndex;
      long filesToProcessStartIndex;
      do {
        filesToProcessEndIndex = HdfsVariables.incrementMisReplicatedIndex(filesToProcess);
        filesToProcessStartIndex = filesToProcessEndIndex - filesToProcess;
        haveMore = blocksMap.haveFilesWithIdGreaterThan(filesToProcessEndIndex);
      } while (!blocksMap.haveFilesWithIdBetween(filesToProcessStartIndex,
          filesToProcessEndIndex) && haveMore);

      addToMisReplicatedRangeQueue(new MisReplicatedRange(namesystem.getNamenodeId(), filesToProcessStartIndex));

      processMissreplicatedInt(filesToProcessStartIndex, filesToProcessEndIndex, filesToProcess, nrInvalid,
          nrOverReplicated, nrUnderReplicated, nrPostponed, nrUnderConstruction, totalProcessed);

      addToMisReplicatedRangeQueue(new MisReplicatedRange(namesystem.getNamenodeId(), -1));

      // there is a possibility that if any of the blocks deleted/added during
      // initialisation, then progress might be different.
      replicationQueuesInitProgress = Math.min((double) totalProcessed.get()
          / totalBlocks, 1.0);
      if (!haveMore) {
        removeFromMisReplicatedRangeQueue(new MisReplicatedRange(namesystem.getNamenodeId(), -1));
        if (namesystem.isLeader()) {
          //get the list of indexes that should have been scanned by namenode that are now dead
          List<MisReplicatedRange> toProcess = checkMisReplicatedRangeQueue();
          //(re)scan the corresponding blocks
          for (MisReplicatedRange range : toProcess) {
            long startIndex = range.getStartIndex();
            if (startIndex > 0) {
              processMissreplicatedInt(startIndex, startIndex + filesToProcess, filesToProcess,nrInvalid,
                  nrOverReplicated, nrUnderReplicated, nrPostponed, nrUnderConstruction, totalProcessed);
            }
          }
        }
        LOG.info("Total number of blocks            = " + blocksMap.size());
        LOG.info("Number of invalid blocks          = " + nrInvalid.get());
        LOG.info("Number of under-replicated blocks = " + nrUnderReplicated.get());
        LOG.info("Number of  over-replicated blocks = " + nrOverReplicated.get()
            + ((nrPostponed.get() > 0) ? (" (" + nrPostponed.get() + " postponed)") : ""));
        LOG.info("Number of blocks being written    = " + nrUnderConstruction.get());
        NameNode.stateChangeLog
            .info("STATE* Replication Queue initialization "
                + "scan for invalid, over- and under-replicated blocks "
                + "completed in " + (Time.now() - startTimeMisReplicatedScan)
                + " msec");
        break;
      }
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.info("Interrupted while processing replication queues.");
    }
  }
  
  private void processMissreplicatedInt(long filesToProcessStartIndex, long filesToProcessEndIndex, int filesToProcess,
      final AtomicLong nrInvalid, final AtomicLong nrOverReplicated, final AtomicLong nrUnderReplicated,
      final AtomicLong nrPostponed, final AtomicLong nrUnderConstruction, final AtomicLong totalProcessed)
      throws IOException {
    final List<INodeIdentifier> allINodes = blocksMap
        .getAllINodeFiles(filesToProcessStartIndex, filesToProcessEndIndex);
    LOG.info("processMisReplicated read  " + allINodes.size() + "/" + filesToProcess + " in the Ids range ["
        + filesToProcessStartIndex + " - " + filesToProcessEndIndex + "]");

    try {
      Slicer.slice(allINodes.size(), processMisReplicatedBatchSize, processMisReplicatedNoThreads,
          new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex)
            throws Exception {
          final List<INodeIdentifier> inodeIdentifiers = allINodes.subList(startIndex, endIndex);

          final HopsTransactionalRequestHandler processMisReplicatedBlocksHandler = new HopsTransactionalRequestHandler(
              HDFSOperationType.PROCESS_MIS_REPLICATED_BLOCKS_PER_INODE_BATCH) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              locks.add(lf.getBatchedINodesLock(inodeIdentifiers))
                  .add(lf.getSqlBatchedBlocksLock()).add(
                  lf.getSqlBatchedBlocksRelated(BLK.RE, BLK.IV, BLK.CR, BLK.UR,
                      BLK.ER));

            }

            @Override
            public Object performTask() throws IOException {
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
                      nrUnderReplicated.incrementAndGet();
                      break;
                    case OVER_REPLICATED:
                      nrOverReplicated.incrementAndGet();
                      break;
                    case INVALID:
                      nrInvalid.incrementAndGet();
                      break;
                    case POSTPONE:
                      nrPostponed.incrementAndGet();
                      postponeBlock(block);
                      break;
                    case UNDER_CONSTRUCTION:
                      nrUnderConstruction.incrementAndGet();
                      break;
                    case OK:
                      break;
                    default:
                      throw new AssertionError("Invalid enum value: " + res);
                  }
                  totalProcessed.incrementAndGet();
                }
              }
              return null;
            }
          };
          processMisReplicatedBlocksHandler.handle(namesystem);
        }
      });
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  /**
   * Get the progress of the Replication queues initialisation
   * 
   * @return Returns values between 0 and 1 for the progress.
   */
  public double getReplicationQueuesInitProgress() {
    //should we store this in the DB to have update from all the NN?
    return replicationQueuesInitProgress;
  }

  private void addToMisReplicatedRangeQueue(final MisReplicatedRange range)
      throws IOException {
    new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        HdfsStorageFactory.getConnector().writeLock();
        MisReplicatedRangeQueueDataAccess da =
            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        da.insert(range);
        return null;
      }
    }.handle();
  }

  private void removeFromMisReplicatedRangeQueue(final MisReplicatedRange range) throws IOException {
    new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_MIS_REPLICATED_RANGE_QUEUE) {
      @Override
      public Object performTask() throws IOException {
        HdfsStorageFactory.getConnector().writeLock();
        MisReplicatedRangeQueueDataAccess da =
            (MisReplicatedRangeQueueDataAccess) HdfsStorageFactory
                .getDataAccess(MisReplicatedRangeQueueDataAccess.class);
        da.remove(range);
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
      throws IOException {
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
      throws IOException {
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
  private void processOverReplicatedBlock(
      final Block block,
      final short replication,
      final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint)
      throws IOException {

    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(getBlockInfo(block));

    for (DatanodeStorageInfo storage : blocksMap.storageList(block, State.NORMAL)){
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (storage.areBlockContentsStale()) {
        LOG.info("BLOCK* processOverReplicatedBlock: Postponing processing of over-replicated " +
            block + " since storage " + storage +
            " does not yet have up-to-date block information.");
        postponeBlock(block);
        return;
      }
      if (!excessReplicateMap.contains(storage, getBlockInfo(block))) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(storage);
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
  private void chooseExcessReplicates(final Collection<DatanodeStorageInfo> nonExcess,
      Block b, short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint, BlockPlacementPolicy replicator)
      throws StorageException, TransactionContextException {

    // first form a rack to datanodes map and
    BlockCollection bc = getBlockCollection(b);
    final BlockStoragePolicy storagePolicy =
        storagePolicySuite.getPolicy(bc.getStoragePolicyID());

    final List<StorageType> excessTypes = storagePolicy.chooseExcess(
        replication, DatanodeStorageInfo.toStorageTypes(nonExcess));

    final Map<String, List<DatanodeStorageInfo>> rackMap
        = new HashMap<String, List<DatanodeStorageInfo>>();
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<DatanodeStorageInfo>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<DatanodeStorageInfo>();

    // split nodes into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    replicator.splitNodesWithRack(nonExcess, rackMap, moreThanOne, exactlyOne);

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    final DatanodeStorageInfo delNodeHintStorage
        = DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, delNodeHint);
    final DatanodeStorageInfo addedNodeStorage
        = DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, addedNode);
    while (nonExcess.size() - replication > 0) {
      final DatanodeStorageInfo cur;
      if (useDelHint(firstOne, delNodeHintStorage, addedNodeStorage,
          moreThanOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = replicator
            .chooseReplicaToDelete(bc, b, replication, moreThanOne, exactlyOne, excessTypes);
      }
      firstOne = false;

      // adjust rackmap, moreThanOne, and exactlyOne
      replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne,
          exactlyOne, cur);

      nonExcess.remove(cur);
      addToExcessReplicate(cur, b);

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.
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

  /** Check if we can use delHint */
  static boolean useDelHint(boolean isFirst, DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThan1Racks,
      List<StorageType> excessTypes) {
    if (!isFirst) {
      return false; // only consider delHint for the first case
    } else if (delHint == null) {
      return false; // no delHint
    } else if (!excessTypes.contains(delHint.getStorageType())) {
      return false; // delHint storage type is not an excess type
    } else {
      // check if removing delHint reduces the number of racks
      if (moreThan1Racks.contains(delHint)) {
        return true; // delHint and some other nodes are under the same rack
      } else if (added != null && !moreThan1Racks.contains(added)) {
        return true; // the added node adds a new rack
      }
      return false; // removing delHint reduces the number of racks;
    }
  }

  private void addToExcessReplicate(DatanodeStorageInfo storage, Block block)
      throws StorageException, TransactionContextException {
    BlockInfo blockInfo = getBlockInfo(block);

    if (excessReplicateMap.put(storage.getSid(), blockInfo)) {
      excessBlocksCount.incrementAndGet();
      if (blockLog.isDebugEnabled()) {
        blockLog.debug(
            "BLOCK* addToExcessReplicate:" + " (" + storage + ", " + block +
                ") is added to excessReplicateMap");
      }
    }
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  public void removeStoredBlock(Block block, DatanodeDescriptor node)
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
    if (excessReplicateMap.remove(node, getBlockInfo(block))) {
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

  public void removeStoredBlock(Block block, int sid)
      throws IOException {
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* removeStoredBlock: " + block + " from " + sid);
    }
    if (!blocksMap.removeNode(block, sid)) {
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* removeStoredBlock: " + block +
            " has already been removed from node " + sid);
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

    // Remove the replica from corruptReplicas
    corruptReplicas.forceRemoveFromCorruptReplicasMap(getBlockInfo(block), sid);

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
  private long addBlocks(final List<Block> blocks, List<BlockWithLocations> results)
      throws IOException {

    final Map<Block, List<DatanodeStorageInfo>> locationsMap = new HashMap<>();

    new HopsTransactionalRequestHandler(HDFSOperationType.GET_VALID_BLK_LOCS) {
      List<INodeIdentifier> inodeIdentifiers;

      @Override
      public void setUp() throws StorageException {
        long[] blockIds = new long[blocks.size()];
        for(int i=0; i<blocks.size(); i++){
          blockIds[i] = blocks.get(i).getBlockId();
        }
        List<Integer> inodeIds = new ArrayList<>(INodeUtil.getINodeIdsForBlockIds(blockIds).keySet());
        inodeIdentifiers = INodeUtil.resolveINodesFromIds(inodeIds);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getBatchedINodesLock(inodeIdentifiers))
            .add(lf.getSqlBatchedBlocksLock())
            .add(lf.getSqlBatchedBlocksRelated(BLK.RE, BLK.IV));
      }

      @Override
      public Object performTask() throws IOException {
        for (Block block : blocks) {
          BlockInfo temp = getBlockInfo(block);
          final List<DatanodeStorageInfo> ms = getValidLocations(temp);
          if (!ms.isEmpty()) {
            locationsMap.put(block, ms);
          }
        }
        return null;
      }
    }.handle(namesystem);

    if (locationsMap.isEmpty()) {
      return 0;
    } else {
      long numBytes = 0;
      for (Block block : locationsMap.keySet()) {
        List<DatanodeStorageInfo> locations = locationsMap.get(block);
        final String[] datanodeUuids = new String[locations.size()];
        final String[] storageIDs = new String[datanodeUuids.length];
        final StorageType[] storageTypes = new StorageType[datanodeUuids.length];
        for (int i = 0; i < locations.size(); i++) {
          final DatanodeStorageInfo s = locations.get(i);
          datanodeUuids[i] = s.getDatanodeDescriptor().getDatanodeUuid();
          storageIDs[i] = s.getStorageID();
          storageTypes[i] = s.getStorageType();
        }

        results.add(new BlockWithLocations(block, datanodeUuids, storageIDs,
            storageTypes));
        numBytes += block.getNumBytes();
      }

      return numBytes;
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  @VisibleForTesting
  void addBlock(DatanodeStorageInfo storage, Block block, String delHint)
      throws IOException {
    DatanodeDescriptor node = storage.getDatanodeDescriptor();
    // Decrement number of blocks scheduled to this datanode.
    // for a retry request (of DatanodeProtocol#blockReceivedAndDeleted with
    // RECEIVED_BLOCK), we currently also decrease the approximate number.
    node.decrementBlocksScheduled(storage.getStorageType());

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeManager.getDatanodeByUuid(delHint);
      if (delHintNode == null) {
        blockLog.warn("BLOCK* blockReceived: " + block
            + " is expected to be removed from an unrecorded node " + delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.decrement(getBlockInfo(block), node);
    processAndHandleReportedBlock(storage, block, ReplicaState.FINALIZED,
        delHintNode);
  }

  /**
   *
   * @param storage
   * @param block
   * @param reportedState
   * @param delHintNode
   * @return true if the block was new, false otherwise
   * @throws IOException
   */
  private void processAndHandleReportedBlock(
      DatanodeStorageInfo storage, Block block,
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    // blockReceived reports a finalized block
    Collection<BlockInfo> toAdd = new LinkedList<>();
    Collection<Block> toInvalidate = new LinkedList<>();
    Collection<BlockToMarkCorrupt> toCorrupt =
        new LinkedList<>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<>();
    final DatanodeDescriptor node = storage.getDatanodeDescriptor();
    
    processIncrementallyReportedBlock(storage, block, reportedState, toAdd, toInvalidate,
        toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert
        toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <=
            1 : "The block should be only in one of the lists.";

    for (StatefulBlockInfo b : toUC) {
      addStoredBlockUnderConstruction(b, storage);
    }
    long numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, storage, delHintNode, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.info("BLOCK* addBlock: logged info for " + maxNumBlocksToLog
          + " of " + numBlocksLogged + " reported.");
    }
    for (Block b : toInvalidate) {
      blockLog.info("BLOCK* addBlock: block " + b + " on " + storage + " size " +
          b.getNumBytes() + " does not belong to any file");
      addToInvalidates(b, storage.getDatanodeDescriptor());
    }

    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, storage, storage.getDatanodeDescriptor());
    }
  }

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   */
  public void processIncrementalBlockReport(DatanodeRegistration nodeID,
      final StorageReceivedDeletedBlocks blockInfos)
    throws IOException {
    //hack to have the variables final to pass then to the handler.
    final int[] received = {0};
    final int[] deleted = {0};
    final int[] receiving = {0};

    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);

    if (node == null || !node.isAlive) {
      blockLog
          .warn("BLOCK* processIncrementalBlockReport"
              + " is received from dead or unregistered node "
              + nodeID);
      throw new IOException(
          "Got incremental block report from unregistered or dead node");
    }

    // Little hack; since we can't reassign final s if s==null, we have to
    // declare s as a normal variable and then assign it to a statically
    // declared variable
    DatanodeStorageInfo s = node.getStorageInfo(blockInfos.getStorage().getStorageID());
    if (s == null) {
      // The DataNode is reporting an unknown storage. Usually the NN learns
      // about new storages from heartbeats but during NN restart we may
      // receive a block report or incremental report before the heartbeat.
      // We must handle this for protocol compatibility. This issue was
      // uncovered by HDFS-6094.
      s = node.updateStorage(blockInfos.getStorage());
    }
    final DatanodeStorageInfo storage = s;


    HopsTransactionalRequestHandler processIncrementalBlockReportHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.BLOCK_RECEIVED_AND_DELETED_INC_BLK_REPORT) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            ReceivedDeletedBlockInfo rdbi = (ReceivedDeletedBlockInfo) getParams()[0];
            LOG.debug("reported block id=" + rdbi.getBlock().getBlockId());
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(rdbi.getBlock());
            if (inodeIdentifier == null) {
              LOG.error("Invalid State. deleted blk is not recognized. bid=" +
                  rdbi.getBlock().getBlockId());
            }
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            ReceivedDeletedBlockInfo rdbi = (ReceivedDeletedBlockInfo) getParams()[0];
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
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
            locks.add(lf.getIndividualHashBucketLock(storage.getSid(), HashBuckets
                .getInstance().getBucketForBlock(rdbi.getBlock())));
          }

          @Override
          public Object performTask() throws IOException {
            ReceivedDeletedBlockInfo rdbi =
                    (ReceivedDeletedBlockInfo) getParams()[0];
            LOG.debug("BLOCK_RECEIVED_AND_DELETED_INC_BLK_REPORT " +
                rdbi.getStatus() + " bid=" +rdbi.getBlock().getBlockId() +
                " dataNode=" + node.getXferAddr() + " storage=" + storage.getStorageID() +
                    " sid: " + storage.getSid() + " status=" + rdbi.getStatus());
            HashBuckets hashBuckets = HashBuckets.getInstance();
            switch (rdbi.getStatus()) {
              case CREATING:
                processAndHandleReportedBlock(storage, rdbi.getBlock(),
                        ReplicaState.RBW, null);
                received[0]++;
                break;
              case APPENDING:
                processAndHandleReportedBlock(storage, rdbi.getBlock(),
                    ReplicaState.RBW, null);
                received[0]++;
                break;
              case RECOVERING_APPEND:
                processAndHandleReportedBlock(storage, rdbi.getBlock(),
                    ReplicaState.RBW, null);
                received[0]++;
                break;
              case RECEIVED:
                addBlock(storage, rdbi.getBlock(), rdbi.getDelHints());
                hashBuckets.applyHash(storage.getSid(), ReplicaState.FINALIZED, rdbi.getBlock());
                received[0]++;
                break;
              case UPDATE_RECOVERED:
                addBlock(storage, rdbi.getBlock(), rdbi.getDelHints());
                received[0]++;
                break;
              case DELETED:
                removeStoredBlock(rdbi.getBlock(), storage.getDatanodeDescriptor());
                deleted[0]++;
                break;
              default:
                String msg =
                    "Unknown block status code reported by " + storage.getStorageID() + ": " + 
                        rdbi;
                blockLog.warn(msg);
                assert false : msg; // if assertions are enabled, throw.
                break;
            }
            if (blockLog.isDebugEnabled()) {
              blockLog.debug("BLOCK* block " + (rdbi.getStatus()) + ": " + rdbi.getBlock() +
                  " is received from " + storage.getStorageID());
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

      for (ReceivedDeletedBlockInfo rdbi : blockInfos.getBlocks()) {
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
      throws IOException {
    int decommissioned = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    int stale = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(getBlockInfo(b));

    for(DatanodeStorageInfo storage: blocksMap.storageList(b, State.NORMAL)) {

      final DatanodeDescriptor node = storage.getDatanodeDescriptor();

      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned++;
      } else {
        LightWeightLinkedSet<Block> blocksExcess = excessReplicateMap.get(storage.getSid());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
          live++;
        }
      }
      if (storage.areBlockContentsStale()) {
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
    List<DatanodeStorageInfo> storages = blocksMap.storageList(b, State.NORMAL);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
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
    List<DatanodeStorageInfo> storages = blocksMap.storageList(block);
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages){
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      nodeList.append(node);
      nodeList.append(" ");
    }
    LOG.info("Block: " + block + ", Expected Replicas: " + curExpectedReplicas +
        ", live replicas: " + curReplicas + ", corrupt replicas: " +
        num.corruptReplicas() + ", decommissioned replicas: " +
        num.decommissionedReplicas() + ", excess replicas: " +
        num.excessReplicas() + ", Is Open File: " + bc.isUnderConstruction() +
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
    final Iterator<? extends Block> it = srcNode.getBlockIterator(true);
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
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
                .add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.UR, BLK.IV));
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
    final boolean[] firstReplicationLog = new boolean[]{true};
    final int[] underReplicatedBlocks = new int[]{0};
    final int[] decommissionOnlyReplicas = new int[]{0};
    final int[] underReplicatedInOpenFiles = new int[]{0};

    final Iterator<? extends Block> it = srcNode.getBlockIterator(true);

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
                .add(lf.getBlockLock()).add(
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
                  if (bc.isUnderConstruction()) {
                    if (block.equals(bc.getLastBlock()) && curReplicas > minReplication) {
                      return null;
                    }
                    underReplicatedInOpenFiles[0]++;
                  }

                  //Log info about one block for this node which needs replication
                  if (!status[0]) {
                    status[0] = true;
                    if (firstReplicationLog[0]) {
                      logBlockReplicationInfo(block, srcNode, num);
                    }
                    // Allowing decommission as long as default replication is met
                    if (curReplicas >= defaultReplication) {
                      status[0] = false;
                      firstReplicationLog[0] = false;
                    }
                  }
                  underReplicatedBlocks[0]++;
                  if ((curReplicas == 0) &&
                      (num.decommissionedReplicas() > 0)) {
                    decommissionOnlyReplicas[0]++;
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

  public DatanodeStorageInfo[] getStorages(BlockInfo block)
      throws TransactionContextException, StorageException {
    return block.getStorages(datanodeManager);
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
    // Remove the block from pendingReplications and neededReplications
    pendingReplications.remove(storedBlock);
    neededReplications.remove(storedBlock, UnderReplicatedBlocks.LEVEL);
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
      throws IOException {
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
      throws IOException {
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
  private int invalidateWorkForOneNode(Map.Entry<DatanodeInfo, List<Integer>> entry) throws IOException {
    // blocks should not be replicated or removed if safe mode is on
    if (namesystem.isInSafeMode()) {
      LOG.debug("In safemode, not computing replication work");
      return 0;
    }
    // get blocks to invalidate for the nodeId

    DatanodeDescriptor dnDescriptor = datanodeManager.getDatanode(entry.getKey());

    if (dnDescriptor == null) {
      LOG.warn("DataNode " + entry.getKey() + " cannot be found for sids " +
            Arrays.toString(entry.getValue().toArray()) + ", removing block invalidation work.");
      invalidateBlocks.remove(entry.getValue());
      return 0;
    }
    final List<Block> toInvalidate = invalidateBlocks.invalidateWork(dnDescriptor);

    if (toInvalidate == null) {
      return 0;
    }

    if (blockLog.isInfoEnabled()) {
      blockLog.info("BLOCK* " + getClass().getSimpleName()
          + ": ask " + entry.getKey() + " to delete " + toInvalidate);
    }

    return toInvalidate.size();
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
    for (DatanodeStorageInfo storage : blocksMap.storageList(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
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
  public List<DatanodeStorageInfo> storageList(final Block block)
      throws StorageException, TransactionContextException {
    return blocksMap.storageList(block);
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
   * Get the replicas which are corrupt for a given block.
   */
  public Collection<DatanodeDescriptor> getCorruptReplicas(Block block) throws StorageException,
      TransactionContextException {
    return corruptReplicas.getNodes(getBlockInfo(block));
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
            LOG.debug("Running replication monitor");
            computeDatanodeWork();
            processPendingReplications();
          } else {
            LOG.debug("Namesystem is not leader: will not run replication monitor");
          }
          Thread.sleep(replicationRecheckInterval);
        } catch (Throwable t) {
          if(t instanceof TransientStorageException){
            continue;
          }
          if (!namesystem.isRunning()) {
            LOG.info("Stopping ReplicationMonitor.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("ReplicationMonitor received an exception"
                  + " while shutting down.", t);
            }
            break;
          } else if (!checkNSRunning && t instanceof InterruptedException) {
            LOG.info("Stopping ReplicationMonitor for testing.");
            break;
          }
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

  private static class ReplicationWork {

    private final Block block;
    private final BlockCollection bc;

    private final DatanodeDescriptor srcNode;
    private final List<DatanodeDescriptor> containingNodes;
    private final List<DatanodeStorageInfo> liveReplicaStorages;
    private final int additionalReplRequired;

    private DatanodeStorageInfo targets[];
    private final int priority;

    public ReplicationWork(Block block,
        BlockCollection bc,
        DatanodeDescriptor srcNode,
        List<DatanodeDescriptor> containingNodes,
        List<DatanodeStorageInfo> liveReplicaStorages,
        int additionalReplRequired,
        int priority) {
      this.block = block;
      this.bc = bc;
      this.srcNode = srcNode;
      this.srcNode.incrementPendingReplicationWithoutTargets();
      this.containingNodes = containingNodes;
      this.liveReplicaStorages = liveReplicaStorages;
      this.additionalReplRequired = additionalReplRequired;
      this.priority = priority;
      this.targets = null;
    }

    private void chooseTargets(BlockPlacementPolicy blockplacement,
        BlockStoragePolicySuite storagePolicySuite,
        Set<Node> excludedNodes)
        throws TransactionContextException, StorageException {
      try {
        //HOP: [M] srcPath is not used
        targets = blockplacement.chooseTarget(null /*bc.getName()*/,
            additionalReplRequired, srcNode, liveReplicaStorages, false,
            excludedNodes, block.getNumBytes(),
            storagePolicySuite.getPolicy(bc.getStoragePolicyID()));
      } finally {
        srcNode.decrementPendingReplicationWithoutTargets();
      }
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

  private void removeStoredBlocksTx(final List<Integer> inodeIds,
      final Map<Integer, List<Long>> inodeIdsToBlockMap, final DatanodeDescriptor node) throws
      IOException {
    final AtomicInteger removedBlocks = new AtomicInteger(0);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_STORED_BLOCKS) {
      List<INodeIdentifier> inodeIdentifiers;
      
      @Override
      public void setUp() throws StorageException {
        inodeIdentifiers = INodeUtil.resolveINodesFromIds(inodeIds);
      }
      
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getBatchedINodesLock(inodeIdentifiers))
            .add(lf.getSqlBatchedBlocksLock()).add(
            lf.getSqlBatchedBlocksRelated(BLK.RE, BLK.IV, BLK.CR, BLK.UR,
                BLK.ER));

        if (((FSNamesystem) namesystem).isErasureCodingEnabled() && inodeIdentifiers != null) {
          locks.add(lf.getBatchedEncodingStatusLock(LockType.WRITE, inodeIdentifiers));
        }
      }

      @Override
      public Object performTask() throws IOException {
        for(INodeIdentifier identifier: inodeIdentifiers){
          for (long blockId : inodeIdsToBlockMap.get(identifier.getInodeId())) {
            BlockInfo block = EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId, blockId);
            removeStoredBlock(block, node);
            removedBlocks.incrementAndGet();
          }
        }
        return null;
      }
    }.handle(namesystem);
    LOG.info("removed " + removedBlocks.get() + " replicas from " + node.getName());
  }
  
  private void removeStoredBlocksTx(final List<Integer> inodeIds,
      final Map<Integer, List<Long>> inodeIdsToBlockMap, final int sid) throws
      IOException {
    final AtomicInteger removedBlocks = new AtomicInteger(0);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_STORED_BLOCKS) {
      List<INodeIdentifier> inodeIdentifiers;
      
      @Override
      public void setUp() throws StorageException {
        inodeIdentifiers = INodeUtil.resolveINodesFromIds(inodeIds);
      }
      
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getBatchedINodesLock(inodeIdentifiers))
            .add(lf.getSqlBatchedBlocksLock()).add(
            lf.getSqlBatchedBlocksRelated(BLK.RE, BLK.IV, BLK.CR, BLK.UR,
                BLK.ER));

        if (((FSNamesystem) namesystem).isErasureCodingEnabled() && inodeIdentifiers != null) {
          locks.add(lf.getBatchedEncodingStatusLock(LockType.WRITE, inodeIdentifiers));
        }
      }

      @Override
      public Object performTask() throws IOException {
        for(INodeIdentifier identifier: inodeIdentifiers){
          for (long blockId : inodeIdsToBlockMap.get(identifier.getInodeId())) {
            BlockInfo block = EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId, blockId);
            removeStoredBlock(block, sid);
            removedBlocks.incrementAndGet();
          }
        }
        return null;
      }
    }.handle(namesystem);
    LOG.info("removed " + removedBlocks.get() + " replicas from " + sid);
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
        locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
            .add(lf.getBlockLock(b.getBlockId(), inodeIdentifier))
            .add(lf.getVariableLock(Variable.Finder.ReplicationIndex, LockType.WRITE))
            .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.PE, BLK.UR, BLK.UC));
      }

      @Override
      public Object performTask() throws IOException {
        return computeReplicationWorkForBlockInternal(b, priority);
      }
    }.handle(namesystem);
  }

  public BlockInfo tryToCompleteBlock(final BlockCollection bc,
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
    namesystem.adjustSafeModeBlockTotals(null, 1);
    namesystem.incrementSafeBlockCount(Math.min(numNodes, minReplication),curBlock);

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
          " setting inodeId to be " + BlockInfo.NON_EXISTING_ID);
      binfo = new BlockInfo(b, BlockInfo.NON_EXISTING_ID);
    }
    return binfo;
  }

  private Block addStoredBlockTx(final BlockInfo block,
      final DatanodeStorageInfo storage, final DatanodeDescriptor
      delNodeHint, final boolean logEveryBlock) throws IOException {
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
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
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
        return addStoredBlock(block, storage, delNodeHint, logEveryBlock);
      }
    }.handle();
  }

  private void addStoredBlockUnderConstructionTx( final StatefulBlockInfo ucBlock,
      final DatanodeStorageInfo storage) throws IOException {

    new HopsTransactionalRequestHandler(
        HDFSOperationType.AFTER_PROCESS_REPORT_ADD_UC_BLK) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(ucBlock.reportedBlock);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
            .add(lf.getIndividualBlockLock(ucBlock.reportedBlock.getBlockId(), inodeIdentifier))
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
        addStoredBlockUnderConstruction(ucBlock, storage);
        return null;
      }
    }.handle();
  }

  private void addToInvalidates(final Collection<Block> blocks,
      final DatanodeStorageInfo storage) throws IOException {
    invalidateBlocks.add(blocks, storage);
  }

  private void markBlockAsCorruptTx(final BlockToMarkCorrupt b,
      final DatanodeStorageInfo storage) throws IOException {
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
        markBlockAsCorrupt(b, storage, storage.getDatanodeDescriptor());
        return null;
      }
    }.handle();
  }

  public int getTotalCompleteBlocks() throws IOException {
    return blocksMap.sizeCompleteOnly();
  }

  private void addStoredBlockUnderConstructionImmediateTx(
      final BlockInfoUnderConstruction block, final DatanodeStorageInfo storage,
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
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
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
        block.addReplicaIfNotPresent(storage, reportedState, block.getGenerationStamp());
        //and fall through to next clause
        //add replica if appropriate
        if (reportedState == ReplicaState.FINALIZED) {
          addStoredBlockImmediate(block, storage, false);
        }
        return null;
      }
    }.handle();
  }

  private void addStoredBlockImmediateTx(final BlockInfo block,
      final DatanodeStorageInfo storage, final boolean logEveryBlock) throws IOException {
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
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
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
        addStoredBlockImmediate(block, storage, logEveryBlock);
        return null;
      }
    }.handle();
  }
 
  public void shutdown() {
    stopReplicationInitializer();
  }
}
