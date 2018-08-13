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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ExceptionCheck;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;

import static org.apache.hadoop.util.Time.now;

import static org.apache.hadoop.util.Time.now;

/**
 * One instance per block-pool/namespace on the DN, which handles the
 * heartbeats
 * to the active and standby NNs for that namespace. This class manages an
 * instance of {@link BPServiceActor} for each NN, and delegates calls to both
 * NNs. It also maintains the state about which of the NNs is considered
 * active.
 */
@InterfaceAudience.Private
class BPOfferService implements Runnable {

  static final Log LOG = DataNode.LOG;
  /**
   * Information about the namespace that this service is registering with.
   * This
   * is assigned after the first phase of the handshake.
   */
  NamespaceInfo bpNSInfo;
  /**
   * The registration information for this block pool. This is assigned after
   * the second phase of the handshake.
   */
  volatile DatanodeRegistration bpRegistration;
  private final DataNode dn;
  /**
   * A reference to the BPServiceActor associated with the currently ACTIVE NN.
   * In the case that all NameNodes are in STANDBY mode, this can be null. If
   * non-null, this must always refer to a member of the {@link #bpServices}
   * list.
   */
  private BPServiceActor bpServiceToActive = null;
  /**
   * The list of all actors for namenodes in this nameservice, regardless of
   * their active or standby states.
   */
  private List<BPServiceActor> bpServices =
      new CopyOnWriteArrayList<>();
  /**
   * Each time we receive a heartbeat from a NN claiming to be ACTIVE, we
   * record
   * that NN's most recent transaction ID here, so long as it is more recent
   * than the previous value. This allows us to detect split-brain scenarios in
   * which a prior NN is still asserting its ACTIVE state but with a too-low
   * transaction ID. See HDFS-2627 for details.
   */
  private long lastActiveClaimTxId = -1;

  private final DNConf dnConf;
  // IBR = Incremental Block Report. If this flag is set then an IBR will be
  // sent immediately by the actor thread without waiting for the IBR timer
  // to elapse.
  private volatile boolean sendImmediateIBR = false;
  volatile long lastDeletedReport = 0;
  // lastBlockReport, lastDeletedReport and lastHeartbeat may be assigned/read
  // by testing threads (through BPServiceActor#triggerXXX), while also
  // assigned/read by the actor thread. Thus they should be declared as volatile
  // to make sure the "happens-before" consistency.
  private volatile long lastBlockReport = 0;
  private boolean resetBlockReportTime = true;
  
  volatile long lastCacheReport = 0;
  
  private BPServiceActor blkReportHander = null;
  private List<ActiveNode> nnList = Collections.synchronizedList(new ArrayList<ActiveNode>());
  private List<InetSocketAddress> blackListNN = Collections.synchronizedList(new ArrayList<InetSocketAddress>());
//  private Object nnListSync = new Object();
  private volatile int rpcRoundRobinIndex = 0;
  // you have bunch of NNs, which one to send the incremental block report
  private volatile int refreshNNRoundRobinIndex = 0;
  final int maxNumIncrementalReportThreads;
  private final ExecutorService incrementalBRExecutor;
  private final ExecutorService brDispatcher;
  //in a heart beat only one actor should talk to name node and get the updated list of NNs
  //how to stop actors from communicating with all the NN at the same time for same RPC?
  //for that we will use a separate RR which will be incremented after Delta time (heartbeat time)
  /**
   * Between block reports (which happen on the order of once an hour) the DN
   * reports smaller incremental changes to its block list. This map, keyed by
   * block ID, contains the pending changes which have yet to be reported to
   * the
   * NN. Access should be synchronized on this object.
   */
  private final Map<DatanodeStorage, PerStoragePendingIncrementalBR>
      pendingIncrementalBRperStorage = Maps.newHashMap();

  private Thread blockReportThread = null;


  BPOfferService(List<InetSocketAddress> nnAddrs, DataNode dn) {
    Preconditions
        .checkArgument(!nnAddrs.isEmpty(), "Must pass at least one NN.");
    this.dn = dn;

    for (InetSocketAddress addr : nnAddrs) {
      this.bpServices.add(new BPServiceActor(addr, this));
      nnList.add(new ActiveNodePBImpl(0, "", addr.getAddress().getHostAddress(),
          addr.getPort(), "", addr.getAddress().getHostAddress(), addr.getPort()));
    }

    dnConf = dn.getDnConf();


    maxNumIncrementalReportThreads = dnConf.iBRDispatherTPSize;
    incrementalBRExecutor = Executors.newFixedThreadPool(maxNumIncrementalReportThreads);
    brDispatcher = Executors.newSingleThreadExecutor();

  }

  void refreshNNList(ArrayList<InetSocketAddress> addrs) throws IOException {
    Set<InetSocketAddress> oldAddrs = Sets.newHashSet();
    for (BPServiceActor actor : bpServices) {
      oldAddrs.add(actor.getNNSocketAddress());
    }
    Set<InetSocketAddress> newAddrs = Sets.newHashSet(addrs);


    SetView<InetSocketAddress> deadNNs = Sets.difference(oldAddrs, newAddrs);
    SetView<InetSocketAddress> newNNs = Sets.difference(newAddrs, oldAddrs);

    // stop the dead threads
    if (deadNNs.size() != 0) {
      for (InetSocketAddress deadNN : deadNNs) {
        BPServiceActor deadActor = stopAnActor(deadNN);
        bpServices.remove(
            deadActor); // NNs will not change frequently. so modification ops will not be expensive on the copyonwirte list
        LOG.debug("Stopped actor for " + deadActor.getNNSocketAddress());
      }
    }

    // start threads for new NNs
    if (newNNs.size() != 0) {
      for (InetSocketAddress newNN : newNNs) {
        BPServiceActor newActor = startAnActor(newNN);
        bpServices.add(
            newActor); // NNs will not change frequently. so modification ops will not be expensive on the copyonwirte list
        LOG.debug("Started actor for " + newActor.getNNSocketAddress());
      }
    }

  }

  /**
   * @return true if the service has registered with at least one NameNode.
   */
  boolean isInitialized() {
    return bpRegistration != null;
  }

  /**
   * @return true if there is at least one actor thread running which is talking
   * to a NameNode.
   */
  boolean isAlive() {
    for (BPServiceActor actor : bpServices) {
      if (actor.isAlive()) {
        return true;
      }
    }
    return false;
  }

  synchronized String getBlockPoolId() {
    if (bpNSInfo != null) {
      return bpNSInfo.getBlockPoolID();
    } else {
      LOG.warn("Block pool ID needed, but service not yet registered with NN",
          new Exception("trace"));
      return null;
    }
  }

  synchronized NamespaceInfo getNamespaceInfo() {
    return bpNSInfo;
  }

  @Override
  public synchronized String toString() {
    if (bpNSInfo == null) {
      // If we haven't yet connected to our NN, we don't yet know our
      // own block pool ID.
      // If _none_ of the block pools have connected yet, we don't even
      // know the DatanodeID ID of this DN.
      String datanodeUuid = dn.getDatanodeUuid();
      if (datanodeUuid == null || datanodeUuid.isEmpty()) {
        datanodeUuid = "unassigned";
      }
      return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
    } else {
      return "Block pool " + getBlockPoolId()
          + " (Datanode Uuid " + dn.getDatanodeUuid() + ")";
    }
  }

  void reportBadBlocks(ExtendedBlock block, String storageUuid, StorageType storageType) {
    checkBlock(block);
    try {
      reportBadBlocksWithRetry(block, storageUuid, storageType);
    } catch (Exception e) {
      LOG.error("Failed to send bad block report to any namenode ");
      e.printStackTrace();
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint,
      String storageUuid) {
    checkBlock(block);
    checkDelHint(delHint);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECEIVED, delHint);

    notifyNamenodeBlockImmediatelyInt(bInfo, storageUuid, true);
  }

  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null, "block is null");
    Preconditions.checkArgument(block.getBlockPoolId().equals(getBlockPoolId()),
        "block belongs to BP %s instead of BP %s", block.getBlockPoolId(),
        getBlockPoolId());
  }

  private void checkDelHint(String delHint) {
    Preconditions.checkArgument(delHint != null, "delHint is null");
  }

  void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.DELETED, null);

    notifyNamenodeDeletedBlockInt(bInfo, dn.getFSDataset().getStorage(storageUuid));
  }
  
  public void notifyNamenodeCreatingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.CREATING, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, storageUuid, false);
  }

  public void notifyNamenodeAppendingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.APPENDING, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, storageUuid, false);
  }

  public void notifyNamenodeAppendingRecoveredAppend(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECOVERING_APPEND, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, storageUuid, true);
  }

  public void notifyNamenodeUpdateRecoveredBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.UPDATE_RECOVERED, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, storageUuid, true);
  }


  //This must be called only by blockPoolManager
  void start() {
    for (BPServiceActor actor : bpServices) {
      actor.start();
    }
  }

  //This must be called only by blockPoolManager.
  void stop() {
    for (BPServiceActor actor : bpServices) {
      actor.stop();
    }
  }

  //This must be called only by blockPoolManager
  void join() {
    for (BPServiceActor actor : bpServices) {
      actor.join();
    }
  }

  DataNode getDataNode() {
    return dn;
  }

  /**
   * Called by the BPServiceActors when they handshake to a NN. If this is the
   * first NN connection, this sets the namespace info for this BPOfferService.
   * If it's a connection to a new NN, it verifies that this namespace matches
   * (eg to prevent a misconfiguration where a StandbyNode from a different
   * cluster is specified)
   */
  synchronized void verifyAndSetNamespaceInfo(NamespaceInfo nsInfo)
      throws IOException {
    if (this.bpNSInfo == null) {
      this.bpNSInfo = nsInfo;
      boolean success = false;
      
      // Now that we know the namespace ID, etc, we can pass this to the DN.
      // The DN can now initialize its local storage if we are the
      // first BP to handshake, etc.
      try {
        dn.initBlockPool(this);
        success = true;
      } finally {
        if (!success) {
          // The datanode failed to initialize the BP. We need to reset
          // the namespace info so that other BPService actors still have
          // a chance to set it, and re-initialize the datanode.
          this.bpNSInfo = null;
        }
      }
    } else {
      checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
          "Blockpool ID");
      checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
          "Namespace ID");
      checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
          "Cluster ID");
    }
  }

  /**
   * After one of the BPServiceActors registers successfully with the NN, it
   * calls this function to verify that the NN it connected to is consistent
   * with other NNs serving the block-pool.
   */
  synchronized void registrationSucceeded(BPServiceActor bpServiceActor,
      DatanodeRegistration reg) throws IOException {
    if (bpRegistration != null) {
      checkNSEquality(bpRegistration.getStorageInfo().getNamespaceID(),
          reg.getStorageInfo().getNamespaceID(), "namespace ID");
      checkNSEquality(bpRegistration.getStorageInfo().getClusterID(),
          reg.getStorageInfo().getClusterID(), "cluster ID");
    } else {
      bpRegistration = reg;
    }

    dn.bpRegistrationSucceeded(bpRegistration, getBlockPoolId());
    // Add the initial block token secret keys to the DN's secret manager.
    if (dn.isBlockTokenEnabled) {
      dn.blockPoolTokenSecretManager
          .addKeys(getBlockPoolId(), reg.getExportedKeys());
    }
  }

  /**
   * Verify equality of two namespace-related fields, throwing an exception if
   * they are unequal.
   */
  private static void checkNSEquality(Object ourID, Object theirID,
      String idHelpText) throws IOException {
    if (!ourID.equals(theirID)) {
      throw new IOException(
          idHelpText + " mismatch: " + "previously connected to " + idHelpText +
              " " + ourID + " but now connected to " + idHelpText + " " +
              theirID);
    }
  }

  synchronized DatanodeRegistration createRegistration() throws IOException {
    Preconditions.checkState(bpNSInfo != null,
        "getRegistration() can only be called after initial handshake");
    return dn.createBPRegistration(bpNSInfo);
  }

  /**
   * Called when an actor shuts down. If this is the last actor to shut down,
   * shuts down the whole blockpool in the DN.
   */
  synchronized void shutdownActor(BPServiceActor actor) {
    if (bpServiceToActive == actor) {
      bpServiceToActive = null;
    }

    bpServices.remove(actor);

    // remove from nnList
    for (ActiveNode ann : nnList) {
      if (ann.getRpcServerAddressForDatanodes().equals(actor.getNNSocketAddress())) {
          nnList.remove(ann);
        break;
      }
    }

    if (bpServices.isEmpty()) {
      dn.shutdownBlockPool(this);
    }
  }

  /**
   * Called by the DN to report an error to the NNs.
   */
  void trySendErrorReport(int errCode, String errMsg) {
    //HOP error report should be sent to all the NN.
    //Leader will delete the blocks and clear the in meomory data structs from Datanode manager and HB Manager.
    //Non leader NNs will only clear the in memory data structures.
    for (BPServiceActor actor : bpServices) {
      actor.trySendErrorReport(errCode, errMsg);
    }
  }

  /**
   * Ask each of the actors to schedule a block report after the specified
   * delay.
   */
  void scheduleBlockReport(long delay) {
    scheduleBlockReportInt(delay);
  }

  /**
   * Ask each of the actors to report a bad block hosted on another DN.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block) {
    try {
      reportRemoteBadBlockWithRetry(dnInfo, block);
    } catch (IOException e) {
      LOG.warn("Couldn't report bad block " + block + "" + e);
    }
  }

  /**
   * @return a proxy to the active NN, or null if the BPOS has not acknowledged
   * any NN as active yet.
   */
  synchronized DatanodeProtocolClientSideTranslatorPB getActiveNN() {
    if (bpServiceToActive != null) {
      return bpServiceToActive.bpNamenode;
    } else {
      return null;
    }
  }

  @VisibleForTesting
  List<BPServiceActor> getBPServiceActors() {
    return Lists.newArrayList(bpServices);
  }

  /**
   * @return true if the given NN address is one of the NNs for this block pool
   */
  boolean containsNN(InetSocketAddress addr) {
    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(addr)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  int countNameNodes() {
    return bpServices.size();
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() throws IOException {
    triggerBlockReportForTestsInt();
  }

  /**
   * Run an immediate deletion report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerDeletionReportForTests() throws IOException {
    triggerDeletionReportForTestsInt();
  }

  /**
   * Run an immediate heartbeat from all actors. Used by tests.
   */
  @VisibleForTesting
  void triggerHeartbeatForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerHeartbeatForTests();
    }
  }

  boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {

    assert bpServices.contains(actor);
    
    if (cmd == null) {
      return true;
    }

    /*
     * Datanode Registration can be done asynchronously here. No need to hold
     * the lock. for more info refer HDFS-5014
     */
    if (DatanodeProtocol.DNA_REGISTER == cmd.getAction()) {
      LOG.info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr);
      actor.reRegister();
      return true;
    }
    
    synchronized (this) {
      return processCommandFromActive(cmd, actor);
    }
  }

  private String blockIdArrayToString(long ids[]) {
    long maxNumberOfBlocksToLog = dn.getMaxNumberOfBlocksToLog();
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (int i = 0; i < ids.length; i++) {
      if (i >= maxNumberOfBlocksToLog) {
        bld.append("...");
        break;
      }
      bld.append(prefix).append(ids[i]);
      prefix = ", ";
    }
    return bld.toString();
  }
  
  /**
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    final BlockCommand bcmd =
        cmd instanceof BlockCommand ? (BlockCommand) cmd : null;
    final BlockIdCommand blockIdCmd = 
      cmd instanceof BlockIdCommand ? (BlockIdCommand)cmd: null;
    
    switch (cmd.getAction()) {
      case DatanodeProtocol.DNA_TRANSFER:
        // Send a copy of a block to another datanode
        dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
            bcmd.getTargets(), bcmd.getTargetStorageTypes());
        dn.metrics.incrBlocksReplicated(bcmd.getBlocks().length);
        break;
      case DatanodeProtocol.DNA_INVALIDATE:
        //
        // Some local block(s) are obsolete and can be
        // safely garbage-collected.
        //
        Block toDelete[] = bcmd.getBlocks();
        try {
          if (dn.blockScanner != null) {
            dn.blockScanner.deleteBlocks(bcmd.getBlockPoolId(), toDelete);
          }
          // using global fsdataset
          dn.getFSDataset().invalidate(bcmd.getBlockPoolId(), toDelete);
        } catch (IOException e) {
          // Exceptions caught here are not expected to be disk-related.
          throw e;
        }
        dn.metrics.incrBlocksRemoved(toDelete.length);
        break;
      case DatanodeProtocol.DNA_CACHE:
        LOG.info("DatanodeCommand action: DNA_CACHE for " + blockIdCmd.getBlockPoolId() + " of ["
            + blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
        dn.getFSDataset().cache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
        break;
      case DatanodeProtocol.DNA_UNCACHE:
        LOG.info("DatanodeCommand action: DNA_UNCACHE for " + blockIdCmd.getBlockPoolId() + " of ["
            + blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
        dn.getFSDataset().uncache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
        break;
      case DatanodeProtocol.DNA_SHUTDOWN:
        // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
        // See HDFS-2987.
        throw new UnsupportedOperationException(
            "Received unimplemented DNA_SHUTDOWN");
      case DatanodeProtocol.DNA_FINALIZE:
        String bp = ((FinalizeCommand) cmd).getBlockPoolId();
        assert getBlockPoolId().equals(bp) :
            "BP " + getBlockPoolId() + " received DNA_FINALIZE " +
                "for other block pool " + bp;

        dn.finalizeUpgradeForPool(bp);
        break;
      case DatanodeProtocol.DNA_RECOVERBLOCK:
        String who = "NameNode at " + actor.getNNSocketAddress();
        dn.recoverBlocks(who,
            ((BlockRecoveryCommand) cmd).getRecoveringBlocks());
        break;
      case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
        LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
        if (dn.isBlockTokenEnabled) {
          dn.blockPoolTokenSecretManager.addKeys(getBlockPoolId(),
              ((KeyUpdateCommand) cmd).getExportedKeys());
        }
        break;
      case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
        LOG.info("DatanodeCommand action: DNA_BALANCERBANDWIDTHUPDATE");
        long bandwidth =
            ((BalancerBandwidthCommand) cmd).getBalancerBandwidthValue();
        if (bandwidth > 0) {
          DataXceiverServer dxcs =
              (DataXceiverServer) dn.dataXceiverServer.getRunnable();
          LOG.info("Updating balance throttler bandwidth from " +
              dxcs.balanceThrottler.getBandwidth() + " bytes/s " + "to: " +
              bandwidth + " bytes/s.");
          dxcs.balanceThrottler.setBandwidth(bandwidth);
        }
        break;
      default:
        LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }


  private BPServiceActor stopAnActor(InetSocketAddress address) {

    BPServiceActor actor = getAnActor(address);
    if (actor != null) {
      actor.stop();
//      actor.join();
      return actor;
    } else {
      return null;
    }
  }

  private BPServiceActor startAnActor(InetSocketAddress address) {
    BPServiceActor actor = new BPServiceActor(address, this);
    actor.start();
    return actor;
  }

  private BPServiceActor getAnActor(InetSocketAddress address) {
    if (address == null) {
      return null;
    }

    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(address)) {
        return actor;
      }
    }
    return null;
  }

  /**
   * Main loop for each BP thread. Run until shutdown, forever calling remote
   * NameNode functions.
   */
  private void whirlingLikeASufi()
      throws Exception {   //http://en.wikipedia.org/wiki/Sufi_whirling

    while (dn.shouldRun) {  //as long as datanode is alive keep working
      try {
        long startTime = now();

        if (sendImmediateIBR ||
            (startTime - lastDeletedReport > dnConf.deleteReportInterval)) {
          reportReceivedDeletedBlocks();
          lastDeletedReport = startTime;
        }

        startBRThread();

        //
        // There is no work to do;  sleep until hearbeat timer elapses,
        // or work arrives, and then iterate again.
        //
        long waitTime = 1000;
        synchronized (pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {
            try {
              pendingIncrementalBRperStorage.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized

        forwardRRIndex();//after every 1000ms increment the refreshNNRoundRobinIndex
      } catch (Exception re) {
        LOG.warn("Exception in whirlingLikeASufi", re);
        try {
          long sleepTime = 1000;
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    } // while (shouldRun())
  } // offerService

  private BRTask brTask = new BRTask();
  private Future futur = null;
  private void startBRThread() throws InterruptedException, ExecutionException{
    if (futur == null || futur.isDone()) {
      if (futur != null) {
        //check that previous run did not end with an exception
        try {
          futur.get();
        } finally {
          futur = null;
        }
      }
      futur = brDispatcher.submit(brTask);
    }
  }

  private class BRTask implements Callable{
  /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public Object call() throws Exception {
      List<DatanodeCommand> cmds = blockReport();

      if(cmds != null && blkReportHander != null) { //it is not null if the block report is
        // successful
        blkReportHander.processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));
      }
      
      DatanodeCommand cmd = cacheReport(cmds!=null);
      blkReportHander.processCommand(new DatanodeCommand[]{cmd});
      
      // Now safe to start scanning the block pool.
      // If it has already been started, this is a no-op.
      if (dn.blockScanner != null) {
        dn.blockScanner.addBlockPool(getBlockPoolId());
      }
      return null;
    }
  }

  private final Object incrementalBRLock = new Object();
  private int incrementalBRCounter = 0;
  private final IncrementalBRTask incrementalBRTask = new IncrementalBRTask();
  /**
   * Report received blocks and delete hints to the Namenode
   *
   * @throws IOException
   */
  private void reportReceivedDeletedBlocks() throws IOException {
    synchronized (incrementalBRLock) {
      if (incrementalBRCounter < maxNumIncrementalReportThreads){
        incrementalBRCounter++;
        incrementalBRExecutor.submit(incrementalBRTask);
      }
    }
  }

public class IncrementalBRTask implements Callable{
    @Override
    public Object call() throws Exception {
    // Generate a list of the pending reports for each storage under the lock
    List<StorageReceivedDeletedBlocks> reports =
      new ArrayList<>(pendingIncrementalBRperStorage.size());
    synchronized (pendingIncrementalBRperStorage) {
      for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
          pendingIncrementalBRperStorage.entrySet()) {
        final DatanodeStorage storage = entry.getKey();
        final PerStoragePendingIncrementalBR perStorageMap = entry.getValue();
        if (perStorageMap.getBlockInfoCount() > 0) {
          // Send newly-received and deleted blockids to namenode
          ReceivedDeletedBlockInfo[] rdbi = perStorageMap.dequeueBlockInfos();
          reports.add(new StorageReceivedDeletedBlocks(storage, rdbi));
        }
      }
      sendImmediateIBR = false;
    }

    if (reports.size() == 0) {
      // Nothing new to report.
      synchronized (incrementalBRLock){
        incrementalBRCounter--;
      }
      return null;
      }
      // Send incremental block reports to the Namenode outside the lock
      boolean success = false;
      try {
        blockReceivedAndDeletedWithRetry(reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]));
        success = true;
      } finally {
        if (!success) {
          synchronized (pendingIncrementalBRperStorage) {
            for (StorageReceivedDeletedBlocks report : reports) {
              // If we didn't succeed in sending the report, put all of the
              // blocks back onto our queue, but only in the case where we
              // didn't put something newer in the meantime.
              PerStoragePendingIncrementalBR perStorageMap = pendingIncrementalBRperStorage.get(report.getStorage());
              perStorageMap.putMissingBlockInfos(report.getBlocks());
              sendImmediateIBR = true;
            }
          }
        }
      }
    synchronized (incrementalBRLock){
      incrementalBRCounter--;
    }
    return null;
  }
}

  /**
   * Retrieve the incremental BR state for a given storage UUID
   * @param storageUuid
   * @return
   */
  private PerStoragePendingIncrementalBR getIncrementalBRMapForStorage(
      DatanodeStorage storage) {
    PerStoragePendingIncrementalBR mapForStorage =
        pendingIncrementalBRperStorage.get(storage);
    if (mapForStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      mapForStorage = new PerStoragePendingIncrementalBR();
      pendingIncrementalBRperStorage.put(storage, mapForStorage);
    }
    return mapForStorage;
  }
  /**
   * Add a blockInfo for notification to NameNode. If another entry
   * exists for the same block it is removed.
   *
   * Caller must synchronize access using pendingIncrementalBRperStorage.
   * @param bInfo
   * @param storageUuid
   */
  boolean addPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo,
      DatanodeStorage storage) {
    // Make sure another entry for the same block is first removed.
    // There may only be one such entry.
    boolean isNew = true;
    for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
        pendingIncrementalBRperStorage.entrySet()) {
      if (entry.getValue().removeBlockInfo(bInfo)) {
        isNew = false;
        break;
      }
    }
    getIncrementalBRMapForStorage(storage).putBlockInfo(bInfo);
    return isNew;
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeBlockImmediatelyInt(
      ReceivedDeletedBlockInfo bInfo, String storageUuid, boolean now) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(bInfo, dn.getFSDataset().getStorage(storageUuid));
      sendImmediateIBR = true;
      if (now) {
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }

  void notifyNamenodeDeletedBlockInt(
      ReceivedDeletedBlockInfo bInfo, DatanodeStorage storage) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(bInfo, storage);
    }
  }

  /**
   * Report the list blocks to the Namenode
   *
   * @throws IOException
   */
  
  List<DatanodeCommand> blockReport() throws IOException {
    // send block report if timer has expired.
    final long startTime = now();
    if (startTime - lastBlockReport <= dnConf.blockReportInterval) {
      return null;
    }

      

    ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // Flush any block information that precedes the block report. Otherwise
    // we have a chance that we will miss the delHint information
    // or we will report an RBW replica after the BlockReport already reports
    // a FINALIZED one.
    reportReceivedDeletedBlocks();
    lastDeletedReport = startTime;

    long brCreateStartTime = now();
    Map<DatanodeStorage, BlockReport> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(getBlockPoolId());

    // Convert the reports to the format expected by the NN.
    int i = 0;
    int totalBlockCount = 0;
    StorageBlockReport reports[] =
        new StorageBlockReport[perVolumeBlockLists.size()];

    for(Map.Entry<DatanodeStorage, BlockReport> kvPair : perVolumeBlockLists.entrySet()) {
      BlockReport blockList = kvPair.getValue();
      reports[i++] = new StorageBlockReport(kvPair.getKey(), blockList);
      totalBlockCount += blockList.getNumberOfBlocks();
    }
    
    // Get a namenode to send the report(s) to
    ActiveNode an = nextNNForBlkReport(totalBlockCount);
    if (an != null) {
      blkReportHander = getAnActor(an.getRpcServerAddressForDatanodes());
      if (blkReportHander == null || !blkReportHander.isInitialized()) {
        return null; //no one is ready to handle the request, return now without changing the values of lastBlockReport. it will be retried in next cycle
      }
    } else {
      LOG.warn("Unable to send block report");
      return null;
    }

    // Send the reports to the NN.
    int numReportsSent;
    long brSendStartTime = now();
    if (totalBlockCount < dnConf.blockReportSplitThreshold) {
      // Below split threshold, send all reports in a single message.
      numReportsSent = 1;
      DatanodeCommand cmd =
          blkReportHander.blockReport(bpRegistration, getBlockPoolId(), reports);
      if (cmd != null) {
        cmds.add(cmd);
      }
    } else {
      // Send one block report per message.
      numReportsSent = i;
      for (StorageBlockReport report : reports) {
        StorageBlockReport singleReport[] = { report };
        DatanodeCommand cmd = blkReportHander.blockReport(
            bpRegistration, getBlockPoolId(), singleReport);
        if (cmd != null) {
          cmds.add(cmd);
        }
      }
    }
    // Log the block report processing stats from Datanode perspective
    long brSendCost = now() - brSendStartTime;
    long brCreateCost = brSendStartTime - brCreateStartTime;
    dn.getMetrics().addBlockReport(brSendCost);
    LOG.info("Sent " + numReportsSent + " blockreports " + totalBlockCount +
        " blocks total. Took " + brCreateCost +
        " msec to generate and " + brSendCost +
        " msecs for RPC and NN processing. " +
        " Got back commands " +
            (cmds.size() == 0 ? "none" : Joiner.on("; ").join(cmds)));

    scheduleNextBlockReport(startTime);
    return cmds.size() == 0 ? null : cmds;
  }

  private void scheduleNextBlockReport(long previousReportStartTime) {
    // If we have sent the first set of block reports, then wait a random
    // time before we start the periodic block reports.
    if (resetBlockReportTime) {
      lastBlockReport = previousReportStartTime -
          DFSUtil.getRandom().nextInt((int)(dnConf.blockReportInterval));
      resetBlockReportTime = false;
    } else {
      /* say the last block report was at 8:20:14. The current report
       * should have started around 9:20:14 (default 1 hour interval).
       * If current time is :
       *   1) normal like 9:20:18, next report should be at 10:20:14
       *   2) unexpected like 11:35:43, next report should be at 12:20:14
       */
      lastBlockReport += (now() - lastBlockReport) /
          dnConf.blockReportInterval * dnConf.blockReportInterval;
    }
  }

  DatanodeCommand cacheReport(boolean hasHandler) throws IOException {
    // If caching is disabled, do not send a cache report
    if (dn.getFSDataset().getCacheCapacity() == 0) {
      return null;
    }
    // send cache report if timer has expired.
    DatanodeCommand cmd = null;
    long startTime = Time.monotonicNow();
    if (startTime - lastCacheReport > dnConf.cacheReportInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending cacheReport from service actor: " + this);
      }
      lastCacheReport = startTime;

      String bpid = getBlockPoolId();
      List<Long> blockIds = dn.getFSDataset().getCacheReport(bpid);
      long createTime = Time.monotonicNow();

      // Get a namenode to send the report(s) to (if we did a block report we send to the same node)
      if (!hasHandler) {
        ActiveNode an = nextNNForCacheReport(blockIds.size());
        if (an != null) {
          blkReportHander = getAnActor(an.getRpcServerAddressForDatanodes());
          if (blkReportHander == null || !blkReportHander.isInitialized()) {
            return null; //no one is ready to handle the request, return now without changing the values of lastBlockReport. it will be retried in next cycle
          }
        } else {
          LOG.warn("Unable to send cache report");
          return null;
        }
      }
      
      cmd = blkReportHander.cacheReport(bpRegistration, bpid, blockIds);
      long sendTime = Time.monotonicNow();
      long createCost = createTime - startTime;
      long sendCost = sendTime - createTime;
      dn.getMetrics().addCacheReport(sendCost);
      LOG.debug("CacheReport of " + blockIds.size()
          + " block(s) took " + createCost + " msec to generate and "
          + sendCost + " msecs for RPC and NN processing");
    }
    return cmd;
  }
  
  /**
   * This methods arranges for the data node to send the block report at the
   * next heartbeat.
   */
  void scheduleBlockReportInt(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = Time.now() - (dnConf.blockReportInterval -
          DFSUtil.getRandom().nextInt((int) (delay)));
    } else { // send at next heartbeat
      lastBlockReport = 0;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  void triggerBlockReportForTestsInt() {
    synchronized (pendingIncrementalBRperStorage) {
      lastBlockReport = 0;
      pendingIncrementalBRperStorage.notifyAll();
      while (lastBlockReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  void triggerDeletionReportForTestsInt() {
    synchronized (pendingIncrementalBRperStorage) {
      lastDeletedReport = 0;
      pendingIncrementalBRperStorage.notifyAll();

      while (lastDeletedReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  boolean hasPendingIBR() {
    return sendImmediateIBR;
  }

  synchronized void updateNNList(SortedActiveNodeList list) throws IOException {
    ArrayList<InetSocketAddress> nnAddresses =
        new ArrayList<InetSocketAddress>();
    for (ActiveNode ann : list.getActiveNodes()) {
      nnAddresses.add(ann.getRpcServerAddressForDatanodes());
    }

    refreshNNList(nnAddresses);

    if (list.getLeader() != null) {
      bpServiceToActive = getAnActor(list.getLeader().getRpcServerAddressForDatanodes());
    }

      nnList.clear();
      nnList.addAll(list.getActiveNodes());
      blackListNN.clear();
  }


  long lastupdate = System.currentTimeMillis();
  synchronized boolean canUpdateNNList(InetSocketAddress address) {
    if (nnList == null || nnList.size() == 0) {
      return true; // for edge case, any one can update. after that actors will take trun in updating the nnlist
    }

    if( (System.currentTimeMillis() - lastupdate ) > 2000 ){
      lastupdate = System.currentTimeMillis();
      return  true;
    }else{
        return false;
      }
    }

  public void startWhirlingSufiThread() {
    if (blockReportThread == null || !blockReportThread.isAlive()) {
      blockReportThread = new Thread(this, "BlkReportThread");
      blockReportThread.setDaemon(true); // needed for JUnit testing
      blockReportThread.start();
    }
  }

  @Override
  public void run() {
    try {
      whirlingLikeASufi();
    } catch (Exception ex) {
      LOG.warn("Unexpected exception in BPOfferService " + this, ex);
    }
  }

  private void reportBadBlocksWithRetry(final ExtendedBlock block,
      final String storageUuid, final StorageType storageType)
      throws IOException {
    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.reportBadBlocks(block, storageUuid, storageType);
        return null;
      }
    });
  }

  private void blockReceivedAndDeletedWithRetry(
      final StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {

    String blocks = "";
    for(StorageReceivedDeletedBlocks srdb : receivedAndDeletedBlocks) {
      blocks += "[";
      for(ReceivedDeletedBlockInfo b : srdb.getBlocks()) {
        blocks += " " + b.getBlock().getBlockId() + b.toString();
      }
      blocks += "]";
    }
    NameNode.LOG.info("sending blockReceivedAndDeletedWithRetry for blocks "
        + blocks);

    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.blockReceivedAndDeleted(bpRegistration, getBlockPoolId(),
            receivedAndDeletedBlocks);
        return null;
      }
    });
  }

  private void reportRemoteBadBlockWithRetry(final DatanodeInfo dnInfo,
      final ExtendedBlock block) throws IOException {
    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.reportRemoteBadBlock(dnInfo, block);
        return null;
      }
    });
  }

  public byte[] getSmallFileDataFromNN(final int id) throws IOException {
    byte[] data = (byte[]) doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        return actor.getSmallFileDataFromNN(id);
      }
    });
    return data;
  }

  private interface ActorActionHandler {

    Object doAction(BPServiceActor actor) throws IOException;
  }

  private Object doActorActionWithRetry(ActorActionHandler handler)
      throws IOException {
    Exception exception = null;
    boolean success = false;
    BPServiceActor actor = null;
    final int MAX_RPC_RETRIES = nnList.size();

    for (int i = 0; i <= MAX_RPC_RETRIES; i++) { // min value of MAX_RPC_RETRIES is 0
      try {
        actor = nextNNForNonBlkReportRPC();
        if (actor != null) {
          Object obj = handler.doAction(actor);
          //no exception
          success = true;
          return obj;
        }

      } catch (Exception e) {
        exception = e;
        if (ExceptionCheck.isLocalConnectException(e) ||
                ExceptionCheck.isRetriableException(e)) { //retry
          //black list the namenode
          //so that it is not used again
          LOG.debug("RPC faild. NN used was " + actor.getNNSocketAddress() +
              ", retries left (" + (MAX_RPC_RETRIES - (i)) + ")  Exception " +
              e);

          if( ExceptionCheck.isLocalConnectException(e)) {
            blackListNN.add(actor.getNNSocketAddress());
          }
          continue;
        } else {
          break;
        }
      }
    }

    if (!success) {
      if (exception != null) {
        if (exception instanceof RemoteException) {
          throw (RemoteException) exception;
        } else {
          throw (IOException) exception;
        }
      }
    }
    return null;
  }

  private synchronized BPServiceActor nextNNForNonBlkReportRPC() {
    if (nnList == null || nnList.isEmpty()) {
      return null;
    }

      for (int i = 0; i < 10; i++) {
        try {
          rpcRoundRobinIndex = ++rpcRoundRobinIndex % nnList.size();
          ActiveNode ann = nnList.get(rpcRoundRobinIndex);
        if (!this.blackListNN.contains(ann.getRpcServerAddressForDatanodes())) {
          BPServiceActor actor = getAnActor(ann.getRpcServerAddressForDatanodes());
            if (actor != null) {
              return actor;
            }
          }
        } catch (Exception e) {
          //any kind of exception try again
          continue;
        }
      }
    return null;
  }

  private ActiveNode nextNNForBlkReport(long noOfBlks) throws IOException {
    if (nnList == null || nnList.isEmpty()) {
      return null;
    }

    ActiveNode annToBR = null;
    BPServiceActor leaderActor = getLeaderActor();
    if (leaderActor != null) {
      try {
        annToBR = leaderActor.nextNNForBlkReport(noOfBlks);
      } catch (RemoteException e) {
        if (e.getClassName().equals(BRLoadBalancingException.class.getName())) {
          LOG.warn(e);
        } else {
          throw e;
        }
      }
    }
    return annToBR;
  }

  private ActiveNode nextNNForCacheReport(long noOfBlks) throws IOException {
    if (nnList == null || nnList.isEmpty()) {
      return null;
    }

    ActiveNode annToBR = null;
    BPServiceActor leaderActor = getLeaderActor();
    if (leaderActor != null) {
      try {
        annToBR = leaderActor.nextNNForCacheReport(noOfBlks);
      } catch (RemoteException e) {
        if (e.getClassName().equals(CRLoadBalancingException.class.getName())) {
          LOG.warn(e);
        } else {
          throw e;
        }
      }
    }
    return annToBR;
  }
  
  private BPServiceActor getLeaderActor() {
    if (nnList.size() > 0) {
      ActiveNode leaderNode = null;
      for (ActiveNode an : nnList) {
        if (leaderNode == null) {
          leaderNode = an;
        }

        if (leaderNode.getId() > an.getId()) {
          leaderNode = an;
        }
      }
      BPServiceActor leaderActor = this.getAnActor(leaderNode.getRpcServerAddressForDatanodes());
      return leaderActor;
    }
    return null;
  }

  private synchronized void forwardRRIndex() {
      if (nnList != null && !nnList.isEmpty()) {
        // watch out for black listed NN
        for (int i = 0; i < 10; i++) {
          refreshNNRoundRobinIndex = ++refreshNNRoundRobinIndex % nnList.size();
          ActiveNode ann = nnList.get(refreshNNRoundRobinIndex);
        if (!this.blackListNN.contains(ann.getRpcServerAddressForDatanodes())) {
            return;
          }
        }
      } else {
        refreshNNRoundRobinIndex = -1;
      }
    }

  private static class PerStoragePendingIncrementalBR {
    private Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
        Maps.newHashMap();
    /**
     * Return the number of blocks on this storage that have pending
     * incremental block reports.
     * @return
     */
    int getBlockInfoCount() {
      return pendingIncrementalBR.size();
    }
    /**
     * Dequeue and return all pending incremental block report state.
     * @return
     */
    ReceivedDeletedBlockInfo[] dequeueBlockInfos() {
      ReceivedDeletedBlockInfo[] blockInfos =
          pendingIncrementalBR.values().toArray(
              new ReceivedDeletedBlockInfo[getBlockInfoCount()]);
      pendingIncrementalBR.clear();
      return blockInfos;
    }
    /**
     * Add blocks from blockArray to pendingIncrementalBR, unless the
     * block already exists in pendingIncrementalBR.
     * @param blockArray list of blocks to add.
     * @return the number of missing blocks that we added.
     */
    int putMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray) {
      int blocksPut = 0;
      for (ReceivedDeletedBlockInfo rdbi : blockArray) {
        if (!pendingIncrementalBR.containsKey(rdbi.getBlock().getBlockId())) {
          pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
          ++blocksPut;
        }
      }
      return blocksPut;
    }
    /**
     * Add pending incremental block report for a single block.
     * @param blockInfo
     */
    void putBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      pendingIncrementalBR.put(blockInfo.getBlock().getBlockId(), blockInfo);
    }
    /**
     * Remove pending incremental block report for a single block if it
     * exists.
     *
     * @param blockInfo
     * @return true if a report was removed, false if no report existed for
     *         the given block.
     */
    boolean removeBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      return (pendingIncrementalBR.remove(blockInfo.getBlock().getBlockId()) != null);
    }
  }
}
