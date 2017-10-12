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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
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
  DatanodeRegistration bpRegistration;
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
  private volatile int pendingReceivedRequests = 0;
  volatile long lastDeletedReport = 0;
  // lastBlockReport, lastDeletedReport and lastHeartbeat may be assigned/read
  // by testing threads (through BPServiceActor#triggerXXX), while also 
  // assigned/read by the actor thread. Thus they should be declared as volatile
  // to make sure the "happens-before" consistency.
  private volatile long lastBlockReport = 0;
  private boolean resetBlockReportTime = true;
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
  private final Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
      Maps.newHashMap();
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

  String getBlockPoolId() {
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
  public String toString() {
    if (bpNSInfo == null) {
      // If we haven't yet connected to our NN, we don't yet know our
      // own block pool ID.
      // If _none_ of the block pools have connected yet, we don't even
      // know the storage ID of this DN.
      String storageId = dn.getStorageId();
      if (storageId == null || "".equals(storageId)) {
        storageId = "unknown";
      }
      return "Block pool <registering> (storage id " + storageId + ")";
    } else {
      return "Block pool " + getBlockPoolId() + " (storage id " +
          dn.getStorageId() + ")";
    }
  }

  void reportBadBlocks(ExtendedBlock block) {
    checkBlock(block);
    try {
      reportBadBlocksWithRetry(block);
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
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    checkBlock(block);
    checkDelHint(delHint);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECEIVED, delHint);
  
    notifyNamenodeBlockImmediatelyInt(bInfo, true);
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

  void notifyNamenodeDeletedBlock(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.DELETED, null);
    
    notifyNamenodeDeletedBlockInt(bInfo);
  }
  
  public void notifyNamenodeCreatingBlock(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.CREATING, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, false);
  }
  
  public void notifyNamenodeAppendingBlock(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.APPENDING, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, false);
  }
  
  public void notifyNamenodeAppendingRecoveredAppend(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.RECOVERING_APPEND, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, true);
  }
  
  public void notifyNamenodeUpdateRecoveredBlock(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo =
        new ReceivedDeletedBlockInfo(block.getLocalBlock(),
            BlockStatus.UPDATE_RECOVERED, null);
    notifyNamenodeBlockImmediatelyInt(bInfo, true);
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

      // Now that we know the namespace ID, etc, we can pass this to the DN.
      // The DN can now initialize its local storage if we are the
      // first BP to handshake, etc.
      dn.initBlockPool(this);
      return;
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
  void registrationSucceeded(BPServiceActor bpServiceActor,
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

  synchronized DatanodeRegistration createRegistration() {
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

  synchronized boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {

    assert bpServices.contains(actor);
    return processCommandFromActive(cmd, actor);
  }

  /**
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    if (cmd == null) {
      return true;
    }
    final BlockCommand bcmd =
        cmd instanceof BlockCommand ? (BlockCommand) cmd : null;

    switch (cmd.getAction()) {
      case DatanodeProtocol.DNA_TRANSFER:
        // Send a copy of a block to another datanode
        dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
            bcmd.getTargets());
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
      case DatanodeProtocol.DNA_SHUTDOWN:
        // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
        // See HDFS-2987.
        throw new UnsupportedOperationException(
            "Received unimplemented DNA_SHUTDOWN");
      case DatanodeProtocol.DNA_REGISTER:
        // namenode requested a registration - at start or if NN lost contact
        LOG.info("DatanodeCommand action: DNA_REGISTER");
        actor.reRegister();
        break;
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

        if (pendingReceivedRequests > 0 ||
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
        synchronized (pendingIncrementalBR) {
          if (pendingReceivedRequests == 0) {
            try {
              pendingIncrementalBR.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized

        forwardRRIndex();

      } catch (Exception re) {
        LOG.warn("Exception in BPOfferService. NNs: "+Arrays.toString(nnList.toArray()), re);
        try {
          long sleepTime = 1000;
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    } // while (shouldRun())
  } // offerService
  
  private final Object BRLock = new Object();
  private boolean brIsRunning = false;
  private BRTask brTask = new BRTask();
  private void startBRThread(){
    synchronized (BRLock) {
      if (!brIsRunning) {
        brIsRunning = true;
        brDispatcher.submit(brTask);
      }
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
      DatanodeCommand cmd = blockReport();
      if (cmd != null) {
        blkReportHander.processCommand(new DatanodeCommand[]{cmd});
      }
      // Now safe to start scanning the block pool.
      // If it has already been started, this is a no-op.
      if (dn.blockScanner != null) {
        dn.blockScanner.addBlockPool(getBlockPoolId());
      }
      synchronized (BRLock) {
        brIsRunning = false;
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
      // check if there are newly received blocks
      ReceivedDeletedBlockInfo[] receivedAndDeletedBlockArray = null;
      synchronized (pendingIncrementalBR) {
        int numBlocks = pendingIncrementalBR.size();
        if (numBlocks > 0) {
          //
          // Send newly-received and deleted blockids to namenode
          //
          receivedAndDeletedBlockArray = pendingIncrementalBR.values()
                  .toArray(new ReceivedDeletedBlockInfo[numBlocks]);
        }
        pendingIncrementalBR.clear();
      }
      if (receivedAndDeletedBlockArray != null) {
        StorageReceivedDeletedBlocks[] report =
                {new StorageReceivedDeletedBlocks(bpRegistration.getStorageID(),
                        receivedAndDeletedBlockArray)};
        boolean success = false;
        try {
          try {
            long time = System.currentTimeMillis();
            blockReceivedAndDeletedWithRetry(report);
          } catch (IOException e) {
            e.printStackTrace();
          }
          success = true;
        } finally {
          synchronized (pendingIncrementalBR) {
            if (!success) {
              // If we didn't succeed in sending the report, put all of the
              // blocks back onto our queue, but only in the case where we didn't
              // put something newer in the meantime.
              for (ReceivedDeletedBlockInfo rdbi : receivedAndDeletedBlockArray) {
                if (!pendingIncrementalBR
                        .containsKey(rdbi.getBlock().getBlockId())) {
                  pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
                }
              }
            }
            pendingReceivedRequests = pendingIncrementalBR.size();
          }
        }
      }
      synchronized (incrementalBRLock){
        incrementalBRCounter--;
      }
      return null;
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeBlockImmediatelyInt(ReceivedDeletedBlockInfo bInfo, boolean now) {
    synchronized (pendingIncrementalBR) {
      pendingIncrementalBR.put(bInfo.getBlock().getBlockId(), bInfo);
      pendingReceivedRequests++;
      if(now) {
        pendingIncrementalBR.notifyAll();
      }
    }
  }

  void notifyNamenodeDeletedBlockInt(ReceivedDeletedBlockInfo bInfo) {
    synchronized (pendingIncrementalBR) {
      pendingIncrementalBR.put(bInfo.getBlock().getBlockId(), bInfo);
    }
  }
  
  /**
   * Report the list blocks to the Namenode
   *
   * @throws IOException
   */
  DatanodeCommand blockReport() throws IOException {
    // send block report if timer has expired.
    DatanodeCommand cmd = null;
    long startTime = now();
    if (startTime - lastBlockReport > dnConf.blockReportInterval) {

      // Flush any block information that precedes the block report. Otherwise
      // we have a chance that we will miss the delHint information
      // or we will report an RBW replica after the BlockReport already reports
      // a FINALIZED one.
      reportReceivedDeletedBlocks();

      // Create block report
      long brCreateStartTime = now();
      BlockReport bReport =
          dn.getFSDataset().getBlockReport(getBlockPoolId());

      // Send block report
      long brSendStartTime = now();
      StorageBlockReport[] report = {new StorageBlockReport(
          new DatanodeStorage(bpRegistration.getStorageID()),
          bReport)};

      ActiveNode an = nextNNForBlkReport(bReport.getNumBlocks());
      if (an != null) {
        blkReportHander = getAnActor(an.getRpcServerAddressForDatanodes());
        if (blkReportHander == null || !blkReportHander.isInitialized()) {
          return null; //no one is ready to handle the request, return now without changing the values of lastBlockReport. it will be retried in next cycle
        }
      } else {
        LOG.warn("Unable to send block report. Current namenodes are: "+ Arrays.toString(nnList.toArray()));
        return null;
      }

      cmd =
          blkReportHander.blockReport(bpRegistration, getBlockPoolId(), report);

      // Log the block report processing stats from Datanode perspective
      long brSendCost = now() - brSendStartTime;
      long brCreateCost = brSendStartTime - brCreateStartTime;
      dn.getMetrics().addBlockReport(brSendCost);
      LOG.info(
          "BlockReport of " + bReport.getNumBlocks() + " blocks took " +
              brCreateCost + " msec to generate and " + brSendCost +
              " msecs for RPC and NN processing");

      // If we have sent the first block report, then wait a random
      // time before we start the periodic block reports.
      if (resetBlockReportTime) {
        lastBlockReport = startTime -
            DFSUtil.getRandom().nextInt((int) (dnConf.blockReportInterval));
        resetBlockReportTime = false;
      } else {
        /* say the last block report was at 8:20:14. The current report
         * should have started around 9:20:14 (default 1 hour interval).
         * If current time is :
         *   1) normal like 9:20:18, next report should be at 10:20:14
         *   2) unexpected like 11:35:43, next report should be at 12:20:14
         */
        lastBlockReport +=
            (now() - lastBlockReport) / dnConf.blockReportInterval *
                dnConf.blockReportInterval;
      }
      LOG.info("sent block report, processed command:" + cmd);
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
    synchronized (pendingIncrementalBR) {
      lastBlockReport = 0;
      pendingIncrementalBR.notifyAll();
      while (lastBlockReport == 0) {
        try {
          pendingIncrementalBR.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  void triggerDeletionReportForTestsInt() {
    synchronized (pendingIncrementalBR) {
      lastDeletedReport = 0;
      pendingIncrementalBR.notifyAll();

      while (lastDeletedReport == 0) {
        try {
          pendingIncrementalBR.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  synchronized void updateNNList(SortedActiveNodeList list) throws IOException {
    long time  = System.currentTimeMillis();
    ArrayList<InetSocketAddress> nnAddresses = new ArrayList<>();
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
    LOG.debug("After Checking DN connection with NNs. "+Arrays.toString(nnList.toArray())+" Update Time: "+(System.currentTimeMillis()-time));
  }


  long lastupdate = System.currentTimeMillis();
  synchronized boolean canUpdateNNList(InetSocketAddress address) {
    if (nnList == null || nnList.size() == 0) {
      return true;
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
  
  private void reportBadBlocksWithRetry(final ExtendedBlock block)
      throws IOException {
    doActorActionWithRetry(new ActorActionHandler() {
      @Override
      public Object doAction(BPServiceActor actor) throws IOException {
        actor.reportBadBlocks(block);
        return null;
      }
    });
  }

  private void blockReceivedAndDeletedWithRetry(
      final StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
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
        if (ExceptionCheck.isLocalConnectException(e)) {
          //black list the namenode 
          //so that it is not used again
          LOG.debug("RPC faild. NN used was " + actor.getNNSocketAddress() +
              ", retries left (" + (MAX_RPC_RETRIES - (i)) + ")  Exception " +
              e);
          blackListNN.add(actor.getNNSocketAddress());
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
      } catch (BRLoadBalancingException e) {
        LOG.warn(e);
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
  
  private synchronized  void forwardRRIndex() {
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
}
