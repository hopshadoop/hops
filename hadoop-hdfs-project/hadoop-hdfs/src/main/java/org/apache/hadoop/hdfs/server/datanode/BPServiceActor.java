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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Collection;

import static org.apache.hadoop.util.Time.now;

/**
 * A thread per active or standby namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 */
@InterfaceAudience.Private
class BPServiceActor implements Runnable {
  
  static final Log LOG = DataNode.LOG;
  final InetSocketAddress nnAddr;
  BPOfferService bpos;
  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;
  private volatile long lastHeartbeat = 0;
  private volatile boolean initialized = false;
  private volatile boolean shouldServiceRun = true;
  private final DataNode dn;
  private final DNConf dnConf;

  private DatanodeRegistration bpRegistration;
  
  private final Object waitForHeartBeats = new Object();
  
  BPServiceActor(InetSocketAddress nnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
  }

  /**
   * returns true if BP thread has completed initialization of storage
   * and has registered with the corresponding namenode
   *
   * @return true if initialized
   */
  boolean isInitialized() {
    return initialized;
  }
  
  boolean isAlive() {
    return shouldServiceRun && bpThread.isAlive();
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }
  
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   *
   * @return the NamespaceInfo
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        nsInfo = bpNamenode.versionRequest();
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch (IOException e) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nnVersion = nsInfo.getSoftwareVersion();
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive =
          new IncorrectVersionException(minimumNameNodeVersion, nnVersion,
              "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }

    if (HdfsConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
      LOG.warn("DataNode and NameNode layout versions must be the same." +
          " Expected: " + HdfsConstants.LAYOUT_VERSION +
          " actual " + nsInfo.getLayoutVersion());
      throw new IncorrectVersionException(nsInfo.getLayoutVersion(),
          "namenode");
    }
  }

  private void connectToNNAndHandshake() throws IOException {
    // get NN proxy
    
    bpNamenode = dn.connectToNN(nnAddr);
    // First phase of the handshake with NN - get the namespace
    // info.
    NamespaceInfo nsInfo = retrieveNamespaceInfo();
    
    // Verify that this matches the other NN in this HA pair.
    // This also initializes our block pool in the DN if we are
    // the first NN connection for this BP.
    bpos.verifyAndSetNamespaceInfo(nsInfo);
    
    // Second phase of the handshake with the NN.
    register();
  }
  
  void reportBadBlocks(ExtendedBlock block) throws IOException {
    if (bpRegistration == null) {
      return;
    }
    DatanodeInfo[] dnArr = {new DatanodeInfo(bpRegistration)};
    LocatedBlock[] blocks = {new LocatedBlock(block, dnArr)};
    
    try {
      bpNamenode.reportBadBlocks(blocks);
    } catch (IOException e) {
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block " + block + " to namenode : " +
          " Exception", e);
      //HOP we will retry by sending it to another nn. it could be because of NN failue
      throw e;
    }
  }
  
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    lastHeartbeat = 0;
    synchronized (waitForHeartBeats) {
      waitForHeartBeats.notifyAll();
    }
  }
  
  HeartbeatResponse sendHeartBeat() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat from service actor: " + this);
    }
    // reports number of failed volumes
    StorageReport[] report =
        {new StorageReport(bpRegistration.getStorageID(), false,
            dn.getFSDataset().getCapacity(), dn.getFSDataset().getDfsUsed(),
            dn.getFSDataset().getRemaining(),
            dn.getFSDataset().getBlockPoolUsed(bpos.getBlockPoolId()))};
    return bpNamenode
        .sendHeartbeat(bpRegistration, report, dn.getXmitsInProgress(),
            dn.getXceiverCount(), dn.getFSDataset().getNumFailedVolumes());
  }
  
  //This must be called only by BPOfferService
  void start() {
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }
  
  private String formatThreadName() {
    Collection<URI> dataDirs = DataNode.getStorageDirs(dn.getConf());
    return "DataNode: [" +
        StringUtils.uriToString(dataDirs.toArray(new URI[0])) + "] " +
        " heartbeating to " + nnAddr;
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    shouldServiceRun = false;
    if (bpThread != null) {
      bpThread.interrupt();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) {
    }
  }
  
  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {
    
    shouldServiceRun = false;
    IOUtils.cleanup(LOG, bpNamenode);

    bpos.shutdownActor(this);
    
    
  }

  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using DELETEREPORT_INTERVAL of " +
        dnConf.deleteReportInterval + " msec " + " BLOCKREPORT_INTERVAL of " +
        dnConf.blockReportInterval + "msec" + " Initial delay: " +
        dnConf.initialBlockReportDelay + "msec" + "; heartBeatInterval=" +
        dnConf.heartBeatInterval);


    bpos.startWhirlingSufiThread();

    
    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        long startTime = now();

        //
        // Every so often, send heartbeat or block-report
        //
        if (startTime - lastHeartbeat > dnConf.heartBeatInterval) {


          refreshNNConnection(); //[S] is the frequency of this operation high? only one actor s  

          
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          lastHeartbeat = startTime;
          if (!dn.areHeartbeatsDisabledForTests()) {
            HeartbeatResponse resp = sendHeartBeat();
            assert resp != null;
            dn.getMetrics().addHeartbeat(now() - startTime);

            long startProcessCommands = now();
            if (!processCommand(resp.getCommands())) {
              continue;
            }
            long endProcessCommands = now();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands) +
                  "ms to process " + resp.getCommands().length +
                  " commands from NN");
            }
          }
        }
        
        long waitTime =
            Math.abs(dnConf.heartBeatInterval - (Time.now() - startTime));
        if (waitTime > dnConf.heartBeatInterval) {
          // above code took longer than dnConf.heartBeatInterval to execute 
          // set wait time to 1 ms to send a new HB immediately
          waitTime = 1;
        }
        synchronized (waitForHeartBeats) {
          waitForHeartBeats.wait(waitTime);
        }
        
      } catch (RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn(this + " is shutting down", re);
          shouldServiceRun = false;
          return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (InterruptedException e) {
        LOG.warn("OfferService interrupted", e);
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
      }
    } // while (shouldRun())
  } // offerService

  /**
   * Register one bp with the corresponding NameNode
   * <p/>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and
   * 2) to receive a registrationID
   * <p/>
   * issued by the namenode to recognize registered datanodes.
   *
   * @throws IOException
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   */
  void register() throws IOException {
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info
    bpRegistration = bpos.createRegistration();

    while (shouldRun()) {
      try {
        // Use returned registration from namenode with updated fields
        bpRegistration = bpNamenode.registerDatanode(bpRegistration);
        break;
      } catch (SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    
    LOG.info("Block pool " + this + " successfully registered with NN");
    bpos.registrationSucceeded(this, bpRegistration);

    // random short delay - helps scatter the BR from all DNs
    bpos.scheduleBlockReport(dnConf.initialBlockReportDelay);
  }


  private void sleepAndLogInterrupts(int millis, String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this + " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   * <p/>
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      // init stuff
      try {
        // setup storage
        connectToNNAndHandshake();
      } catch (IOException ioe) {
        // Initial handshake, storage recovery or registration failed
        // End BPOfferService thread
        LOG.fatal("Initialization failed for block pool " + this, ioe);
        cleanUp(); //cean up and return. if the nn comes back online then it will be restarted
        return;
      }

      initialized = true; // bp is initialized;
      
      while (shouldRun()) {
        try {
          offerService();
        } catch (Exception ex) {
          LOG.error("Exception in BPOfferService for " + this, ex);
          cleanUp(); //cean up and return. if the nn comes back online then it will be restarted
        }
      }
    } catch (Throwable ex) {
      LOG.warn("Unexpected exception in block pool " + this, ex);
    } finally {
      LOG.warn("Ending block pool service for: " + this);
      cleanUp(); //cean up and return. if the nn comes back online then it will be restarted
    }
  }

  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   *
   * @param cmds
   *     an array of datanode commands
   * @return true if further processing may be required or false otherwise.
   */
  boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (bpos.processCommandFromActor(cmd, this) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }

  void trySendErrorReport(int errCode, String errMsg) {
    try {
      bpNamenode.errorReport(bpRegistration, errCode, errMsg);
    } catch (IOException e) {
      LOG.warn("Error reporting an error to NameNode " + nnAddr, e);
    }
  }

  /**
   * Report a bad block from another DN in this cluster.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block)
      throws IOException {
    LocatedBlock lb = new LocatedBlock(block, new DatanodeInfo[]{dnInfo});
    bpNamenode.reportBadBlocks(new LocatedBlock[]{lb});
  }

  void reRegister() throws IOException {
    if (shouldRun()) {
      // re-retrieve namespace info to make sure that, if the NN
      // was restarted, we still match its version (HDFS-2120)
      retrieveNamespaceInfo();
      // and re-register
      register();
    }
  }


  @Override
  public boolean equals(Object obj) {
    //Two actors are same if they are connected to save NN
    BPServiceActor that = (BPServiceActor) obj;
    if (this.getNNSocketAddress().equals(that.getNNSocketAddress())) {
      return true;
    }
    return false;
  }

  /**
   * get the list of active namenodes in the system. It connects to new
   * namenodes and stops the threads connected to dead namenodes
   */
  private void refreshNNConnection() throws IOException {
    if (!bpos.canUpdateNNList(nnAddr)) {
      return;
    }

    SortedActiveNodeList list = this.bpNamenode.getActiveNamenodes();
    bpos.updateNNList(list);

  }

  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
    if (bpNamenode != null) {
      bpNamenode.blockReceivedAndDeleted(registration, poolId,
          receivedAndDeletedBlocks);
    }
  }

  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, StorageBlockReport[] reports) throws IOException {
    return bpNamenode.blockReport(registration, poolId, reports);
  }

  public ActiveNode nextNNForBlkReport(long noOfBlks) throws IOException {
    if (bpNamenode != null) {
      ActiveNode an = bpNamenode.getNextNamenodeToSendBlockReport(noOfBlks);
      return an;
    } else {
      return null;
    }
  }

  public byte[] getSmallFileDataFromNN(int id)throws IOException {
    if (bpNamenode != null) {
      return bpNamenode.getSmallFileData(id);
    } else {
      return null;
    }
  }
}
