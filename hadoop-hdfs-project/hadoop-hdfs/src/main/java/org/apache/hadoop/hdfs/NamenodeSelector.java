/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.collect.Sets;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class keep track of all namenodes in the cluster. At the start, It
 * connects to the list of namenodes in the configuation file. Periodically, it
 * asynchronously asks one of the current namenodes what is the latest
 * up-to-date list of active namenodes.
 */
public class NamenodeSelector extends Thread {

  /**
   * Policy for selection next namenode to be used by the client. Current
   * supported policies are ROUND_ROBIN and RANDOM. RANDOM is the default
   * policy used if no policy set in the configuation file.
   */
  enum NNSelectionPolicy {

    RANDOM("RANDOM"),
    RANDOM_STICKY("RANDOM_STICKY"),
    ROUND_ROBIN("ROUND_ROBIN");
    private String description = null;

    private NNSelectionPolicy(String arg) {
      this.description = arg;
    }

    @Override
    public String toString() {
      return description;
    }
  }
  
  public static class NamenodeHandle {

    final private ClientProtocol namenodeRPCHandle;
    final private ActiveNode namenode;

    public NamenodeHandle(ClientProtocol proto, ActiveNode an) {
      this.namenode = an;
      this.namenodeRPCHandle = proto;
    }

    public ClientProtocol getRPCHandle() {
      return this.namenodeRPCHandle;
    }

    public ActiveNode getNamenode() {
      return this.namenode;
    }

    @Override
    public String toString() {
      return "[RPC handle connected to " + namenode.getInetSocketAddress() +
          "] ";
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NamenodeSelector.NamenodeHandle)) {
        return false;
      } else {
        NamenodeSelector.NamenodeHandle that =
            (NamenodeSelector.NamenodeHandle) obj;
        boolean res = this.namenode.equals(that.getNamenode());
        return res;
      }
    }
  }

  
  /* List of name nodes */
  private List<NamenodeSelector.NamenodeHandle> nnList =
      new CopyOnWriteArrayList<NamenodeSelector.NamenodeHandle>();
  private List<NamenodeSelector.NamenodeHandle> blackListedNamenodes =
      new CopyOnWriteArrayList<NamenodeSelector.NamenodeHandle>();
  private static Log LOG = LogFactory.getLog(NamenodeSelector.class);
  private final URI defaultUri;
  private final NamenodeSelector.NNSelectionPolicy policy;
  private NamenodeSelector.NamenodeHandle stickyHandle = null; //only used if
  // RANDOM_STICKY policy is used
  protected final Configuration conf;
  private boolean periodicNNListUpdate = true;
  private final Object wiatObjectForUpdate = new Object();
  private final int namenodeListUpdateTimePeriod;
  Random rand = new Random((UUID.randomUUID()).hashCode());


  //only for testing
  NamenodeSelector(Configuration conf, ClientProtocol namenode)
      throws IOException {
    this.defaultUri = null;
    ActiveNode dummyActiveNamenode =
        new ActiveNodePBImpl(1, "localhost", "127.0.0.1", 9999,
            "0.0.0.0:50070");
    this.nnList.add(
        new NamenodeSelector.NamenodeHandle(namenode, dummyActiveNamenode));
    this.conf = conf;
    this.policy = NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN;
    this.namenodeListUpdateTimePeriod = -1;
  }

  public NamenodeSelector(Configuration conf, URI defaultUri) throws IOException {
    this.defaultUri = defaultUri;
    this.conf = conf;

    namenodeListUpdateTimePeriod =
        conf.getInt(DFSConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_IN_MS_KEY,
            DFSConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_IN_MS_DEFAULT);

    // Getting appropriate policy
    // supported policies are 'RANDOM' and 'ROUND_ROBIN'
    String policyName =
        conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY,
            DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_DEFAULT);
    if (policyName.equals(NamenodeSelector.NNSelectionPolicy.RANDOM.toString())){
      policy = NamenodeSelector.NNSelectionPolicy.RANDOM;
    } else if (policyName.equals(NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN.toString())) {
      policy = NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN;
    }else if (policyName.equals(NamenodeSelector.NNSelectionPolicy.RANDOM_STICKY.toString())) {
      policy = NamenodeSelector.NNSelectionPolicy.RANDOM_STICKY;
    } else {
      policy = NamenodeSelector.NNSelectionPolicy.RANDOM_STICKY;
    }
    LOG.debug("Client's namenode selection policy is " + policy);

    //get the list of Namenodes
    createNamenodeClientsFromConfiguration();

    //start periodic Namenode list update thread.
    start();
  }

  @Override
  public void run() {
    while (periodicNNListUpdate) {
      try {
        //first sleep and then update
        synchronized (wiatObjectForUpdate) {
          wiatObjectForUpdate.wait(namenodeListUpdateTimePeriod);
        }
        if (periodicNNListUpdate) {
          periodicNamenodeClientsUpdate();
        }
      } catch (Exception ex) {
        LOG.warn(ex);
      }
    }
    LOG.debug("Shuting down client");
  }

  private void asyncNNListUpdate() {
    synchronized (wiatObjectForUpdate) {
      wiatObjectForUpdate.notify();
    }
  }

  /**
   * Stop the periodic update and close all the conncetions to the namenodes
   */
  public synchronized void close() {
    stopPeriodicUpdates();

    //close all clients
    for (NamenodeSelector.NamenodeHandle namenode : nnList) {
      ClientProtocol rpc = namenode.getRPCHandle();
      nnList.remove(namenode);
      RPC.stopProxy(rpc);
    }
  }

  public void stopPeriodicUpdates() {
    periodicNNListUpdate = false;
    synchronized (wiatObjectForUpdate) {
      wiatObjectForUpdate.notify();
    }
  }

  /**
   * Get all namenodes in the cluster
   * @return list of all namenodes
   * @throws IOException
   */
  public List<NamenodeSelector.NamenodeHandle> getAllNameNode()
      throws IOException {
    if (nnList == null || nnList.isEmpty()) {
      asyncNNListUpdate();
      throw new NoAliveNamenodeException();
    }
    return nnList;
  }

  /**
   * Get the leader namenode
   * @return NamenodeHandle leader
   * @throws IOException
   */
  public NamenodeHandle getLeadingNameNode() throws IOException {
    NamenodeHandle leaderHandle = null; // leader is one with the least id
    for(NamenodeHandle handle : getAllNameNode()){
      if(leaderHandle == null){
        leaderHandle = handle;
      }
      
      if(leaderHandle.getNamenode().getId() > handle.getNamenode().getId()){
       leaderHandle = handle; 
      }
    }
    return leaderHandle;
  }

  /**
   * Gets the appropriate namenode for a read/write operation
   */
  int rrIndex = 0;

  /**
   * Get the next namenode to be used for the operation. It returns the
   * namenode based on the policy defined in the
   * configuration. @see NNSelectionPolicy.
   * @return a namenode
   * @throws IOException
   */
  public NamenodeSelector.NamenodeHandle getNextNamenode() throws IOException {


    if (nnList == null || nnList.isEmpty()) {
      asyncNNListUpdate();
      throw new NoAliveNamenodeException("No NameNode is active");
    }

    NamenodeSelector.NamenodeHandle handle = getNextNNBasedOnPolicy();
    if (handle == null || handle.getRPCHandle() == null) {
      //update the list right now
      asyncNNListUpdate();
      throw new NoAliveNamenodeException(
          " Started an asynchronous update of the namenode list. ");
    }
    return handle;
  }

  private synchronized NamenodeSelector.NamenodeHandle getNextNNBasedOnPolicy() {
    if (policy == NamenodeSelector.NNSelectionPolicy.RANDOM) {
      return getRandomNNInternal();
    } else if (policy == NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN) {
      for (int i = 0; i < nnList.size() + 1; i++) {
        rrIndex = (++rrIndex) % nnList.size();
        NamenodeSelector.NamenodeHandle handle = nnList.get(rrIndex);
        if (!this.blackListedNamenodes.contains(handle)) {
          LOG.debug("ROUND_ROBIN returning "+handle);
          return handle;
        }
      }
      return null;
    }  else if( policy == NamenodeSelector.NNSelectionPolicy.RANDOM_STICKY) {
      // stick to a random NN untill the NN dies
      //stickyHandle
      if(stickyHandle != null && nnList.contains(stickyHandle) &&
          !blackListedNamenodes.contains(stickyHandle)){
        LOG.debug("RANDOM_STICKY returning "+stickyHandle);
        return stickyHandle;
      } else { // stick to some other random alive NN
        stickyHandle = getRandomNNInternal();
        return stickyHandle;
      }
    } else {
      throw new UnsupportedOperationException(
          "Namenode selection policy is not supported. Selected policy is " +
              policy);
    }
  }
  
  // synchronize by the calling method
  private NamenodeSelector.NamenodeHandle getRandomNNInternal(){
    for (int i = 0; i < 10; i++) {
      int index = rand.nextInt(nnList.size());
      NamenodeSelector.NamenodeHandle handle = nnList.get(index);
      if (!this.blackListedNamenodes.contains(handle)) {
        LOG.debug("RANDOM returning "+handle);
        return handle;
      }
    }
    return null;
  }

  String printNamenodes() {
    String nns = "Client is connected to namenodes: ";
    for (NamenodeSelector.NamenodeHandle namenode : nnList) {
      nns += namenode + ", ";
    }

    nns += " Black Listed Nodes are ";
    for (NamenodeSelector.NamenodeHandle namenode : this.blackListedNamenodes) {
      nns += namenode + ", ";
    }
    return nns;
  }

  public int getTotalConnectedNamenodes() {
    return nnList.size();
  }

  /**
   * try connecting to the default uri and get the list of NN from there if it
   * fails then read the list of NNs from the config file and connect to them
   * for list of Namenodes in the syste.
   */
  private void createNamenodeClientsFromConfiguration() throws IOException {
    SortedActiveNodeList anl = null;
    ClientProtocol handle = null;
    if (defaultUri != null) {
      try {
        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
            NameNodeProxies.createProxy(conf, defaultUri, ClientProtocol.class);
        handle = proxyInfo.getProxy();
        if (handle != null) {
          anl = handle.getActiveNamenodesForClient();
          RPC.stopProxy(handle);
        }
      } catch (Exception e) {
        LOG.warn("Failed to get list of NN from default NN. Default NN was " +
            defaultUri);
        RPC.stopProxy(handle);
      }
    }

    if (anl ==
        null) { // default failed, now try the list of NNs from the config file
      List<URI> namenodes = DFSUtil.getNameNodesRPCAddressesAsURIs(conf);
      LOG.debug("Trying to the list of NN from the config file  " + namenodes);
      for (URI nn : namenodes) {
        try {
          LOG.debug("Trying to connect to  " + nn);
          NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
              NameNodeProxies.createProxy(conf, nn, ClientProtocol.class);
          handle = proxyInfo.getProxy();
          if (handle != null) {
            anl = handle.getActiveNamenodesForClient();
            RPC.stopProxy(handle);
          }
          if (anl != null && !anl.getActiveNodes().isEmpty()) {

            break; // we got the list
          }
        } catch (Exception e) {
          LOG.error(e);
          if (handle != null) {
            RPC.stopProxy(handle);
          }
        }
      }
    }
    if (anl != null) {
      LOG.debug("Refreshing the Namenode handles");
      refreshNamenodeList(anl);
    } else {
      LOG.warn("No new namenodes were found");
    }
  }

  /**
   * we already have a list of NNs in 'clients' try contacting these namenodes
   * to get a fresh list of namenodes if all of the namenodes in the 'clients'
   * map fail then call the 'createDFSClientsForFirstTime' function. with will
   * try to connect to defaults namenode provided at the initialization phase.
   */
  private synchronized void periodicNamenodeClientsUpdate() throws IOException {
    SortedActiveNodeList anl = null;
    LOG.debug("Fetching new list of namenodes");
    if (!nnList.isEmpty()) {
      for (NamenodeSelector.NamenodeHandle namenode : nnList) { //TODO dont try with black listed nodes
        try {
          ClientProtocol handle = namenode.getRPCHandle();
          anl = handle.getActiveNamenodesForClient();
          if (anl == null || anl.size() == 0) {
            anl = null;
            continue;
          } else {
            // we get a fresh list of anl
            refreshNamenodeList(anl);
            return;
          }
        } catch (IOException e) {
          continue;
        }
      }
    }

    if (anl == null) { // try contacting default NNs
      createNamenodeClientsFromConfiguration();
    }

  }


  private synchronized void refreshNamenodeList(SortedActiveNodeList anl) {
    if (anl == null) {
      return;
    }
    //NOTE should not restart a valid client

    //find out which client to start and stop
    //make sets objects of old and new lists
    Set<InetSocketAddress> oldClients = Sets.newHashSet();
    for (NamenodeSelector.NamenodeHandle namenode : nnList) {
      ActiveNode ann = namenode.getNamenode();
      oldClients.add(ann.getInetSocketAddress());
    }

    //new list
    Set<InetSocketAddress> updatedClients = Sets.newHashSet();
    for (ActiveNode ann : anl.getActiveNodes()) {
      updatedClients.add(ann.getInetSocketAddress());
    }


    Sets.SetView<InetSocketAddress> deadNNs =
        Sets.difference(oldClients, updatedClients);
    Sets.SetView<InetSocketAddress> newNNs =
        Sets.difference(updatedClients, oldClients);

    // stop the dead threads
    if (deadNNs.size() != 0) {
      for (InetSocketAddress deadNNAddr : deadNNs) {
        removeDFSClient(deadNNAddr);
      }
    }

    // start threads for new NNs
    if (newNNs.size() != 0) {
      for (InetSocketAddress newNNAddr : newNNs) {
        addDFSClient(newNNAddr, anl.getActiveNode(newNNAddr));
      }
    }


    LOG.debug("nnList Size:"+nnList.size()+"  Handles: " + Arrays.toString(nnList.toArray()));
    //clear black listed nodes
    this.blackListedNamenodes.clear();
  }

  //thse are synchronized using external methods
  private void removeDFSClient(InetSocketAddress address) {
    NamenodeSelector.NamenodeHandle handle = getNamenodeHandle(address);
    if (handle != null) {
      if (nnList.remove(handle)) {
        RPC.stopProxy(handle.getRPCHandle());
      } else {
        LOG.warn("Failed to Remove RPC proxy for " + address);
      }
    }
  }
  //thse are synchronized using external methods

  private void addDFSClient(InetSocketAddress address, ActiveNode ann) {
    if (address == null || ann == null) {
      LOG.warn("Unable to add proxy for namenode. ");
      return;
    }
    try {
      URI uri = NameNode.getUri(address);
      NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
          NameNodeProxies.createProxy(conf, uri, ClientProtocol.class);
      ClientProtocol handle = proxyInfo.getProxy();
      nnList.add(new NamenodeSelector.NamenodeHandle(handle, ann));
    } catch (IOException e) {
      LOG.warn("Unable to Start RPC proxy for " + address);
    }
  }

  private boolean pingNamenode(ClientProtocol namenode) {
    try {
      namenode.ping();
      return true;
    } catch (IOException ex) {
      return false;
    }
  }

  protected NamenodeSelector.NamenodeHandle getNamenodeHandle(
      InetSocketAddress address) {
    for (NamenodeSelector.NamenodeHandle handle : nnList) {
      if (handle.getNamenode().getInetSocketAddress().equals(address)) {
        return handle;
      }
    }
    return null;
  }

  /**
   * Black list a namenode (handle). The black list status will be revoked
   * when the @link{NamenodeSelector} gets a new updated list of the
   * namenodes. All dead namenodes will be removed by next update round.
   * @param handle
   *      the namenode to black list
   */
  public void blackListNamenode(NamenodeSelector.NamenodeHandle handle) {
    if (!this.blackListedNamenodes.contains(handle)) {
      this.blackListedNamenodes.add(handle);
    }

    if(policy == NamenodeSelector.NNSelectionPolicy.RANDOM_STICKY &&
            stickyHandle != null && stickyHandle == handle  ){
      stickyHandle = null;
    }

    //if a bad namenode is detected then update the list of Namenodes in the system
    synchronized (wiatObjectForUpdate) {
      wiatObjectForUpdate.notify();
    }
  }

  public int getNameNodesCount() throws IOException {
//    periodicNamenodeClientsUpdate();
    return nnList.size();
  }
}

