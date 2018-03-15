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
import com.google.common.net.InetAddresses;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.StorageMap;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static io.hops.transaction.lock.LockFactory.BLK;
import org.apache.hadoop.net.NetUtils;
import static org.apache.hadoop.util.Time.now;

/**
 * Manage datanodes, include decommission and other activities.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeManager {
  static final Log LOG = LogFactory.getLog(DatanodeManager.class);

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final HeartbeatManager heartbeatManager;
  private Daemon decommissionthread = null;

  /**
   * Maps datanode uuid's to the DatanodeDescriptor
   * <p/>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by
   * uuid. In order to keep the storage map consistent it tracks
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul>
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * <p/>
   */
  private final NavigableMap<String, DatanodeDescriptor> datanodeMap =
      new TreeMap<>();

  /**
   * Cluster network topology
   */
  private final NetworkTopology networktopology;

  /**
   * Host names to datanode descriptors mapping.
   */
  private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();

  private final DNSToSwitchMapping dnsToSwitchMapping;
  private final boolean rejectUnresolvedTopologyDN;

  /**
   * Read include/exclude files
   */
  private final HostsFileReader hostsReader;

  /**
   * The period to wait for datanode heartbeat.
   */
  private final long heartbeatExpireInterval;
  /**
   * Ask Datanode only up to this many blocks to delete.
   */
  final int blockInvalidateLimit;

  /**
   * The interval for judging stale DataNodes for read/write
   */
  private final long staleInterval;
  
  /**
   * Whether or not to avoid using stale DataNodes for reading
   */
  private final boolean avoidStaleDataNodesForRead;

  /**
   * Whether or not to avoid using stale DataNodes for writing.
   * Note that, even if this is configured, the policy may be
   * temporarily disabled when a high percentage of the nodes
   * are marked as stale.
   */
  private final boolean avoidStaleDataNodesForWrite;

  /**
   * When the ratio of stale datanodes reaches this number, stop avoiding
   * writing to stale datanodes, i.e., continue using stale nodes for writing.
   */
  private final float ratioUseStaleDataNodesForWrite;
  
  /**
   * The number of stale DataNodes
   */
  private volatile int numStaleNodes;
  
  /**
   * Whether or not this cluster has ever consisted of more than 1 rack,
   * according to the NetworkTopology.
   */
  private boolean hasClusterEverBeenMultiRack = false;

  private final boolean checkIpHostnameInRegistration;
  
  private final StorageMap storageMap = new StorageMap();
  
  DatanodeManager(final BlockManager blockManager, final Namesystem namesystem,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.blockManager = blockManager;

    this.networktopology = NetworkTopology.getInstance(conf);

    this.heartbeatManager =
        new HeartbeatManager(namesystem, blockManager, conf);

    this.hostsReader =
        new HostsFileReader(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
            conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);
    
    this.rejectUnresolvedTopologyDN = conf.getBoolean(
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY,
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT);
    
    // If the dns to switch mapping supports cache, resolve network
    // locations of those hosts in the include list and store the mapping
    // in the cache; so future calls to resolve will be fast.
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<>(hostsReader.getHosts()));
    }
    
    final long heartbeatIntervalSeconds =
        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    final int heartbeatRecheckInterval =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
    this.heartbeatExpireInterval =
        2 * heartbeatRecheckInterval + 10 * 1000 * heartbeatIntervalSeconds;
    final int blockInvalidateLimit =
        Math.max(20 * (int) (heartbeatIntervalSeconds),
            DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
    this.blockInvalidateLimit =
        conf.getInt(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY,
            blockInvalidateLimit);
    LOG.info(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY + "=" +
        this.blockInvalidateLimit);

    this.checkIpHostnameInRegistration = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY,
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY
        + "=" + checkIpHostnameInRegistration);

    this.avoidStaleDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT);
    this.avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
    this.staleInterval =
        getStaleIntervalFromConf(conf, heartbeatExpireInterval);
    this.ratioUseStaleDataNodesForWrite = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
    Preconditions.checkArgument((ratioUseStaleDataNodesForWrite > 0 &&
            ratioUseStaleDataNodesForWrite <= 1.0f),
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY +
            " = '" + ratioUseStaleDataNodesForWrite + "' is invalid. " +
            "It should be a positive non-zero float value, not greater than 1.0f.");
  }
  
  private static long getStaleIntervalFromConf(Configuration conf,
      long heartbeatExpireInterval) {
    long staleInterval =
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    Preconditions.checkArgument(staleInterval > 0,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY +
            " = '" + staleInterval + "' is invalid. " +
            "It should be a positive non-zero value.");
    
    final long heartbeatIntervalSeconds =
        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    // The stale interval value cannot be smaller than 
    // 3 times of heartbeat interval 
    final long minStaleInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT) *
        heartbeatIntervalSeconds * 1000;
    if (staleInterval < minStaleInterval) {
      LOG.warn(
          "The given interval for marking stale datanode = " + staleInterval +
              ", which is less than " +
              DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT +
              " heartbeat intervals. This may cause too frequent changes of " +
              "stale states of DataNodes since a heartbeat msg may be missing " +
              "due to temporary short-term failures. Reset stale interval to " +
              minStaleInterval + ".");
      staleInterval = minStaleInterval;
    }
    if (staleInterval > heartbeatExpireInterval) {
      LOG.warn(
          "The given interval for marking stale datanode = " + staleInterval +
              ", which is larger than heartbeat expire interval " +
              heartbeatExpireInterval + ".");
    }
    return staleInterval;
  }
  
  void activate(final Configuration conf) {
    final DecommissionManager dm =
        new DecommissionManager(namesystem, blockManager);
    this.decommissionthread = new Daemon(dm.new Monitor(
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT),
        conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT)));
    decommissionthread.start();

    heartbeatManager.activate(conf);
  }

  void close() {
    if (decommissionthread != null) {
      decommissionthread.interrupt();
      try {
        decommissionthread.join(3000);
      } catch (InterruptedException e) {
      }
    }
    heartbeatManager.close();
  }

  /**
   * @return the network topology.
   */
  public NetworkTopology getNetworkTopology() {
    return networktopology;
  }

  /**
   * @return the heartbeat manager.
   */
  HeartbeatManager getHeartbeatManager() {
    return heartbeatManager;
  }

  /**
   * @return the datanode statistics.
   */
  public DatanodeStatistics getDatanodeStatistics() {
    return heartbeatManager;
  }

  /**
   * Sort the located blocks by the distance to the target host.
   */
  public void sortLocatedBlocks(final String targethost,
      final List<LocatedBlock> locatedblocks) {
    //sort the blocks
    final DatanodeDescriptor client = getDatanodeByHost(targethost);
    
    Comparator<DatanodeInfo> comparator = avoidStaleDataNodesForRead ?
        new DFSUtil.DecomStaleComparator(staleInterval) :
        DFSUtil.DECOM_COMPARATOR;

    for (LocatedBlock b : locatedblocks) {
      networktopology.pseudoSortByDistance(client, b.getLocations());
      // Move decommissioned/stale datanodes to the bottom
      Arrays.sort(b.getLocations(), comparator);
    }
  }
  
  CyclicIteration<String, DatanodeDescriptor> getDatanodeCyclicIteration(
      final String firstkey) {
    return new CyclicIteration<>(datanodeMap,
        firstkey);
  }

  /**
   * @return the datanode descriptor for the host.
   */
  public DatanodeDescriptor getDatanodeByHost(final String host) {
    return host2DatanodeMap.getDatanodeByHost(host);
  }

  /**
   * Get a datanode descriptor given corresponding datanode uuid
   */
  public DatanodeDescriptor getDatanodeByUuid(final String uuid) {
    if (uuid == null) {
      return null;
    }

    return datanodeMap.get(uuid);
  }

  public DatanodeDescriptor getDatanodeBySid(final int sid) {
    DatanodeStorageInfo storage = this.getStorage(sid);

    return (storage == null) ? null : storage.getDatanodeDescriptor();
  }

  /**
   * Get data node by datanode ID.
   *
   * @param nodeID datanode ID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws UnregisteredNodeException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID)
      throws UnregisteredNodeException {
    DatanodeDescriptor node = null;
    if (nodeID != null && nodeID.getDatanodeUuid() != null &&
        !nodeID.getDatanodeUuid().equals("")) {
      node = getDatanodeByUuid(nodeID.getDatanodeUuid());
    }
    if (node == null) {
      return null;
    }
    if (!node.getXferAddr().equals(nodeID.getXferAddr())) {
      final UnregisteredNodeException e =
          new UnregisteredNodeException(nodeID, node);
      NameNode.stateChangeLog
          .fatal("BLOCK* NameSystem.getDatanode: " + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }

  public DatanodeStorageInfo[] getDatanodeStorageInfos(
      DatanodeID[] datanodeID, String[] storageIDs)
      throws UnregisteredNodeException {
    if (datanodeID.length == 0) {
      return null;
    }
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[datanodeID.length];
    for(int i = 0; i < datanodeID.length; i++) {
      final DatanodeDescriptor dd = getDatanode(datanodeID[i]);
      storages[i] = dd.getStorageInfo(storageIDs[i]);
    }
    return storages;
  }

  /**
   * Remove a datanode descriptor.
   *
   * @param nodeInfo
   *     datanode descriptor.
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) throws IOException {
    heartbeatManager.removeDatanode(nodeInfo);
    if (namesystem.isLeader()) {
      NameNode.stateChangeLog.info(
          "DataNode is dead. Removing all replicas for" +
              " datanode " + nodeInfo +
              " StorageID " + nodeInfo.getDatanodeUuid() +
              " index " + nodeInfo.getHostName());
      blockManager.datanodeRemoved(nodeInfo);

    }
    networktopology.remove(nodeInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("removed datanode " + nodeInfo);
    }
    namesystem.checkSafeMode();
  }

  /**
   * Remove a datanode
   *
   * @throws UnregisteredNodeException
   */
  public void removeDatanode(final DatanodeID node
      //Called my NameNodeRpcServer
  ) throws UnregisteredNodeException, IOException {
    final DatanodeDescriptor descriptor = getDatanode(node);
    if (descriptor != null) {
      removeDatanode(descriptor);
    } else {
      NameNode.stateChangeLog
          .warn("BLOCK* removeDatanode: " + node + " does not exist");
    }
  }

  /**
   * Remove a dead datanode.
   */
  void removeDeadDatanode(final DatanodeID nodeID) throws IOException {
    DatanodeDescriptor d;
    boolean removeDatanode = false;
    synchronized (datanodeMap) {

      try {
        d = getDatanode(nodeID);
      } catch (IOException e) {
        d = null;
      }
      if (d != null && isDatanodeDead(d)) {
        NameNode.stateChangeLog
            .info("BLOCK* removeDeadDatanode: lost heartbeat from " + d);
        removeDatanode = true;
      }
    }
    //HOP removeDatanode might take verylong time. taking it out of the synchronized section.
    if (removeDatanode) {
      removeDatanode(d);
    }
  }

  /**
   * Is the datanode dead?
   */
  boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() < (Time.now() - heartbeatExpireInterval));
  }

  /**
   * Add a datanode.
   */
  void addDatanode(final DatanodeDescriptor node) throws IOException {
    // To keep host2DatanodeMap consistent with datanodeMap,
    // remove from host2DatanodeMap the datanodeDescriptor removed
    // from datanodeMap before adding node to host2DatanodeMap.
    synchronized (datanodeMap) {
      host2DatanodeMap.remove(datanodeMap.put(node.getDatanodeUuid(), node));
    }

    host2DatanodeMap.add(node);
    networktopology.add(node);
    checkIfClusterIsNowMultiRack(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".addDatanode: " + "node " + node +
          " is added to datanodeMap.");
    }
  }

  /**
   * Physically remove node from datanodeMap.
   */
  private void wipeDatanode(final DatanodeID node) {
    final String key = node.getDatanodeUuid();
    synchronized (datanodeMap) {
      host2DatanodeMap.remove(datanodeMap.remove(key));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          getClass().getSimpleName() + ".wipeDatanode(" + node + "): storage " +
              key + " is removed from datanodeMap.");
    }
  }

  private boolean inHostsList(DatanodeID node) {
    return checkInList(node, hostsReader.getHosts(), false);
  }
  
  private boolean inExcludedHostsList(DatanodeID node) {
    return checkInList(node, hostsReader.getExcludedHosts(), true);
  }

  /**
   * Remove an already decommissioned data node who is neither in include nor
   * exclude hosts lists from the the list of live or dead nodes.  This is used
   * to not display an already decommssioned data node to the operators.
   * The operation procedure of making a already decommissioned data node not
   * to be displayed is as following:
   * <ol>
   * <li>
   * Host must have been in the include hosts list and the include hosts list
   * must not be empty.
   * </li>
   * <li>
   * Host is decommissioned by remaining in the include hosts list and added
   * into the exclude hosts list. Name node is updated with the new
   * information by issuing dfsadmin -refreshNodes command.
   * </li>
   * <li>
   * Host is removed from both include hosts and exclude hosts lists.  Name
   * node is updated with the new informationby issuing dfsamin -refreshNodes
   * command.
   * <li>
   * </ol>
   *
   * @param nodeList
   *     , array list of live or dead nodes.
   */
  private void removeDecomNodeFromList(
      final List<DatanodeDescriptor> nodeList) {
    // If the include list is empty, any nodes are welcomed and it does not
    // make sense to exclude any nodes from the cluster. Therefore, no remove.
    if (hostsReader.getHosts().isEmpty()) {
      return;
    }
    
    for (Iterator<DatanodeDescriptor> it = nodeList.iterator();
         it.hasNext(); ) {
      DatanodeDescriptor node = it.next();
      if ((!inHostsList(node)) && (!inExcludedHostsList(node)) &&
          node.isDecommissioned()) {
        // Include list is not empty, an existing datanode does not appear
        // in both include or exclude lists and it has been decommissioned.
        // Remove it from the node list.
        it.remove();
      }
    }
  }

  /**
   * Check if the given DatanodeID is in the given (include or exclude) list.
   *
   * @param node
   *     the DatanodeID to check
   * @param hostsList
   *     the list of hosts in the include/exclude file
   * @param isExcludeList
   *     true if this is the exclude list
   * @return true if the node is in the list, false otherwise
   */
  private static boolean checkInList(final DatanodeID node,
      final Set<String> hostsList, final boolean isExcludeList) {
    // if include list is empty, host is in include list
    if ((!isExcludeList) && (hostsList.isEmpty())) {
      return true;
    }
    for (String name : getNodeNamesForHostFiltering(node)) {
      if (hostsList.contains(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Decommission the node if it is in exclude list.
   */
  private void checkDecommissioning(DatanodeDescriptor nodeReg)
      throws IOException {
    // If the registered node is in exclude list, then decommission it
    if (inExcludedHostsList(nodeReg)) {
      startDecommission(nodeReg);
    }
  }

  /**
   * Change, if appropriate, the admin state of a datanode to
   * decommission completed. Return true if decommission is complete.
   */
  boolean checkDecommissionState(DatanodeDescriptor node) throws IOException {
    // Check to see if all blocks in this decommissioned
    // node has reached their target replication factor.
    if (node.isDecommissionInProgress()) {
      if (!blockManager.isReplicationInProgress(node)) {
        node.setDecommissioned();
        LOG.info("Decommission complete for " + node);
      }
    }
    return node.isDecommissioned();
  }

  /**
   * Start decommissioning the specified datanode.
   */
  private void startDecommission(DatanodeDescriptor node) throws IOException {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      for (DatanodeStorageInfo storage : node.getStorageInfos()) {
        LOG.info("Start Decommissioning " + node + " " + storage
            + " with " + storage.numBlocks() + " blocks");
      }
      heartbeatManager.startDecommission(node);
      node.decommissioningStatus.setStartTime(now());
      
      // all the blocks that reside on this node have to be replicated.
      checkDecommissionState(node);
    }
  }

  /**
   * Stop decommissioning the specified datanodes.
   */
  void stopDecommission(DatanodeDescriptor node) throws IOException {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      LOG.info("Stop Decommissioning " + node);
      heartbeatManager.stopDecommission(node);
      // Over-replicated blocks will be detected and processed when 
      // the dead node comes back and send in its full block report.
      if (node.isAlive) {
        blockManager.processOverReplicatedBlocksOnReCommission(node);
      }
    }
  }

  /**
   * Register the given datanode with the namenode. NB: the given
   * registration is mutated and given back to the datanode.
   *
   * @param nodeReg
   *     the datanode registration
   * @throws DisallowedDatanodeException
   *     if the registration request is
   *     denied because the datanode does not match includes/excludes
   */
  public void registerDatanode(DatanodeRegistration nodeReg)
      throws DisallowedDatanodeException, IOException {
    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      String hostname = dnAddress.getHostName();
      String ip = dnAddress.getHostAddress();
      if (checkIpHostnameInRegistration && !isNameResolved(dnAddress)) {
        // Reject registration of unresolved datanode to prevent performance
        // impact of repetitive DNS lookups later.
        final String message = "hostname cannot be resolved (ip="
            + ip + ", hostname=" + hostname + ")";
        LOG.warn("Unresolved datanode registration: " + message);
        throw new DisallowedDatanodeException(nodeReg, message);
      }
      // update node registration with the ip and hostname from rpc request
      nodeReg.setIpAddr(ip);
      nodeReg.setPeerHostName(hostname);
    }

    nodeReg.setExportedKeys(blockManager.getBlockKeys());

    // Checks if the node is not on the hosts list.  If it is not, then
    // it will be disallowed from registering. 
    if (!inHostsList(nodeReg)) {
      throw new DisallowedDatanodeException(nodeReg);
    }

    NameNode.stateChangeLog.info(
        "BLOCK* registerDatanode: from " + nodeReg + " storage " +
            nodeReg.getDatanodeUuid());

    DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getDatanodeUuid());
    DatanodeDescriptor nodeN = host2DatanodeMap
        .getDatanodeByXferAddr(nodeReg.getIpAddr(), nodeReg.getXferPort());

    if (nodeN != null && nodeN != nodeS) {
      NameNode.LOG.info("BLOCK* registerDatanode: " + nodeN);
      // nodeN previously served a different data storage, 
      // which is not served by anybody anymore.
      removeDatanode(nodeN);
      // physically remove node from datanodeMap
      wipeDatanode(nodeN);
      nodeN = null;
    }

    if (nodeS != null) {
      if (nodeN == nodeS) {
        // The same datanode has been just restarted to serve the same data 
        // storage. We do not need to remove old data blocks, the delta will
        // be calculated on the next block report from the datanode
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog
              .debug("BLOCK* registerDatanode: " + "node restarted.");
        }
      } else {
        // nodeS is found
        /* The registering datanode is a replacement node for the existing 
          data storage, which from now on will be served by a new node.
          If this message repeats, both nodes might have same storageID 
          by (insanely rare) random chance. User needs to restart one of the
          nodes with its data cleared (or user can just remove the StorageID
          value in "VERSION" file under the data directory of the datanode,
          but this is might not work if VERSION file format has changed 
       */
        NameNode.stateChangeLog.info("BLOCK* registerDatanode: " + nodeS
            + " is replaced by " + nodeReg + " with the same storageID "
            + nodeReg.getDatanodeUuid());
      }
      // update cluster map
      getNetworkTopology().remove(nodeS);
      nodeS.updateRegInfo(nodeReg);
      nodeS.setDisallowed(false); // Node is in the include list
      
      // resolve network location
      if (this.rejectUnresolvedTopologyDN) {
        nodeS.setNetworkLocation(resolveNetworkLocation(nodeS));
      } else {
        nodeS.setNetworkLocation(
            resolveNetworkLocationWithFallBackToDefaultLocation(nodeS));
      }
      getNetworkTopology().add(nodeS);

      // also treat the registration message as a heartbeat
      heartbeatManager.register(nodeS);
      checkDecommissioning(nodeS);
      return;
    }

    // register new datanode
    DatanodeDescriptor nodeDescr = new DatanodeDescriptor(this.storageMap,
        nodeReg, NetworkTopology.DEFAULT_RACK);

    // resolve network location
        if (this.rejectUnresolvedTopologyDN) {
      nodeDescr.setNetworkLocation(resolveNetworkLocation(nodeDescr));
    } else {
      nodeDescr.setNetworkLocation(
          resolveNetworkLocationWithFallBackToDefaultLocation(nodeDescr));
    }
    addDatanode(nodeDescr);
    checkDecommissioning(nodeDescr);
    
    // also treat the registration message as a heartbeat
    // no need to update its timestamp
    // because its is done when the descriptor is created
    heartbeatManager.addDatanode(nodeDescr);
  }

  /**
   * Rereads conf to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   */
  public void refreshNodes(final Configuration conf) throws IOException {
    // refreshNodes starts/stops decommission/recommission process
    // it should only be handled by the leader node. 
    // because it depends upon threads like replication_deamon which is only active
    // on the leader node. 

    if (!this.namesystem.isLeader()) {
      throw new UnsupportedOperationException(
          "Only Leader NameNode can do refreshNodes");
    }
    refreshHostsReader(conf);
    refreshDatanodes();
  }

  /**
   * Reread include/exclude files.
   */
  private void refreshHostsReader(Configuration conf) throws IOException {
    // Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list.
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    hostsReader.updateFileNames(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
        conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
    hostsReader.refresh();
  }
  
  /**
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned.
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  private void refreshDatanodes() throws IOException {
    for (DatanodeDescriptor node : datanodeMap.values()) {
      // Check if not include.
      if (!inHostsList(node)) {
        node.setDisallowed(true); // case 2.
      } else {
        if (inExcludedHostsList(node)) {
          startDecommission(node); // case 3.
        } else {
          stopDecommission(node); // case 4.
        }
      }
    }
  }

  /**
   * @return the number of live datanodes.
   */
  public int getNumLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        if (!isDatanodeDead(dn)) {
          numLive++;
        }
      }
    }
    return numLive;
  }

  /**
   * @return the number of dead datanodes.
   */
  public int getNumDeadDataNodes() {
    int numDead = 0;
    synchronized (datanodeMap) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        if (isDatanodeDead(dn)) {
          numDead++;
        }
      }
    }
    return numDead;
  }

  /**
   * @return list of datanodes where decommissioning is in progress.
   */
  public List<DatanodeDescriptor> getDecommissioningNodes() {
    final List<DatanodeDescriptor> decommissioningNodes =
        new ArrayList<>();
    final List<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.LIVE);
    for (DatanodeDescriptor node : results) {
      if (node.isDecommissionInProgress()) {
        decommissioningNodes.add(node);
      }
    }
    return decommissioningNodes;
  }
  
  /* Getter and Setter for stale DataNodes related attributes */

  /**
   * Whether stale datanodes should be avoided as targets on the write path.
   * The result of this function may change if the number of stale datanodes
   * eclipses a configurable threshold.
   *
   * @return whether stale datanodes should be avoided on the write path
   */
  public boolean shouldAvoidStaleDataNodesForWrite() {
    // If # stale exceeds maximum staleness ratio, disable stale
    // datanode avoidance on the write path
    return avoidStaleDataNodesForWrite && (numStaleNodes <=
        heartbeatManager.getLiveDatanodeCount() *
            ratioUseStaleDataNodesForWrite);
  }
  
  /**
   * @return The time interval used to mark DataNodes as stale.
   */
  long getStaleInterval() {
    return staleInterval;
  }

  /**
   * Set the number of current stale DataNodes. The HeartbeatManager got this
   * number based on DataNodes' heartbeats.
   *
   * @param numStaleNodes
   *     The number of stale DataNodes to be set.
   */
  void setNumStaleNodes(int numStaleNodes) {
    this.numStaleNodes = numStaleNodes;
  }
  
  /**
   * @return Return the current number of stale DataNodes (detected by
   * HeartbeatManager).
   */
  public int getNumStaleNodes() {
    return this.numStaleNodes;
  }

  /**
   * Fetch live and dead datanodes.
   */
  public void fetchDatanodes(final List<DatanodeDescriptor> live,
      final List<DatanodeDescriptor> dead,
      final boolean removeDecommissionNode) {
    if (live == null && dead == null) {
      throw new HadoopIllegalArgumentException(
          "Both live and dead lists are null");
    }

    final List<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.ALL);
    for (DatanodeDescriptor node : results) {
      if (isDatanodeDead(node)) {
        if (dead != null) {
          dead.add(node);
        }
      } else {
        if (live != null) {
          live.add(node);
        }
      }
    }
    
    if (removeDecommissionNode) {
      if (live != null) {
        removeDecomNodeFromList(live);
      }
      if (dead != null) {
        removeDecomNodeFromList(dead);
      }
    }
  }

  /**
   * @return true if this cluster has ever consisted of multiple racks, even if
   * it is not now a multi-rack cluster.
   */
  boolean hasClusterEverBeenMultiRack() {
    return hasClusterEverBeenMultiRack;
  }

  /**
   * Check if the cluster now consists of multiple racks. If it does, and this
   * is the first time it's consisted of multiple racks, then process blocks
   * that may now be misreplicated.
   *
   * @param node
   *     DN which caused cluster to become multi-rack. Used for logging.
   */
  @VisibleForTesting
  void checkIfClusterIsNowMultiRack(DatanodeDescriptor node)
      throws IOException {
    if (!hasClusterEverBeenMultiRack && networktopology.getNumOfRacks() > 1) {
      String message =
          "DN " + node + " joining cluster has expanded a formerly " +
              "single-rack cluster to be multi-rack. ";
      if (namesystem.isPopulatingReplQueues()) {
        message +=
            "Re-checking all blocks for replication, since they should " +
                "now be replicated cross-rack";
        LOG.info(message);
      } else {
        message +=
            "Not checking for mis-replicated blocks because this NN is " +
                "not yet processing repl queues.";
        LOG.debug(message);
      }
      hasClusterEverBeenMultiRack = true;
      if (namesystem.isPopulatingReplQueues()) {
        blockManager.processMisReplicatedBlocks();
      }
    }
  }

  /**
   * Parse a DatanodeID from a hosts file entry
   *
   * @param hostLine
   *     of form [hostname|ip][:port]?
   * @return DatanodeID constructed from the given string
   */
  private DatanodeID parseDNFromHostsEntry(String hostLine) {
    DatanodeID dnId;
    String hostStr;
    int port;
    int idx = hostLine.indexOf(':');

    if (-1 == idx) {
      hostStr = hostLine;
      port = DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;
    } else {
      hostStr = hostLine.substring(0, idx);
      port = Integer.valueOf(hostLine.substring(idx + 1));
    }

    if (InetAddresses.isInetAddress(hostStr)) {
      // The IP:port is sufficient for listing in a report
      dnId = new DatanodeID(hostStr, "", "", port,
          DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    } else {
      String ipAddr = "";
      try {
        ipAddr = InetAddress.getByName(hostStr).getHostAddress();
      } catch (UnknownHostException e) {
        LOG.warn("Invalid hostname " + hostStr + " in hosts file");
      }
      dnId = new DatanodeID(ipAddr, hostStr, "", port,
          DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    }
    return dnId;
  }

  /**
   * For generating datanode reports
   */
  public List<DatanodeDescriptor> getDatanodeListForReport(
      final DatanodeReportType type) {
    boolean listLiveNodes =
        type == DatanodeReportType.ALL || type == DatanodeReportType.LIVE;
    boolean listDeadNodes =
        type == DatanodeReportType.ALL || type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<>();

    if (listDeadNodes) {
      // Put all nodes referenced in the hosts files in the map
      Iterator<String> it = hostsReader.getHosts().iterator();
      while (it.hasNext()) {
        mustList.put(it.next(), "");
      }
      it = hostsReader.getExcludedHosts().iterator();
      while (it.hasNext()) {
        mustList.put(it.next(), "");
      }
    }

    ArrayList<DatanodeDescriptor> nodes = null;
    
    synchronized (datanodeMap) {
      nodes = new ArrayList<>(
          datanodeMap.size() + mustList.size());
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        final boolean isDead = isDatanodeDead(dn);
        if ((isDead && listDeadNodes) || (!isDead && listLiveNodes)) {
          nodes.add(dn);
        }
        for (String name : getNodeNamesForHostFiltering(dn)) {
          mustList.remove(name);
        }
      }
    }
    
    if (listDeadNodes) {
      for (String s : mustList.keySet()) {
        // The remaining nodes are ones that are referenced by the hosts
        // files but that we do not know about, ie that we have never
        // head from. Eg. a host that is no longer part of the cluster
        // or a bogus entry was given in the hosts files
        DatanodeID dnId = parseDNFromHostsEntry(s);
        DatanodeDescriptor dn = new DatanodeDescriptor(this.storageMap, dnId);
        dn.setLastUpdate(0); // Consider this node dead for reporting
        nodes.add(dn);
      }
    }
    return nodes;
  }
  
  private static List<String> getNodeNamesForHostFiltering(DatanodeID node) {
    String ip = node.getIpAddr();
    String regHostName = node.getHostName();
    int xferPort = node.getXferPort();
    
    List<String> names = new ArrayList<>();
    names.add(ip);
    names.add(ip + ":" + xferPort);
    names.add(regHostName);
    names.add(regHostName + ":" + xferPort);

    String peerHostName = node.getPeerHostName();
    if (peerHostName != null) {
      names.add(peerHostName);
      names.add(peerHostName + ":" + xferPort);
    }
    return names;
  }

  /**
   * Checks if name resolution was successful for the given address.  If IP
   * address and host name are the same, then it means name resolution has
   * failed.  As a special case, local addresses are also considered
   * acceptable.  This is particularly important on Windows, where 127.0.0.1
   * does
   * not resolve to "localhost".
   *
   * @param address
   *     InetAddress to check
   * @return boolean true if name resolution successful or address is local
   */
  private static boolean isNameResolved(InetAddress address) {
    String hostname = address.getHostName();
    String ip = address.getHostAddress();
    return !hostname.equals(ip) || NetUtils.isLocalAddress(address);
  }
  
  private void setDatanodeDead(DatanodeDescriptor node) {
    node.setLastUpdate(0);
  }

  /**
   * Handle heartbeat from datanodes.
   */
  public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, final String blockPoolId, int xceiverCount,
      int maxTransfers, int failedVolumes) throws IOException {
    synchronized (heartbeatManager) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch (UnregisteredNodeException e) {
          return new DatanodeCommand[]{RegisterCommand.REGISTER};
        }
        
        // Check if this datanode should actually be shutdown instead. 
        if (nodeinfo != null && nodeinfo.isDisallowed()) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }

        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{RegisterCommand.REGISTER};
        }

        heartbeatManager.updateHeartbeat(nodeinfo, reports, xceiverCount, failedVolumes);

        // If we are in safemode, do not send back any recovery / replication
        // requests. Don't even drain the existing queue of work.
        if(namesystem.isInSafeMode()) {
          return new DatanodeCommand[0];
        }

        //check lease recovery
        BlockInfoUnderConstruction[] blocks =
            nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (blocks != null) {
          BlockRecoveryCommand brCommand =
              new BlockRecoveryCommand(blocks.length);
          for (BlockInfoUnderConstruction b : blocks) {
            final DatanodeStorageInfo[] storages = getStorageInfosTx(b);

            // Skip stale nodes during recovery - not heart beated for some time (30s by default).
            final List<DatanodeStorageInfo> recoveryLocations =
                new ArrayList<DatanodeStorageInfo>(storages.length);
            for (int i = 0; i < storages.length; i++) {
              if (!storages[i].getDatanodeDescriptor().isStale(staleInterval)) {
                recoveryLocations.add(storages[i]);
              }
            }
            // If we only get 1 replica after eliminating stale nodes, then choose all
            // replicas for recovery and let the primary data node handle failures.
            if (recoveryLocations.size() > 1) {
              if (recoveryLocations.size() != storages.length) {
                LOG.info("Skipped stale nodes for recovery : " +
                    (storages.length - recoveryLocations.size()));
              }
              brCommand.add(new RecoveringBlock(
                  new ExtendedBlock(blockPoolId, b),
                  DatanodeStorageInfo.toDatanodeInfos(recoveryLocations),
                  b.getBlockRecoveryId()));
            } else {
              // If too many replicas are stale, then choose all replicas to participate
              // in block recovery.
              brCommand.add(new RecoveringBlock(
                  new ExtendedBlock(blockPoolId, b),
                  DatanodeStorageInfo.toDatanodeInfos(storages),
                  b.getBlockRecoveryId()));
            }
          }
          return new DatanodeCommand[] { brCommand };
        }

        final List<DatanodeCommand> cmds = new ArrayList<>();
        //check pending replication
        List<BlockTargetPair> pendingList =
            nodeinfo.getReplicationCommand(maxTransfers);
        if (pendingList != null) {
          cmds.add(new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blockPoolId,
              pendingList));
        }
        //check block invalidation
        Block[] blks = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
        if (blks != null) {
          cmds.add(
              new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, blockPoolId,
                  blks));
        }
        
        blockManager.addKeyUpdateCommand(cmds, nodeinfo);

        // check for balancer bandwidth update
        if (nodeinfo.getBalancerBandwidth() > 0) {
          cmds.add(
              new BalancerBandwidthCommand(nodeinfo.getBalancerBandwidth()));
          // set back to 0 to indicate that datanode has been sent the new value
          nodeinfo.setBalancerBandwidth(0);
        }

        if (!cmds.isEmpty()) {
          return cmds.toArray(new DatanodeCommand[cmds.size()]);
        }
      }
    }

    return new DatanodeCommand[0];
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.balance.bandwidthPerSec.
   * <p/>
   * A system administrator can tune the balancer bandwidth parameter
   * (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
   * "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
   * following 'bandwidth' variable gets updated with the new value for each
   * node. Once the heartbeat command is issued to update the value on the
   * specified datanode, this value will be set back to 0.
   *
   * @param bandwidth
   *     Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    synchronized (datanodeMap) {
      for (DatanodeDescriptor nodeInfo : datanodeMap.values()) {
        nodeInfo.setBalancerBandwidth(bandwidth);
      }
    }
  }
  
  public void markAllDatanodesStale() {
    LOG.info("Marking all datandoes as stale");
    synchronized (datanodeMap) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        for(DatanodeStorageInfo storage : dn.getStorageInfos()) {
          storage.markStaleAfterFailover();
        }
      }
    }
  }

  /**
   * Clear any actions that are queued up to be sent to the DNs
   * on their next heartbeats. This includes block invalidations,
   * recoveries, and replication requests.
   */
  public void clearPendingQueues() {
    synchronized (datanodeMap) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        dn.clearBlockQueues();
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + host2DatanodeMap;
  }

  /** @return the Host2NodesMap */
  public Host2NodesMap getHost2DatanodeMap() {
    return this.host2DatanodeMap;
  }

  /**
   * Given datanode address or host name, returns the DatanodeDescriptor for the
   * same, or if it doesn't find the datanode, it looks for a machine local and
   * then rack local datanode, if a rack local datanode is not possible either,
   * it returns the DatanodeDescriptor of any random node in the cluster.
   *
   * @param address hostaddress:transfer address
   * @return the best match for the given datanode
   */
  DatanodeDescriptor getDatanodeDescriptor(String address) {
    DatanodeID dnId = parseDNFromHostsEntry(address);
    String host = dnId.getIpAddr();
    int xferPort = dnId.getXferPort();
    DatanodeDescriptor node = getDatanodeByXferAddr(host, xferPort);
    if (node == null) {
      node = getDatanodeByHost(host);
    }
    if (node == null) {
      String networkLocation =
          resolveNetworkLocationWithFallBackToDefaultLocation(dnId);

      // If the current cluster doesn't contain the node, fallback to
      // something machine local and then rack local.
      List<Node> rackNodes = getNetworkTopology()
          .getDatanodesInRack(networkLocation);
      if (rackNodes != null) {
        // Try something machine local.
        for (Node rackNode : rackNodes) {
          if (((DatanodeDescriptor) rackNode).getIpAddr().equals(host)) {
            node = (DatanodeDescriptor) rackNode;
            break;
          }
        }

        // Try something rack local.
        if (node == null && !rackNodes.isEmpty()) {
          node = (DatanodeDescriptor) (rackNodes
              .get(DFSUtil.getRandom().nextInt(rackNodes.size())));
        }
      }

      // If we can't even choose rack local, just choose any node in the
      // cluster.
      if (node == null) {
        node = (DatanodeDescriptor)getNetworkTopology()
            .chooseRandom(NodeBase.ROOT);
      }
    }
    return node;
  }

  /**
   *  Resolve a node's network location. If the DNS to switch mapping fails
   *  then this method guarantees default rack location.
   *  @param node to resolve to network location
   *  @return network location path
   */
  private String resolveNetworkLocationWithFallBackToDefaultLocation (
      DatanodeID node) {
    String networkLocation;
    try {
      networkLocation = resolveNetworkLocation(node);
    } catch (UnresolvedTopologyException e) {
      LOG.error("Unresolved topology mapping. Using " +
          NetworkTopology.DEFAULT_RACK + " for host " + node.getHostName());
      networkLocation = NetworkTopology.DEFAULT_RACK;
    }
    return networkLocation;
  }

  /**
   * Resolve a node's network location. If the DNS to switch mapping fails,
   * then this method throws UnresolvedTopologyException.
   * @param node to resolve to network location
   * @return network location path.
   * @throws UnresolvedTopologyException if the DNS to switch mapping fails
   *    to resolve network location.
   */
  private String resolveNetworkLocation (DatanodeID node)
      throws UnresolvedTopologyException {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      names.add(node.getIpAddr());
    } else {
      names.add(node.getHostName());
    }

    List<String> rName = resolveNetworkLocation(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null!");
      throw new UnresolvedTopologyException(
          "Unresolved topology mapping for host " + node.getHostName());
    } else {
      networkLocation = rName.get(0);
    }
    return networkLocation;
  }

  /**
   * Resolve network locations for specified hosts
   *
   * @param names
   * @return Network locations if available, Else returns null
   */
  public List<String> resolveNetworkLocation(List<String> names) {
    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    return rName;
  }

  private DatanodeStorageInfo[] getStorageInfosTx(
      final BlockInfoUnderConstruction b) throws IOException {
    final DatanodeManager datanodeManager = this;

    return (DatanodeStorageInfo[]) new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_EXPECTED_BLK_LOCATIONS) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.READ, inodeIdentifier))
            .add(lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.UC));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return b.getExpectedStorageLocations(datanodeManager);
      }
    }.handle();
  }

  /**
   * @return the datanode descriptor for the host.
   */
  public DatanodeDescriptor getDatanodeByXferAddr(String host, int xferPort) {
    return host2DatanodeMap.getDatanodeByXferAddr(host, xferPort);
  }
  
  // only for testing
  @VisibleForTesting
  void addDnToStorageMapInDB(DatanodeDescriptor nodeDescr) throws IOException {

    // Loop over all storages in the datanode
    for(DatanodeStorageInfo storage: nodeDescr.getStorageInfos()) {
      // Allow lookup of sid (int) -> storageInfo (DatanodeStorageInfo)
      updateStorage(storage);
    }
  }

  Random rand = new Random(System.currentTimeMillis());
  public DatanodeDescriptor getRandomDN(){
    if(datanodeMap.isEmpty()){
        return null;
    }else{
        
      return (DatanodeDescriptor) datanodeMap.values().toArray()[rand.nextInt(datanodeMap.size())];
    }
  }

  /**
   * Adds or replaces storageinfo for the sid
   */
  public void updateStorage(DatanodeStorageInfo storageInfo)
      throws IOException {
    this.storageMap.updateStorage(storageInfo);
  }

  public DatanodeStorageInfo getStorage(int sid) {
    return this.storageMap.getStorage(sid);
  }
  
  public int getSid(String StorageId){
    return this.storageMap.getSId(StorageId);
  }

  public List<Integer> getSidsOnDatanode(String datanodeUuid) {
    return this.storageMap.getSidsForDatanodeUuid(datanodeUuid);
  }
}
