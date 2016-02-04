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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.hadoop.util.Time.now;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine,
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on " +
          LOG.getClass().getName();

  private boolean considerLoad;
  private boolean preferLocalNode = true;
  private NetworkTopology clusterMap;
  private FSClusterStats stats;
  private long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes

  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  private int tolerateHeartbeatMultiplier;

  BlockPlacementPolicyDefault(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }

  protected BlockPlacementPolicyDefault() {
  }

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap) {
    this.considerLoad =
        conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
            true);
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.heartbeatInterval =
        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000;
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval =
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
  }

  private ThreadLocal<StringBuilder> threadLocalBuilder =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeStorageInfo writer, List<DatanodeStorageInfo> chosenStorages,
      long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenStorages, false, null,
        blocksize);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeStorageInfo writer, List<DatanodeStorageInfo> chosenStorages,
      boolean returnChosenStorages, HashSet<Node> excludedStorages,
      long blocksize) {
    return chooseTarget(numOfReplicas, writer, chosenStorages, returnChosenStorages,
        excludedStorages, blocksize);
  }

  /**
   * This is the implementation.
   */
  DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeStorageInfo writer, List<DatanodeStorageInfo> chosenStorages,
      boolean returnChosenNodes, HashSet<Node> excludedStorages,
      long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return new DatanodeStorageInfo[0];
    }

    if (excludedStorages == null) {
      excludedStorages = new HashSet<Node>();
    }

    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = chosenStorages.size() + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas - clusterSize);
      totalNumOfReplicas = clusterSize;
    }

    int maxNodesPerRack =
        (totalNumOfReplicas - 1) / clusterMap.getNumOfRacks() + 2;

    List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosenStorages);
    for (DatanodeStorageInfo storage : chosenStorages) {
      excludedStorages.put(node, node);
    }

    if (!clusterMap.contains(writer)) {
      writer = null;
    }

    boolean avoidStaleNodes =
        (stats != null && stats.isAvoidingStaleDataNodesForWrite());
    DatanodeStorageInfo localStorage =
        chooseTarget(numOfReplicas, writer, excludedStorages, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
    if (!returnChosenNodes) {
      results.removeAll(chosenStorages);
    }

    // sorting nodes to form a pipeline
    return getPipeline((writer == null) ? localStorage : writer,
        results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  /* choose <i>numOfReplicas</i> from all data nodes */
  private DatanodeStorageInfo chooseTarget(int numOfReplicas,
      DatanodeStorageInfo writer, HashSet<Node> excludedStorages,
      long blocksize, int maxNodesPerRack, List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return writer;
    }
    int totalReplicasExpected = numOfReplicas + results.size();

    int numOfResults = results.size();
    boolean newBlock = (numOfResults == 0);
    if (writer == null && !newBlock) {
      writer = results.get(0);
    }

    // Keep a copy of original excludedStorages
    final HashSet<Node> oldExcludedNodes =
        avoidStaleNodes ? new HashSet<Node>(excludedStorages) : null;
    try {
      if (numOfResults == 0) {
        writer =
            chooseLocalNode(writer, excludedStorages, blocksize, maxNodesPerRack,
                results, avoidStaleNodes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 1) {
        chooseRemoteRack(1, results.get(0), excludedStorages, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        if (clusterMap.isOnSameRack(results.get(0), results.get(1))) {
          chooseRemoteRack(1, results.get(0), excludedStorages, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        } else if (newBlock) {
          chooseLocalRack(results.get(1), excludedStorages, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        } else {
          chooseLocalRack(writer, excludedStorages, blocksize, maxNodesPerRack,
              results, avoidStaleNodes);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedStorages, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e) {
      LOG.warn("Not able to place enough replicas, still in need of " +
          (totalReplicasExpected - results.size()) + " to reach " +
          totalReplicasExpected + "\n" + e.getMessage());
      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedStorages contains the initial excludedStorages and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the
        // result list in the successful calls to choose*() above.
        for (Node node : results) {
          oldExcludedNodes.put(node, node);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false);
      }
    }
    return writer;
  }

  /* choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   * @return the chosen node
   */
  private DatanodeStorageInfo chooseLocalNode(DatanodeStorageInfo localMachine,
      HashSet<DatanodeStorageInfo> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    }
    if (preferLocalNode) {
      // otherwise try local machine first

      // HashSet.add: "If this set already contains the element, the call leaves
      // the set unchanged and returns false."
      boolean notContained = excludedNodes.add(localMachine);

      if (notContained) { // was not in the excluded list
        if (isGoodTarget(localMachine, blocksize, maxNodesPerRack, false,
            results, avoidStaleNodes)) {
          results.add(localMachine);
          return localMachine;
        }
      }
    }
    // try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes);
  }

  /* choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node
   * in the cluster.
   * @return the chosen node
   */
  private DatanodeStorageInfo chooseLocalRack(DatanodeDescriptor localMachine,
      HashSet<DatanodeStorageInfo> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes);
    }

    // choose one from the local rack
    try {
      return chooseRandom(localMachine.getNetworkLocation(), excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeStorageInfo newLocal = null;
      for (Iterator<DatanodeStorageInfo> iter = results.iterator();
           iter.hasNext(); ) {
        DatanodeStorageInfo nextStorage = iter.next();
        if (nextStorage.getDatanodeDescriptor() != localMachine) {
          newLocal = nextStorage;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(
              newLocal.getDatanodeDescriptor().getNetworkLocation(),
              excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes);
        } catch (NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes);
      }
    }
  }

  /* choose <i>numOfReplicas</i> nodes from the racks
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones
   * from the local rack
   */

  private void chooseRemoteRack(int numOfReplicas,
      DatanodeDescriptor localMachine, HashSet<Node> excludedNodes,
      long blocksize, int maxReplicasPerRack, List<DatanodeDescriptor> results,
      boolean avoidStaleNodes) throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas),
          localMachine.getNetworkLocation(), excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes);
    }
  }

  /* Randomly choose one target from <i>nodes</i>.
   * @return the chosen node
   */
  private DatanodeStorageInfo chooseRandom(
      String nodes,
      HashSet<DatanodeStorageInfo> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes)
      throws NotEnoughReplicasException {
    int numOfAvailableNodes =
        clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = threadLocalBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    while (numOfAvailableNodes > 0) {
      DatanodeStorageInfo chosenStorage =
          (DatanodeStorageInfo) (clusterMap.chooseRandom(nodes));

      Node oldNode = excludedNodes.add(chosenStorage);
      if (oldNode == null) { // choosendNode was not in the excluded list
        numOfAvailableNodes--;
        if (isGoodTarget(chosenStorage, blocksize, maxNodesPerRack, results,
            avoidStaleNodes)) {
          results.add(chosenStorage);
          return chosenStorage;
        } else {
          badTarget = true;
        }
      }
    }

    String detail = enableDebugLogging;
    if (LOG.isDebugEnabled()) {
      if (badTarget && builder != null) {
        detail = builder.append("]").toString();
        builder.setLength(0);
      } else {
        detail = "";
      }
    }
    throw new NotEnoughReplicasException(detail);
  }

  /* Randomly choose <i>numOfReplicas</i> targets from <i>nodes</i>.
   */
  private void chooseRandom(int numOfReplicas, String nodes,
      HashSet<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes)
      throws NotEnoughReplicasException {

    int numOfAvailableNodes =
        clusterMap.countNumOfAvailableNodes(nodes, excludedNodes.keySet());
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = threadLocalBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    while (numOfReplicas > 0 && numOfAvailableNodes > 0) {
      DatanodeStorageInfo chosenStorage = clusterMap.chooseRandom(nodes);

      // HashSet.add: "If this set already contains the element, the call leaves
      // the set unchanged and returns false."
      boolean notContained = excludedNodes.add(chosenStorage);
      if (notContained) {
        numOfAvailableNodes--;

        if (isGoodTarget(chosenStorage, blocksize, maxNodesPerRack, results,
            avoidStaleNodes)) {
          numOfReplicas--;
          results.add(chosenStorage);
        } else {
          badTarget = true;
        }
      }
    }

    if (numOfReplicas > 0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.append("]").toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }
  }

  /* judge if a storage is a good target.
   * return true if <i>storage</i> has enough space,
   * does not have too much load, and the rack does not have too many nodes
   */
  private boolean isGoodTarget(DatanodeStorageInfo storage, long blockSize,
      int maxTargetPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes) {
    return isGoodTarget(storage, blockSize, maxTargetPerRack, this.considerLoad,
        results, avoidStaleNodes);
  }

  /**
   * Determine if a storage is a good target.
   *
   * @param storage
   *     The target storage
   * @param blockSize
   *     Size of block
   * @param maxTargetPerRack
   *     Maximum number of targets per rack. The value of
   *     this parameter depends on the number of racks in
   *     the cluster and total number of replicas for a block
   * @param considerLoad
   *     whether or not to consider load of the target node
   * @param results
   *     A list containing currently chosen nodes. Used to check if
   *     too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes
   *     Whether or not to avoid choosing stale nodes
   * @return Return true if <i>node</i> has enough space,
   * does not have too much load,
   * and the rack does not have too many nodes.
   */
  private boolean isGoodTarget(DatanodeStorageInfo storage, long blockSize,
      int maxTargetPerRack, boolean considerLoad,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes) {

    // TODO per storage?
    // We're doing load balancing etc. per host:
    DatanodeDescriptor node = storage.getDatanodeDescriptor();

    // check if the node is (being) decommissed
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      if (LOG.isDebugEnabled()) {
        threadLocalBuilder.get().append(node.toString()).append(": ")
            .append("Node ").append(NodeBase.getPath(node)).append(
            " is not chosen because the node is (being) decommissioned ");
      }
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        if (LOG.isDebugEnabled()) {
          threadLocalBuilder.get().append(node.toString()).append(": ")
              .append("Node ").append(NodeBase.getPath(node))
              .append(" is not chosen because the node is stale ");
        }
        return false;
      }
    }

    long remaining =
        storage.getRemaining() - (storage.getBlocksScheduled() * blockSize);
    // check the remaining capacity of the target machine
    if (blockSize * HdfsConstants.MIN_BLOCKS_FOR_WRITE > remaining) {
      if (LOG.isDebugEnabled()) {
        threadLocalBuilder.get().append(storage.toString()).append(": ")
            .append("Node ").append(storage.toString()).append(
            " is not chosen because the node does not have enough space ");
      }
      return false;
    }

    // check the communication traffic of the target machine
    if (considerLoad) {
      double avgLoad = 0;
      int size = clusterMap.getNumOfLeaves();
      if (size != 0 && stats != null) {
        avgLoad = (double) stats.getTotalLoad() / size;
      }
      if (node.getXceiverCount() > (2.0 * avgLoad)) {
        if (LOG.isDebugEnabled()) {
          threadLocalBuilder.get().append(node.toString()).append(": ")
              .append("Node ").append(NodeBase.getPath(node))
              .append(" is not chosen because the node is too busy ");
        }
        return false;
      }
    }

    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter = 1;
    for (Iterator<DatanodeStorageInfo> iter = results.iterator();
         iter.hasNext(); ) {
      Node nodeStoringReplica = iter.next().getDatanodeDescriptor();
      if (rackname.equals(nodeStoringReplica.getNetworkLocation())) {
        counter++;
      }
    }
    if (counter > maxTargetPerRack) {
      if (LOG.isDebugEnabled()) {
        threadLocalBuilder.get().append(storage.toString()).append(": ")
            .append("storage ").append(storage.toString()).append(
            " is not chosen because the rack has too many replicas of this block");
      }
      return false;
    }
    return true;
  }

  /* Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeDescriptor[] getPipeline(DatanodeDescriptor writer,
      DatanodeDescriptor[] nodes) {
    if (nodes.length == 0) {
      return nodes;
    }

    synchronized (clusterMap) {
      int index = 0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = nodes[0];
      }
      for (; index < nodes.length; index++) {
        DatanodeDescriptor shortestNode = nodes[index];
        int shortestDistance = clusterMap.getDistance(writer, shortestNode);
        int shortestIndex = index;
        for (int i = index + 1; i < nodes.length; i++) {
          DatanodeDescriptor currentNode = nodes[i];
          int currentDistance = clusterMap.getDistance(writer, currentNode);
          if (shortestDistance > currentDistance) {
            shortestDistance = currentDistance;
            shortestNode = currentNode;
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          nodes[shortestIndex] = nodes[index];
          nodes[index] = shortestNode;
        }
        writer = shortestNode;
      }
    }
    return nodes;
  }

  @Override
  public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk,
      int minRacks) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null) {
      locs = new DatanodeInfo[0];
    }
    int numRacks = clusterMap.getNumOfRacks();
    if (numRacks <= 1) // only one rack
    {
      return 0;
    }
    minRacks = Math.min(minRacks, numRacks);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs) {
      racks.add(dn.getNetworkLocation());
    }
    return minRacks - racks.size();
  }

  @Override
  public DatanodeStorageInfo chooseReplicaToDelete(BlockCollection bc,
      Block block, short replicationFactor,
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second) {
    long oldestHeartbeat =
        now() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for(DatanodeStorageInfo storage : pickupReplicaSet(first, second)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = node.getRemaining();
      long lastHeartbeat = node.getLastUpdate();
      if(lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    final DatanodeStorageInfo storage;
    if (oldestHeartbeatStorage != null) {
      storage = oldestHeartbeatStorage;
    } else if (minSpaceStorage != null) {
      storage = minSpaceStorage;
    } else {
      return null;
    }
    return storage;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated.
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * So pick up first set if not empty. If first is empty, then pick second.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second) {
    return first.isEmpty() ? second : first;
  }

  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }

  public DatanodeDescriptor getRandomNode() {
    return (DatanodeDescriptor) (clusterMap.chooseRandom(NodeBase.ROOT));
  }
}

