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
package org.apache.hadoop.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled 
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.  
 * 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetworkTopology {
  public final static String DEFAULT_RACK = "/default-rack";
  public static final Logger LOG =
      LoggerFactory.getLogger(NetworkTopology.class);

  private static final char PATH_SEPARATOR = '/';
  private static final String PATH_SEPARATOR_STR = "/";
  private static final String ROOT = "/";

  public static class InvalidTopologyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public InvalidTopologyException(String msg) {
      super(msg);
    }
  }
  
  /**
   * Get an instance of NetworkTopology based on the value of the configuration
   * parameter net.topology.impl.
   * 
   * @param conf the configuration to be used
   * @return an instance of NetworkTopology
   */
  public static NetworkTopology getInstance(Configuration conf){
    return getInstance(conf, InnerNodeImpl.FACTORY);
  }

  public static NetworkTopology getInstance(Configuration conf,
      InnerNode.Factory factory) {
    NetworkTopology nt = ReflectionUtils.newInstance(
        conf.getClass(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY,
            NetworkTopology.class, NetworkTopology.class), conf);
    return nt.init(factory);
  }

  protected NetworkTopology init(InnerNode.Factory factory) {
    if (!factory.equals(this.factory)) {
      // the constructor has initialized the factory to default. So only init
      // again if another factory is specified.
      this.factory = factory;
      this.clusterMap = factory.newInnerNode(NodeBase.ROOT);
    }
    return this;
  }

  InnerNode.Factory factory;
  /**
   * the root cluster map
   */
  InnerNode clusterMap;
  /** Depth of all leaf nodes */
  private int depthOfAllLeaves = -1;
  /** rack counter */
  protected int numOfRacks = 0;

  /**
   * Whether or not this cluster has ever consisted of more than 1 rack,
   * according to the NetworkTopology.
   */
  private boolean clusterEverBeenMultiRack = false;

  /** the lock used to manage access */
  protected ReadWriteLock netlock = new ReentrantReadWriteLock(true);

  // keeping the constructor because other components like MR still uses this.
  public NetworkTopology() {
    this.factory = InnerNodeImpl.FACTORY;
    this.clusterMap = factory.newInnerNode(NodeBase.ROOT);
  }

  /** Add a leaf node
   * Update node counter & rack counter if necessary
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave 
                                         or node to be added is not a leaf
   */
  public void add(Node node) {
    if (node==null) return;
    int newDepth = NodeBase.locationToDepth(node.getNetworkLocation()) + 1;
    netlock.writeLock().lock();
    try {
      if( node instanceof InnerNode ) {
        throw new IllegalArgumentException(
          "Not allow to add an inner node: "+NodeBase.getPath(node));
      }
      if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth)) {
        LOG.error("Error: can't add leaf node {} at depth {} to topology:{}\n",
            NodeBase.getPath(node), newDepth, this);
        throw new InvalidTopologyException("Failed to add " + NodeBase.getPath(node) +
            ": You cannot have a rack and a non-rack node at the same " +
            "level of the network topology.");
      }
      Node rack = getNodeForNetworkLocation(node);
      if (rack != null && !(rack instanceof InnerNode)) {
        throw new IllegalArgumentException("Unexpected data node " 
                                           + node.toString() 
                                           + " at an illegal network location");
      }
      if (clusterMap.add(node)) {
        LOG.info("Adding a new node: "+NodeBase.getPath(node));
        if (rack == null) {
          incrementRacks();
        }
        if (!(node instanceof InnerNode)) {
          if (depthOfAllLeaves == -1) {
            depthOfAllLeaves = node.getLevel();
          }
        }
      }
      LOG.debug("NetworkTopology became:\n{}", this);
    } finally {
      netlock.writeLock().unlock();
    }
  }

  protected void incrementRacks() {
    numOfRacks++;
    if (!clusterEverBeenMultiRack && numOfRacks > 1) {
      clusterEverBeenMultiRack = true;
    }
  }

  /**
   * Return a reference to the node given its string representation.
   * Default implementation delegates to {@link #getNode(String)}.
   * 
   * <p>To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full {@link #add(Node)}
   *  method.
   * 
   * @param node The string representation of this node's network location is
   * used to retrieve a Node object. 
   * @return a reference to the node; null if the node is not in the tree
   * 
   * @see #add(Node)
   * @see #getNode(String)
   */
  protected Node getNodeForNetworkLocation(Node node) {
    return getNode(node.getNetworkLocation());
  }
  
  /**
   * Given a string representation of a rack, return its children
   * @param loc a path-like string representation of a rack
   * @return a newly allocated list with all the node's children
   */
  public List<Node> getDatanodesInRack(String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      if (!NodeBase.ROOT.equals(loc)) {
        loc = loc.substring(1);
      }
      InnerNode rack = (InnerNode) clusterMap.getLoc(loc);
      if (rack == null) {
        return null;
      }
      return new ArrayList<Node>(rack.getChildren());
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** Remove a node
   * Update node counter and rack counter if necessary
   * @param node node to be removed; can be null
   */ 
  public void remove(Node node) {
    if (node==null) return;
    if( node instanceof InnerNode ) {
      throw new IllegalArgumentException(
        "Not allow to remove an inner node: "+NodeBase.getPath(node));
    }
    LOG.info("Removing a node: "+NodeBase.getPath(node));
    netlock.writeLock().lock();
    try {
      if (clusterMap.remove(node)) {
        InnerNode rack = (InnerNode)getNode(node.getNetworkLocation());
        if (rack == null) {
          numOfRacks--;
        }
      }
      LOG.debug("NetworkTopology became:\n{}", this);
    } finally {
      netlock.writeLock().unlock();
    }
  }

  /** Check if the tree contains node <i>node</i>
   * 
   * @param node a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  public boolean contains(Node node) {
    if (node == null) return false;
    netlock.readLock().lock();
    try {
      Node parent = node.getParent();
      for (int level = node.getLevel(); parent != null && level > 0;
           parent = parent.getParent(), level--) {
        if (parent == clusterMap) {
          return true;
        }
      }
    } finally {
      netlock.readLock().unlock();
    }
    return false; 
  }
    
  /** Given a string representation of a node, return its reference
   * 
   * @param loc
   *          a path-like string representation of a node
   * @return a reference to the node; null if the node is not in the tree
   */
  public Node getNode(String loc) {
    netlock.readLock().lock();
    try {
      loc = NodeBase.normalize(loc);
      if (!NodeBase.ROOT.equals(loc))
        loc = loc.substring(1);
      return clusterMap.getLoc(loc);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * @return true if this cluster has ever consisted of multiple racks, even if
   *         it is not now a multi-rack cluster.
   */
  public boolean hasClusterEverBeenMultiRack() {
    return clusterEverBeenMultiRack;
  }

  /** Given a string representation of a rack for a specific network
   *  location
   *
   * To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full 
   * {@link #getRack(String)} method.
   * @param loc
   *          a path-like string representation of a network location
   * @return a rack string
   */
  public String getRack(String loc) {
    return loc;
  }
  
  /** @return the total number of racks */
  public int getNumOfRacks() {
    netlock.readLock().lock();
    try {
      return numOfRacks;
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** @return the total number of leaf nodes */
  public int getNumOfLeaves() {
    netlock.readLock().lock();
    try {
      return clusterMap.getNumOfLeaves();
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** Return the distance between two nodes
   * It is assumed that the distance from one node to its parent is 1
   * The distance between two nodes is calculated by summing up their distances
   * to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance between node1 and node2 which is zero if they are the same
   *  or {@link Integer#MAX_VALUE} if node1 or node2 do not belong to the cluster
   */
  public int getDistance(Node node1, Node node2) {
    if ((node1 != null && node1.equals(node2)) ||
        (node1 == null && node2 == null))  {
      return 0;
    }
    if (node1 == null || node2 == null) {
      LOG.warn("One of the nodes is a null pointer");
      return Integer.MAX_VALUE;
    }
    Node n1=node1, n2=node2;
    int dis = 0;
    netlock.readLock().lock();
    try {
      int level1=node1.getLevel(), level2=node2.getLevel();
      while(n1!=null && level1>level2) {
        n1 = n1.getParent();
        level1--;
        dis++;
      }
      while(n2!=null && level2>level1) {
        n2 = n2.getParent();
        level2--;
        dis++;
      }
      while(n1!=null && n2!=null && n1.getParent()!=n2.getParent()) {
        n1=n1.getParent();
        n2=n2.getParent();
        dis+=2;
      }
    } finally {
      netlock.readLock().unlock();
    }
    if (n1==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node1));
      return Integer.MAX_VALUE;
    }
    if (n2==null) {
      LOG.warn("The cluster does not contain node: "+NodeBase.getPath(node2));
      return Integer.MAX_VALUE;
    }
    return dis+2;
  }

  /** Return the distance between two nodes by comparing their network paths
   * without checking if they belong to the same ancestor node by reference.
   * It is assumed that the distance from one node to its parent is 1
   * The distance between two nodes is calculated by summing up their distances
   * to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance between node1 and node2
   */
  static public int getDistanceByPath(Node node1, Node node2) {
    if (node1 == null && node2 == null) {
      return 0;
    }
    if (node1 == null || node2 == null) {
      LOG.warn("One of the nodes is a null pointer");
      return Integer.MAX_VALUE;
    }
    String[] paths1 = NodeBase.getPathComponents(node1);
    String[] paths2 = NodeBase.getPathComponents(node2);
    int dis = 0;
    int index = 0;
    int minLevel = Math.min(paths1.length, paths2.length);
    while (index < minLevel) {
      if (!paths1[index].equals(paths2[index])) {
        // Once the path starts to diverge,  compute the distance that include
        // the rest of paths.
        dis += 2 * (minLevel - index);
        break;
      }
      index++;
    }
    dis += Math.abs(paths1.length - paths2.length);
    return dis;
  }

  /** Check if two nodes are on the same rack
   * @param node1 one node (can be null)
   * @param node2 another node (can be null)
   * @return true if node1 and node2 are on the same rack; false otherwise
   * @exception IllegalArgumentException when either node1 or node2 is null, or
   * node1 or node2 do not belong to the cluster
   */
  public boolean isOnSameRack( Node node1,  Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
      
    netlock.readLock().lock();
    try {
      return isSameParents(node1, node2);
    } finally {
      netlock.readLock().unlock();
    }
  }
  
  /**
   * Check if network topology is aware of NodeGroup
   */
  public boolean isNodeGroupAware() {
    return false;
  }
  
  /** 
   * Return false directly as not aware of NodeGroup, to be override in sub-class
   */
  public boolean isOnSameNodeGroup(Node node1, Node node2) {
    return false;
  }

  /**
   * Compare the parents of each node for equality
   * 
   * <p>To be overridden in subclasses for specific NetworkTopology 
   * implementations, as alternative to overriding the full 
   * {@link #isOnSameRack(Node, Node)} method.
   * 
   * @param node1 the first node to compare
   * @param node2 the second node to compare
   * @return true if their parents are equal, false otherwise
   * 
   * @see #isOnSameRack(Node, Node)
   */
  protected boolean isSameParents(Node node1, Node node2) {
    return node1.getParent()==node2.getParent();
  }

  private static final Random r = new Random();

  @VisibleForTesting
  void setRandomSeed(long seed) {
    r.setSeed(seed);
  }

  /**
   * Randomly choose a node.
   *
   * @param scope range of nodes from which a node will be chosen
   * @return the chosen node
   *
   * @see #chooseRandom(String, Collection)
   */
  public Node chooseRandom(final String scope) {
    return chooseRandom(scope, null);
  }

  /**
   * Randomly choose one node from <i>scope</i>.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from
   * @return the chosen node
   */
  public Node chooseRandom(final String scope,
      final Collection<Node> excludedNodes) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return chooseRandom(NodeBase.ROOT, scope.substring(1), excludedNodes);
      } else {
        return chooseRandom(scope, null, excludedNodes);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  protected Node chooseRandom(final String scope, String excludedScope,
      final Collection<Node> excludedNodes) {
    if (excludedScope != null) {
      if (scope.startsWith(excludedScope)) {
        return null;
      }
      if (!excludedScope.startsWith(scope)) {
        excludedScope = null;
      }
    }
    Node node = getNode(scope);
    if (!(node instanceof InnerNode)) {
      return excludedNodes != null && excludedNodes.contains(node) ?
          null : node;
    }
    InnerNode innerNode = (InnerNode)node;
    int numOfDatanodes = innerNode.getNumOfLeaves();
    if (excludedScope == null) {
      node = null;
    } else {
      node = getNode(excludedScope);
      if (!(node instanceof InnerNode)) {
        numOfDatanodes -= 1;
      } else {
        numOfDatanodes -= ((InnerNode)node).getNumOfLeaves();
      }
    }
    if (numOfDatanodes <= 0) {
      LOG.debug("Failed to find datanode (scope=\"{}\" excludedScope=\"{}\")."
              + " numOfDatanodes={}",
          scope, excludedScope, numOfDatanodes);
      return null;
    }
    final int availableNodes;
    if (excludedScope == null) {
      availableNodes = countNumOfAvailableNodes(scope, excludedNodes);
    } else {
      availableNodes =
          countNumOfAvailableNodes("~" + excludedScope, excludedNodes);
    }
    LOG.debug("Choosing random from {} available nodes on node {},"
        + " scope={}, excludedScope={}, excludeNodes={}. numOfDatanodes={}.",
        availableNodes, innerNode, scope, excludedScope, excludedNodes,
        numOfDatanodes);
    Node ret = null;
    if (availableNodes > 0) {
      ret = chooseRandom(innerNode, node, excludedNodes, numOfDatanodes,
          availableNodes);
    }
    LOG.debug("chooseRandom returning {}", ret);
    return ret;
  }

  /**
   * Randomly choose one node under <i>parentNode</i>, considering the exclude
   * nodes and scope. Should be called with {@link #netlock}'s readlock held.
   *
   * @param parentNode        the parent node
   * @param excludedScopeNode the node corresponding to the exclude scope.
   * @param excludedNodes     a collection of nodes to be excluded from
   * @param totalInScopeNodes total number of nodes under parentNode, excluding
   *                          the excludedScopeNode
   * @param availableNodes    number of available nodes under parentNode that
   *                          could be chosen, excluding excludedNodes
   * @return the chosen node, or null if none can be chosen
   */
  private Node chooseRandom(final InnerNode parentNode,
      final Node excludedScopeNode, final Collection<Node> excludedNodes,
      final int totalInScopeNodes, final int availableNodes) {
    Preconditions.checkArgument(
        totalInScopeNodes >= availableNodes && availableNodes > 0, String
            .format("%d should >= %d, and both should be positive.",
                totalInScopeNodes, availableNodes));
    if (excludedNodes == null || excludedNodes.isEmpty()) {
      // if there are no excludedNodes, randomly choose a node
      final int index = r.nextInt(totalInScopeNodes);
      return parentNode.getLeaf(index, excludedScopeNode);
    }

    // excludedNodes non empty.
    // Choose the nth VALID node, where n is random. VALID meaning it can be
    // returned, after considering exclude scope and exclude nodes.
    // The probability of being chosen should be equal for all VALID nodes.
    // Notably, we do NOT choose nth node, and find the next valid node
    // if n is excluded - this will make the probability of the node immediately
    // after an excluded node higher.
    //
    // Start point is always 0 and that's fine, because the nth valid node
    // logic provides equal randomness.
    //
    // Consider this example, where 1,3,5 out of the 10 nodes are excluded:
    // 1 2 3 4 5 6 7 8 9 10
    // x   x   x
    // We will randomly choose the nth valid node where n is [0,6].
    // We do NOT choose a random number n and just use the closest valid node,
    // for example both n=3 and n=4 will choose 4, making it a 2/10 probability,
    // higher than the expected 1/7
    // totalInScopeNodes=10 and availableNodes=7 in this example.
    int nthValidToReturn = r.nextInt(availableNodes);
    LOG.debug("nthValidToReturn is {}", nthValidToReturn);
    Node ret =
        parentNode.getLeaf(r.nextInt(totalInScopeNodes), excludedScopeNode);
    if (!excludedNodes.contains(ret)) {
      // return if we're lucky enough to get a valid node at a random first pick
      LOG.debug("Chosen node {} from first random", ret);
      return ret;
    } else {
      ret = null;
    }
    Node lastValidNode = null;
    for (int i = 0; i < totalInScopeNodes; ++i) {
      ret = parentNode.getLeaf(i, excludedScopeNode);
      if (!excludedNodes.contains(ret)) {
        if (nthValidToReturn == 0) {
          break;
        }
        --nthValidToReturn;
        lastValidNode = ret;
      } else {
        LOG.debug("Node {} is excluded, continuing.", ret);
        ret = null;
      }
    }
    if (ret == null && lastValidNode != null) {
      LOG.error("BUG: Found lastValidNode {} but not nth valid node. "
              + "parentNode={}, excludedScopeNode={}, excludedNodes={}, "
              + "totalInScopeNodes={}, availableNodes={}, nthValidToReturn={}.",
          lastValidNode, parentNode, excludedScopeNode, excludedNodes,
          totalInScopeNodes, availableNodes, nthValidToReturn);
      ret = lastValidNode;
    }
    return ret;
  }

  /** return leaves in <i>scope</i>
   * @param scope a path string
   * @return leaves nodes under specific scope
   */
  public List<Node> getLeaves(String scope) {
    Node node = getNode(scope);
    List<Node> leafNodes = new ArrayList<Node>();
    if (!(node instanceof InnerNode)) {
      leafNodes.add(node);
    } else {
      InnerNode innerNode = (InnerNode) node;
      for (int i=0;i<innerNode.getNumOfLeaves();i++) {
        leafNodes.add(innerNode.getLeaf(i, null));
      }
    }
    return leafNodes;
  }

  /** return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
   * if scope starts with ~, return the number of nodes that are not
   * in <i>scope</i> and <i>excludedNodes</i>; 
   * @param scope a path string that may start with ~
   * @param excludedNodes a list of nodes
   * @return number of available nodes
   */
  @VisibleForTesting
  public int countNumOfAvailableNodes(String scope,
                                      Collection<Node> excludedNodes) {
    boolean isExcluded=false;
    if (scope.startsWith("~")) {
      isExcluded=true;
      scope=scope.substring(1);
    }
    scope = NodeBase.normalize(scope);
    int excludedCountInScope = 0; // the number of nodes in both scope & excludedNodes
    int excludedCountOffScope = 0; // the number of nodes outside scope & excludedNodes
    netlock.readLock().lock();
    try {
      if (excludedNodes != null) {
        for (Node node : excludedNodes) {
          node = getNode(NodeBase.getPath(node));
          if (node == null) {
            continue;
          }
          if ((NodeBase.getPath(node) + NodeBase.PATH_SEPARATOR_STR)
              .startsWith(scope + NodeBase.PATH_SEPARATOR_STR)) {
            excludedCountInScope++;
          } else {
            excludedCountOffScope++;
          }
        }
      }
      Node n = getNode(scope);
      int scopeNodeCount = 0;
      if (n != null) {
        scopeNodeCount++;
      }
      if (n instanceof InnerNode) {
        scopeNodeCount=((InnerNode)n).getNumOfLeaves();
      }
      if (isExcluded) {
        return clusterMap.getNumOfLeaves() - scopeNodeCount
            - excludedCountOffScope;
      } else {
        return scopeNodeCount - excludedCountInScope;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /** convert a network tree to a string. */
  @Override
  public String toString() {
    // print the number of racks
    StringBuilder tree = new StringBuilder();
    tree.append("Number of racks: ");
    tree.append(numOfRacks);
    tree.append("\n");
    // print the number of leaves
    int numOfLeaves = getNumOfLeaves();
    tree.append("Expected number of leaves:");
    tree.append(numOfLeaves);
    tree.append("\n");
    // print nodes
    for(int i=0; i<numOfLeaves; i++) {
      tree.append(NodeBase.getPath(clusterMap.getLeaf(i, null)));
      tree.append("\n");
    }
    return tree.toString();
  }
  
  /**
   * Divide networklocation string into two parts by last separator, and get 
   * the first part here.
   * 
   * @param networkLocation
   * @return
   */
  public static String getFirstHalf(String networkLocation) {
    int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
    return networkLocation.substring(0, index);
  }

  /**
   * Divide networklocation string into two parts by last separator, and get 
   * the second part here.
   * 
   * @param networkLocation
   * @return
   */
  public static String getLastHalf(String networkLocation) {
    int index = networkLocation.lastIndexOf(NodeBase.PATH_SEPARATOR_STR);
    return networkLocation.substring(index);
  }

  /**
   * Returns an integer weight which specifies how far away {node} is away from
   * {reader}. A lower value signifies that a node is closer.
   * 
   * @param reader Node where data will be read
   * @param node Replica of data
   * @return weight
   */
  protected int getWeight(Node reader, Node node) {
    // 0 is local, 2 is same rack, and each level on each node increases the
    //weight by 1
    //Start off by initializing to Integer.MAX_VALUE
    int weight = Integer.MAX_VALUE;
    if (reader != null && node != null) {
      if(reader.equals(node)) {
        return 0;
      }
      int maxReaderLevel = reader.getLevel();
      int maxNodeLevel = node.getLevel();
      int currentLevelToCompare = maxReaderLevel > maxNodeLevel ?
          maxNodeLevel : maxReaderLevel;
      Node r = reader;
      Node n = node;
      weight = 0;
      while(r != null && r.getLevel() > currentLevelToCompare) {
        r = r.getParent();
        weight++;
      }
      while(n != null && n.getLevel() > currentLevelToCompare) {
        n = n.getParent();
        weight++;
      }
      while(r != null && n != null && !r.equals(n)) {
        r = r.getParent();
        n = n.getParent();
        weight+=2;
      }
    }
    return weight;
  }

  /**
   * Returns an integer weight which specifies how far away <i>node</i> is
   * from <i>reader</i>. A lower value signifies that a node is closer.
   * It uses network location to calculate the weight
   *
   * @param reader Node where data will be read
   * @param node Replica of data
   * @return weight
   */
  private static int getWeightUsingNetworkLocation(Node reader, Node node) {
    //Start off by initializing to Integer.MAX_VALUE
    int weight = Integer.MAX_VALUE;
    if(reader != null && node != null) {
      String readerPath = normalizeNetworkLocationPath(
          reader.getNetworkLocation());
      String nodePath = normalizeNetworkLocationPath(
          node.getNetworkLocation());

      //same rack
      if(readerPath.equals(nodePath)) {
        if(reader.getName().equals(node.getName())) {
          weight = 0;
        } else {
          weight = 2;
        }
      } else {
        String[] readerPathToken = readerPath.split(PATH_SEPARATOR_STR);
        String[] nodePathToken = nodePath.split(PATH_SEPARATOR_STR);
        int maxLevelToCompare = readerPathToken.length > nodePathToken.length ?
            nodePathToken.length : readerPathToken.length;
        int currentLevel = 1;
        //traverse through the path and calculate the distance
        while(currentLevel < maxLevelToCompare) {
          if(!readerPathToken[currentLevel]
              .equals(nodePathToken[currentLevel])){
            break;
          }
          currentLevel++;
        }
        weight = (readerPathToken.length - currentLevel) +
            (nodePathToken.length - currentLevel);
      }
    }
    return weight;
  }

  /** Normalize a path by stripping off any trailing {@link #PATH_SEPARATOR}.
   * @param path path to normalize.
   * @return the normalised path
   * If <i>path</i>is null or empty {@link #ROOT} is returned
   * @throws IllegalArgumentException if the first character of a non empty path
   * is not {@link #PATH_SEPARATOR}
   */
  private static String normalizeNetworkLocationPath(String path) {
    if (path == null || path.length() == 0) {
      return ROOT;
    }

    if (path.charAt(0) != PATH_SEPARATOR) {
      throw new IllegalArgumentException("Network Location"
          + "path doesn't start with " +PATH_SEPARATOR+ ": "+path);
    }

    int len = path.length();
    if (path.charAt(len-1) == PATH_SEPARATOR) {
      return path.substring(0, len-1);
    }
    return path;
  }

  /**
   * Sort nodes array by network distance to <i>reader</i>.
   * <p/>
   * In a three-level topology, a node can be either local, on the same rack,
   * or on a different rack from the reader. Sorting the nodes based on network
   * distance from the reader reduces network traffic and improves
   * performance.
   * <p/>
   * As an additional twist, we also randomize the nodes at each network
   * distance. This helps with load balancing when there is data skew.
   *
   * @param reader    Node where data will be read
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   */
  public void sortByDistance(Node reader, Node[] nodes, int activeLen) {
    /*
     * This method is called if the reader is a datanode,
     * so nonDataNodeReader flag is set to false.
     */
    sortByDistance(reader, nodes, activeLen, false);
  }

  /**
   * Sort nodes array by network distance to <i>reader</i>.
   * <p/> using network location. This is used when the reader
   * is not a datanode. Sorting the nodes based on network distance
   * from the reader reduces network traffic and improves
   * performance.
   * <p/>
   *
   * @param reader    Node where data will be read
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   */
  public void sortByDistanceUsingNetworkLocation(Node reader, Node[] nodes,
      int activeLen) {
    /*
     * This method is called if the reader is not a datanode,
     * so nonDataNodeReader flag is set to true.
     */
    sortByDistance(reader, nodes, activeLen, true);
  }

  /**
   * Sort nodes array by network distance to <i>reader</i>.
   * <p/>
   * As an additional twist, we also randomize the nodes at each network
   * distance. This helps with load balancing when there is data skew.
   *
   * @param reader    Node where data will be read
   * @param nodes     Available replicas with the requested data
   * @param activeLen Number of active nodes at the front of the array
   * @param nonDataNodeReader True if the reader is not a datanode
   */
  private void sortByDistance(Node reader, Node[] nodes, int activeLen,
      boolean nonDataNodeReader) {
    /** Sort weights for the nodes array */
    int[] weights = new int[activeLen];
    for (int i=0; i<activeLen; i++) {
      if(nonDataNodeReader) {
        weights[i] = getWeightUsingNetworkLocation(reader, nodes[i]);
      } else {
        weights[i] = getWeight(reader, nodes[i]);
      }
    }
    // Add weight/node pairs to a TreeMap to sort
    TreeMap<Integer, List<Node>> tree = new TreeMap<Integer, List<Node>>();
    for (int i=0; i<activeLen; i++) {
      int weight = weights[i];
      Node node = nodes[i];
      List<Node> list = tree.get(weight);
      if (list == null) {
        list = Lists.newArrayListWithExpectedSize(1);
        tree.put(weight, list);
      }
      list.add(node);
    }

    int idx = 0;
    for (List<Node> list: tree.values()) {
      if (list != null) {
        Collections.shuffle(list, r);
        for (Node n: list) {
          nodes[idx] = n;
          idx++;
        }
      }
    }
    Preconditions.checkState(idx == activeLen,
        "Sorted the wrong number of nodes!");
  }
}
