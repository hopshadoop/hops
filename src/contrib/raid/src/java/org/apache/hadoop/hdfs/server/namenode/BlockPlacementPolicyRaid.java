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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Comparator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.util.StringUtils;

/**
 * This BlockPlacementPolicy spreads out the group of blocks which used by RAID
 * for recovering each other. This is important for the availability
 * of the blocks. This class can be used by multiple threads. It has to be
 * thread safe.
 */
public class BlockPlacementPolicyRaid extends BlockPlacementPolicy {
  public static final Log LOG =
    LogFactory.getLog(BlockPlacementPolicyRaid.class);
  Configuration conf;
  private int stripeLength;
  private int xorParityLength;
  private int rsParityLength;
  private String xorPrefix = null;
  private String rsPrefix = null;
  private String raidTempPrefix = null;
  private String raidrsTempPrefix = null;
  private String raidHarTempPrefix = null;
  private String raidrsHarTempPrefix = null;
  private FSNamesystem namesystem = null;
  private BlockPlacementPolicyDefault defaultPolicy;

  CachedLocatedBlocks cachedLocatedBlocks;
  CachedFullPathNames cachedFullPathNames;

  /** {@inheritDoc} */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap) {
    this.conf = conf;
    this.stripeLength = RaidNode.getStripeLength(conf);
    this.rsParityLength = RaidNode.rsParityLength(conf);
    this.xorParityLength = 1;
    try {
      this.xorPrefix = RaidNode.xorDestinationPath(conf).toUri().getPath();
      this.rsPrefix = RaidNode.rsDestinationPath(conf).toUri().getPath();
    } catch (IOException e) {
    }
    if (this.xorPrefix == null) {
      this.xorPrefix = RaidNode.DEFAULT_RAID_LOCATION;
    }
    if (this.rsPrefix == null) {
      this.rsPrefix = RaidNode.DEFAULT_RAIDRS_LOCATION;
    }
    // Throws ClassCastException if we cannot cast here.
    this.namesystem = (FSNamesystem) stats;
    this.cachedLocatedBlocks = new CachedLocatedBlocks(namesystem);
    this.cachedFullPathNames = new CachedFullPathNames(namesystem);
    this.raidTempPrefix = RaidNode.xorTempPrefix(conf);
    this.raidrsTempPrefix = RaidNode.rsTempPrefix(conf);
    this.raidHarTempPrefix = RaidNode.xorHarTempPrefix(conf);
    this.raidrsHarTempPrefix = RaidNode.rsHarTempPrefix(conf);
    defaultPolicy = new BlockPlacementPolicyDefault(conf, stats, clusterMap);
  }

  @Override
  DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    HashMap<Node, Node> excluded = new HashMap<Node, Node>();
    return chooseTarget(srcPath, numOfReplicas, writer, chosenNodes,
        excluded, blocksize);
  }

  @Override
  DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      HashMap<Node, Node> excludedNodes, long blocksize) {
    try {
      if (excludedNodes == null) {
        excludedNodes = new HashMap<Node, Node>();
      }
      addExcludedNodes(srcPath, excludedNodes);
      DatanodeDescriptor[] result =
        defaultPolicy.chooseTarget(numOfReplicas, writer,
          chosenNodes, excludedNodes, blocksize);
      cachedLocatedBlocks.get(srcPath).
          add(new LocatedBlock(new Block(), result));
      return result;
    } catch (Exception e) {
      String trace = StringUtils.stringifyException(e);
      System.out.println(trace);
      FSNamesystem.LOG.debug(
        "Error happend when choosing datanode to write.", e);
      return defaultPolicy.chooseTarget(srcPath, numOfReplicas, writer,
                                chosenNodes, blocksize);
    }
  }

  @Override
  public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk,
      int minRacks) {
    return defaultPolicy.verifyBlockPlacement(srcPath, lBlk, minRacks);
  }

  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
      Block block, short replicationFactor,
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {

    DatanodeDescriptor chosenNode = null;
    try {
      String path = cachedFullPathNames.get(inode);
      List<LocatedBlock> companionBlocks = getCompanionBlocks(path, block);
      if (companionBlocks == null || companionBlocks.size() == 0) {
        // Use the default method if it is not a valid raided or parity file
        return defaultPolicy.chooseReplicaToDelete(
            inode, block, replicationFactor, first, second);
      }
      // Delete from the first collection first
      // This ensures the number of unique rack of this block is not reduced
      Collection<DatanodeDescriptor> all = new HashSet<DatanodeDescriptor>();
      all.addAll(first);
      all.addAll(second);
      chosenNode = chooseReplicaToDelete(companionBlocks, all);
      if (chosenNode != null) {
        return chosenNode;
      }
      return defaultPolicy.chooseReplicaToDelete(
          inode, block, replicationFactor, first, second);
    } catch (Exception e) {
      LOG.debug("Failed to choose the correct replica to delete", e);
      return defaultPolicy.chooseReplicaToDelete(
          inode, block, replicationFactor, first, second);
    }
  }

  /**
   * Obtain the excluded nodes for the current block that is being written
   */
  void addExcludedNodes(String file, HashMap<Node, Node> excluded)
      throws IOException {
    for (LocatedBlock b : getCompanionBlocks(file)) {
      for (Node n : b.getLocations()) {
        excluded.put(n, n);
      }
    }
  }

  private DatanodeDescriptor chooseReplicaToDelete(
      Collection<LocatedBlock> companionBlocks,
      Collection<DatanodeDescriptor> dataNodes) throws IOException {

    if (dataNodes.isEmpty()) {
      return null;
    }
    // Count the number of replicas on each node and rack
    final Map<String, Integer> nodeCompanionBlockCount =
      countCompanionBlocks(companionBlocks, false);
    final Map<String, Integer> rackCompanionBlockCount =
      countCompanionBlocks(companionBlocks, true);

    NodeComparator comparator =
      new NodeComparator(nodeCompanionBlockCount, rackCompanionBlockCount);
    return Collections.max(dataNodes, comparator);
  }

  /**
   * Count how many companion blocks are on each datanode or the each rack
   * @param companionBlocks a collection of all the companion blocks
   * @param doRackCount count the companion blocks on the racks of datanodes
   * @param result the map from node name to the number of companion blocks
   */
  static Map<String, Integer> countCompanionBlocks(
      Collection<LocatedBlock> companionBlocks, boolean doRackCount) {
    Map<String, Integer> result = new HashMap<String, Integer>();
    for (LocatedBlock block : companionBlocks) {
      for (DatanodeInfo d : block.getLocations()) {
        String name = doRackCount ? d.getParent().getName() : d.getName();
        if (result.containsKey(name)) {
          int count = result.get(name) + 1;
          result.put(name, count);
        } else {
          result.put(name, 1);
        }
      }
    }
    return result;
  }

  /**
   * Compares the datanodes based on the number of companion blocks on the same
   * node and rack. If even, compare the remaining space on the datanodes.
   */
  class NodeComparator implements Comparator<DatanodeDescriptor> {
    private Map<String, Integer> nodeBlockCount;
    private Map<String, Integer> rackBlockCount;
    private NodeComparator(Map<String, Integer> nodeBlockCount,
                           Map<String, Integer> rackBlockCount) {
      this.nodeBlockCount = nodeBlockCount;
      this.rackBlockCount = rackBlockCount;
    }
    @Override
    public int compare(DatanodeDescriptor d1, DatanodeDescriptor d2) {
      int res = compareBlockCount(d1, d2, nodeBlockCount);
      if (res != 0) {
        return res;
      }
      res = compareBlockCount(d1.getParent(), d2.getParent(), rackBlockCount);
      if (res != 0) {
        return res;
      }
      if (d1.getRemaining() > d2.getRemaining()) {
        return -1;
      }
      if (d1.getRemaining() < d2.getRemaining()) {
        return 1;
      }
      return 0;
    }
    private int compareBlockCount(Node node1, Node node2,
                                  Map<String, Integer> blockCount) {
      Integer count1 = blockCount.get(node1.getName());
      Integer count2 = blockCount.get(node2.getName());
      count1 = count1 == null ? 0 : count1;
      count2 = count2 == null ? 0 : count2;
      if (count1 > count2) {
        return 1;
      }
      if (count1 < count2) {
        return -1;
      }
      return 0;
    }
  }

  /**
   * Obtain the companion blocks of the block that is currently being written.
   * Companion blocks are defined as the blocks that can help recover each
   * others by using raid decoder.
   * @param path the path of the file contains the block
   * @return the block locations of companion blocks
   */
  List<LocatedBlock> getCompanionBlocks(String path)
      throws IOException {
    // This will be the index of the block which is currently being written
    int blockIndex = cachedLocatedBlocks.get(path).size();
    return getCompanionBlocks(path, blockIndex);
  }

  /**
   * Obtain the companion blocks of the give block
   * Companion blocks are defined as the blocks that can help recover each
   * others by using raid decoder.
   * @param path the path of the file contains the block
   * @param block the given block
   * @return the block locations of companion blocks
   */
  List<LocatedBlock> getCompanionBlocks(String path, Block block)
      throws IOException {
    int blockIndex = getBlockIndex(path, block);
    return getCompanionBlocks(path, blockIndex);
  }

  List<LocatedBlock> getCompanionBlocks(String path, int blockIndex)
      throws IOException {
    if (isXorHarTempParityFile(path)) {
      // temp har xor parity file
      return getCompanionBlocksForHarParityBlock(
          path, xorParityLength, blockIndex);
    }
    if (isRsHarTempParityFile(path)) {
      // temp har rs parity file
      return getCompanionBlocksForHarParityBlock(
          path, rsParityLength, blockIndex);
    }
    if (isXorTempParityFile(path)) {
      // temp xor parity file
      return getCompanionBlocksForParityBlock(
          getSourceFile(path, raidTempPrefix), path,
          xorParityLength, blockIndex);
    }
    if (isRsTempParityFile(path)) {
      // temp rs parity file
      return getCompanionBlocksForParityBlock(
          getSourceFile(path, raidrsTempPrefix), path,
          rsParityLength, blockIndex);
    }
    if (isXorParityFile(path)) {
      // xor parity file
      return getCompanionBlocksForParityBlock(getSourceFile(path, xorPrefix),
          path, xorParityLength, blockIndex);
    }
    if (isRsParityFile(path)) {
      // rs parity file
      return getCompanionBlocksForParityBlock(getSourceFile(path, rsPrefix),
          path, rsParityLength, blockIndex);
    }
    String parity = getParityFile(path);
    if (parity == null) {
      // corresponding parity file not found.
      // return an empty list
      return new ArrayList<LocatedBlock>();
    }
    if (isXorParityFile(parity)) {
      // xor raided source file
      return getCompanionBlocksForSourceBlock(
          path, parity, xorParityLength, blockIndex);
    }
    if (isRsParityFile(parity)) {
      // rs raided source file
      return getCompanionBlocksForSourceBlock(
          path, parity, rsParityLength, blockIndex);
    }
    // return an empty list
    return new ArrayList<LocatedBlock>();
  }

  private List<LocatedBlock> getCompanionBlocksForHarParityBlock(
      String parity, int parityLength, int blockIndex)
      throws IOException {
    // consider only parity file in this case because source file block
    // location is not easy to obtain
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    synchronized (parityBlocks) {
      int start = Math.max(0, blockIndex - parityLength + 1);
      int end = Math.min(parityBlocks.size(), blockIndex + parityLength);
      result = parityBlocks.subList(start, end);
    }
    return result;
  }

  private List<LocatedBlock> getCompanionBlocksForParityBlock(
      String src, String parity, int parityLength, int blockIndex)
      throws IOException {
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    int stripeIndex = blockIndex / parityLength;
    synchronized (parityBlocks) {
      int parityStart = stripeIndex * parityLength;
      int parityEnd = Math.min(parityStart + parityLength,
                               parityBlocks.size());
      // for parity, always consider the neighbor blocks as companion blocks
      if (parityStart < parityBlocks.size()) {
        result.addAll(parityBlocks.subList(parityStart, parityEnd));
      }
    }

    if (src == null) {
      return result;
    }
    List<LocatedBlock> sourceBlocks = cachedLocatedBlocks.get(src);
    synchronized (sourceBlocks) {
      int sourceStart = stripeIndex * stripeLength;
      int sourceEnd = Math.min(sourceStart + stripeLength,
                               sourceBlocks.size());
      if (sourceStart < sourceBlocks.size()) {
        result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
      }
    }
    return result;
  }

  private List<LocatedBlock> getCompanionBlocksForSourceBlock(
      String src, String parity, int parityLength, int blockIndex)
      throws IOException {
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> sourceBlocks = cachedLocatedBlocks.get(src);
    int stripeIndex = blockIndex / stripeLength;
    synchronized (sourceBlocks) {
      int sourceStart = stripeIndex * stripeLength;
      int sourceEnd = Math.min(sourceStart + stripeLength,
                               sourceBlocks.size());
      if (sourceStart < sourceBlocks.size()) {
        result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
      }
    }
    if (parity == null) {
      return result;
    }
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    synchronized (parityBlocks) {
      int parityStart = stripeIndex * parityLength;
      int parityEnd = Math.min(parityStart + parityLength,
                               parityBlocks.size());
      if (parityStart < parityBlocks.size()) {
        result.addAll(parityBlocks.subList(parityStart, parityEnd));
      }
    }
    return result;
  }

  private int getBlockIndex(String file, Block block) throws IOException {
    List<LocatedBlock> blocks = cachedLocatedBlocks.get(file);
    synchronized (blocks) {
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().equals(block)) {
          return i;
        }
      }
    }
    throw new IOException("Cannot locate " + block + " in file " + file);
  }

  /**
   * Cache results for FSInodeInfo.getFullPathName()
   */
  static class CachedFullPathNames {
    FSNamesystem namesystem;
    CachedFullPathNames(FSNamesystem namesystem) {
      this.namesystem = namesystem;
    }
    private Cache<INodeWithHashCode, String> cacheInternal =
      new Cache<INodeWithHashCode, String>() {
        @Override
        public String getDirectly(INodeWithHashCode inode) throws IOException {
          namesystem.readLock();
          try {
            return inode.getFullPathName();
          } finally {
            namesystem.readUnlock();
          }
        }
      };

    static private class INodeWithHashCode {
      FSInodeInfo inode;
      INodeWithHashCode(FSInodeInfo inode) {
        this.inode = inode;
      }
      @Override
      public boolean equals(Object obj) {
        return inode == obj;
      }
      @Override
      public int hashCode() {
        return System.identityHashCode(inode);
      }
      String getFullPathName() {
        return inode.getFullPathName();
      }
    }

    public String get(FSInodeInfo inode) throws IOException {
      return cacheInternal.get(new INodeWithHashCode(inode));
    }
  }

  /**
   * Cache results for FSNamesystem.getBlockLocations()
   */
  static class CachedLocatedBlocks extends Cache<String, List<LocatedBlock>> {
    FSNamesystem namesystem;
    CachedLocatedBlocks(FSNamesystem namesystem) {
      this.namesystem = namesystem;
    }
    @Override
    public List<LocatedBlock> getDirectly(String file) throws IOException {
      long len = namesystem.getFileInfo(file, true).getLen();
      List<LocatedBlock> result = namesystem.getBlockLocations(
          file, 0L, len, false, false).getLocatedBlocks();
      if (result == null || result.isEmpty()) {
        result = new ArrayList<LocatedBlock>();
      }
      return Collections.synchronizedList(result);
    }
  }

  static abstract class Cache<K, V> {
    private Map<K, ValueWithTime> cache;
    private static final long CACHE_TIMEOUT = 300000L; // 5 minutes
    // The timeout is long but the consequence of stale value is not serious
    Cache() {
      Map<K, ValueWithTime> map = new LinkedHashMap<K, ValueWithTime>() {
        private static final long serialVersionUID = 1L;
          final private int MAX_ENTRIES = 50000;
          @Override
          protected boolean removeEldestEntry(
            Map.Entry<K, ValueWithTime> eldest) {
            return size() > MAX_ENTRIES;
          }
        };
      this.cache = Collections.synchronizedMap(map);
    }

    // Note that this method may hold FSNamesystem.readLock() and it may
    // be called inside FSNamesystem.writeLock(). If we make this method
    // synchronized, it will deadlock.
    abstract protected V getDirectly(K key) throws IOException;

    public V get(K key) throws IOException {
      // The method is not synchronized so we may get some stale value here but
      // it's OK.
      ValueWithTime result = cache.get(key);
      long now = System.currentTimeMillis();
      if (result != null &&
          now - result.cachedTime < CACHE_TIMEOUT) {
        return result.value;
      }
      result = new ValueWithTime();
      result.value = getDirectly(key);
      result.cachedTime = now;
      cache.put(key, result);
      return result.value;
    }
    private class ValueWithTime {
      V value = null;
      long cachedTime = 0L;
    }
  }

  /**
   * Get path for the corresponding source file for a valid parity
   * file. Returns null if it does not exists
   * @param parity the toUri path of the parity file
   * @return the toUri path of the source file
   */
  String getSourceFile(String parity, String prefix) throws IOException {
    if (isHarFile(parity)) {
      return null;
    }
    // remove the prefix
    String src = parity.substring(prefix.length());
    if (namesystem.dir.getFileInfo(src, true) == null) {
      return null;
    }
    return src;
  }

  /**
   * Get path for the corresponding parity file for a source file.
   * Returns null if it does not exists
   * @param src the toUri path of the source file
   * @return the toUri path of the parity file
   */
  String getParityFile(String src) throws IOException {
    String xorParity = getParityFile(xorPrefix, src);
    if (xorParity != null) {
      return xorParity;
    }
    String rsParity = getParityFile(rsPrefix, src);
    if (rsParity != null) {
      return rsParity;
    }
    return null;
  }

  /**
   * Get path for the parity file. Returns null if it does not exists
   * @param parityPrefix usuall "/raid/" or "/raidrs/"
   * @return the toUri path of the parity file
   */
  private String getParityFile(String parityPrefix, String src)
      throws IOException {
    String parity = parityPrefix + src;
    if (namesystem.dir.getFileInfo(parity, true) == null) {
      return null;
    }
    return parity;
  }

  private boolean isHarFile(String path) {
    return path.lastIndexOf(RaidNode.HAR_SUFFIX) != -1;
  }

  private boolean isXorHarTempParityFile(String path) {
    return path.startsWith(raidHarTempPrefix + Path.SEPARATOR);
  }

  private boolean isRsHarTempParityFile(String path) {
    return path.startsWith(raidrsHarTempPrefix + Path.SEPARATOR);
  }

  private boolean isXorTempParityFile(String path) {
    return path.startsWith(raidTempPrefix + Path.SEPARATOR);
  }

  private boolean isRsTempParityFile(String path) {
    return path.startsWith(raidrsTempPrefix + Path.SEPARATOR);
  }

  private boolean isXorParityFile(String path) {
    return path.startsWith(xorPrefix + Path.SEPARATOR);
  }

  private boolean isRsParityFile(String path) {
    return path.startsWith(rsPrefix + Path.SEPARATOR);
  }


}
