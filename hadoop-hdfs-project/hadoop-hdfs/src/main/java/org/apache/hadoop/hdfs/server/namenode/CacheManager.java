/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock.Type;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import io.hops.common.IDsGeneratorFactory;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.metadata.hdfs.dal.CachePoolDataAccess;
import io.hops.metadata.hdfs.dal.CachedBlockDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.permission.FsPermission;
import static io.hops.transaction.lock.LockFactory.getInstance;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import static io.hops.transaction.lock.LockFactory.BLK;
import java.util.TreeMap;

/**
 * The Cache Manager handles caching on DataNodes.
 * <p>
 * This class is instantiated by the FSNamesystem.
 * It maintains the mapping of cached blocks to datanodes via processing
 * datanode cache reports. Based on these reports and addition and removal of
 * caching directives, we will schedule caching and uncaching work.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class CacheManager {

  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  private static final float MIN_CACHED_BLOCKS_PERCENT = 0.001f;

  // TODO: add pending / underCached / schedule cached blocks stats.
  /**
   * The FSNamesystem that contains this CacheManager.
   */
  private final FSNamesystem namesystem;

  /**
   * The BlockManager associated with the FSN that owns this CacheManager.
   */
  private final BlockManager blockManager;

  /**
   * Maximum number of cache pools to list in one operation.
   */
  private final int maxListCachePoolsResponses;

  /**
   * Maximum number of cache pool directives to list in one operation.
   */
  private final int maxListCacheDirectivesNumResponses;

  /**
   * Interval between scans in milliseconds.
   */
  private final long scanIntervalMs;

  /**
   * Lock which protects the CacheReplicationMonitor.
   */
  private final ReentrantLock crmLock = new ReentrantLock();

  /**
   * The CacheReplicationMonitor.
   */
  private CacheReplicationMonitor monitor;

  CacheManager(FSNamesystem namesystem, Configuration conf,
      BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.maxListCachePoolsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
    this.maxListCacheDirectivesNumResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT);
    scanIntervalMs = conf.getLong(
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS,
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT);
    float cachedBlocksPercent = conf.getFloat(
        DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT,
        DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT);
    if (cachedBlocksPercent < MIN_CACHED_BLOCKS_PERCENT) {
      LOG.info("Using minimum value " + MIN_CACHED_BLOCKS_PERCENT + " for "
          + DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT);
      cachedBlocksPercent = MIN_CACHED_BLOCKS_PERCENT;
    }

  }

  public void startMonitorThread() {
    crmLock.lock();
    try {
      if (this.monitor == null) {
        this.monitor = new CacheReplicationMonitor(namesystem, this,
            scanIntervalMs, crmLock);
        this.monitor.start();
      }
    } finally {
      crmLock.unlock();
    }
  }

  public void stopMonitorThread() {
    crmLock.lock();
    try {
      if (this.monitor != null) {
        CacheReplicationMonitor prevMonitor = this.monitor;
        this.monitor = null;
        IOUtils.closeQuietly(prevMonitor);
      }
    } finally {
      crmLock.unlock();
    }
  }

  //HOPS as we are distributed we may stop one NN without stopping all of them. In this case we should not clear
  //the information used by the other NN.
  //TODO: check if there is some case where we really need to do the clear.
//  public void clearDirectiveStats() {
//    assert namesystem.hasWriteLock();
//    for (CacheDirective directive : directivesById.values()) {
//      directive.resetStatistics();
//    }
//  }
  /**
   * @return Unmodifiable view of the collection of CachePools.
   */
  public Collection<CachePool> getCachePools() throws TransactionContextException, StorageException {
    return Collections.unmodifiableCollection(EntityManager.findList(CachePool.Finder.All));
  }

  /**
   * @return Unmodifiable view of the collection of CacheDirectives.
   */
  public Collection<CacheDirective> getCacheDirectives() throws TransactionContextException, StorageException {
    return Collections.unmodifiableCollection(EntityManager.findList(CacheDirective.Finder.All));
  }

  @VisibleForTesting
  public Set<CachedBlock> getCachedBlocks(final DatanodeManager datanodeManager) throws TransactionContextException,
      StorageException, IOException {
    Collection<CachedBlock> all
        = (Collection<CachedBlock>) new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
          locks.add(lf.getAllCachedBlockLocks());
      }

      @Override
      public Object performTask() throws IOException {
        return CachedBlock.getAll(datanodeManager);
      }
    }.handle();
    Set<CachedBlock> result = new HashSet();
    for (CachedBlock block : all) {
      if (block.isCached()) {
        result.add(block);
      }
    }
    return result;
  }

  public CachedBlock getCachedBlock(CachedBlock block) throws TransactionContextException, StorageException {
    Collection<io.hops.metadata.hdfs.entity.CachedBlock> dalCachedBlocks = EntityManager.findList(
        io.hops.metadata.hdfs.entity.CachedBlock.Finder.ByBlockIdAndInodeId, block.getBlockId(), block.getInodeId());
    if (dalCachedBlocks == null || dalCachedBlocks.isEmpty()) {
      return null;
    }
    CachedBlock cachedBlock = null;
    for (io.hops.metadata.hdfs.entity.CachedBlock dalBlock : dalCachedBlocks) {
      if(dalBlock.getStatus().equals("")){
        continue;
      }
      if (cachedBlock == null) {
        cachedBlock = new CachedBlock(dalBlock.getBlockId(), dalBlock.getInodeId(), dalBlock.getReplicationAndMark());
      }
      if (!CachedBlock.Type.valueOf(dalBlock.getStatus()).equals(CachedBlock.Type.INIT)) {
        cachedBlock.addDatanode(blockManager.getDatanodeManager().getDatanodeByUuid(dalBlock.getDatanodeId()), dalBlock.
            getStatus());
      }
    }
    return cachedBlock;
  }
  
  

  public long getNextDirectiveId() throws IOException {
    long nextDirectiveId = IDsGeneratorFactory.getInstance().getUniqueCacheDirectiveID();
    if (nextDirectiveId >= Long.MAX_VALUE - 1) {
      throw new IOException("No more available IDs.");
    }
    return nextDirectiveId;
  }

  // Helper getter / validation methods
  private static void checkWritePermission(FSPermissionChecker pc,
      CachePool pool) throws AccessControlException {
    if ((pc != null)) {
      pc.checkPermission(pool, FsAction.WRITE);
    }
  }

  public static String validatePoolName(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    String pool = directive.getPool();
    if (pool == null) {
      throw new InvalidRequestException("No pool specified.");
    }
    if (pool.isEmpty()) {
      throw new InvalidRequestException("Invalid empty pool name.");
    }
    return pool;
  }

  public static String validatePath(CacheDirectiveInfo directive)
      throws InvalidRequestException {
    if (directive.getPath() == null) {
      throw new InvalidRequestException("No path specified.");
    }
    String path = directive.getPath().toUri().getPath();
    if (!DFSUtil.isValidName(path)) {
      throw new InvalidRequestException("Invalid path '" + path + "'.");
    }
    return path;
  }

  private static short validateReplication(CacheDirectiveInfo directive,
      short defaultValue) throws InvalidRequestException {
    short repl = (directive.getReplication() != null)
        ? directive.getReplication() : defaultValue;
    if (repl <= 0) {
      throw new InvalidRequestException("Invalid replication factor " + repl
          + " <= 0");
    }
    return repl;
  }

  /**
   * Calculates the absolute expiry time of the directive from the
   * {@link CacheDirectiveInfo.Expiration}. This converts a relative Expiration
   * into an absolute time based on the local clock.
   *
   * @param info to validate.
   * @param maxRelativeExpiryTime of the info's pool.
   * @return the expiration time, or the pool's max absolute expiration if the
   * info's expiration was not set.
   * @throws InvalidRequestException if the info's Expiration is invalid.
   */
  private static long validateExpiryTime(CacheDirectiveInfo info,
      long maxRelativeExpiryTime) throws InvalidRequestException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Validating directive " + info
          + " pool maxRelativeExpiryTime " + maxRelativeExpiryTime);
    }
    final long now = new Date().getTime();
    final long maxAbsoluteExpiryTime = now + maxRelativeExpiryTime;
    if (info == null || info.getExpiration() == null) {
      return maxAbsoluteExpiryTime;
    }
    Expiration expiry = info.getExpiration();
    if (expiry.getMillis() < 0l) {
      throw new InvalidRequestException("Cannot set a negative expiration: "
          + expiry.getMillis());
    }
    long relExpiryTime, absExpiryTime;
    if (expiry.isRelative()) {
      relExpiryTime = expiry.getMillis();
      absExpiryTime = now + relExpiryTime;
    } else {
      absExpiryTime = expiry.getMillis();
      relExpiryTime = absExpiryTime - now;
    }
    // Need to cap the expiry so we don't overflow a long when doing math
    if (relExpiryTime > Expiration.MAX_RELATIVE_EXPIRY_MS) {
      throw new InvalidRequestException("Expiration "
          + expiry.toString() + " is too far in the future!");
    }
    // Fail if the requested expiry is greater than the max
    if (relExpiryTime > maxRelativeExpiryTime) {
      throw new InvalidRequestException("Expiration " + expiry.toString()
          + " exceeds the max relative expiration time of "
          + maxRelativeExpiryTime + " ms.");
    }
    return absExpiryTime;
  }

  /**
   * Throws an exception if the CachePool does not have enough capacity to
   * cache the given path at the replication factor.
   *
   * @param pool CachePool where the path is being cached
   * @param path Path that is being cached
   * @param replication Replication factor of the path
   * @throws InvalidRequestException if the pool does not have enough capacity
   */
  private void checkLimit(CachePool pool, String path,
      short replication) throws InvalidRequestException, StorageException, TransactionContextException {
    CacheDirectiveStats stats = computeNeeded(path, replication);
    if (pool.getLimit() == CachePoolInfo.LIMIT_UNLIMITED) {
      return;
    }
    if (pool.getBytesNeeded() + (stats.getBytesNeeded() * replication) > pool
        .getLimit()) {
      throw new InvalidRequestException("Caching path " + path + " of size "
          + stats.getBytesNeeded() / replication + " bytes at replication "
          + replication + " would exceed pool " + pool.getPoolName()
          + "'s remaining capacity of "
          + (pool.getLimit() - pool.getBytesNeeded()) + " bytes.");
    }
  }

  /**
   * Computes the needed number of bytes and files for a path.
   *
   * @return CacheDirectiveStats describing the needed stats for this path
   */
  private CacheDirectiveStats computeNeeded(String path, short replication) throws StorageException,
      TransactionContextException {
    FSDirectory fsDir = namesystem.getFSDirectory();
    INode node;
    long requestedBytes = 0;
    long requestedFiles = 0;
    CacheDirectiveStats.Builder builder = new CacheDirectiveStats.Builder();
    try {
      node = fsDir.getINode(path);
    } catch (UnresolvedLinkException e) {
      // We don't cache through symlinks
      return builder.build();
    }
    if (node == null) {
      return builder.build();
    }
    if (node.isFile()) {
      requestedFiles = 1;
      INodeFile file = node.asFile();
      requestedBytes = file.computeFileSize();
    } else if (node.isDirectory()) {
      INodeDirectory dir = node.asDirectory();
      List<INode> children = dir.getChildrenList();
      requestedFiles = children.size();
      for (INode child : children) {
        if (child.isFile()) {
          requestedBytes += child.asFile().computeFileSize();
        }
      }
    }
    return new CacheDirectiveStats.Builder()
        .setBytesNeeded(requestedBytes)
        .setFilesCached(requestedFiles)
        .build();
  }

  /**
   * Get a CacheDirective by ID, validating the ID and that the directive
   * exists.
   */
  private CacheDirective getById(long id) throws InvalidRequestException, TransactionContextException, StorageException {
    // Check for invalid IDs.
    if (id <= 0) {
      throw new InvalidRequestException("Invalid negative ID.");
    }
    // Find the directive.
    CacheDirective directive = EntityManager.find(CacheDirective.Finder.ById, id);
    if (directive == null) {
      throw new InvalidRequestException("No directive with ID " + id
          + " found.");
    }
    return directive;
  }

  /**
   * Get a CachePool by name, validating that it exists.
   */
  private CachePool getCachePool(String poolName)
      throws InvalidRequestException, TransactionContextException, StorageException {
    CachePool pool = EntityManager.find(CachePool.Finder.ByName, poolName);
    if (pool == null) {
      throw new InvalidRequestException("Unknown pool " + poolName);
    }
    return pool;
  }

  // RPC handlers
  private void addInternal(CacheDirective directive, CachePool pool) throws StorageException,
      TransactionContextException,
      IOException {
    directive.setPoolName(pool.getPoolName());

    // Fix up pool stats
    CacheDirectiveStats stats = computeNeeded(directive.getPath(), directive.getReplication());
    directive.addBytesNeeded(stats.getBytesNeeded());
    directive.addFilesNeeded(directive.getFilesNeeded());

    EntityManager.update(directive);

    setNeedsRescan();
  }

  public CacheDirectiveInfo addDirective(
      CacheDirectiveInfo info, FSPermissionChecker pc, EnumSet<CacheFlag> flags, long id)
      throws IOException {
    CacheDirective directive;
    try {
      CachePool pool = getCachePool(validatePoolName(info));
      checkWritePermission(pc, pool);
      String path = validatePath(info);
      short replication = validateReplication(info, (short) 1);
      long expiryTime = validateExpiryTime(info, pool.getMaxRelativeExpiryMs());
      // Do quota validation if required
      if (!flags.contains(CacheFlag.FORCE)) {
        checkLimit(pool, path, replication);
      }
      // All validation passed
      // Add a new entry with the next available ID.

      directive = new CacheDirective(id, path, replication, expiryTime);
      addInternal(directive, pool);
    } catch (IOException e) {
      LOG.warn("addDirective of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("addDirective of " + info + " successful.");
    return directive.toInfo();
  }

  /**
   * Factory method that makes a new CacheDirectiveInfo by applying fields in a
   * CacheDirectiveInfo to an existing CacheDirective.
   *
   * @param info with some or all fields set.
   * @param defaults directive providing default values for unset fields in
   * info.
   *
   * @return new CacheDirectiveInfo of the info applied to the defaults.
   */
  private static CacheDirectiveInfo createFromInfoAndDefaults(
      CacheDirectiveInfo info, CacheDirective defaults) {
    // Initialize the builder with the default values
    CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(defaults.toInfo());
    // Replace default with new value if present
    if (info.getPath() != null) {
      builder.setPath(info.getPath());
    }
    if (info.getReplication() != null) {
      builder.setReplication(info.getReplication());
    }
    if (info.getPool() != null) {
      builder.setPool(info.getPool());
    }
    if (info.getExpiration() != null) {
      builder.setExpiration(info.getExpiration());
    }
    return builder.build();
  }

  public void modifyDirective(CacheDirectiveInfo info,
      FSPermissionChecker pc, EnumSet<CacheFlag> flags) throws IOException {
    String idString = (info.getId() == null) ? "(null)" : info.getId().toString();
    try {
      // Check for invalid IDs.
      Long id = info.getId();
      if (id == null) {
        throw new InvalidRequestException("Must supply an ID.");
      }
      CacheDirective prevEntry = getById(id);
      checkWritePermission(pc, prevEntry.getPool());

      // Fill in defaults
      CacheDirectiveInfo infoWithDefaults = createFromInfoAndDefaults(info, prevEntry);
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(infoWithDefaults);

      // Do validation
      validatePath(infoWithDefaults);
      validateReplication(infoWithDefaults, (short) -1);
      // Need to test the pool being set here to avoid rejecting a modify for a
      // directive that's already been forced into a pool
      CachePool srcPool = prevEntry.getPool();
      CachePool destPool = getCachePool(validatePoolName(infoWithDefaults));
      if (!srcPool.getPoolName().equals(destPool.getPoolName())) {
        checkWritePermission(pc, destPool);
        if (!flags.contains(CacheFlag.FORCE)) {
          checkLimit(destPool, infoWithDefaults.getPath().toUri().getPath(),
              infoWithDefaults.getReplication());
        }
      }
      // Verify the expiration against the destination pool
      validateExpiryTime(infoWithDefaults, destPool.getMaxRelativeExpiryMs());

      // Indicate changes to the CRM
      setNeedsRescan();

      // Validation passed
      removeInternal(prevEntry);
      addInternal(new CacheDirective(builder.build()), destPool);
    } catch (IOException e) {
      LOG.warn("modifyDirective of " + idString + " failed: ", e);
      throw e;
    }
    LOG.info("modifyDirective of " + idString + " successfully applied " + info + ".");
  }

  private void removeInternal(CacheDirective directive)
      throws InvalidRequestException, StorageException, TransactionContextException, IOException {
    // Remove the corresponding entry in directivesByPath.
//    check that the path in db match the one in directive ??

    // Fix up the stats from removing the pool
    directive.addBytesNeeded(-directive.getBytesNeeded());
    directive.addFilesNeeded(-directive.getFilesNeeded());
    EntityManager.remove(directive);

    setNeedsRescan();
  }

  public void removeDirective(long id, FSPermissionChecker pc)
      throws IOException {
    try {
      CacheDirective directive = getById(id);
      checkWritePermission(pc, directive.getPool());
      removeInternal(directive);
    } catch (IOException e) {
      LOG.warn("removeDirective of " + id + " failed: ", e);
      throw e;
    }
    LOG.info("removeDirective of " + id + " successful.");
  }

  public BatchedListEntries<CacheDirectiveEntry>
      listCacheDirectives(final long prevId,
          final CacheDirectiveInfo filter,
          final FSPermissionChecker pc) throws IOException {
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    final String filterPath[] = new String[1];
    if (filter.getId() != null) {
      throw new IOException("Filtering by ID is unsupported.");
    }
    if (filter.getPath() != null) {
      filterPath[0] = validatePath(filter);
    }
    if (filter.getReplication() != null) {
      throw new IOException("Filtering by replication is unsupported.");
    }
    final ArrayList<CacheDirectiveEntry> replies = new ArrayList<CacheDirectiveEntry>(NUM_PRE_ALLOCATED_ENTRIES);

    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.LIST_CACHE_DIRECTIVE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getCacheDirectiveLock(prevId + 1, filterPath[0], filter.getPool(), maxListCacheDirectivesNumResponses+1)).
            add(lf.getCachePoolLock(TransactionLockTypes.LockType.READ));
      }

      @Override
      public Object performTask() throws IOException {

        int numReplies = 0;
        Map<Long, CacheDirective> tailMap = getDirectivesByIdPathAndPool(prevId + 1, filterPath[0], filter.getPool(), maxListCacheDirectivesNumResponses+1);
        for (Entry<Long, CacheDirective> cur : tailMap.entrySet()) {
          if (numReplies >= maxListCacheDirectivesNumResponses) {
            return new BatchedListEntries<CacheDirectiveEntry>(replies, true);
          }
          CacheDirective curDirective = cur.getValue();
          CacheDirectiveInfo info = cur.getValue().toInfo();
          if (filter.getPool() != null && !info.getPool().equals(filter.getPool())) {
            continue;
          }
          if (filterPath[0] != null && !info.getPath().toUri().getPath().equals(filterPath[0])) {
            continue;
          }
          boolean hasPermission = true;
          if (pc != null) {
            try {
              pc.checkPermission(curDirective.getPool(), FsAction.READ);
            } catch (AccessControlException e) {
              hasPermission = false;
            }
          }
          if (hasPermission) {
            replies.add(new CacheDirectiveEntry(info, cur.getValue().toStats()));
            numReplies++;
          }
        }
        return new BatchedListEntries<CacheDirectiveEntry>(replies, false);
      }
    };
    return (BatchedListEntries<CacheDirectiveEntry>) handler.handle();
  }

  private Map<Long, CacheDirective> getDirectivesByIdPathAndPool(final long id, final String path, final String pool, int maxNumberResults)
      throws IOException {
    List<CacheDirective> directives = (List<CacheDirective>) EntityManager.findList(
        CacheDirective.Finder.ByIdPoolAndPath, id, pool, path, maxNumberResults);

    Map<Long, CacheDirective> result = new TreeMap<>();
    for (CacheDirective directive : directives) {
      result.put(directive.getId(), directive);
    }
    return result;
  }

  /**
   * Create a cache pool.
   * <p>
   * Only the superuser should be able to call this function.
   *
   * @param info The info for the cache pool to create.
   * @return Information about the cache pool we created.
   */
  public CachePoolInfo addCachePool(CachePoolInfo info)
      throws IOException {
    CachePool pool;
    try {
      CachePoolInfo.validate(info);
      String poolName = info.getPoolName();
      pool = EntityManager.find(CachePool.Finder.ByName, poolName);
      if (pool != null) {
        throw new InvalidRequestException("Cache pool " + poolName
            + " already exists.");
      }
      pool = CachePool.createFromInfoAndDefaults(info);
      EntityManager.update(pool);
    } catch (IOException e) {
      LOG.info("addCachePool of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("addCachePool of " + info + " successful.");
    return pool.getInfo(true);
  }

  /**
   * Modify a cache pool.
   * <p>
   * Only the superuser should be able to call this function.
   *
   * @param info
   * The info for the cache pool to modify.
   */
  public void modifyCachePool(CachePoolInfo info)
      throws IOException {
    StringBuilder bld = new StringBuilder();
    try {
      CachePoolInfo.validate(info);
      String poolName = info.getPoolName();
      CachePool pool = EntityManager.find(CachePool.Finder.ByName, poolName);
      if (pool == null) {
        throw new InvalidRequestException("Cache pool " + poolName
            + " does not exist.");
      }
      String prefix = "";
      if (info.getOwnerName() != null) {
        pool.setOwnerName(info.getOwnerName());
        bld.append(prefix).
            append("set owner to ").append(info.getOwnerName());
        prefix = "; ";
      }
      if (info.getGroupName() != null) {
        pool.setGroupName(info.getGroupName());
        bld.append(prefix).
            append("set group to ").append(info.getGroupName());
        prefix = "; ";
      }
      if (info.getMode() != null) {
        pool.setMode(info.getMode());
        bld.append(prefix).append("set mode to " + info.getMode());
        prefix = "; ";
      }
      if (info.getLimit() != null) {
        pool.setLimit(info.getLimit());
        bld.append(prefix).append("set limit to " + info.getLimit());
        prefix = "; ";
        // New limit changes stats, need to set needs refresh
        setNeedsRescan();
      }
      if (info.getMaxRelativeExpiryMs() != null) {
        final Long maxRelativeExpiry = info.getMaxRelativeExpiryMs();
        pool.setMaxRelativeExpiryMs(maxRelativeExpiry);
        bld.append(prefix).append("set maxRelativeExpiry to "
            + maxRelativeExpiry);
        prefix = "; ";
      }
      if (prefix.isEmpty()) {
        bld.append("no changes.");
      }
      EntityManager.update(pool);
    } catch (IOException e) {
      LOG.info("modifyCachePool of " + info + " failed: ", e);
      throw e;
    }
    LOG.info("modifyCachePool of " + info.getPoolName() + " successful; "
        + bld.toString());
  }

  /**
   * Remove a cache pool.
   * <p>
   * Only the superuser should be able to call this function.
   *
   * @param poolName
   * The name for the cache pool to remove.
   */
  public void removeCachePool(String poolName)
      throws IOException {
    try {
      CachePoolInfo.validateName(poolName);
      CachePool pool = EntityManager.find(CachePool.Finder.ByName, poolName);
      if (pool == null) {
        throw new InvalidRequestException(
            "Cannot remove non-existent cache pool " + poolName);
      }
      EntityManager.remove(pool);
      // Remove all directives in this pool.
      Iterator<CacheDirective> iter = pool.getDirectiveList().iterator();
      while (iter.hasNext()) {
        CacheDirective directive = iter.next();
        EntityManager.remove(directive);
        iter.remove();
      }
      setNeedsRescan();
    } catch (IOException e) {
      LOG.info("removeCachePool of " + poolName + " failed: ", e);
      throw e;
    }
    LOG.info("removeCachePool of " + poolName + " successful.");
  }

  public BatchedListEntries<CachePoolEntry>
      listCachePools(FSPermissionChecker pc, String prevKey) throws IOException {
    final int NUM_PRE_ALLOCATED_ENTRIES = 16;
    ArrayList<CachePoolEntry> results = new ArrayList<>(NUM_PRE_ALLOCATED_ENTRIES);
    Map<String, CachePool> tailMap = getCachePoolMap(prevKey);
    int numListed = 0;
    for (Entry<String, CachePool> cur : tailMap.entrySet()) {
      if (numListed++ >= maxListCachePoolsResponses) {
        return new BatchedListEntries<>(results, true);
      }
      results.add(cur.getValue().getEntry(pc));
    }
    return new BatchedListEntries<>(results, false);
  }

  private Map<String, CachePool> getCachePoolMap(final String poolName) throws IOException {
    Collection<CachePool> cachePools = (Collection<CachePool>) new LightWeightRequestHandler(
            HDFSOperationType.LIST_CACHE_POOL) {
      @Override
      public Object performTask() throws IOException {
        CachePoolDataAccess<io.hops.metadata.hdfs.entity.CachePool> da
            = (CachePoolDataAccess) HdfsStorageFactory.getDataAccess(CachePoolDataAccess.class);
        return da.findAboveName(poolName);

      }
    }.handle();
    Map<String, CachePool> result = new TreeMap<>();
    for (CachePool dalPool : cachePools) {
      result.put(dalPool.getPoolName(), new CachePool(dalPool.getPoolName(), dalPool.getOwnerName(), dalPool.
          getGroupName(), new FsPermission(dalPool.getMode()), dalPool.getLimit(), dalPool.getMaxRelativeExpiryMs(),
          dalPool.getBytesNeeded(),
          dalPool.getBytesCached(), dalPool.getFilesNeeded(), dalPool.getFilesCached()));
    }
    return result;
  }

  public void setCachedLocations(LocatedBlock block, int inodeId) throws TransactionContextException, StorageException {
    CachedBlock cachedBlock = new CachedBlock(block.getBlock().getBlockId(), inodeId, (short) 0, false);
    cachedBlock = getCachedBlock(cachedBlock);
    if (cachedBlock == null) {
      return;
    }
    List<DatanodeDescriptor> datanodes = cachedBlock.getDatanodes(Type.CACHED);
    for (DatanodeDescriptor datanode : datanodes) {
      block.addCachedLoc(datanode);
    }
  }

  public final void processCacheReport(final DatanodeID datanodeID,
      final List<Long> blockIds, final long cacheCapacity, final long cacheUsed) throws IOException {

    final long startTime = Time.monotonicNow();
    final long endTime;
    new HopsTransactionalRequestHandler(HDFSOperationType.PROCESS_CACHED_BLOCKS_REPORT) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getCachedBlockReportingLocks(blockIds, datanodeID)); 
      }

      @Override
      public Object performTask() throws IOException {

        final DatanodeDescriptor datanode = blockManager.getDatanodeManager().getDatanode(datanodeID);
        if (datanode == null || !datanode.isAlive) {
          throw new IOException(
              "processCacheReport from dead or unregistered datanode: " + datanode);
        }
        datanode.setCacheCapacity(cacheCapacity);
        datanode.setCacheUsed(cacheUsed);
        processCacheReportImpl(datanode, blockIds);
        return null;
      }
    }.handle();
    endTime = Time.monotonicNow();
    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addCacheBlockReport((int) (endTime - startTime));
    }
    LOG.info("Processed cache report from "
        + datanodeID + ", blocks: " + blockIds.size()
        + ", processing time: " + (endTime - startTime) + " msecs");
  }

  private void processCacheReportImpl(final DatanodeDescriptor datanode,
      final List<Long> blockIds) throws TransactionContextException, StorageException {
    Collection<CachedBlock> oldCached = datanode.getCached(blockManager.getDatanodeManager());
    Set<CachedBlock> pendingUncache = new HashSet<>(datanode.getPendingUncached(blockManager.getDatanodeManager()));

    Set<CachedBlock> cached = new HashSet<>();
    for (Iterator<Long> iter = blockIds.iterator(); iter.hasNext();) {
      long blockId = iter.next();
      INodeIdentifier inode = INodeUtil.resolveINodeFromBlockID(blockId);
      int inodeId = -1;
      if(inode!=null){
        inodeId=inode.getInodeId();
      }
      CachedBlock cachedBlock = new CachedBlock(blockId, inodeId,
          (short) 0, false, datanode, Type.CACHED);
      CachedBlock prevCachedBlock = getCachedBlock(cachedBlock);
      // Add the block ID from the cache report to the cachedBlocks map
      // if it's not already there.
      if (prevCachedBlock != null) {
        cachedBlock = prevCachedBlock;
        cachedBlock.switchPendingCachedToCached(datanode);
      }
      cached.add(cachedBlock);
      if(pendingUncache.contains(cachedBlock)){
        //do not overwrite pending_uncached with cached
        continue;
      }
      cachedBlock.save();
    }

    //remove block that are not cached on the node anymore
    for (CachedBlock block : oldCached) {
      if (!cached.contains(block)) {
        block.remove(datanode);
      }
    }
    for (CachedBlock block : pendingUncache) {
      if (!cached.contains(block)) {
        block.remove(datanode);
      }
    }
  }

  public void waitForRescanIfNeeded() throws StorageException, TransactionContextException, IOException {
    crmLock.lock();
    try {
      if (monitor != null) {
        monitor.waitForRescanIfNeeded();
      }
    } finally {
      crmLock.unlock();
    }
  }

  private void setNeedsRescan() throws StorageException, TransactionContextException, IOException {
    crmLock.lock();
    try {
      if (monitor != null) {
        monitor.setNeedsRescan();
      }
    } finally {
      crmLock.unlock();
    }
  }

  @VisibleForTesting
  public Thread getCacheReplicationMonitor() {
    crmLock.lock();
    try {
      return monitor;
    } finally {
      crmLock.unlock();
    }
  }
}
