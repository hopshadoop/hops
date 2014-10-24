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
package org.apache.hadoop.hdfs.server.blockmanagement;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock.Type;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import static io.hops.transaction.lock.LockFactory.BLK;
import static org.apache.hadoop.util.ExitUtil.terminate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Scans the namesystem, scheduling blocks to be cached as appropriate.
 * <p>
 * The CacheReplicationMonitor does a full scan when the NameNode first
 * starts up, and at configurable intervals afterwards.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class CacheReplicationMonitor extends Thread implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CacheReplicationMonitor.class);

  private final FSNamesystem namesystem;

  private final BlockManager blockManager;

  private final CacheManager cacheManager;

  /**
   * Pseudorandom number source
   */
  private static final Random random = new Random();

  /**
   * The interval at which we scan the namesystem for caching changes.
   */
  private final long intervalMs;

  /**
   * The CacheReplicationMonitor (CRM) lock. Used to synchronize starting and
   * waiting for rescan operations.
   */
  private final ReentrantLock lock;

  /**
   * Notifies the scan thread that an immediate rescan is needed.
   */
  private final Condition doRescan;

  /**
   * Notifies waiting threads that a rescan has finished.
   */
  private final Condition scanFinished;

  /**
   * True if this monitor should terminate. Protected by the CRM lock.
   */
  private boolean shutdown = false;

  /**
   * Mark status of the current scan.
   */
  private boolean mark = false;

  /**
   * Cache directives found in the previous scan.
   */
  private int scannedDirectives;

  /**
   * Blocks found in the previous scan.
   */
  private long scannedBlocks;

  public CacheReplicationMonitor(FSNamesystem namesystem,
      CacheManager cacheManager, long intervalMs, ReentrantLock lock) {
    this.namesystem = namesystem;
    this.blockManager = namesystem.getBlockManager();
    this.cacheManager = cacheManager;
    this.intervalMs = intervalMs;
    this.lock = lock;
    this.doRescan = this.lock.newCondition();
    this.scanFinished = this.lock.newCondition();
  }

  @Override
  public void run() {
    long startTimeMs = 0;
    Thread.currentThread().setName("CacheReplicationMonitor(" + System.identityHashCode(this) + ")");
    LOG.info("Starting CacheReplicationMonitor with interval " + intervalMs + " milliseconds");
    try {
      long curTimeMs = Time.monotonicNow();
      while (true) {
        lock.lock();
        try {
          while (true) {
            if (namesystem.isLeader()) {
              if (shutdown) {
                LOG.debug("Shutting down CacheReplicationMonitor");
                return;
              }
              if (HdfsVariables.getNeedRescan()) {
                LOG.debug("Rescanning because of pending operations");
                break;
              }
            }
            long delta = (startTimeMs + intervalMs) - curTimeMs;
            if (delta <= 0) {
              if (namesystem.isLeader()) {
                LOG.debug("Rescanning after " + (curTimeMs - startTimeMs) + " milliseconds");
                break;
              } else {
                startTimeMs = curTimeMs;
                delta = (startTimeMs + intervalMs) - curTimeMs;
              }
            }
            doRescan.await(delta, TimeUnit.MILLISECONDS);
            curTimeMs = Time.monotonicNow();
          }
        } finally {
          lock.unlock();
        }
        startTimeMs = curTimeMs;
        mark = !mark;
        rescan();
        curTimeMs = Time.monotonicNow();
        // Update synchronization-related variables.
        lock.lock();
        try {
          HdfsVariables.setCompletedAndCurScanCount();
          scanFinished.signalAll();
        } finally {
          lock.unlock();
        }
        LOG.debug("Scanned " + scannedDirectives + " directive(s) and " + scannedBlocks + " block(s) in " + (curTimeMs
            - startTimeMs) + " " + "millisecond(s).");
      }
    } catch (InterruptedException e) {
      LOG.info("Shutting down CacheReplicationMonitor.");
      return;
    } catch (Throwable t) {
      LOG.error("Thread exiting", t);
      terminate(1, t);
    }
  }

  /**
   * Waits for a rescan to complete. This doesn't guarantee consistency with
   * pending operations, only relative recency, since it will not force a new
   * rescan if a rescan is already underway.
   * <p>
   * Note that this call will release the FSN lock, so operations before and
   * after are not atomic.
   */
  public void waitForRescanIfNeeded() throws StorageException, TransactionContextException, IOException {
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when waiting for a rescan.");
    if (!HdfsVariables.getNeedRescan()) {
      return;
    }
    if (!namesystem.isLeader()) {
      throw new RuntimeException("Asked non leading node to rescan cache");
    }
    // If no scan is already ongoing, mark the CRM as dirty and kick
    if (HdfsVariables.getCurScanCount() < 0) {
      doRescan.signal();
    }
    // Wait until the scan finishes and the count advances
    while ((!shutdown) && (HdfsVariables.getNeedRescan())) {
      try {
        scanFinished.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for CacheReplicationMonitor"
            + " rescan", e);
        break;
      }
    }
  }

  /**
   * Indicates to the CacheReplicationMonitor that there have been CacheManager
   * changes that require a rescan.
   */
  public void setNeedsRescan() throws StorageException, TransactionContextException, IOException {
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when setting the needsRescan bit.");
    HdfsVariables.setNeedRescan();
  }

  /**
   * Shut down the monitor thread.
   */
  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      if (shutdown) {
        return;
      }
      // Since we hold both the FSN write lock and the CRM lock here,
      // we know that the CRM thread cannot be currently modifying
      // the cache manager state while we're closing it.
      // Since the CRM thread checks the value of 'shutdown' after waiting
      // for a lock, we know that the thread will not modify the cache
      // manager state after this point.
      shutdown = true;
      doRescan.signalAll();
      scanFinished.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void rescan() throws InterruptedException, StorageException, TransactionContextException, IOException {
    scannedDirectives = 0;
    scannedBlocks = 0;
    lock.lock();
    try{
      if (shutdown) {
        throw new InterruptedException("CacheReplicationMonitor was " +
            "shut down.");
      }
      HdfsVariables.setCurScanCount();
    }
    finally {
      lock.unlock();
    }
    new HopsTransactionalRequestHandler(HDFSOperationType.LIST_CACHE_DIRECTIVE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.//add(lf.getCacheDirectiveLock()).
            add(lf.getCachePoolLock(TransactionLockTypes.LockType.WRITE));
        //add(lf.getAllCachedBlockLocks());

      }

      @Override
      public Object performTask() throws IOException {
        resetStatistics();
        return null;
      }
    }.handle();

    rescanCacheDirectives();
    rescanCachedBlockMap();
    blockManager.getDatanodeManager().resetLastCachingDirectiveSentTime();
  }

  private void resetStatistics() throws TransactionContextException, StorageException {
    for (CachePool pool : cacheManager.getCachePools()) {
      pool.resetStatistics();
    }
    //contrary to Apache Hadoop we reset the cache directives in rescanCacheDirectives to not take
    //all the chached directives lock at once.
  }

  /**
   * Scan all CacheDirectives. Use the information to figure out
   * what cache replication factor each block should have.
   */
  private void rescanCacheDirectives() throws StorageException, TransactionContextException, IOException {
    final long now = new Date().getTime();
    Collection<CacheDirective> directives = getCacheDirectives();
    for (final CacheDirective tmpDirective : directives) {
      scannedDirectives++;
      new HopsTransactionalRequestHandler(HDFSOperationType.RESCAN_CACHE_DIRECTIVE) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          locks.add(lf.getCacheDirectiveLock(tmpDirective.getId())).
              add(lf.getCachePoolLock(tmpDirective.getPoolName()));
          INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ,
              TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, tmpDirective.getPath())
              .setNameNodeID(namesystem.getNamenodeId())
              .setActiveNameNodes(namesystem.getNameNode().getActiveNameNodes().getActiveNodes());
          locks.add(il).
              add(lf.getBlockLock()).
              add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.CA));
        }

        @Override
        public Object performTask() throws IOException {
          CacheDirective directive = EntityManager.find(CacheDirective.Finder.ById, tmpDirective.getId());
          if (directive == null) {
            //the directive has benn removed between the transaction that listed directives and this transaction
            return null;
          }
          directive.resetStatistics();
          // Skip processing this entry if it has expired
          if (directive.getExpiryTime() > 0 && directive.getExpiryTime() <= now) {
            LOG.debug("Directive {}: the directive expired at {} (now = {})",
              directive.getId(), directive.getExpiryTime(), now);
            return null;
          }
          final String path = directive.getPath();

          INode node = namesystem.getINode(directive.getPath());

          if (node == null) {
            LOG.debug("Directive {}: No inode found at {}", directive.getId(), path);
          } else if (node.isSymlink()) {
            // We don't cache through symlinks
            LOG.debug("Directive {}: got UnresolvedLinkException while resolving "
                + "path {}", directive.getId(), path
            );
            return null;
          } else if (node.isDirectory()) {
            INodeDirectory dir = node.asDirectory();
            List<INode> children = dir.getChildrenList();
            for (INode child : children) {
              if (child.isFile()) {
                rescanFile(directive, child.asFile());
              }
            }
          } else if (node.isFile()) {
            rescanFile(directive, node.asFile());
          } else {
            LOG.debug("Directive {}: ignoring non-directive, non-file inode {} ",
              directive.getId(), node);
          }
          return null;

        }
      }.handle();
    }
  }

  private Collection<CacheDirective> getCacheDirectives() throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.GET_INODE) {
      @Override
      public Object performTask() throws IOException {

        CacheDirectiveDataAccess da = (CacheDirectiveDataAccess) HdfsStorageFactory
            .getDataAccess(CacheDirectiveDataAccess.class);
        return da.findAll();
      }
    };
    return (Collection<CacheDirective>) handler.handle();
  }

  /**
   * Apply a CacheDirective to a file.
   *
   * @param directive The CacheDirective to apply.
   * @param file The file.
   */
  private void rescanFile(CacheDirective directive, INodeFile file) throws StorageException, TransactionContextException {
    BlockInfo[] blockInfos = file.getBlocks();

    // Increment the "needed" statistics
    directive.addFilesNeeded(1);
    // We don't cache UC blocks, don't add them to the total here
    long neededTotal = file.computeFileSizeNotIncludingLastUcBlock() * directive.getReplication();
    directive.addBytesNeeded(neededTotal);

    // The pool's bytesNeeded is incremented as we scan. If the demand
    // thus far plus the demand of this file would exceed the pool's limit,
    // do not cache this file.
    CachePool pool = directive.getPool();
    if (pool.getBytesNeeded() > pool.getLimit()) {
      LOG.debug("Directive {}: not scanning file {} because " +
          "bytesNeeded for pool {} is {}, but the pool's limit is {}",
          directive.getId(),
          file.getFullPathName(),
          pool.getPoolName(),
          pool.getBytesNeeded(),
          pool.getLimit());
      return;
    }

    long cachedTotal = 0;
    for (BlockInfo blockInfo : blockInfos) {
      if (!blockInfo.getBlockUCState().equals(BlockUCState.COMPLETE)) {
        // We don't try to cache blocks that are under construction.
        LOG.trace("Directive {}: can't cache block {} because it is in state "
                + "{}, not COMPLETE.", directive.getId(), blockInfo,
            blockInfo.getBlockUCState()
        );
        continue;
      }
      Block block = new Block(blockInfo.getBlockId());
      CachedBlock ncblock = new CachedBlock(block.getBlockId(), blockInfo.getInodeId(),
          directive.getReplication(), mark);
      CachedBlock ocblock = cacheManager.getCachedBlock(ncblock);
      if (ocblock == null) {
        ocblock = ncblock;
        ocblock.save();
      } else {
        // Update bytesUsed using the current replication levels.
        // Assumptions: we assume that all the blocks are the same length
        // on each datanode.  We can assume this because we're only caching
        // blocks in state COMPLETE.
        // Note that if two directives are caching the same block(s), they will
        // both get them added to their bytesCached.
        List<DatanodeDescriptor> cachedOn = ocblock.getDatanodes(Type.CACHED);
        long cachedByBlock = Math.min(cachedOn.size(),
            directive.getReplication()) * blockInfo.getNumBytes();
        cachedTotal += cachedByBlock;

        if ((mark != ocblock.getMark()) || (ocblock.getReplication() < directive.getReplication())) {
          //
          // Overwrite the block's replication and mark in two cases:
          //
          // 1. If the mark on the CachedBlock is different from the mark for
          // this scan, that means the block hasn't been updated during this
          // scan, and we should overwrite whatever is there, since it is no
          // longer valid.
          //
          // 2. If the replication in the CachedBlock is less than what the
          // directive asks for, we want to increase the block's replication
          // field to what the directive asks for.
          //
          ocblock.setReplicationAndMark(directive.getReplication(), mark);
          ocblock.save();
        }
      }
      LOG.trace("Directive {}: setting replication for block {} to {}",
          directive.getId(), blockInfo, ocblock.getReplication());
    }
    // Increment the "cached" statistics
    directive.addBytesCached(cachedTotal);
    if (cachedTotal == neededTotal) {
      directive.addFilesCached(1);
    }
    LOG.debug("Directive {}: caching {}: {}/{} bytes", directive.getId(),
        file.getFullPathName(), cachedTotal, neededTotal);
  }

  private String findReasonForNotCaching(CachedBlock cblock,
      BlockInfo blockInfo) throws TransactionContextException, StorageException {
    if (blockInfo == null) {
      // Somehow, a cache report with the block arrived, but the block
      // reports from the DataNode haven't (yet?) described such a block.
      // Alternately, the NameNode might have invalidated the block, but the
      // DataNode hasn't caught up.  In any case, we want to tell the DN
      // to uncache this.
      return "not tracked by the BlockManager";
    } else if (!blockInfo.isComplete()) {
      // When a cached block changes state from complete to some other state
      // on the DataNode (perhaps because of append), it will begin the
      // uncaching process.  However, the uncaching process is not
      // instantaneous, especially if clients have pinned the block.  So
      // there may be a period of time when incomplete blocks remain cached
      // on the DataNodes.
      return "not complete";
    } else if (cblock.getReplication() == 0) {
      // Since 0 is not a valid value for a cache directive's replication
      // field, seeing a replication of 0 on a CacheBlock means that it
      // has never been reached by any sweep.
      return "not needed by any directives";
    } else if (cblock.getMark() != mark) {
      // Although the block was needed in the past, we didn't reach it during
      // the current sweep.  Therefore, it doesn't need to be cached any more.
      // Need to set the replication to 0 so it doesn't flip back to cached
      // when the mark flips on the next scan
      cblock.setReplicationAndMark((short) 0, mark);
      cblock.save();
      return "no longer needed by any directives";
    }
    return null;
  }

  /**
   * Scan through the cached block map.
   * Any blocks which are under-replicated should be assigned new Datanodes.
   * Blocks that are over-replicated should be removed from Datanodes.
   */
  private void rescanCachedBlockMap() throws StorageException, TransactionContextException, IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RESCAN_BLOCK_MAP) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        
        LockFactory lf = LockFactory.getInstance();        
            locks.add(lf.getAllCachedBlockLocks());
            locks.add(lf.getBlockLock());
            locks.add(lf.getBlockRelated(BLK.RE, BLK.CR));
      }

      @Override
      public Object performTask() throws IOException {

        Collection<CachedBlock> cachedBlocks = CachedBlock.getAll(blockManager.getDatanodeManager());
        for (CachedBlock cblock : cachedBlocks) {
          scannedBlocks++;
          List<DatanodeDescriptor> pendingCached = cblock.getDatanodes(Type.PENDING_CACHED);
          List<DatanodeDescriptor> cached = cblock.getDatanodes(Type.CACHED);
          List<DatanodeDescriptor> pendingUncached = cblock.getDatanodes(Type.PENDING_UNCACHED);

          BlockInfo blockInfo = null;
          if(cblock.getInodeId()>0){
            blockInfo = blockManager.
              getStoredBlock(new Block(cblock.getBlockId()));
          }
          
          String reason = findReasonForNotCaching(cblock, blockInfo);
          int neededCached = 0;
          if (reason != null) {
            LOG.trace("Block {}: can't cache block because it is {}",
              cblock.getBlockId(), reason);
          } else {
            neededCached = cblock.getReplication();
          }
          int numCached = cached.size();
          if (numCached >= neededCached) {
            // If we have enough replicas, drop all pending cached.
            for (Iterator<DatanodeDescriptor> iter = pendingCached.iterator();
                iter.hasNext();) {
              DatanodeDescriptor datanode = iter.next();
              cblock.removePending(datanode);
              iter.remove();
              LOG.trace("Block {}: removing from PENDING_CACHED for node {}"
                  + "because we already have {} cached replicas and we only" + " need {}",
                  cblock.getBlockId(), datanode.getDatanodeUuid(), numCached,
                  neededCached
              );
            }
          }
          if (numCached < neededCached) {
            // If we don't have enough replicas, drop all pending uncached.
            for (Iterator<DatanodeDescriptor> iter = pendingUncached.iterator();
                iter.hasNext();) {
              DatanodeDescriptor datanode = iter.next();
              cblock.switchPendingUncachedToCached(datanode);
              iter.remove();
              LOG.trace("Block {}: removing from PENDING_UNCACHED for node {} "
                  + "because we only have {} cached replicas and we need " + "{}", cblock.getBlockId(), datanode.
                  getDatanodeUuid(),
                  numCached, neededCached
              );
            }
          }
          int neededUncached = numCached - (pendingUncached.size() + neededCached);
          if (neededUncached > 0) {
            addNewPendingUncached(neededUncached, cblock, cached,
                pendingUncached);
          } else {
            int additionalCachedNeeded = neededCached - (numCached + pendingCached.size());
            if (additionalCachedNeeded > 0) {
              addNewPendingCached(additionalCachedNeeded, cblock, cached,
                  pendingCached);
            }
          }
          if ((neededCached == 0) && pendingUncached.isEmpty() && pendingCached.isEmpty()) {
            // we have nothing more to do with this block.
            LOG.trace("Block {}: removing from cachedBlocks, since neededCached "
                + "== 0, and pendingUncached and pendingCached are empty.",
                cblock.getBlockId());
            cblock.remove();
          }
        }
        return null;
      }
    }.handle();
  }

  /**
   * Add new entries to the PendingUncached list.
   *
   * @param neededUncached The number of replicas that need to be uncached.
   * @param cachedBlock The block which needs to be uncached.
   * @param cached A list of DataNodes currently caching the block.
   * @param pendingUncached A list of DataNodes that will soon uncache the
   * block.
   */
  private void addNewPendingUncached(int neededUncached,
      CachedBlock cachedBlock, List<DatanodeDescriptor> cached,
      List<DatanodeDescriptor> pendingUncached) throws TransactionContextException, StorageException {
    // Figure out which replicas can be uncached.
    LinkedList<DatanodeDescriptor> possibilities = new LinkedList<DatanodeDescriptor>();
    for (DatanodeDescriptor datanode : cached) {
      if (!pendingUncached.contains(datanode)) {
        possibilities.add(datanode);
      }
    }
    while (neededUncached > 0) {
      if (possibilities.isEmpty()) {
        LOG.warn("Logic error: we're trying to uncache more replicas than " + "actually exist for " + cachedBlock);
        return;
      }
      DatanodeDescriptor datanode = possibilities.remove(random.nextInt(possibilities.size()));
      pendingUncached.add(datanode);
      boolean added = cachedBlock.setPendingUncached(datanode);
      assert added;
      neededUncached--;
    }
  }

  /**
   * Add new entries to the PendingCached list.
   *
   * @param neededCached The number of replicas that need to be cached.
   * @param cachedBlock The block which needs to be cached.
   * @param cached A list of DataNodes currently caching the block.
   * @param pendingCached A list of DataNodes that will soon cache the
   * block.
   */
  private void addNewPendingCached(final int neededCached,
      CachedBlock cachedBlock, List<DatanodeDescriptor> cached,
      List<DatanodeDescriptor> pendingCached) throws StorageException, TransactionContextException {
    // To figure out which replicas can be cached, we consult the
    // blocksMap.  We don't want to try to cache a corrupt replica, though.
    BlockInfo blockInfo = blockManager.
        getStoredBlock(new Block(cachedBlock.getBlockId()));
    if (blockInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block {}: can't add new cached replicas," + " because there is no record of this block "
            + "on the NameNode.", cachedBlock.getBlockId());
      }
      return;
    }
    if (!blockInfo.isComplete()) {
      LOG.debug("Block {}: can't cache this block, because it is not yet"
          + " complete.", cachedBlock.getBlockId());
      return;
    }
    // Filter the list of replicas to only the valid targets
    List<DatanodeDescriptor> possibilities = new LinkedList<DatanodeDescriptor>();
    DatanodeStorageInfo[] storages = blockInfo.getStorages(blockManager.getDatanodeManager());
    int numReplicas = storages.length;
    Collection<DatanodeDescriptor> corrupt = blockManager.getCorruptReplicas(blockInfo);
    int outOfCapacity = 0;
    for (int i = 0; i < numReplicas; i++) {
      DatanodeDescriptor datanode = storages[i].getDatanodeDescriptor();
      if (datanode == null) {
        continue;
      }
      if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
        continue;
      }
      if (corrupt != null && corrupt.contains(datanode)) {
        continue;
      }
      if (pendingCached.contains(datanode) || cached.contains(datanode)) {
        continue;
      }
      long pendingBytes = 0;
      // Subtract pending cached blocks from effective capacity
      Iterator<CachedBlock> it = datanode.getPendingCached(blockManager.getDatanodeManager()).iterator();
      while (it.hasNext()) {
        CachedBlock cBlock = it.next();
        BlockInfo info = blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
        if (info != null) {
          pendingBytes -= info.getNumBytes();
        }
      }
      it = datanode.getPendingUncached(blockManager.getDatanodeManager()).iterator();
      // Add pending uncached blocks from effective capacity
      while (it.hasNext()) {
        CachedBlock cBlock = it.next();
        BlockInfo info = blockManager.getStoredBlock(new Block(cBlock.getBlockId()));
        if (info != null) {
          pendingBytes += info.getNumBytes();
        }
      }
      long pendingCapacity = pendingBytes + datanode.getCacheRemaining();
      if (pendingCapacity < blockInfo.getNumBytes()) {
        LOG.trace("Block {}: DataNode {} is not a valid possibility "
            + "because the block has size {}, but the DataNode only has {}"
            + "bytes of cache remaining ({} pending bytes, {} already cached.",
            blockInfo.getBlockId(), datanode.getDatanodeUuid(),
            blockInfo.getNumBytes(), pendingCapacity, pendingBytes,
            datanode.getCacheRemaining());
        outOfCapacity++;
        continue;
      } else {
         if (LOG.isTraceEnabled()) {
          LOG.trace("Datanode " + datanode.getDatanodeUuid() + " is a valid possibility for"
              + " block " + blockInfo.getBlockId() + " of size "
              + blockInfo.getNumBytes() + " bytes, has "
              + datanode.getCacheRemaining() + " bytes of cache remaining.");
        }
      }
      possibilities.add(datanode);
    }
    List<DatanodeDescriptor> chosen = chooseDatanodesForCaching(possibilities,
        neededCached, blockManager.getDatanodeManager().getStaleInterval(), blockInfo.getBlockId());
    for (DatanodeDescriptor datanode : chosen) {
      LOG.trace("Block {}: added to PENDING_CACHED on DataNode {}",
          blockInfo.getBlockId(), datanode.getDatanodeUuid());
      pendingCached.add(datanode);
      boolean added = cachedBlock.addPendingCached(datanode);
      assert added;
    }
    // We were unable to satisfy the requested replication factor
    if (neededCached > chosen.size()) {
      LOG.debug("Block {}: we only have {} of {} cached replicas."
          + " {} DataNodes have insufficient cache capacity.",
          blockInfo.getBlockId(),
          (cachedBlock.getReplication() - neededCached + chosen.size()),
          cachedBlock.getReplication(), outOfCapacity);
    }
  }

  /**
   * Chooses datanode locations for caching from a list of valid possibilities.
   * Non-stale nodes are chosen before stale nodes.
   *
   * @param possibilities List of candidate datanodes
   * @param neededCached Number of replicas needed
   * @param staleInterval Age of a stale datanode
   * @return A list of chosen datanodes
   */
  private static List<DatanodeDescriptor> chooseDatanodesForCaching(
      final List<DatanodeDescriptor> possibilities, final int neededCached,
      final long staleInterval, long blockId) {
    // Make a copy that we can modify
    List<DatanodeDescriptor> targets = new ArrayList<DatanodeDescriptor>(possibilities);
    // Selected targets
    List<DatanodeDescriptor> chosen = new LinkedList<DatanodeDescriptor>();

    // Filter out stale datanodes
    List<DatanodeDescriptor> stale = new LinkedList<DatanodeDescriptor>();
    Iterator<DatanodeDescriptor> it = targets.iterator();
    while (it.hasNext()) {
      DatanodeDescriptor d = it.next();
      if (d.isStale(staleInterval)) {
        it.remove();
        stale.add(d);
      }
    }
    // Select targets
    while (chosen.size() < neededCached) {
      // Try to use stale nodes if we're out of non-stale nodes, else we're done
      if (targets.isEmpty()) {
        if (!stale.isEmpty()) {
          targets = stale;
        } else {
          break;
        }
      }
      // Select a random target
      DatanodeDescriptor target = chooseRandomDatanodeByRemainingCapacity(targets);
      if (LOG.isTraceEnabled()) {
          LOG.trace("Datanode " + target.getDatanodeUuid() + " is a chosen for"
              + " block " + blockId + " of size "
              + neededCached + " bytes, has "
              + target.getCacheRemaining() + " bytes of cache remaining.");
        }
      chosen.add(target);
      targets.remove(target);
    }
    return chosen;
  }

  /**
   * Choose a single datanode from the provided list of possible
   * targets, weighted by the percentage of free space remaining on the node.
   *
   * @return The chosen datanode
   */
  private static DatanodeDescriptor chooseRandomDatanodeByRemainingCapacity(
      final List<DatanodeDescriptor> targets) {
    // Use a weighted probability to choose the target datanode
    float total = 0;
    for (DatanodeDescriptor d : targets) {
      total += d.getCacheRemainingPercent();
    }
    // Give each datanode a portion of keyspace equal to its relative weight
    // [0, w1) selects d1, [w1, w2) selects d2, etc.
    TreeMap<Integer, DatanodeDescriptor> lottery = new TreeMap<Integer, DatanodeDescriptor>();
    int offset = 0;
    for (DatanodeDescriptor d : targets) {
      // Since we're using floats, be paranoid about negative values
      int weight = Math.max(1, (int) ((d.getCacheRemainingPercent() / total) * 1000000));
      offset += weight;
      lottery.put(offset, d);
    }
    // Choose a number from [0, offset), which is the total amount of weight,
    // to select the winner
    DatanodeDescriptor winner = lottery.higherEntry(random.nextInt(offset)).getValue();
    return winner;
  }
}
