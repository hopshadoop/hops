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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import io.hops.common.IDsGeneratorFactory;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.SubtreeLockHelper;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.fs.StorageType;

/**
 * Daemon that is asynchronously updating the quota counts of directories.
 * Each operation that affects the quota adds a log entry to our database.
 * This daemon periodically reads a batch of these updates, combines them if
 * possible and applies them.
 */
public class QuotaUpdateManager {

  static final Log LOG = LogFactory.getLog(QuotaUpdateManager.class);

  private final FSNamesystem namesystem;

  private final int updateInterval;
  private final int updateLimit;

  private final Daemon updateThread = new Daemon(new QuotaUpdateMonitor());

  public boolean pauseAsyncOps = false;
  private final ConcurrentLinkedQueue<Iterator<Long>> prioritizedUpdates =
      new ConcurrentLinkedQueue<>();

  public QuotaUpdateManager(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;
    updateInterval =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_DEFAULT);
    updateLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_LIMIT_KEY,
        DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_LIMIT_DEFAULT);
  }

  public void activate() {
    LOG.debug("QuotaUpdateMonitor is running");
    updateThread.start();
  }

  public void close() {
    if (updateThread != null) {
      updateThread.interrupt();
      try {
        updateThread.join(3000);
      } catch (InterruptedException e) {
        LOG.error("QuotaUpdateManager Thread Interrupted");
        Thread.currentThread().interrupt();
      }
    }
  }

  private int nextId() throws StorageException {
    return IDsGeneratorFactory.getInstance().getUniqueQuotaUpdateID();
  }

  public void addUpdate(final long inodeId, QuotaCounts counts)
      throws StorageException, TransactionContextException {

    Map<QuotaUpdate.StorageType, Long> typeSpaces = new HashMap<>();
    for(StorageType t: StorageType.asList()){
      typeSpaces.put(QuotaUpdate.StorageType.valueOf(t.name()), counts.getTypeSpace(t));
    }
    QuotaUpdate update =
        new QuotaUpdate(nextId(), inodeId, counts.getNameSpace(), counts.getStorageSpace(), typeSpaces);
    EntityManager.add(update);
  }

  private class QuotaUpdateMonitor implements Runnable {
    @Override
    public void run() {
      long startTime;
      while (namesystem.isRunning()) {
        try {
          if (!namesystem.isLeader()) {
            Thread.sleep(updateInterval);
            continue;
          }

          startTime = System.currentTimeMillis();

          boolean rerunImmediatly = false;
          applyAllPrioritizedUpdates();

          if (!pauseAsyncOps/*only for testing*/) {
            processNextUpdateBatch();
            rerunImmediatly = countPendingQuota() > 0 || prioritizedUpdates.size() > 0;
          }
          
          //if there is parrent updates apply them immediately
          if (!rerunImmediatly) {
            long sleepDuration = updateInterval - (System.currentTimeMillis() - startTime);
            if (sleepDuration > 0) {
              synchronized (updateThread){
                updateThread.wait(updateInterval);
              }
            }
          }
        } catch (InterruptedException ie) {
          LOG.warn("QuotaUpdateMonitor thread received InterruptedException.",
              ie);
          break;
        } catch (StorageException e) {
          LOG.warn("QuotaUpdateMonitor thread received StorageException.", e);
        } catch (Throwable t) {
          LOG.error("QuotaUpdateMonitor thread received Runtime exception. ",t);
        }
      }
    }
  }

  private int countPendingQuota() throws IOException {
    LightWeightRequestHandler quotaApplicationChecker =
      new LightWeightRequestHandler(HDFSOperationType.COUNT_QUOTA_UPDATES) {
        @Override
        public Object performTask() throws StorageException, IOException {
          QuotaUpdateDataAccess da = (QuotaUpdateDataAccess) HdfsStorageFactory.getDataAccess(QuotaUpdateDataAccess.class);
          return da.getCount();
        }
      };
    return (int) quotaApplicationChecker.handle();
  }

  
  private void applyAllPrioritizedUpdates() throws IOException {
    if (namesystem.isLeader()) {
      if (!prioritizedUpdates.isEmpty()) {
        Iterator<Long> iterator = prioritizedUpdates.poll();
        while (iterator.hasNext()) {
          applyBatchedUpdateForINode(iterator.next(), true);
        }
        synchronized (iterator) {
          iterator.notify();
        }
      }
    }
  }

  private List<Long> getPendingInodes() throws IOException {
   return (List<Long>) (new LightWeightRequestHandler(HDFSOperationType.GET_QUOTA_PENDING_INODES) {
              @Override
              public Object performTask() throws IOException {
                QuotaUpdateDataAccess<QuotaUpdate> dataAccess =
                        (QuotaUpdateDataAccess) HdfsStorageFactory
                                .getDataAccess(QuotaUpdateDataAccess.class);
                return dataAccess.getDistinctInodes();
              }
            }).handle();
  }

  private void processNextUpdateBatch() throws IOException {
    if(namesystem.isLeader()) {
      List<Long> pendingInodes = getPendingInodes();
      // Sort in descending order.
      // In general leaves have higher ID than the parent.
      Collections.sort(pendingInodes, Collections.reverseOrder());

      for (Long inodeID : pendingInodes) {
        applyBatchedUpdateForINode(inodeID, false);

        // if there is priority work then break
        if (prioritizedUpdates.size() > 0) {
          break;
        }
      }
    }
  }

  private void applyBatchedUpdateForINode(final Long inodeID, boolean thisIsPriorityWork)
          throws IOException {
    LOG.debug("Applying quota updates for INode ID: "+inodeID+" Priority: "+thisIsPriorityWork);
    HopsTransactionalRequestHandler handler =
      new HopsTransactionalRequestHandler(HDFSOperationType.APPLY_QUOTA_UPDATE) {
      INodeIdentifier iNodeIdentifier;

      @Override
      public void setUp() throws IOException {
        super.setUp();
        iNodeIdentifier = new INodeIdentifier(inodeID);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, iNodeIdentifier));
        locks.add(lf.getQuotaUpdateLock(inodeID, updateLimit));
      }

      @Override
      public Object performTask() throws IOException {
        // get the updates for this inode
        Collection<QuotaUpdate> dbUpdates = EntityManager.findList(QuotaUpdate.Finder.ByINodeId,
          inodeID, updateLimit);
        LOG.debug("Read "+dbUpdates.size()+" quota updates for INode ID: "+inodeID);

        if (dbUpdates.size() == 0) {
          return 0;
        }

        INodeDirectory dir = (INodeDirectory) EntityManager.find(INode.Finder.ByINodeIdFTIS, inodeID);

        if (dir != null && SubtreeLockHelper.isSTOLocked(dir.isSTOLocked(), dir.getSTOLockOwner(),
          namesystem.getNameNode().getActiveNameNodes().getActiveNodes()) && dir.getSTOLockOwner() != namesystem.getNamenodeId()) {
          LOG.warn("Ignoring updates as the subtree lock is set");
          /*
           * We cannot process updates to keep move operations consistent. Otherwise the calculated size of the subtree
           * could differ from the view of the parent if outstanding quota updates are applied after being considered
           * by the QuotaCountingFileTree but before successfully moving the subtree.
           */
          return false;
        }

        QuotaCounts counts = new QuotaCounts.Builder().build();
        for (QuotaUpdate update : dbUpdates) {
          counts.addStorageSpace(update.getStorageSpaceDelta());
          counts.addNameSpace(update.getNamespaceDelta());

          for (Map.Entry<QuotaUpdate.StorageType, Long> entry : update.getTypeSpaces().entrySet()) {
            counts.addTypeSpace(StorageType.valueOf(entry.getKey().name()), entry.getValue());
          }
          LOG.debug("handling " + update);
          EntityManager.remove(update);
        }

        if (dir == null) {
          LOG.warn("Dropping update for INode ID: " + inodeID + " because the node has been " +
              "deleted. Quota " + counts.toString());
          return dbUpdates.size();
        }

        if (dir != null && dir.isQuotaSet()) {
          final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q != null) {
            q.addSpaceConsumed2Cache(counts);

            LOG.debug("applying aggregated update for directory " + dir.getId() +
                    " with quota " + counts);
          }
        }

        if (dir != null && dir.getId() != INodeDirectory.ROOT_INODE_ID) {
          boolean allNull = counts.getStorageSpace()==0 && counts.getNameSpace()==0;
          Map<QuotaUpdate.StorageType, Long > typeSpace = new HashMap<>();
          for(StorageType type : StorageType.asList()){
            typeSpace.put(QuotaUpdate.StorageType.valueOf(type.name()), counts.getTypeSpace(type));
            allNull = allNull && counts.getTypeSpace(type)==0;
          }
          if (!allNull) {
            QuotaUpdate parentUpdate = new QuotaUpdate(nextId(), dir.getParentId(), counts.getNameSpace(),
                    counts.getStorageSpace(), typeSpace);
            EntityManager.add(parentUpdate);
            LOG.debug("adding parent update " + parentUpdate);
          }
        }
        return dbUpdates.size();
      }
    };

    long processed = 0;
    do {
      processed = (int) handler.handle();
      if (!thisIsPriorityWork) {
        if (prioritizedUpdates.size() > 0) {
          break; // break as high priority work is waiting
        }
      }
    } while (processed > 0);
  }

  /**
   * Ids which updates need to be applied. Note that children must occur before
   * their parents
   * in order to guarantee that updates are applied completely.
   *
   * @param iterator
   *     Ids to be updates sorted from the leaves to the root of the subtree
   */
  @VisibleForTesting
  public void addPrioritizedUpdates(Iterator<Long> iterator) throws QuotaUpdateException {
    if (namesystem.isLeader()) {
      prioritizedUpdates.add(iterator);
      synchronized (updateThread){
        updateThread.notify();
      }
    } else {
      throw new QuotaUpdateException("Non leader name" +
        "node cannot prioritize quota updates for inodes");
    }
  }
}
