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

import io.hops.common.IDsGeneratorFactory;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.exception.TransientStorageException;
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

import static org.apache.hadoop.util.ExitUtil.terminate;

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
        e.printStackTrace();
      }
    }
  }

  private int nextId() {
    return IDsGeneratorFactory.getInstance().getUniqueQuotaUpdateID();
  }

  public void addUpdate(final long inodeId, final long namespaceDelta,
      final long diskspaceDelta)
      throws StorageException, TransactionContextException {

    QuotaUpdate update =
        new QuotaUpdate(nextId(), inodeId, namespaceDelta, diskspaceDelta);
    EntityManager.add(update);
  }

  private class QuotaUpdateMonitor implements Runnable {
    @Override
    public void run() {
      long startTime;
      while (namesystem.isRunning()) {
        startTime = System.currentTimeMillis();
        try {
          if (namesystem.isLeader()) {
            if (!prioritizedUpdates.isEmpty()) {
              Iterator<Long> iterator = prioritizedUpdates.poll();
              while (iterator.hasNext()) {
                processUpdates(iterator.next());
              }
              synchronized (iterator) {
                iterator.notify();
              }
            }
            processNextUpdateBatch();
          }
          long sleepDuration =
              updateInterval - (System.currentTimeMillis() - startTime);
          if (sleepDuration > 0) {
            Thread.sleep(updateInterval);
          }
        } catch (InterruptedException ie) {
          LOG.warn("QuotaUpdateMonitor thread received InterruptedException.",
              ie);
          break;
        } catch (StorageException e) {
          LOG.warn("QuotaUpdateMonitor thread received StorageException.", e);
          if( e instanceof TransientStorageException) {
            continue; // do not quit thread on storage exception
          }
          terminate(1, e);
        } catch (Throwable t) {
          LOG.fatal("QuotaUpdateMonitor thread received Runtime exception. ",
              t);
          terminate(1, t);
        }
      }
    }
  }

  private final Comparator<QuotaUpdate> quotaUpdateComparator =
      new Comparator<QuotaUpdate>() {
        @Override
        public int compare(QuotaUpdate quotaUpdate, QuotaUpdate quotaUpdate2) {
          if (quotaUpdate.getInodeId() < quotaUpdate2.getInodeId()) {
            return -1;
          }
          if (quotaUpdate.getInodeId() > quotaUpdate2.getInodeId()) {
            return 1;
          }
          return 0;
        }
      };

  private void processUpdates(final Long id) throws IOException {
    LightWeightRequestHandler findHandler =
        new LightWeightRequestHandler(HDFSOperationType.GET_UPDATES_FOR_ID) {
          @Override
          public Object performTask() throws IOException {
            QuotaUpdateDataAccess<QuotaUpdate> dataAccess =
                (QuotaUpdateDataAccess) HdfsStorageFactory
                    .getDataAccess(QuotaUpdateDataAccess.class);
            return dataAccess.findByInodeId(id);
          }
        };

    List<QuotaUpdate> quotaUpdates = (List<QuotaUpdate>) findHandler.handle();
    LOG.debug("processUpdates for inode id="+id+" quotaUpdates ids are "+ Arrays.toString(quotaUpdates.toArray()));
    applyBatchedUpdate(quotaUpdates);
  }

  private void processNextUpdateBatch() throws IOException {
    LightWeightRequestHandler findHandler =
        new LightWeightRequestHandler(HDFSOperationType.GET_NEXT_QUOTA_BATCH) {
          @Override
          public Object performTask() throws IOException {
            QuotaUpdateDataAccess<QuotaUpdate> dataAccess =
                (QuotaUpdateDataAccess) HdfsStorageFactory
                    .getDataAccess(QuotaUpdateDataAccess.class);
            return dataAccess.findLimited(updateLimit);
          }
        };

    List<QuotaUpdate> quotaUpdates = (List<QuotaUpdate>) findHandler.handle();
    Collections.sort(quotaUpdates, quotaUpdateComparator);

    ArrayList<QuotaUpdate> batch = new ArrayList<>();
    for (QuotaUpdate update : quotaUpdates) {
      if (batch.size() == 0 ||
          batch.get(0).getInodeId() == update.getInodeId()) {
        batch.add(update);
      } else {
        applyBatchedUpdate(batch);
        batch = new ArrayList<>();
        batch.add(update);
      }
    }

    if (batch.size() != 0) {
      applyBatchedUpdate(batch);
    }
  }

  private void applyBatchedUpdate(final List<QuotaUpdate> updates)
      throws IOException {
    if (updates.size() == 0) {
      return;
    }
    new HopsTransactionalRequestHandler(HDFSOperationType.APPLY_QUOTA_UPDATE) {
      INodeIdentifier iNodeIdentifier;

      @Override
      public void setUp() throws IOException {
        super.setUp();
        iNodeIdentifier = new INodeIdentifier(updates.get(0).getInodeId());
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE,
                iNodeIdentifier));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory dir = (INodeDirectory) EntityManager
            .find(INode.Finder.ByINodeIdFTIS, updates.get(0).getInodeId());
        if (dir != null && SubtreeLockHelper
            .isSTOLocked(dir.isSTOLocked(), dir.getSTOLockOwner(),
                namesystem.getNameNode().getActiveNameNodes()
                    .getActiveNodes()) && dir.getSTOLockOwner() != namesystem.getNamenodeId()) {
          LOG.warn("Ignoring updates as the subtree lock is set");
          /*
           * We cannot process updates to keep move operations consistent. Otherwise the calculated size of the subtree
           * could differ from the view of the parent if outstanding quota updates are applied after being considered
           * by the QuotaCountingFileTree but before successfully moving the subtree.
           */
          return null;
        }

        long namespaceDelta = 0;
        long diskspaceDelta = 0;
        for (QuotaUpdate update : updates) {
          namespaceDelta += update.getNamespaceDelta();
          diskspaceDelta += update.getDiskspaceDelta();
          LOG.debug("handling " + update);
          EntityManager.remove(update);
        }

        if (dir == null) {
          LOG.debug("dropping update for " + updates.get(0) + " ns " +
              namespaceDelta + " ds " + diskspaceDelta +
              " because of deletion");
          return null;
        }
        if (namespaceDelta == 0 && diskspaceDelta == 0) {
          return null;
        }

        if (dir != null && dir.isQuotaSet()) {
          final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q != null) {
            INodeAttributes attributes = q.getINodeAttributes(dir);
            attributes.setNsCount(attributes.getNsCount() + namespaceDelta);
            attributes.setDiskspace(attributes.getDiskspace() + diskspaceDelta);
            LOG.debug("applying aggregated update for directory " + dir.getId() +
                " with namespace delta " + namespaceDelta +
                " and diskspace delta " + diskspaceDelta);
          }
        }

        if (dir != null && dir.getId() != INodeDirectory.ROOT_INODE_ID) {
          QuotaUpdate parentUpdate =
              new QuotaUpdate(nextId(), dir.getParentId(), namespaceDelta,
                  diskspaceDelta);
          EntityManager.add(parentUpdate);
          LOG.debug("adding parent update " + parentUpdate);
        }
        return null;
      }
    }.handle(this);
  }

  /**
   * Ids which updates need to be applied. Note that children must occur before
   * their parents
   * in order to guarantee that updates are applied completely.
   *
   * @param iterator
   *     Ids to be updates sorted from the leaves to the root of the subtree
   */
  void addPrioritizedUpdates(Iterator<Long> iterator) throws QuotaUpdateException {
      if(namesystem.isLeader()) {
        prioritizedUpdates.add(iterator);
      } else {
        throw  new QuotaUpdateException("Non leader name" +
                "node cannot prioritize quota updates for inodes");
      }
  }
}
