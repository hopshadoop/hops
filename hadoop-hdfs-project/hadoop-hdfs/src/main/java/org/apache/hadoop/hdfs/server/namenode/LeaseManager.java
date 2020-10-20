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

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static io.hops.transaction.lock.LockFactory.BLK;
import static io.hops.transaction.lock.LockFactory.getInstance;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.UnresolvedLinkException;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * LeaseManager does the lease housekeeping for writing on files.
 * This class also provides useful static methods for lease recovery.
 * <p/>
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p
 * <p/>
 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 * with the new generation stamp and the minimum block length
 * 2.7) p acknowledges the namenode the update results
 * <p/>
 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 * and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
@InterfaceAudience.Private
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;
  
  private Daemon lmthread;
  private volatile boolean shouldRunMonitor;

  LeaseManager(FSNamesystem fsnamesystem) {
    this.fsnamesystem = fsnamesystem;
  }

  Lease getLease(String holder)
      throws StorageException, TransactionContextException {
    return EntityManager.find(Lease.Finder.ByHolder, holder, Lease.getHolderId(holder));
  }
  
  @VisibleForTesting
  int getNumSortedLeases() throws IOException {
    HopsTransactionalRequestHandler getNumSortedLeasesHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_SORTED_LEASES) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        LeaseDataAccess<Lease> da = (LeaseDataAccess) HdfsStorageFactory
            .getDataAccess(LeaseDataAccess.class);
        return da.countAll();
      }
    };
    return (int) getNumSortedLeasesHandler.handle(fsnamesystem);
  }
  
  private SortedSet<Lease> getSortedLeases() throws IOException {
    HopsTransactionalRequestHandler getSortedLeasesHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_SORTED_LEASES) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            LeaseDataAccess<Lease> da = (LeaseDataAccess) HdfsStorageFactory
                .getDataAccess(LeaseDataAccess.class);
            return da.findAll();
          }
        };
    return new TreeSet<>(
        (Collection<? extends Lease>) getSortedLeasesHandler
            .handle(fsnamesystem));
  }
  
  /**
   * This method iterates through all the leases and counts the number of blocks
   * which are not COMPLETE. The FSNamesystem read lock MUST be held before
   * calling this method.
   * @return
   */
  synchronized long getNumUnderConstructionBlocks() throws IOException {
    
    final AtomicLong numUCBlocks = new AtomicLong(0);
    for (final Lease lease : getSortedLeases()) {
      new HopsTransactionalRequestHandler(
          HDFSOperationType.GET_LISTING) {
        private Set<String> leasePaths = null;

        @Override
        public void setUp() throws StorageException {
          String holder = lease.getHolder();
          leasePaths = INodeUtil.findPathsByLeaseHolder(holder);
          if (leasePaths != null) {
            LOG.debug("Total Paths " + leasePaths.size() + " Paths: " + Arrays.toString(leasePaths.toArray()));
          }
        }

        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          String holder = lease.getHolder();
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN,
              leasePaths.toArray(new String[leasePaths.size()]))
              .setNameNodeID(fsnamesystem.getNameNode().getId())
              .setActiveNameNodes(fsnamesystem.getNameNode().getActiveNameNodes().getActiveNodes());
          locks.add(il).add(lf.getLeaseLockAllPaths(LockType.READ, holder,
                  fsnamesystem.getLeaseCreationLockRows()))
              .add(lf.getLeasePathLock(LockType.READ)).add(lf.getBlockLock())
              .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
        }

        @Override
        public Object performTask() throws IOException {
          Lease transactionLease = EntityManager.find(Lease.Finder.ByHolder, lease.getHolder(), lease.getHolderID());
          if(transactionLease == null){
            //the lease does not exist anymore, no block will be under construction
            LOG.debug("the lease for holder: " + lease.getHolder() + " does not exist anymore");
            return null;
          }
          for (LeasePath leasePath : transactionLease.getPaths()) {
            final String path = leasePath.getPath();
            final INodeFile cons;
            try {
              INode inode = fsnamesystem.getFSDirectory().getINode(path);
              if(inode == null ) {
                // Eventually this will be cleaned by the lease monitor
                LOG.warn("Unable to find inode for the lease "+path);
                continue;
              }
              cons = inode.asFile();
              Preconditions.checkState(cons.isUnderConstruction());
            } catch (UnresolvedLinkException e) {
              throw new AssertionError("Lease files should reside on this FS");
            }
            BlockInfoContiguous[] blocks = cons.getBlocks();
            if (blocks == null) {
              continue;
            }
            for (BlockInfoContiguous b : blocks) {
              if (!b.isComplete()) {
                numUCBlocks.incrementAndGet();
              }
            }
          }
          return null;
        }
      }.handle();
    }
    LOG.info("Number of blocks under construction: " + numUCBlocks.get());
    return numUCBlocks.get();
  }



  /**
   * @return the lease containing src
   */
  public Lease getLeaseByPath(String src)
      throws StorageException, TransactionContextException {
    LeasePath leasePath = EntityManager.find(LeasePath.Finder.ByPath, src);
    if (leasePath != null) {
      int holderID = leasePath.getHolderId();
      Lease lease = EntityManager.find(Lease.Finder.ByHolderId, holderID);
      return lease;
    } else {
      return null;
    }
  }

  /**
   * @return the number of leases currently in the system
   */
  public int countLease() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_LEASE) {
      @Override
      public Object performTask() throws StorageException, IOException {
        LeaseDataAccess da = (LeaseDataAccess) HdfsStorageFactory
            .getDataAccess(LeaseDataAccess.class);
        return da.countAll();
      }
    }.handle(fsnamesystem);
  }

  /**
   * This method is never called in the stateless implementation
   *
   * @return the number of paths contained in all leases
   */
  int countPath() throws StorageException, TransactionContextException {
    return EntityManager.count(Lease.Counter.All);
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  Lease addLease(String holder, String src)
      throws StorageException, TransactionContextException {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder, 
              org.apache.hadoop.hdfs.server.namenode.Lease.getHolderId(holder)
              , now(), 0);
      lease.savePersistent();
    } else {
      renewLease(lease);
    }

    if(src != null) {
      LeasePath lPath = new LeasePath(src, lease.getHolderID());
      lease.addPath(lPath);
      lPath.savePersistent();
    }

    return lease;
  }

  /**
   * Remove the specified lease and src.
   */
  void removeLease(Lease lease, LeasePath src)
      throws StorageException, TransactionContextException {
    if(lease == null){
      LOG.warn("Lease not found. Removing lease path");
      src.deletePersistent();
      return;
    }

    if (!lease.removePath(src)) {
      LOG.error(src + " not found in lease.paths (=" + lease.getPaths() + ")");
    }
    src.deletePersistent();

    if (!lease.hasPath()) {
      lease.deletePersistent();
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  void removeLease(String holder, String src)
      throws StorageException, TransactionContextException {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, new LeasePath(src, lease.getHolderID()));
    } else {
      LOG.warn("Removing non-existent lease! holder=" + holder +
          " src=" + src);
    }
  }

  void removeAllLeases() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_ALL_LEASES) {
      @Override
      public Object performTask() throws StorageException, IOException {
        LeaseDataAccess lda = (LeaseDataAccess) HdfsStorageFactory
            .getDataAccess(LeaseDataAccess.class);
        LeasePathDataAccess lpda = (LeasePathDataAccess) HdfsStorageFactory
            .getDataAccess(LeasePathDataAccess.class);
        lda.removeAll();
        lpda.removeAll();
        return null;
      }
    }.handle(fsnamesystem);
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  Lease reassignLease(Lease lease, String src, String newHolder)
      throws StorageException, TransactionContextException {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      LeasePath lp =  new LeasePath(src, lease.getHolderID());
      if (!lease.removePath(lp)) {
        LOG.error(
            src + " not found in lease.paths (=" + lease.getPaths() + ")");
      }
      lp.deletePersistent();

      if (!lease.hasPath() && !lease.getHolder().equals(newHolder)) {
        lease.deletePersistent();
      }
    }
    
    Lease newLease = getLease(newHolder);
    LeasePath lPath = null;
    if (newLease == null) {
      newLease = new Lease(newHolder, 
              org.apache.hadoop.hdfs.server.namenode.Lease.getHolderId(newHolder)
              , now(), 0);
      newLease.savePersistent();
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addPath(lPath);
    } else {
      renewLease(newLease);
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addPath(lPath);
    }
    // update lease-paths' holder
    lPath.savePersistent();
    
    return newLease;
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder)
      throws StorageException, TransactionContextException {
    renewLease(getLease(holder));
  }

  void renewLease(Lease lease)
      throws StorageException, TransactionContextException {
    if (lease != null) {
      lease.setLastUpdate(now());
      lease.savePersistent();
    }
  }

  void changeLease(String src, String dst)
      throws StorageException, TransactionContextException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
          " src=" + src + ", dest=" + dst);
    }

    final int len = src.length();
    Collection <LeasePath> paths = findLeasePathsWithPrefix(src);
    Collection <LeasePath> newLPs = new ArrayList<>(paths.size());
    Collection <LeasePath> deletedLPs = new ArrayList<>(paths.size());
    for (final LeasePath oldPath : paths) {
      if (oldPath.getPath().length() > len && oldPath.getPath().charAt(len) != Path.SEPARATOR_CHAR) {
        continue;
      }
      final int holderId = oldPath.getHolderId();
      final LeasePath newpath =
          new LeasePath(dst + oldPath.getPath().substring(len), holderId,
              oldPath.getLastBlockId(), oldPath.getPenultimateBlockId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldPath + " with " + newpath);
      }
      newLPs.add(newpath);
      deletedLPs.add(oldPath);
    }
    
    for(LeasePath newPath: newLPs){
      newPath.savePersistent();
    }
    for(LeasePath deletedLP: deletedLPs){
      deletedLP.deletePersistent();
    }
  }
  
  void removeLeaseWithPrefixPath(String prefix)
      throws StorageException, TransactionContextException {
    for (Map.Entry<LeasePath, Lease> entry : findLeaseWithPrefixPath(prefix)
        .entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName() +
            ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());
    }
  }
  
  //HOP: bug fix changes HDFS-4242
  private Map<LeasePath, Lease> findLeaseWithPrefixPath(String prefix)
      throws StorageException, TransactionContextException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    Collection<LeasePath> leasePathSet =
        EntityManager.findList(LeasePath.Finder.ByPrefix, prefix);

    final Map<LeasePath, Lease> entries = new HashMap<>();
    int srclen = prefix.length();
    
    // prefix may ended with '/'
    if (prefix.charAt(srclen - 1) == Path.SEPARATOR_CHAR) {
      srclen -= 1;
    }

    for (LeasePath lPath : leasePathSet) {
      if (!lPath.getPath().startsWith(prefix)) {
        LOG.warn(
            "LeasePath fetched by prefix does not start with the prefix: \n" +
                "LeasePath: " + lPath + "\t Prefix: " + prefix);
        return entries;
      }
      if (lPath.getPath().length() == srclen ||
          lPath.getPath().charAt(srclen) == Path.SEPARATOR_CHAR) {
        Lease lease =
            EntityManager.find(Lease.Finder.ByHolderId, lPath.getHolderId());
        entries.put(lPath, lease);
      }
    }
    return entries;
  }

    private Collection<LeasePath> findLeasePathsWithPrefix(String prefix)
      throws StorageException, TransactionContextException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    Collection<LeasePath> leasePathSet =
        EntityManager.findList(LeasePath.Finder.ByPrefix, prefix);

    return leasePathSet;
  }
    
  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
  }
  
  /**
   * ***************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   * ****************************************************
   */
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    /**
     * Check leases periodically.
     */
    @Override
    public void run() {
      for (; shouldRunMonitor && fsnamesystem.isRunning(); ) {
        try {
          if (fsnamesystem.isLeader()) {
            try {
              if (!fsnamesystem.isInSafeMode()) {
                checkLeases();
              }
            } catch (IOException ex) {
              LOG.error(ex);
            }
          }
          Thread.sleep(HdfsServerConstants.NAMENODE_LEASE_RECHECK_INTERVAL);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        } catch(Throwable e) {
          LOG.warn("Unexpected throwable: ", e);
        }
      }
    }

  }

  /**
   * Check the leases beginning from the oldest.
   *
   * @return true is sync is needed.
   */
  @VisibleForTesting
  boolean checkLeases()
      throws StorageException, TransactionContextException, IOException {

    boolean needSync = false;

    SortedSet<Lease> sortedLeases = (SortedSet<Lease>) new LightWeightRequestHandler(
        HDFSOperationType.PREPARE_LEASE_MANAGER_MONITOR) {
      @Override
      public Object performTask() throws StorageException, IOException {
        long expiredTime = now() - hardLimit;
        LeaseDataAccess da = (LeaseDataAccess) HdfsStorageFactory
            .getDataAccess(LeaseDataAccess.class);
        return new TreeSet<Lease>(da.findByTimeLimit(expiredTime));
      }
    }.handle(fsnamesystem);

    if (sortedLeases != null) {
      for (Lease expiredLease : sortedLeases) {
        HopsTransactionalRequestHandler expiredLeaseHandler = new HopsTransactionalRequestHandler(
            HDFSOperationType.LEASE_MANAGER_MONITOR) {
          private Set<String> leasePaths = null;

          @Override
          public void setUp() throws StorageException {
            String holder = (String) getParams()[0];
            leasePaths = INodeUtil.findPathsByLeaseHolder(holder);
            if (leasePaths != null) {
              LOG.debug("Total Paths " + leasePaths.size() + " Paths: " + Arrays.toString(leasePaths.toArray()));
            }

          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            String holder = (String) getParams()[0];
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE,
                INodeResolveType.PATH,
                leasePaths.toArray(new String[leasePaths.size()])).setNameNodeID(fsnamesystem.getNameNode().getId())
                .setActiveNameNodes(fsnamesystem.getNameNode().getActiveNameNodes().getActiveNodes());

            locks.add(il).add(lf.getNameNodeLeaseLock(LockType.WRITE))
                .add(lf.getLeaseLockAllPaths(LockType.WRITE, holder,
                        fsnamesystem.getLeaseCreationLockRows()))
                .add(lf.getLeasePathLock(LockType.WRITE, leasePaths.size()))
                .add(lf.getBlockLock()).add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            String holder = (String) getParams()[0];
            boolean needSync = false;
            if (holder != null) {
              Lease leaseToCheck = EntityManager.find(Lease.Finder.ByHolder, holder, Lease.getHolderId(holder));
              if (leaseToCheck == null) {
                return needSync;
              }
              if (!expiredHardLimit(leaseToCheck)) {
                LOG.warn("Unable to release hard-limit expired lease: "
                    + leaseToCheck);
                return needSync;
              }

              LOG.info("Lease " + leaseToCheck + " has expired hard limit");

              if(!leaseToCheck.hasPath()){
                leaseToCheck.deletePersistent();
                return true;
              }

              final List<LeasePath> removing = new ArrayList<>();
              // need to create a copy of the oldest lease paths, because 
              // internalReleaseLease() removes paths corresponding to empty files,
              // i.e. it needs to modify the collection being iterated over
              // causing ConcurrentModificationException
              LeasePath[] leasePaths = new LeasePath[leaseToCheck.getPaths().size()];
              leaseToCheck.getPaths().toArray(leasePaths);
              for (LeasePath p : leasePaths) {
                try {
                  INodesInPath iip = fsnamesystem.getFSDirectory().getINodesInPath(p.getPath(),
                      true);
                  if(iip == null || iip.getLastINode() == null){
                    removing.add(p);
                    continue;
                  }
                  boolean completed = fsnamesystem.internalReleaseLease(leaseToCheck, p.getPath(),
                          iip, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
                  if (LOG.isDebugEnabled()) {
                    if (completed) {
                      LOG.debug("Lease recovery for " + p + " is complete. File closed.");
                    } else {
                      LOG.debug("Started block recovery " + p + " lease " + leaseToCheck);
                    }
                  }

                  // If a lease recovery happened, we need to sync later.
                  if (!needSync && !completed) {
                    needSync = true;
                  }

                } catch (IOException e) {
                  LOG.error(
                      "Cannot release the path " + p + " in the lease " + leaseToCheck, e);
                  removing.add(p);
                }
              }

              for (LeasePath p : removing) {
                if (leaseToCheck.getPaths().contains(p)) {
                  removeLease(leaseToCheck, p);
                }
              }
            }
            return needSync;
          }
        };

        needSync = needSync || (boolean) expiredLeaseHandler.setParams(expiredLease.getHolder())
            .handle(fsnamesystem);
      }
    }

    return needSync;
  }

  void startMonitor() {
    Preconditions.checkState(lmthread == null, "Lease Monitor already running");
    shouldRunMonitor = true;
    lmthread = new Daemon(new Monitor());
    lmthread.start();
  }
  
  void stopMonitor() {
    if (lmthread != null) {
      shouldRunMonitor = false;
      try {
        lmthread.interrupt();
        lmthread.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      lmthread = null;
    }
  }

  /**
   * Trigger the currently-running Lease monitor to re-check
   * its leases immediately. This is for use by unit tests.
   */
  @VisibleForTesting
  void triggerMonitorCheckNow() {
    Preconditions.checkState(lmthread != null, "Lease monitor is not running");
    lmthread.interrupt();
  }
  
  private boolean expiredHardLimit(Lease lease) {
    return now() - lease.getLastUpdate() > hardLimit;
  }

  public boolean expiredSoftLimit(Lease lease) {
    return now() - lease.getLastUpdate() > softLimit;
  }
}
