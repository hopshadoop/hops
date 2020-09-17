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
package io.hops.transaction.lock;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.LeasePath;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import io.hops.transaction.lock.TransactionLockTypes.LeaseHolderResolveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public final class LeasePathLock extends Lock {

  private final TransactionLockTypes.LockType lockType;
  private final List<LeasePath> leasePaths;
  private final int expectedCount;
  private final String src; //read all leases of src dir. needed for rename

  private LeasePathLock(TransactionLockTypes.LockType lockType, int expectedCount, 
          String src) {
    this.lockType = lockType;
    this.leasePaths = new ArrayList<>();
    this.expectedCount = expectedCount;
    this.src = src;
  }
  
  LeasePathLock(TransactionLockTypes.LockType lockType, int expectedCount) {
    this(lockType, expectedCount, null);
  }

  LeasePathLock(TransactionLockTypes.LockType lockType) {
    this(lockType, Integer.MAX_VALUE,null);
  }
  
  LeasePathLock(TransactionLockTypes.LockType lockType, String src) {
    this(lockType, Integer.MAX_VALUE,src);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (locks.containsLock(Type.NameNodeLease)) {
      NameNodeLeaseLock nameNodeLeaseLock =
          (NameNodeLeaseLock) locks.getLock(Type.NameNodeLease);
      if (nameNodeLeaseLock.getNameNodeLease() != null) {
        acquireLeasePaths(nameNodeLeaseLock.getNameNodeLease());
      }
    }

    LeaseLock leaseLock = (LeaseLock) locks.getLock(Type.Lease);
    LeaseHolderResolveType resolveType = leaseLock.getResolveType();
    for (Lease lease : leaseLock.getLeases()) {
      if(resolveType == LeaseHolderResolveType.SINGLE_PATH ) {
        acquireLeasePath(lease, leaseLock.getSingleFileLock());
      } else {
        assert resolveType == LeaseHolderResolveType.ALL_PATHS ||
               resolveType == LeaseHolderResolveType.ALL_SYSTEM_PATHS_FOR_TESTSING;
        acquireLeasePaths(lease);
      }
    }

    if(src != null && !src.equals("")){
      acquireAllLeasePathsForDir(src);
    }

    if (leasePaths.size() > expectedCount) {
      // This is only for the LeaseManager
      // TODO: It should retry again, cause there are new lease-paths for this lease which we have not acquired their inodes locks.
    }
  }

  private void acquireLeasePaths(Lease lease)
      throws StorageException, TransactionContextException {
    setLockMode(lockType);
    Collection<LeasePath> result =
        acquireLockList(lockType, LeasePath.Finder.ByHolderId,
            lease.getHolderID());
    if (!lease.getHolder().equals(
        HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease.
      leasePaths.addAll(result);
    }
  }

  private void acquireLeasePath(Lease lease, String path)
          throws StorageException, TransactionContextException {
    setLockMode(lockType);
    LeasePath leasePath = acquireLock(lockType, LeasePath.Finder.ByPath, path);
    if (leasePath != null) {
      leasePaths.add(leasePath);
    }
  }

  private void acquireAllLeasePathsForDir(String src)
      throws StorageException, TransactionContextException {
    Collection<LeasePath> result =
        acquireLockList(lockType, LeasePath.Finder.ByPrefix,
            src);
      leasePaths.addAll(result);
  }
  
  @Override
  protected final Type getType() {
    return Type.LeasePath;
  }

  Collection<LeasePath> getLeasePaths() {
    return leasePaths;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }
}
