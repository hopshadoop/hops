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

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import io.hops.transaction.lock.TransactionLockTypes.LeaseHolderResolveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class LeaseLock extends Lock {

  private final TransactionLockTypes.LockType lockType;
  private final String leaseHolder;
  private final List<Lease> leases;
  private final String singleFileLock;
  private final LeaseHolderResolveType resolveType;

  LeaseLock(TransactionLockTypes.LockType lockType, LeaseHolderResolveType resolveType,
            String leaseHolder, String singleFileLock ) {
    this.lockType = lockType;
    this.leaseHolder = leaseHolder;
    this.leases = new ArrayList<>();
    this.resolveType = resolveType;
    this.singleFileLock = singleFileLock;

    if(resolveType == LeaseHolderResolveType.SINGLE_PATH &&
            (singleFileLock == null || singleFileLock.isEmpty())) {
      throw new IllegalArgumentException("Please specify a lease path to lock");
    }
  }

  LeaseLock(TransactionLockTypes.LockType lockType) {
    this(lockType, LeaseHolderResolveType.ALL_PATHS, null, null);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(lockType);

    Set<String> hldrs = new HashSet<>();
    if (leaseHolder != null) {
      hldrs.add(leaseHolder);
    }

    if (locks.containsLock(Type.INode)) {
      BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);

      for (INode f : inodeLock.getAllResolvedINodes()) {
        if ((f instanceof INodeFile) && f.isUnderConstruction()) {
          hldrs.add(((INodeFile) f).getFileUnderConstructionFeature().getClientName());
        }
      }
    }

    List<String> holders = new ArrayList<>(hldrs);
    Collections.sort(holders);
    if (holders.isEmpty() && !locks.containsLock(Type.INode)) {
      Collection<Lease> allLeases = acquireLockList(lockType, Lease.Finder.All);
      if (leases != null) {
        leases.addAll(allLeases);
      }
    }
    for (String h : holders) {
      Lease lease = acquireLock(lockType, Lease.Finder.ByHolder, h, Lease.getHolderId(h));
      if (lease != null) {
        leases.add(lease);
      }
    }
  }

  Collection<Lease> getLeases() {
    return leases;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }

  @Override
  protected final Type getType() {
    return Type.Lease;
  }

  public String getSingleFileLock(){
    return singleFileLock;
  }

  public LeaseHolderResolveType getResolveType(){
    return resolveType;
  }

  public String getLeaseHolder(){
    return leaseHolder;
  }
}
