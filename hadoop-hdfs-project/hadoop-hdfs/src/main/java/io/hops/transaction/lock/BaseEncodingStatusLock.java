/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract class BaseEncodingStatusLock extends Lock {
  private final TransactionLockTypes.LockType lockType;

  protected BaseEncodingStatusLock(TransactionLockTypes.LockType lockType) {
    this.lockType = lockType;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }

  @Override
  protected final Type getType() {
    return Type.EncodingStatus;
  }

  final static class EncodingStatusLock extends BaseEncodingStatusLock {
    private final String[] targets;
    private final boolean includeChildren;

    EncodingStatusLock(TransactionLockTypes.LockType lockType,
                       String... targets) {
      super(lockType);
      this.targets = targets.clone();
      this.includeChildren = false;
    }

    EncodingStatusLock(boolean includeChildren, TransactionLockTypes.LockType lockType,
                       String... targets) {
      super(lockType);
      this.targets = targets;
      this.includeChildren = includeChildren;
    }

    @Override
    protected void acquire(TransactionLocks locks) throws IOException {
      BaseINodeLock iNodeLock = (BaseINodeLock) locks.getLock(Type.INode);
      Arrays.sort(targets);
      for (String target : targets) {
        INode iNode = iNodeLock.getTargetINode(target);
        if (!BaseINodeLock.isStoredInDB(iNode)) {
          acquireLocks(iNode);
          if (includeChildren) {
            List<INode> children = iNodeLock.getChildINodes(target);
            if (children != null) {
              for (INode child : children) {
                acquireLocks(child);
              }
            }
          }
        } else {
          LOG.debug("Stuffed Inode:  BaseEncodingStatusLock. Skipping acquring locks on the inode named: " + iNode.getLocalName() + " as the file is stored in the database");
        }
      }
}


    private void acquireLocks(INode iNode) throws TransactionContextException, StorageException {
      EncodingStatus status = acquireLock(getLockType(),
              EncodingStatus.Finder.ByInodeId,
              iNode.getId());
      if (status != null) {
        // It's a source file
        return;
      }
      // It's a parity file
      acquireLock(getLockType(), EncodingStatus.Finder.ByParityInodeId,
              iNode.getId());
    }

  }


  final static class IndividualEncodingStatusLock
          extends BaseEncodingStatusLock {
    private final long inodeId;

    IndividualEncodingStatusLock(TransactionLockTypes.LockType lockType,
                                 long inodeId) {
      super(lockType);
      this.inodeId = inodeId;
    }

    @Override
    protected void acquire(TransactionLocks locks) throws IOException {
      // TODO STEFFEN - Should only acquire the locks if we know it has a status and also not twice.
      // Maybe add a flag to iNode specifying whether it's encoded or a parity file

      EncodingStatus status = acquireLock(
              getLockType(), EncodingStatus.Finder.ByInodeId, inodeId);
      if (status != null) {
        // It's a source file
        return;
      }
      // It's a parity file
      acquireLock(getLockType(), EncodingStatus.Finder.ByParityInodeId, inodeId);
    }
  }
  
  final static class BatchedEncodingStatusLock extends BaseEncodingStatusLock {

    private final Set<Long> inodeIds;

    BatchedEncodingStatusLock(TransactionLockTypes.LockType lockType, List<INodeIdentifier> inodeIdentifiers) {
      super(lockType);
      inodeIds = new HashSet<>();
      for (INodeIdentifier identifier : inodeIdentifiers) {
        inodeIds.add(identifier.getInodeId());
      }
    }

    @Override
    protected void acquire(TransactionLocks locks) throws IOException {
      // TODO STEFFEN - Should only acquire the locks if we know it has a status and also not twice.
      // Maybe add a flag to iNode specifying whether it's encoded or a parity file
      Collection<EncodingStatus> status = acquireLockList(getLockType(), EncodingStatus.Finder.ByInodeIds, inodeIds);

      for (EncodingStatus s : status) {
        inodeIds.remove(s.getInodeId());
      }

      if (inodeIds.isEmpty()) {
        //it is all source files
        return;
      }
      // It's parity files
      acquireLockList(getLockType(), EncodingStatus.Finder.ByParityInodeIds, inodeIds);
    }
  }
}
