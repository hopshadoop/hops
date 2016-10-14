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
package io.hops.transaction.lock;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
      this.targets = targets;
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
      INodeLock iNodeLock = (INodeLock) locks.getLock(Type.INode);
      Arrays.sort(targets);
      for (String target : targets) {
        INode iNode = iNodeLock.getTargetINode(target);
        acquireLocks(iNode);
        if(includeChildren){
          List<INode> children = iNodeLock.getChildINodes(target);
          if(children!=null){
            for(INode child:children){
              acquireLocks(child);
            }
          }
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
    private final int inodeId;

    IndividualEncodingStatusLock(TransactionLockTypes.LockType lockType,
        int inodeId) {
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
      acquireLock(getLockType(), EncodingStatus.Finder.ByParityInodeId,
          inodeId);
    }
  }
}
