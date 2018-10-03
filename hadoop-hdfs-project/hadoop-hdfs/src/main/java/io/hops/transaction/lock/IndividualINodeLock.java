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

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.List;

final class IndividualINodeLock extends BaseINodeLock {

  private static final INodeIdentifier NON_EXISTING_INODE =
      new INodeIdentifier();
  
  private final TransactionLockTypes.INodeLockType lockType;
  private final INodeIdentifier inodeIdentifier;
  private final boolean readUpPathInodes;

  IndividualINodeLock(TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier, boolean readUpPathInodes) {
    this.lockType = lockType;
    this.inodeIdentifier =
        inodeIdentifier == null ? NON_EXISTING_INODE : inodeIdentifier;
    this.readUpPathInodes = readUpPathInodes;
    if (lockType.equals(
        TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT)) {
      throw new UnsupportedOperationException();
    }
  }

  IndividualINodeLock(TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier) {
    this(lockType, inodeIdentifier, false);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setPartitioningKey(inodeIdentifier.getInodeId());

    INode inode = null;
    if (inodeIdentifier.getName() != null && inodeIdentifier.getPid() != null) {
      inode =
          find(lockType, inodeIdentifier.getName(), inodeIdentifier.getPid(),
              inodeIdentifier.getPartitionId(), inodeIdentifier.getInodeId());
    } else if (inodeIdentifier.getInodeId() != null) {
      inode = find(lockType, inodeIdentifier.getInodeId());
    } else {
      throw new StorageException(
          "INodeIdentifier object is not properly " + "initialized ");
    }

    if (readUpPathInodes && inode != null) {
      List<INode> pathInodes = readUpInodes(inode);
      addPathINodesAndUpdateResolvingCache(INodeUtil.constructPath(pathInodes),
          pathInodes);
    } else {
      addIndividualINode(inode);
    }
    acquireINodeAttributes();
  }

  @Override
  public String toString() {
    if ( lockType != null && inodeIdentifier != null){
      return "Individual InodeLock = { ID: "+inodeIdentifier.getInodeId()+" Name: "+inodeIdentifier.getName()+" Lock: "+lockType+" }";
    }
    return "Individual Inode Lock not set";
  }

}
