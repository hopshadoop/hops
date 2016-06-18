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
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

final class IndividualINodeLock extends BaseINodeLock {

  private static final INodeIdentifier NON_EXISTING_INODE =
      new INodeIdentifier(INode.NON_EXISTING_ID);
  
  private final TransactionLockTypes.INodeLockType lockType;
  private final INodeIdentifier inodeIdentifier;
  private final boolean readUpPathInodes;
  private boolean readBackUpRow=false;
  private boolean isForBlockReport=false;

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

  IndividualINodeLock(TransactionLockTypes.INodeLockType lockType,
                      INodeIdentifier inodeIdentifier, boolean readUpPathInodes, boolean readBackUpRow) {
    this(lockType,inodeIdentifier,readUpPathInodes);
    this.readBackUpRow=readBackUpRow;
  }

  public IndividualINodeLock(boolean isForBlockReport, TransactionLockTypes.INodeLockType lockType, INodeIdentifier inodeIdentifier) {
    this(lockType, inodeIdentifier);
    this.isForBlockReport = isForBlockReport;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setPartitioningKey(inodeIdentifier.getInodeId());

    INode inode = null;
    if (inodeIdentifier.getName() != null && inodeIdentifier.getPid() != null) {
      inode =
          find(lockType, inodeIdentifier.getName(), inodeIdentifier.getPid(),
              inodeIdentifier.getInodeId());

      if(readBackUpRow){
        INode backUpNode = find(lockType, inodeIdentifier.getName(), -inodeIdentifier.getPid(),
                -inodeIdentifier.getInodeId());
        //addIndividualINode(backUpNode);

      }
    } else if (inodeIdentifier.getInodeId() != null) {
      inode = find(lockType, inodeIdentifier.getInodeId());

      if(readBackUpRow){
        INode backUpNode = find(lockType, -inodeIdentifier.getInodeId());
        //addIndividualINode(backUpNode);
      }
    } else {
      throw new StorageException(
          "INodeIdentifier object is not properly " + "initialized ");
    }


    if(inode==null && isForBlockReport){
      //InCase of file is created and deleted. or created then modified and then deleted. Those cases before or after creating the snapshot.
      //We have to read the back-up inode row.
      inode = find(lockType,-inodeIdentifier.getInodeId());
    }

    if (readUpPathInodes) {
      List<INode> pathInodes = readUpInodes(inode);
      addPathINodesAndUpdateResolvingCache(INodeUtil.constructPath(pathInodes),
          pathInodes);
    } else {
      addIndividualINode(inode);
    }

      acquireINodeAttributes();

  }

  private List<INode> readUpInodes(INode leaf)
      throws StorageException, TransactionContextException {
    LinkedList<INode> pathInodes = new LinkedList<INode>();
    pathInodes.add(leaf);
    INode curr = leaf;
    while (curr.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      curr = find(TransactionLockTypes.INodeLockType.READ_COMMITTED,
          curr.getParentId());
      pathInodes.addFirst(curr);
    }
    return pathInodes;
  }
}
