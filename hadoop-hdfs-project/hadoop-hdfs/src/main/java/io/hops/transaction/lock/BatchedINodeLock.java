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

public class BatchedINodeLock extends BaseINodeLock {

  private final List<INodeIdentifier> inodeIdentifiers;
  private long[] inodeIds;

  public BatchedINodeLock(List<INodeIdentifier> inodeIdentifiers) {
    this.inodeIdentifiers = inodeIdentifiers;
    inodeIds = new long[inodeIdentifiers.size()];
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (inodeIdentifiers != null && !inodeIdentifiers.isEmpty()) {
      String[] names = new String[inodeIdentifiers.size()];
      long[] parentIds = new long[inodeIdentifiers.size()];
      long[] partitionIds = new long[inodeIdentifiers.size()];
      for (int i = 0; i < inodeIdentifiers.size(); i++) {
        INodeIdentifier inodeIdentifier = inodeIdentifiers.get(i);
        names[i] = inodeIdentifier.getName();
        parentIds[i] = inodeIdentifier.getPid();
        partitionIds[i] = inodeIdentifier.getPartitionId();
        inodeIds[i] = inodeIdentifier.getInodeId();
      }

      List<INode> inodes = find(getDefaultInodeLockType(), names, parentIds,partitionIds, false);
      for (INode inode : inodes) {
        if (inode != null) {
          List<INode> pathInodes = readUpInodes(inode);
          addPathINodesAndUpdateResolvingCache(INodeUtil.constructPath(pathInodes),
              pathInodes);
        }
      }
      addIndividualINodes(inodes);
    } else {
      throw new StorageException(
          "INodeIdentifier object is not properly initialized ");
    }
  }

  long[] getINodeIds() {
    return inodeIds;
  }

  @Override
  public String toString() {
    if(inodeIdentifiers != null){
      return "Batch INode Lock = { "+inodeIdentifiers.size()+" inodes locked "+" }";
    }
    return "No inodes selected for batch locking";
  }
}
