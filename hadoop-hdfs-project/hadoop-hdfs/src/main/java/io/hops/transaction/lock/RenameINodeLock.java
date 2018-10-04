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

import io.hops.leader_election.node.ActiveNode;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

final class RenameINodeLock extends INodeLock {

  private static final Comparator PATH_COMPARTOR = new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
      String[] o1Path = INode.getPathNames(o1);
      String[] o2Path = INode.getPathNames(o2);
      if (o1Path.length > o2Path.length) {
        return 1;
      } else if (o1Path.length == o2Path.length) {
        return o1.compareTo(o2);
      }
      return -1;
    }
  };
  private final boolean legacyRename;

  public RenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String src, String dst,
      boolean legacyRename) {
    super(lockType, resolveType, src, dst);
    this.legacyRename = legacyRename;
  }

  public RenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String src, String dst) {
    this(lockType, resolveType, src, dst, false);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    //consider src = /a/b/c and dst = /d
    //during the acquire lock of src write locks will be acquired on parent of c and c
    //during the acquire lock of dst write lock on the root will be acquired but the snapshot 
    //layer will not let the request go to the db as it has already cached the root inode
    //one simple solution is that to acquire lock on the short path first
    //setPartitioningKey(PathMemcache.getInstance().getPartitionKey(locks.getInodeParam()[0]));
    String src = paths[0];
    String dst = paths[1];
    Arrays.sort(paths, PATH_COMPARTOR);
    acquirePathsINodeLocks();

    if (legacyRename) // In deprecated rename, it allows to move a dir to an existing destination.
    {
      List<INode> dstINodes = getPathINodes(dst);
      String[] dstComponents = INode.getPathNames(dst);
      String[] srcComponents = INode.getPathNames(src);
      INode lastComp = dstINodes.get(dstINodes.size() - 1);

      if (dstINodes.size() == dstComponents.length && lastComp.isDirectory()) {
        //the dst exist and is a directory.
        long parttitionId = INode.calculatePartitionId(lastComp.getId(), srcComponents[srcComponents.length - 1],
            (short)(lastComp.myDepth()+1));
        find(srcComponents[srcComponents.length - 1], lastComp.getId(), parttitionId);
      }
    }

    if(!skipReadingQuotaAttr) {
      acquireINodeAttributes();
    }
  }
}
