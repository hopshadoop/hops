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
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.util.StringUtils;

final class INodesLocks extends BaseINodeLock {

  private static final INodeIdentifier NON_EXISTING_INODE =
      new INodeIdentifier();
  
  private final TransactionLockTypes.INodeLockType lockType;
  private final List<INodeIdentifier> inodeIdentifiers;
  

  INodesLocks(TransactionLockTypes.INodeLockType lockType,
      List<INodeIdentifier> inodeIdentifiers) {
    this.lockType = lockType;
    this.inodeIdentifiers = inodeIdentifiers;
    
    if (lockType.equals(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT)) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (inodeIdentifiers == null || inodeIdentifiers.isEmpty()) {
      return;
    }

    Collections.sort(inodeIdentifiers);
    for (INodeIdentifier inodeIdentifier : inodeIdentifiers) {
      List<INode> resolvedINodes = resolveUsingCache(lockType, inodeIdentifier.getInodeId());
      
      String path = INodeUtil.constructPath(resolvedINodes);
      addPathINodesAndUpdateResolvingCache(path, resolvedINodes);

    }
    acquireINodeAttributes();

  }
  
  @Override
  public String toString() {
    if ( lockType != null && inodeIdentifiers != null && !inodeIdentifiers.isEmpty()){
      String ids = StringUtils.join(", ", inodeIdentifiers);
      return "InodesLocks = { identifiers: "+ ids + " Lock: "+lockType+" }";
    }
    return "Individual Inode Lock not set";
  }

}
