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
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.List;

final class QuotaUpdateLock extends Lock {
  private final String[] targets;
  private final boolean includeChildren;
  private final List<QuotaUpdate> updates;
  
  QuotaUpdateLock(boolean includeChildren, String... targets) {
    this.includeChildren = includeChildren;
    this.targets = targets;
    updates = null;
  }

  QuotaUpdateLock(String... paths) {
    this(false, paths);
  }

  QuotaUpdateLock(List<QuotaUpdate> updates) {
    this.includeChildren=false;
    this.targets=null;
    this.updates = updates;
  }
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (targets != null) {
      INodeLock inodeLock = (INodeLock) locks.getLock(Type.INode);
      for (String target : targets) {
        acquireQuotaUpdate(inodeLock.getTargetINode(target));
        if (includeChildren) {
          acquireQuotaUpdate(inodeLock.getChildINodes(target));
        }
      }
    } else if (updates!=null){
      for(QuotaUpdate update : updates){
        acquireQuotaUpdate(update);
      }
    }
  }

  private void acquireQuotaUpdate(List<INode> iNodes)
      throws StorageException, TransactionContextException {
    if(iNodes != null){
      for (INode iNode : iNodes) {
      acquireQuotaUpdate(iNode);
      }
    }
  }

  private void acquireQuotaUpdate(INode iNode)
      throws StorageException, TransactionContextException {
    acquireLockList(DEFAULT_LOCK_TYPE, QuotaUpdate.Finder.ByINodeId,
        iNode.getId());
  }
  
  private void acquireQuotaUpdate(QuotaUpdate update)
      throws StorageException, TransactionContextException {
    acquireLock(DEFAULT_LOCK_TYPE, QuotaUpdate.Finder.ByKey, update.getId(), update.getInodeId());
  }

  @Override
  protected final Type getType() {
    return Type.QuotaUpdate;
  }
}
