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

import io.hops.metadata.hdfs.entity.QuotaUpdate;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;

final class QuotaUpdateLock extends Lock {
  private final int limit;
  private final long inodeID;
  
  // limit is needed as we may have lots of rows for this inode
  QuotaUpdateLock(final long inodeID, final int limit) {
    this.inodeID = inodeID;
    this.limit = limit;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (inodeID != INode.NON_EXISTING_INODE_ID) {
      acquireLockList(DEFAULT_LOCK_TYPE, QuotaUpdate.Finder.ByINodeId,
        inodeID, limit);
    }
  }

  @Override
  protected final Type getType() {
    return Type.QuotaUpdate;
  }
}
