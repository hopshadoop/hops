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


import java.io.IOException;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

public final class SqlBatchedBlocksLock extends BaseIndividualBlockLock {

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    Lock inodeLock = locks.getLock(Type.INode);
    long[] inodeIds = null;
    if (inodeLock instanceof BatchedINodeLock) {
      inodeIds = ((BatchedINodeLock) inodeLock).getINodeIds();
    } else if (inodeLock instanceof MultipleINodesLock) {
      inodeIds = ((MultipleINodesLock) inodeLock).getINodeIds();
    } else {
      throw new TransactionLocks.LockNotAddedException(
              "HopsBatchedINodeLock wasn't added");
    }
    blocks.addAll(
            acquireLockList(DEFAULT_LOCK_TYPE, BlockInfoContiguous.Finder.ByINodeIds,
                    inodeIds));
  }
}
