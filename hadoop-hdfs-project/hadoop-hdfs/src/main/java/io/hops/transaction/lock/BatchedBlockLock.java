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
import io.hops.common.Pair;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

import java.io.IOException;

final class BatchedBlockLock extends BaseIndividualBlockLock {

  private final long[] blockIds;
  private int[] inodeIds;

  public BatchedBlockLock(long[] blockIds, int[] inodeIds) {
    this.blockIds = blockIds;
    this.inodeIds = inodeIds;
  }

  public BatchedBlockLock(long[] blockIds) {
    this(blockIds, null);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (inodeIds == null) {
      inodeIds = INodeUtil.resolveINodesFromBlockIds(blockIds);
    }
    blocks.addAll(acquireLockList(DEFAULT_LOCK_TYPE,
        BlockInfo.Finder.ByBlockIdsAndINodeIds, blockIds, inodeIds));
  }
  
  Pair<int[], long[]> getINodeBlockIds() {
    return new Pair<int[], long[]>(inodeIds, blockIds);
  }
  
}
