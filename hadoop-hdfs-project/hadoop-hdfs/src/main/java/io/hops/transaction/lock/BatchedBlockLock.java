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

import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

final class BatchedBlockLock extends BaseIndividualBlockLock {

  private long[] blockIds;
  private long[] inodeIds;
  private final long[] unresolvedBlockIds;
  

  public BatchedBlockLock(long[] blockIds, long[] inodeIds, long[] unresolvedBlockIds) {
    this.blockIds = blockIds;
    this.inodeIds = inodeIds;
    this.unresolvedBlockIds = unresolvedBlockIds;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    //INodes are not locked in this case
    if (unresolvedBlockIds != null && unresolvedBlockIds.length != 0) {
      long[] inodeIdsForURBlks = INodeUtil.resolveINodesFromBlockIds(unresolvedBlockIds);
      blockIds = ArrayUtils.addAll(blockIds, unresolvedBlockIds);
      inodeIds = ArrayUtils.addAll(inodeIds, inodeIdsForURBlks);
    }
    
    blocks.addAll(acquireLockList(DEFAULT_LOCK_TYPE,
        BlockInfoContiguous.Finder.ByBlockIdsAndINodeIds, blockIds, inodeIds));
  }
  
  Pair<long[], long[]> getINodeBlockIds() {
    return new Pair<>(inodeIds, blockIds);
  }
  
}
