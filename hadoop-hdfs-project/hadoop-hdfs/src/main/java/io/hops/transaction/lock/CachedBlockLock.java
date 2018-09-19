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

import com.google.common.primitives.Longs;
import io.hops.common.INodeUtil;
import io.hops.metadata.hdfs.entity.CachedBlock;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

public class CachedBlockLock extends Lock {

  private long[] blockIds;
  private DatanodeID datanodeId;
  private Collection<CachedBlock> cachedBlocks;

  public CachedBlockLock(List<Long> blockIds, DatanodeID datanodeId) {
    this.blockIds = Longs.toArray(blockIds);
    this.datanodeId = datanodeId;
  }

  public CachedBlockLock(DatanodeID datanodeId) {
    this.datanodeId = datanodeId;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    cachedBlocks = acquireLockList(TransactionLockTypes.LockType.WRITE, CachedBlock.Finder.ByDatanodeId, datanodeId);
    if (blockIds != null) {
      long[] inodeIds = INodeUtil.resolveINodesFromBlockIds(blockIds);
      cachedBlocks.addAll(acquireLockList(TransactionLockTypes.LockType.WRITE,
          CachedBlock.Finder.ByBlockIdsAndINodeIds, blockIds, inodeIds, datanodeId));
    }

  }

  @Override
  protected Lock.Type getType() {
    return Lock.Type.CachedBlock;
  }

  public Collection<CachedBlock> getAllResolvedCachedBlock() {
    return cachedBlocks;
  }
}
