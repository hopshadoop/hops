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

import io.hops.common.Pair;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

abstract class BatchedBlocksRelatedLock extends Lock {
/*
  private final FinderType finder;
  private final int storageId;

  public BatchedBlocksRelatedLock(FinderType finder, int storageId) {
    this.finder = finder;
    this.storageId = storageId;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    Lock blockLock = locks.getLock(Type.Block);
    if (blockLock instanceof BatchedBlockLock) {
      Pair<int[], long[]> inodeBlockIds =
          ((BatchedBlockLock) blockLock).getINodeBlockIds();
      acquireLockList(DEFAULT_LOCK_TYPE, finder, inodeBlockIds.getR(),
          inodeBlockIds.getL(), storageId);
    } else {
      throw new TransactionLocks.LockNotAddedException(
          "HopsBatchedBlockLock wasn't added");
    }
  }

  final static class BatchedInvalidatedBlocksLock
      extends BatchedBlocksRelatedLock {

    public BatchedInvalidatedBlocksLock(int storageId) {
      super(InvalidatedBlock.Finder.ByStorageId, storageId);
    }

    @Override
    protected Type getType() {
      return Type.InvalidatedBlock;
    }
  }*/
}
