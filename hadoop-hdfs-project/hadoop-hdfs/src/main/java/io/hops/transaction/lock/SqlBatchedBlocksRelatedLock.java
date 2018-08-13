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

import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.entity.CachedBlock;
import io.hops.metadata.hdfs.entity.CorruptReplica;
import io.hops.metadata.hdfs.entity.ExcessReplica;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.hdfs.entity.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;

import java.io.IOException;

final class SqlBatchedBlocksRelatedLock extends LockWithType {

  SqlBatchedBlocksRelatedLock(Type type) {
    super(type);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    Lock inodeLock = locks.getLock(Type.INode);
    if (inodeLock instanceof BatchedINodeLock) {
      int[] inodeIds = ((BatchedINodeLock) inodeLock).getINodeIds();
      acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(), inodeIds);
    } else {
      throw new TransactionLocks.LockNotAddedException(
          "Batched Inode Lock wasn't added");
    }
  }

  private FinderType getFinderType() {
    switch (getType()) {
      case Replica:
        return Replica.Finder.ByINodeIds;
      case CorruptReplica:
        return CorruptReplica.Finder.ByINodeIds;
      case ExcessReplica:
        return ExcessReplica.Finder.ByINodeIds;
      case ReplicaUnderConstruction:
        return ReplicaUnderConstruction.Finder.ByINodeIds;
      case InvalidatedBlock:
        return InvalidatedBlock.Finder.ByINodeIds;
      case UnderReplicatedBlock:
        return UnderReplicatedBlock.Finder.ByINodeIds;
      case PendingBlock:
        return PendingBlockInfo.Finder.ByINodeIds;
      case CachedBlock :
        return CachedBlock.Finder.ByInodeIds;
    }
    return null;
  }
}
