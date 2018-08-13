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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.io.IOException;

final class BlockRelatedLock extends LockWithType {

  BlockRelatedLock(Type type) {
    super(type);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    // FIXME handle null block
    Lock lock = locks.getLock(Type.Block);
    if (lock instanceof BaseIndividualBlockLock) {
      BaseIndividualBlockLock individualBlockLock =
              (BaseIndividualBlockLock) lock;
      //get by blocksId
      for (BlockInfo blk : individualBlockLock.getBlocks()) {
        if (isList()) {
          acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(true),
                  blk.getBlockId(), blk.getInodeId());
        } else {
          acquireLock(DEFAULT_LOCK_TYPE, getFinderType(true), blk.getBlockId(),
                  blk.getInodeId());
        }
      }
      if (lock instanceof BlockLock) {
        //get by inodeId
        BlockLock blockLock = (BlockLock) lock;
        for (INodeFile file : blockLock.getFiles()) {
          if(!file.isFileStoredInDB()) {
            acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(false),
                    file.getId());
          }else{
            LOG.debug("Stuffed Inode:  BlockRelateLock. " + getType() + "'s lock skipped as the file(s) data is stored in the database. File Name: "+file.getLocalName());
          }
        }
      }
    } else {
      throw new TransactionLocks.LockNotAddedException(
              "Block Lock wasn't added");
    }
  }

  private FinderType getFinderType(boolean byBlockID) {
    switch (getType()) {
      case Replica:
        return byBlockID ? Replica.Finder.ByBlockIdAndINodeId :
                Replica.Finder.ByINodeId;
      case CorruptReplica:
        return byBlockID ? CorruptReplica.Finder.ByBlockIdAndINodeId :
                CorruptReplica.Finder.ByINodeId;
      case ExcessReplica:
        return byBlockID ? ExcessReplica.Finder.ByBlockIdAndINodeId :
                ExcessReplica.Finder.ByINodeId;
      case ReplicaUnderConstruction:
        return byBlockID ? ReplicaUnderConstruction.Finder.ByBlockIdAndINodeId :
                ReplicaUnderConstruction.Finder.ByINodeId;
      case InvalidatedBlock:
        return byBlockID ? InvalidatedBlock.Finder.ByBlockIdAndINodeId :
                InvalidatedBlock.Finder.ByINodeId;
      case UnderReplicatedBlock:
        return byBlockID ? UnderReplicatedBlock.Finder.ByBlockIdAndINodeId :
                UnderReplicatedBlock.Finder.ByINodeId;
      case PendingBlock:
        return byBlockID ? PendingBlockInfo.Finder.ByBlockIdAndINodeId :
                PendingBlockInfo.Finder.ByINodeId;
      case CachedBlock:
        return byBlockID ? CachedBlock.Finder.ByBlockIdAndInodeId : CachedBlock.Finder.ByInodeId;
    }
    return null;
  }

  private boolean isList() {
    switch (getType()) {
      case UnderReplicatedBlock:
      case PendingBlock:
        return false;
      default:
        return true;
    }
  }

}
