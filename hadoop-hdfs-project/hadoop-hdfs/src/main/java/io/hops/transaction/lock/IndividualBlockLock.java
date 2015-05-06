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

import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;

class IndividualBlockLock extends BaseIndividualBlockLock {

  private final static long NON_EXISTING_BLOCK = Long.MIN_VALUE;
  protected final long blockId;
  protected final int inodeId;

  public IndividualBlockLock() {
    this.blockId = NON_EXISTING_BLOCK;
    this.inodeId = INode.NON_EXISTING_ID;
  }

  IndividualBlockLock(long blockId, INodeIdentifier inode) {
    this.blockId = blockId;
    this.inodeId = inode == null ? INode.NON_EXISTING_ID : inode.getInodeId();
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    if (blockId != NON_EXISTING_BLOCK) {
      BlockInfo result =
          acquireLock(DEFAULT_LOCK_TYPE, BlockInfo.Finder.ByBlockIdAndINodeId,
              blockId, inodeId);
      if (result != null) {
        blocks.add(result);
      } else {
        announceBlockDoesNotExist();
      }
    }
  }

  private void announceBlockDoesNotExist() throws TransactionContextException {
    EntityManager.snapshotMaintenance
        (HdfsTransactionContextMaintenanceCmds.BlockDoesNotExist, blockId, inodeId);
  }

  protected void announceEmptyFile(int inodeFileId) throws TransactionContextException {
    EntityManager.snapshotMaintenance
        (HdfsTransactionContextMaintenanceCmds.EmptyFile, inodeFileId);
  }

}
