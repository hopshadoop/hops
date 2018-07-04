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

import com.google.common.collect.Iterables;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class BlockLock extends IndividualBlockLock {

  private final List<INodeFile> files;

  BlockLock() {
    super();
    this.files = new ArrayList<>();
  }

  BlockLock(long blockId, INodeIdentifier inode) {
    super(blockId, inode);
    this.files = new ArrayList<>();
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    boolean individualBlockAlreadyRead = false;
    Iterable blks = Collections.EMPTY_LIST;
    for (INode inode : inodeLock.getAllResolvedINodes()) {
      if(BaseINodeLock.isStoredInDB(inode)){
        LOG.debug("Stuffed Inode:  BlockLock. Skipping acquring locks on the inode named: "+inode.getLocalName()+" as the file is stored in the database");
        announceEmptyFile(inode.getId());
        continue;
      }
      if (inode instanceof INodeFile) {
        Collection<BlockInfo> inodeBlocks = Collections.EMPTY_LIST;
        if(((INodeFile) inode).hasBlocks()) {
          inodeBlocks =
                  acquireLockList(DEFAULT_LOCK_TYPE, BlockInfo.Finder.ByINodeId,
                          inode.getId());
        }

        if (!individualBlockAlreadyRead) {
          individualBlockAlreadyRead = inode.getId() == inodeId;
        }

        if (inodeBlocks == null || inodeBlocks.isEmpty()) {
          announceEmptyFile(inode.getId());
        }

        blks = Iterables.concat(blks, inodeBlocks);
        files.add((INodeFile) inode);
      }
    }

    if (!individualBlockAlreadyRead) {
      super.acquire(locks);
    }

  }

  Collection<INodeFile> getFiles() {
    return files;
  }
}
