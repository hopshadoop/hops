/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.lock;

import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;

class BlockChecksumLock extends Lock {
  private final String target;
  private final int blockIndex;

  BlockChecksumLock(String target, int blockIndex) {
    this.target = target;
    this.blockIndex = blockIndex;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {

    BaseINodeLock iNodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    INode iNode = iNodeLock.getTargetINode(target);
    if (!BaseINodeLock.isStoredInDB(iNode)) {
      BlockChecksumDataAccess.KeyTuple key =
              new BlockChecksumDataAccess.KeyTuple(iNode.getId(), blockIndex);
      acquireLock(DEFAULT_LOCK_TYPE, BlockChecksum.Finder.ByKeyTuple, key);
    } else {
      LOG.debug("Stuffed Inode:  BlockChecksumLock. Skipping acquring locks on the inode named: " + iNode.getLocalName() + " as the file is stored in the database");
    }

  }

  @Override
  protected final Type getType() {
    return Type.BlockChecksum;
  }
}
