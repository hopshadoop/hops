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

import io.hops.metadata.hdfs.entity.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Collection;

final class LastTwoBlocksLock extends IndividualBlockLock{

  private final String path;
  public LastTwoBlocksLock(String src){
    super();
    this.path = src;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    INodeLock iNodeLock = (INodeLock) locks.getLock(Type.INode);
    LeasePathLock leasePathLock = (LeasePathLock) locks.getLock(Type.LeasePath);
    Collection<LeasePath> lps = leasePathLock.getLeasePaths();
    for(LeasePath lp : lps){
      if(lp.getPath().equals(path)){
        INode targetInode = iNodeLock.getTargetINode(path);
        readBlock(lp.getLastBlockId(), targetInode.getId());
        readBlock(lp.getPenultimateBlockId(), targetInode.getId());

        if(getBlocks() == null || getBlocks().isEmpty()){
          announceEmptyFile(targetInode.getId());
        }

        break;
      }
    }
  }

}
