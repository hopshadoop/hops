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

import io.hops.metadata.hdfs.entity.Ace;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;

public class AcesLock extends Lock {
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    for (INode inode : inodeLock.getAllResolvedINodes()){
      if(inode.getNumAces() > 0){
        //Retrieve aces for inode
        int[] indices = new int[inode.getNumAces()];
        for (int i = 0 ; i < inode.getNumAces() ; i++ ){
          indices[i] = i;
        }
        acquireLockList(TransactionLockTypes.LockType.WRITE, Ace.Finder.ByInodeIdAndIndices, inode.getId(), indices);
      }
    }
  }
  
  @Override
  protected Type getType() {
    return Type.Ace;
  }
}
