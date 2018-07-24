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

import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.HashBuckets;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LastBlockReplicasHashBucketLock extends Lock {
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BlockLock blockLock = (BlockLock) locks.getLock(Type.Block);
    for (INodeFile iNodeFile : blockLock.getFiles()) {
      Block lastBlock = iNodeFile.getLastBlock();
      if (iNodeFile.getLastBlock() != null) {
        List<Replica> replicas = (List<Replica>) EntityManager
            .findList(Replica.Finder.ByBlockIdAndINodeId,
                lastBlock.getBlockId(),
                iNodeFile.getId());
        if (replicas != null) {
          Collections.sort(replicas, new Comparator<Replica>() {
            @Override
            public int compare(Replica o1, Replica o2) {
              return new Integer(o1.getStorageId()).compareTo(o2.getStorageId());
            }
          });

          //FIXME-BR why lock buckets for all the replicas. Only the last
          // replica should be locked. Am I missing something
          final int bucketId = HashBuckets.getInstance().getBucketForBlock(lastBlock);
          for (Replica replica : replicas) {
            EntityManager.find(HashBucket.Finder.ByStorageIdAndBucketId, replica
                .getStorageId(), bucketId);
          }
        }
      }
    }
  }
  
  @Override
  protected Type getType() {
    return Type.HashBucket;
  }
}
