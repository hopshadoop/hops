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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class HashBucketsLocksAllFileBlocks extends LockWithType {

  HashBucketsLocksAllFileBlocks() {
    super(Type.HashBucket);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(TransactionLockTypes.LockType.WRITE);
    if (locks.containsLock(Type.Replica) || locks.containsLock(Type.ReplicaUnderConstruction)) {
      Collection<Object> blks = ((BlockRelatedLock) locks.getLock(Type.Replica)).getBlocks();
      blks.addAll(((BlockRelatedLock) locks.getLock(Type.ReplicaUnderConstruction)).getBlocks());

      List<Replica> replicas = new ArrayList<>();
      for (Object blk : blks) {
        Replica replica = (Replica) blk;
        replicas.add(replica);
      }
      //sort blks for total order of locks
      Collections.sort(replicas);

      for (Replica replica : replicas) {
        if (EntityManager.find(HashBucket.Finder.ByStorageIdAndBucketId,
                replica.getStorageId(), replica.getBucketId()) == null){
          EntityManager.update(new HashBucket(replica.getStorageId(),
                  replica.getBucketId(), 0));
          LOG.warn("The accessed bucket had not been initialized. There might be a misconfiguration.");
        }

      }
    }
  }

}

