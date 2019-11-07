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
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HashBuckets;

import java.io.IOException;

public class IndividualHashBucketLock extends Lock {
  private final int storageId;
  private final int bucketId;
  
  public IndividualHashBucketLock(int storageId, int bucketId) {
    this.storageId = storageId;
    this.bucketId = bucketId;
  }
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(TransactionLockTypes.LockType.WRITE);
    if (EntityManager.find(HashBucket.Finder.ByStorageIdAndBucketId,
        storageId, bucketId) == null){
      EntityManager.update(new HashBucket(storageId, bucketId, HashBuckets.initalizeHash()));
      LOG.warn("The accessed bucket (StorageID: "+storageId+" bucketID: "+bucketId+") has not " +
              "been initialized. There might be a misconfiguration.");
    }
  }
  
  @Override
  protected Type getType() {
    return Type.HashBucket;
  }

  public int getStorageId() {
    return storageId;
  }

  public int getBucketId() {
    return bucketId;
  }
}

