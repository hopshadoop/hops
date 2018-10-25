/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.HashBucketDataAccess;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.metadata.hdfs.entity.HashBucket.PrimaryKey;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashBucketContext
    extends BaseEntityContext<PrimaryKey, HashBucket> {
  
  private HashBucketDataAccess dataAccess;
  private Map<Integer,Collection<HashBucket>> lockedBuckets = new HashMap<>();
  
  public HashBucketContext(HashBucketDataAccess hashBucketDataAccess) {
    this.dataAccess = hashBucketDataAccess;
  }
  
  @Override
  public HashBucket find(FinderType<HashBucket> finder, Object... params)
      throws TransactionContextException, StorageException {
    HashBucket.Finder hbFinder = (HashBucket.Finder) finder;
    switch (hbFinder){
      case ByStorageIdAndBucketId:
        return findByPrimaryKey(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  @Override
  public Collection<HashBucket> findList(FinderType<HashBucket> finder, Object... params)
      throws TransactionContextException, StorageException {
    HashBucket.Finder hbFinder = (HashBucket.Finder) finder;
    switch (hbFinder){
      case ByStorageId:
        return findByStorageId(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  private HashBucket findByPrimaryKey(HashBucket.Finder hbFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    int storageId = (Integer) params[0];
    int bucketId = (Integer) params[1];
    HashBucket.PrimaryKey pk = new HashBucket.PrimaryKey(storageId,
        bucketId);
    HashBucket result;
    if (contains(pk)){
      result = get(pk);
      hit(hbFinder, result, "sid", storageId, "bucketId", bucketId);
    } else {
      aboutToAccessStorage(hbFinder, params);
      result = (HashBucket) dataAccess.findBucket(storageId, bucketId);
      gotFromDB(pk, result);
      miss(hbFinder, result, "sid", storageId, "bucketId", bucketId);
    }
    
    return result;
  }
  
  private Collection<HashBucket> findByStorageId(HashBucket.Finder hbFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    int storageId = (Integer) params[0];
    
    Collection<HashBucket> result;
    if (lockedBuckets.containsKey(storageId)){
      result = lockedBuckets.get(storageId);
      hit(hbFinder, result, "sid", storageId);
    } else {
      aboutToAccessStorage(hbFinder, params);
      result = dataAccess.findBucketsByStorageId(storageId);
      gotFromDB(result);
      lockedBuckets.put(storageId, result);
      miss(hbFinder, result, "sid", storageId);
    }
    
    return result;
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(),getModified());
  }
  
  @Override
  HashBucket.PrimaryKey getKey(HashBucket hashBucket) {
    return new HashBucket.PrimaryKey(hashBucket.getStorageId(),hashBucket
        .getBucketId());
  }
}