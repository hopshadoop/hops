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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.HashBucketDataAccess;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;

import java.io.IOException;
import java.util.*;

public class HashBuckets {
  
  static final Log LOG = LogFactory.getLog(HashBuckets.class);
  
  private static HashBuckets instance;
  private static int numBuckets;

  public static void initialize(int numBuckets){
    if (instance != null){
      LOG.warn("initialize called again after already initialized.");
    } else {
      instance = new HashBuckets(numBuckets);
    }
  }
  
  private HashBuckets(int numBuckets){
    this.numBuckets = numBuckets;
  }
  
  public static HashBuckets getInstance() {
    if (instance != null){
      return instance;
    } else {
      throw new RuntimeException("HashBuckets have not been initialized");
    }
  }
  
  public int getBucketForBlock(Block block){
    return (int) (block.getBlockId() % numBuckets);
  }
  
  public List<HashBucket> getBucketsForStorage(final DatanodeStorageInfo storage)
      throws IOException {
    LightWeightRequestHandler findHashesHandler = new
        LightWeightRequestHandler(HDFSOperationType.GET_STORAGE_HASHES) {
      @Override
      public Object performTask() throws IOException {
        HashBucketDataAccess da = (HashBucketDataAccess) HdfsStorageFactory.getDataAccess
            (HashBucketDataAccess.class);
        return da.findBucketsByStorageId(storage.getSid());
      }
    };
    
    return (List<HashBucket>) findHashesHandler.handle();
  }

  //only for testing
  @VisibleForTesting
  public void corruptHashBuckets(final DatanodeStorageInfo storage)
          throws IOException {
    Random rand = new Random(System.currentTimeMillis());
    LightWeightRequestHandler corruptedHashes = new
            LightWeightRequestHandler(HDFSOperationType.RESET_STORAGE_HASHES) {
              @Override
              public Object performTask() throws IOException {
                final List<HashBucket> newBuckets = new ArrayList<>();
                for(int i = 0; i < numBuckets; i++){
                    newBuckets.add(new HashBucket(storage.getSid(), i, rand.nextLong()));
                }
                HashBucketDataAccess da = (HashBucketDataAccess) HdfsStorageFactory.getDataAccess
                        (HashBucketDataAccess.class);
                da.prepare(Collections.EMPTY_LIST, newBuckets);
                return null;
              }
            };
    corruptedHashes.handle();
  }

  //only for testing
  @VisibleForTesting
  public void deleteHashBuckets(final DatanodeStorageInfo storage)
          throws IOException {
    LightWeightRequestHandler deleteHashes = new
            LightWeightRequestHandler(HDFSOperationType.RESET_STORAGE_HASHES) {
              @Override
              public Object performTask() throws IOException {
                final List<HashBucket> deleted = new ArrayList<HashBucket>();
                for(int i = 0; i < numBuckets; i++){
                  deleted.add(new HashBucket(storage.getSid(), i, 0));
                }
                HashBucketDataAccess da = (HashBucketDataAccess) HdfsStorageFactory.getDataAccess
                        (HashBucketDataAccess.class);
                da.prepare(deleted, Collections.EMPTY_LIST);
                return null;
              }
            };
    deleteHashes.handle();
  }

  public void createBucketsForStorage(final DatanodeStorageInfo storage)
          throws IOException {
    List<HashBucket> existing = getBucketsForStorage(storage);
    final Map<Integer,HashBucket> existingMap = new HashMap<Integer, HashBucket>();

    for(HashBucket bucket: existing){
      existingMap.put(bucket.getBucketId(), bucket);
    }

    final List<HashBucket> newBuckets = new ArrayList<HashBucket>();
    for(int i = 0; i < numBuckets; i++){
      if(!existingMap.containsKey(i)){
        newBuckets.add(new HashBucket(storage.getSid(), i, 0));
      }
    }

    LightWeightRequestHandler findHashesHandler = new
            LightWeightRequestHandler(HDFSOperationType.CREATE_ALL_STORAGE_HASHES) {
              @Override
              public Object performTask() throws IOException {
                HashBucketDataAccess da = (HashBucketDataAccess) HdfsStorageFactory.getDataAccess
                        (HashBucketDataAccess.class);
                da.prepare(Collections.EMPTY_LIST, newBuckets);
                LOG.debug("Created "+newBuckets.size()+" buckets for the " +
                        "storage "+storage+" Existing Buckets: "+existingMap.size());
                return null;
              }
            };
    findHashesHandler.handle();
  }


  HashBucket getBucket(int storageId, int bucketId)
      throws TransactionContextException, StorageException {
    HashBucket result = EntityManager.find(HashBucket.Finder
        .ByStorageIdAndBucketId, storageId, bucketId);
    if(result == null){
      result = new HashBucket(storageId, bucketId, 0);
    }
    return result;
  }
  
  Collection<HashBucket> getBuckets(int storageId)
      throws TransactionContextException, StorageException {
    Collection<HashBucket> result = EntityManager.findList(HashBucket.Finder.ByStorageId, storageId);
    return result;
  }
  
  private static String blockToString(Block block){
    return "(id: " + block.getBlockId() + ",#bytes: "+block.getNumBytes() +
        ",GS: " + block.getGenerationStamp()+")";
  }
  public void applyHash(int storageId, HdfsServerConstants.ReplicaState state,
      Block block ) throws TransactionContextException, StorageException {
    int bucketId = getBucketForBlock(block);
    HashBucket bucket = getBucket(storageId, bucketId);
    long newHash = bucket.getHash() + BlockReport.hash(block, state);
    LOG.debug("Applying block:" + blockToString
        (block) + "sid: " + storageId + "state: " + state.name() + ", hash: "
        + BlockReport.hash(block, state));
    
    bucket.setHash(newHash);
  }
  
  public void undoHash(int storageId, HdfsServerConstants.ReplicaState
      state, Block block) throws TransactionContextException, StorageException {
    int bucketId = getBucketForBlock(block);
    HashBucket bucket = getBucket(storageId, bucketId);
    long newHash = bucket.getHash() - BlockReport.hash(block, state);
    LOG.debug("Undo block:" + blockToString
        (block) + "sid: " + storageId + "state: " + state.name() + ", hash: " +
        BlockReport.hash(block,state));
    
    bucket.setHash(newHash);
  }
  
  public void resetBuckets(final int storageId) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RESET_STORAGE_HASHES) {
      @Override
      public void acquireLock(TransactionLocks tl) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        tl.add(lf.getHashBucketLock(storageId));
      }
      
      @Override
      public Object performTask() throws IOException {
        Collection<HashBucket> buckets = getBuckets(storageId);
        for(HashBucket bucket: buckets){
            bucket.setHash(0);
        }
        return null;
      }
    }.handle();
  }
}
