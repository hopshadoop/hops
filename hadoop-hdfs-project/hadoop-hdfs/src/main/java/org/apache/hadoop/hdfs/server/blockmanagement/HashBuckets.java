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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.HashBucketDataAccess;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;

import java.io.IOException;
import java.util.List;

public class HashBuckets {
  
  static final Log LOG = LogFactory.getLog(HashBuckets.class);
  
  private static HashBuckets instance;
  private int numBuckets;
  
  public static void initialize(int numBuckets){
    if (instance != null){
      //TODO log warning
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
  
  List<HashBucket> getBucketsForDatanode(final DatanodeStorageInfo storage)
      throws IOException {
    LightWeightRequestHandler findHashesHandler = new
        LightWeightRequestHandler(HDFSOperationType.GET_ALL_MACHINE_HASHES) {
      @Override
      public Object performTask() throws IOException {
        HashBucketDataAccess da = (HashBucketDataAccess) HdfsStorageFactory.getDataAccess
            (HashBucketDataAccess.class);
        return da.findBucketsByStorageId(storage.getSid());
      }
    };
    
    return (List<HashBucket>) findHashesHandler.handle();
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
}
