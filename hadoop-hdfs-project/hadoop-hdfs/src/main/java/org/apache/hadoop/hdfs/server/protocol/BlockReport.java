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
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.Replica;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.hadoop.hdfs.server.protocol.BlockReportBlockState.FINALIZED;

public class BlockReport implements Iterable<BlockReportBlock> {
  
  private BlockReportBucket[] buckets;
  private long[] hashes;
  private int numBlocks;
  
  public BlockReportBucket[] getBuckets(){
    return buckets;
  }
  
  public long[] getHashes(){
    return hashes;
  }
  
  public int getNumBlocks(){
    return numBlocks;
  }
  
  public BlockReport(BlockReportBucket[] buckets, long[] hashes, int
      numBlocks){
    this.buckets = buckets;
    this.hashes = hashes;
    this.numBlocks = numBlocks;
  }
 
  //TODO: THIS WILL BE REMOVED WITH HOPS-64
  public Object[] getBlocksAndIdsAndStates(int startIndex, int endIndex){
    int size = endIndex - startIndex;
    Block[] blocks = new Block[size];
    long[] blockIds = new long[size];
    HdfsServerConstants.ReplicaState[] blockStates = new HdfsServerConstants
        .ReplicaState[size];
    Iterator<BlockReportBlock> iterator = iterator();
    for (int i = 0; i < startIndex ; i++){
      //Skip the first startIndex blocks
      iterator.next();
    }
    for (int index = startIndex ; index < endIndex ; index++){
      BlockReportBlock current = iterator.next();
      
      //Create and store block
      Block block = new Block();
      block.setNoPersistance(current.getBlockId(),current.getLength(),current.getGenerationStamp());
      blocks[index - startIndex] = block;
      
      //Store block id
      blockIds[index-startIndex] = block.getBlockId();
      
      //Store block state
      blockStates[index-startIndex] = fromBlockReportBlockState(current
          .getState());
    }
    
    return new Object[]{blockIds, blocks, blockStates};
  }
  
  private HdfsServerConstants.ReplicaState fromBlockReportBlockState
      (BlockReportBlockState state){
    switch (state){
      case FINALIZED:
        return HdfsServerConstants.ReplicaState.FINALIZED;
      case RBW:
        return HdfsServerConstants.ReplicaState.RBW;
      case RUR:
        return HdfsServerConstants.ReplicaState.RUR;
      case RWR:
        return HdfsServerConstants.ReplicaState.RWR;
      case TEMPORARY:
        return HdfsServerConstants.ReplicaState.TEMPORARY;
      default:
        throw new RuntimeException("Unimplemented state");
    }
  }
  
  @VisibleForTesting
  public Iterable<Block> blockIterable() {
    
    return new Iterable<Block>() {
      @Override
      public Iterator<Block> iterator() {
        return new Iterator<Block>() {
          Iterator<BlockReportBlock> it = BlockReport.this.iterator();
          
          @Override
          public boolean hasNext() {
            return it.hasNext();
          }
          
          @Override
          public Block next() {
            BlockReportBlock next = it.next();
            return new Block(next.getBlockId(), next.getLength(), next
                .getGenerationStamp());
          }
        };
      }
    };
  }
  
  @Override
  public Iterator<BlockReportBlock> iterator() {
    return new BlockReportBlockIterator();
  }
  
  public static Builder builder(int numBuckets){
    return new Builder(numBuckets);
  }
  
  public static int bucket(Replica replica, int numBuckets){
    return bucket(replica.getBlockId(), numBuckets);
  }
  
  private static int bucket(Block block, int numBuckets){
    return bucket(block.getBlockId(), numBuckets);
  }
  
  private static int bucket(long blockId, int numBuckets){
    int reminder = (int)(blockId % numBuckets);
    return reminder >= 0 ? reminder : numBuckets + reminder;
    
  }
  
  private static long hashAsFinalized(Block theBlock) {
    return hash(theBlock.getBlockId(), theBlock.getGenerationStamp(),
        theBlock.getNumBytes(), HdfsServerConstants.ReplicaState.FINALIZED
            .getValue());
  }
  
  private static long hash(Replica replica){
    return hash(replica.getBlockId(), replica.getGenerationStamp(), replica
        .getNumBytes(), replica.getState().getValue());
  }
  
  private static long hash(long blockId, long generationStamp, long numBytes,
      int replicaState){
    return Hashing.md5().newHasher()
        .putLong(blockId)
        .putLong(generationStamp)
        .putLong(numBytes)
        .putInt(replicaState)
        .hash().asLong();
  }
  
  public static class Builder {
    private final int NUM_BUCKETS;
    private ArrayList<BlockReportBlock>[] buckets;
    private long[] hashes;
    private int blockCounter = 0;
  
    private Builder(int numBuckets) {
      NUM_BUCKETS = numBuckets;
      buckets = new ArrayList[NUM_BUCKETS];
      hashes = new long[NUM_BUCKETS];
      for (int i = 0; i < NUM_BUCKETS; i++) {
        buckets[i] = new ArrayList<>();
      }
    }
  
    public Builder add(Replica replica) {
      int bucket = bucket(replica, NUM_BUCKETS);
      buckets[bucket].add(new BlockReportBlock(replica.getBlockId(), replica
          .getGenerationStamp(), replica.getNumBytes(), fromReplicaState(
          replica.getState())));
      hashes[bucket] += hash(replica);
      blockCounter++;
      return this;
    }
    
    public Builder addAllAsFinalized(List<Block> blocks){
      for (Block block : blocks){
        addAsFinalized(block);
      }
      return this;
    }
    
    public Builder addAsFinalized(Block theBlock) {
      int bucket = bucket(theBlock, NUM_BUCKETS);
      buckets[bucket].add(new BlockReportBlock(theBlock.getBlockId(),
          theBlock.getGenerationStamp(), theBlock.getNumBytes(),
          FINALIZED));
      hashes[bucket] += hashAsFinalized(theBlock);
      blockCounter++;
      return this;
    }
  
  
    public BlockReport build(){
      BlockReportBucket[] bucketArray = new BlockReportBucket[NUM_BUCKETS];
      for (int i = 0; i < NUM_BUCKETS; i++){
        bucketArray[i] = new BlockReportBucket(buckets[i].toArray(new
            BlockReportBlock[buckets[i].size()]));
      }
      return new BlockReport(bucketArray, hashes, blockCounter);
    }
  
  
    private BlockReportBlockState fromReplicaState(HdfsServerConstants
        .ReplicaState state) {
      switch (state) {
        case FINALIZED:
          return FINALIZED;
        case RBW:
          return BlockReportBlockState.RBW;
        case RUR:
          return BlockReportBlockState.RUR;
        case RWR:
          return BlockReportBlockState.RWR;
        case TEMPORARY:
          return BlockReportBlockState.TEMPORARY;
        default:
          throw new RuntimeException("Unimplemented state");
      }
    }
  
  }
  
  private class BlockReportBlockIterator implements Iterator<BlockReportBlock>{
  
    int currentBucket;
    int currentBucketOffset;
  
    BlockReportBlockIterator(){
      currentBucket = 0;
      currentBucketOffset = 0;
    }
    
    @Override
    public boolean hasNext() {
      if (currentBucket < buckets.length){
        if(currentBucketOffset < buckets[currentBucket].getBlocks().length){
          return true;
        } else {
          currentBucket++;
          currentBucketOffset=0;
          return hasNext();
        }
      }
      return false;
    }
  
    @Override
    public BlockReportBlock next() {
      if (hasNext()) {
        return buckets[currentBucket].getBlocks()[currentBucketOffset++];
      } else {
        throw new NoSuchElementException();
      }
    }
  
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not allowed to remove blocks" +
          " from blockReport.");
    }
  }
}
