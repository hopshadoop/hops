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
import io.hops.metadata.hdfs.entity.HashBucket;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.HashBuckets;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.Replica;

import java.util.*;

import static org.apache.hadoop.hdfs.server.protocol.BlockReportBlockState.FINALIZED;

public class BlockReport implements Iterable<ReportedBlock> {
  
  private Bucket[] buckets;
  private int numBlocks;
  
  public Bucket[] getBuckets(){
    return buckets;
  }
  
  public int getNumberOfBlocks(){
    return numBlocks;
  }
  
  public BlockReport(Bucket[] buckets, int numBlocks){
    this.buckets = buckets;
    this.numBlocks = numBlocks;
  }
 
  @VisibleForTesting
  public Iterable<Block> blockIterable() {
    
    return new Iterable<Block>() {
      @Override
      public Iterator<Block> iterator() {
        return new Iterator<Block>() {
          Iterator<ReportedBlock> it = BlockReport.this.iterator();
          
          @Override
          public boolean hasNext() {
            return it.hasNext();
          }
          
          @Override
          public Block next() {
            ReportedBlock next = it.next();
            return new Block(next.getBlockId(), next.getLength(), next
                .getGenerationStamp());
          }
        };
      }
    };
  }
  
  @Override
  public Iterator<ReportedBlock> iterator() {
    return new BlockReportIterator();
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

  /**
   * Corrupt the generation stamp of the block with the given index.
   * Not meant to be used outside of tests.
   */
  @VisibleForTesting
  public BlockReport corruptBlockGSForTesting(final int blockIndex, Random rand) {
    Builder corruptReportBuilder = builder(buckets.length);
    int i = 0;
    for (ReportedBlock reportedBlock : this){
      ReportedBlock toAdd;
      if (i == blockIndex){
        toAdd = new ReportedBlock(reportedBlock.getBlockId(), rand.nextInt(), reportedBlock.getLength(), reportedBlock.getState());
      } else {
        toAdd = reportedBlock;
      }
      corruptReportBuilder.add(toAdd);
      i++;
    }
    return corruptReportBuilder.build();
  }

  /**
   * Corrupt the length of the block with the given index by truncation.
   * Not meant to be used outside of tests.
   */
  @VisibleForTesting
  public BlockReport corruptBlockLengthForTesting(final int blockIndex, Random rand) {
    Builder corruptReportBuilder = builder(buckets.length);
    int i = 0;
    for (ReportedBlock reportedBlock : this){
      ReportedBlock toAdd;
      if (i == blockIndex){
        toAdd = new ReportedBlock(reportedBlock.getBlockId(), reportedBlock.getGenerationStamp(), rand.nextInt(), reportedBlock.getState());
      } else {
        toAdd = reportedBlock;
      }
      corruptReportBuilder.add(toAdd);
      i++;
    }
    return corruptReportBuilder.build();
  }

  private static byte[] hashAsFinalized(Block theBlock) {
    return HashBuckets.hash(theBlock.getBlockId(), theBlock.getGenerationStamp(),
        theBlock.getNumBytes(), HdfsServerConstants.ReplicaState.FINALIZED
            .getValue());
  }
  
  public static byte[] hashAsFinalized(ReportedBlock block){
    Block toHash = new Block(block.getBlockId(), block.getLength(),
        block.getGenerationStamp());
    return hashAsFinalized(toHash);
  }
  private static byte[] hash(Replica replica){
    return HashBuckets.hash(replica.getBlockId(), replica.getGenerationStamp(), replica
        .getNumBytes(), replica.getState().getValue());
  }
  
  public static byte[] hash(Block block, HdfsServerConstants.ReplicaState state){
    return HashBuckets.hash(block.getBlockId(), block.getGenerationStamp(), block
        .getNumBytes(), state.getValue());
  }

  public static class Builder {
    private final int NUM_BUCKETS;
    private ArrayList<ReportedBlock>[] buckets;
    private byte[][] hashes;
    private int blockCounter = 0;
  
    private Builder(int numBuckets) {
      NUM_BUCKETS = numBuckets;
      buckets = new ArrayList[NUM_BUCKETS];
      hashes = new byte[NUM_BUCKETS][HashBuckets.HASH_LENGTH];
      for (int i = 0; i < NUM_BUCKETS; i++) {
        buckets[i] = new ArrayList<>();
      }
    }

    @VisibleForTesting
    public Builder add(ReportedBlock reportBlock){
      int bucket = bucket(reportBlock.getBlockId(), NUM_BUCKETS);
      buckets[bucket].add(reportBlock);
      HdfsServerConstants.ReplicaState replicaState = null;
      switch (reportBlock.getState()){
        case FINALIZED:
          replicaState = HdfsServerConstants.ReplicaState.FINALIZED;
          break;
        case RBW:
          replicaState = HdfsServerConstants.ReplicaState.RBW;
          break;
        case RUR:
          replicaState = HdfsServerConstants.ReplicaState.RUR;
          break;
        case RWR:
          replicaState = HdfsServerConstants.ReplicaState.RWR;
          break;
        case TEMPORARY:
          replicaState = HdfsServerConstants.ReplicaState.TEMPORARY;
          break;
      }
      byte[] hash = HashBuckets.hash(reportBlock.getBlockId(),reportBlock.getGenerationStamp(),reportBlock.getLength(),
              replicaState.getValue());
      HashBuckets.XORHashes(hashes[bucket], hash);
      blockCounter++;
      return this;
    }
  
    public Builder add(Replica replica) {
      int bucket = bucket(replica, NUM_BUCKETS);
      buckets[bucket].add(new ReportedBlock(replica.getBlockId(), replica
          .getGenerationStamp(), replica.getNumBytes(), fromReplicaState(
          replica.getState())));
      HashBuckets.XORHashes(hashes[bucket], hash(replica));
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
      buckets[bucket].add(new ReportedBlock(theBlock.getBlockId(),
          theBlock.getGenerationStamp(), theBlock.getNumBytes(),
          FINALIZED));
      HashBuckets.XORHashes(hashes[bucket], hashAsFinalized(theBlock));
      blockCounter++;
      return this;
    }
  
  
    public BlockReport build(){
      Bucket[] bucketArray = new Bucket[NUM_BUCKETS];
      for (int i = 0; i < NUM_BUCKETS; i++){
        bucketArray[i] = new Bucket(buckets[i].toArray(new
                ReportedBlock[buckets[i].size()]));
        bucketArray[i].setHash(hashes[i]);
      }
      return new BlockReport(bucketArray, blockCounter);
    }
  
  
    private BlockReportBlockState fromReplicaState(HdfsServerConstants
        .ReplicaState state) {
      switch (state) {
        case FINALIZED:
          return BlockReportBlockState.FINALIZED;
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
  
  private class BlockReportIterator implements Iterator<ReportedBlock>{
  
    int currentBucket;
    int currentBucketOffset;
  
    BlockReportIterator(){
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
    public ReportedBlock next() {
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
