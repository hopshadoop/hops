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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.metadata.common.FinderType;

/**
 * An object that contains information about a block that is being replicated.
 * It records the timestamp when the system started replicating the most recent
 * copy of this block. It also records the number of replication requests that
 * are in progress.
 */
public class PendingBlockInfo {

  public static enum Finder implements FinderType<PendingBlockInfo> {

    ByBlockIdAndINodeId,
    ByINodeId,
    ByINodeIds,
    All;

    @Override
    public Class getType() {
      return PendingBlockInfo.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByBlockIdAndINodeId:
          return Annotation.PrimaryKey;
        case ByINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        case All:
          return Annotation.FullTable;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private long blockId;
  private int inodeId;
  private long timeStamp;
  private int numReplicasInProgress;

  public PendingBlockInfo(long blockId, int inodeId, long timestamp,
      int numReplicas) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.timeStamp = timestamp;
    this.numReplicasInProgress = numReplicas;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  void setTimeStamp(long timestamp) {
    timeStamp = timestamp;
  }

  void incrementReplicas(int increment) {
    numReplicasInProgress += increment;
  }

  void decrementReplicas() {
    numReplicasInProgress--;
    assert (numReplicasInProgress >= 0);
  }

  public int getNumReplicas() {
    return numReplicasInProgress;
  }

  /**
   * @return the blockId
   */
  public long getBlockId() {
    return blockId;
  }
  
  public int getInodeId() {
    return inodeId;
  }

  /**
   * @param blockId
   *     the blockId to set
   */
  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public void setInodeId(int inodeId) {
    this.inodeId = inodeId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof PendingBlockInfo)) {
      return false;
    }

    PendingBlockInfo other = (PendingBlockInfo) obj;
    if (this.getBlockId() == other.getBlockId()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 71 * hash + (int) (this.blockId ^ (this.blockId >>> 32));
    return hash;
  }
}
