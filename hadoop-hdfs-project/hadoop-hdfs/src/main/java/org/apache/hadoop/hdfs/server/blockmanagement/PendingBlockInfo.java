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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  private long inodeId;
  private long timeStamp;
  private final List<String> targets;

  public PendingBlockInfo(long blockId, long inodeId, long timestamp,
      DatanodeDescriptor[] targets) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.timeStamp = timestamp;
    this.targets = new ArrayList<String>();
    if(targets!=null){
      for (DatanodeDescriptor dn : targets) {
          this.targets.add(dn.getDatanodeUuid());
      }
    }
  }

  public PendingBlockInfo(long blockId, long inodeId, long timestamp,
      List<String> targets) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.timeStamp = timestamp;
    this.targets = targets;
  }
  
  public PendingBlockInfo(long blockId, long inodeId, long timestamp,
      String target) {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.timeStamp = timestamp;
    this.targets = new ArrayList<>();
    this.targets.add(target);
  }
  
  public long getTimeStamp() {
    return timeStamp;
  }

  void setTimeStamp(long timestamp) {
    timeStamp = timestamp;
  }

  void incrementReplicas(DatanodeDescriptor... newTargets) {
      if (newTargets != null) {
        for (DatanodeDescriptor dn : newTargets) {
          targets.add(dn.getDatanodeUuid());
        }
      }
  }

  boolean decrementReplicas(DatanodeDescriptor dn) {
    return targets.remove(dn.getDatanodeUuid());
  }

  public int getNumReplicas() {
    return targets.size();
  }

  /**
   * @return the blockId
   */
  public long getBlockId() {
    return blockId;
  }
  
  public long getInodeId() {
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

  public List<String> getTargets() {
    return targets;
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
