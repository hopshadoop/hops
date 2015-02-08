/**
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

import org.apache.hadoop.hdfs.server.namenode.INode;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

class BlockPK {
  private long blockId = Long.MIN_VALUE;
  private long inodeId = BlockInfoContiguous.NON_EXISTING_ID;

  BlockPK() {
  }

  BlockPK(Long blockId, Long inodeId) {
    if(blockId!=null){
      this.blockId = blockId;
    }
    if(inodeId!=null){
      this.inodeId = inodeId;
    }
  }

  boolean hasBlockId() {
    return this.blockId != Long.MIN_VALUE;
  }

  boolean hasINodeId() {
    return this.inodeId != BlockInfoContiguous.NON_EXISTING_ID;
  }

  long getBlockId() {
    return blockId;
  }

  long getInodeId() {
    return inodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockPK)) {
      return false;
    }

    BlockPK blockPK = (BlockPK) o;

    if (blockId != blockPK.blockId) {
      return false;
    }
    if (inodeId != blockPK.inodeId) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + Long.hashCode(inodeId);
    return result;
  }

  static class ReplicaPK extends BlockPK {
    private int storageId;

    ReplicaPK(long blockId, long inodeId, int storageId) {
      super(blockId, inodeId);
      this.storageId = storageId;
    }

    ReplicaPK(long blockId, int storageId) {
      super(blockId, null);
      this.storageId = storageId;
    }

    ReplicaPK(long inodeId) {
      super(null, inodeId);
    }

    int getStorageId() {
      return storageId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ReplicaPK)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      // So it's the same block, now check if it's the same replica
      ReplicaPK replicaPK = (ReplicaPK) o;
      return storageId == replicaPK.storageId;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + storageId;
      return result;
    }

    static List<ReplicaPK> getKeys(long[] blockIds, int storageId) {
      List<BlockPK.ReplicaPK> keys = new ArrayList<>(blockIds.length);
      for (long blockId : blockIds) {
        keys.add(new BlockPK.ReplicaPK(blockId, storageId));
      }
      return keys;
    }

    static List<BlockPK.ReplicaPK> getKeys(long[] inodeIds) {
      List<BlockPK.ReplicaPK> keys =
          new ArrayList<>(inodeIds.length);
      for (long inodeId : inodeIds) {
        keys.add(new BlockPK.ReplicaPK(inodeId));
      }
      return keys;
    }

    static List<ReplicaPK> getKeys(long[] blockIds, long[] inodeIds, int storageId) {
      List<BlockPK.ReplicaPK> keys = new ArrayList<>(blockIds.length);
      for (int i = 0; i < blockIds.length; i++) {
        keys.add(new BlockPK.ReplicaPK(blockIds[i], inodeIds[i], storageId));
      }
      return keys;
    }
  }

  static List<BlockPK> getBlockKeys(long[] inodeIds) {
    List<BlockPK> keys = new ArrayList<>(inodeIds.length);
    for (long inodeId : inodeIds) {
      keys.add(new BlockPK(null, inodeId));
    }
    return keys;
  }
  
  static class CachedBlockPK extends BlockPK {
    private String datanodeId=null;

    CachedBlockPK(long blockId, long inodeId, String datanodeId) {
      super(blockId, inodeId);
      this.datanodeId = datanodeId;
    }

    CachedBlockPK(long blockId, long inodeId) {
      super(blockId, inodeId);
    }
      
    CachedBlockPK(String datanodeId) {
      this.datanodeId = datanodeId;
    }

    CachedBlockPK(long inodeId) {
      super(null, inodeId);
    }

    String getDatanodeId() {
      return datanodeId;
    }
    
    boolean hasDatanodeId(){
      return datanodeId!=null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CachedBlockPK)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      // So it's the same block, now check if it's the same replica
      CachedBlockPK cachedBlockPK = (CachedBlockPK) o;
      return datanodeId.equals(cachedBlockPK.datanodeId);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + datanodeId.hashCode();
      return result;
    }

    static List<CachedBlockPK> getKeys(long[] blockIds, int storageId) {
      List<BlockPK.CachedBlockPK> keys = new ArrayList<>(blockIds.length);
      for (long blockId : blockIds) {
        keys.add(new BlockPK.CachedBlockPK(blockId, storageId));
      }
      return keys;
    }

    static List<BlockPK.CachedBlockPK> getKeys(long[] inodeIds) {
      List<BlockPK.CachedBlockPK> keys = new ArrayList<>(inodeIds.length);
      for (long inodeId : inodeIds) {
        keys.add(new BlockPK.CachedBlockPK(inodeId));
      }
      return keys;
    }

    static List<CachedBlockPK> getKeys(long[] blockIds, int[] inodeIds, String datanodeId) {
      List<BlockPK.CachedBlockPK> keys = new ArrayList<>(blockIds.length);
      for (int i = 0; i < blockIds.length; i++) {
        keys.add(new BlockPK.CachedBlockPK(blockIds[i], inodeIds[i], datanodeId));
      }
      return keys;
    }
  }
}
