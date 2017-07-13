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

class BlockPK {
  private long blockId = Long.MIN_VALUE;
  private int inodeId = INode.NON_EXISTING_ID;

  BlockPK(long blockId) {
    this.blockId = blockId;
  }

  BlockPK(int inodeId) {
    this.inodeId = inodeId;
  }

  BlockPK(long blockId, int inodeId) {
    this.blockId = blockId;
    this.inodeId = inodeId;
  }

  boolean hasBlockId() {
    return this.blockId != Long.MIN_VALUE;
  }

  boolean hasINodeId() {
    return this.inodeId != INode.NON_EXISTING_ID;
  }

  long getBlockId() {
    return blockId;
  }

  int getInodeId() {
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
    result = 31 * result + inodeId;
    return result;
  }

  static class ReplicaPK extends BlockPK {
    private int storageId;

    ReplicaPK(long blockId, int inodeId, int storageId) {
      super(blockId, inodeId);
      this.storageId = storageId;
    }

    ReplicaPK(long blockId, int storageId) {
      super(blockId);
      this.storageId = storageId;
    }

    ReplicaPK(int inodeId) {
      super(inodeId);
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

      ReplicaPK replicaPK = (ReplicaPK) o;

      if (storageId != replicaPK.storageId) {
        return false;
      }

      return true;
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

    static List<BlockPK.ReplicaPK> getKeys(int[] inodeIds) {
      List<BlockPK.ReplicaPK> keys =
          new ArrayList<>(inodeIds.length);
      for (int inodeId : inodeIds) {
        keys.add(new BlockPK.ReplicaPK(inodeId));
      }
      return keys;
    }

    static List<ReplicaPK> getKeys(long[] blockIds, int[] inodeIds,
        int storageId) {
      List<BlockPK.ReplicaPK> keys = new ArrayList<>(blockIds.length);
      for (int i = 0; i < blockIds.length; i++) {
        keys.add(new BlockPK.ReplicaPK(blockIds[i], inodeIds[i], storageId));
      }
      return keys;
    }
  }

  static List<BlockPK> getBlockKeys(int[] inodeIds) {
    List<BlockPK> keys = new ArrayList<>(inodeIds.length);
    for (int inodeId : inodeIds) {
      keys.add(new BlockPK(inodeId));
    }
    return keys;
  }
}
