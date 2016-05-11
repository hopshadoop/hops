/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.security.token.Token;

import java.util.Comparator;
import java.util.HashSet;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  private ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfo[] locs;
  // Storage type for each replica, if reported.
  private StorageType[] storageTypes;
  // Storage ID for each replica, if reported.
  private String[] storageIDs;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken =
      new Token<BlockTokenIdentifier>();

  // Used when there are no locations
  private static final DatanodeInfo[] EMPTY_LOCS = new DatanodeInfo[0];

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, boolean corrupt) {
    this(b, locs, null, null, startOffset, corrupt);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages) {
    this(b, storages, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs, StorageType[] storageTypes) {
    this(b, locs, storageIDs, storageTypes, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages, long startOffset, boolean corrupt) {
    this(b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
      StorageType[] storageTypes, long startOffset, boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs == null) {
      this.locs = new DatanodeInfo[0];
    } else {
      this.locs = locs;
    }
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  public String[] getStorageIDs() {
    return storageIDs;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public DatanodeInfo[] getUniqueLocations() {
    HashSet<DatanodeInfo> dns = new HashSet<DatanodeInfo>();
    for(DatanodeInfo dn : this.locs) {
      dns.add(dn);
    }
    return dns.toArray(new DatanodeInfo[0]);
  }

  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }
  
  public boolean isCorrupt() {
    return this.corrupt;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b + "; getBlockSize()=" +
        getBlockSize() + "; corrupt=" + corrupt + "; offset=" + offset +
        "; locs=" + java.util.Arrays.asList(locs) + "}";
  }

  public final static Comparator<LocatedBlock> blockIdComparator =
      new Comparator<LocatedBlock>() {

        @Override
        public int compare(LocatedBlock locatedBlock,
            LocatedBlock locatedBlock2) {
          if (locatedBlock.getBlock().getBlockId() <
              locatedBlock2.getBlock().getBlockId()) {
            return -1;
          }
          if (locatedBlock.getBlock().getBlockId() >
              locatedBlock2.getBlock().getBlockId()) {
            return 1;
          }
          return 0;
        }
      };
}
