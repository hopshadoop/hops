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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.Comparator;

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
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken =
      new Token<>();

  private byte[] data = null;

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset) {
    this(b, locs, startOffset, false);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset,
      boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs == null) {
      this.locs = new DatanodeInfo[0];
    } else {
      this.locs = locs;
    }
    this.data = null;
  }

  public void setData(byte[] data) {
    String error = null;
    if(locs != null){
      if(isPhantomBlock()){
        this.data = data;
      }else{
        error = "Can not set data. Not a phantom data block";
      }
    }else{
      error = "Can not set data. No datanode found";
    }
    if(error != null){
      throw new UnsupportedOperationException(error);
    }
  }

  public boolean isPhantomBlock(){
    if (locs != null && locs.length == 1) {
      if (b.getBlockId() < 0) {
        return true;
      }
    }
    return  false;
  }

  public boolean isDataSet(){
     if(isPhantomBlock() && data!= null && data.length > 0) {
       return true;
     }else{
       return false;
     }
  }

  public byte[] getData() {
    String error = null;
    if(isDataSet()) {
      return data;
    }else if(isPhantomBlock()) {
      error = "The file data is not set";
    }else {
      error = "The file data is not stored in the database";
    }
    throw new UnsupportedOperationException(error);
  }


  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
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
