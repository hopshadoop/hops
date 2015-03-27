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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Maintains an array of blocks and their corresponding storage IDs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlocksWithLocations {

  /**
   * A class to keep track of a block and its locations
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockWithLocations {
    Block block;
    String storageIDs[];
    
    /**
     * constructor
     */
    public BlockWithLocations(Block block, String[] storageIDs) {
      this.block = block;
      this.storageIDs = storageIDs;
    }
    
    /**
     * get the block
     */
    public Block getBlock() {
      return block;
    }
    
    /**
     * get the block's locations
     */
    public String[] getStorageIDs() {
      return storageIDs;
    }
  }

  private BlockWithLocations[] blocks;

  /**
   * Constructor with one parameter
   */
  public BlocksWithLocations(BlockWithLocations[] blocks) {
    this.blocks = blocks;
  }

  /**
   * getter
   */
  public BlockWithLocations[] getBlocks() {
    return blocks;
  }
}
