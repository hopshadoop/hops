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

import com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.util.ArrayList;
import java.util.Collection;

/**
 * BlockRecoveryCommand is an instruction to a data-node to recover
 * the specified blocks.
 * <p/>
 * The data-node that receives this command treats itself as a primary
 * data-node in the recover process.
 * <p/>
 * Block recovery is identified by a recoveryId, which is also the new
 * generation stamp, which the block will have after the recovery succeeds.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockRecoveryCommand extends DatanodeCommand {
  Collection<RecoveringBlock> recoveringBlocks;

  /**
   * This is a block with locations from which it should be recovered
   * and the new generation stamp, which the block will have after
   * successful recovery.
   * <p/>
   * The new generation stamp of the block, also plays role of the recovery id.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class RecoveringBlock extends LocatedBlock {
    private long newGenerationStamp;

    /**
     * Create RecoveringBlock.
     */
    public RecoveringBlock(ExtendedBlock b, DatanodeInfo[] locs, long newGS) {
      super(b, locs, -1, false); // startOffset is unknown
      this.newGenerationStamp = newGS;
    }

    /**
     * Create RecoveringBlock.
     */
    public RecoveringBlock(ExtendedBlock b, DatanodeStorageInfo[] locs, long newGS) {
      super(b, locs); // startOffset is unknown
      this.newGenerationStamp = newGS;
    }

    /**
     * Return the new generation stamp of the block,
     * which also plays role of the recovery id.
     */
    public long getNewGenerationStamp() {
      return newGenerationStamp;
    }
  }

  /**
   * Create empty BlockRecoveryCommand.
   */
  public BlockRecoveryCommand() {
    this(0);
  }

  /**
   * Create BlockRecoveryCommand with
   * the specified capacity for recovering blocks.
   */
  public BlockRecoveryCommand(int capacity) {
    this(new ArrayList<RecoveringBlock>(capacity));
  }
  
  public BlockRecoveryCommand(Collection<RecoveringBlock> blocks) {
    super(DatanodeProtocol.DNA_RECOVERBLOCK);
    recoveringBlocks = blocks;
  }

  /**
   * Return the list of recovering blocks.
   */
  public Collection<RecoveringBlock> getRecoveringBlocks() {
    return recoveringBlocks;
  }

  /**
   * Add recovering block to the command.
   */
  public void add(RecoveringBlock block) {
    recoveringBlocks.add(block);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockRecoveryCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, recoveringBlocks);
    sb.append("\n)");
    return sb.toString();
  }
}
