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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Namesystem operations.
 */
@InterfaceAudience.Private
public interface Namesystem extends SafeMode {
  /**
   * Is this name system running?
   */
  public boolean isRunning();

  /**
   * Check if the user has superuser privilege.
   */
  public void checkSuperuserPrivilege() throws AccessControlException;

  /**
   * @return the block pool ID
   */
  public String getBlockPoolId();

  public boolean isGenStampInFuture(Block block)
      throws StorageException;

  public void adjustSafeModeBlockTotals(List<Block> deltaSafe, int deltaTotal)
      throws IOException;
  

  /**
   * Is it a Leader
   */
  public boolean isLeader();
  
  /**
   * Returns the namenode id
   */
  public long getNamenodeId();

  /**
   * Get the associated NameNode
   * @return the @link{NameNode}
   */
  public NameNode getNameNode();

  /**
   * Adjust the safeblocks if the current namenode is in safemode
   * @param safeBlocks
   *      list of blocks to be considered safe
   * @throws IOException
   */
  public void adjustSafeModeBlocks(Set<Long> safeBlocks) throws IOException;


}