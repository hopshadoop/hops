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

import java.io.IOException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

/**
 * SafeMode related operations.
 */
@InterfaceAudience.Private
public interface SafeMode {
  /**
   * Check safe mode conditions.
   * If the corresponding conditions are satisfied,
   * trigger the system to enter/leave safe mode.
   */
  public void checkSafeMode() throws IOException;

  /**
   * Is the system in safe mode?
   */
  public boolean isInSafeMode() throws IOException;

  /**
   * Is the system in startup safe mode, i.e. the system is starting up with
   * safe mode turned on automatically?
   */
  public boolean isInStartupSafeMode() throws IOException;

  /**
   * Check whether replication queues are being populated.
   */
  public boolean isPopulatingReplQueues() throws IOException;

  /**
   * Increment number of blocks that reached minimal replication.
   *
   * @param blk
   *     current block
   */
  public void incrementSafeBlockCount(int replication, BlockInfoContiguous blk) throws IOException;

  /**
   * Decrement number of blocks that reached minimal replication.
   * @param blk
   *     current block
   */
  public void decrementSafeBlockCount(BlockInfoContiguous blk)
      throws StorageException, IOException;
}
