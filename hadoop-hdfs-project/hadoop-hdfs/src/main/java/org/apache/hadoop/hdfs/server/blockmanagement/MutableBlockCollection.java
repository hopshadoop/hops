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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;

import java.io.IOException;

/**
 * This interface is used by the block manager to expose a
 * few characteristics of a collection of Block/BlockUnderConstruction.
 */
public interface MutableBlockCollection extends BlockCollection {
  /**
   * Set the block at the given index.
   */
  public void setBlock(int index, BlockInfo blk)
      throws TransactionContextException, StorageException;

  /**
   * Convert the last block of the collection to an under-construction block
   * and set the locations.
   */
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeStorageInfo[] locations) throws IOException, StorageException;
}
