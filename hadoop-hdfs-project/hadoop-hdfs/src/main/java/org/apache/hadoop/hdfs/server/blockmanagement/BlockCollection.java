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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;

import java.io.IOException;

/**
 * This interface is used by the block manager to expose a
 * few characteristics of a collection of Block/BlockUnderConstruction.
 */
@InterfaceAudience.Private
public interface BlockCollection {
  /**
   * Get the last block of the collection.
   */
  public BlockInfo getLastBlock() throws IOException, StorageException;

  /**
   * Get content summary.
   */
  public ContentSummary computeContentSummary()
      throws StorageException, TransactionContextException;

  /**
   * @return the number of blocks
   */
  public int numBlocks() throws StorageException, TransactionContextException;

  /**
   * Get the blocks.
   */
  public BlockInfo[] getBlocks()
      throws StorageException, TransactionContextException;

  /**
   * Get preferred block size for the collection
   *
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize();

  /**
   * Get block replication for the collection
   *
   * @return block replication value
   */
  public short getBlockReplication();

  /**
   * @return the storage policy ID.
   */
  public byte getStoragePolicyID();

  /**
   * Get the name of the collection.
   */
  public String getName() throws StorageException, TransactionContextException;
  
  /**
   * HOP:
   * Get the Id of associated INode
   */
  public int getId();
}
