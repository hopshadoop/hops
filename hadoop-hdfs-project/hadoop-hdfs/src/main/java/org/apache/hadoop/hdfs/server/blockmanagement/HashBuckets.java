/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;

public class HashBuckets {
  
  private static HashBuckets instance;
  private int numBuckets;
  
  public static void initialize(int numBuckets){
    if (instance != null){
      //TODO log warning
    } else {
      instance = new HashBuckets(numBuckets);
    }
  }
  
  private HashBuckets(int numBuckets){
    this.numBuckets = numBuckets;
  }
  
  public static HashBuckets getInstance() {
    if (instance != null){
      return instance;
    } else {
      throw new RuntimeException("HashBuckets have not been initialized");
    }
  }
  
  public int getBucketForBlock(Block block){
    return (int) (block.getBlockId() % numBuckets);
  }
}
