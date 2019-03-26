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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A BlockCommand is an instruction to a datanode to send block report to
 * the mismatching buckets
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HashesMismatchCommand extends DatanodeCommand {
  Map<String, List<Integer>> missMatchingBuckets;

  public HashesMismatchCommand() {
    super(DatanodeProtocol.DNA_HASHMISMATCH);
    this.missMatchingBuckets = new HashMap<>();
  }

  public void addStorageBuckets(String storageID, List<Integer> buckets){
    missMatchingBuckets.put(storageID, buckets);
  }

  public Map<String, List<Integer>> getMissMatchingBuckets(){
    return missMatchingBuckets;
  }
}