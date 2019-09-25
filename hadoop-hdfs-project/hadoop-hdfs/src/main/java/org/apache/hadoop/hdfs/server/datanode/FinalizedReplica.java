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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.io.File;

/**
 * This class describes a replica that has been finalized.
 */
public class FinalizedReplica extends ReplicaInfo {
  private boolean unlinked;      // copy-on-write done for block

  /**
   * Constructor
   *
   * @param blockId
   *     block id
   * @param len
   *     replica length
   * @param genStamp
   *     replica generation stamp
   * @param vol
   *     volume where replica is located
   * @param dir
   *     directory path where block and meta files are located
   */
  public FinalizedReplica(long blockId, long len, long genStamp, short cloudBucketID,
      FsVolumeSpi vol, File dir) {
    super(blockId, len, genStamp, cloudBucketID, vol, dir);
  }
  
  /**
   * Constructor
   *
   * @param block
   *     a block
   * @param vol
   *     volume where replica is located
   * @param dir
   *     directory path where block and meta files are located
   */
  public FinalizedReplica(Block block, FsVolumeSpi vol, File dir) {
    super(block, vol, dir);
  }

  /**
   * Copy constructor.
   *
   * @param from
   */
  public FinalizedReplica(FinalizedReplica from) {
    super(from);
    this.unlinked = from.isUnlinked();
  }

  @Override  // ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.FINALIZED;
  }
  
  @Override // ReplicaInfo
  public boolean isUnlinked() {
    return unlinked;
  }

  @Override  // ReplicaInfo
  public void setUnlinked() {
    unlinked = true;
  }
  
  @Override
  public long getVisibleLength() {
    return getNumBytes();       // all bytes are visible
  }

  @Override
  public long getBytesOnDisk() {
    return getNumBytes();
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
  
  @Override
  public String toString() {
    return super.toString() + "\n  unlinked          =" + unlinked;
  }
}
