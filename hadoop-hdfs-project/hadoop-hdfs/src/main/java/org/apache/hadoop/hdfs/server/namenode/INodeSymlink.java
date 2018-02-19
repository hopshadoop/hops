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
import io.hops.exception.TransactionContextException;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.util.List;

/**
 * An INode representing a symbolic link.
 */
@InterfaceAudience.Private
public class INodeSymlink extends INode {
  private byte[] symlink; // The target URI

  public INodeSymlink(String value, long modTime, long atime,
      PermissionStatus permissions) throws IOException {
    super(permissions, modTime, atime);
    assert value != null;
    setLinkValue(value);
    setModificationTimeForceNoPersistance(modTime);
    setAccessTimeNoPersistance(atime);
  }

  @Override
  public boolean isSymlink() {
    return true;
  }
  
  void setLinkValue(String value) {
    this.symlink = DFSUtil.string2Bytes(value);
  }

  public String getLinkValue() {
    return DFSUtil.bytes2String(symlink);
  }

  public byte[] getSymlink() {
    return symlink;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    return counts;
  }
  
  @Override
  int collectSubtreeBlocksAndClear(List<Block> v)
      throws StorageException, TransactionContextException {
    //HOP:
    EntityManager.remove(this);
    return 1;
  }

  @Override
  long[] computeContentSummary(long[] summary) {
    summary[1]++; // Increment the file count
    return summary;
  }
 
  @Override
  public byte getStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }

  @Override
  public byte getLocalStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }
}
