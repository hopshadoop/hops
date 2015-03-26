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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

/**
 * An {@link INode} representing a symbolic link.
 */
@InterfaceAudience.Private
public class INodeSymlink extends INodeWithAdditionalFields {
  private final byte[] symlink; // The target URI

  public INodeSymlink(long id, String value, long mtime, long atime, PermissionStatus permissions) throws IOException {
    this(id, value, mtime, atime, permissions, false);
  }

  public INodeSymlink(long id, String value, long mtime, long atime,
      PermissionStatus permissions, boolean inTree) throws IOException {
    super(id, permissions, mtime, atime, inTree);
    this.symlink = DFSUtil.string2Bytes(value);
  }

  public INodeSymlink(INodeSymlink other) throws IOException{
    super(other);
    this.symlink = Arrays.copyOf(other.symlink, other.symlink.length);
  }
  
  @Override
  public boolean isSymlink() {
    return true;
  }

  /**
   * @return this object.
   */
  @Override
  public INodeSymlink asSymlink() {
    return this;
  }

  public String getSymlinkString() {
    return DFSUtil.bytes2String(symlink);
  }

  public byte[] getSymlink() {
    return symlink;
  }

  @Override
  QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps, QuotaCounts counts) {
    counts.addNameSpace(1);
    return counts;
  }
  
  @Override
  public void destroyAndCollectBlocks(final BlockStoragePolicySuite bsps,
      BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    removedINodes.add(this);
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    summary.getCounts().addContent(Content.SYMLINK, 1);
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
  
  @Override
  public INode cloneInode () throws IOException{
    return new INodeSymlink(this);
  }
  
  /**
   * getAclFeature is not overridden because it is needed for resolving
   * symlinks.
  @Override
  final AclFeature getAclFeature(int snapshotId) {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }
  */
  @Override
  public void removeAclFeature() {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }
  @Override
  public void addAclFeature(AclFeature f) {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }

  @Override
  final XAttrFeature getXAttrFeature() {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  @Override
  public void removeXAttrFeature() {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
}
