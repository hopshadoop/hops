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
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import java.io.IOException;

/**
 * Directory INode class that has a quota restriction
 */
public class INodeDirectoryWithQuota extends INodeDirectory {

  /**
   * Convert an existing directory inode to one with the given quota
   *
   * @param nsQuota
   *     Namespace quota to be assigned to this inode
   * @param dsQuota
   *     Diskspace quota to be assigned to this indoe
   * @param other
   *     The other inode from which all other properties are copied
   */
  INodeDirectoryWithQuota(INodeDirectory other, Long nsQuota, Long dsQuota)
      throws IOException {
    super(other);
    INode.DirCounts counts = new INode.DirCounts();
    other.spaceConsumedInTree(counts);
    createINodeAttributes(nsQuota, counts.getNsCount(), dsQuota,
        counts.getDsCount());
    setQuota(nsQuota, dsQuota);
  }

  public INodeDirectoryWithQuota(INodeDirectory other,
      Quota.Counts quota) throws IOException {
    this(other, quota.get(Quota.NAMESPACE), quota.get(Quota.DISKSPACE));
  }
  
  INodeDirectoryWithQuota(Long nsQuota, Long dsQuota, Long nsCount,
      Long dsCount, INodeDirectory other)
      throws IOException {
    super(other);
    createINodeAttributes(nsQuota, nsCount, dsQuota, dsCount);
    setQuota(nsQuota, dsQuota);
  }
  
  /**
   * constructor with no quota verification
   */
  public INodeDirectoryWithQuota(int id, String name, PermissionStatus permissions) throws IOException{
    super(id, name, permissions);
  }
      
  public INodeDirectoryWithQuota(int id, String name, PermissionStatus permissions, boolean inTree)
      throws IOException {
    super(id, name, permissions, inTree);
  }
  
  public INodeDirectoryWithQuota(INodeDirectoryWithQuota other) throws IOException{
    this(other, other.getQuotaCounts());
  }
  
  @Override
  public Quota.Counts getQuotaCounts() throws StorageException, TransactionContextException {
    return getINodeAttributes().getQuotaCounts();
  }
  
  /**
   * Set this directory's quota
   *
   * @param newNsQuota
   *     Namespace quota to be set
   * @param newDsQuota
   *     diskspace quota to be set
   */
  void setQuota(Long newNsQuota, Long newDsQuota)
      throws StorageException, TransactionContextException {
    getINodeAttributes().setNsQuota(newNsQuota);
    getINodeAttributes().setDsQuota(newDsQuota);
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) throws StorageException, TransactionContextException {
    final long original = summary.getCounts().get(Content.DISKSPACE);
    long oldYieldCount = summary.getYieldCount();
    super.computeContentSummary(summary);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkDiskspace(summary.getCounts().get(Content.DISKSPACE) - original);
    }
    return summary;
  }
  
  private void checkDiskspace(final long computed) throws StorageException, TransactionContextException {
    long diskspace = diskspaceConsumed();
    if (-1 != getQuotaCounts().get(Quota.DISKSPACE) && diskspace != computed) {
      NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
          + getFullPathName() + ". Cached = " + diskspace
          + " != Computed = " + computed);
    }
  }
  
  @Override
  DirCounts spaceConsumedInTree(DirCounts counts)
      throws StorageException, TransactionContextException {
    counts.nsCount += getINodeAttributes().getNsCount();
    counts.dsCount += getINodeAttributes().getDiskspace();
    return counts;
  }

  /**
   * Get the number of names in the subtree rooted at this directory
   *
   * @return the size of the subtree rooted at this directory
   */
  public Long numItemsInTree()
      throws StorageException, TransactionContextException {
    return getINodeAttributes().getNsCount();
  }
  
  public Long diskspaceConsumed()
      throws StorageException, TransactionContextException {
    return getINodeAttributes().getDiskspace();
  }
  
  /**
   * Update the size of the tree
   *
   * @param nsDelta
   *     the change of the tree size
   * @param dsDelta
   *     change to disk space occupied
   */
  void addSpaceConsumed(Long nsDelta, Long dsDelta)
      throws StorageException, TransactionContextException {
    setSpaceConsumed(getINodeAttributes().getNsCount() + nsDelta, getINodeAttributes().getDiskspace() + dsDelta);
  }
  
  /**
   * Sets namespace and diskspace take by the directory rooted
   * at this INode. This should be used carefully. It does not check
   * for quota violations.
   *
   * @param namespace
   *     size of the directory to be set
   * @param diskspace
   *     disk space take by all the nodes under this directory
   */
  public void setSpaceConsumed(Long namespace, Long diskspace)
      throws StorageException, TransactionContextException {
    getINodeAttributes().setNsCount(namespace);
    getINodeAttributes().setDiskspace(diskspace);
  }
  
  public void setSpaceConsumedNoPersistance(Long namespace, Long diskspace)
      throws StorageException, TransactionContextException {
    getINodeAttributes().setNsCountNoPersistance(namespace);
    getINodeAttributes().setDiskspaceNoPersistance(diskspace);
  }

  /**
   * Verify if the namespace count disk space satisfies the quota restriction
   *
   * @throws QuotaExceededException
   *     if the given quota is less than the count
   */
  void verifyQuota(Long nsDelta, Long dsDelta)
      throws QuotaExceededException, StorageException,
      TransactionContextException {
    Long newCount = getINodeAttributes().getNsCount() + nsDelta;
    Long newDiskspace = getINodeAttributes().getDiskspace() + dsDelta;

    if (nsDelta > 0 || dsDelta > 0) {
      if (getQuotaCounts().get(Quota.NAMESPACE) >= 0 &&
          getQuotaCounts().get(Quota.NAMESPACE) < newCount) {
        throw new NSQuotaExceededException(getQuotaCounts().get(Quota.NAMESPACE),
            newCount);
      }
      if (getQuotaCounts().get(Quota.DISKSPACE) >= 0 &&
          getQuotaCounts().get(Quota.DISKSPACE) < newDiskspace) {
        throw new DSQuotaExceededException(getQuotaCounts().get(Quota.DISKSPACE),
            newDiskspace);
      }
    }
  }


  public static INodeDirectoryWithQuota createRootDir(
      PermissionStatus permissions) throws IOException {
    INodeDirectoryWithQuota newRootINode = new INodeDirectoryWithQuota(ROOT_INODE_ID, ROOT_NAME, permissions);
    newRootINode.inTree();
    newRootINode.setParentIdNoPersistance(ROOT_PARENT_ID);
    newRootINode.setPartitionIdNoPersistance(getRootDirPartitionKey());
    return newRootINode;
  }

  public static INodeDirectoryWithQuota getRootDir()
      throws StorageException, TransactionContextException {
    INode inode = EntityManager
        .find(INode.Finder.ByINodeIdFTIS, ROOT_INODE_ID);
    return (INodeDirectoryWithQuota) inode;
  }
  
  public INodeAttributes getINodeAttributes()
      throws StorageException, TransactionContextException {
    return EntityManager.find(INodeAttributes.Finder.ByINodeId, id);
  }
  
  private void createINodeAttributes(Long nsQuota, Long nsCount, Long dsQuota,
      Long diskspace) throws StorageException, TransactionContextException {
    INodeAttributes attr =
        new INodeAttributes(id, inTree, nsQuota, nsCount, dsQuota, diskspace);
    EntityManager.add(attr);
  }
  
  protected void persistAttributes()
      throws StorageException, TransactionContextException {
    getINodeAttributes().saveAttributes();
  }

  protected void removeAttributes()
      throws StorageException, TransactionContextException {
    INodeAttributes attributes = getINodeAttributes();
    if(attributes!=null){
      attributes.removeAttributes();
    }
  }
  
  protected void changeAttributesPkNoPersistance(Integer id, boolean inTree)
      throws StorageException, TransactionContextException {
    getINodeAttributes().setInodeIdNoPersistance(id, inTree);
  }

  @Override
  public INode cloneInode () throws IOException{
    return new INodeDirectoryWithQuota(this);
  }
}
