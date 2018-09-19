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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

/**
 * Quota feature for {@link INodeDirectory}
 */
public class DirectoryWithQuotaFeature implements INode.Feature {
  
  public DirectoryWithQuotaFeature() {
  }
  
  DirectoryWithQuotaFeature(final INodeDirectory dir, long nsQuota, long nsCount, long dsQuota, long dsCount)
    throws StorageException, TransactionContextException {
    createINodeAttributes(dir, nsQuota, nsCount, dsQuota, dsCount);
    setQuota(dir, nsQuota, dsQuota);
  }
  
  /** @return the quota set or null if it is not set. */
  Quota.Counts getQuota(INodeDirectory dir) throws StorageException, TransactionContextException {
    return getINodeAttributes(dir).getQuotaCounts();
  }
  
  /**
   * Set the directory's quota
   *
   * @param nsQuota Namespace quota to be set
   * @param dsQuota Diskspace quota to be set
   */
  void setQuota(final INodeDirectory dir, Long nsQuota, Long dsQuota)
      throws StorageException, TransactionContextException {
    getINodeAttributes(dir).setNsQuota(nsQuota);
    getINodeAttributes(dir).setDsQuota(dsQuota);
  }
  
  Quota.Counts addNamespaceDiskspace(final INodeDirectory dir, Quota.Counts counts)
    throws StorageException, TransactionContextException {
    INodeAttributes iNodeAttributes = getINodeAttributes(dir);
    counts.add(Quota.NAMESPACE, iNodeAttributes.getNsCount());
    counts.add(Quota.DISKSPACE, iNodeAttributes.getDiskspace());
    return counts;
  }
  
  INode.DirCounts spaceConsumedInTree(final INodeDirectory dir, INode.DirCounts counts)
    throws StorageException, TransactionContextException {
    counts.nsCount += getINodeAttributes(dir).getNsCount();
    counts.dsCount += getINodeAttributes(dir).getDiskspace();
    return counts;
  }
  
  ContentSummaryComputationContext computeContentSummary(final INodeDirectory dir,
      final ContentSummaryComputationContext summary)
    throws StorageException, TransactionContextException {
    final long original = summary.getCounts().get(Content.DISKSPACE);
    long oldYieldCount = summary.getYieldCount();
    dir.computeDirectoryContentSummary(summary);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkDiskspace(dir, summary.getCounts().get(Content.DISKSPACE) - original);
    }
    return summary;
  }
  
  private void checkDiskspace(final INodeDirectory dir, final long computed)
    throws StorageException, TransactionContextException {
    long diskspace = diskspaceConsumed(dir);
    if (-1 != getQuota(dir).get(Quota.DISKSPACE) && diskspace != computed) {
      NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
          + dir.getFullPathName() + ". Cached = " + diskspace
          + " != Computed = " + computed);
    }
  }
  
  /**
   * Get the number of names in the subtree rooted at this directory
   * @return the size of the subtree rooted at this directory
   * @throws StorageException
   * @throws TransactionContextException
   */
  Long numItemsInTree(final INodeDirectory dir)
    throws StorageException, TransactionContextException {
    return getINodeAttributes(dir).getNsCount();
  }
  
  Long diskspaceConsumed(final INodeDirectory dir)
    throws StorageException, TransactionContextException {
    return getINodeAttributes(dir).getDiskspace();
  }
  
  /**
   * Update the size of the tree
   *
   * @param dir Directory i-node
   * @param nsDelta the change of the tree size
   * @param dsDelta change to diskspace occupied
   * @throws StorageException
   * @throws TransactionContextException
   */
  void addSpaceConsumed(final INodeDirectory dir, final Long nsDelta, final Long dsDelta)
    throws StorageException, TransactionContextException {
    if (dir.isQuotaSet()) {
      setSpaceConsumed(dir, getINodeAttributes(dir).getNsCount() + nsDelta,
          getINodeAttributes(dir).getDiskspace() + dsDelta);
    }
  }
  
  /**
   * Sets the namespace and diskspace taken by the directory rooted
   * at this INode. This should be used carefully. It does not check
   * for quota violations.
   *
   * @param dir Directory i-node
   * @param namespace size of the directory to be set
   * @param diskspace disk space take by all the nodes under this directory
   * @throws StorageException
   * @throws TransactionContextException
   */
  void setSpaceConsumed(final INodeDirectory dir, final Long namespace, final Long diskspace)
    throws StorageException, TransactionContextException {
    getINodeAttributes(dir).setNsCount(namespace);
    getINodeAttributes(dir).setDiskspace(diskspace);
  }
  
  public Quota.Counts getSpaceConsumed(final INodeDirectory dir)
    throws StorageException, TransactionContextException {
    INodeAttributes iNodeAttributes = getINodeAttributes(dir);
    return Quota.Counts.newInstance(iNodeAttributes.getNsCount(), iNodeAttributes.getDiskspace());
  }
  
  void setSpaceConsumedNoPersistance(final INodeDirectory dir, Long namespace, Long diskSpace)
    throws StorageException, TransactionContextException {
    getINodeAttributes(dir).setNsCountNoPersistance(namespace);
    getINodeAttributes(dir).setDiskspaceNoPersistance(diskSpace);
  }
  
  /**
   * Verify if namespace or diskspace quota is violated
   * after applying deltas.
   *
   * @throws QuotaExceededException
   * @throws StorageException
   * @throws TransactionContextException
   */
  void verifyQuota(final INodeDirectory dir, final Long nsDelta, final Long dsDelta)
    throws QuotaExceededException, StorageException, TransactionContextException {
    final INodeAttributes iNodeAttributes = getINodeAttributes(dir);
    final Quota.Counts counts = getQuota(dir);
    verifyNamespaceQuota(iNodeAttributes, counts, nsDelta);
    verifyDiskspaceQuota(iNodeAttributes, counts, dsDelta);
  }
  
  private void verifyNamespaceQuota(final INodeAttributes iNodeAttributes, final Quota.Counts counts,
      final Long nsDelta) throws NSQuotaExceededException {
    final Long newNamespace = iNodeAttributes.getNsCount() + nsDelta;
    final Long namespace = counts.get(Quota.NAMESPACE);
    if (namespace >= 0 && namespace < newNamespace) {
      throw new NSQuotaExceededException(namespace, newNamespace);
    }
  }
  
  private void verifyDiskspaceQuota(final INodeAttributes iNodeAttributes, final Quota.Counts counts,
      final Long dsDelta) throws DSQuotaExceededException {
    final Long newDiskspace = iNodeAttributes.getDiskspace() + dsDelta;
    final Long diskSpace = counts.get(Quota.DISKSPACE);
    if (diskSpace >= 0 && diskSpace < newDiskspace) {
      throw new DSQuotaExceededException(diskSpace, newDiskspace);
    }
  }
  
  public INodeAttributes getINodeAttributes(final INodeDirectory dir)
    throws StorageException, TransactionContextException {
    return EntityManager.find(INodeAttributes.Finder.ByINodeId, dir.getId());
  }
  
  private void createINodeAttributes(final INodeDirectory dir, final Long nsQuota, final Long nameSpace,
      final Long dsQuota, final Long diskSpace) throws StorageException, TransactionContextException {
    INodeAttributes iNodeAttributes =
        new INodeAttributes(dir.getId(), dir.isInTree(), nsQuota, nameSpace, dsQuota, diskSpace);
    EntityManager.add(iNodeAttributes);
  }
  
  void removeAttributes(final INodeDirectory dir)
    throws StorageException, TransactionContextException {
    INodeAttributes iNodeAttributes = getINodeAttributes(dir);
    if (iNodeAttributes != null) {
      iNodeAttributes.removeAttributes();
    }
  }
  
  void changeAttributesPkNoPersistance(final INodeDirectory dir, final Long id, final boolean inTree)
    throws StorageException, TransactionContextException {
    getINodeAttributes(dir).setInodeIdNoPersistance(id, inTree);
  }
}
