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
  INodeDirectoryWithQuota(Long nsQuota, Long dsQuota, INodeDirectory other)
      throws IOException {
    super(other);
    INode.DirCounts counts = new INode.DirCounts();
    other.spaceConsumedInTree(counts);
    createINodeAttributes(nsQuota, counts.getNsCount(), dsQuota,
        counts.getDsCount());
    setQuota(nsQuota, dsQuota);
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
  public INodeDirectoryWithQuota(String name, PermissionStatus permissions)
      throws IOException {
    super(name, permissions);
  }
  
  /**
   * Get this directory's namespace quota
   *
   * @return this directory's namespace quota
   */
  @Override
  public long getNsQuota()
      throws StorageException, TransactionContextException {
    return getINodeAttributes().getNsQuota();
  }
  
  /**
   * Get this directory's diskspace quota
   *
   * @return this directory's diskspace quota
   */
  @Override
  public long getDsQuota()
      throws StorageException, TransactionContextException {
    return getINodeAttributes().getDsQuota();
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
  void updateNumItemsInTree(Long nsDelta, Long dsDelta)
      throws StorageException, TransactionContextException {
    getINodeAttributes()
        .setNsCount(getINodeAttributes().getNsCount() + nsDelta);
    getINodeAttributes()
        .setDiskspace(getINodeAttributes().getDiskspace() + dsDelta);
  }
  
  /**
   * Update the size of the tree
   *
   * @param nsDelta
   *     the change of the tree size
   * @param dsDelta
   *     change to disk space occupied
   */
  void unprotectedUpdateNumItemsInTree(Long nsDelta, Long dsDelta)
      throws StorageException, TransactionContextException {
    getINodeAttributes()
        .setNsCount(getINodeAttributes().getNsCount() + nsDelta);
    getINodeAttributes()
        .setDiskspace(getINodeAttributes().getDiskspace() + dsDelta);
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
      if (getINodeAttributes().getNsQuota() >= 0 &&
          getINodeAttributes().getNsQuota() < newCount) {
        throw new NSQuotaExceededException(getINodeAttributes().getNsQuota(),
            newCount);
      }
      if (getINodeAttributes().getDsQuota() >= 0 &&
          getINodeAttributes().getDsQuota() < newDiskspace) {
        throw new DSQuotaExceededException(getINodeAttributes().getDsQuota(),
            newDiskspace);
      }
    }
  }


  public static INodeDirectoryWithQuota createRootDir(
      PermissionStatus permissions) throws IOException {
    INodeDirectoryWithQuota newRootINode =
        new INodeDirectoryWithQuota(ROOT_NAME, permissions);
    newRootINode.setIdNoPersistance(ROOT_ID);
    newRootINode.setParentIdNoPersistance(ROOT_PARENT_ID);
    newRootINode.setPartitionIdNoPersistance(getRootDirPartitionKey());
    return newRootINode;
  }

  public static INodeDirectoryWithQuota getRootDir()
      throws StorageException, TransactionContextException {
    INode inode = EntityManager
        .find(INode.Finder.ByINodeIdFTIS, ROOT_ID);
    return (INodeDirectoryWithQuota) inode;
  }
  
  public INodeAttributes getINodeAttributes()
      throws StorageException, TransactionContextException {
    return EntityManager.find(INodeAttributes.Finder.ByINodeId, id);
  }
  
  private void createINodeAttributes(Long nsQuota, Long nsCount, Long dsQuota,
      Long diskspace) throws StorageException, TransactionContextException {
    INodeAttributes attr =
        new INodeAttributes(id, nsQuota, nsCount, dsQuota, diskspace);
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
  
  protected void changeAttributesPkNoPersistance(Integer id)
      throws StorageException, TransactionContextException {
    getINodeAttributes().setInodeIdNoPersistance(id);
  }

}
