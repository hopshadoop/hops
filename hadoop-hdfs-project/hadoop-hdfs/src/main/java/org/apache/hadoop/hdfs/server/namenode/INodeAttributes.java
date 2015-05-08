/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;

/**
 * right now it holds quota info. later we can add more
 * information like access time ( if we want to remove locks from the parent
 * dirs )
 */
public class INodeAttributes {

  public static enum Finder implements FinderType<INodeAttributes> {

    ByINodeId,
    ByINodeIds;

    @Override
    public Class getType() {
      return INodeAttributes.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByINodeId:
          return Annotation.PrimaryKey;
        case ByINodeIds:
          return Annotation.Batched;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private Integer inodeId;
  private Long nsQuota; /// NameSpace quota
  private Long nsCount;
  private Long dsQuota; /// disk space quota
  private Long diskspace;
  private Integer status;//To indicate whether Quota has been changed ot not.

  public INodeAttributes(Integer inodeId, Long nsQuota, Long nsCount,
      Long dsQuota, Long diskspace,Integer status) {
    this.inodeId = inodeId;
    if (nsQuota != null) {
      this.nsQuota = nsQuota;
    } else {
      this.nsQuota = FSDirectory.UNKNOWN_DISK_SPACE;
    }
    if (nsCount != null) {
      this.nsCount = nsCount;
    } else {
      this.nsCount = 1L;
    }
    if (dsQuota != null) {
      this.dsQuota = dsQuota;
    } else {
      this.dsQuota = Long.MAX_VALUE;
    }
    if (diskspace != null) {
      this.diskspace = diskspace;
    } else {
      throw new IllegalStateException(
          "default value for diskspace is not defined");
    }

    this.status = status;
  }

  public Integer getInodeId() {
    return inodeId;
  }

  public Long getNsQuota() {
    return nsQuota;
  }

  public Long getNsCount() {
    return nsCount;
  }

  public Long getDsQuota() {
    return dsQuota;
  }

  public Long getDiskspace() {
    return diskspace;
  }
public Integer getStatus() {
    return status;
  }
  public void setInodeId(Integer inodeId)
      throws StorageException, TransactionContextException {
    setInodeIdNoPersistance(inodeId);
    saveAttributes();
  }

  public void setNsQuota(Long nsQuota)
      throws StorageException, TransactionContextException {
    setNsQuotaNoPersistance(nsQuota);
    saveAttributes();
  }

  public void setNsCount(Long nsCount)
      throws StorageException, TransactionContextException {
    setNsCountNoPersistance(nsCount);
    saveAttributes();
  }

  public void setDsQuota(Long dsQuota)
      throws StorageException, TransactionContextException {
    setDsQuotaNoPersistance(dsQuota);
    saveAttributes();
  }

  public void setDiskspace(Long diskspace)
      throws StorageException, TransactionContextException {
    setDiskspaceNoPersistance(diskspace);
    saveAttributes();
  }

  public void setStatus(Integer status) throws  StorageException, TransactionContextException {
    setStatusNoPersistance(status);
    saveAttributes();
  }
  
  public void setNsQuotaNoPersistance(Long nsQuota) {
    this.nsQuota = nsQuota;
  }

  public void setNsCountNoPersistance(Long nsCount) {
    this.nsCount = nsCount;
  }

  public void setDsQuotaNoPersistance(Long dsQuota) {
    this.dsQuota = dsQuota;
  }

  public void setDiskspaceNoPersistance(Long diskspace) {
    this.diskspace = diskspace;
  }

  public void setInodeIdNoPersistance(Integer inodeId) {
    this.inodeId = inodeId;
  }
public void setStatusNoPersistance(Integer status) {
    this.status = status;
  }
  protected void saveAttributes()
      throws StorageException, TransactionContextException {
    EntityManager.update(this);
  }

  protected void removeAttributes()
      throws StorageException, TransactionContextException {
    EntityManager.remove(this);
  }
}
