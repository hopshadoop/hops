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
package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.entity.INodeAttributes;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.Quota;

public class INodeAttributeDALAdaptor extends
    DalAdaptor<org.apache.hadoop.hdfs.server.namenode.INodeAttributes, INodeAttributes>
    implements
    INodeAttributesDataAccess<org.apache.hadoop.hdfs.server.namenode.INodeAttributes> {

  private INodeAttributesDataAccess<INodeAttributes> dataAccess;

  public INodeAttributeDALAdaptor(
      INodeAttributesDataAccess<INodeAttributes> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.INodeAttributes findAttributesByPk(
      Long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPk(inodeId));
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.namenode.INodeAttributes> modified,
      Collection<org.apache.hadoop.hdfs.server.namenode.INodeAttributes> removed)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(modified), convertHDFStoDAL(removed));

  }

  @Override
  public INodeAttributes convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.namenode.INodeAttributes attribute)
      throws StorageException {
    if (attribute != null) {
      INodeAttributes hia =
          new INodeAttributes(attribute.getInodeId(), attribute.getQuotaCounts().get(Quota.NAMESPACE),
              attribute.getNsCount(), attribute.getQuotaCounts().get(Quota.DISKSPACE),
              attribute.getDiskspace());
      return hia;
    } else {
      return null;
    }
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.INodeAttributes convertDALtoHDFS(
      INodeAttributes hia) throws StorageException {
    if (hia != null) {
      org.apache.hadoop.hdfs.server.namenode.INodeAttributes iNodeAttributes =
          new org.apache.hadoop.hdfs.server.namenode.INodeAttributes(
              hia.getInodeId(), hia.getNsQuota(), hia.getNsCount(),
              hia.getDsQuota(), hia.getDiskspace());
      return iNodeAttributes;
    } else {
      return null;
    }
  }

  @Override
  public Collection<org.apache.hadoop.hdfs.server.namenode.INodeAttributes> findAttributesByPkList(
      List<INodeCandidatePrimaryKey> inodePks) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPkList(inodePks));
  }
}
