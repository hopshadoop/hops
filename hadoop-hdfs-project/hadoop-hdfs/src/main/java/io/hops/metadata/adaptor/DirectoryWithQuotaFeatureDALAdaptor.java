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
import io.hops.metadata.hdfs.dal.DirectoryWithQuotaFeatureDataAccess;
import io.hops.metadata.hdfs.entity.DirectoryWithQuotaFeature;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.QuotaUpdate;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.util.EnumCounters;

public class DirectoryWithQuotaFeatureDALAdaptor extends
    DalAdaptor<org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature, DirectoryWithQuotaFeature>
    implements
    DirectoryWithQuotaFeatureDataAccess<org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature> {

  private DirectoryWithQuotaFeatureDataAccess<DirectoryWithQuotaFeature> dataAccess;

  public DirectoryWithQuotaFeatureDALAdaptor(
      DirectoryWithQuotaFeatureDataAccess<DirectoryWithQuotaFeature> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature findAttributesByPk(
      Long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPk(inodeId));
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature> modified,
      Collection<org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature> removed)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(modified), convertHDFStoDAL(removed));

  }

  @Override
  public DirectoryWithQuotaFeature convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature dir)
      throws StorageException {
    if (dir != null) {
      Map<QuotaUpdate.StorageType, Long> typeQuota = new HashMap<>();
      Map<QuotaUpdate.StorageType, Long> typeUsage = new HashMap<>();
      for(StorageType type: StorageType.asList()){
        typeQuota.put(QuotaUpdate.StorageType.valueOf(type.name()), dir.getQuota().getTypeSpaces().get(type));
        typeUsage.put(QuotaUpdate.StorageType.valueOf(type.name()), dir.getSpaceConsumed().getTypeSpaces().get(type));
      }
      DirectoryWithQuotaFeature hia = 
          new DirectoryWithQuotaFeature(dir.getInodeId(), dir.getQuota().getNameSpace(), dir.getSpaceConsumed().getNameSpace(),
              dir.getQuota().getStorageSpace(), dir.getSpaceConsumed().getStorageSpace(), typeQuota, typeUsage);
      return hia;
    } else {
      return null;
    }
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature convertDALtoHDFS(
      DirectoryWithQuotaFeature hia) throws StorageException {
    if (hia != null) {
      EnumCounters<StorageType> typeQuotas = new EnumCounters<StorageType>(StorageType.class);
      EnumCounters<StorageType> typeUsage = new EnumCounters<StorageType>(StorageType.class);
      for(StorageType type : StorageType.asList()){
        typeQuotas.add(type, hia.getTypeQuota().get(QuotaUpdate.StorageType.valueOf(type.name())));
        typeUsage.add(type, hia.getTypeUsed().get(QuotaUpdate.StorageType.valueOf(type.name())));
      }
      org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature dir
          = new org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.Builder(hia.getInodeId()).
              nameSpaceQuota(hia.getNsQuota()).storageSpaceQuota(hia.getSSQuota()).spaceUsage(hia.getSSUsed()).nameSpaceUsage(
              hia.getNsUsed()).typeQuotas(typeQuotas).typeUsages(typeUsage).build();
      return dir;
    } else {
      return null;
    }
  }

  @Override
  public Collection<org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature> findAttributesByPkList(
      List<INodeCandidatePrimaryKey> inodePks) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPkList(inodePks));
  }
}
