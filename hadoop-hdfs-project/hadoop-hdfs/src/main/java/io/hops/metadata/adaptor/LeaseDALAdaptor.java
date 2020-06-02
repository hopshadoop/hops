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
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.entity.Lease;

import java.util.Collection;

public class LeaseDALAdaptor
    extends DalAdaptor<org.apache.hadoop.hdfs.server.namenode.Lease, Lease>
    implements LeaseDataAccess<org.apache.hadoop.hdfs.server.namenode.Lease> {

  private final LeaseDataAccess<Lease> dataAccess;

  public LeaseDALAdaptor(LeaseDataAccess<Lease> dataAcess) {
    this.dataAccess = dataAcess;
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();
  }

  @Override
  public Collection<org.apache.hadoop.hdfs.server.namenode.Lease> findByTimeLimit(
      long timeLimit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByTimeLimit(timeLimit));
  }

  @Override
  public Collection<org.apache.hadoop.hdfs.server.namenode.Lease> findAll()
      throws StorageException {
    return convertDALtoHDFS(dataAccess.findAll());
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.Lease findByPKey(String holder, int holderId)
      throws StorageException {
    return convertDALtoHDFS(dataAccess.findByPKey(holder,holderId));
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.Lease findByHolderId(
      int holderId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByHolderId(holderId));
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.namenode.Lease> removed,
      Collection<org.apache.hadoop.hdfs.server.namenode.Lease> newLeases,
      Collection<org.apache.hadoop.hdfs.server.namenode.Lease> modified)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newLeases),
        convertHDFStoDAL(modified));
  }

  @Override
  public void removeAll() throws StorageException {
    dataAccess.removeAll();
  }

  @Override
  public Lease convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.namenode.Lease hdfsClass)
      throws StorageException {
    if (hdfsClass != null) {
      return new Lease(hdfsClass.getHolder(), hdfsClass.getHolderID(),
          hdfsClass.getLastUpdate(), hdfsClass.getCount());
    } else {
      return null;
    }
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.Lease convertDALtoHDFS(
      Lease dalClass) throws StorageException {
    if (dalClass != null) {
      return new org.apache.hadoop.hdfs.server.namenode.Lease(
          dalClass.getHolder(), dalClass.getHolderId(),
          dalClass.getLastUpdate(), dalClass.getCount());
    } else {
      return null;
    }
  }
}
