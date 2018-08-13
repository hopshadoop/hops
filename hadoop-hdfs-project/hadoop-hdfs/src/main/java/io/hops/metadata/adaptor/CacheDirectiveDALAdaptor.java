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
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import java.util.Collection;
import org.apache.hadoop.hdfs.protocol.CacheDirective;

public class CacheDirectiveDALAdaptor extends DalAdaptor<CacheDirective, io.hops.metadata.hdfs.entity.CacheDirective>
    implements CacheDirectiveDataAccess<CacheDirective>  {
  
  private CacheDirectiveDataAccess<io.hops.metadata.hdfs.entity.CacheDirective> dataAccess;

  public CacheDirectiveDALAdaptor(CacheDirectiveDataAccess<io.hops.metadata.hdfs.entity.CacheDirective> dataAccess) {
    this.dataAccess = dataAccess;
  }
  
  @Override
  public io.hops.metadata.hdfs.entity.CacheDirective convertHDFStoDAL(CacheDirective cacheDirective)throws StorageException {
    return new io.hops.metadata.hdfs.entity.CacheDirective(cacheDirective.getId(), cacheDirective.getPath(), cacheDirective.getReplication(),
        cacheDirective.getExpiryTime(), cacheDirective.getBytesNeeded(), cacheDirective.getBytesCached(),
        cacheDirective.getFilesNeeded(), cacheDirective.getFilesCached(), cacheDirective.getPoolName());
  }
  
  @Override
  public CacheDirective convertDALtoHDFS(io.hops.metadata.hdfs.entity.CacheDirective cacheDirective){
    if(cacheDirective==null){
      return null;
    }
    return new CacheDirective(cacheDirective.getId(), cacheDirective.getPath(), cacheDirective.getReplication(),
        cacheDirective.getExpiryTime(), cacheDirective.getBytesNeeded(), cacheDirective.getBytesCached(),
        cacheDirective.getFilesNeeded(), cacheDirective.getFilesCached(), cacheDirective.getPool());
  }
  
  @Override
  public void prepare(Collection<CacheDirective> removed, Collection<CacheDirective> newed)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed));
  }
  
  @Override
  public CacheDirective find(long key) throws StorageException{
    return convertDALtoHDFS(dataAccess.find(key));
  }
    
  @Override
  public Collection<CacheDirective> findByPool(String pool) throws StorageException{
    return convertDALtoHDFS(dataAccess.findByPool(pool));
  }
  
  @Override
  public Collection<CacheDirective> findByIdAndPool(long id, String pool) throws StorageException{
    return convertDALtoHDFS(dataAccess.findByIdAndPool(id, pool));
  }
  
  @Override
  public Collection<CacheDirective> findAll() throws StorageException{
    return convertDALtoHDFS(dataAccess.findAll());
  }
}
