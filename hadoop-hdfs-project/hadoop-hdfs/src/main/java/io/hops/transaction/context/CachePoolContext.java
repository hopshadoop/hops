/*
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
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.CachePoolDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdfs.server.namenode.CachePool;

public class CachePoolContext extends BaseEntityContext<String, CachePool> {

  private CachePoolDataAccess<CachePool> dataAccess;
  private Set<CachePool> pools;
  public CachePoolContext(CachePoolDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public CachePool find(FinderType<CachePool> finder, Object... params) throws TransactionContextException,
      StorageException {
    CachePool.Finder hbFinder = (CachePool.Finder) finder;
    switch (hbFinder) {
      case ByName:
        return findByPrimaryKey(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<CachePool> findList(FinderType<CachePool> finder, Object... params) throws
      TransactionContextException,
      StorageException {
    CachePool.Finder hbFinder = (CachePool.Finder) finder;
    switch (hbFinder) {
      case All:
        return findAll(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  private CachePool findByPrimaryKey(CachePool.Finder hbFinder, Object[] params) throws StorageException,
      StorageCallPreventedException {
    String pk = (String) params[0];
    CachePool result;
    if (contains(pk)) {
      result = get(pk);
      hit(hbFinder, result, "name", pk);
    } else {
      aboutToAccessStorage(hbFinder, params);
      result = dataAccess.find(pk);
      if(pools==null){
        pools = new HashSet<>();
      }
      pools.add(result);
      gotFromDB(pk, result);
      miss(hbFinder, result, "name", pk);
    }

    return result;
  }

  private Collection<CachePool> findAll(CachePool.Finder hbFinder, Object[] params) throws StorageException,
      StorageCallPreventedException, TransactionContextException {
    if(pools!=null){
      return pools;
    } else {
      aboutToAccessStorage(hbFinder, params);
      Collection<CachePool> result = dataAccess.findAll();
      if(pools==null){
        pools = new HashSet<>();
      }
      pools.addAll(result);
      gotFromDB(result);
      miss(hbFinder, result);
      return pools;
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getModified());
  }

  @Override
  String getKey(CachePool CachePool) {
    return CachePool.getPoolName();
  }

}
