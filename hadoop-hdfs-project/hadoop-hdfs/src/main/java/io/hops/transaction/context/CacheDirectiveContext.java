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
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.protocol.CacheDirective;

public class CacheDirectiveContext extends BaseEntityContext<Long, CacheDirective> {

  private CacheDirectiveDataAccess<CacheDirective> dataAccess;
  private Map<String, Collection<CacheDirective>> cacheDirectivesByPool = new HashMap<>();
  private Map<Long, CacheDirective> cacheDirectivesById = new HashMap<>();
  private boolean fetchedCacheDirectivesById = false;
  private Collection<CacheDirective> all = null;

  public CacheDirectiveContext(CacheDirectiveDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public CacheDirective find(FinderType<CacheDirective> finder, Object... params)
      throws TransactionContextException, StorageException {
    CacheDirective.Finder hbFinder = (CacheDirective.Finder) finder;
    switch (hbFinder) {
      case ById:
        return findByPrimaryKey(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<CacheDirective> findList(FinderType<CacheDirective> finder, Object... params)
      throws TransactionContextException, StorageException {
    CacheDirective.Finder hbFinder = (CacheDirective.Finder) finder;
    switch (hbFinder) {
      case ByPoolName:
        return findByPoolName(hbFinder, params);
      case ByIdPoolAndPath:
        return findByIdPoolAndPath(hbFinder, params);
      case All:
        return findAll(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  private CacheDirective findByPrimaryKey(CacheDirective.Finder hbFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    long pk = (Long) params[0];
    CacheDirective result;
    if (contains(pk)) {
      result = get(pk);
      hit(hbFinder, result, "id", pk);
    } else {
      aboutToAccessStorage(hbFinder, params);
      result = dataAccess.find(pk);
      gotFromDB(pk, result);
      miss(hbFinder, result, "Id", pk);
    }

    return result;
  }

  private Collection<CacheDirective> findByPoolName(CacheDirective.Finder hbFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    String poolName = (String) params[0];
    Collection<CacheDirective> results;
    if (cacheDirectivesByPool.containsKey(poolName)) {
      results = cacheDirectivesByPool.get(poolName);
      hit(hbFinder, results, "poolName", poolName);
    } else {
      aboutToAccessStorage(hbFinder, params);
      results = dataAccess.findByPool(poolName);
      cacheDirectivesByPool.put(poolName, results);
      gotFromDB(results);
      miss(hbFinder, results, "poolName", poolName);
    }
    return results;
  }

  private Collection<CacheDirective> findByIdPoolAndPath(CacheDirective.Finder hbFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    long id = (long) params[0];
    String poolName = (String) params[1];
    String path = (String) params[2];
    int maxNumResults = (int) params[3];
    Collection<CacheDirective> results;
    if (fetchedCacheDirectivesById){
      results = new ArrayList<>();
      Set<Long> ids = new TreeSet<>(cacheDirectivesById.keySet());
      for(long directiveId : ids){
        CacheDirective directive = cacheDirectivesById.get(directiveId);
        if(poolName!=null && !directive.getPoolName().equals(poolName)){
          continue;
        }
        if(path!=null && !directive.getPath().equals(path)){
          continue;
        }
        results.add(directive);
        if(results.size()>=maxNumResults){
          break;
        }
      }
      hit(hbFinder, results, "id", id, "poolName", poolName, "path", path);
    } else {
      aboutToAccessStorage(hbFinder, params);
      results = dataAccess.findByIdAndPool(id, poolName);
      fetchedCacheDirectivesById = true;
      for(CacheDirective directive: results){
        cacheDirectivesById.put(directive.getId(), directive);
        gotFromDB(directive);
      }
      miss(hbFinder, results, "id", id, "poolName", poolName, "path", path);
    }
    return results;
  }
  
  private Collection<CacheDirective> findAll(CacheDirective.Finder hbFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    Collection<CacheDirective> results;
    if (all != null) {
      results = all;
      hit(hbFinder, results, "all");
    } else {
      aboutToAccessStorage(hbFinder, params);
      results = dataAccess.findAll();
    }
    return results;
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getModified());
  }

  @Override
  Long getKey(CacheDirective cacheDirective) {
    return cacheDirective.getId();
  }
}
