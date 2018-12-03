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
package io.hops.transaction.context;

import com.google.common.base.Predicate;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LeasePathContext extends BaseEntityContext
        <LeasePathContext.LeasePathPK, LeasePath> {

  private final LeasePathDataAccess<LeasePath> dataAccess;
  private final Map<Integer, Set<LeasePath>> hIdToLPsMap =
      new HashMap<>();
  private final List<String> nullLPs = new ArrayList<>();

  public LeasePathContext(LeasePathDataAccess<LeasePath> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(LeasePath hopLeasePath)
      throws TransactionContextException {
    super.update(hopLeasePath);
    addToHIdToLPsMap(hopLeasePath);
    if(isLogTraceEnabled()) {
      log("added-lpath", "path", hopLeasePath.getPath(), "hid",
              hopLeasePath.getHolderId());
    }
  }

  @Override
  public void remove(LeasePath hopLeasePath)
      throws TransactionContextException {
    super.remove(hopLeasePath);
    removeFromHIdToLPsMap(hopLeasePath);
    if(isLogTraceEnabled()) {
      log("removed-lpath", "path", hopLeasePath.getPath(),
              "holderId ", hopLeasePath.getHolderId());
    }
  }

  @Override
  public LeasePath find(FinderType<LeasePath> finder, Object... params)
      throws TransactionContextException, StorageException {
    LeasePath.Finder lFinder = (LeasePath.Finder) finder;
    switch (lFinder) {
      case ByPath:
        return findByPath(lFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<LeasePath> findList(FinderType<LeasePath> finder,
      Object... params) throws TransactionContextException, StorageException {
    LeasePath.Finder lFinder = (LeasePath.Finder) finder;
    switch (lFinder) {
      case ByHolderId:
        return findByHolderId(lFinder, params);
      case ByPrefix:
        return findByPrefix(lFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    hIdToLPsMap.clear();
  }

  @Override
  LeasePathPK getKey(LeasePath hopLeasePath) {
    return new LeasePathPK(hopLeasePath.getPath(), hopLeasePath.getHolderId());
  }

  private LeasePath findByPath(LeasePath.Finder lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final String path = (String) params[0];
    LeasePath result = null;
    if (containsInHIdToLPsMap(path)) {
      result = getLPFromHIdToLPsMap(path);
      hit(lFinder, result, "path", path);
    }else if(nullLPs.contains(path)){
      return null;
    }else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPath(path);
      if(result != null){
        gotFromDBInternal(result);
      }else{
        nullLPs.add(path);
      }
      
      miss(lFinder, result, "path", path);
    }
    return result;
  }

  private Collection<LeasePath> findByHolderId(LeasePath.Finder lFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int holderId = (Integer) params[0];
    Collection<LeasePath> result = null;
    if (hIdToLPsMap.containsKey(holderId)) {
      result = new ArrayList<>(hIdToLPsMap.get(holderId));
      hit(lFinder, result, "hid", holderId);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByHolderId(holderId);
      gotFromDBInternal(holderId, result);
      miss(lFinder, result, "hid", holderId);
    }
    return result;
  }

  private Collection<LeasePath> findByPrefix(LeasePath.Finder lFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final String prefix = (String) params[0];
    Collection<LeasePath> result = null;
    try {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPrefix(prefix);
      gotFromDBInternal(result);
      miss(lFinder, result, "prefix", prefix, "numOfLps", result.size());
    } catch (StorageCallPreventedException ex) {
      // This is allowed in querying lease-path by prefix, this is needed in delete operation for example.
      result = getFilteredByPrefix(prefix);
      hit(lFinder, result, "prefix", prefix, "numOfLps", result.size());
    }
    return result;
  }

  private Collection<LeasePath> getFilteredByPrefix(final String prefix) {
    return get(new Predicate<ContextEntity>() {
      @Override
      public boolean apply(ContextEntity input) {
        if (input.getState() != State.REMOVED) {
          LeasePath leasePath = input.getEntity();
          if (leasePath != null) {
            return leasePath.getPath().contains(prefix);
          }
        }
        return false;
      }
    });
  }
 
  void gotFromDBInternal(LeasePath leasePath) {
    if(leasePath != null){
        super.gotFromDB(new LeasePathContext.LeasePathPK(leasePath.getPath(),
            leasePath.getHolderId()), leasePath);
    addToHIdToLPsMap(leasePath);
    }
    
  }
  
  void gotFromDBInternal(Collection<LeasePath> entityList) {
      if(entityList != null && !entityList.isEmpty()){
          for(LeasePath lp : entityList){
          gotFromDBInternal(lp);
        }
      }
  }

  void gotFromDBInternal(Integer holderId, Collection<LeasePath> entityList) {
    gotFromDBInternal(entityList);

    if (entityList == null || entityList.isEmpty()) {
      getPathList(holderId).clear();
    }
  }
  
  private Set<LeasePath> getPathList(int holderId) {
    Set<LeasePath> hopLeasePaths = hIdToLPsMap.get(holderId);
    if (hopLeasePaths == null) {
      hopLeasePaths = new HashSet<>();
      hIdToLPsMap.put(holderId, hopLeasePaths);
    }
    return hopLeasePaths;
  }

  private void removeFromHIdToLPsMap(LeasePath hopLeasePath) {
    Set<LeasePath> hopLeasePaths = getPathList(hopLeasePath.getHolderId());
    hopLeasePaths.remove(hopLeasePath);
  }
 
  private void addToHIdToLPsMap(LeasePath leasePath) {    
    Set<LeasePath> hopLeasePaths = getPathList(leasePath.getHolderId());
    hopLeasePaths.add(leasePath);
  }
  
  private boolean containsInHIdToLPsMap(String path){
    for(int hid : hIdToLPsMap.keySet()){
      for(LeasePath lp : hIdToLPsMap.get(hid)){
        if(lp.getPath().equals(path)){
          return true;
        }
      }
    }
    return false;
  }
  
  private LeasePath getLPFromHIdToLPsMap(String path) throws StorageException{
    LeasePath leasePath = null;
    int lpCount = 0;
    for(int hid : hIdToLPsMap.keySet()){
      for(LeasePath lp : hIdToLPsMap.get(hid)){
        if(lp.getPath().equals(path)){
          lpCount ++;
          leasePath = lp;
        }
      }
    }
    
    if(lpCount > 1){
      throw new StorageException("A path can be lease only once at a time");
    }
    
    return leasePath;
  }
  
  class LeasePathPK {
    
    private final String path;
    private final int holderId;

    public LeasePathPK(String path, int holderId) {
        this.path = path;
        this.holderId = holderId;
    }

    public String getPath() {
        return path;
    }

    public int getHolderId() {
        return holderId;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.path != null ? this.path.hashCode() : 0);
        hash = 97 * hash + this.holderId;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final io.hops.transaction.context.LeasePathContext.LeasePathPK other 
                = (io.hops.transaction.context.LeasePathContext.LeasePathPK) obj;
        if ((this.path == null) ? (other.path != null) : !this.path.equals(other.path)) {
            return false;
        }
        if (this.holderId != other.holderId) {
            return false;
        }
        return true;
    }
}

}
