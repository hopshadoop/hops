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
import java.util.Map;
import java.util.Set;

public class LeasePathContext extends BaseEntityContext<String, LeasePath> {

  private final LeasePathDataAccess<LeasePath> dataAccess;
  private final Map<Integer, Set<LeasePath>> holderIdToLeasePath =
      new HashMap<Integer, Set<LeasePath>>();

  public LeasePathContext(LeasePathDataAccess<LeasePath> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(LeasePath hopLeasePath)
      throws TransactionContextException {
    super.update(hopLeasePath);
    addInternal(hopLeasePath);
    log("added-lpath", "path", hopLeasePath.getPath(), "hid",
        hopLeasePath.getHolderId());
  }

  @Override
  public void remove(LeasePath hopLeasePath)
      throws TransactionContextException {
    super.remove(hopLeasePath);
    removeInternal(hopLeasePath);
    log("removed-lpath", "path", hopLeasePath.getPath());
  }

  @Override
  public LeasePath find(FinderType<LeasePath> finder, Object... params)
      throws TransactionContextException, StorageException {
    LeasePath.Finder lFinder = (LeasePath.Finder) finder;
    switch (lFinder) {
      case ByPath:
        return findByPrimaryKey(lFinder, params);
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
    holderIdToLeasePath.clear();
  }

  @Override
  String getKey(LeasePath hopLeasePath) {
    return hopLeasePath.getPath();
  }

  private LeasePath findByPrimaryKey(LeasePath.Finder lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final String path = (String) params[0];
    LeasePath result = null;
    if (contains(path)) {
      result = get(path);
      hit(lFinder, result, "path", path);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPKey(path);
      gotFromDB(path, result);
      miss(lFinder, result, "path", path);
    }
    return result;
  }

  private Collection<LeasePath> findByHolderId(LeasePath.Finder lFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final int holderId = (Integer) params[0];
    Collection<LeasePath> result = null;
    if (holderIdToLeasePath.containsKey(holderId)) {
      result = new ArrayList<LeasePath>(holderIdToLeasePath.get(holderId));
      hit(lFinder, result, "hid", holderId);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByHolderId(holderId);
      gotFromDB(holderId, result);
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
      gotFromDB(result);
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

  @Override
  void gotFromDB(String entityKey, LeasePath leasePath) {
    super.gotFromDB(entityKey, leasePath);
    addInternal(leasePath);
  }

  @Override
  void gotFromDB(Collection<LeasePath> entityList) {
    super.gotFromDB(entityList);
    addInternal(entityList);
  }

  private void gotFromDB(int holderId, Collection<LeasePath> leasePaths) {
    gotFromDB(leasePaths);
    if (leasePaths == null) {
      addInternal(holderId, null);
    }
  }

  private void addInternal(Collection<LeasePath> leasePaths) {
    if (leasePaths == null) {
      return;
    }
    for (LeasePath leasePath : leasePaths) {
      addInternal(leasePath);
    }
  }

  private void addInternal(LeasePath leasePath) {
    if (leasePath == null) {
      return;
    }
    addInternal(leasePath.getHolderId(), leasePath);
  }

  private void addInternal(int holderId, LeasePath leasePath) {
    Set<LeasePath> hopLeasePaths = holderIdToLeasePath.get(holderId);
    if (hopLeasePaths == null) {
      hopLeasePaths = new HashSet<LeasePath>();
      holderIdToLeasePath.put(holderId, hopLeasePaths);
    }
    hopLeasePaths.add(leasePath);
  }

  private void removeInternal(LeasePath hopLeasePath) {
    Set<LeasePath> hopLeasePaths =
        holderIdToLeasePath.get(hopLeasePath.getHolderId());
    if (hopLeasePaths != null) {
      hopLeasePaths.remove(hopLeasePath);
    }
  }
}
