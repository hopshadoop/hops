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

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.util.HashMap;
import java.util.Map;

public class LeaseContext extends BaseEntityContext<String, Lease> {

  private final LeaseDataAccess<Lease> dataAccess;
  private final Map<Integer, Lease> idToLease = new HashMap<>();

  public LeaseContext(LeaseDataAccess<Lease> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(Lease lease) throws TransactionContextException {
    super.update(lease);
    idToLease.put(lease.getHolderID(), lease);
    if(isLogTraceEnabled()) {
      log("added-lease", "holder", lease.getHolder(), "hid", lease.getHolderID());
    }
  }

  @Override
  public int count(CounterType<Lease> counter, Object... params)
      throws TransactionContextException, StorageException {
    Lease.Counter lCounter = (Lease.Counter) counter;
    switch (lCounter) {
      case All:
        if(isLogTraceEnabled()) {
          log("count-all-leases");
        }
        return dataAccess.countAll();
    }
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public Lease find(FinderType<Lease> finder, Object... params)
      throws TransactionContextException, StorageException {
    Lease.Finder lFinder = (Lease.Finder) finder;
    switch (lFinder) {
      case ByHolder:
        return findByHolder(lFinder, params);

      case ByHolderId:
        return findByHolderId(lFinder, params);

    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<Lease> findList(FinderType<Lease> finder, Object... params)
      throws TransactionContextException, StorageException {
    Lease.Finder lFinder = (Lease.Finder) finder;
    switch (lFinder) {
      case All:
        return findAll(lFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  @Override
  public void remove(Lease lease) throws TransactionContextException {
    super.remove(lease);
    idToLease.remove(lease.getHolderID());
    if(isLogTraceEnabled()) {
      log("removed-lease", "holder", lease.getHolder());
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    idToLease.clear();
  }

  @Override
  String getKey(Lease lease) {
    return lease.getHolder();
  }

  private Lease findByHolder(Lease.Finder lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final String holder = (String) params[0];
    final int holderId = (Integer) params[1];
    Lease result = null;
    if (contains(holder)) {
      result = get(holder);
      hit(lFinder, result, "holder", holder, "holderId", holderId);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPKey(holder, holderId);
      gotFromDB(holder, result);
      idToLease.put(holderId, result);
      miss(lFinder, result, "holder", holder, "holderId", holderId);
    }
    return result;
  }

  private Lease findByHolderId(Lease.Finder lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int holderId = (Integer) params[0];
    Lease result = null;
    if (idToLease.containsKey(holderId)) {
      result = idToLease.get(holderId);
      hit(lFinder, result, "hid", holderId);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByHolderId(holderId);
      gotFromDB(result);
      idToLease.put(holderId, result);
      miss(lFinder, result, "hid", holderId);
    }
    return result;
  }

  private Collection<Lease> findAll(Lease.Finder lFinder, Object[] params) throws StorageCallPreventedException,
      StorageException {
    Collection<Lease> result = null;
    aboutToAccessStorage(lFinder, params);
    result = dataAccess.findAll();
    gotFromDB(result);
    for (Lease lease : result) {
      idToLease.put(lease.getHolderID(), lease);
    }
    miss(lFinder, result);

    return result;
  }
}
