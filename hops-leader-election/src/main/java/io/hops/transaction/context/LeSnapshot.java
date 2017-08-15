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
import io.hops.metadata.common.FinderType;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;

public abstract class LeSnapshot extends BaseEntityContext<Long, LeDescriptor> {

  private final LeDescriptorDataAccess<LeDescriptor> dataAccess;
  private boolean allRead = false;

  public LeSnapshot(LeDescriptorDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(LeDescriptor desc) throws TransactionContextException {
    super.update(desc);
    log("added-le-desc", "id", desc.getId(), "hostName", desc.getRpcAddresses(),
        "counter", desc.getCounter());
  }

  @Override
  public void remove(LeDescriptor desc) throws TransactionContextException {
    super.remove(desc);
    log("removed-le-desc", "id", desc.getId(), "hostName", desc.getRpcAddresses(),
        "counter", desc.getCounter());
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    allRead = false;
  }

  @Override
  Long getKey(LeDescriptor desc) {
    return desc.getId();
  }

  protected LeDescriptor findById(LeDescriptor.LeDescriptorFinder lFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long id = (Long) params[0];
    final int partitionKey = (Integer) params[1];
    LeDescriptor result = null;
    if (allRead || contains(id)) {
      result = get(id);
      hit(lFinder, result, "id", id);
    } else {
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPkey(id, partitionKey);
      gotFromDB(id, result);
      miss(lFinder, result, "id", id);
    }
    return result;
  }

  protected Collection<LeDescriptor> findAll(
      LeDescriptor.LeDescriptorFinder lFinder)
      throws StorageCallPreventedException, StorageException {
    Collection<LeDescriptor> result = null;
    if (allRead) {
      result = getAll();
      hit(lFinder, result);
    } else {
      aboutToAccessStorage(lFinder);
      result = dataAccess.findAll();
      allRead = true;
      gotFromDB(result);
      miss(lFinder, result);
    }
    return new ArrayList<LeDescriptor>(result);
  }

  public static class HdfsLESnapshot extends LeSnapshot {

    public HdfsLESnapshot(LeDescriptorDataAccess dataAccess) {
      super(dataAccess);
    }
    
    @Override
    public LeDescriptor find(FinderType<LeDescriptor> finder, Object... params)
        throws TransactionContextException, StorageException {
      LeDescriptor.HdfsLeDescriptor.Finder lFinder =
          (LeDescriptor.HdfsLeDescriptor.Finder) finder;
      switch (lFinder) {
        case ById:
          return findById(lFinder, params);
      }

      throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public Collection<LeDescriptor> findList(FinderType<LeDescriptor> finder,
        Object... params) throws TransactionContextException, StorageException {
      LeDescriptor.HdfsLeDescriptor.Finder lFinder =
          (LeDescriptor.HdfsLeDescriptor.Finder) finder;
      switch (lFinder) {
        case All:
          return findAll(lFinder);
      }
      throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  public static class YarnLESnapshot extends LeSnapshot {

    public YarnLESnapshot(LeDescriptorDataAccess dataAccess) {
      super(dataAccess);
    }

    @Override
    public LeDescriptor find(FinderType<LeDescriptor> finder, Object... params)
        throws TransactionContextException, StorageException {
      LeDescriptor.YarnLeDescriptor.Finder lFinder =
          (LeDescriptor.YarnLeDescriptor.Finder) finder;
      switch (lFinder) {
        case ById:
          return findById(lFinder, params);
      }

      throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public Collection<LeDescriptor> findList(FinderType<LeDescriptor> finder,
        Object... params) throws TransactionContextException, StorageException {
      LeDescriptor.YarnLeDescriptor.Finder lFinder =
          (LeDescriptor.YarnLeDescriptor.Finder) finder;
      switch (lFinder) {
        case All:
          return findAll(lFinder);
      }
      throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
}
