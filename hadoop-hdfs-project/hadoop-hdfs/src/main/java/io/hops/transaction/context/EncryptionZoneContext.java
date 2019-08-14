/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.EncryptionZoneDataAccess;
import io.hops.metadata.hdfs.entity.EncryptionZone;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class EncryptionZoneContext extends BaseEntityContext<Long, EncryptionZone> {

  private final EncryptionZoneDataAccess<EncryptionZone> dataAccess;

  public EncryptionZoneContext(EncryptionZoneDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  Long getKey(EncryptionZone encryptionZone) {
    return encryptionZone.getInodeId();
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public EncryptionZone find(FinderType<EncryptionZone> finder, Object... params) throws TransactionContextException,
      StorageException {
    EncryptionZone.Finder xfinder = (EncryptionZone.Finder) finder;
    switch (xfinder) {
      case ByPrimaryKeyInContext:
        return findInContextByPrimaryKey(xfinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<EncryptionZone> findList(FinderType<EncryptionZone> finder, Object... params) throws
      TransactionContextException, StorageException {
    EncryptionZone.Finder xfinder = (EncryptionZone.Finder) finder;
    switch (xfinder) {
      case ByPrimaryKeyBatch:
        return findByPrimaryKeyBatch(xfinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  private EncryptionZone findInContextByPrimaryKey(EncryptionZone.Finder finder, Object[] params) throws
      StorageException, StorageCallPreventedException {
    final Long pk = (Long) params[0];
    EncryptionZone result = null;
    if (contains(pk)) {
      result = get(pk);
      hit(finder, result, "pk", pk, "results", result);
    }
    return result;
  }

  private Collection<EncryptionZone> findByPrimaryKeyBatch(EncryptionZone.Finder finder, Object[] params) throws
      StorageException, StorageCallPreventedException {
    final List<Long> pks = (List<Long>) params[0];
    List<EncryptionZone> results = null;
    if (containsAll(pks)) {
      results = getAll(pks);
      hit(finder, results, "pks", pks, "results", results);
    } else {
      aboutToAccessStorage(finder, params);
      results = dataAccess.getEncryptionZoneByInodeIdBatch(pks);
      gotFromDB(pks, results);
      miss(finder, results, "pks", pks, "results", results);
    }
    return results;
  }

  private void gotFromDB(List<Long> pks, List<EncryptionZone> results) {
    Set<Long> notFoundPks = Sets.newHashSet(pks);
    for (EncryptionZone ez : results) {
      if (ez.getZoneInfo() != null) {
        gotFromDB(ez);
      } else {
        gotFromDB(ez.getInodeId(), null);
      }
      notFoundPks.remove(ez.getInodeId());
    }

    for (Long pk : notFoundPks) {
      gotFromDB(pk, null);
    }
  }

  private boolean containsAll(List<Long> pks) {
    for (Long pk : pks) {
      if (!contains(pk)) {
        return false;
      }
    }
    return true;
  }

  private List<EncryptionZone> getAll(List<Long> pks) {
    List<EncryptionZone> attrs = Lists.newArrayListWithExpectedSize(pks.size());
    for (Long pk : pks) {
      attrs.add(get(pk));
    }
    return attrs;
  }

}
