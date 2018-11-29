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
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuotaUpdateContext
    extends BaseEntityContext<Integer, QuotaUpdate> {

  private final QuotaUpdateDataAccess<QuotaUpdate> dataAccess;
  private final Map<Long, List<QuotaUpdate>> inodeIdToQuotaUpdates =
      new HashMap<>();

  public QuotaUpdateContext(QuotaUpdateDataAccess<QuotaUpdate> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(QuotaUpdate quotaUpdate)
      throws TransactionContextException {
    super.update(quotaUpdate);
    if(isLogTraceEnabled()) {
      log("added-quotaUpdate", "id", quotaUpdate.getId(), "inodeId", quotaUpdate.getInodeId(), "dsDeltea", quotaUpdate.getDiskspaceDelta(), "nsDelta", quotaUpdate.getNamespaceDelta());
    }
  }

  @Override
  public void remove(QuotaUpdate quotaUpdate)
      throws TransactionContextException {
    if (quotaUpdate != null) {
      if (!contains(quotaUpdate.getId())) {
        super.update(quotaUpdate);
      }
    }
    super.remove(quotaUpdate);
    if(isLogTraceEnabled()) {
      log("removed-quotaUpdate", "id", quotaUpdate);
    }
  }

  @Override
  public Collection<QuotaUpdate> findList(FinderType<QuotaUpdate> finder,
      Object... params) throws TransactionContextException, StorageException {
    QuotaUpdate.Finder qFinder = (QuotaUpdate.Finder) finder;
    switch (qFinder) {
      case ByINodeId:
        return findByINodeId(qFinder, params);
    }
    throw new UnsupportedOperationException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    Collection<QuotaUpdate> modified =
        new ArrayList<>(getModified());
    modified.addAll(getAdded());
    dataAccess.prepare(modified, getRemovedForced());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeIdToQuotaUpdates.clear();
  }

  @Override
  Integer getKey(QuotaUpdate quotaUpdate) {
    return quotaUpdate.getId();
  }

  private List<QuotaUpdate> findByINodeId(QuotaUpdate.Finder qFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];
    List<QuotaUpdate> result = null;
    if (inodeIdToQuotaUpdates.containsKey(inodeId)) {
      result = inodeIdToQuotaUpdates.get(inodeId);
      hit(qFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(qFinder, params);
      result = dataAccess.findByInodeId(inodeId);
      gotFromDB(inodeId, result);
      miss(qFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private void gotFromDB(long inodeId, List<QuotaUpdate> quotaUpdates) {
    gotFromDB(quotaUpdates);
    inodeIdToQuotaUpdates.put(inodeId, quotaUpdates);
  }


}
