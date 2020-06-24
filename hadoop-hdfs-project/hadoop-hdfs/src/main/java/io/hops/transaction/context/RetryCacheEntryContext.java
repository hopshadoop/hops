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
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.metadata.hdfs.entity.RetryCacheEntry.PrimaryKey;
import io.hops.transaction.lock.TransactionLocks;

public class RetryCacheEntryContext extends BaseEntityContext<PrimaryKey, RetryCacheEntry> {
  private RetryCacheEntryDataAccess dataAccess;
  
  public RetryCacheEntryContext(RetryCacheEntryDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }
  
  @Override
  public RetryCacheEntry find(FinderType<RetryCacheEntry> finder, Object... params)
      throws TransactionContextException, StorageException {
    RetryCacheEntry.Finder hbFinder = (RetryCacheEntry.Finder) finder;
    switch (hbFinder){
      case ByPK:
        return findByPrimaryKey(hbFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  private RetryCacheEntry findByPrimaryKey(RetryCacheEntry.Finder hbFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    byte[] clientId = (byte[]) params[0];
    int callId = (Integer) params[1];
    long epoch = (Long) params[2];
    RetryCacheEntry.PrimaryKey pk = new RetryCacheEntry.PrimaryKey(clientId, callId, epoch);
    RetryCacheEntry result;
    if (contains(pk)){
      result = get(pk);
      hit(hbFinder, result, "clientId", clientId, "callId", callId, "epoch", epoch);
    } else {
      aboutToAccessStorage(hbFinder, params);
      result = dataAccess.find(pk);
      gotFromDB(pk, result);
      miss(hbFinder, result, "clientId", clientId, "callId", callId, "epoch", epoch);
    }
    
    return result;
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(),getModified());
  }
  
  @Override
  RetryCacheEntry.PrimaryKey getKey(RetryCacheEntry retryCacheEntry) {
    return new RetryCacheEntry.PrimaryKey(retryCacheEntry.getClientId(),
            retryCacheEntry.getCallId(), retryCacheEntry.getEpoch());
  }
}
