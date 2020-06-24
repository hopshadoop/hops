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
package io.hops.transaction.lock;

import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.EntityManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ipc.RetryCache;

public class RetryCacheEntryLock extends Lock {

  private final List<byte[]> clientId  = new ArrayList<>();
  private final List<Integer> callId = new ArrayList<>();
  private final List<Long> epochs = new ArrayList<>();

  RetryCacheEntryLock(byte[] clientId, int callId, long epoch) {
    this.clientId.add(clientId);
    this.callId.add(callId);
    this.epochs.add(epoch);
  }


  RetryCacheEntryLock(List<RetryCache.CacheEntry> entries) {
    for(RetryCache.CacheEntry entry : entries){
      this.clientId.add(entry.getClientId());
      this.callId.add(entry.getCallId());
      this.epochs.add(entry.getEpoch());
    }
  }
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(TransactionLockTypes.LockType.WRITE);
    for (int i = 0; i < clientId.size(); i++) {
      byte[] cid = clientId.get(i);
      int clid = callId.get(i);
      long epoch = epochs.get(i);
      EntityManager.find(RetryCacheEntry.Finder.ByPK, cid,
              clid, epoch);
    }
  }

  @Override
  protected Type getType() {
    return Type.retryCachEntry;
  }
}
