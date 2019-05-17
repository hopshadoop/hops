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
package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.EntityManager;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCacheDistributed;
import org.apache.hadoop.ipc.RetryCacheDistributed.CacheEntryWithPayload;

/**
 * A low memory footprint Cache which extends {@link LightWeightGSet}.
 * An entry in the cache is expired if
 * (1) it is added to the cache longer than the creation-expiration period, and
 * (2) it is not accessed for the access-expiration period.
 * When an entry is expired, it may be evicted from the cache.
 * When the size limit of the cache is set, the cache will evict the entries
 * with earliest expiration time, even if they are not expired.
 * 
 * It is guaranteed that number of entries in the cache is less than or equal
 * to the size limit.  However, It is not guaranteed that expired entries are
 * evicted from the cache. An expired entry may possibly be accessed after its
 * expiration time. In such case, the expiration time may be updated.
 *
 * This class does not support null entry.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the entries
 * @param <E> Entry type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link Entry} interface, and
 */
@InterfaceAudience.Private
public class LightWeightCacheDistributed extends LightWeightCache<CacheEntry, CacheEntry> {
  
  final private LinkedBlockingQueue<CacheEntry> toRemove = new LinkedBlockingQueue<>();
  /**
   * Entries of {@link LightWeightCache}.
   */
  public static interface Entry extends LightWeightCache.Entry {
    public void setFromDB();
    public boolean isFromDB();
  }

  /**
   * @param recommendedLength Recommended size of the internal array.
   * @param sizeLimit the limit of the size of the cache.
   *            The limit is disabled if it is <= 0.
   * @param creationExpirationPeriod the time period C > 0 in nanoseconds that
   *            the creation of an entry is expired if it is added to the cache
   *            longer than C.
   * @param accessExpirationPeriod the time period A >= 0 in nanoseconds that
   *            the access of an entry is expired if it is not accessed
   *            longer than A. 
   */
  public LightWeightCacheDistributed(final int recommendedLength,
      final int sizeLimit,
      final long creationExpirationPeriod,
      final long accessExpirationPeriod) {
    super(recommendedLength, sizeLimit,
        creationExpirationPeriod, accessExpirationPeriod, new Timer());
  }

  @VisibleForTesting
  LightWeightCacheDistributed(final int recommendedLength,
      final int sizeLimit,
      final long creationExpirationPeriod,
      final long accessExpirationPeriod,
      final Timer timer) {
    super(recommendedLength, sizeLimit, creationExpirationPeriod, accessExpirationPeriod, timer);
  }


  @Override
  protected CacheEntry evict() {
    CacheEntry polled= super.evict();
    toRemove.add(polled);
    return polled;
  }

  
  @Override
  public CacheEntry get(CacheEntry key) {
    CacheEntry entry = super.get(key);
    try {
      RetryCacheEntry existInDB = EntityManager.find(RetryCacheEntry.Finder.ByClientIdAndCallId, key.getClientId(), key.
          getCallId());
      if (existInDB != null && existInDB.getExpirationTime() > timer.now()) {
        byte state = existInDB.getState() == CacheEntry.INPROGRESS ? CacheEntry.FAILED : existInDB.getState();
        RetryCacheDistributed.CacheEntry exist = new CacheEntryWithPayload(existInDB.getClientId(), existInDB.
            getCallId(), existInDB.getPayload(), existInDB.getExpirationTime(), state);
        if (entry == null) {
          super.put(exist, accessExpirationPeriod);
          entry = exist;
        }
        existInDB.setExpirationTime(entry.getExpirationTime());
        EntityManager.update(existInDB);
      } else if (entry != null) {
        super.remove(entry);
        entry = null;
      }
    } catch (StorageException | TransactionContextException ex) {
      LOG.error("failed to get or update entry in DB", ex);
      entry = null;
    }
    return entry;
  }

  @Override
  public CacheEntry put(final CacheEntry entry) {
    final CacheEntry existing = super.put(entry);
    try{
    byte[] payload = null;
    if(entry instanceof CacheEntryWithPayload){
      payload = ((CacheEntryWithPayload)entry).getPayload();
    }
    EntityManager.update(new RetryCacheEntry(entry.getClientId(), entry.getCallId(), payload, entry.getExpirationTime(),
        entry.getState()));
    }catch(StorageException | TransactionContextException ex){
      LOG.error("failed to put entry ind db", ex);
    }
    return existing;
  }

  @Override
  public CacheEntry remove(CacheEntry key) {
    final CacheEntry removed = super.remove(key);
    if (removed != null) {
      toRemove.add(removed);
    }
    return removed;
  }
  
  public LinkedBlockingQueue<CacheEntry> getToRemove(){
    return toRemove;
  }
}
