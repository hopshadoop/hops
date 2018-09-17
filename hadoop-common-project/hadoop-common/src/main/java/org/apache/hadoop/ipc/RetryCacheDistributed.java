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
package org.apache.hadoop.ipc;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.LightWeightCacheDistributed;
import org.apache.hadoop.util.LightWeightGSet;
import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.EntityManager;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Maintains a cache of non-idempotent requests that have been successfully
 * processed by the RPC server implementation, to handle the retries. A request
 * is uniquely identified by the unique client ID + call ID of the RPC request.
 * On receiving retried request, an entry will be found in the
 * {@link RetryCache} and the previous response is sent back to the request.
 * <p>
 * To look an implementation using this cache, see HDFS FSNamesystem class.
 */
@InterfaceAudience.Private
public class RetryCacheDistributed extends RetryCache{

  
   /**
   * CacheEntry with payload that tracks the previous response or parts of
   * previous response to be used for generating response for retried requests.
   */
  public static class CacheEntryWithPayload extends CacheEntry {
    private byte[] payload;
    
    CacheEntryWithPayload(byte[] clientId, int callId, byte[] payload,
        long expirationTime) {
      super(clientId, callId, expirationTime);
      this.payload = payload;
    }

    public CacheEntryWithPayload(byte[] clientId, int callId, byte[] payload,
        long expirationTime, byte state) {
      super(clientId, callId, expirationTime);
      this.payload = payload;
      this.state = state;
    }
    
    CacheEntryWithPayload(byte[] clientId, int callId, byte[] payload,
        long expirationTime, boolean success) {
     super(clientId, callId, expirationTime, success);
     this.payload = payload;
   }

    /** Override equals to avoid findbugs warnings */
    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }

    /** Override hashcode to avoid findbugs warnings */
    @Override
    public int hashCode() {
      return super.hashCode();
    }

    public byte[] getPayload() {
      return payload;
    }
  }
  
//  private LightWeightGSet<CacheEntry, CacheEntry> set;
  
  /**
   * Constructor
   * @param cacheName name to identify the cache by
   * @param percentage percentage of total java heap space used by this cache
   * @param expirationTime time for an entry to expire in nanoseconds
   */
  public RetryCacheDistributed(String cacheName, double percentage, long expirationTime) {
    super(cacheName, percentage, expirationTime);
    int capacity = LightWeightGSet.computeCapacity(percentage, cacheName);
    capacity = capacity > MAX_CAPACITY ? capacity : MAX_CAPACITY;
    this.set = new LightWeightCacheDistributed(capacity, capacity, expirationTime, 0);
  }

  /**
   * This method handles the following conditions:
   * <ul>
   * <li>If retry is not to be processed, return null</li>
   * <li>If there is no cache entry, add a new entry {@code newEntry} and return
   * it.</li>
   * <li>If there is an existing entry, wait for its completion. If the
   * completion state is {@link CacheEntry#FAILED}, the expectation is that the
   * thread that waited for completion, retries the request. the
   * {@link CacheEntry} state is set to {@link CacheEntry#INPROGRESS} again.
   * <li>If the completion state is {@link CacheEntry#SUCCESS}, the entry is
   * returned so that the thread that waits for it can can return previous
   * response.</li>
   * <ul>
   * 
   * @return {@link CacheEntry}.
   */
  private CacheEntry waitForCompletion(CacheEntry newEntry) {
    CacheEntry mapEntry = null;
    lock.lock();
    try {
      mapEntry = (CacheEntry) set.get(newEntry);
      // If an entry in the cache does not exist, add a new one
      if (mapEntry == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding Rpc request clientId "
              + newEntry.clientIdMsb + newEntry.clientIdLsb + " callId "
              + newEntry.callId + " to retryCache");
        }
        set.put(newEntry);
        retryCacheMetrics.incrCacheUpdated();
        return newEntry;
      } else {
        retryCacheMetrics.incrCacheHit();
      }
    } finally {
      lock.unlock();
    }
    // Entry already exists in cache. Wait for completion and return its state
    Preconditions.checkNotNull(mapEntry,
        "Entry from the cache should not be null");
    // Wait for in progress request to complete
    synchronized (mapEntry) {
      while (mapEntry.state == CacheEntry.INPROGRESS) {
        try {
          mapEntry.wait();
        } catch (InterruptedException ie) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
      // Previous request has failed, the expectation is is that it will be
      // retried again.
      if (mapEntry.state != CacheEntry.SUCCESS) {
        mapEntry.state = CacheEntry.INPROGRESS;
      }
    }
    return mapEntry;
  }
  
  /** 
   * Add a new cache entry into the retry cache. The cache entry consists of 
   * clientId and callId extracted from editlog.
   */
  public void addCacheEntry(byte[] clientId, int callId) {
    CacheEntry newEntry = new CacheEntry(clientId, callId, System.currentTimeMillis()
        + expirationTime, true);
    lock.lock();
    try {
      set.put(newEntry);
    } finally {
      lock.unlock();
    }
    retryCacheMetrics.incrCacheUpdated();
  }
  
  public void addCacheEntryWithPayload(byte[] clientId, int callId,
      byte[] payload) {
    // since the entry is loaded from editlog, we can assume it succeeded.    
    CacheEntry newEntry = new CacheEntryWithPayload(clientId, callId, payload,
        System.currentTimeMillis() + expirationTime, true);
    lock.lock();
    try {
      set.put(newEntry);
    } finally {
      lock.unlock();
    }
    retryCacheMetrics.incrCacheUpdated();
  }

  private static CacheEntry newEntry(long expirationTime) {
    return new CacheEntry(Server.getClientId(), Server.getCallId(),
        System.currentTimeMillis() + expirationTime);
  }

  private static CacheEntryWithPayload newEntry(byte[] payload,
      long expirationTime) {
    return new CacheEntryWithPayload(Server.getClientId(), Server.getCallId(),
        payload, System.currentTimeMillis() + expirationTime);
  }
  
  /** Static method that provides null check for retryCache */
  public static CacheEntry waitForCompletion(RetryCacheDistributed cache) {
    if (skipRetryCache()) {
      return null;
    }
    return cache != null ? cache
        .waitForCompletion(newEntry(cache.expirationTime)) : null;
  }

  /** Static method that provides null check for retryCache */
  public static CacheEntryWithPayload waitForCompletion(RetryCacheDistributed cache,
      byte[] payload) {
    if (skipRetryCache()) {
      return null;
    }
    return (CacheEntryWithPayload) (cache != null ? cache
        .waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
  }

  public static void setState(CacheEntry e, boolean success) {
    if (e == null) {
      return;
    }
    e.completed(success);
    try{
    EntityManager.update(new RetryCacheEntry(e.getClientId(), e.getCallId(), null, e.getExpirationTime(),
        e.getState()));
    }catch(StorageException | TransactionContextException ex){
      LOG.error("did not persist cach to the database", ex);
    }
  }

  public static void setState(CacheEntryWithPayload e, boolean success, byte[] payload) throws TransactionContextException, StorageException {
    if (e == null) {
      return;
    }
    e.payload = payload;
    e.completed(success);
    EntityManager.update(new RetryCacheEntry(e.getClientId(), e.getCallId(), e.getPayload(), e.getExpirationTime(),
        e.getState()));
  }

  public static void clear(RetryCacheDistributed cache) {
    if (cache != null) {
      cache.set.clear();
      cache.incrCacheClearedCounter();
    }
  }
  
  public LinkedBlockingQueue<CacheEntry> getToRemove(){
    return ((LightWeightCacheDistributed)set).getToRemove();
  }
}
