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
package org.apache.hadoop.crypto.key.kms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A Utility class that maintains a Queue of entries for a given key. It tries
 * to ensure that there is are always at-least <code>numValues</code> entries
 * available for the client to consume for a particular key.
 * It also uses an underlying Cache to evict queues for keys that have not been
 * accessed for a configurable period of time.
 * Implementing classes are required to implement the
 * <code>QueueRefiller</code> interface that exposes a method to refill the
 * queue, when empty
 */
@InterfaceAudience.Private
public class ValueQueue <E> {

  /**
   * QueueRefiller interface a client must implement to use this class
   */
  public interface QueueRefiller <E> {
    /**
     * Method that has to be implemented by implementing classes to fill the
     * Queue.
     * @param keyName Key name
     * @param keyQueue Queue that needs to be filled
     * @param numValues number of Values to be added to the queue.
     * @throws IOException
     */
    public void fillQueueForKey(String keyName,
        Queue<E> keyQueue, int numValues) throws IOException;
  }

  private static final String REFILL_THREAD =
      ValueQueue.class.getName() + "_thread";
  private static final int LOCK_ARRAY_SIZE = 16;
  // Using a mask assuming array size is the power of 2, of MAX_VALUE.
  private static final int MASK = LOCK_ARRAY_SIZE == Integer.MAX_VALUE ?
      LOCK_ARRAY_SIZE :
      LOCK_ARRAY_SIZE - 1;

  private final LoadingCache<String, LinkedBlockingQueue<E>> keyQueues;
  // Stripped rwlocks based on key name to synchronize the queue from
  // the sync'ed rw-thread and the background async refill thread.
  private final List<ReadWriteLock> lockArray =
      new ArrayList<>(LOCK_ARRAY_SIZE);
  private final ThreadPoolExecutor executor;
  private final UniqueKeyBlockingQueue queue = new UniqueKeyBlockingQueue();
  private final QueueRefiller<E> refiller;
  private final SyncGenerationPolicy policy;

  private final int numValues;
  private final float lowWatermark;

  private volatile boolean executorThreadsStarted = false;

  /**
   * A <code>Runnable</code> which takes a string name.
   */
  private abstract static class NamedRunnable implements Runnable {
    final String name;
    private AtomicBoolean canceled = new AtomicBoolean(false);
    private NamedRunnable(String keyName) {
      this.name = keyName;
    }

    public void cancel() {
      canceled.set(true);
    }

    public boolean isCanceled() {
      return canceled.get();
    }
  }

  private void readLock(String keyName) {
    getLock(keyName).readLock().lock();
  }

  private void readUnlock(String keyName) {
    getLock(keyName).readLock().unlock();
  }

  private void writeUnlock(String keyName) {
    getLock(keyName).writeLock().unlock();
  }

  private void writeLock(String keyName) {
    getLock(keyName).writeLock().lock();
  }

  /**
   * Get the stripped lock given a key name.
   *
   * @param keyName The key name.
   */
  private ReadWriteLock getLock(String keyName) {
    return lockArray.get(indexFor(keyName));
  }

  private static int indexFor(String keyName) {
    return keyName.hashCode() & MASK;
  }

  /**
   * This backing blocking queue used in conjunction with the
   * <code>ThreadPoolExecutor</code> used by the <code>ValueQueue</code>. This
   * Queue accepts a task only if the task is not currently in the process
   * of being run by a thread which is implied by the presence of the key
   * in the <code>keysInProgress</code> set.
   *
   * NOTE: Only methods that ware explicitly called by the
   * <code>ThreadPoolExecutor</code> need to be over-ridden.
   */
  private static class UniqueKeyBlockingQueue extends
      LinkedBlockingQueue<Runnable> {

    private static final long serialVersionUID = -2152747693695890371L;
    private HashMap<String, Runnable> keysInProgress = new HashMap<>();

    @Override
    public synchronized void put(Runnable e) throws InterruptedException {
      if (!keysInProgress.containsKey(((NamedRunnable)e).name)) {
        keysInProgress.put(((NamedRunnable)e).name, e);
        super.put(e);
      }
    }

    @Override
    public Runnable take() throws InterruptedException {
      Runnable k = super.take();
      if (k != null) {
        keysInProgress.remove(((NamedRunnable)k).name);
      }
      return k;
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit)
        throws InterruptedException {
      Runnable k = super.poll(timeout, unit);
      if (k != null) {
        keysInProgress.remove(((NamedRunnable)k).name);
      }
      return k;
    }

    public Runnable deleteByName(String name) {
      NamedRunnable e = (NamedRunnable) keysInProgress.remove(name);
      if (e != null) {
        e.cancel();
        super.remove(e);
      }
      return e;
    }
  }

  /**
   * Policy to decide how many values to return to client when client asks for
   * "n" values and Queue is empty.
   * This decides how many values to return when client calls "getAtMost"
   */
  public enum SyncGenerationPolicy {
    ATLEAST_ONE, // Return atleast 1 value
    LOW_WATERMARK, // Return min(n, lowWatermark * numValues) values
    ALL // Return n values
  }

  /**
   * Constructor takes the following tunable configuration parameters
   * @param numValues The number of values cached in the Queue for a
   *    particular key.
   * @param lowWatermark The ratio of (number of current entries/numValues)
   *    below which the <code>fillQueueForKey()</code> funciton will be
   *    invoked to fill the Queue.
   * @param expiry Expiry time after which the Key and associated Queue are
   *    evicted from the cache.
   * @param numFillerThreads Number of threads to use for the filler thread
   * @param policy The SyncGenerationPolicy to use when client
   *    calls "getAtMost"
   * @param refiller implementation of the QueueRefiller
   */
  public ValueQueue(final int numValues, final float lowWatermark,
      long expiry, int numFillerThreads, SyncGenerationPolicy policy,
      final QueueRefiller<E> refiller) {
    Preconditions.checkArgument(numValues > 0, "\"numValues\" must be > 0");
    Preconditions.checkArgument(((lowWatermark > 0)&&(lowWatermark <= 1)),
        "\"lowWatermark\" must be > 0 and <= 1");
    final int watermarkValue = (int) (numValues * lowWatermark);
    Preconditions.checkArgument(watermarkValue > 0,
        "(int) (\"numValues\" * \"lowWatermark\") must be > 0");
    Preconditions.checkArgument(expiry > 0, "\"expiry\" must be > 0");
    Preconditions.checkArgument(numFillerThreads > 0,
        "\"numFillerThreads\" must be > 0");
    Preconditions.checkNotNull(policy, "\"policy\" must not be null");
    this.refiller = refiller;
    this.policy = policy;
    this.numValues = numValues;
    this.lowWatermark = lowWatermark;
    for (int i = 0; i < LOCK_ARRAY_SIZE; ++i) {
      lockArray.add(i, new ReentrantReadWriteLock());
    }
    keyQueues = CacheBuilder.newBuilder()
            .expireAfterAccess(expiry, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<String, LinkedBlockingQueue<E>>() {
                  @Override
                  public LinkedBlockingQueue<E> load(String keyName)
                      throws Exception {
                    LinkedBlockingQueue<E> keyQueue =
                        new LinkedBlockingQueue<E>();
                    refiller.fillQueueForKey(keyName, keyQueue, watermarkValue);
                    return keyQueue;
                  }
                });

    executor =
        new ThreadPoolExecutor(numFillerThreads, numFillerThreads, 0L,
            TimeUnit.MILLISECONDS, queue, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(REFILL_THREAD).build());
  }

  public ValueQueue(final int numValues, final float lowWaterMark, long expiry,
      int numFillerThreads, QueueRefiller<E> fetcher) {
    this(numValues, lowWaterMark, expiry, numFillerThreads,
        SyncGenerationPolicy.ALL, fetcher);
  }

  /**
   * Initializes the Value Queues for the provided keys by calling the
   * fill Method with "numInitValues" values
   * @param keyNames Array of key Names
   * @throws ExecutionException
   */
  public void initializeQueuesForKeys(String... keyNames)
      throws ExecutionException {
    for (String keyName : keyNames) {
      keyQueues.get(keyName);
    }
  }

  /**
   * This removes the value currently at the head of the Queue for the
   * provided key. Will immediately fire the Queue filler function if key
   * does not exist.
   * If Queue exists but all values are drained, It will ask the generator
   * function to add 1 value to Queue and then drain it.
   * @param keyName String key name
   * @return E the next value in the Queue
   * @throws IOException
   * @throws ExecutionException
   */
  public E getNext(String keyName)
      throws IOException, ExecutionException {
    return getAtMost(keyName, 1).get(0);
  }

  /**
   * Drains the Queue for the provided key.
   *
   * @param keyName the key to drain the Queue for
   */
  public void drain(String keyName) {
    try {
      Runnable e;
      while ((e = queue.deleteByName(keyName)) != null) {
        executor.remove(e);
      }
      writeLock(keyName);
      try {
        keyQueues.get(keyName).clear();
      } finally {
        writeUnlock(keyName);
      }
    } catch (ExecutionException ex) {
      //NOP
    }
  }

  /**
   * Get size of the Queue for keyName. This is only used in unit tests.
   * @param keyName the key name
   * @return int queue size
   */
  public int getSize(String keyName) {
    readLock(keyName);
    try {
      // We can't do keyQueues.get(keyName).size() here,
      // since that will have the side effect of populating the cache.
      Map<String, LinkedBlockingQueue<E>> map =
          keyQueues.getAllPresent(Arrays.asList(keyName));
      if (map.get(keyName) == null) {
        return 0;
      }
      return map.get(keyName).size();
    } finally {
      readUnlock(keyName);
    }
  }

  /**
   * This removes the "num" values currently at the head of the Queue for the
   * provided key. Will immediately fire the Queue filler function if key
   * does not exist
   * How many values are actually returned is governed by the
   * <code>SyncGenerationPolicy</code> specified by the user.
   * @param keyName String key name
   * @param num Minimum number of values to return.
   * @return List<E> values returned
   * @throws IOException
   * @throws ExecutionException
   */
  public List<E> getAtMost(String keyName, int num) throws IOException,
      ExecutionException {
    LinkedBlockingQueue<E> keyQueue = keyQueues.get(keyName);
    // Using poll to avoid race condition..
    LinkedList<E> ekvs = new LinkedList<E>();
    try {
      for (int i = 0; i < num; i++) {
        readLock(keyName);
        E val = keyQueue.poll();
        readUnlock(keyName);
        // If queue is empty now, Based on the provided SyncGenerationPolicy,
        // figure out how many new values need to be generated synchronously
        if (val == null) {
          // Synchronous call to get remaining values
          int numToFill = 0;
          switch (policy) {
          case ATLEAST_ONE:
            numToFill = (ekvs.size() < 1) ? 1 : 0;
            break;
          case LOW_WATERMARK:
            numToFill =
                Math.min(num, (int) (lowWatermark * numValues)) - ekvs.size();
            break;
          case ALL:
            numToFill = num - ekvs.size();
            break;
          }
          // Synchronous fill if not enough values found
          if (numToFill > 0) {
            refiller.fillQueueForKey(keyName, ekvs, numToFill);
          }
          // Asynch task to fill > lowWatermark
          if (i <= (int) (lowWatermark * numValues)) {
            submitRefillTask(keyName, keyQueue);
          }
          return ekvs;
        }
        ekvs.add(val);
      }
    } catch (Exception e) {
      throw new IOException("Exception while contacting value generator ", e);
    }
    return ekvs;
  }

  private void submitRefillTask(final String keyName,
      final Queue<E> keyQueue) throws InterruptedException {
    if (!executorThreadsStarted) {
      synchronized (this) {
        if (!executorThreadsStarted) {
          // To ensure all requests are first queued, make coreThreads =
          // maxThreads
          // and pre-start all the Core Threads.
          executor.prestartAllCoreThreads();
          executorThreadsStarted = true;
        }
      }
    }
    // The submit/execute method of the ThreadPoolExecutor is bypassed and
    // the Runnable is directly put in the backing BlockingQueue so that we
    // can control exactly how the runnable is inserted into the queue.
    queue.put(
        new NamedRunnable(keyName) {
          @Override
          public void run() {
            int cacheSize = numValues;
            int threshold = (int) (lowWatermark * (float) cacheSize);
            // Need to ensure that only one refill task per key is executed
            try {
              writeLock(keyName);
              try {
                if (keyQueue.size() < threshold && !isCanceled()) {
                  refiller.fillQueueForKey(name, keyQueue,
                      cacheSize - keyQueue.size());
                }
                if (isCanceled()) {
                  keyQueue.clear();
                }
              } finally {
                writeUnlock(keyName);
              }
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
        );
  }

  /**
   * Cleanly shutdown
   */
  public void shutdown() {
    executor.shutdownNow();
  }

}
