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
package org.apache.hadoop.security.ssl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.ShutdownHookManager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool used by reloading key managers to schedule tasks
 */
public final class KeyManagersReloaderThreadPool {
  public static final int MAX_NUMBER_OF_RETRIES = 3;
  
  private static final int THREAD_POOL_SIZE = 10;
  private final ScheduledExecutorService scheduler;
  // For testing
  private final boolean isForTesting;
  private final List<ScheduledFuture> scheduledTasks;
  
  private static volatile KeyManagersReloaderThreadPool _INSTANCE;
  
  private KeyManagersReloaderThreadPool(boolean isForTesting) {
    this.isForTesting = isForTesting;
    scheduledTasks = new CopyOnWriteArrayList<>();
    scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Client certificate reloader Thread #%d")
            .build());
  
    ShutdownHookManager.get().addShutdownHook(new ShutdownHook(), 5);
  }
  
  public static KeyManagersReloaderThreadPool getInstance() {
    return getInstance(false);
  }
  
  public static KeyManagersReloaderThreadPool getInstance(boolean isForTesting) {
    if (_INSTANCE == null) {
      synchronized (KeyManagersReloaderThreadPool.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new KeyManagersReloaderThreadPool(isForTesting);
        }
      }
    }
    return _INSTANCE;
  }
  
  public ScheduledFuture scheduleTask(Runnable task, long period, TimeUnit timeUnit) {
    ScheduledFuture scheduledTask = scheduler.scheduleAtFixedRate(task, 0, period, timeUnit);
    if (isForTesting) {
      scheduledTasks.add(scheduledTask);
    }
    return scheduledTask;
  }
  
  // For testing
  public void clearListOfTasks() {
    scheduledTasks.clear();
  }
  
  public List<ScheduledFuture> getListOfTasks() {
    return scheduledTasks;
  }
  
  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      scheduler.shutdownNow();
    }
  }

  @VisibleForTesting
  public static void clearThreadPool() {
    _INSTANCE = null;
  }
}
