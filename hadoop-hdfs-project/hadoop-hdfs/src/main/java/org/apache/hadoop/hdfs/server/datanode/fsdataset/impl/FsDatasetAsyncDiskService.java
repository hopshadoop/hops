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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is a container of multiple thread pools, each for a volume,
 * so that we can schedule async disk operations easily.
 * <p/>
 * Examples of async disk operations are deletion of block files.
 * We don't want to create a new thread for each of the deletion request, and
 * we don't want to do all deletions in the heartbeat thread since deletion
 * can be slow, and we don't want to use a single thread pool because that
 * is inefficient when we have more than 1 volume.  AsyncDiskService is the
 * solution for these.
 * <p/>
 * This class and {@link org.apache.hadoop.util.AsyncDiskService} are similar.
 * They should be combined.
 */
class FsDatasetAsyncDiskService {
  public static final Log LOG =
      LogFactory.getLog(FsDatasetAsyncDiskService.class);
  
  // ThreadPool core pool size
  private static final int CORE_THREADS_PER_VOLUME = 1;
  // ThreadPool maximum pool size
  private static final int MAXIMUM_THREADS_PER_VOLUME = 4;
  // ThreadPool keep-alive time for threads over core pool size
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;
  
  private final DataNode datanode;
  private final ThreadGroup threadGroup;
  private Map<File, ThreadPoolExecutor> executors =
      new HashMap<File, ThreadPoolExecutor>();
  
  /**
   * Create a AsyncDiskServices with a set of volumes (specified by their
   * root directories).
   * <p/>
   * The AsyncDiskServices uses one ThreadPool per volume to do the async
   * disk operations.
   */
  FsDatasetAsyncDiskService(DataNode datanode) {
    this.datanode = datanode;
    this.threadGroup = new ThreadGroup(getClass().getSimpleName());
  }

  private void addExecutorForVolume(final File volume) {
    ThreadFactory threadFactory = new ThreadFactory() {
      int counter = 0;

      @Override
      public Thread newThread(Runnable r) {
        int thisIndex;
        synchronized (this) {
          thisIndex = counter++;
        }
        Thread t = new Thread(threadGroup, r);
        t.setName("Async disk worker #" + thisIndex +
            " for volume " + volume);
        return t;
      }
    };

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME,
        THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory);

    // This can reduce the number of running threads
    executor.allowCoreThreadTimeOut(true);
    executors.put(volume, executor);
  }

  /**
   * Starts AsyncDiskService for a new volume
   * @param volume the root of the new data volume.
   */
  synchronized void addVolume(File volume) {
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    if (executors.containsKey(volume)) {
      throw new RuntimeException("Volume " + volume + " already exists.");
    }
    addExecutorForVolume(volume);
  }

  /**
   * Stops AsyncDiskService for a volume.
   * @param volume the root of the volume.
   */
  synchronized void removeVolume(File volume) {
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(volume);
    if (executor == null) {
      throw new RuntimeException("Can not find volume " + volume
          + " to remove.");
    } else {
      executor.shutdown();
      executors.remove(volume);
    }
  }
  
  synchronized long countPendingDeletions() {
    long count = 0;
    for (ThreadPoolExecutor exec : executors.values()) {
      count += exec.getTaskCount() - exec.getCompletedTaskCount();
    }
    return count;
  }
  
  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  synchronized void execute(File root, Runnable task) {
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(root);
    if (executor == null) {
      throw new RuntimeException(
          "Cannot find root " + root + " for execution of task " + task);
    } else {
      executor.execute(task);
    }
  }
  
  /**
   * Gracefully shut down all ThreadPool. Will wait for all deletion
   * tasks to finish.
   */
  synchronized void shutdown() {
    if (executors == null) {
      LOG.warn("AsyncDiskService has already shut down.");
    } else {
      LOG.info("Shutting down all async disk service threads");
      
      for (Map.Entry<File, ThreadPoolExecutor> e : executors.entrySet()) {
        e.getValue().shutdown();
      }
      // clear the executor map so that calling execute again will fail.
      executors = null;
      
      LOG.info("All async disk service threads have been shut down");
    }
  }

  /**
   * Delete the block file and meta file from the disk asynchronously, adjust
   * dfsUsed statistics accordingly.
   */
  void deleteAsync(FsVolumeImpl volume, File blockFile, File metaFile,
      ExtendedBlock block) {
    LOG.info("Scheduling " + block.getLocalBlock() + " file " + blockFile +
        " for deletion");
    ReplicaFileDeleteTask deletionTask =
        new ReplicaFileDeleteTask(volume, blockFile, metaFile, block);
    execute(volume.getCurrentDir(), deletionTask);
  }
  
  /**
   * A task for deleting a block file and its associated meta file, as well
   * as decrement the dfs usage of the volume.
   */
  class ReplicaFileDeleteTask implements Runnable {
    final FsVolumeImpl volume;
    final File blockFile;
    final File metaFile;
    final ExtendedBlock block;
    
    ReplicaFileDeleteTask(FsVolumeImpl volume, File blockFile, File metaFile,
        ExtendedBlock block) {
      this.volume = volume;
      this.blockFile = blockFile;
      this.metaFile = metaFile;
      this.block = block;
    }

    @Override
    public String toString() {
      // Called in AsyncDiskService.execute for displaying error messages.
      return "deletion of block " + block.getBlockPoolId() + " " +
          block.getLocalBlock() + " with block file " + blockFile +
          " and meta file " + metaFile + " from volume " + volume;
    }

    @Override
    public void run() {
      long dfsBytes = blockFile.length() + metaFile.length();
      if (!blockFile.delete() || (!metaFile.delete() && metaFile.exists())) {
        LOG.warn("Unexpected error trying to delete block " +
            block.getBlockPoolId() + " " + block.getLocalBlock() + " at file " +
            blockFile + ". Ignored.");
      } else {
        if (block.getLocalBlock().getNumBytes() != BlockCommand.NO_ACK) {
          datanode.notifyNamenodeDeletedBlock(block, volume.getStorageID());
        }
        volume.decDfsUsed(block.getBlockPoolId(), dfsBytes);
        LOG.info(
            "Deleted " + block.getBlockPoolId() + " " + block.getLocalBlock() +
                " file " + blockFile);
      }
    }
  }
}
