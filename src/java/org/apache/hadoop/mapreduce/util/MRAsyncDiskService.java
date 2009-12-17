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
package org.apache.hadoop.mapreduce.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.AsyncDiskService;
import org.apache.hadoop.util.StringUtils;

/*
 * This class is a container of multiple thread pools, each for a volume,
 * so that we can schedule async disk operations easily.
 * 
 * Examples of async disk operations are deletion of files.
 * We can move the files to a "TO_BE_DELETED" folder before asychronously
 * deleting it, to make sure the caller can run it faster.
 * 
 * This class also contains all operations that will be performed by the
 * thread pools. 
 */
public class MRAsyncDiskService {
  
  public static final Log LOG = LogFactory.getLog(MRAsyncDiskService.class);
  
  AsyncDiskService asyncDiskService;
  
  /**
   * Create a AsyncDiskServices with a set of volumes (specified by their
   * root directories).
   * 
   * The AsyncDiskServices uses one ThreadPool per volume to do the async
   * disk operations.
   * 
   * @param localFileSystem The localFileSystem used for deletions.
   * @param volumes The roots of the file system volumes.
   */
  public MRAsyncDiskService(FileSystem localFileSystem, String[] volumes) throws IOException {
    
    asyncDiskService = new AsyncDiskService(volumes);
    
    this.localFileSystem = localFileSystem;
    this.volumes = volumes;
    
    // Create one ThreadPool per volume
    for (int v = 0 ; v < volumes.length; v++) {
      // Create the root for file deletion
      if (!localFileSystem.mkdirs(new Path(volumes[v], SUBDIR))) {
        throw new IOException("Cannot create " + SUBDIR + " in " + volumes[v]);
      }
    }
    
  }
  
  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  synchronized void execute(String root, Runnable task) {
    asyncDiskService.execute(root, task);
  }
  
  /**
   * Gracefully start the shut down of all ThreadPools.
   */
  synchronized void shutdown() {
    asyncDiskService.shutdown();
  }

  /**
   * Shut down all ThreadPools immediately.
   */
  public synchronized List<Runnable> shutdownNow() {
    return asyncDiskService.shutdownNow();
  }
  
  /**
   * Wait for the termination of the thread pools.
   * 
   * @param milliseconds  The number of milliseconds to wait
   * @return   true if all thread pools are terminated without time limit
   * @throws InterruptedException 
   */
  public synchronized boolean awaitTermination(long milliseconds) 
      throws InterruptedException {
    return asyncDiskService.awaitTermination(milliseconds);
  }
  
  public static final String SUBDIR = "toBeDeleted";
  
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
  
  private FileSystem localFileSystem;
  
  private String[] volumes; 
                 
  private int uniqueId = 0;
  
  /** A task for deleting a pathName from a volume.
   */
  class DeleteTask implements Runnable {

    /** The volume that the file is on*/
    String volume;
    /** The file name before the move */
    String originalPath;
    /** The file name after the move */
    String pathToBeDeleted;
    
    /**
     * Delete a file/directory (recursively if needed).
     * @param volume        The volume that the file/dir is in.
     * @param originalPath  The original name, relative to volume root.
     * @param pathToBeDeleted  The name after the move, relative to volume root,
     *                         containing SUBDIR.
     */
    DeleteTask(String volume, String originalPath, String pathToBeDeleted) {
      this.volume = volume;
      this.originalPath = originalPath;
      this.pathToBeDeleted = pathToBeDeleted;
    }
    
    @Override
    public String toString() {
      // Called in AsyncDiskService.execute for displaying error messages.
      return "deletion of " + pathToBeDeleted + " on " + volume
          + " with original name " + originalPath;
    }

    @Override
    public void run() {
      boolean success = false;
      Exception e = null;
      try {
        Path absolutePathToBeDeleted = new Path(volume, pathToBeDeleted);
        success = localFileSystem.delete(absolutePathToBeDeleted, true);
      } catch (Exception ex) {
        e = ex;
      }
      
      if (!success) {
        if (e != null) {
          LOG.warn("Failure in " + this + " with exception " + StringUtils.stringifyException(e));
        } else {
          LOG.warn("Failure in " + this);
        }
      } else {
        LOG.debug("Successfully did " + this.toString());
      }
    }
  };
  
  
  /**
   * Move the path name on one volume to a temporary location and then 
   * delete them.
   * 
   * This functions returns when the moves are done, but not necessarily all
   * deletions are done. This is usually good enough because applications 
   * won't see the path name under the old name anyway after the move. 
   * 
   * @param volume       The disk volume
   * @param pathName     The path name relative to volume.
   * @throws IOException If the move failed 
   */
  public boolean moveAndDelete(String volume, String pathName) throws IOException {
    // Move the file right now, so that it can be deleted later
    String newPathName;
    synchronized (this) {
      newPathName = format.format(new Date()) + "_" + uniqueId;
      uniqueId ++;
    }
    newPathName = SUBDIR + Path.SEPARATOR_CHAR + newPathName;
    
    Path source = new Path(volume, pathName);
    Path target = new Path(volume, newPathName); 
    try {
      if (!localFileSystem.rename(source, target)) {
        return false;
      }
    } catch (FileNotFoundException e) {
      // Return false in case that the file is not found.  
      return false;
    }

    DeleteTask task = new DeleteTask(volume, pathName, newPathName);
    execute(volume, task);
    return true;
  }

  /**
   * Move the path name on each volume to a temporary location and then 
   * delete them.
   * 
   * This functions returns when the moves are done, but not necessarily all
   * deletions are done. This is usually good enough because applications 
   * won't see the path name under the old name anyway after the move. 
   * 
   * @param pathName     The path name on each volume.
   * @throws IOException If the move failed 
   */
  public void moveAndDeleteFromEachVolume(String pathName) throws IOException {
    for (int i = 0; i < volumes.length; i++) {
      moveAndDelete(volumes[i], pathName);
    }
  }
  
}
