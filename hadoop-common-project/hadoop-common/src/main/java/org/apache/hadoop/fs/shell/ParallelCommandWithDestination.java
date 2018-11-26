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

package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

abstract class ParallelCommandWithDestination extends  CommandWithDestination{

  private ExecutorService copier ;
  private ConcurrentLinkedQueue<Future> activeCopiers = new ConcurrentLinkedQueue<>();

  /**
   *  Iterates over the given expanded paths and invokes
   *  {@link #processPath(PathData)} on each element.  If "recursive" is true,
   *  will do a post-visit DFS on directories.
   *  @param parent if called via a recurse, will be the parent dir, else null
   *  @param items a list of {@link PathData} objects to process
   *  @throws IOException if anything goes wrong...
   */
  protected void processPaths(PathData parent, PathData ... items)
          throws IOException {

    //go through all files first and copy them
    for (PathData item : items) {
      ProcessPathThread processFile = new ProcessPathThread(item);
      Future f = getThreadProol(getConf()).submit(processFile);
      activeCopiers.add(f);
    }

    //wait for all files to be copied
    List<PathData> sucessfull = new ArrayList<>();
    while(true) {
      Future future = activeCopiers.poll();
      if (future == null) {
        break;
      }
      try {
        PathData processed  = (PathData)future.get();
        sucessfull.add(processed);
      }catch (Exception e) {
        if(e instanceof ExecutionException){
          e = (Exception)e.getCause();
        }
        displayError(e);
      }
    }

    //go through all directories now
    for (PathData item : items) {
      try {
        if (recursive && isPathRecursable(item)) {
          recursePath(item);
        }
        if(sucessfull.contains(item)) {
          postProcessPath(item);
        }
      } catch (IOException e) {
        displayError(e);
      }
    }
  }

  private class ProcessPathThread implements Callable {
    PathData item;
    ProcessPathThread(PathData item){
      this.item = item;
    }

    @Override
    public Object call() throws Exception {
      processPath(item);
      return item;
    }
  }

//    @Override
//    protected void processPath(PathData src) throws IOException {
//      super.processPath(src);
//    }

  protected ExecutorService getThreadProol(Configuration conf){
    if(copier == null){
      int numThreads = 1;
      if(conf != null){
        numThreads =  conf.getInt(CommonConfigurationKeys.DFS_CLIENT_COPY_TO_OR_FROM_LOCAL_PARALLEL_THREADS,
                CommonConfigurationKeys.DFS_CLIENT_COPY_TO_OR_FROM_LOCAL_PARALLEL_THREADS_DEFAULT );
      }
      copier = Executors.newFixedThreadPool(numThreads);
    }
    return copier;
  }
}
