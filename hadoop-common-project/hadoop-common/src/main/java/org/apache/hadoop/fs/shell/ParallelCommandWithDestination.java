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

    if(getNumThreads() == 1){
      super.processPaths(parent, items);
    } else {
      parallelCopy(parent,items);
    }
  }

  List<SrcDstPair> fileToDst = new ArrayList<>();
  List<PathData> sucessfull = new ArrayList<>();
  boolean filesSubmitted = false;
  private void parallelCopy(PathData parent, PathData ... items){

    List<PathData> curdirs = new ArrayList<>();
    try {
      for (PathData item : items) {  // create all dirs at this level in parallel
        if (isPathRecursable(item)) {
          ProcessPathThread processFile = new ProcessPathThread(item, dst/*dst dir*/);
          Future f = getThreadProol(getConf()).submit(processFile);
          activeCopiers.add(f);
          curdirs.add(item);
        } else { // collect files
          fileToDst.add(new SrcDstPair(item, dst));

        }
      }

      //wait for dirs to be created
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

      for(PathData dir : curdirs){
        if (recursive && isPathRecursable(dir)) {
          recursePath(dir);
        }
      }

      if(parent==null){  //only top level recursive method will create the files
        filesSubmitted = true;
        //all dirs have been created and files collected
        //create files in parallel

        long time = System.currentTimeMillis();
        Collections.shuffle(fileToDst);
        for(SrcDstPair srcDstPair : fileToDst){
          ProcessPathThread processFile = new ProcessPathThread(srcDstPair.src, srcDstPair.dst);
          Future f = getThreadProol(getConf()).submit(processFile);
          activeCopiers.add(f);
        }

        //wait for files to be created
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

        //post processing
        //Right now I do not forsee any probelm with random order of postprocessing
        for(PathData path : sucessfull){
          postProcessPath(path);
        }
      }

    } catch (IOException e) {
      displayError(e);
    }
  }

  ThreadLocal<FileSystem> fileSystems = new ThreadLocal<>();
  private class ProcessPathThread implements Callable {
    PathData src;
    PathData destDir;
    ProcessPathThread(PathData src, PathData destDir){
      this.src = src;
      this.destDir = destDir;
    }

    @Override
    public Object call() throws Exception {
      //for remote destination
      PathData dest = getTargetPath(src, destDir);

      if( isDstRemote() ){
        FileSystem dfs = fileSystems.get();
        if( dfs == null ){
          dfs = FileSystem.newInstance(dest.getURI(),getConf());
          fileSystems.set(dfs);
        }
        dest.overrideFS(dfs);
      }
      
      processPath(src,dest);
      return src;
    }
  }

  private class SrcDstPair {
    PathData src;
    PathData dst;

    public SrcDstPair(PathData src, PathData dst) {
      this.src = src;
      this.dst = dst;
    }
  }

  protected ExecutorService getThreadProol(Configuration conf){
    if(copier == null){
      copier = Executors.newFixedThreadPool(getNumThreads());
    }
    return copier;
  }

  protected PathData getTargetPath(PathData src, PathData dest) throws IOException {
    PathData target;
    // on the first loop, the dst may be directory or a file, so only create
    // a child path if dst is a dir; after recursion, it's always a dir
    if ((getDepth() > 0) || (dest.exists && dest.stat.isDirectory())) {
      target = dest.getPathDataForChild(src);
    } else if (dest.representsDirectory()) { // see if path looks like a dir
      target = dest.getPathDataForChild(src);
    } else {
      target = dest;
    }
    return target;
  }
}
