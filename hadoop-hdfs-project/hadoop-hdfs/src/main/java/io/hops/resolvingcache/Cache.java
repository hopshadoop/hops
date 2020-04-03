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
package io.hops.resolvingcache;

import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static io.hops.transaction.context.TransactionsStats.ResolvingCacheStat;

public abstract class Cache {

  private static Cache instance = null;

  protected static final Log LOG = LogFactory.getLog(Cache.class);

  private boolean isStarted;
  private boolean isEnabled;

  protected Cache() {
  }

  public static Cache getInstance(Configuration conf) throws IOException {
    if (instance == null) {
      String  memType = conf.get(DFSConfigKeys.DFS_RESOLVING_CACHE_TYPE,
          DFSConfigKeys.DFS_RESOLVING_CACHE_TYPE_DEFAULT).toLowerCase();
      if(memType.equals("inmemory")){
        instance = new InMemoryCache();
      }else {
        throw new IllegalArgumentException("Cache has only two " +
            "Memcache implementations, Inode based and Path based: wrong " +
            "parameter " +
            memType);
      }
      instance.setConfiguration(conf);
    }
    return instance;
  }

  public static Cache getInstance() {
    if (instance == null) {
      throw new IllegalStateException("Memcache should have started first " +
          "with configuration");
    }
    return instance;
  }

  protected void setConfiguration(Configuration conf) throws IOException {
    isEnabled = conf.getBoolean(DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED,
        DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED_DEFAULT);

    if (isEnabled) {
      start();
    }
  }


  private void start() throws IOException {
    if (!isStarted) {
      LOG.info("starting Resolving Cache [" + instance.getClass()
          .getSimpleName() +"]");
      startInternal();
      isStarted = true;
    }
  }

  private void stop() {
    if (isStarted) {
      LOG.info("stopping Resolving Cache [" + instance.getClass()
          .getSimpleName() +"]");      stopInternal();
      isStarted = false;
    }
  }

  public void enableOrDisable(boolean forceEnable) throws IOException {
    if (forceEnable) {
      start();
    } else {
      stop();
    }
    isEnabled = forceEnable;
  }

  public final void set(final String path, final INode[] inodes){
    set(path, Arrays.asList(inodes));
  }

  public final void set(final String path, final List<INode> inodes){
    if(isStarted){
     setInternal(path, inodes);
      if(TransactionsStats.getInstance().isEnabled()){
        TransactionsStats.getInstance().pushResolvingCacheStats(
            new ResolvingCacheStat(
                ResolvingCacheStat.Op.SET, 0,
                getRoundTrips(inodes)));
      }
    }
  }

  public final void set(final INode inode){
    if(isStarted){
      setInternal(inode);
      if(TransactionsStats.getInstance().isEnabled()){
        TransactionsStats.getInstance().pushResolvingCacheStats(
            new ResolvingCacheStat(
                ResolvingCacheStat.Op.SET, 0,1));
      }
    }
  }

  public final long[] get(final String path) throws IOException{
    if(isStarted){
      final long startTime = System.currentTimeMillis();
      long[] result = getInternal(path);
      final long elapsed =  (System.currentTimeMillis() - startTime);
      LOG.trace("GET for path (" + path + ")  got value = " + Arrays.toString
          (result) + " in " + elapsed + " " +
          "msec");
      if(TransactionsStats.getInstance().isEnabled()){
        TransactionsStats.getInstance().pushResolvingCacheStats(
            new ResolvingCacheStat(
                ResolvingCacheStat.Op.GET, elapsed,
                getRoundTrips(path)));
      }
      return result;
    }
    return null;
  }

  public final INodeIdentifier get(final long inodeId) throws IOException {
    if (isStarted) {
      final long startTime = System.currentTimeMillis();
      INodeIdentifier result = getInternal(inodeId);
      final long elapsed = (System.currentTimeMillis() - startTime);
      LOG.trace("GET for inodeId (" + inodeId + ")  got value = " + result + " in " + elapsed + " " + "msec");
      if (TransactionsStats.getInstance().isEnabled()) {
        TransactionsStats.getInstance().pushResolvingCacheStats(
            new ResolvingCacheStat(ResolvingCacheStat.Op.GET, elapsed, 1));
      }
      return result;
    }
    return null;
  }

  public final void delete(final String path){
    if(isStarted){
      deleteInternal(path);
    }
  }

  public final void delete(final INode inode){
    if(isStarted){
      deleteInternal(inode);
    }
  }

  public final void delete(final INodeIdentifier inode) {
    if (isStarted) {
      deleteInternal(inode);
    }
  }

  public final void flush(){
    if(isStarted){
     flushInternal();
    }
  }


  protected abstract void startInternal() throws IOException;
  protected abstract void stopInternal();

  protected abstract void setInternal(final String path, final List<INode>
      inodes);

  protected abstract void setInternal(final INode inode);
  
  protected abstract long[] getInternal(final String path) throws IOException;
  protected abstract INodeIdentifier getInternal(final long inodeId) throws IOException;
  protected abstract void deleteInternal(final String path);
  protected abstract void deleteInternal(final INode inode);
  protected abstract void deleteInternal(final INodeIdentifier inode);
  protected abstract void flushInternal();

  protected abstract int getRoundTrips(String path);
  protected abstract int getRoundTrips(List<INode> inodes);
}
