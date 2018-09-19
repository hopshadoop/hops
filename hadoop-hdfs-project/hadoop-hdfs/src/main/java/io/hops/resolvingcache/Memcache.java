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

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.List;

public abstract class Memcache extends Cache {

  private MemcachedClientPool mcpool;
  protected int keyExpiry;
  protected String keyPrefix;

  private int numberOfConnections;
  private String server;

  @Override
  protected void setConfiguration(Configuration conf) throws IOException {
    numberOfConnections =
        conf.getInt(DFSConfigKeys.DFS_MEMCACHE_CONNECTION_POOL_SIZE,
            DFSConfigKeys.DFS_MEMCACHE_CONNECTION_POOL_SIZE_DEFAULT);
    server = conf.get(DFSConfigKeys.DFS_MEMCACHE_SERVER,
        DFSConfigKeys.DFS_MEMCACHE_SERVER_DEFAULT);
    keyExpiry = conf.getInt(DFSConfigKeys.DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS,
        DFSConfigKeys.DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS_DEFAULT);
    keyPrefix = conf.get(DFSConfigKeys.DFS_MEMCACHE_KEY_PREFIX,
        DFSConfigKeys.DFS_MEMCACHE_KEY_PREFIX_DEFAULT);

    super.setConfiguration(conf);
  }


  @Override
  protected final void startInternal() throws IOException {
    mcpool = new MemcachedClientPool(numberOfConnections, server);
  }

  @Override
  protected final void stopInternal() {
    mcpool.shutdown();
  }

  @Override
  protected final void setInternal(final String path,
      final List<INode> inodes) {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return;
    }
    setInternal(mc, path, inodes);
  }

  @Override
  protected void setInternal(INode inode) {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return;
    }
    setInternal(mc, inode);
  }

  @Override
  protected final long[] getInternal(final String path) throws IOException {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return null;
    }
    return getInternal(mc, path);
  }

  @Override
  protected final void deleteInternal(final String path) {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return;
    }
    deleteInternal(mc, path);
  }

  @Override
  protected final void deleteInternal(final INode inode) {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return;
    }
    deleteInternal(mc, inode);
  }

  @Override
  protected final void flushInternal() {
    MemcachedClient mc = mcpool.poll();
    if (mc == null) {
      return;
    }
    mc.flush().addListener(new OperationCompletionListener() {

      @Override
      public void onComplete(OperationFuture<?> f) throws Exception {
        LOG.debug("Memcache flushed");
      }
    });
  }


  protected abstract void setInternal(final MemcachedClient mc, final String
      path, final List<INode> inodes);

  protected abstract void setInternal(final MemcachedClient mc, final INode inode);

  protected abstract long[] getInternal(final MemcachedClient mc, final String
      path) throws IOException;

  protected abstract void deleteInternal(final MemcachedClient mc, final
  String path);

  protected abstract void deleteInternal(final MemcachedClient mc, final
  INode inode);
}
