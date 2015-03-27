/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.memcache;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PathMemcache {
  
  private static final Log LOG = LogFactory.getLog(PathMemcache.class);
  private static PathMemcache instance = null;
  private MemcachedClientPool mcpool;
  private boolean isEnabled;
  private int keyExpiry;
  private String keyPrefix;

  private boolean isStarted;
  private int numberOfConnections;
  private String server;
  
  private PathMemcache() {
  }

  public static PathMemcache getInstance() {
    if (instance == null) {
      instance = new PathMemcache();
    }
    return instance;
  }

  public void setConfiguration(Configuration conf) throws IOException {
    numberOfConnections =
        conf.getInt(DFSConfigKeys.DFS_MEMCACHE_CONNECTION_POOL_SIZE,
            DFSConfigKeys.DFS_MEMCACHE_CONNECTION_POOL_SIZE_DEFAULT);
    server = conf.get(DFSConfigKeys.DFS_MEMCACHE_SERVER,
        DFSConfigKeys.DFS_MEMCACHE_SERVER_DEFAULT);
    keyExpiry = conf.getInt(DFSConfigKeys.DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS,
        DFSConfigKeys.DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS_DEFAULT);
    keyPrefix = conf.get(DFSConfigKeys.DFS_MEMCACHE_KEY_PREFIX,
        DFSConfigKeys.DFS_MEMCACHE_KEY_PREFIX_DEFAULT);
    isEnabled = conf.getBoolean(DFSConfigKeys.DFS_MEMCACHE_ENABLED,
        DFSConfigKeys.DFS_MEMCACHE_ENABLED_DEFAULT);
    if (isEnabled) {
      start();
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

  private void start() throws IOException {
    if (!isStarted) {
      LOG.info("starting PathMemcached");
      mcpool = new MemcachedClientPool(numberOfConnections, server);
      isStarted = true;
    }
  }

  private void stop() {
    if (isStarted) {
      LOG.info("stoping PathMemcached");
      mcpool.shutdown();
      isStarted = false;
    }
  }

  public void set(final String path, final INode[] inodes) {
    set(path, Arrays.asList(inodes));
  }

  public void set(final String path, final List<INode> inodes) {
    if (isStarted) {
      if (INode.getPathNames(path).length != inodes.size()) {
        return;
      }
      MemcachedClient mc = mcpool.poll();
      if (mc == null) {
        return;
      }
      final String key = getKey(path);
      final int[] inodeIds = getINodeIds(inodes);
      final long startTime = System.currentTimeMillis();
      mc.set(key, keyExpiry, new CacheEntry(inodeIds))
          .addListener(new OperationCompletionListener() {
            @Override
            public void onComplete(OperationFuture<?> f) throws Exception {
              long elapsed = System.currentTimeMillis() - startTime;
              LOG.debug("SET for path (" + path + ")  " + key + "=" +
                  Arrays.toString(inodeIds) + " in " + elapsed + " msec");
            }
          });
    }
  }

  public int[] get(String path) throws IOException {
    if (isStarted) {
      MemcachedClient mc = mcpool.poll();
      if (mc == null) {
        return null;
      }
      final long startTime = System.currentTimeMillis();
      Object ce = null;
      try {
        ce = mc.get(getKey(path));
      } catch (Exception ex) {
        LOG.error(ex);
      }
      if (ce != null && ce instanceof CacheEntry) {
        LOG.debug("GET for path (" + path + ")  got value = " + ce + " in " +
            (System.currentTimeMillis() - startTime) + " msec");
        return ((CacheEntry) ce).getInodeIds();
      }
    }
    return null;
  }

  public void delete(final String path) {
    if (isStarted) {
      MemcachedClient mc = mcpool.poll();
      if (mc == null) {
        return;
      }
      final String key = getKey(path);
      mc.delete(key).addListener(new OperationCompletionListener() {
        @Override
        public void onComplete(OperationFuture<?> f) throws Exception {
          LOG.debug("DELETE for path (" + path + ")  " + key);
        }
      });
    }
  }

  public void flush() {
    if (isStarted) {
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
  }

  private String getKey(String path) {
    return keyPrefix + DigestUtils.sha256Hex(path);
  }

  private int[] getINodeIds(List<INode> inodes) {
    int[] inodeIds = new int[inodes.size()];
    for (int i = 0; i < inodes.size(); i++) {
      inodeIds[i] = inodes.get(i).getId();
    }
    return inodeIds;
  }
}
