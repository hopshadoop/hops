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
package io.hops.resolvingcache;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PathMemcache extends Memcache {

  public static class CacheEntry implements Serializable {

    private long[] inodeIds;

    public CacheEntry(long[] inodeIds) {
      this.inodeIds = inodeIds;
    }

    public long[] getInodeIds() {
      return inodeIds;
    }

    @Override
    public String toString() {
      return "CacheEntry{" + "inodeIds=" + Arrays.toString(inodeIds) + '}';
    }
  }

  @Override
  protected void setInternal(final MemcachedClient mc, final String path,
      final List<INode> inodes) {
    if (INode.getPathNames(path).length != inodes.size()) {
      return;
    }
    final String key = getKey(path);
    final long[] inodeIds = getINodeIds(inodes);
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

  @Override
  protected void setInternal(MemcachedClient mc, INode inode) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected long[] getInternal(final MemcachedClient mc, String path) throws
      IOException {
    Object ce = null;
    Future<Object> f = mc.asyncGet(getKey(path));
    try {
      ce = f.get(1, TimeUnit.SECONDS);
    } catch (Exception ex) {
      LOG.error(ex);
      f.cancel(true);
    }
    if (ce != null && ce instanceof CacheEntry) {
      return ((CacheEntry) ce).getInodeIds();
    }
    return null;
  }

  @Override
  protected void deleteInternal(final MemcachedClient mc, final String path) {
    final String key = getKey(path);
    mc.delete(key).addListener(new OperationCompletionListener() {
      @Override
      public void onComplete(OperationFuture<?> f) throws Exception {
        LOG.debug("DELETE for path (" + path + ")  " + key);
      }
    });
  }

  @Override
  protected void deleteInternal(MemcachedClient mc, INode inode) {
    throw new UnsupportedOperationException();
  }

  private String getKey(String path) {
    return keyPrefix + DigestUtils.sha256Hex(path);
  }

  private long[] getINodeIds(List<INode> inodes) {
    long[] inodeIds = new long[inodes.size()];
    for (int i = 0; i < inodes.size(); i++) {
      inodeIds[i] = inodes.get(i).getId();
    }
    return inodeIds;
  }

  @Override
  protected int getRoundTrips(String path) {
    return 1;
  }

  @Override
  protected int getRoundTrips(List<INode> inodes) {
    return 1;
  }
}
