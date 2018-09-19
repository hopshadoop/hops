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
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class INodeMemcache extends Memcache {
  
  @Override
  protected void setInternal(MemcachedClient mc, String path, List<INode>
      inodes) {
    for(INode inode : inodes){
      setInternal(mc, inode);
    }
  }

  @Override
  protected long[] getInternal(MemcachedClient mc, String path) throws
      IOException {
    String[] pathComponents = INode.getPathNames(path);
    long[] inodeIds = new long[pathComponents.length];
    long parentId = INodeDirectory.ROOT_PARENT_ID;
    int index = 0;
    while(index <pathComponents.length){
      String cmp = pathComponents[index];
      Long inodeId = getInternal(mc, cmp, parentId);
      if(inodeId != null){
        parentId = inodeId;
        inodeIds[index] = inodeId;
      }else{
        break;
      }
      index++;
    }

    //only the root was found
    if(index <= 1)
      return null;

    return Arrays.copyOf(inodeIds, index);
  }

  @Override
  protected void setInternal(MemcachedClient mc, INode inode) {
    setInternal(mc, keyPrefix, keyExpiry, inode);
  }

  @Override
  protected void deleteInternal(MemcachedClient mc, String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deleteInternal(MemcachedClient mc, INode inode) {
    deleteInternal(mc, keyPrefix, inode);
  }

  private String getKey(INode inode) {
    return getKey(keyPrefix, inode);
  }

  private Long getInternal(MemcachedClient mc, String name, Long
      parentId){
    return getInternal(mc, keyPrefix, name, parentId);
  }

  private static String getKey(String KEY_PREFIX, INode inode) {
    return KEY_PREFIX + inode.nameParentKey();
  }

  private static String getKey(String KEY_PREFIX, String name, Long
      parentId){
    return KEY_PREFIX + INode.nameParentKey(parentId, name);
  }

  static Long getInternal(MemcachedClient mc, String KEY_PREFIX, String
      name, Long parentId){
    Long inodeId = null;
    Future<Object> f = mc.asyncGet(getKey(KEY_PREFIX, name, parentId));
    try{
      Object res = f.get(1, TimeUnit.SECONDS);
      if(res instanceof Long){
        inodeId = (Long)res;
      }
    }catch(Exception ex){
      LOG.error(ex);
      f.cancel(true);
    }
    return inodeId;
  }

  static void setInternal(MemcachedClient mc, String KEY_PREFIX, int
      KEY_EXPIRY, INode inode){
    mc.set(getKey(KEY_PREFIX, inode), KEY_EXPIRY, inode.getId());
  }

  static void deleteInternal(MemcachedClient mc, String KEY_PREFIX, INode
      inode) {
    mc.delete(getKey(KEY_PREFIX, inode));
  }

  @Override
  protected int getRoundTrips(String path) {
    return INode.getPathNames(path).length;
  }

  @Override
  protected int getRoundTrips(List<INode> inodes) {
    return inodes.size();
  }
}
