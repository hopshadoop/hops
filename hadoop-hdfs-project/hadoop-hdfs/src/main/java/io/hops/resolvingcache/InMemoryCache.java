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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class InMemoryCache extends Cache{

  private ConcurrentLinkedHashMap<String, Long> cache;
  private int CACHE_MAXIMUM_SIZE;

  @Override
  protected void setConfiguration(Configuration conf) throws IOException {
    CACHE_MAXIMUM_SIZE = conf.getInt(DFSConfigKeys.DFS_INMEMORY_CACHE_MAX_SIZE,
        DFSConfigKeys.DFS_INMEMORY_CACHE_MAX_SIZE_DEFAULT);
    super.setConfiguration(conf);
  }

  @Override
  protected void startInternal() throws IOException {
    cache = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (CACHE_MAXIMUM_SIZE).build();
  }

  @Override
  protected void stopInternal() {
  }

  @Override
  protected void setInternal(String path, List<INode> inodes) {
    for(INode iNode : inodes){
      if(iNode != null) {
        cache.put(iNode.nameParentKey(), iNode.getId());
      }
    }
  }

  @Override
  protected long[] getInternal(String path) throws IOException {
    String[] pathComponents = INode.getPathNames(path);
    long[] inodeIds = new long[pathComponents.length];
    long parentId = INodeDirectory.ROOT_PARENT_ID;
    int index = 0;
    while(index <pathComponents.length){
      String cmp = pathComponents[index];
      Long inodeId = cache.get(INode.nameParentKey(parentId, cmp));
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
  protected void setInternal(INode inode) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deleteInternal(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deleteInternal(INode inode) {
    cache.remove(inode.nameParentKey());
  }

  @Override
  protected void flushInternal() {
    cache.clear();
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
