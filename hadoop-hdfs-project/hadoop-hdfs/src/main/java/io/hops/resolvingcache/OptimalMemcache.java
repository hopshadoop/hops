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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class OptimalMemcache extends PathMemcache{
  @Override
  protected void setInternal(MemcachedClient mc, String path,
      List<INode> inodes) {
    if(INode.getPathNames(path).length  != inodes.size())
      return;

    int lastIndex = path.lastIndexOf(Path.SEPARATOR);
    if(lastIndex <= 0)
      return;

    INode file = inodes.get(inodes.size() - 1);
    if(file.isDirectory()){
      super.setInternal(mc, path, inodes);
      return;
    }

    String parentPath = path.substring(0, lastIndex);
    super.setInternal(mc, parentPath, inodes.subList(0, inodes.size() - 1));
    setInternal(mc, file);
  }

  @Override
  protected long[] getInternal(MemcachedClient mc, String path)
      throws IOException {
    int lastIndex = path.lastIndexOf(Path.SEPARATOR);
    if(lastIndex <= 0)
      return null;

    String parentPath = path.substring(0, lastIndex);
    long[] inodeIds = super.getInternal(mc, parentPath);
    if(inodeIds == null)
      return null;

    String file = path.substring(lastIndex + 1, path.length());
    long fileParentId = inodeIds[inodeIds.length - 1];
    Long fileInodeId = INodeMemcache.getInternal(mc, keyPrefix, file,
        fileParentId);
    if(fileInodeId != null){
      inodeIds = Arrays.copyOf(inodeIds, inodeIds.length + 1);
      inodeIds[inodeIds.length - 1] = fileInodeId;
    }
    return inodeIds;
  }

  @Override
  protected void setInternal(MemcachedClient mc, INode inode) {
    INodeMemcache.setInternal(mc, keyPrefix, keyExpiry, inode);
  }

  @Override
  protected void deleteInternal(MemcachedClient mc, String path) {
    int lastIndex = path.lastIndexOf(Path.SEPARATOR);
    if(lastIndex == -1)
      return;
    String parentPath = path.substring(0, lastIndex);
    super.deleteInternal(mc, parentPath);
  }

  @Override
  protected void deleteInternal(MemcachedClient mc, INode inode) {
    INodeMemcache.deleteInternal(mc, keyPrefix, inode);
  }

  @Override
  protected int getRoundTrips(String path) {
    return 2;
  }

  @Override
  protected int getRoundTrips(List<INode> inodes) {
    return 2;
  }
}
