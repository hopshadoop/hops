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
package io.hops.common;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.NoSuchElementException;

public class INodeResolver {
  private final byte[][] components;
  private final boolean resolveLink;
  private final boolean transactional;
  private INode currentInode;
  private int count = 0;
  private int depth = INodeDirectory.ROOT_DIR_DEPTH;

  public INodeResolver(byte[][] components, INode baseINode,
      boolean resolveLink, boolean transactional) {
    this.components = components;
    currentInode = baseINode;
    this.resolveLink = resolveLink;
    this.transactional = transactional;
  }

  public INodeResolver(byte[][] components, INode baseINode,
      boolean resolveLink, boolean transactional, int initialCount) {
    this(components, baseINode, resolveLink, transactional);
    this.count = initialCount;
    this.depth = INodeDirectory.ROOT_DIR_DEPTH + (initialCount);
  }

  public boolean hasNext() {
    if (currentInode == null) {
      return false;
    }
    if (currentInode.isFile()) {
      return false;
    }
    return count + 1 < components.length;
  }

  public INode next() throws UnresolvedPathException, StorageException,
      TransactionContextException {
    boolean lastComp = (count == components.length - 1);
    if (currentInode.isSymlink() && (!lastComp || resolveLink)) {
      final String symPath =
          INodeUtil.constructPath(components, 0, components.length);
      final String preceding = INodeUtil.constructPath(components, 0, count);
      final String remainder =
          INodeUtil.constructPath(components, count + 1, components.length);
      final String link = DFSUtil.bytes2String(components[count]);
      final String target = ((INodeSymlink) currentInode).getSymlinkString();
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "UnresolvedPathException " + " path: " + symPath + " preceding: " +
                preceding + " count: " + count + " link: " + link +
                " target: " + target + " remainder: " + remainder);
      }
      throw new UnresolvedPathException(symPath, preceding, remainder, target);
    }

    if (!hasNext()) {
      throw new NoSuchElementException(
          "Trying to read more components than available");
    }

    depth++;
    count++;
    long partitionId = INode.calculatePartitionId(currentInode.getId(), DFSUtil.bytes2String(components[count]), (short) depth);

    currentInode = INodeUtil
        .getNode(components[count], currentInode.getId(), partitionId, transactional);
    return currentInode;
  }

  public int getCount() {
    return count;
  }
}
