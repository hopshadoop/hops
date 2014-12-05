/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;

/**
 * Contains INodes information resolved from a given path.
 */
public class INodesInPath {

  public static final Log LOG = LogFactory.getLog(INodesInPath.class);

   static INodesInPath fromINode(INode inode) throws StorageException, TransactionContextException {
    int depth = 0, index;
    INode tmp = inode;
    while (tmp != null) {
      depth++;
      tmp = tmp.getParent();
    }
    final byte[][] path = new byte[depth][];
    final INode[] inodes = new INode[depth];
    final INodesInPath iip = new INodesInPath(path, depth);
    tmp = inode;
    index = depth;
    while (tmp != null) {
      index--;
      path[index] = tmp.getLocalNameBytes();
      inodes[index] = tmp;
      tmp = tmp.getParent();
    }
    iip.setINodes(inodes);
    return iip;
  }
    
  /**
   * Given some components, create a path name.
   *
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  private static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components) throws UnresolvedLinkException, StorageException, TransactionContextException {
    return resolve(startingDir, components, components.length, false);
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the INode of the final component.
   * <p>
   * An UnresolvedPathException is always thrown when an intermediate path
   * component refers to a symbolic link. If the final path component refers
   * to a symbolic link then an UnresolvedPathException is only thrown if
   * resolveLink is true.
   * <p>
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * <p>
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * <p>
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * <p>
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootINode,c1,c2,null], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootINode,c1,c2,null]
   *
   * @param startingDir the starting directory
   * @param components array of path component name
   * @param numOfINodes number of INodes to return
   * @param resolveLink indicates whether UnresolvedLinkException should
   * be thrown when the path refers to a symbolic link.
   * @return the specified number of existing INodes in the path
   */
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components, final int numOfINodes,
      final boolean resolveLink) throws UnresolvedLinkException, StorageException, TransactionContextException {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    final INodesInPath existing = new INodesInPath(components, numOfINodes);
    int count = 0;
    int index = numOfINodes - components.length;
    if (index > 0) {
      index = 0;
    }
    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);
      if (index >= 0) {
        existing.addNode(curNode);
      }
      final boolean isDir = curNode.isDirectory();
      final INodeDirectory dir = isDir ? curNode.asDirectory() : null;
      if (curNode.isSymlink() && (!lastComp || resolveLink)) {
        final String path = constructPath(components, 0, components.length);
        final String preceding = constructPath(components, 0, count);
        final String remainder = constructPath(components, count + 1, components.length);
        final String link = DFSUtil.bytes2String(components[count]);
        final String target = curNode.asSymlink().getSymlinkString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("UnresolvedPathException " + " path: " + path + " preceding: " + preceding + " count: " + count
              + " link: " + link + " target: " + target + " remainder: " + remainder);
        }
        throw new UnresolvedPathException(path, preceding, remainder, target);
      }
      if (lastComp || !isDir) {
        break;
      }
      final byte[] childName = components[count + 1];

      // normal case, and also for resolving file/dir under snapshot root
      curNode = dir.getChildINode(childName);
      count++;
      index++;
    }
    return existing;
  }

  private final byte[][] path;
  /**
   * Array with the specified number of INodes resolved for a given path.
   */
  private INode[] inodes;
  /**
   * Indicate the number of non-null elements in {@link #inodes}
   */
  private int numNonNull;
  /**
   * The path for a snapshot file/dir contains the .snapshot thus makes the
   * length of the path components larger the number of inodes. We use
   * the capacity to control this special case.
   */
  private int capacity;

  private INodesInPath(byte[][] path, int number) {
    this.path = path;
    assert (number >= 0);
    inodes = new INode[number];
    capacity = number;
    numNonNull = 0;
  }

  /**
   * @return a new array of inodes excluding the null elements introduced by
   * snapshot path elements. E.g., after resolving path "/dir/.snapshot",
   * {@link #inodes} is {/, dir, null}, while the returned array only contains
   * inodes of "/" and "dir". Note the length of the returned array is always
   * equal to {@link #capacity}.
   */
  INode[] getINodes() {
    if (capacity == inodes.length) {
      return inodes;
    }

    INode[] newNodes = new INode[capacity];
    System.arraycopy(inodes, 0, newNodes, 0, capacity);
    return newNodes;
  }

  /**
   * @return the i-th inode if i >= 0;
   * otherwise, i < 0, return the (length + i)-th inode.
   */
  public INode getINode(int i) {
    return inodes[i >= 0 ? i : inodes.length + i];
  }

  /**
   * @return the last inode.
   */
  public INode getLastINode() {
    return inodes[inodes.length - 1];
  }

  byte[] getLastLocalName() {
    return path[path.length - 1];
  }
  
  /** @return the full path in string form */
  public String getPath() {
    return DFSUtil.byteArray2PathString(path);
  }
  
  /**
   * Add an INode at the end of the array
   */
  private void addNode(INode node) {
    inodes[numNonNull++] = node;
  }

  private void setINodes(INode inodes[]) {
    this.inodes = inodes;
    this.numNonNull = this.inodes.length;
  }

  void setINode(int i, INode inode) {
    inodes[i >= 0 ? i : inodes.length + i] = inode;
  }

  void setLastINode(INode last) {
    inodes[inodes.length - 1] = last;
  }

  /**
   * @return The number of non-null elements
   */
  int getNumNonNull() {
    return numNonNull;
  }

  private static String toString(INode inode) {
    return inode == null ? null : inode.getLocalName();
  }

  @Override
  public String toString() {
    try {
      return toString(true);
    } catch (StorageException | TransactionContextException ex) {
      LOG.error(ex,ex);
      return null;
    }
  }

  private String toString(boolean vaildateObject) throws StorageException, TransactionContextException {
    if (vaildateObject) {
      validate();
    }

    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append(": path = ").append(DFSUtil.byteArray2PathString(path))
        .append("\n  inodes = ");
    if (inodes == null) {
      b.append("null");
    } else if (inodes.length == 0) {
      b.append("[]");
    } else {
      b.append("[").append(toString(inodes[0]));
      for (int i = 1; i < inodes.length; i++) {
        b.append(", ").append(toString(inodes[i]));
      }
      b.append("], length=").append(inodes.length);
    }
    b.append("\n  numNonNull = ").append(numNonNull)
        .append("\n  capacity   = ").append(capacity);
    return b.toString();
  }

  void validate() throws StorageException, TransactionContextException {
    // check parent up to snapshotRootIndex or numNonNull
    final int n = numNonNull;
    int i = 0;
    if (inodes[i] != null) {
      for (i++; i < n && inodes[i] != null; i++) {
        final INodeDirectory parent_i = inodes[i].getParent();
        final INodeDirectory parent_i_1 = inodes[i - 1].getParent();
        if (parent_i != inodes[i - 1] && (parent_i_1 == null || parent_i != parent_i_1)) {
          throw new AssertionError(
              "inodes[" + i + "].getParent() != inodes[" + (i - 1)
              + "]\n  inodes[" + i + "]=" + inodes[i].toDetailString()
              + "\n  inodes[" + (i - 1) + "]=" + inodes[i - 1].toDetailString()
              + "\n this=" + toString(false));
        }
      }
    }
    if (i != n) {
      throw new AssertionError("i = " + i + " != " + n
          + ", this=" + toString(false));
    }
  }
}
