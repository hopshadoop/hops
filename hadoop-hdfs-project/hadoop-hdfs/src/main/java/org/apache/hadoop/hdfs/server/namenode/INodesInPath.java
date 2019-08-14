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


import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

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
    tmp = inode;
    index = depth;
    while (tmp != null) {
      index--;
      path[index] = tmp.getLocalNameBytes();
      inodes[index] = tmp;
      tmp = tmp.getParent();
    }
    return new INodesInPath(inodes, path);
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

  /**
   * Retrieve existing INodes from a path. For non-snapshot path,
   * the number of INodes is equal to the number of path components. For
   * snapshot path (e.g., /foo/.snapshot/s1/bar), the number of INodes is
   * (number_of_path_components - 1).
   * <p>
   * An UnresolvedPathException is always thrown when an intermediate path
   * component refers to a symbolic link. If the final path component refers
   * to a symbolic link then an UnresolvedPathException is only thrown if
   * resolveLink is true.
   * <p>
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"]
   * <p>
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"])</code> should fill
   * the array with [rootINode,c1,c2], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"])</code> should
   * fill the array with [rootINode,c1,c2,null]
   *
   * @param startingDir the starting directory
   * @param components array of path component name
   * @param resolveLink indicates whether UnresolvedLinkException should
   * be thrown when the path refers to a symbolic link.
   * @return the specified number of existing INodes in the path
   */
  // TODO: Eliminate null elements from inodes (to be provided by HDFS-7104)
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components, final boolean resolveLink) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    int count = 0;
    int inodeNum = 0;
    INode[] inodes = new INode[components.length];
    
    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);
      inodes[inodeNum++] = curNode;
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
    }
    return new INodesInPath(inodes, components);
  }

  /**
   * Replace an inode of the given INodesInPath in the given position. We do a
   * deep copy of the INode array.
   * @param pos the position of the replacement
   * @param inode the new inode
   * @return a new INodesInPath instance
   */
  public static INodesInPath replace(INodesInPath iip, int pos, INode inode) {
    Preconditions.checkArgument(iip.length() > 0 && pos > 0 // no for root
        && pos < iip.length());
    if (iip.getINode(pos) == null) {
      Preconditions.checkState(iip.getINode(pos - 1) != null);
    }
    INode[] inodes = new INode[iip.inodes.length];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length);
    inodes[pos] = inode;
    return new INodesInPath(inodes, iip.path);
  }

    /**
   * Extend a given INodesInPath with a child INode. The child INode will be
   * appended to the end of the new INodesInPath.
   */
  public static INodesInPath append(INodesInPath iip, INode child,
      byte[] childName) {
    Preconditions.checkArgument(iip.length() > 0);
    Preconditions.checkArgument(iip.getLastINode() != null && iip
        .getLastINode().isDirectory());
    INode[] inodes = new INode[iip.length() + 1];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length - 1);
    inodes[inodes.length - 1] = child;
    byte[][] path = new byte[iip.path.length + 1][];
    System.arraycopy(iip.path, 0, path, 0, path.length - 1);
    path[path.length - 1] = childName;
    return new INodesInPath(inodes, path);
  }
  
  private final byte[][] path;
  /**
   * Array with the specified number of INodes resolved for a given path.
   */
  private final INode[] inodes;

  private INodesInPath(INode[] inodes, byte[][] path) {
    Preconditions.checkArgument(inodes != null && path != null);
    this.inodes = inodes;
    this.path = path;
  }
  
  /**
   * @return the inodes array excluding the null elements.
   */
  public INode getINode(int i) {
    if (inodes == null || inodes.length == 0) {
      throw new NoSuchElementException("inodes is null or empty");
    }
    int index = i >= 0 ? i : inodes.length + i;
    if (index < inodes.length && index >= 0) {
      return inodes[index];
    } else {
      throw new NoSuchElementException("inodes.length == " + inodes.length);
    }
  }

  /**
   * @return the last inode.
   */
  public INode getLastINode() {
    return getINode(-1);
  }

  byte[] getLastLocalName() {
    return path[path.length - 1];
  }
  
  public byte[][] getPathComponents() {
    return path;
  }
  
  /** @return the full path in string form */
  public String getPath() {
    return DFSUtil.byteArray2PathString(path);
  }
  
  public String getParentPath() {
    return getPath(path.length - 1);
  }

  public String getPath(int pos) {
    return DFSUtil.byteArray2PathString(path, 0, pos);
  }

  /**
   * @param offset start endpoint (inclusive)
   * @param length number of path components
   * @return sub-list of the path
   */
  public List<String> getPath(int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length
        <= path.length);
    ImmutableList.Builder<String> components = ImmutableList.builder();
    for (int i = offset; i < offset + length; i++) {
      components.add(DFSUtil.bytes2String(path[i]));
    }
    return components.build();
  }

  public int length() {
    return inodes.length;
  }

  public List<INode> getReadOnlyINodes() {
    return Collections.unmodifiableList(Arrays.asList(inodes));
  }

   /**
   * @param length number of ancestral INodes in the returned INodesInPath
   *               instance
   * @return the INodesInPath instance containing ancestral INodes. Note that
   * this method only handles non-snapshot paths.
   */
  private INodesInPath getAncestorINodesInPath(int length) {
    Preconditions.checkArgument(length >= 0 && length < inodes.length);
    final INode[] anodes = new INode[length];
    final byte[][] apath = new byte[length][];
    System.arraycopy(this.inodes, 0, anodes, 0, length);
    System.arraycopy(this.path, 0, apath, 0, length);
    return new INodesInPath(anodes, apath);
  }

  /**
   * @return an INodesInPath instance containing all the INodes in the parent
   *         path. We do a deep copy here.
   */
  public INodesInPath getParentINodesInPath() {
    return inodes.length > 1 ? getAncestorINodesInPath(inodes.length - 1) :
        null;
  }

  /**
   * @return a new INodesInPath instance that only contains exisitng INodes.
   * Note that this method only handles non-snapshot paths.
   */
  public INodesInPath getExistingINodes() {
    int i = 0;
    for (; i < inodes.length; i++) {
      if (inodes[i] == null) {
        break;
      }
    }
    INode[] existing = new INode[i];
    byte[][] existingPath = new byte[i][];
    System.arraycopy(inodes, 0, existing, 0, i);
    System.arraycopy(path, 0, existingPath, 0, i);
    return new INodesInPath(existing, existingPath);
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
    
    return b.toString();
  }

  void validate() throws StorageException, TransactionContextException {
    int i = 0;
    if (inodes[i] != null) {
      for (i++; i < inodes.length && inodes[i] != null; i++) {
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
    if (i != inodes.length) {
      throw new AssertionError("i = " + i + " != " + inodes.length
          + ", this=" + toString(false));
    }
  }
}
