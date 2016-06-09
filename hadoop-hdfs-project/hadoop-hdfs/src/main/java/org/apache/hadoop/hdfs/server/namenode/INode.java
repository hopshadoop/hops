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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.primitives.SignedBytes;
import io.hops.erasure_coding.ErasureCodingManager;
import io.hops.exception.HopsException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.transaction.EntityManager;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements Comparable<byte[]> {
  
  static final List<INode> EMPTY_LIST =
      Collections.unmodifiableList(new ArrayList<INode>());


  public static enum Finder implements FinderType<INode> {

    ByINodeId,
    ByParentId,
    ByNameAndParentId,
    ByNamesAndParentIds;

    @Override
    public Class getType() {
      return INode.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByINodeId:
          return Annotation.IndexScan;
        case ByParentId:
          return Annotation.PrunedIndexScan;
        case ByNameAndParentId:
          return Annotation.PrimaryKey;
        case ByNamesAndParentIds:
          return Annotation.Batched;
        default:
          throw new IllegalStateException();
      }
    }

  }

  public static enum Order implements Comparator<INode> {

    ByName() {
      @Override
      public int compare(INode o1, INode o2) {
        // TODO - JIM why not compare by ID - more efficient?
        return o1.compareTo(o2.getLocalNameBytes());
      }
    };

    @Override
    public abstract int compare(INode o1, INode o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }


  /**
   * The inode name is in java UTF8 encoding;
   * The name in HdfsFileStatus should keep the same encoding as this.
   * if this encoding is changed, implicitly getFileInfo and listStatus in
   * clientProtocol are changed; The decoding at the client
   * side should change accordingly.
   */
  protected byte[] name;
  protected INodeDirectory parent;
  protected long modificationTime;
  protected long accessTime;
  
//START_HOP_CODE
  //START_ROOT_LEVEL_SNAPSHOT
  protected int status=SnapShotConstants.Original;
  protected int isDeleted;
  //END_ROOT_LEVRL_SNAPSHOT
  public static final int NON_EXISTING_ID = -10;
  protected int id = NON_EXISTING_ID;
  protected int parentId = NON_EXISTING_ID;

  protected boolean subtreeLocked;
  protected long subtreeLockOwner;


  /**
   * Simple wrapper for two counters :
   * nsCount (namespace consumed) and dsCount (diskspace consumed).
   */
  static class DirCounts {
    long nsCount = 0;
    long dsCount = 0;
    
    /**
     * returns namespace count
     */
    long getNsCount() {
      return nsCount;
    }

    /**
     * returns diskspace count
     */
    long getDsCount() {
      return dsCount;
    }
  }
  
  //Only updated by updatePermissionStatus(...).
  //Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  INode(PermissionStatus permissions, long mTime, long atime) {
    this.setLocalNameNoPersistance((byte[]) null);
    this.parent = null;
    this.modificationTime = mTime;
    setAccessTimeNoPersistance(atime);
    setPermissionStatusNoPersistance(permissions);
  }

  protected INode(String name, PermissionStatus permissions) {
    this(permissions, 0L, 0L);
    setLocalNameNoPersistance(name);
  }
  
  /**
   * copy constructor
   *
   * @param other
   *     Other node to be copied
   */
  INode(INode other) throws StorageException, TransactionContextException {
    setLocalNameNoPersistance(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatusNoPersistance(other.getPermissionStatus());
    setModificationTimeNoPersistance(other.getModificationTime());
    setAccessTimeNoPersistance(other.getAccessTime());

    this.parentId = other.getParentId();
    this.id = other.getId();
    
    //START ROOT_LEVEL_SNAPSHOT
    this.status = other.status;
    this.isDeleted = other.isDeleted;
   //END ROOT_LEVEL_SNAPSHOT
  
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return name.length == 0;
  }

  /**
   * Set the {@link PermissionStatus}
   */
  private void setPermissionStatusNoPersistance(PermissionStatus ps) {
    setUserNoPersistance(ps.getUserName());
    setGroupNoPersistance(ps.getGroupName());
    setPermissionNoPersistance(ps.getPermission());
  }
  
  /**
   * Get the {@link PermissionStatus}
   */
  public PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(), getGroupName(),
        getFsPermission());
  }

  private void updatePermissionStatus(PermissionStatusFormat f, long n) {
    permission = f.combine(n, permission);
  }

  /**
   * Get user name
   */
  public String getUserName() {
    int n = (int) PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }

  /**
   * Set user
   */
  private void setUserNoPersistance(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }

  /**
   * Get group name
   */
  public String getGroupName() {
    int n = (int) PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }

  /**
   * Set group
   */
  private void setGroupNoPersistance(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }

  /**
   * Get the {@link FsPermission}
   */
  public FsPermission getFsPermission() {
    return new FsPermission(
        (short) PermissionStatusFormat.MODE.retrieve(permission));
  }

  protected short getFsPermissionShort() {
    return (short) PermissionStatusFormat.MODE.retrieve(permission);
  }

  /**
   * Set the {@link FsPermission} of this {@link INode}
   */
  private void setPermissionNoPersistance(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE, permission.toShort());
  }

  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return false;
  }

  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this INode is deleted.
   */
  abstract int collectSubtreeBlocksAndClear(List<Block> v)
      throws StorageException, TransactionContextException;

  /**
   * Compute {@link ContentSummary}.
   */
  public final ContentSummary computeContentSummary()
      throws StorageException, TransactionContextException {
    long[] a = computeContentSummary(new long[]{0, 0, 0, 0});
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), a[3],
        getDsQuota());
  }

  /**
   * @return an array of three longs.
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary)
      throws StorageException, TransactionContextException;
  
  /**
   * Get the quota set for this inode
   *
   * @return the quota if it is set; -1 otherwise
   */
  public long getNsQuota()
      throws StorageException, TransactionContextException {
    return -1;
  }

  public long getDsQuota()
      throws StorageException, TransactionContextException {
    return -1;
  }
  
  boolean isQuotaSet() throws StorageException, TransactionContextException {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total number of names and total disk space taken under
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts)
      throws StorageException, TransactionContextException;
  
  /**
   * Get local file name
   *
   * @return local file name
   */
  public String getLocalName() {
    return DFSUtil.bytes2String(name);
  }


  String getLocalParentDir()
      throws StorageException, TransactionContextException {
    INode inode = isRoot() ? this : getParent();
    String parentDir = "";
    if (inode != null) {
      parentDir = inode.getFullPathName();
    }
    return (parentDir != null) ? parentDir : "";
  }

  /**
   * Get local file name
   *
   * @return local file name
   */
  byte[] getLocalNameBytes() {
    return name;
  }

  
  /**
   * Set local file name
   */
  public void setLocalNameNoPersistance(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }

  /**
   * Set local file name
   */
  public void setLocalNameNoPersistance(byte[] name) {
    this.name = name;
  }
  
  public String getFullPathName()
      throws StorageException, TransactionContextException {
    // Get the full path name of this inode.
    return FSDirectory.getFullPathName(this);
  }

  @Override
  public String toString() {
    try {
      return "\"" + getFullPathName() + "\":" + getUserName() + ":" +
          getGroupName() + ":" + (isDirectory() ? "d" : "-") +
          getFsPermission();
    } catch (HopsException ex) {
      Logger.getLogger(INode.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  /**
   * Get parent directory
   *
   * @return parent INode
   */
  INodeDirectory getParent()
      throws StorageException, TransactionContextException {

    if (isRoot()) {
      return null;
    }
    if (parent == null) {
      parent = (INodeDirectory) EntityManager
          .find(INode.Finder.ByINodeId, getParentId());
    }

    return this.parent;
  }

  /**
   * Get last modification time of inode.
   *
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  public void setModificationTimeNoPersistance(long modtime) {
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  protected void setModificationTimeForceNoPersistance(long modtime) {
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   *
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  public void setAccessTimeNoPersistance(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isSymlink() {
    return false;
  }

  /**
   * Breaks file path into components.
   *
   * @param path
   * @return array of byte arrays each of which represents
   * a single path component.
   */
  public static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /**
   * Convert strings to byte arrays for path components.
   */
  public static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    }
    return bytes;
  }

  /**
   * Splits an absolute path into an array of path components.
   *
   * @param path
   * @return array of path components.
   * @throws AssertionError
   *     if the given path is invalid.
   */
  public static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Given some components, create a path name.
   *
   * @param components
   *     The path components
   * @param start
   *     index
   * @param end
   *     index
   * @return concatenated path
   */
  static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }


  boolean removeNode() throws StorageException, TransactionContextException {
    if (parent == null) {
      return false;
    } else {
      parent.removeChild(this);
      parent = null;
      return true;
    }
  }

  private static final byte[] EMPTY_BYTES = {};

  @Override
  public final int compareTo(byte[] bytes) {
    final byte[] left = name == null ? EMPTY_BYTES : name;
    final byte[] right = bytes == null ? EMPTY_BYTES : bytes;
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof INode)) {
      return false;
    }
    if (Arrays.equals(this.name, ((INode) that).name) &&
        this.id == ((INode) that).id &&
        this.parentId == ((INode) that).parentId) {
      return true;
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return Arrays.hashCode(this.name);
  }

  public final void setIdNoPersistance(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }
  
  public void setParent(INodeDirectory p)
      throws StorageException, TransactionContextException {
    setParentNoPersistance(p);
    save();
  }

  public void setParentNoPersistance(INodeDirectory p) {
    this.parent = p;
    this.parentId = p.getId();
  }
  
  public void setParentIdNoPersistance(int pid) {
    this.parentId = pid;
  }

  public int getParentId() {
    return this.parentId;
  }

  public static String nameParentKey(Integer parentId, String name) {
    return parentId + name;
  }

  public int getStatus(){
      return status;
  }

  public void setStatusNoPersistance(int status){
      this.status = status;
  }
  
  public int getIsDeleted(){
      return isDeleted;
  }
  
  public void setIsDeletedNoPersistance(int isDeleted){
      this.isDeleted= isDeleted;
  }
  
  public String nameParentKey() {
    return nameParentKey(parentId, getLocalName());
  }
  
  /**
   * Set user
   */
  protected void setUser(String user)
      throws StorageException, TransactionContextException {
    setUserNoPersistance(user);
    save();
  }

  protected void setGroup(String group)
      throws StorageException, TransactionContextException {
    setGroupNoPersistance(group);
    save();
  }

  void setPermission(FsPermission permission)
      throws StorageException, TransactionContextException {
    setPermissionNoPersistance(permission);
    save();
  }

  protected void setPermissionStatus(PermissionStatus ps)
      throws StorageException, TransactionContextException {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }

  public void setLocalName(String name)
      throws StorageException, TransactionContextException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  public void setLocalName(byte[] name)
      throws StorageException, TransactionContextException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  public void setModificationTime(long modtime)
      throws StorageException, TransactionContextException {
    setModificationTimeNoPersistance(modtime);
    save();
  }

  public void setAccessTime(long atime)
      throws StorageException, TransactionContextException {
    setAccessTimeNoPersistance(atime);
    save();
  }

  void setModificationTimeForce(long modtime)
      throws StorageException, TransactionContextException {
    setModificationTimeForceNoPersistance(modtime);
    save();
  }
  
  public boolean exists() {
    if (id == NON_EXISTING_ID) {
      return false;
    }
    
    return true;
  }

  protected void save() throws StorageException, TransactionContextException {
    save(this);
  }

  protected void save(INode node)
      throws StorageException, TransactionContextException {
    EntityManager.update(node);
  }

  protected void remove() throws StorageException, TransactionContextException {
    remove(this);
  }

  protected void remove(INode node)
      throws StorageException, TransactionContextException {
    EntityManager.remove(node);
    //if This inode is of type INodeDirectoryWithQuota then also delete the INode Attribute table
    if (node instanceof INodeDirectoryWithQuota) {
      ((INodeDirectoryWithQuota) node).removeAttributes();
    }
    cleanParity(node);
  }

  private void cleanParity(INode node)
      throws StorageException, TransactionContextException {
    if (ErasureCodingManager.isEnabled()) {
      EncodingStatus status =
          EntityManager.find(EncodingStatus.Finder.ByInodeId, node.getId());
      if (status != null) {
        status.setStatus(EncodingStatus.Status.DELETED);
        EntityManager.update(status);
        return;
      }
    }
  }

  public boolean isSubtreeLocked() {
    return subtreeLocked;
  }

  public void setSubtreeLocked(boolean subtreeLocked) {
    this.subtreeLocked = subtreeLocked;
  }

  public long getSubtreeLockOwner() {
    return subtreeLockOwner;
  }

  public void setSubtreeLockOwner(long subtreeLockOwner) {
    this.subtreeLockOwner = subtreeLockOwner;
  }

  public void lockSubtree(long subtreeLockOwner) {
    setSubtreeLocked(true);
    setSubtreeLockOwner(subtreeLockOwner);
  }

  public void unlockSubtree() {
    setSubtreeLocked(false);
  }

  public boolean isFile() {
    return !isDirectory() && !isSymlink();
  }

  long getPermission() {
    return permission;
  }
}
