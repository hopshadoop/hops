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
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.AccessTimeLogDataAccess;
import io.hops.metadata.hdfs.entity.AccessTimeLogEntry;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
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

    ByINodeIdFTIS,//FTIS full table index scan
    ByParentIdFTIS,
    ByParentIdAndPartitionId,
    ByNameParentIdAndPartitionId,
    ByNamesParentIdsAndPartitionIdsCheckLocal,
    ByNamesParentIdsAndPartitionIds;

    @Override
    public Class getType() {
      return INode.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByINodeIdFTIS:
          return Annotation.IndexScan;
        case ByParentIdFTIS:
          return Annotation.IndexScan;
        case ByParentIdAndPartitionId:
          return Annotation.PrunedIndexScan;
        case ByNameParentIdAndPartitionId:
          return Annotation.PrimaryKey;
        case ByNamesParentIdsAndPartitionIds:
          return Annotation.Batched;
        case ByNamesParentIdsAndPartitionIdsCheckLocal:CheckLocal:
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
  

  public static final int NON_EXISTING_ID = 0;
  protected int id = NON_EXISTING_ID;
  protected int parentId = NON_EXISTING_ID;
  public static int RANDOM_PARTITIONING_MAX_LEVEL=1;
  protected int partitionId;

  protected boolean subtreeLocked;
  protected long subtreeLockOwner;


  //Number of bits for Block size
  final static short BLOCK_BITS = 48;
  final static short REPLICATION_BITS = 8;
  final static short BOOLEAN_BITS = 8;
  final static short HAS_BLKS_BITS = 1; // this is out of the 8 bits for the storing booleans
  //Header mask 64-bit representation
  //Format:[8 bits for flags][8 bits for replication degree][48 bits for PreferredBlockSize]
  final static long BLOCK_SIZE_MASK =  0x0000FFFFFFFFFFFFL;
  final static long REPLICATION_MASK = 0x00FF000000000000L;
  final static long FLAGS_MASK =       0xFF00000000000000L;
  final static long HAS_BLKS_MASK =    0x0100000000000000L;
  //[8 bits for flags]
  //0 bit : 1 if the file has blocks. 0 blocks
  //remaining bits are not yet used
  long header;

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

  private String userName;
  private String groupName;

  private int userId;
  private int groupId;

  private FsPermission permission;

  INode(PermissionStatus permissions, long mTime, long atime)
      throws IOException {
    this.setLocalNameNoPersistance((byte[]) null);
    this.parent = null;
    this.modificationTime = mTime;
    setAccessTimeNoPersistance(atime);
    setPermissionStatusNoPersistance(permissions);
  }

  protected INode(String name, PermissionStatus permissions)
      throws IOException {
    this(permissions, 0L, 0L);
    setLocalNameNoPersistance(name);
  }
  
  /**
   * copy constructor
   *
   * @param other
   *     Other node to be copied
   */
  INode(INode other) throws IOException {
    setLocalNameNoPersistance(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatusNoPersistance(other.getPermissionStatus());
    setModificationTimeNoPersistance(other.getModificationTime());
    setAccessTimeNoPersistance(other.getAccessTime());

    this.parentId = other.getParentId();
    this.id = other.getId();
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
  private void setPermissionStatusNoPersistance(PermissionStatus ps)
      throws IOException {
    setUserNoPersistance(ps.getUserName());
    setGroupNoPersistance(ps.getGroupName());
    setPermissionNoPersistance(ps.getPermission());
  }
  
  /**
   * Get the {@link PermissionStatus}
   */
  public PermissionStatus getPermissionStatus() throws IOException {
    return new PermissionStatus(getUserName(), getGroupName(),
        getFsPermission());
  }

  /**
   * Get user name
   */
  public String getUserName() throws IOException {
    if(userName == null || userName.isEmpty()){
      userName = UsersGroups.getUser(userId);
    }
    return userName;
  }

  public int getUserID(){
    return userId;
  }

  public void setUserIDNoPersistance(int userId){
    this.userId = userId;
  }
  /**
   * Set user
   */
  public void setUserNoPersistance(String user) throws IOException {
    this.userName = user;
    this.userId = UsersGroups.getUserID(user);
  }

  /**
   * Get group name
   */
  public String getGroupName() throws IOException {
    if(groupName == null || groupName.isEmpty()){
      groupName = UsersGroups.getGroup(groupId);
    }
    return groupName;
  }

  public int getGroupID(){
    return groupId;
  }

  public void setGroupIDNoPersistance(int groupId){
    this.groupId = groupId;
  }

  /**
   * Set group
   */
  public void setGroupNoPersistance(String group) throws IOException {
    this.groupName = group;
    this.groupId = UsersGroups.getGroupID(group);
  }

  /**
   * Get the {@link FsPermission}
   */
  public FsPermission getFsPermission() {
    return permission;
  }

  protected short getFsPermissionShort() {
    return permission.toShort();
  }

  /**
   * Set the {@link FsPermission} of this {@link INode}
   */
  private void setPermissionNoPersistance(FsPermission permission) {
    this.permission = permission;
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
    } catch (IOException ex) {
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
          .find(INode.Finder.ByINodeIdFTIS, getParentId());
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

  public String nameParentKey() {
    return nameParentKey(parentId, getLocalName());
  }
  
  /**
   * Set user
   */
  protected void setUser(String user)
      throws IOException {
    setUserNoPersistance(user);
    save();
  }

  protected void setGroup(String group)
      throws IOException {
    setGroupNoPersistance(group);
    save();
  }

  void setPermission(FsPermission permission)
      throws StorageException, TransactionContextException {
    setPermissionNoPersistance(permission);
    save();
  }

  protected void setPermissionStatus(PermissionStatus ps)
      throws IOException {
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
      throws TransactionContextException, StorageException {
    setAccessTimeNoPersistance(atime);
    if (isPathMetaEnabled()) {
      AccessTimeLogDataAccess da = (AccessTimeLogDataAccess)
          HdfsStorageFactory.getDataAccess(AccessTimeLogDataAccess.class);
      int userId = -1; // TODO get userId
      da.add(new AccessTimeLogEntry(getId(), userId, atime));
    }
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

  public void logMetadataEvent(MetadataLogEntry.Operation operation)
      throws StorageException, TransactionContextException {
    if(isUnderConstruction()){
      return;
    }
    if (isPathMetaEnabled()) {
      INodeDirectory datasetDir = getMetaEnabledParent();
      EntityManager.add(new MetadataLogEntry(datasetDir.getId(), getId(),
          getParentId(), getLocalName(), operation));
    }
  }

  boolean isPathMetaEnabled() throws TransactionContextException, StorageException {
    return getMetaEnabledParent() != null ? true : false;
  }

  INodeDirectory getMetaEnabledParent()
      throws TransactionContextException, StorageException {
    INodeDirectory dir = getParent();
    while (!isRoot() && !dir.isRoot()) {
      if (dir.isMetaEnabled()) {
        return dir;
      }
      dir = dir.getParent();
    }
    return null;
  }

  public int getPartitionId(){
    return  partitionId;
  }

  public void setPartitionIdNoPersistance(int partitionId){
    this.partitionId = partitionId;
  }

  public void setPartitionId(int partitionId) throws TransactionContextException, StorageException {
    setPartitionIdNoPersistance(partitionId);
    save();
  }

  public void calculateAndSetPartitionIdNoPersistance(int parentId, String name, short depth){
    setPartitionIdNoPersistance(calculatePartitionId(parentId,name,depth));
  }

  public void calculateAndSetPartitionId(int parentId, String name, short depth)
      throws TransactionContextException, StorageException {
    setPartitionIdNoPersistance(calculatePartitionId(parentId,name,depth));
    save();
  }
  public static int calculatePartitionId(int parentId, String name, short depth){
    if(isTreeLevelRandomPartitioned(depth)){
      return partitionIdHashFunction(parentId,name,depth);
    }else{
      return parentId;
    }
  }

  private static int partitionIdHashFunction(int parentId, String name, short depth){
    if(depth == INodeDirectory.ROOT_DIR_DEPTH){
      return INodeDirectory.ROOT_DIR_PARTITION_KEY;
    }else{
      return (name+parentId).hashCode();
      //    String partitionid = String.format("%04d%04d",parentId,depth);
      //    return Integer.parseInt(partitionid);
    }
  }

  public static boolean isTreeLevelRandomPartitioned(short depth){
    if(depth > RANDOM_PARTITIONING_MAX_LEVEL){
      return false;
    }else{
      return true;
    }
  }

  public static short getBlockReplication(long header) {
    long val = (header & REPLICATION_MASK);
    long val2 = val >> BLOCK_BITS;
    return (short) val2;
  }

  void setReplicationNoPersistance(short replication) {
    if (replication <= 0 || replication > (Math.pow(2, REPLICATION_BITS) - 1)) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "replication [" + replication + "]. Expected [1:" + (Math.pow(2, REPLICATION_BITS) - 1) + "]");
    }
    header = ((long) replication << BLOCK_BITS) | (header & ~REPLICATION_MASK);
  }

  public static long getPreferredBlockSize(long header) {
    return header & BLOCK_SIZE_MASK;
  }

  protected void setPreferredBlockSizeNoPersistance(long preferredBlkSize) {
    if ((preferredBlkSize < 0) || (preferredBlkSize > (Math.pow(2, BLOCK_BITS) - 1))) {
      throw new IllegalArgumentException("Unexpected value for the block " +
          "size [" + preferredBlkSize + "]. Expected [1:" + (Math.pow(2, BLOCK_BITS) - 1) + "]");
    }
    header = (header & ~BLOCK_SIZE_MASK) | (preferredBlkSize & BLOCK_SIZE_MASK);
  }

  public long getHeader() {
    return header;
  }

  public void setHeaderNoPersistance(long header) {
    long preferecBlkSize = getPreferredBlockSize(header);
    short replication = getBlockReplication(header);
    if (preferecBlkSize < 0) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "block size [" + preferecBlkSize + "]");
    }

    if (replication < 0) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "replication [" + replication + "]");
    }

    this.header = header;
  }

  public void setHasBlocks(boolean hasBlocks) throws TransactionContextException, StorageException {
    setHasBlocksNoPersistance(hasBlocks);
    save();
  }

  public void setHasBlocksNoPersistance(boolean hasBlocks) {
    long val = (hasBlocks) ? 1 : 0;
    header = ((long) val << (BLOCK_BITS + REPLICATION_BITS)) | (header & ~HAS_BLKS_MASK);
  }

  public static boolean hasBlocks(long header) {
    long val = (header & HAS_BLKS_MASK);
    long val2 = val >> (BLOCK_BITS + REPLICATION_BITS);
    if (val2 == 1) {
      return true;
    } else if (val2 == 0) {
      return false;
    } else {
      throw new IllegalStateException("Flags in the inode header are messed up");
    }
  }

  public boolean hasBlocks(){
   return hasBlocks(header);
  }

  public short myDepth() throws TransactionContextException, StorageException {
    if(id == NON_EXISTING_ID){
      throw new IllegalStateException("INode is not connected to the file system tree yet");
    }

    if(id == INodeDirectory.ROOT_ID){
      return INodeDirectory.ROOT_DIR_DEPTH;
    }

    INode parentInode = EntityManager.find(Finder.ByINodeIdFTIS, getParentId());
    return (short) (parentInode.myDepth()+1);
  }
}
