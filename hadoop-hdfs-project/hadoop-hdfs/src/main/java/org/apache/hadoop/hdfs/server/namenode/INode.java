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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.SignedBytes;
import io.hops.erasure_coding.ErasureCodingManager;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import java.io.FileNotFoundException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements Comparable<byte[]>, LinkedElement {
  
  public static final List<INode> EMPTY_LIST =
      Collections.unmodifiableList(new ArrayList<INode>());
  public enum Finder implements FinderType<INode> {

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

  public enum Order implements Comparator<INode> {

    ByName() {
      @Override
      public int compare(INode o1, INode o2) {
        // TODO - JIM why not compare by ID - more efficient?
        return o1.compareTo(o2.getLocalNameBytes());
      }
    };

    @Override
    public abstract int compare(INode o1, INode o2);
  }


  protected byte blockStoragePolicyID;
  

  protected boolean inTree = false;
  protected int parentId = 0;
  public static int RANDOM_PARTITIONING_MAX_LEVEL=1;

  protected boolean subtreeLocked;
  protected long subtreeLockOwner;

  public final static int ROOT_PARENT_ID = 0;
  public final static int ROOT_INODE_ID = 1;
  
  /**
   * To check if the request id is the same as saved id. Don't check fileId
   * with GRANDFATHER_INODE_ID for backward compatibility.
   */
  public static void checkId(long requestId, INode inode)
      throws FileNotFoundException {
    if (requestId != ROOT_PARENT_ID && requestId != inode.getId()) {
      throw new FileNotFoundException(
          "ID mismatch. Request id and saved id: " + requestId + " , "
          + inode.getId());
    }
  }
  
  
  public static class HeaderFormat {

    /**
     * Number of bits for Block size
     */
    static final int BLOCKBITS = 48;
    //Number of bits for Block size
    final static short REPLICATION_BITS = 8;
    final static short BOOLEAN_BITS = 8;
    final static short HAS_BLKS_BITS = 1; // this is out of the 8 bits for the storing booleans
    //Header mask 64-bit representation
    //Format:[8 bits for flags][8 bits for replication degree][48 bits for PreferredBlockSize]
    final static long MAX_BLOCK_SIZE = 0x0000FFFFFFFFFFFFL;
    final static long REPLICATION_MASK = 0x00FF000000000000L;
    final static long FLAGS_MASK = 0xFF00000000000000L;
    final static long HAS_BLKS_MASK = 0x0100000000000000L;
    //[8 bits for flags]
    //0 bit : 1 if the file has blocks. 0 blocks
    //remaining bits are not yet used

    static public short getReplication(long header) {
      return (short) ((header & REPLICATION_MASK) >> BLOCKBITS);
    }

    static long combineReplication(long header, short replication) {
      if (replication <= 0 || replication > (Math.pow(2, REPLICATION_BITS) - 1)) {
        throw new IllegalArgumentException("Unexpected value for the " + "replication [" + replication
            + "]. Expected [1:" + (Math.pow(2, REPLICATION_BITS) - 1) + "]");
      }
      return ((long) replication << BLOCKBITS) | (header & ~REPLICATION_MASK);
    }

    static public long getPreferredBlockSize(long header) {
      return header & MAX_BLOCK_SIZE;
    }

    static long combinePreferredBlockSize(long header, long blockSize) {
      if ((blockSize < 0) || (blockSize > (Math.pow(2, BLOCKBITS) - 1))) {
        throw new IllegalArgumentException("Unexpected value for the block " + "size [" + blockSize + "]. Expected [1:"
            + (Math.pow(2, BLOCKBITS) - 1) + "]");
      }
      return (header & ~MAX_BLOCK_SIZE) | (blockSize & MAX_BLOCK_SIZE);
    }

    static long combineHasBlocksNoPersistance(long header, boolean hasBlocks) {
      long val = (hasBlocks) ? 1 : 0;
      return ((long) val << (BLOCKBITS + REPLICATION_BITS)) | (header & ~HAS_BLKS_MASK);
    }

    static public boolean hasBlocks(long header) {
      long val = (header & HAS_BLKS_MASK);
      long val2 = val >> (BLOCKBITS + REPLICATION_BITS);
      if (val2 == 1) {
        return true;
      } else if (val2 == 0) {
        return false;
      } else {
        throw new IllegalStateException("Flags in the inode header are messed up");
      }
    }

  }
  
  /** Wrapper of two counters for namespace consumed and diskspace consumed. */
  static class DirCounts {
    /** namespace count */
    long nsCount = 0;
    /** diskspace count */
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

  private int numAces;
  /** parent is {@link INodeDirectory} */
  protected INode parent = null;
  protected LinkedElement next = null;
  
  INode(INode parent) {
    this.parent = parent;
  }

  /** Get inode id */
  public abstract int getId();

  public AclFeature getAclFeature() throws TransactionContextException, StorageException, AclException {
    return INodeAclHelper.getAclFeature(this);
  }

  public void addAclFeature(AclFeature aclFeature) throws TransactionContextException, StorageException, AclException {
    INodeAclHelper.addAclFeature(this, aclFeature);
  }
  
  public void removeAclFeature() throws TransactionContextException, StorageException {
    INodeAclHelper.removeAclFeature(this);
  }
  
  /**
   * Check whether this is the root inode.
   */
  final boolean isRoot() {
    return getLocalNameBytes().length == 0;
  }
  
  /**
   * Get the {@link PermissionStatus}
   */
  public abstract PermissionStatus getPermissionStatus() throws IOException;

  /**
   * Get user name
   */
  public abstract String getUserName() throws IOException;

  public abstract int getUserID();

  /**
   * Get group name
   */
  public abstract String getGroupName() throws IOException;

  public abstract int getGroupID();

  /**
   * Get the {@link FsPermission}
   */
  public abstract FsPermission getFsPermission();

  protected abstract short getFsPermissionShort();

  /**
   * Check whether it's a file.
   */
  public boolean isFile() {
    return false;
  }

  /**
   * Cast this inode to an {@link INodeFile}.
   */
  public INodeFile asFile() throws StorageException, TransactionContextException {
    throw new IllegalStateException("Current inode is not a file: "
        + this.toDetailString());
  }

  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return false;
  }

  /**
   * Cast this inode to an {@link INodeDirectory}.
   */
  public INodeDirectory asDirectory() throws StorageException, TransactionContextException {
    throw new IllegalStateException("Current inode is not a directory: "
        + this.toDetailString());
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isSymlink() {
    return false;
  }

  /**
   * Cast this inode to an {@link INodeSymlink}.
   */
  public INodeSymlink asSymlink() throws StorageException, TransactionContextException {
    throw new IllegalStateException("Current inode is not a symlink: "
        + this.toDetailString());
  }
  
  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this INode is deleted.
   */
  abstract int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo v)
      throws StorageException, TransactionContextException;

  /**
   * Compute {@link ContentSummary}.
   */
  public final ContentSummary computeContentSummary()
      throws StorageException, TransactionContextException {
    return computeAndConvertContentSummary(
        new ContentSummaryComputationContext());
  }
  
  /**
   * Compute {@link ContentSummary}.
   */
  public final ContentSummary computeAndConvertContentSummary(
      ContentSummaryComputationContext summary) throws StorageException, TransactionContextException {
    Content.Counts counts = computeContentSummary(summary).getCounts();
    final Quota.Counts q = getQuotaCounts();
    return new ContentSummary(counts.get(Content.LENGTH),
        counts.get(Content.FILE) + counts.get(Content.SYMLINK),
        counts.get(Content.DIRECTORY), q.get(Quota.NAMESPACE),
        counts.get(Content.DISKSPACE), q.get(Quota.DISKSPACE));
  }

  /**
   * @param summary the context object holding counts for the subtree.
   * @return The same objects as summary.
   */
  abstract ContentSummaryComputationContext computeContentSummary(ContentSummaryComputationContext summary)
      throws StorageException, TransactionContextException;
  
  public void addSpaceConsumed(long nsDelta, long dsDelta)
    throws StorageException, TransactionContextException {
    addSpaceConsumed2Parent(nsDelta, dsDelta);
  }
  
  void addSpaceConsumed2Parent(long nsDelta, long dsDelta)
    throws StorageException, TransactionContextException {
    if (parent != null) {
      parent.addSpaceConsumed(nsDelta, dsDelta);
    }
  }
  
  /**
   * Get the quota set for this inode
   *
   *  @return the quota counts.  The count is -1 if it is not set.
   */
  public Quota.Counts getQuotaCounts() throws StorageException, TransactionContextException{
    return Quota.Counts.newInstance(-1, -1);
  }
  
  boolean isQuotaSet() throws StorageException, TransactionContextException {
    final Quota.Counts q = getQuotaCounts();
    return q.get(Quota.NAMESPACE) >= 0 || q.get(Quota.DISKSPACE) >= 0;
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
   * @return null if the local name is null; otherwise, return the local name.
   */
  public String getLocalName() {
    final byte[] name = getLocalNameBytes();
    return name == null? null: DFSUtil.bytes2String(name);
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
   * @return null if the local name is null;
   *         otherwise, return the local name byte array.
   */
  abstract byte[] getLocalNameBytes();

  
  /**
   * Set local file name
   */
  public abstract void setLocalNameNoPersistance(String name);

  /**
   * Set local file name
   */
  public abstract void setLocalNameNoPersistance(byte[] name);
  
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

  @VisibleForTesting
  public final String getObjectString() {
    return getClass().getSimpleName() + "@"
        + Integer.toHexString(super.hashCode());
  }

  /**
   * @return a string description of the parent.
   */
  @VisibleForTesting
  public final String getParentString() throws StorageException, TransactionContextException {
    final INodeDirectory parentDir = getParent();
    if (parentDir != null) {
      return "parentDir=" + parentDir.getLocalName() + "/";
    } else {
      return "parent=null";
    }
  }

  @VisibleForTesting
  public String toDetailString() throws StorageException, TransactionContextException {
    final INodeDirectory p = getParent();
    return toStringWithObjectType()
        + ", parent=" + (p == null ? null : p.toStringWithObjectType());
  }

  @VisibleForTesting
  public String toStringWithObjectType() {
    return toString() + "(" + getObjectString() + ")";
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
      parent = (INode) EntityManager
          .find(INode.Finder.ByINodeIdFTIS, getParentId());
    }

    return this.parent.asDirectory();
  }

  /**
   * Get last modification time of inode.
   *
   * @return access time
   */
  public abstract long getModificationTime();

  /**
   * Set last modification time of inode.
   */
  public abstract void setModificationTimeNoPersistance(long modtime);

  /**
   * Get access time of inode.
   *
   * @return access time
   */
  public abstract long getAccessTime();

  /**
   * Set last access time of inode.
   */
  public abstract void setAccessTimeNoPersistance(long atime);

  /**
   * 1) If the file or directory is specificed with a storage policy, return it.
   * 2) For an unspecified file or directory, if it is the root directory,
   *    return the default storage policy. Otherwise, return its parent's
   *    effective storage policy.
   */
  public byte getStoragePolicyID() throws TransactionContextException, StorageException {
    byte id = getLocalStoragePolicyID();
    if (id == BlockStoragePolicySuite.ID_UNSPECIFIED) {
      return this.getParent() != null ? this.getParent().getStoragePolicyID() : id;
    }
    return id;
  }

  /**
   * @return the storage policy directly specified on the INode. Return
   * {@link BlockStoragePolicySuite#ID_UNSPECIFIED} if no policy has
   * been specified.
   */
  public byte getLocalStoragePolicyID() {
    return this.blockStoragePolicyID;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
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
      ((INodeDirectory) parent).removeChild(this);
      parent = null;
      return true;
    }
  }

  private static final byte[] EMPTY_BYTES = {};

  @Override
  public final int compareTo(byte[] bytes) {
    final byte[] name = getLocalNameBytes();
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
    
    return this.getId() == ((INode) that).getId();
  }
  
  @Override
  public final int hashCode() {
    final int id = this.getId();
    return (int)(id^(id>>>32));
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
  protected abstract void setUser(String user) throws IOException;
  
  public abstract void setUserIDNoPersistance(int userID);

  protected abstract void setGroup(String group) throws IOException;
  
  public abstract void setGroupIDNoPersistance(int groupID);

  abstract void setPermission(FsPermission permission)
      throws StorageException, TransactionContextException;

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
  
  public abstract void setLocalName(byte[] name)
      throws StorageException, TransactionContextException;
  
  public abstract void setModificationTime(long modtime)
      throws StorageException, TransactionContextException;

  public abstract void setAccessTime(long atime)
      throws TransactionContextException, StorageException;

  public void setBlockStoragePolicyID(byte blockStoragePolicyID)
      throws TransactionContextException, StorageException {
    setBlockStoragePolicyIDNoPersistance(blockStoragePolicyID);
    save();
  }

  public void setBlockStoragePolicyIDNoPersistance(byte blockStoragePolicyID)
      throws TransactionContextException, StorageException {
    this.blockStoragePolicyID = blockStoragePolicyID;
  }

  abstract void setModificationTimeForce(long modtime)
      throws StorageException, TransactionContextException;
  
  public int getNumAces(){
    return numAces;
  }
  
  public void setNumAces(int numAces) throws TransactionContextException, StorageException {
    setNumAcesNoPersistence(numAces);
    save();
  }
  
  public void setNumAcesNoPersistence(int numAces){
    this.numAces = numAces;
  }
  
  public void inTree() {
    inTree = true;
  }

  public boolean isInTree() {
    return inTree;
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
    if ((node instanceof INodeDirectory) && ((INodeDirectory) node).isWithQuota()) {
      final DirectoryWithQuotaFeature q = ((INodeDirectory) node).getDirectoryWithQuotaFeature();
      q.removeAttributes((INodeDirectory) node);
      ((INodeDirectory) node).removeFeature(q);
    }
    
    cleanParity(node);
  }

  private void cleanParity(INode node)
      throws StorageException, TransactionContextException {
    if (ErasureCodingManager.isEnabled()) {
      if(node instanceof INodeFile && ((INodeFile)node).isFileStoredInDB()){
        // files stored in database are not erasure coded
        return;
      }

      EncodingStatus status =
          EntityManager.find(EncodingStatus.Finder.ByInodeId, node.getId());
      if (status != null) {
        status.setStatus(EncodingStatus.Status.DELETED);
        EntityManager.update(status);
      }
    }
  }

  public boolean isSTOLocked() {
    return subtreeLocked;
  }

  public void setSubtreeLocked(boolean subtreeLocked) {
    this.subtreeLocked = subtreeLocked;
  }

  public long getSTOLockOwner() {
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

  public abstract void logMetadataEvent(MetadataLogEntry.Operation operation)
      throws StorageException, TransactionContextException;

  boolean isPathMetaEnabled() throws TransactionContextException, StorageException {
    return getMetaEnabledParent() != null;
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

  public abstract Integer getPartitionId();

  public abstract void setPartitionIdNoPersistance(Integer partitionId);

  public abstract void setPartitionId(Integer partitionId) throws
      TransactionContextException, StorageException;

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

  public abstract long getHeader();

  public abstract void setHeaderNoPersistance(long header);

  public void setHasBlocks(boolean hasBlocks) throws TransactionContextException, StorageException {
    setHasBlocksNoPersistance(hasBlocks);
    save();
  }

  @VisibleForTesting
  public abstract void setHasBlocksNoPersistance(boolean hasBlocks)
      throws TransactionContextException, StorageException;
  
  public abstract boolean hasBlocks();

  public short myDepth() throws TransactionContextException, StorageException {
    if(getId() == INodeDirectory.ROOT_INODE_ID){
      return INodeDirectory.ROOT_DIR_DEPTH;
    }

    INode parentInode = EntityManager.find(Finder.ByINodeIdFTIS, getParentId());
    return (short) (parentInode.myDepth()+1);
  }

  public boolean equalsIdentifier(INodeIdentifier iNodeIdentifier){
    if(iNodeIdentifier == null)
      return false;

    if(getId() != iNodeIdentifier.getInodeId())
      return false;

    if(getParentId() != iNodeIdentifier.getPid())
      return false;

    if(iNodeIdentifier.getPartitionId() == null)
      return false;

    if(!getPartitionId().equals(iNodeIdentifier.getPartitionId()))
      return false;

    if(!getLocalName().equals(iNodeIdentifier.getName()))
      return false;

    return true;
  }

  public abstract int getLogicalTime();

  public abstract void setLogicalTimeNoPersistance(Integer logicalTime);
  
  public abstract int incrementLogicalTime();
  
  /**
   * Dump the subtree starting from this inode.
   *
   * @return a text representation of the tree.
   */
  @VisibleForTesting
  public StringBuffer dumpTreeRecursively() throws StorageException, TransactionContextException {
    final StringWriter out = new StringWriter();
    dumpTreeRecursively(new PrintWriter(out, true), new StringBuilder());
    return out.getBuffer();
  }

  /**
   * Dump tree recursively.
   *
   * @param prefix The prefix string that each line should print.
   */
  @VisibleForTesting
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix) throws StorageException,
      TransactionContextException {
    out.print(prefix);
    out.print(" ");
    out.print(getLocalName());
    out.print("   (");
    final String s = super.toString();
    out.print(s.substring(s.lastIndexOf(getClass().getSimpleName())));
    out.println(")");
  }
  
  /**
   * Information used for updating the blocksMap when deleting files.
   */
  public static class BlocksMapUpdateInfo {

    /**
     * The list of blocks that need to be removed from blocksMap
     */
    private List<Block> toDeleteList;

    public BlocksMapUpdateInfo(List<Block> toDeleteList) {
      this.toDeleteList = toDeleteList == null ? new ChunkedArrayList<Block>()
          : toDeleteList;
    }

    public BlocksMapUpdateInfo() {
      toDeleteList = new ArrayList<Block>();
    }

    /**
     * @return The list of blocks that need to be removed from blocksMap
     */
    public List<Block> getToDeleteList() {
      return toDeleteList;
    }

    /**
     * Add a to-be-deleted block into the
     * {@link BlocksMapUpdateInfo#toDeleteList}
     *
     * @param toDelete the to-be-deleted block
     */
    public void addDeleteBlock(Block toDelete) {
      if (toDelete != null) {
        toDeleteList.add(toDelete);
      }
    }

    /**
     * Clear {@link BlocksMapUpdateInfo#toDeleteList}
     */
    public void clear() {
      toDeleteList.clear();
    }
  }
  
  public abstract INode cloneInode() throws IOException;
  
  @Override
  public void setNext(LinkedElement next) {
    this.next = next;
  }

  @Override
  public LinkedElement getNext() {
    return next;
  }
  
  /**
   * INode feature such as {@link FileUnderConstructionFeature}
   * and {@link DirectoryWithQuotaFeature}.
   */
  public interface Feature {
  }
}
