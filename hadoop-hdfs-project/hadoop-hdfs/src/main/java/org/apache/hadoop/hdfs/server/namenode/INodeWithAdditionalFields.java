/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;

import java.io.IOException;

public abstract class INodeWithAdditionalFields extends INode {
  
  /** The inode id */
  final protected int id;
  /**
   * The inode name is in java UTF8 encoding;
   * The name in HdfsFileStatus should keep the same encoding as this.
   * if this encoding is changed, implicitly getFileInfo and listStatus in
   * clientProtocol are changed; The decoding at the client
   * side should change accordingly.
   */
  private byte[] name = null;
  
  private FsPermission permission;
  /** The last modification time */
  protected  long modificationTime = 0L;
  /** The last access time */
  protected long accessTime = 0L;
  
  private String userName;
  private String groupName;
  
  private int userId;
  private int groupId;
  
  long header = 0L;
  protected Integer partitionId;
  
  /** An array {@link Feature}s. */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];
  protected Feature[] features = EMPTY_FEATURE;
  
  private INodeWithAdditionalFields(INode parent, int id, byte[] name,
      PermissionStatus permission, long modificationTime, long accessTime, boolean inTree)
      throws IOException {
    super(parent);
    this.id = id;
    this.name = name;
    this.permission = permission.getPermission();
    this.userName = permission.getUserName();
    this.userId = UsersGroups.getUserID(permission.getUserName());
    this.groupName = permission.getGroupName();
    this.groupId = UsersGroups.getGroupID(permission.getGroupName());
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.inTree = inTree;
  }
  
  INodeWithAdditionalFields(int id, byte[] name, PermissionStatus permission,
      long modificationTime, long accessTime, boolean inTree) throws IOException {
    this(null, id, name, permission, modificationTime, accessTime, inTree);
  }
  
  INodeWithAdditionalFields(INodeWithAdditionalFields other) throws IOException {
    this(other.getParent(), other.getId(), other.getLocalNameBytes(),
        other.getPermissionStatus(), other.modificationTime, other.accessTime, other.inTree);
    this.header = other.getHeader();
    this.partitionId = other.getPartitionId();
    this.parentId = other.getParentId();
    this.logicalTime = other.getLogicalTime();
  }
  
  INodeWithAdditionalFields(int id, String name, PermissionStatus permissions)
    throws IOException {
    this(id, name, permissions, false);
  }
  
  INodeWithAdditionalFields(int id, String name, PermissionStatus permissions, boolean inTree)
    throws IOException {
    this(null, id, DFSUtil.string2Bytes(name), permissions, 0L, 0L, inTree);
  }
  
  INodeWithAdditionalFields(int id, PermissionStatus permissions, long modificationTime, long accessTime)
    throws IOException {
    this(id, permissions, modificationTime, accessTime, false);
  }
  
  INodeWithAdditionalFields(int id, PermissionStatus permissions, long modificationTime,
      long accessTime, boolean inTree) throws IOException {
    this(id, null, permissions, modificationTime, accessTime, inTree);
  }
  
  /** Get the inode id */
  public int getId() {
    return this.id;
  }
  
  /**
   * Get local file name
   *
   * @return null if the local name is null;
   *          otherwise, return the local name byte array
   */
  @Override
  public final byte[] getLocalNameBytes() {
    return name;
  }
  
  @Override
  public final void setLocalName(byte[] name)
    throws StorageException, TransactionContextException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  /** Clone the {@link FsPermission}. */
  final void clonePermission(INodeWithAdditionalFields that)
    throws StorageException, TransactionContextException {
    this.permission = that.permission;
    save();
  }
  
  @Override
  public final PermissionStatus getPermissionStatus() throws IOException {
    return new PermissionStatus(getUserName(), getGroupName(), getFsPermission());
  }
  
  /**
   * Set user
   */
  protected final void setUser(String user) throws IOException {
    setUserNoPersistance(user);
    save();
  }
  
  /**
   * Get user name
   */
  public final String getUserName() throws IOException {
    if(userName == null || userName.isEmpty()){
      userName = UsersGroups.getUser(userId);
    }
    return userName;
  }
  
  public final int getUserID() {
    return userId;
  }
  
  public final int getGroupID() {
    return groupId;
  }
  
  /**
   * Get group name
   */
  public final String getGroupName() throws IOException {
    if(groupName == null || groupName.isEmpty()){
      groupName = UsersGroups.getGroup(groupId);
    }
    return groupName;
  }
  
  protected final void setGroup(String group) throws IOException {
    setGroupNoPersistance(group);
    save();
  }
  
  protected final short getFsPermissionShort() {
    return permission.toShort();
  }
  
  @Override
  void setPermission(FsPermission permission)
    throws StorageException, TransactionContextException {
    setPermissionNoPersistance(permission);
    save();
  }
  
  /**
   * Get last modification time of inode.
   *
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }
  
  public final void setModificationTime(long modificationTime)
    throws StorageException, TransactionContextException {
    setModificationTimeNoPersistance(modificationTime);
    save();
  }
  
  final void setModificationTimeForce(long modificationTime)
    throws StorageException, TransactionContextException {
    setModificationTimeForceNoPersistence(modificationTime);
    save();
  }
  
  final void cloneModificationTime(INodeWithAdditionalFields that)
    throws StorageException, TransactionContextException {
    this.modificationTime = that.modificationTime;
    save();
  }
  
  /**
   * Get access time of inode.
   *
   * @return access time
   */
  public final long getAccessTime() {
    return accessTime;
  }
  
  public final void setAccessTime(long accessTime)
    throws StorageException, TransactionContextException {
    setAccessTimeNoPersistance(accessTime);
    save();
  }
  /**
   * Get the {@link FsPermission}
   */
  public final FsPermission getFsPermission() {
    return permission;
  }
  
  /**
   * Set user
   */
  public void setUserNoPersistance(String user) throws IOException {
    this.userName = user;
    this.userId = UsersGroups.getUserID(user);
  }
  
  /**
   * Set group
   */
  public void setGroupNoPersistance(String group) throws IOException {
    this.groupName = group;
    this.groupId = UsersGroups.getGroupID(group);
  }
  
  public final long getHeader() {
    return header;
  }
  
  public final void setHeaderNoPersistance(long header) {
    this.header = header;
  }
  
  public final void setHasBlocksNoPersistance(boolean hasBlocks)
    throws StorageException, TransactionContextException {
    header = HeaderFormat.combineHasBlocksNoPersistance(header, hasBlocks);
  }
  
  public boolean hasBlocks() {
    return HeaderFormat.hasBlocks(header);
  }
  
  public final Integer getPartitionId() {
    return partitionId;
  }
  
  public final void setPartitionIdNoPersistance(Integer partitionId) {
    this.partitionId = partitionId;
  }
  
  public final void setPartitionId(Integer partitionId)
    throws StorageException, TransactionContextException {
    setPartitionIdNoPersistance(partitionId);
    save();
  }
  
  private int logicalTime;
  
  public void logMetadataEvent(MetadataLogEntry.Operation operation)
      throws StorageException, TransactionContextException {
    if(isUnderConstruction()){
      return;
    }
    if (isPathMetaEnabled()) {
      if(getPartitionId() == null){
        throw new RuntimeException("Trying to log metadata for an inode that " +
            "wasn't commited to the database");
      }
      INodeDirectory datasetDir = getMetaEnabledParent();
      EntityManager.add(new MetadataLogEntry(datasetDir.getId(), getId(),
          getPartitionId(), getParentId(), getLocalName(), incrementLogicalTime(),
          operation));
      save();
    }
  }
  
  public final int getLogicalTime() {
    return logicalTime;
  }
  
  public final void setLogicalTimeNoPersistance(Integer logicalTime) {
    this.logicalTime = logicalTime;
  }
  
  public final int incrementLogicalTime(){
    return ++logicalTime;
  }
  
  /**
   * Set local file name
   */
  public final void setLocalNameNoPersistance(byte[] name) {
    this.name = name;
  }
  
  public void setLocalNameNoPersistance(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }
  
  /**
   * Set the {@link FsPermission} of this {@link INode}
   */
  private void setPermissionNoPersistance(FsPermission permission) {
    this.permission = permission;
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
   * Always set the modification time of inode.
   */
  protected void setModificationTimeForceNoPersistence(long modificationTime) {
    this.modificationTime = modificationTime;
  }
  
  /**
   * Set last access time of inode.
   */
  public void setAccessTimeNoPersistance(long atime) {
    accessTime = atime;
  }
  
  public void setUserIDNoPersistance(int userId) {
    this.userId = userId;
  }
  
  public void setGroupIDNoPersistance(int groupId) {
    this.groupId = groupId;
  }
  
  public void addFeature(Feature f) {
    int size = features.length;
    Feature[] arr = new Feature[size + 1];
    if (size != 0) {
      System.arraycopy(features, 0, arr, 0, size);
    }
    arr[size] = f;
    features = arr;
  }
  
  protected void removeFeature(Feature f) {
    int size = features.length;
    Preconditions.checkState(size > 0, "Feature "
      + f.getClass().getSimpleName() + " not found.");
    if (size == 1) {
      Preconditions.checkState(features[0] == f, "Feature "
        + f.getClass().getSimpleName() + " not found.");
      features = EMPTY_FEATURE;
      return;
    }
    
    Feature[] arr = new Feature[size - 1];
    int j = 0;
    boolean overflow = false;
    for (Feature f1 : features) {
      if (f1 != f) {
        if (j == size - 1) {
          overflow = true;
          break;
        } else {
          arr[j++] = f1;
        }
      }
    }
    
    Preconditions.checkState(!overflow && j == size - 1, "Feature "
      + f.getClass().getSimpleName() + " not found.");
    features = arr;
  }
}
