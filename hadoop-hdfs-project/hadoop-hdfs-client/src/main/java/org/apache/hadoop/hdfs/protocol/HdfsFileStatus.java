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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

import java.net.URI;

/**
 * Interface that represents the over the wire information for a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsFileStatus {

  private byte[] path;  // local name of the inode that's encoded in java UTF8
  private byte[] symlink; // symlink target encoded in java UTF8 or null
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private FsPermission permission;
  private String owner;
  private String group;
  private long fileId;
  
  private final FileEncryptionInfo feInfo;
  
  // Used by dir, not including dot and dotdot. Always zero for a regular file.
  private int childrenNum;
  
  private boolean isFileStoredInDB;
  private final byte storagePolicy;

  public static final byte[] EMPTY_NAME = new byte[0];

  /**
   * Constructor
   *
   * @param length the number of bytes the file has
   * @param isdir if the path is a directory
   * @param block_replication the replication factor
   * @param blocksize the block size
   * @param modification_time modification time
   * @param access_time access time
   * @param permission permission
   * @param owner the owner of the path
   * @param group the group of the path
   * @param path the local name in java UTF8 encoding the same as that in-memory
   * @param fileid the inode id of the file
   * @param feInfo the file's encryption info
   */
  public HdfsFileStatus(long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, long fileId, int childrenNum, FileEncryptionInfo feInfo, 
      boolean isFileStoredInDB, byte storagePolicy) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short) block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    this.permission = (permission == null) ?
        ((isdir || symlink!=null) ?
            FsPermission.getDefault() :
            FsPermission.getFileDefault()) :
        permission;
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
    this.fileId = fileId;
    this.isFileStoredInDB = isFileStoredInDB;
    this.storagePolicy = storagePolicy;
    this.childrenNum = childrenNum;
    this.feInfo = feInfo;
  }

  /**
   * Get the length of this file, in bytes.
   *
   * @return the length of this file, in bytes.
   */
  public final long getLen() {
    return length;
  }

  /**
   * Is this a directory?
   *
   * @return true if this is a directory
   */
  public final boolean isDir() {
    return isdir;
  }

  /**
   * Is this a symbolic link?
   *
   * @return true if this is a symbolic link
   */
  public boolean isSymlink() {
    return symlink != null;
  }

  /**
   * Get the block size of the file.
   *
   * @return the number of bytes
   */
  public final long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   *
   * @return the replication factor of a file.
   */
  public final short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   *
   * @return the modification time of file in milliseconds since January 1, 1970
   * UTC.
   */
  public final long getModificationTime() {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   *
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public final long getAccessTime() {
    return access_time;
  }

  /**
   * Get FsPermission associated with the file.
   *
   * @return permssion
   */
  public final FsPermission getPermission() {
    return permission;
  }

  /**
   * Get the owner of the file.
   *
   * @return owner of the file
   */
  public final String getOwner() {
    return owner;
  }

  /**
   * Get the group associated with the file.
   * @return group for the file.
   */
  public final String getGroup() {
    return group;
  }

  /**
   * Check if the local name is empty
   *
   * @return true if the name is empty
   */
  public final boolean isEmptyLocalName() {
    return path.length == 0;
  }

  /**
   * Get the string representation of the local name
   *
   * @return the local name in string
   */
  public final String getLocalName() {
    return DFSUtilClient.bytes2String(path);
  }

  /**
   * Get the Java UTF8 representation of the local name
   *
   * @return the local name in java UTF8
   */
  public final byte[] getLocalNameInBytes() {
    return path;
  }

  /**
   * Get the string representation of the full path name
   *
   * @param parent
   *     the parent path
   * @return the full path in string
   */
  public final String getFullName(final String parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    StringBuilder fullName = new StringBuilder(parent);
    if (!parent.endsWith(Path.SEPARATOR)) {
      fullName.append(Path.SEPARATOR);
    }
    fullName.append(getLocalName());
    return fullName.toString();
  }

  /**
   * Get the full path
   *
   * @param parent
   *     the parent path
   * @return the full path
   */
  public final Path getFullPath(final Path parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    return new Path(parent, getLocalName());
  }

  /**
   * Get the string representation of the symlink.
   *
   * @return the symlink as a string.
   */
  public final String getSymlink() {
    return DFSUtilClient.bytes2String(symlink);
  }

  public final byte[] getSymlinkInBytes() {
    return symlink;
  }

  public final long getFileId() {
    return fileId;
  }
  
  public final FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  public final int getChildrenNum() {
    return childrenNum;
  }

  public final boolean isFileStoredInDB(){ 
    return isFileStoredInDB; 
  }

  public void setFileStoredInDB(boolean isFileStoredInDB){
    this.isFileStoredInDB = isFileStoredInDB;
  }
  /**
   * Resolve the short name of the Path given the URI, parent provided. This
   * FileStatus reference will not contain a valid Path until it is resolved
   * by this method.
   * @param defaultUri FileSystem to fully qualify HDFS path.
   * @param parent Parent path of this element.
   * @return Reference to this instance.
   */
  public final FileStatus makeQualified(URI defaultUri, Path path) {
    return new FileStatus(getLen(), isDir(), getReplication(),
        getBlockSize(), getModificationTime(),
        getAccessTime(),
        getPermission(), getOwner(), getGroup(),
        isSymlink() ? new Path(getSymlink()) : null,
        (getFullPath(path)).makeQualified(
        defaultUri, null),getPermission().getAclBit(),getPermission().getEncryptedBit(),false); // fully-qualify path
  }

  /** @return the storage policy id */
  public final byte getStoragePolicy() {
    return storagePolicy;
  }
}
