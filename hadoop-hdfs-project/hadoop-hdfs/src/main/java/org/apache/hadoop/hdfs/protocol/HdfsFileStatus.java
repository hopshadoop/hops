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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;

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
  private byte storagePolicy;

  public static final byte[] EMPTY_NAME = new byte[0];

  /**
   * Constructor
   *
   * @param length
   *     the number of bytes the file has
   * @param isdir
   *     if the path is a directory
   * @param block_replication
   *     the replication factor
   * @param blocksize
   *     the block size
   * @param modification_time
   *     modification time
   * @param access_time
   *     access time
   * @param permission
   *     permission
   * @param owner
   *     the owner of the path
   * @param group
   *     the group of the path
   * @param path
   *     the local name in java UTF8 encoding the same as that in-memory
   */
  public HdfsFileStatus(long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, byte storagePolicy) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short) block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    this.permission = (permission == null) ?
        ((isdir || symlink != null) ? FsPermission.getDefault() :
            FsPermission.getFileDefault()) : permission;
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
    this.storagePolicy = storagePolicy;
  }

  /**
   * Get the length of this file, in bytes.
   *
   * @return the length of this file, in bytes.
   */
  final public long getLen() {
    return length;
  }

  /**
   * Is this a directory?
   *
   * @return true if this is a directory
   */
  final public boolean isDir() {
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
  final public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   *
   * @return the replication factor of a file.
   */
  final public short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   *
   * @return the modification time of file in milliseconds since January 1, 1970
   * UTC.
   */
  final public long getModificationTime() {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   *
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  final public long getAccessTime() {
    return access_time;
  }

  /**
   * Get FsPermission associated with the file.
   *
   * @return permssion
   */
  final public FsPermission getPermission() {
    return permission;
  }
  
  /**
   * Get the owner of the file.
   *
   * @return owner of the file
   */
  final public String getOwner() {
    return owner;
  }
  
  /**
   * Get the group associated with the file.
   *
   * @return group for the file.
   */
  final public String getGroup() {
    return group;
  }
  
  /**
   * Check if the local name is empty
   *
   * @return true if the name is empty
   */
  final public boolean isEmptyLocalName() {
    return path.length == 0;
  }

  /**
   * Get the string representation of the local name
   *
   * @return the local name in string
   */
  final public String getLocalName() {
    return DFSUtil.bytes2String(path);
  }
  
  /**
   * Get the Java UTF8 representation of the local name
   *
   * @return the local name in java UTF8
   */
  final public byte[] getLocalNameInBytes() {
    return path;
  }

  /**
   * Get the string representation of the full path name
   *
   * @param parent
   *     the parent path
   * @return the full path in string
   */
  final public String getFullName(final String parent) {
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
  final public Path getFullPath(final Path parent) {
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
  final public String getSymlink() {
    return DFSUtil.bytes2String(symlink);
  }
  
  final public byte[] getSymlinkInBytes() {
    return symlink;
  }

  /** @return the storage policy id */
  public final byte getStoragePolicy() {
    return storagePolicy;
  }
}
