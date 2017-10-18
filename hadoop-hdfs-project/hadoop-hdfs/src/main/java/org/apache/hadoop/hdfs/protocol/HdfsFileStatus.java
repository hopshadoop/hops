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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;

import java.io.IOException;
import java.net.URI;

/**
 * Interface that represents the over the wire information for a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsFileStatus extends FileStatus {

  private byte[] path;  // local name of the inode that's encoded in java UTF8
  private byte[] symlink; // symlink target encoded in java UTF8 or null

  private final long fileId;
  private boolean isFileStoredInDB;

  public static final byte[] EMPTY_NAME = new byte[0];

  /**
   * Constructor
   *
   * @param fileid
   *    the inode id of the file
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
  public HdfsFileStatus(long fileid, long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, boolean isFileStoredInDB) {

    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, permission, owner, group,
        path == null || path.length == 0 ? null : new Path(DFSUtil.bytes2String(path)));

    this.fileId = fileid;
    this.symlink = symlink;
    this.path = path;
    this.isFileStoredInDB = isFileStoredInDB;
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
  @Override
  final public Path getSymlink() throws IOException {
    if (isSymlink()) {
      return new Path(DFSUtil.bytes2String(symlink));
    }
    throw new IOException("Path " + getPath() + " is not a symbolic link");
  }

  @Override
  public void setSymlink(Path sym) {
    symlink = DFSUtil.string2Bytes(sym.toString());
  }

  final public byte[] getSymlinkInBytes() {
    return symlink;
  }

  final public long getFileId() { return fileId; }

  final public boolean isFileStoredInDB(){ return isFileStoredInDB; }

  /**
   * Resolve the short name of the Path given the URI, parent provided. This
   * FileStatus reference will not contain a valid Path until it is resolved
   * by this method.
   * @param defaultUri FileSystem to fully qualify HDFS path.
   * @param parent Parent path of this element.
   * @return Reference to this instance.
   */
  public final FileStatus makeQualified(URI defaultUri, Path parent) {
    // fully-qualify path
    setPath(getFullPath(parent).makeQualified(defaultUri, null));
    return this; // API compatibility
  }
}
