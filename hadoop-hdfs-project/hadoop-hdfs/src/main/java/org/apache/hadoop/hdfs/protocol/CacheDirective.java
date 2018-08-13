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


import java.util.Date;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.CachePool;

import com.google.common.base.Preconditions;
import io.hops.metadata.common.FinderType;
import static com.google.common.base.Preconditions.checkNotNull;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.transaction.EntityManager;

/**
 * Namenode class that tracks state related to a cached path.
 *
 * This is an implementation class, not part of the public API.
 */
@InterfaceAudience.Private
public final class CacheDirective {
  
  public enum Finder implements FinderType<CacheDirective> {
    ById, ByPoolName, ByIdPoolAndPath, All;

    @Override
    public Class getType() {
      return CacheDirective.class;
    }

    @Override
    public FinderType.Annotation getAnnotated() {
      switch (this) {
        case ById:
          return FinderType.Annotation.PrimaryKey;
        case ByPoolName:
          return FinderType.Annotation.IndexScan;
        case ByIdPoolAndPath:
          return FinderType.Annotation.IndexScan;
        case All:
          return FinderType.Annotation.FullTable;
        default:
          throw new IllegalStateException();
      }
    }
  }
  
  private final long id;
  private final String path;
  private final short replication;
  private String poolName; 
  private final long expiryTime;

  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  
  public CacheDirective(CacheDirectiveInfo info) {
    this(
        info.getId(),
        info.getPath().toUri().getPath(),
        info.getReplication(),
        info.getExpiration().getAbsoluteMillis());
  }

  public CacheDirective(long id, String path,
      short replication, long expiryTime) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    this.path = checkNotNull(path);
    Preconditions.checkArgument(replication > 0);
    this.replication = replication;
    this.expiryTime = expiryTime;
  }

  public CacheDirective(long id, String path,
      short replication, long expiryTime, long bytesNeeded, long bytesCached, long filesNeeded, long filesCached,
      String pool) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    this.path = checkNotNull(path);
    Preconditions.checkArgument(replication > 0);
    this.replication = replication;
    this.expiryTime = expiryTime;
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
    this.poolName = pool;
  }
  
  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public short getReplication() {
    return replication;
  }

  public String getPoolName() {
    return poolName;
  }

  public CachePool getPool() throws TransactionContextException, StorageException {
    return EntityManager.find(CachePool.Finder.ByName, poolName);
  }
  
  public void setPoolName(String pool) {
    this.poolName = pool;
  }

  /**
   * @return When this directive expires, in milliseconds since Unix epoch
   */
  public long getExpiryTime() {
    return expiryTime;
  }

  /**
   * @return When this directive expires, as an ISO-8601 formatted string.
   */
  public String getExpiryTimeString() {
    return DFSUtil.dateToIso8601String(new Date(expiryTime));
  }

  /**
   * Returns a {@link CacheDirectiveInfo} based on this CacheDirective.
   * <p>
   * This always sets an absolute expiry time, never a relative TTL.
   */
  public CacheDirectiveInfo toInfo() {
    return new CacheDirectiveInfo.Builder().
        setId(id).
        setPath(new Path(path)).
        setReplication(replication).
        setPool(poolName).
        setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(expiryTime)).
        build();
  }

  public CacheDirectiveStats toStats() {
    return new CacheDirectiveStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setFilesNeeded(filesNeeded).
        setFilesCached(filesCached).
        setHasExpired(new Date().getTime() > expiryTime).
        build();
  }

  public CacheDirectiveEntry toEntry() {
    return new CacheDirectiveEntry(toInfo(), toStats());
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ id:").append(id).
      append(", path:").append(path).
      append(", replication:").append(replication).
      append(", pool:").append(poolName).
      append(", expiryTime: ").append(getExpiryTimeString()).
      append(", bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", filesNeeded:").append(filesNeeded).
      append(", filesCached:").append(filesCached).
      append(" }");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CacheDirective other = (CacheDirective)o;
    return id == other.id;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).toHashCode();
  }

  //
  // Stats related getters and setters
  //

  /**
   * Resets the byte and file statistics being tracked by this CacheDirective.
   */
  public void resetStatistics() throws TransactionContextException, StorageException {
    bytesNeeded = 0;
    bytesCached = 0;
    filesNeeded = 0;
    filesCached = 0;
    save();
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public void addBytesNeeded(long bytes) throws TransactionContextException, StorageException {
    this.bytesNeeded += bytes;
    CachePool pool = getPool();
    pool.addBytesNeeded(bytes);
    pool.save();
    save();
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public void addBytesCached(long bytes) throws TransactionContextException, StorageException {
    this.bytesCached += bytes;
    CachePool pool = getPool();
    pool.addBytesCached(bytes);
    pool.save();
    save();
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public void addFilesNeeded(long files) throws TransactionContextException, StorageException {
    this.filesNeeded += files;
    CachePool pool = getPool();
    pool.addFilesNeeded(files);
    pool.save();
    save();
  }

  public long getFilesCached() {
    return filesCached;
  }

  public void addFilesCached(long files) throws TransactionContextException, StorageException {
    this.filesCached += files;
    CachePool pool = getPool();
    pool.addFilesCached(files);
    pool.save();
    save();
  }
  
  private void save() throws TransactionContextException, StorageException{
    EntityManager.update(this);
  }
  
};
