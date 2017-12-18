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

package org.apache.hadoop.hdfs;

import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LastUpdatedContentSummary;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;


/**
 * *************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 * <p/>
 * ***************************************************************
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "HBase"})
@InterfaceStability.Unstable
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;
  
  static {
    HdfsConfiguration.init();
  }

  public DistributedFileSystem() {
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>hdfs</code>
   */
  @Override
  public String getScheme() {
    return HdfsConstants.HDFS_URI_SCHEME;
  }

  @Deprecated
  public DistributedFileSystem(InetSocketAddress namenode, Configuration conf)
      throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: " + uri);
    }

    this.dfs = new DFSClient(uri, conf, statistics);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = getHomeDirectory();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return dfs.getDefaultReplication();
  }

  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException(
          "Invalid DFS directory name " + result);
    }
    workingDir = makeAbsolute(dir);
  }

  
  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path("/user/" + dfs.ugi.getShortUserName()));
  }

  private String getPathName(Path file) {
    checkPath(file);
    String result = makeAbsolute(file).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
          file + " is not a valid DFS filename.");
    }
    return result;
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    return getFileBlockLocations(file.getPath(), start, len);
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getBlockLocations(getPathName(p), start, len);

  }

  /**
   * Used to query storage location information for a list of blocks. This list
   * of blocks is normally constructed via a series of calls to
   * {@link DistributedFileSystem#getFileBlockLocations(Path, long, long)} to
   * get the blocks for ranges of a file.
   * <p/>
   * The returned array of {@link BlockStorageLocation} augments
   * {@link BlockLocation} with a {@link VolumeId} per block replica. The
   * VolumeId specifies the volume on the datanode on which the replica
   * resides.
   * The VolumeId has to be checked via {@link VolumeId#isValid()} before being
   * used because volume information can be unavailable if the corresponding
   * datanode is down or if the requested block is not found.
   * <p/>
   * This API is unstable, and datanode-side support is disabled by default. It
   * can be enabled by setting "dfs.datanode.hdfs-blocks-metadata.enabled" to
   * true.
   *
   * @param blocks
   *     List of target BlockLocations to query volume location information
   * @return volumeBlockLocations Augmented array of
   * {@link BlockStorageLocation}s containing additional volume location
   * information for each replica of each block.
   */
  @InterfaceStability.Unstable
  public BlockStorageLocation[] getFileBlockStorageLocations(
      List<BlockLocation> blocks)
      throws IOException, UnsupportedOperationException,
      InvalidBlockTokenException {
    return dfs.getBlockStorageLocations(blocks);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /**
   * Start the lease recovery of a file
   *
   * @param f
   *     a file
   * @return true if the file is already closed
   * @throws IOException
   *     if an error occurs
   */
  public boolean recoverLease(Path f) throws IOException {
    return dfs.recoverLease(getPathName(f));
  }

  @SuppressWarnings("deprecation")
  @Override
  public HdfsDataInputStream open(Path f, int bufferSize) throws IOException {
    statistics.incrementReadOps(1);
    return new DFSClient.DFSDataInputStream(
        dfs.open(getPathName(f), bufferSize, verifyChecksum));
  }

  /**
   * This optional operation is not yet supported.
   */
  @Override
  public HdfsDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.append(getPathName(f), bufferSize, progress, statistics);
  }

  /**
   * Create a file that will be erasure-coded asynchronously after creation.
   * Using this method ensures that the file is being written in a way that
   * ensures optimal block placement for the given encoding policy.
   *
   * @param f
   *    the path
   * @param policy
   *    the erasure coding policy to be applied
   * @return
   *    the stream to be written to
   * @throws IOException
   */
  public HdfsDataOutputStream create(Path f, EncodingPolicy policy)
      throws IOException {
    return create(f, true, policy);
  }

  public HdfsDataOutputStream create(Path f, short replication,
      EncodingPolicy policy) throws IOException {
    return create(f, true, replication, policy);
  }

  /**
   * Create a file that will be erasure-coded asynchronously after creation.
   * Using this method ensures that the file is being written in a way that
   * ensures optimal block placement for the given encoding policy.
   *
   * @param f
   *    the path
   * @param overwrite
   *    whether an existing file should be overwritten or not
   * @param replication
   *    the replication factor used to write the file before it is encoded
   * @param policy
   *    the erasure coding policy to be applied
   * @return
   *    the stream to be written to
   * @throws IOException
   */
  public HdfsDataOutputStream create(Path f, boolean overwrite,
      short replication, EncodingPolicy policy) throws IOException {
    return create(f, overwrite, getConf().getInt("io.file.buffer.size", 4096),
        replication, getDefaultBlockSize(f), null, policy);
  }

  /**
   * Create a file that will be erasure-coded asynchronously after creation.
   * Using this method ensures that the file is being written in a way that
   * ensures optimal block placement for the given encoding policy.
   *
   * @param f
   *    the path
   * @param overwrite
   *    whether an existing file should be overwritten or not
   * @param policy
   *    the erasure coding policy to be applied
   * @return
   *    the stream to be written to
   * @throws IOException
   */
  public HdfsDataOutputStream create(Path f, boolean overwrite,
      EncodingPolicy policy) throws IOException {
    return create(f, overwrite, getDefaultReplication(f), policy);
  }

  public HdfsDataOutputStream create(Path f, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress,
      EncodingPolicy policy) throws IOException {
    return this.create(f, FsPermission.getFileDefault()
            .applyUMask(FsPermission.getUMask(getConf())), overwrite,
        bufferSize, replication, blockSize, progress, policy);
  }

  /**
   * Create a file that will be erasure-coded asynchronously after creation.
   * Using this method ensures that the file is being written in a way that
   * ensures optimal block placement for the given encoding policy.
   *
   * @param f
   *    the path
   * @param permission
   *    the file permissions
   * @param overwrite
   *    whether an existing file should be overwritten or not
   * @param bufferSize
   *    the buffer size
   * @param replication
   *    the replication factor used to write the file before it is encoded
   * @param blockSize
   *    the block size to be used
   * @param progress
   *    the Progressable to report to
   * @param policy
   *    the erasure coding policy to be applied
   * @return
   *    the stream to be written to
   * @throws IOException
   */
  public HdfsDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress, EncodingPolicy policy) throws IOException {
    return this.create(f, permission,
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE) :
            EnumSet.of(CreateFlag.CREATE), bufferSize, replication, blockSize,
        progress, null, policy);
  }

  @Override
  public HdfsDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.create(f, permission,
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE) :
            EnumSet.of(CreateFlag.CREATE), bufferSize, replication, blockSize,
        progress, null);
  }
  
  @Override
  public HdfsDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication,
      long blockSize, Progressable progress, ChecksumOpt checksumOpt)
      throws IOException {
    return create(f, permission, cflags, bufferSize, replication, blockSize,
        progress, checksumOpt, null);
  }

  /**
   * Create a file that will be erasure-coded asynchronously after creation.
   * Using this method ensures that the file is being written in a way that
   * ensures optimal block placement for the given encoding policy.
   *
   * @param f
   *    the path
   * @param permission
   *    the file permissions
   * @pram cflags
   *    the creation flags
   * @param bufferSize
   *    the buffer size
   * @param replication
   *    the replication factor used to write the file before it is encoded
   * @param blockSize
   *    the block size to be used
   * @param progress
   *    the Progressable to report to
   * @param checksumOpt
   *    the checksum options
   * @param policy
   *    the erasure coding policy to be applied
   * @return
   *    the stream to be written to
   * @throws IOException
   */
  public HdfsDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> cflags, int bufferSize, short replication,
      long blockSize, Progressable progress, ChecksumOpt checksumOpt,
      EncodingPolicy policy) throws IOException {
    statistics.incrementWriteOps(1);
    final DFSOutputStream out =
        dfs.create(getPathName(f), permission, cflags, replication, blockSize,
            progress, bufferSize, checksumOpt, policy);
    return new HdfsDataOutputStream(out, statistics);
  }
  
  @SuppressWarnings("deprecation")
  @Override
  protected HdfsDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    statistics.incrementWriteOps(1);
    return new HdfsDataOutputStream(
        dfs.primitiveCreate(getPathName(f), absolutePermission, flag, true,
            replication, blockSize, progress, bufferSize, checksumOpt),
        statistics);
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   */
  @Override
  public HdfsDataOutputStream createNonRecursive(Path f,
      FsPermission permission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress)
      throws IOException {
    statistics.incrementWriteOps(1);
    if (flag.contains(CreateFlag.OVERWRITE)) {
      flag.add(CreateFlag.CREATE);
    }
    return new HdfsDataOutputStream(
        dfs.create(getPathName(f), permission, flag, false, replication,
            blockSize, progress, bufferSize, null), statistics);
  }

  public HdfsDataOutputStream sendBlock(Path f, LocatedBlock block,
      Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    statistics.incrementWriteOps(1);
    final DFSOutputStream out =
        dfs.sendBlock(getPathName(f), block, progress, checksumOpt);
    return new HdfsDataOutputStream(out, statistics);
  }

  @Override
  public boolean setReplication(Path src, short replication)
      throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.setReplication(getPathName(src), replication);
  }

  public void setMetaEnabled(Path src, boolean metaEnabled)
      throws IOException {
    statistics.incrementWriteOps(1);
    dfs.setMetaEnabled(getPathName(src), metaEnabled);
  }

  public int getNameNodesCount()
      throws IOException {
    return dfs.getNameNodesCount();
  }
  /**
   * Move blocks from srcs to trg
   * and delete srcs afterwards
   * RESTRICTION: all blocks should be the same size
   *
   * @param trg
   *     existing file to append to
   * @param psrcs
   *     list of files (same block size, same replication)
   * @throws IOException
   */
  public void concat(Path trg, Path[] psrcs) throws IOException {
    String[] srcs = new String[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      srcs[i] = getPathName(psrcs[i]);
    }
    statistics.incrementWriteOps(1);
    dfs.concat(getPathName(trg), srcs);
  }

  
  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.rename(getPathName(src), getPathName(dst));
  }

  /**
   * This rename operation is guaranteed to be atomic.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, Options.Rename... options)
      throws IOException {
    statistics.incrementWriteOps(1);
    dfs.rename(getPathName(src), getPathName(dst), options);
  }
  
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.delete(getPathName(f), recursive);
  }
  
  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getContentSummary(getPathName(f));
  }

  /**
   * Set a directory's quotas
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long,
   * long)
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota)
      throws IOException {
    dfs.setQuota(getPathName(src), namespaceQuota, diskspaceQuota);
  }
  
  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(), (f.getFullPath(parent))
        .makeQualified(getUri(), getWorkingDirectory())); // fully-qualify path
  }

  private LocatedFileStatus makeQualifiedLocated(HdfsLocatedFileStatus f,
      Path parent) {
    return new LocatedFileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(), null,
        (f.getFullPath(parent)).makeQualified(getUri(), getWorkingDirectory()),
        // fully-qualify path
        DFSUtil.locatedBlocks2Locations(f.getBlockLocations()));
  }

  /**
   * List all the entries of a directory
   * <p/>
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    String src = getPathName(p);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing =
        dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = makeQualified(partialListing[i], p);
      }
      statistics.incrementReadOps(1);
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
        partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing = new ArrayList<>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(makeQualified(fileStatus, p));
    }
    statistics.incrementLargeReadOps(1);

    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());

      if (thisListing == null) { // the directory is deleted
        throw new FileNotFoundException("File " + p + " does not exist.");
      }

      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(makeQualified(fileStatus, p));
      }
      statistics.incrementLargeReadOps(1);
    } while (thisListing.hasMore());

    return listing.toArray(new FileStatus[listing.size()]);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter) throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private DirectoryListing thisListing;
      private int i;
      private String src;
      private LocatedFileStatus curStat = null;

      { // initializer
        src = getPathName(p);
        // fetch the first batch of entries in the directory
        thisListing = dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME, true);
        statistics.incrementReadOps(1);
        if (thisListing == null) { // the directory does not exist
          throw new FileNotFoundException("File " + p + " does not exist.");
        }
      }

      @Override
      public boolean hasNext() throws IOException {
        while (curStat == null && hasNextNoFilter()) {
          LocatedFileStatus next = makeQualifiedLocated(
              (HdfsLocatedFileStatus) thisListing.getPartialListing()[i++], p);
          if (filter.accept(next.getPath())) {
            curStat = next;
          }
        }
        return curStat != null;
      }
      
      /** Check if there is a next item before applying the given filter */
      private boolean hasNextNoFilter() throws IOException {
        if (thisListing == null) {
          return false;
        }
        if (i >= thisListing.getPartialListing().length &&
            thisListing.hasMore()) {
          // current listing is exhausted & fetch a new listing
          thisListing = dfs.listPaths(src, thisListing.getLastName(), true);
          statistics.incrementReadOps(1);
          if (thisListing == null) {
            return false;
          }
          i = 0;
        }
        return (i < thisListing.getPartialListing().length);
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          LocatedFileStatus tmp = curStat;
          curStat = null;
          return tmp;
        }
        throw new java.util.NoSuchElementException("No more entry in " + p);
      }
    };
  }
  
  /**
   * Create a directory, only when the parent directories exist.
   * <p/>
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f
   *     The path to create
   * @param permission
   *     The permission.  See FsPermission#applyUMask for
   *     details about how this is used to calculate the
   *     effective permission.
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.mkdirs(getPathName(f), permission, false);
  }

  /**
   * Create a directory and its parent directories.
   * <p/>
   * See {@link FsPermission#applyUMask(FsPermission)} for details of how
   * the permission is applied.
   *
   * @param f
   *     The path to create
   * @param permission
   *     The permission.  See FsPermission#applyUMask for
   *     details about how this is used to calculate the
   *     effective permission.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.mkdirs(getPathName(f), permission, true);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    statistics.incrementWriteOps(1);
    return dfs.primitiveMkdir(getPathName(f), absolutePermission);
  }


  @Override
  public void close() throws IOException {
    try {
      super.processDeleteOnExit();
      dfs.close();
    } finally {
      super.close();
    }
  }

  @Override
  public String toString() {
    return "DFS[" + dfs + "]";
  }

  /**
   * @deprecated DFSClient should not be accessed directly.
   */
  @InterfaceAudience.Private
  @Deprecated
  public DFSClient getClient() {
    return dfs;
  }
  
  /**
   * @deprecated Use {@link org.apache.hadoop.fs.FsStatus} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static class DiskStatus extends FsStatus {
    public DiskStatus(FsStatus stats) {
      super(stats.getCapacity(), stats.getUsed(), stats.getRemaining());
    }

    public DiskStatus(long capacity, long dfsUsed, long remaining) {
      super(capacity, dfsUsed, remaining);
    }

    public long getDfsUsed() {
      return super.getUsed();
    }
  }
  
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getDiskStatus();
  }

  /**
   * Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space
   *
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()}
   * instead
   */
  @Deprecated
  public DiskStatus getDiskStatus() throws IOException {
    return new DiskStatus(dfs.getDiskStatus());
  }
  
  /**
   * Return the total raw capacity of the filesystem, disregarding
   * replication.
   *
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()}
   * instead
   */
  @Deprecated
  public long getRawCapacity() throws IOException {
    return dfs.getDiskStatus().getCapacity();
  }

  /**
   * Return the total raw used space in the filesystem, disregarding
   * replication.
   *
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()}
   * instead
   */
  @Deprecated
  public long getRawUsed() throws IOException {
    return dfs.getDiskStatus().getUsed();
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   *
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks with one of more replica missing.
   *
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   *
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    return new CorruptFileBlockIterator(dfs, path);
  }

  /**
   * @return datanode statistics.
   */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return getDataNodeStats(DatanodeReportType.ALL);
  }

  /**
   * @return datanode statistics for the given type.
   */
  public DatanodeInfo[] getDataNodeStats(final DatanodeReportType type)
      throws IOException {
    return dfs.datanodeReport(type);
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *HdfsConstants.SafeModeAction, boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action)
      throws IOException {
    return setSafeMode(action, false);
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @param action
   *     One of SafeModeAction.ENTER, SafeModeAction.LEAVE and
   *     SafeModeAction.GET
   * @param isChecked
   *     If true check only for Active NNs status, else check first NN's
   *     status
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(SafeModeAction,
   * boolean)
   */
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    return dfs.setSafeMode(action, isChecked);
  }

  /**
   * Refreshes the list of hosts and excluded hosts from the configured
   * files.
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  // We do not see a need for user to report block checksum errors and do not  
  // want to rely on user to report block corruptions.
  @Deprecated
  public boolean reportChecksumFailure(Path f, FSDataInputStream in, long inPos,
      FSDataInputStream sums, long sumsPos) {

    if (!(in instanceof HdfsDataInputStream &&
        sums instanceof HdfsDataInputStream)) {
      throw new IllegalArgumentException(
          "Input streams must be types of HdfsDataInputStream");
    }
    
    LocatedBlock lblocks[] = new LocatedBlock[2];

    // Find block in data stream.
    HdfsDataInputStream dfsIn = (HdfsDataInputStream) in;
    ExtendedBlock dataBlock = dfsIn.getCurrentBlock();
    if (dataBlock == null) {
      LOG.error("Error: Current block in data stream is null! ");
      return false;
    }
    DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()};
    lblocks[0] = new LocatedBlock(dataBlock, dataNode);
    LOG.info("Found checksum error in data stream at " + dataBlock +
        " on datanode=" + dataNode[0]);

    // Find block in checksum stream
    HdfsDataInputStream dfsSums = (HdfsDataInputStream) sums;
    ExtendedBlock sumsBlock = dfsSums.getCurrentBlock();
    if (sumsBlock == null) {
      LOG.error("Error: Current block in checksum stream is null! ");
      return false;
    }
    DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()};
    lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
    LOG.info("Found checksum error in checksum stream at " + sumsBlock +
        " on datanode=" + sumsNode[0]);

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   *
   * @throws FileNotFoundException
   *     if the file does not exist.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    HdfsFileStatus fi = dfs.getFileInfo(getPathName(f));
    if (fi != null) {
      return makeQualified(fi, f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getFileChecksum(getPathName(f));
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    statistics.incrementWriteOps(1);
    dfs.setPermission(getPathName(p), permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    statistics.incrementWriteOps(1);
    dfs.setOwner(getPathName(p), username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    statistics.incrementWriteOps(1);
    dfs.setTimes(getPathName(p), mtime, atime);
  }
  

  @Override
  protected int getDefaultPort() {
    return NameNode.DEFAULT_PORT;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    Token<DelegationTokenIdentifier> result =
        dfs.getDelegationToken(renewer == null ? null : new Text(renewer));
    return result;
  }

  /*
   * Delegation Token Operations
   * These are DFS only operations.
   */
  
  /**
   * Get a valid Delegation Token.
   *
   * @param renewer
   *     Name of the designated renewer for the token
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   * @deprecated use {@link #getDelegationToken(String)}
   */
  @Deprecated
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return getDelegationToken(renewer.toString());
  }
  
  /**
   * Renew an existing delegation token.
   *
   * @param token
   *     delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
      return token.renew(getConf());
    } catch (InterruptedException ie) {
      throw new RuntimeException("Caught interrupted", ie);
    }
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token
   *     delegation token
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    try {
      token.cancel(getConf());
    } catch (InterruptedException ie) {
      throw new RuntimeException("Caught interrupted", ie);
    }
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.balance.bandwidthPerSec.
   * The bandwidth parameter is the max number of bytes per second of network
   * bandwidth to be used by a datanode during balancing.
   *
   * @param bandwidth
   *     Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    dfs.setBalancerBandwidth(bandwidth);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   *
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return dfs.getCanonicalServiceName();
  }

  /**
   * Utility function that returns if the NameNode is in safemode or not. In HA
   * mode, this API will return only ActiveNN's safemode status.
   *
   * @return true if NameNode is in safemode, false otherwise.
   * @throws IOException
   *     when there is an issue communicating with the NameNode
   */
  public boolean isInSafeMode() throws IOException {
    return setSafeMode(SafeModeAction.SAFEMODE_GET, true);
  }

  /**
   * Get the erasure coding status of a file
   *
   * @param filePath
   *    the path of the file
   * @return
   *    the encoding status of the file
   * @throws IOException
   */
  public EncodingStatus getEncodingStatus(final String filePath)
      throws IOException {
    return dfs.getEncodingStatus(filePath);
  }

  /**
   * Request the encoding for the given file according to the given policy.
   * The encoding is executed asynchronously depending on the current utilization
   * of the cluster. NOTE: This requires the whole file to be rewritten in order
   * to ensure durability constraints for the encoded file. Clients reading the
   * file might fail while it is rewritten.
   *
   * @param filePath
   *    the path of the file
   * @param policy
   *    the erasure coding policy to be applied
   * @throws IOException
   */
  public void encodeFile(final String filePath, final EncodingPolicy policy)
      throws IOException {
    dfs.encodeFile(filePath, policy);
  }

  /**
   * Request undoing of the encoding of an erasure-coded file.
   * This sets the replication the the requested factor and deleted the parity
   * file after the required replication was achieved.
   *
   * @param filePath
   *    the path of the file
   * @param replication
   *    the replication factor to be applied after revoking the encoding
   * @throws IOException
   */
  public void revokeEncoding(final String filePath, final short replication)
      throws IOException {
    dfs.revokeEncoding(filePath, replication);
  }


  /**
   * Flush the users/groups cache in all Namenodes
   */
  public void flushCacheAll() throws IOException {
    flushCache(null, null);
  }

  /**
   * Flush all user related cache data in all Namenodes
   * @param user
   *    the user name
   * @throws IOException
   */
  public void flushCacheUser(String user) throws IOException {
    flushCache(user, null);
  }

  /**
   * Flush all group related cache data in all Namenodes
   * @param group
   * @throws IOException
   */
  public void flushCacheGroup(String group) throws IOException {
    flushCache(null, group);
  }

  /**
   * Flush all user and group related cache data in all Namenodes
   * @param user
   * @param group
   * @throws IOException
   */
  public void flushCache(String user, String group) throws IOException {
    dfs.flushCache(user, group);
  }

  public void enableMemcached() throws IOException {
    changeConf(DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED, String.valueOf(true));
  }

  public void disableMemcached() throws IOException {
    changeConf(DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED, String.valueOf(false));
  }

  public void enableSetPartitionKey() throws IOException {
    changeConf(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED,
        String.valueOf(true));
  }
  
  public void disableSetPartitionKey() throws IOException {
    changeConf(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED,
        String.valueOf(false));
  }

  private void changeConf(String prop, String value) throws IOException {
    dfs.changeConf(Arrays.asList(prop), Arrays.asList(value));
  }

  @Override
  public void access(Path path, final FsAction mode) throws IOException {
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        dfs.checkAccess(getPathName(p), mode);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.access(p, mode);
        return null;
      }
    }.resolve(this, absF);
  }

  /***
   * Get the last updated content summary. This method provides a faster yet
   * maybe stale content summary for quote enabled directory. To get
   * the most accurate result use {@link DistributedFileSystem#getContentSummary}
   * @param f path
   * @return
   * @throws IOException
   */
  public LastUpdatedContentSummary getLastUpdatedContentSummary(Path f) throws
      IOException {
    statistics.incrementReadOps(1);
    return dfs.getLastUpdatedContentSummary(getPathName(f));
  }
}