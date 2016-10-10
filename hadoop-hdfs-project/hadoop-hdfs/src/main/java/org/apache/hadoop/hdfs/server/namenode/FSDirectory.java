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
import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.resolvingcache.Cache;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.security.Users;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.security.AccessControlException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdfs.server.namenode.FSNamesystem.LOG;
import static org.apache.hadoop.util.Time.now;

/**
 * **********************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 * <p/>
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * <p/>
 * ***********************************************
 */
public class FSDirectory implements Closeable {

  private final FSNamesystem namesystem;
  private volatile boolean ready = false;
  public static final long UNKNOWN_DISK_SPACE = -1;
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit


  private boolean quotaEnabled;


  /**
   * Caches frequently used file names used in {@link INode} to reuse
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {

    this.quotaEnabled =
        conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_KEY,
            DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_DEFAULT);


    createRootInode(ns.createFsOwnerPermissions(new FsPermission((short) 0755)),
        false /*dont overwrite if root inode already existes*/);

    int configuredLimit = conf.getInt(DFSConfigKeys.DFS_LIST_LIMIT,
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit > 0 ? configuredLimit :
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    
    // filesystem limits
    this.maxComponentLength =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
            DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);

    int threshold =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
            DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG
        .info("Caching file names occuring more than " + threshold + " times");
    nameCache = new NameCache<ByteArray>(threshold);
    namesystem = ns;
  }

  private FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  private BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  /**
   * Notify that loading of this FSDirectory is complete, and
   * it is ready for use
   */
  void imageLoadComplete() {
    Preconditions.checkState(!ready, "FSDirectory already loaded");
    setReady();
  }

  void setReady() {
    if (ready) {
      return;
    }
    setReady(true);
    this.nameCache.initialized();
  }
  
  //This is for testing purposes only
  @VisibleForTesting
  boolean isReady() {
    return ready;
  }

  // exposed for unit tests
  protected void setReady(boolean flag) {
    ready = flag;
  }

  private void incrDeletedFileCount(int count) {
    if (getFSNamesystem() != null) {
      NameNode.getNameNodeMetrics().incrFilesDeleted(count);
    }
  }

  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {

  }

  /**
   * Add the given filename to the fs.
   *
   * @throws FileAlreadyExistsException
   * @throws QuotaExceededException
   * @throws UnresolvedLinkException
   */
  INodeFileUnderConstruction addFile(String path, PermissionStatus permissions,
      short replication, long preferredBlockSize, String clientName,
      String clientMachine, DatanodeDescriptor clientNode, long generationStamp)
      throws IOException {

    // Always do an implicit mkdirs for parent directory tree.
    long modTime = now();
    
    Path parent = new Path(path).getParent();
    if (parent == null) {
      // Trying to add "/" as a file - this path has no
      // parent -- avoids an NPE below.
      return null;
    }
    
    if (!mkdirs(parent.toString(), permissions, true, modTime)) {
      return null;
    }
    INodeFileUnderConstruction newNode =
        new INodeFileUnderConstruction(permissions, replication,
            preferredBlockSize, modTime, clientName, clientMachine, clientNode);


    newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE);

    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " + path);
      return null;
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + path + " is added");
    }
    return newNode;
  }

  INode unprotectedAddFile(String path, PermissionStatus permissions,
      short replication, long modificationTime, long atime,
      long preferredBlockSize, boolean underConstruction, String clientName,
      String clientMachine) throws IOException {
    INode newNode;
    if (underConstruction) {
      newNode = new INodeFileUnderConstruction(permissions, replication,
          preferredBlockSize, modificationTime, clientName, clientMachine,
          null);
    } else {
      newNode = new INodeFile(permissions, BlockInfo.EMPTY_ARRAY, replication,
          modificationTime, atime, preferredBlockSize);
    }

    try {
      newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE);
    } catch (IOException e) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedAddFile: exception when add " + path +
                " to the file system", e);
      }
      return null;
    }
    return newNode;
  }

  INodeDirectory addToParent(byte[] src, INodeDirectory parentINode,
      INode newNode, boolean propagateModTime)
      throws IOException {
    // NOTE: This does not update space counts for parents
    INodeDirectory newParent = null;
    try {
      newParent =
          getRootDir().addToParent(src, newNode, parentINode, propagateModTime);
      cacheName(newNode);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (newParent == null) {
      return null;
    }
    if (!newNode.isDirectory() && !newNode.isSymlink()) {
      // Add file->block mapping
      INodeFile newF = (INodeFile) newNode;
      BlockInfo[] blocks = newF.getBlocks();
      for (int i = 0; i < blocks.length; i++) {
        newF.setBlock(i, getBlockManager().addBlockCollection(blocks[i], newF));
      }
    }
    return newParent;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfo addBlock(String path, INode[] inodes, Block block,
      DatanodeDescriptor targets[])
      throws QuotaExceededException, StorageException,
      TransactionContextException {
    assert inodes[inodes.length - 1]
        .isUnderConstruction() : "INode should correspond to a file under construction";
    INodeFileUnderConstruction fileINode =
        (INodeFileUnderConstruction) inodes[inodes.length - 1];

    // check quota limits and updated space consumed
    updateCount(inodes, inodes.length - 1, 0,
        fileINode.getPreferredBlockSize() * fileINode.getBlockReplication(),
        true);

    // associate new last block for the file
    BlockInfoUnderConstruction blockInfo =
        new BlockInfoUnderConstruction(block, fileINode.getId(),
            BlockUCState.UNDER_CONSTRUCTION, targets);
    getBlockManager().addBlockCollection(blockInfo, fileINode);
    fileINode.addBlock(blockInfo);
    fileINode.setHasBlocks(true);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.addBlock: " + path + " with " + block +
              " block is added to the in-memory " + "file system");
    }
    return blockInfo;
  }

  /**
   * Persist the block list for the inode.
   */
  void persistBlocks(String path, INodeFileUnderConstruction file)
      throws StorageException, TransactionContextException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.persistBlocks: " + path + " with " +
              file.getBlocks().length +
              " blocks is persisted to the file system");
    }
  }
  
  /**
   * Close file.
   */
  void closeFile(String path, INodeFile file)
      throws StorageException, TransactionContextException {
    long now = now();
    // file is closed
    file.setModificationTimeForce(now);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.closeFile: " + path + " with " +
              file.getBlocks().length +
              " blocks is persisted to the file system");
    }
    file.logMetadataEvent(MetadataLogEntry.Operation.ADD);
  }

  /**
   * Remove a block from the file.
   */
  void removeBlock(String path, INodeFileUnderConstruction fileNode,
      Block block) throws IOException, StorageException {
    unprotectedRemoveBlock(path, fileNode, block);
  }
  
  void unprotectedRemoveBlock(String path, INodeFileUnderConstruction fileNode,
      Block block) throws IOException, StorageException {
    // modify file-> block and blocksMap
    fileNode.removeLastBlock(block);
    getBlockManager().removeBlockFromMap(block);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.removeBlock: " + path + " with " + block +
              " block is removed from the file system");
    }

    // update space consumed
    INode[] pathINodes = getExistingPathINodes(path);
    updateCount(pathINodes, pathINodes.length - 1, 0,
        -fileNode.getPreferredBlockSize() * fileNode.getBlockReplication(),
        true);
  }

  /**
   * @see #unprotectedRenameTo(String, String, long)
   * @deprecated Use {@link #renameTo(String, String, Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    long now = now();
    if (!unprotectedRenameTo(src, dst, now)) {
      return false;
    }
    return true;
  }

  @Deprecated
  boolean renameTo(String src, String dst, long srcNsCount, long srcDsCount,
      long dstNsCount, long dstDsCount)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    return unprotectedRenameTo(src, dst, now(), srcNsCount, srcDsCount,
        dstNsCount, dstDsCount);
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException, StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    long now = now();
    if (unprotectedRenameTo(src, dst, now, options)) {
      incrDeletedFileCount(1);
    }
  }

  void renameTo(String src, String dst, long srcNsCount, long srcDsCount,
      long dstNsCount, long dstDsCount, Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException, StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    if (unprotectedRenameTo(src, dst, now(), srcNsCount, srcDsCount, dstNsCount,
        dstDsCount, options)) {
      incrDeletedFileCount(1);
    }
  }

  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      long srcNsCount, long srcDsCount, long dstNsCount, long dstDsCount,
      Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException, StorageException {
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    String error = null;
    final INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
    final INode srcInode = srcInodes[srcInodes.length - 1];
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (srcInodes.length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }

    if (srcInode.isSymlink() &&
        dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink " + src + " to its target " + dst);
    }
    final byte[][] dstComponents = INode.getPathComponents(dst);
    final INode[] dstInodes = new INode[dstComponents.length];
    getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInodes.length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    if (dstInode != null) { // Destination exists
      // It's OK to rename a file to a symlink and vice versa
      if (dstInode.isDirectory() != srcInode.isDirectory()) {
        error = "Source " + src + " and destination " + dst +
            " must both be directories";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
      if (!overwrite) { // If destination exists, overwrite flag must be true
        error = "rename destination " + dst + " already exists";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new FileAlreadyExistsException(error);
      }

      if (dstInode.isDirectory()) {
        //[S] this a hack. handle this in the acquire lock phase
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);

        Short depth = dstInode.myDepth();
        boolean areChildrenRandomlyPartitioned = INode.isTreeLevelRandomPartitioned((short) (depth+1));
        if (ida.hasChildren(dstInode.getId(), areChildrenRandomlyPartitioned)) {
          error =
                  "rename cannot overwrite non empty destination directory " + dst;
          NameNode.stateChangeLog
                  .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
          throw new IOException(error);
        }
      }
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (!dstInodes[dstInodes.length - 2].isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new ParentNotDirectoryException(error);
    }

    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes, srcNsCount, srcDsCount,
        dstNsCount, dstDsCount);
    INode removedSrc =
        removeChildForRename(srcInodes, srcInodes.length - 1, srcNsCount,
            srcDsCount);
    if (removedSrc == null) {
      error = "Failed to rename " + src + " to " + dst +
          " because the source can not be removed";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }

    INode srcClone = cloneINode(removedSrc);

    final String srcChildName = removedSrc.getLocalName();
    String dstChildName = null;
    INode removedDst = null;
    try {
      if (dstInode != null) { // dst exists remove it
        removedDst = removeChild(dstInodes, dstInodes.length - 1,dstNsCount,
                dstDsCount);
        dstChildName = removedDst.getLocalName();
      }

      INode dstChild = null;
      removedSrc.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);
      // add src as dst to complete rename
      dstChild =
          addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedSrc,
              srcNsCount, srcDsCount);

      int filesDeleted = 0;
      if (dstChild != null) {
        removedSrc = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        // update moved lease with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        // Collect the blocks and remove the lease for previous dst
        if (removedDst != null) {
          INode rmdst = removedDst;
          removedDst = null;
          List<Block> collectedBlocks = new ArrayList<Block>();
          filesDeleted = 1; // rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
                            // [S] as the dst dir was empty it will always return 1
          getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
        }

        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            dstChild);

        return filesDeleted > 0;
      }
    } finally {
      if (removedSrc != null) {
        // Rename failed - restore src
        removedSrc.setLocalNameNoPersistance(srcChildName);
        addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, removedSrc,
            srcNsCount, srcDsCount);
      }
      if (removedDst != null) {
        // Rename failed - restore dst
        removedDst.setLocalNameNoPersistance(dstChildName);
        addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedDst,
            dstNsCount, dstDsCount);
      }
    }
    NameNode.stateChangeLog.warn(
        "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
            " to " + dst);
    throw new IOException("rename from " + src + " to " + dst + " failed.");
  }

  /**
   * Change a path name
   *
   * @param src
   *     source path
   * @param dst
   *     destination path
   * @return true if rename succeeds; false otherwise
   * @throws QuotaExceededException
   *     if the operation violates any quota limit
   * @throws FileAlreadyExistsException
   *     if the src is a symlink that points to dst
   * @deprecated See {@link #renameTo(String, String)}
   */
  @Deprecated
  boolean unprotectedRenameTo(String src, String dst, long timestamp)
      throws IOException {
    INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
    INode srcInode = srcInodes[srcInodes.length - 1];
    
    // check the validation of the source
    if (srcInode == null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source does not exist");
      return false;
    }
    if (srcInodes.length == 1) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source is the root");
      return false;
    }
    if (isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }
    
    // check the validity of the destination
    if (dst.equals(src)) {
      return true;
    }
    if (srcInode.isSymlink() &&
        dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink " + src + " to its target " + dst);
    }
    
    // dst cannot be directory or a file under src
    if (dst.startsWith(src) &&
        dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because destination starts with src");
      return false;
    }
    
    byte[][] dstComponents = INode.getPathComponents(dst);
    LOG.debug("destination is " + dst);
    INode[] dstInodes = new INode[dstComponents.length];
    getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
    if (dstInodes[dstInodes.length - 1] != null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst +
              " because destination exists");
      return false;
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst +
              " because destination's parent does not exist");
      return false;
    }
    
    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes);
    
    INode dstChild = null;
    INode srcChild = null;
    String srcChildName = null;
    try {
      // remove src
      srcChild = removeChildForRename(srcInodes, srcInodes.length - 1);
      if (srcChild == null) {
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " +
                src + " to " + dst + " because the source can not be removed");
        return false;
      }
      

      INode srcClone = cloneINode(srcChild);

      srcChildName = srcChild.getLocalName();
      srcChild.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);
      
      // add src to the destination
      dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, srcChild,
          UNKNOWN_DISK_SPACE);
      if (dstChild != null) {
        srcChild = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        // update moved leases with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            dstChild);

        return true;
      }
    } finally {
      if (dstChild == null && srcChild != null) {
        // put it back
        srcChild.setLocalNameNoPersistance(srcChildName);
        addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild,
            UNKNOWN_DISK_SPACE);
      }
    }
    NameNode.stateChangeLog.warn(
        "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
            " to " + dst);
    return false;
  }

  @Deprecated
  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      long srcNsCount, long srcDsCount, long dstNsCount, long dstDsCount)
      throws IOException {
    INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
    INode srcInode = srcInodes[srcInodes.length - 1];

    // check the validation of the source
    if (srcInode == null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source does not exist");
      return false;
    }
    if (srcInodes.length == 1) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source is the root");
      return false;
    }
    if (isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }

    if (srcInode.isSymlink() &&
        dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink " + src + " to its target " + dst);
    }

    byte[][] dstComponents = INode.getPathComponents(dst);
    LOG.debug("destination is " + dst);
    INode[] dstInodes = new INode[dstComponents.length];
    getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
    if (dstInodes[dstInodes.length - 1] != null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst +
              " because destination exists");
      return false;
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst +
              " because destination's parent does not exist");
      return false;
    }

    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes, srcNsCount, srcDsCount,
        dstNsCount, dstDsCount);

    INode dstChild = null;
    INode srcChild = null;
    String srcChildName = null;
    try {
      // remove src
      srcChild =
          removeChildForRename(srcInodes, srcInodes.length - 1, srcNsCount,
              srcDsCount);
      if (srcChild == null) {
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " +
                src + " to " + dst + " because the source can not be removed");
        return false;
      }


      INode srcClone = cloneINode(srcChild);

      srcChildName = srcChild.getLocalName();
      srcChild.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);

      // add src to the destination
      dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, srcChild,
          srcNsCount, srcDsCount);
      if (dstChild != null) {
        srcChild = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        // update moved leases with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);


        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            dstChild);

        return true;
      }
    } finally {
      if (dstChild == null && srcChild != null) {
        // put it back
        srcChild.setLocalNameNoPersistance(srcChildName);
        addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild,
            srcNsCount, srcDsCount);
      }
    }
    NameNode.stateChangeLog.warn(
        "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
            " to " + dst);
    return false;
  }

  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics and exceptions.
   *
   * @param src
   *     source path
   * @param dst
   *     destination path
   * @param timestamp
   *     modification time
   * @param options
   *     Rename options
   */
  boolean unprotectedRenameTo(String src, String dst, long timestamp,
      Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException, StorageException {
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    String error = null;
    final INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
    final INode srcInode = srcInodes[srcInodes.length - 1];
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (srcInodes.length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source " + src + " and destination " + dst + " are the same");
    }
    if (srcInode.isSymlink() &&
        dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink " + src + " to its target " + dst);
    }
    // dst cannot be a directory or a file under src
    if (dst.startsWith(src) &&
        dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      error = "Rename destination " + dst +
          " is a directory or file under source " + src;
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    final byte[][] dstComponents = INode.getPathComponents(dst);
    final INode[] dstInodes = new INode[dstComponents.length];
    getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInodes.length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    if (dstInode != null) { // Destination exists
      // It's OK to rename a file to a symlink and vice versa
      if (dstInode.isDirectory() != srcInode.isDirectory()) {
        error = "Source " + src + " and destination " + dst +
            " must both be directories";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
      if (!overwrite) { // If destination exists, overwrite flag must be true
        error = "rename destination " + dst + " already exists";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new FileAlreadyExistsException(error);
      }
      List<INode> children =
          dstInode.isDirectory() ? ((INodeDirectory) dstInode).getChildren() :
              null;
      if (children != null && children.size() != 0) {
        error =
            "rename cannot overwrite non empty destination directory " + dst;
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (!dstInodes[dstInodes.length - 2].isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new ParentNotDirectoryException(error);
    }

    // Ensure dst has quota to accommodate rename
    verifyQuotaForRename(srcInodes, dstInodes);
    INode removedSrc = removeChildForRename(srcInodes, srcInodes.length - 1);
    if (removedSrc == null) {
      error = "Failed to rename " + src + " to " + dst +
          " because the source can not be removed";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }

    INode srcClone = cloneINode(removedSrc);

    final String srcChildName = removedSrc.getLocalName();
    String dstChildName = null;
    INode removedDst = null;
    try {
      if (dstInode != null) { // dst exists remove it
        removedDst = removeChild(dstInodes, dstInodes.length - 1);
        dstChildName = removedDst.getLocalName();
      }

      INode dstChild = null;
      removedSrc.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);
      // add src as dst to complete rename
      dstChild =
          addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedSrc,
              UNKNOWN_DISK_SPACE);

      int filesDeleted = 0;
      if (dstChild != null) {
        removedSrc = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        // update moved lease with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        // Collect the blocks and remove the lease for previous dst
        if (removedDst != null) {
          INode rmdst = removedDst;
          removedDst = null;
          List<Block> collectedBlocks = new ArrayList<Block>();
          filesDeleted = rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
          getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
        }

        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            dstChild);

        return filesDeleted > 0;
      }
    } finally {
      if (removedSrc != null) {
        // Rename failed - restore src
        removedSrc.setLocalNameNoPersistance(srcChildName);
        addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, removedSrc,
            UNKNOWN_DISK_SPACE);
      }
      if (removedDst != null) {
        // Rename failed - restore dst
        removedDst.setLocalNameNoPersistance(dstChildName);
        addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedDst,
            UNKNOWN_DISK_SPACE);
      }
    }
    NameNode.stateChangeLog.warn(
        "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
            " to " + dst);
    throw new IOException("rename from " + src + " to " + dst + " failed.");
  }

  /**
   * Set file replication
   *
   * @param src
   *     file name
   * @param replication
   *     new replication
   * @param oldReplication
   *     old replication - output parameter
   * @return array of file blocks
   * @throws QuotaExceededException
   */
  Block[] setReplication(String src, short replication, short[] oldReplication)
      throws QuotaExceededException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    Block[] fileBlocks = null;
    fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
    return fileBlocks;
  }

  Block[] unprotectedSetReplication(String src, short replication,
      short[] oldReplication)
      throws QuotaExceededException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    INode[] inodes = getRootDir().getExistingPathINodes(src, true);
    INode inode = inodes[inodes.length - 1];
    if (inode == null) {
      return null;
    }
    assert !inode.isSymlink();
    if (inode.isDirectory()) {
      return null;
    }
    INodeFile fileNode = (INodeFile) inode;
    final short oldRepl = fileNode.getBlockReplication();

    // check disk quota
    long dsDelta =
        (replication - oldRepl) * (fileNode.diskspaceConsumed() / oldRepl);
    updateCount(inodes, inodes.length - 1, 0, dsDelta, true);

    fileNode.setReplication(replication);

    if (oldReplication != null) {
      oldReplication[0] = oldRepl;
    }
    return fileNode.getBlocks();
  }

  /**
   * Get the blocksize of a file
   *
   * @param filename
   *     the filename
   * @return the number of bytes
   */
  long getPreferredBlockSize(String filename)
      throws UnresolvedLinkException, FileNotFoundException, IOException,
      StorageException {
    INode inode = getRootDir().getNode(filename, false);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + filename);
    }
    if (inode.isDirectory() || inode.isSymlink()) {
      throw new IOException("Getting block size of non-file: " + filename);
    }
    return ((INodeFile) inode).getPreferredBlockSize();
  }

  boolean exists(String src) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);
    INode inode = getRootDir().getNode(src, false);
    if (inode == null) {
      return false;
    }
    return inode.isDirectory() || inode.isSymlink() ? true :
        ((INodeFile) inode).getBlocks() != null;
  }

  void setPermission(String src, FsPermission permission)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    unprotectedSetPermission(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    INode inode = getRootDir().getNode(src, true);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    inode.setPermission(permissions);
  }

  void setOwner(String src, String username, String groupname)
      throws IOException {
    unprotectedSetOwner(src, username, groupname);
  }

  void unprotectedSetOwner(String src, String username, String groupname)
      throws IOException {
    INode inode = getRootDir().getNode(src, true);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    Users.addUserToGroup(username, groupname);

    if (username != null) {
      inode.setUser(username);
    }
    if (groupname != null) {
      inode.setGroup(groupname);
    }
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   */
  public void concat(String target, String[] srcs)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    long timestamp = now();
    unprotectedConcat(target, srcs, timestamp);
  }
  

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   *
   * @param target
   *     target file to move the blocks to
   * @param srcs
   *     list of file to move the blocks from
   *     Must be public because also called from EditLogs
   *     NOTE: - it does not update quota (not needed for concat)
   */
  public void unprotectedConcat(String target, String[] srcs, long timestamp)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to " + target);
    }
    // do the move
    
    INode[] trgINodes = getExistingPathINodes(target);
    INodeFile trgInode = (INodeFile) trgINodes[trgINodes.length - 1];
    INodeDirectory trgParent = (INodeDirectory) trgINodes[trgINodes.length - 2];
    
    INodeFile[] allSrcInodes = new INodeFile[srcs.length];
    int i = 0;
    int totalBlocks = 0;
    long concatSize = 0;
    for (String src : srcs) {
      INodeFile srcInode = (INodeFile) getINode(src);
      allSrcInodes[i++] = srcInode;
      totalBlocks += srcInode.numBlocks();
      concatSize += srcInode.getSize();
    }
    List<BlockInfo> oldBlks =
        trgInode.appendBlocks(allSrcInodes, totalBlocks); // copy the blocks
    trgInode.recomputeFileSize();
    //HOP now the blocks are added to the targed file. copy of the old block infos is returned for snapshot maintenance
    

    //params for updating the snapshots
    INodeCandidatePrimaryKey trg_param =
        new INodeCandidatePrimaryKey(trgInode.getId());
    List<INodeCandidatePrimaryKey> srcs_param =
        new ArrayList<INodeCandidatePrimaryKey>();
    for (int j = 0; j < allSrcInodes.length; j++) {
      srcs_param.add(new INodeCandidatePrimaryKey(allSrcInodes[j].getId()));
    }

    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for (INodeFile nodeToRemove : allSrcInodes) {
      if (nodeToRemove == null) {
        continue;
      }
      
      trgParent.removeChild(nodeToRemove);
      count++;
    }
    
    trgInode.setModificationTimeForce(timestamp);
    trgParent.setModificationTime(timestamp);
    // update quota on the parent directory ('count' files removed, 0 space)
    unprotectedUpdateCount(trgINodes, trgINodes.length - 1, -count, 0);
    

    EntityManager
        .snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.Concat,
            trg_param, srcs_param, oldBlks);

  }

  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param src
   *     Path of a directory to delete
   * @param collectedBlocks
   *     Blocks under the deleted directory
   * @return true on successful deletion; else false
   */
  boolean delete(String src, List<Block> collectedBlocks)
      throws UnresolvedLinkException, StorageException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    long now = now();
    int filesRemoved = unprotectedDelete(src, collectedBlocks, now);

    if (filesRemoved <= 0) {
      return false;
    }
    incrDeletedFileCount(filesRemoved);
    // Blocks will be deleted later by the caller of this method
    getFSNamesystem().removePathAndBlocks(src, null);

    return true;
  }
  
  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(String path)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    final INode inode = getRootDir().getNode(path, false);
    if (inode == null || !inode.isDirectory()) {
      //not found or not a directory
      return false;
    }
    return ((INodeDirectory) inode).getChildrenList().size() != 0;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
  * @param src
   *     a string representation of a path to an inode
   * @param mtime
   *     the time the inode is removed
   */
  void unprotectedDelete(String src, long mtime)
      throws UnresolvedLinkException, StorageException, IOException {
    List<Block> collectedBlocks = new ArrayList<Block>();
    int filesRemoved = unprotectedDelete(src, collectedBlocks, mtime);
    if (filesRemoved > 0) {
      getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
    }
  }
  
  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   *
   * @param src
   *     a string representation of a path to an inode
   * @param collectedBlocks
   *     blocks collected from the deleted path
   * @param mtime
   *     the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */
  int unprotectedDelete(String src, List<Block> collectedBlocks, long mtime)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);

    INode[] inodes = getRootDir().getExistingPathINodes(src, false);
    INode targetNode = inodes[inodes.length - 1];

    if (targetNode == null) { // non-existent src
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: " + "failed to remove " + src +
                " because it does not exist");
      }
      return 0;
    }
    if (inodes.length == 1) { // src is the root
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
          "failed to remove " + src +
          " because the root is not allowed to be deleted");
      return 0;
    }

    // Add metadata log entry for all deleted childred.
    addMetaDataLogForDirDeletion(targetNode);

    int pos = inodes.length - 1;
    // Remove the node from the namespace
    targetNode = removeChild(inodes, pos);
    if (targetNode == null) {
      return 0;
    }
    // set the parent's modification time
    inodes[pos - 1].setModificationTime(mtime);

    int filesRemoved = targetNode.collectSubtreeBlocksAndClear(collectedBlocks);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.unprotectedDelete: " + src + " is removed");
    }
    return filesRemoved;
  }

  private void addMetaDataLogForDirDeletion(INode targetNode) throws TransactionContextException, StorageException {
    if (targetNode.isDirectory()) {
      List<INode> children = ((INodeDirectory) targetNode).getChildren();
      for(INode child : children){
       if(child.isDirectory()){
         addMetaDataLogForDirDeletion(child);
       }else{
         child.logMetadataEvent(MetadataLogEntry.Operation.DELETE);
       }
      }
    }
    targetNode.logMetadataEvent(MetadataLogEntry.Operation.DELETE);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src
   *     the directory name
   * @param startAfter
   *     the name to start listing after
   * @param needLocation
   *     if block locations are returned
   * @return a partial listing starting after startAfter
   */
  DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation)
      throws UnresolvedLinkException, IOException, StorageException {
    String srcs = normalizePath(src);
    INode targetNode = getRootDir().getNode(srcs, true);
    if (targetNode == null) {
      return null;
    }

    if (!targetNode.isDirectory()) {
      return new DirectoryListing(new HdfsFileStatus[]{
          createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode,
              needLocation)}, 0);
    }
    INodeDirectory dirInode = (INodeDirectory) targetNode;
    List<INode> contents = dirInode.getChildrenList();
    int startChild = dirInode.nextChild(startAfter);
    int totalNumChildren = contents.size();
    int numOfListing = Math.min(totalNumChildren - startChild, this.lsLimit);
    HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      INode cur = contents.get(startChild + i);
      listing[i] = createFileStatus(cur.name, cur, needLocation);
    }
    return new DirectoryListing(listing,
        totalNumChildren - startChild - numOfListing);
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src
   *     The string representation of the path to the file
   * @param resolveLink
   *     whether to throw UnresolvedLinkException
   * @return object containing information regarding the file
   * or null if file not found
   */
  HdfsFileStatus getFileInfo(String src, boolean resolveLink)
      throws IOException {
    String srcs = normalizePath(src);
    INode targetNode = getRootDir().getNode(srcs, resolveLink);
    if (targetNode == null) {
      return null;
    } else {
      return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
    }
  }

  HdfsFileStatus getFileInfoForCreate(String src, boolean resolveLink)
      throws IOException {
    String srcs = normalizePath(src);
    INode targetNode = getRootDir().getNode(srcs, resolveLink);
    if (targetNode == null) {
      return null;
    } else {
      return createFileStatusForCreate(HdfsFileStatus.EMPTY_NAME, targetNode);
    }
  }

  /**
   * Get the blocks associated with the file.
   */
  Block[] getFileBlocks(String src)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    INode targetNode = getRootDir().getNode(src, false);
    if (targetNode == null) {
      return null;
    }
    if (targetNode.isDirectory()) {
      return null;
    }
    if (targetNode.isSymlink()) {
      return null;
    }
    return ((INodeFile) targetNode).getBlocks();
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  INode getINode(String src) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    return getRootDir().getNode(src, true);
  }

  /**
   * Retrieve the existing INodes along the given path.
   *
   * @param path
   *     the path to explore
   * @return INodes array containing the existing INodes in the order they
   * appear when following the path from the root INode to the
   * deepest INodes. The array size will be the number of expected
   * components in the path, and non existing components will be
   * filled with null
   */
  INode[] getExistingPathINodes(String path)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    return getRootDir().getExistingPathINodes(path, true);
  }
  
  /**
   * Get the parent node of path.
   *
   * @param path
   *     the path to explore
   * @return its parent node
   */
  INodeDirectory getParent(byte[][] path)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    return getRootDir().getParent(path);
  }
  
  /**
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    String srcs = normalizePath(src);
    if (srcs.startsWith("/") &&
        !srcs.endsWith("/") &&
        getRootDir().getNode(srcs, false) == null) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);
    INode node = getRootDir().getNode(src, false);
    return node != null && node.isDirectory();
  }

  /**
   * Updates namespace and diskspace consumed for all
   * directories until the parent directory of file represented by path.
   *
   * @param path
   *     path for the file.
   * @param nsDelta
   *     the delta change of namespace
   * @param dsDelta
   *     the delta change of diskspace
   * @throws QuotaExceededException
   *     if the new count violates any quota limit
  */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
      throws QuotaExceededException, FileNotFoundException,
      UnresolvedLinkException, StorageException, TransactionContextException {
    INode[] inodes = getRootDir().getExistingPathINodes(path, false);
    int len = inodes.length;
    if (inodes[len - 1] == null) {
      throw new FileNotFoundException(path + " does not exist under rootDir.");
    }
    updateCount(inodes, len - 1, nsDelta, dsDelta, true);
  }
  
  /**
   * update count of each inode with quota
   *
   * @param inodes
   *     an array of inodes on a path
   * @param numOfINodes
   *     the number of inodes to update starting from index 0
   * @param nsDelta
   *     the delta change of namespace
   * @param dsDelta
   *     the delta change of diskspace
   * @param checkQuota
   *     if true then check if quota is exceeded
   * @throws QuotaExceededException
   *     if the new count violates any quota limit
   */
  private void updateCount(INode[] inodes, int numOfINodes, long nsDelta,
      long dsDelta, boolean checkQuota)
      throws QuotaExceededException, StorageException,
      TransactionContextException {
    if (!isQuotaEnabled()) {
      return;
    }
    
    if (!ready) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes > inodes.length) {
      numOfINodes = inodes.length;
    }
    if (checkQuota) {
      verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
    }
    INode iNode = inodes[numOfINodes - 1];
    namesystem.getQuotaUpdateManager()
        .addUpdate(iNode.getId(), nsDelta, dsDelta);
  }
  
  /**
   * update quota of each inode and check to see if quota is exceeded.
   * See {@link #updateCount(INode[], int, long, long, boolean)}
   */
  private void updateCountNoQuotaCheck(INode[] inodes, int numOfINodes,
      long nsDelta, long dsDelta)
      throws StorageException, TransactionContextException {
    try {
      updateCount(inodes, numOfINodes, nsDelta, dsDelta, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.updateCountNoQuotaCheck - unexpected ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   *
   * @param inodes
   * @param numOfINodes
   * @param nsDelta
   * @param dsDelta
   */
  void unprotectedUpdateCount(INode[] inodes, int numOfINodes, long nsDelta,
      long dsDelta) throws StorageException, TransactionContextException {
    if (!isQuotaEnabled()) {
      return;
    }

    INode iNode = inodes[numOfINodes - 1];
    namesystem.getQuotaUpdateManager()
        .addUpdate(iNode.getId(), nsDelta, dsDelta);
  }
  
  /**
   * Return the name of the path represented by inodes at [0, pos]
   */
  private static String getFullPathName(INode[] inodes, int pos) {
    StringBuilder fullPathName = new StringBuilder();
    if (inodes[0].isRoot()) {
      if (pos == 0) {
        return Path.SEPARATOR;
      }
    } else {
      fullPathName.append(inodes[0].getLocalName());
    }
    
    for (int i = 1; i <= pos; i++) {
      fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
    }
    return fullPathName.toString();
  }

  /**
   * Return the full path name of the specified inode
   */
  static String getFullPathName(INode inode)
      throws StorageException, TransactionContextException {
    // calculate the depth of this inode from root
    int depth = 0;
    for (INode i = inode; i != null; i = i.getParent()) {
      depth++;
    }
    INode[] inodes = new INode[depth];

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      if (inode == null) {
        NameNode.stateChangeLog.warn("Could not get full path." +
            " Corresponding file might have deleted already.");
        return null;
      }
      inodes[depth - i - 1] = inode;
      inode = inode.getParent();
    }
    return getFullPathName(inodes, depth - 1);
  }
  
  /**
   * Create a directory
   * If ancestor directories do not exist, automatically create them.
   *
   * @param src
   *     string representation of the path to the directory
   * @param permissions
   *     the permission of the directory
   * @param now
   *     creation time
   * @return true if the operation succeeds false otherwise
   * @throws FileNotFoundException
   *     if an ancestor or itself is a file
   * @throws QuotaExceededException
   *     if directory creation violates
   *     any quota limit
   * @throws UnresolvedLinkException
   *     if a symlink is encountered in src.
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean inheritPermission, long now)
      throws IOException {
    src = normalizePath(src);
    String[] names = INode.getPathNames(src);
    byte[][] components = INode.getPathComponents(names);
    INode[] inodes = new INode[components.length];
    final int lastInodeIndex = inodes.length - 1;

    getRootDir().getExistingPathINodes(components, inodes, false);

    // find the index of the first null in inodes[]
    StringBuilder pathbuilder = new StringBuilder();
    int i = 1;
    for (; i < inodes.length && inodes[i] != null; i++) {
      pathbuilder.append(Path.SEPARATOR + names[i]);
      if (!inodes[i].isDirectory()) {
        throw new FileAlreadyExistsException(
            "Parent path is not a directory: " + pathbuilder + " " +
                inodes[i].getLocalName());
      }
    }

    // default to creating parent dirs with the given perms
    PermissionStatus parentPermissions = permissions;

    // if not inheriting and it's the last inode, there's no use in
    // computing perms that won't be used
    if (inheritPermission || (i < lastInodeIndex)) {
      // if inheriting (ie. creating a file or symlink), use the parent dir,
      // else the supplied permissions
      // NOTE: the permissions of the auto-created directories violate posix
      FsPermission parentFsPerm =
          inheritPermission ? inodes[i - 1].getFsPermission() :
              permissions.getPermission();

      // ensure that the permissions allow user write+execute
      if (!parentFsPerm.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
        parentFsPerm = new FsPermission(
            parentFsPerm.getUserAction().or(FsAction.WRITE_EXECUTE),
            parentFsPerm.getGroupAction(), parentFsPerm.getOtherAction());
      }

      if (!parentPermissions.getPermission().equals(parentFsPerm)) {
        parentPermissions =
            new PermissionStatus(parentPermissions.getUserName(),
                parentPermissions.getGroupName(), parentFsPerm);
        // when inheriting, use same perms for entire path
        if (inheritPermission) {
          permissions = parentPermissions;
        }
      }
    }

    // create directories beginning from the first null index
    for (; i < inodes.length; i++) {
      pathbuilder.append(Path.SEPARATOR + names[i]);
      String cur = pathbuilder.toString();
      unprotectedMkdir(inodes, i, components[i],
          (i < lastInodeIndex) ? parentPermissions : permissions, now);
      if (inodes[i] == null) {
        return false;
      }
      // Directory creation also count towards FilesCreated
      // to match count of FilesDeleted metric.
      if (getFSNamesystem() != null) {
        NameNode.getNameNodeMetrics().incrFilesCreated();
      }

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog
            .debug("DIR* FSDirectory.mkdirs: created directory " + cur);
      }
    }
    return true;
  }

  /**
   */
  INode unprotectedMkdir(String src, PermissionStatus permissions,
      long timestamp)
      throws IOException {
    byte[][] components = INode.getPathComponents(src);
    INode[] inodes = new INode[components.length];

    getRootDir().getExistingPathINodes(components, inodes, false);
    unprotectedMkdir(inodes, inodes.length - 1, components[inodes.length - 1],
        permissions, timestamp);
    return inodes[inodes.length - 1];
  }

  /**
   * create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(INode[] inodes, int pos, byte[] name,
      PermissionStatus permission, long timestamp)
      throws IOException {
    inodes[pos] =
        addChild(inodes, pos, new INodeDirectory(name, permission, timestamp),
            -1);
  }
  
  /**
   * Add a node child to the namespace. The full path name of the node is src.
   * childDiskspace should be -1, if unknown.
   * QuotaExceededException is thrown if it violates quota limit
   */
  private <T extends INode> T addNode(String src, T child, long childDiskspace)
      throws IOException {
    byte[][] components = INode.getPathComponents(src);
    byte[] path = components[components.length - 1];
    child.setLocalNameNoPersistance(path);
    cacheName(child);
    INode[] inodes = new INode[components.length];
    getRootDir().getExistingPathINodes(components, inodes, false);
    return addChild(inodes, inodes.length - 1, child, childDiskspace);
  }

  /**
   * Verify quota for adding or moving a new INode with required
   * namespace and diskspace to a given position.
   *
   * @param inodes
   *     INodes corresponding to a path
   * @param pos
   *     position where a new INode will be added
   * @param nsDelta
   *     needed namespace
   * @param dsDelta
   *     needed diskspace
   * @param commonAncestor
   *     Last node in inodes array that is a common ancestor
   *     for a INode that is being moved from one location to the other.
   *     Pass null if a node is not being moved.
   * @throws QuotaExceededException
   *     if quota limit is exceeded.
   */
  private void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta,
      INode commonAncestor) throws QuotaExceededException, StorageException,
      TransactionContextException {
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    if (nsDelta <= 0 && dsDelta <= 0) {
      // if quota is being freed or not being consumed
      return;
    }
    if (pos > inodes.length) {
      pos = inodes.length;
    }
    int i = pos - 1;
    try {
      // check existing components in the path  
      for (; i >= 0; i--) {
        if (commonAncestor == inodes[i]) {
          // Moving an existing node. Stop checking for quota when common
          // ancestor is reached
          return;
        }
        if (inodes[i].isQuotaSet()) { // a directory with quota
          INodeDirectoryWithQuota node = (INodeDirectoryWithQuota) inodes[i];
          node.verifyQuota(nsDelta, dsDelta);
        }
      }
    } catch (QuotaExceededException e) {
      e.setPathName(getFullPathName(inodes, i));
      throw e;
    }
  }
  
  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1]
   * moves
   * dstInodes[dstInodes.length-1]
   *
   * @param srcInodes
   *     directory from where node is being moved.
   * @param dstInodes
   *     directory to where node is moved to.
   * @throws QuotaExceededException
   *     if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes)
      throws QuotaExceededException, StorageException,
      TransactionContextException {

    if (!isQuotaEnabled()) {
      return;    //HOP
    }
    
    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode srcInode = srcInodes[srcInodes.length - 1];
    INode commonAncestor = null;
    for (int i = 0; srcInodes[i] == dstInodes[i]; i++) {
      commonAncestor = srcInodes[i];
    }
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcInode.spaceConsumedInTree(srcCounts);
    long nsDelta = srcCounts.getNsCount();
    long dsDelta = srcCounts.getDsCount();
    
    // Reduce the required quota by dst that is being removed
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInode != null) {
      INode.DirCounts dstCounts = new INode.DirCounts();
      dstInode.spaceConsumedInTree(dstCounts);
      nsDelta -= dstCounts.getNsCount();
      dsDelta -= dstCounts.getDsCount();
    }
    verifyQuota(dstInodes, dstInodes.length - 1, nsDelta, dsDelta,
        commonAncestor);
  }
  
  private void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes,
      long srcNsCount, long srcDsCount, long dstNsCount, long dstDsCount)
      throws QuotaExceededException, StorageException,
      TransactionContextException {

    if (!isQuotaEnabled()) {
      return;
    }

    if (!ready) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode commonAncestor = null;
    for (int i = 0; srcInodes[i] == dstInodes[i]; i++) {
      commonAncestor = srcInodes[i];
    }

    // Reduce the required quota by dst that is being removed
    INode dstInode = dstInodes[dstInodes.length - 1];
    long nsDelta = srcNsCount;
    long dsDelta = srcDsCount;
    if (dstInode != null) {
      nsDelta -= dstNsCount;
      dsDelta -= dstDsCount;
    }
    verifyQuota(dstInodes, dstInodes.length - 1, nsDelta, dsDelta,
        commonAncestor);
  }
  
  /**
   * Verify that filesystem limit constraints are not violated
   *
   * @throws PathComponentTooLongException
   *     child's name is too long
   * @throws MaxDirectoryItemsExceededException
   *     items per directory is exceeded
   */
  protected <T extends INode> void verifyFsLimits(INode[] pathComponents,
      int pos, T child)
      throws FSLimitException, StorageException, TransactionContextException {
    boolean includeChildName = false;
    try {
      if (maxComponentLength != 0) {
        int length = child.getLocalName().length();
        if (length > maxComponentLength) {
          includeChildName = true;
          throw new PathComponentTooLongException(maxComponentLength, length);
        }
      }
      if (maxDirItems != 0) {
        INodeDirectory parent = (INodeDirectory) pathComponents[pos - 1];
        int count = parent.getChildrenList().size();
        if (count >= maxDirItems) {
          throw new MaxDirectoryItemsExceededException(maxDirItems, count);
        }
      }
    } catch (FSLimitException e) {
      String badPath = getFullPathName(pathComponents, pos - 1);
      if (includeChildName) {
        badPath += Path.SEPARATOR + child.getLocalName();
      }
      e.setPathName(badPath);
      // Do not throw if edits log is still being processed
      if (ready) {
        throw (e);
      }
      // log pre-existing paths that exceed limits
      NameNode.LOG
          .error("FSDirectory.verifyFsLimits - " + e.getLocalizedMessage());
    }
  }
  
  /**
   * Add a node child to the inodes at index pos.
   * Its ancestors are stored at [0, pos-1].
   * QuotaExceededException is thrown if it violates quota limit
   */
  private <T extends INode> T addChild(INode[] pathComponents, int pos, T child,
      long childDiskspace, boolean checkQuota)
      throws IOException {
    // The filesystem limits are not really quotas, so this check may appear
    // odd.  It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyFsLimits(pathComponents, pos, child);
    }
    
    INode.DirCounts counts = new INode.DirCounts();
    if (isQuotaEnabled()) {       //HOP
      child.spaceConsumedInTree(counts);
    }
    if (childDiskspace < 0) {
      childDiskspace = counts.getDsCount();
    }

    updateCount(pathComponents, pos, counts.getNsCount(), childDiskspace,
        checkQuota);
    if (pathComponents[pos - 1] == null) {
      throw new NullPointerException("Panic: parent does not exist");
    }
    T addedNode =
        ((INodeDirectory) pathComponents[pos - 1]).addChild(child, true);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -counts.getNsCount(), -childDiskspace,
          true);
    }

    if (addedNode != null) {
      if (!addedNode.isDirectory()) {
        INode[] pc = Arrays.copyOf(pathComponents, pathComponents.length);
        pc[pc.length - 1] = addedNode;
        String path = getFullPathName(pc, pc.length - 1);
        Cache.getInstance().set(path, pc);
      }
    }
    //
    return addedNode;
  }

  private <T extends INode> T addChild(INode[] pathComponents, int pos, T child,
      long childNamespace, long childDiskspace, boolean checkQuota)
      throws IOException {
    // The filesystem limits are not really quotas, so this check may appear
    // odd.  It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyFsLimits(pathComponents, pos, child);
    }

    updateCount(pathComponents, pos, childNamespace, childDiskspace,
        checkQuota);
    if (pathComponents[pos - 1] == null) {
      throw new NullPointerException("Panic: parent does not exist");
    }
    T addedNode =
        ((INodeDirectory) pathComponents[pos - 1]).addChild(child, true);
    if (addedNode == null) {
      updateCount(pathComponents, pos, -childNamespace, -childDiskspace, true);
    }

    if (addedNode != null) {
      if (!addedNode.isDirectory()) {
        INode[] pc = Arrays.copyOf(pathComponents, pathComponents.length);
        pc[pc.length - 1] = addedNode;
        String path = getFullPathName(pc, pc.length - 1);
        Cache.getInstance().set(path, pc);
      }
    }
    //
    return addedNode;
  }

  private <T extends INode> T addChild(INode[] pathComponents, int pos, T child,
      long childDiskspace) throws IOException {
    return addChild(pathComponents, pos, child, childDiskspace, true);
  }
  
  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int pos, T child, long childDiskspace)
      throws IOException {
    T inode = null;
    try {
      inode = addChild(pathComponents, pos, child, childDiskspace, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return inode;
  }
  
  private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
      int pos, T child, long childNamespace, long childDiskspace)
      throws IOException {
    T inode = null;
    try {
      inode =
          addChild(pathComponents, pos, child, childNamespace, childDiskspace,
              false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return inode;
  }
  
  /**
   * Remove an inode at index pos from the namespace.
   * Its ancestors are stored at [0, pos-1].
   * Count of each ancestor with quota is also updated.
   * Return the removed node; null if the removal fails.
   */
  INode removeChild(INode[] pathComponents, int pos, boolean forRename)
      throws StorageException, TransactionContextException {
    INode removedNode = null;
    INode.DirCounts counts = new INode.DirCounts();
    if (forRename) {
      removedNode = pathComponents[pos];
      removedNode.logMetadataEvent(MetadataLogEntry.Operation.DELETE);
    } else {
      if(isQuotaEnabled()){
        INode nodeToBeRemored = pathComponents[pos];
        nodeToBeRemored.spaceConsumedInTree(counts);
      }
      removedNode = ((INodeDirectory) pathComponents[pos - 1])
          .removeChild(pathComponents[pos]);
    }
    if (removedNode != null && isQuotaEnabled()) {
      List<QuotaUpdate> outstandingUpdates = (List<QuotaUpdate>) EntityManager
          .findList(QuotaUpdate.Finder.ByINodeId, removedNode.getId());
      long nsDelta = 0;
      long dsDelta = 0;
      for (QuotaUpdate update : outstandingUpdates) {
        nsDelta += update.getNamespaceDelta();
        dsDelta += update.getDiskspaceDelta();
      }
      updateCountNoQuotaCheck(pathComponents, pos,
          -counts.getNsCount() + nsDelta, -counts.getDsCount() + dsDelta);
    }
    return removedNode;
  }
  
  INode removeChild(INode[] pathComponents, int pos, boolean forRename, 
          final long nsCount, final long dsCount)
      throws StorageException, TransactionContextException {
    INode removedNode = null;
    if (forRename) {
      removedNode = pathComponents[pos];
      removedNode.logMetadataEvent(MetadataLogEntry.Operation.DELETE);
    } else {
      removedNode = ((INodeDirectory) pathComponents[pos - 1])
          .removeChild(pathComponents[pos]);
    }
    if (removedNode != null && isQuotaEnabled()) {
      List<QuotaUpdate> outstandingUpdates = (List<QuotaUpdate>) EntityManager
          .findList(QuotaUpdate.Finder.ByINodeId, removedNode.getId());
      long nsDelta = 0;
      long dsDelta = 0;
      for (QuotaUpdate update : outstandingUpdates) {
        nsDelta += update.getNamespaceDelta();
        dsDelta += update.getDiskspaceDelta();
      }
      //INode.DirCounts counts = new INode.DirCounts();
      //removedNode.spaceConsumedInTree(counts);
      updateCountNoQuotaCheck(pathComponents, pos,
          -nsCount + nsDelta, -dsCount + dsDelta);
    }
    return removedNode;
  }

  INode removeChildNonRecursively(INode[] pathComponents, int pos)
      throws StorageException, TransactionContextException {
    INode removedNode = ((INodeDirectory) pathComponents[pos - 1])
        .removeChild(pathComponents[pos]);
    if (removedNode != null && isQuotaEnabled()) {
      List<QuotaUpdate> outstandingUpdates = (List<QuotaUpdate>) EntityManager
          .findList(QuotaUpdate.Finder.ByINodeId, removedNode.getId());
      long nsDelta = 0;
      long dsDelta = 0;
      for (QuotaUpdate update : outstandingUpdates) {
        nsDelta += update.getNamespaceDelta();
        dsDelta += update.getDiskspaceDelta();
      }
      if (removedNode.isDirectory()) {
        updateCountNoQuotaCheck(pathComponents, pos, -1 + nsDelta, dsDelta);
      } else {
        INode.DirCounts counts = new INode.DirCounts();
        removedNode.spaceConsumedInTree(counts);
        updateCountNoQuotaCheck(pathComponents, pos,
            -counts.getNsCount() + nsDelta, -counts.getDsCount() + dsDelta);
      }
    }
    return removedNode;
  }
  
  private INode removeChild(INode[] pathComponents, int pos)
      throws StorageException, TransactionContextException {
    return removeChild(pathComponents, pos, false);
  }
  
  private INode removeChild(INode[] pathComponents, int pos, final long nsCount,
          final long dsCount)
      throws StorageException, TransactionContextException {
    return removeChild(pathComponents, pos, false,nsCount,dsCount);
  }
  

  private INode removeChildForRename(INode[] pathComponents, int pos)
      throws StorageException, TransactionContextException {
    return removeChild(pathComponents, pos, true);
  }

  private INode removeChildForRename(INode[] pathComponents, int pos,
      long nsCount, long dsCount)
      throws StorageException, TransactionContextException {
    return removeChild(pathComponents, pos, true, nsCount, dsCount);
  }
  
  /**
   */
  String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  ContentSummary getContentSummary(String src)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    String srcs = normalizePath(src);
    INode targetNode = getRootDir().getNode(srcs, false);
    if (targetNode == null) {
      throw new FileNotFoundException("File does not exist: " + srcs);
    } else {
      return targetNode.computeContentSummary();
    }
  }

  /**
   * Update the count of each directory with quota in the namespace
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   * <p/>
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  void updateCountForINodeWithQuota()
      throws StorageException, TransactionContextException {
    // HOP If this one is used for something then we need to modify it to use the QuotaUpdateManager
    updateCountForINodeWithQuota(getRootDir(), new INode.DirCounts(),
        new ArrayList<INode>(50));
  }
  
  /**
   * Update the count of the directory if it has a quota and return the count
   * <p/>
   * This does not throw a QuotaExceededException. This is just an update
   * of of existing state and throwing QuotaExceededException does not help
   * with fixing the state, if there is a problem.
   *
   * @param dir
   *     the root of the tree that represents the directory
   * @param counts
   *     counters for name space and disk space
   * @param nodesInPath
   *     INodes for the each of components in the path.
   */
  private static void updateCountForINodeWithQuota(INodeDirectory dir,
      INode.DirCounts counts, ArrayList<INode> nodesInPath)
      throws StorageException, TransactionContextException {
    long parentNamespace = counts.nsCount;
    long parentDiskspace = counts.dsCount;
    
    counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
    counts.dsCount = 0L;
    
    /* We don't need nodesInPath if we could use 'parent' field in 
     * INode. using 'parent' is not currently recommended. */
    nodesInPath.add(dir);

    for (INode child : dir.getChildrenList()) {
      if (child.isDirectory()) {
        updateCountForINodeWithQuota((INodeDirectory) child, counts,
            nodesInPath);
      } else if (child.isSymlink()) {
        counts.nsCount += 1;
      } else { // reduce recursive calls
        counts.nsCount += 1;
        counts.dsCount += ((INodeFile) child).diskspaceConsumed();
      }
    }

    if (dir.isQuotaSet()) {
      ((INodeDirectoryWithQuota) dir)
          .setSpaceConsumed(counts.nsCount, counts.dsCount);

      // check if quota is violated for some reason.
      if ((dir.getNsQuota() >= 0 && counts.nsCount > dir.getNsQuota()) ||
          (dir.getDsQuota() >= 0 && counts.dsCount > dir.getDsQuota())) {

        // can only happen because of a software bug. the bug should be fixed.
        StringBuilder path = new StringBuilder(512);
        for (INode n : nodesInPath) {
          path.append('/');
          path.append(n.getLocalName());
        }
        
        NameNode.LOG.warn("Quota violation in image for " + path +
            " (Namespace quota : " + dir.getNsQuota() +
            " consumed : " + counts.nsCount + ")" +
            " (Diskspace quota : " + dir.getDsQuota() +
            " consumed : " + counts.dsCount + ").");
      }
    }

    // pop 
    nodesInPath.remove(nodesInPath.size() - 1);
    
    counts.nsCount += parentNamespace;
    counts.dsCount += parentDiskspace;
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   *
   * @throws FileNotFoundException
   *     if the path does not exist or is a file
   * @throws QuotaExceededException
   *     if the directory tree size is
   *     greater than the given quota
   * @throws UnresolvedLinkException
   *     if a symlink is encountered in src.
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
      throws IOException {
    if (!isQuotaEnabled()) {
      return null;    //HOP
    }
    
    // sanity check
    if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET &&
        nsQuota < HdfsConstants.QUOTA_RESET) ||
        (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET &&
            dsQuota < HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
          "dsQuota : " + nsQuota + " and " +
          dsQuota);
    }
    
    String srcs = normalizePath(src);

    INode[] inodes = getRootDir().getExistingPathINodes(src, true);
    INode targetNode = inodes[inodes.length - 1];
    if (targetNode == null) {
      throw new FileNotFoundException("Directory does not exist: " + srcs);
    } else if (!targetNode.isDirectory()) {
      throw new FileNotFoundException(srcs + ": Is not a directory");
    } else if (targetNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException(
          "Cannot clear namespace quota on root.");
    } else { // a directory inode
      INodeDirectory dirNode = (INodeDirectory) targetNode;
      long oldNsQuota = dirNode.getNsQuota();
      long oldDsQuota = dirNode.getDsQuota();
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }

      if (dirNode instanceof INodeDirectoryWithQuota) {
        // a directory with quota; so set the quota to the new value
        ((INodeDirectoryWithQuota) dirNode).setQuota(nsQuota, dsQuota);
        if (!dirNode.isQuotaSet()) {
          // will not come here for root because root's nsQuota is always set
          INodeDirectory newNode = new INodeDirectory(dirNode);
          INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
          dirNode = newNode;
          parent.replaceChild(newNode);
        }
      } else {
        // a non-quota directory; so replace it with a directory with quota
        INodeDirectoryWithQuota newNode =
            new INodeDirectoryWithQuota(nsQuota, dsQuota, dirNode);
        // non-root directory node; parent != null
        INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
        dirNode = newNode;
        parent.replaceChild(newNode);
      }
      return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
    }
  }
  
  /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the
   * contract.
   *
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota)
      throws IOException {
    INodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
    if (dir != null) {
      // Some audit log code is missing here
    }
  }

  void setQuota(String src, long nsQuota, long dsQuota, long nsCount,
      long dsCount) throws IOException {
    INodeDirectory dir =
        unprotectedSetQuota(src, nsQuota, dsQuota, nsCount, dsCount);
    if (dir != null) {
      // Some audit log code is missing here
    }
  }

  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota,
      long nsCount, long dsCount)
      throws IOException {
    if (!isQuotaEnabled()) {
      return null;
    }

    // sanity check
    if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET &&
        nsQuota < HdfsConstants.QUOTA_RESET) ||
        (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET &&
            dsQuota < HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
          "dsQuota : " + nsQuota + " and " +
          dsQuota);
    }

    String srcs = normalizePath(src);

    INode[] inodes = getRootDir().getExistingPathINodes(src, true);
    INode targetNode = inodes[inodes.length - 1];
    if (targetNode == null) {
      throw new FileNotFoundException("Directory does not exist: " + srcs);
    } else if (!targetNode.isDirectory()) {
      throw new FileNotFoundException("Cannot set quota on a file: " + srcs);
    } else if (targetNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException(
          "Cannot clear namespace quota on root.");
    } else { // a directory inode
      INodeDirectory dirNode = (INodeDirectory) targetNode;
      long oldNsQuota = dirNode.getNsQuota();
      long oldDsQuota = dirNode.getDsQuota();
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }

      if (dirNode instanceof INodeDirectoryWithQuota) {
        // a directory with quota; so set the quota to the new value
        ((INodeDirectoryWithQuota) dirNode).setQuota(nsQuota, dsQuota);
        if (!dirNode.isQuotaSet()) {
          // will not come here for root because root's nsQuota is always set
          INodeDirectory newNode = new INodeDirectory(dirNode);
          INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
          dirNode = newNode;
          parent.replaceChild(newNode);
        }
      } else {
        // a non-quota directory; so replace it with a directory with quota
        INodeDirectoryWithQuota newNode =
            new INodeDirectoryWithQuota(nsQuota, dsQuota, nsCount, dsCount,
                dirNode);
        // non-root directory node; parent != null
        INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
        dirNode = newNode;
        parent.replaceChild(newNode);
      }
      return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
    }
  }
  
  long totalInodes() throws IOException {
    // TODO[Hooman]: after fixing quota, we can use root.getNscount instead of this.
    LightWeightRequestHandler totalInodesHandler =
        new LightWeightRequestHandler(HDFSOperationType.TOTAL_FILES) {
          @Override
          public Object performTask() throws StorageException, IOException {
            INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
            return da.countAll();
          }
        };
    return (Integer) totalInodesHandler.handle();
  }

  /**
   * Sets the access time on the file/directory. Logs it in the transaction
   * log.
   */
  void setTimes(String src, INode inode, long mtime, long atime, boolean force)
      throws StorageException, TransactionContextException,
      AccessControlException {
    unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException, AccessControlException {
    INode inode = getINode(src);
    return unprotectedSetTimes(src, inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(String src, INode inode, long mtime,
      long atime, boolean force)
      throws StorageException, TransactionContextException,
      AccessControlException {
    boolean status = false;
    if (mtime != -1) {
      inode.setModificationTimeForce(mtime);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() &&
          !force) {
        status = false;
      } else {
        inode.setAccessTime(atime);
        status = true;
      }
    }
    return status;
  }

  /**
   * Reset the entire namespace tree.
   */
  void reset() throws IOException {
    setReady(false);
    createRootInode(
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        true);
    nameCache.reset();
  }

  /**
   * create an hdfs file status from an inode
   *
   * @param path
   *     the local name
   * @param node
   *     inode
   * @param needLocation
   *     if block locations need to be included or not
   * @return a file status
   * @throws IOException
   *     if any error occurs
   */
  private HdfsFileStatus createFileStatus(byte[] path, INode node,
      boolean needLocation) throws IOException, StorageException {
    if (needLocation) {
      return createLocatedFileStatus(path, node);
    } else {
      return createFileStatus(path, node);
    }
  }

  private HdfsFileStatus createFileStatusForCreate(byte[] path, INode node)
      throws IOException {
    return createFileStatus(path, node, 0);
  }

  /**
   * Create FileStatus by file INode
   */
  private HdfsFileStatus createFileStatus(byte[] path, INode node)
      throws IOException {
    long size = 0;     // length is zero for directories
    if (node instanceof INodeFile) {
      INodeFile fileNode = (INodeFile) node;
      size = fileNode.getSize();//.computeFileSize(true);
      //size = fileNode.computeFileSize(true);
    }
    return createFileStatus(path, node, size);
  }

  private HdfsFileStatus createFileStatus(byte[] path, INode node, long size)
      throws IOException {
    short replication = 0;
    long blocksize = 0;
    if (node instanceof INodeFile) {
      INodeFile fileNode = (INodeFile) node;
      replication = fileNode.getBlockReplication();
      blocksize = fileNode.getPreferredBlockSize();
    }
    // TODO Add encoding status
    return new HdfsFileStatus(size, node.isDirectory(), replication, blocksize,
        node.getModificationTime(), node.getAccessTime(),
        node.getFsPermission(), node.getUserName(), node.getGroupName(),
        node.isSymlink() ? ((INodeSymlink) node).getSymlink() : null, path);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private HdfsLocatedFileStatus createLocatedFileStatus(byte[] path, INode node)
      throws IOException, StorageException {
    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    if (node instanceof INodeFile) {
      INodeFile fileNode = (INodeFile) node;
      size = fileNode.computeFileSize(true);
      replication = fileNode.getBlockReplication();
      blocksize = fileNode.getPreferredBlockSize();
      loc = getFSNamesystem().getBlockManager()
          .createLocatedBlocks(fileNode.getBlocks(),
              fileNode.computeFileSize(false), fileNode.isUnderConstruction(),
              0L, size, false);
      if (loc == null) {
        loc = new LocatedBlocks();
      }
    }
    return new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
        blocksize, node.getModificationTime(), node.getAccessTime(),
        node.getFsPermission(), node.getUserName(), node.getGroupName(),
        node.isSymlink() ? ((INodeSymlink) node).getSymlink() : null, path,
        loc);
  }


  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  INodeSymlink addSymlink(String path, String target, PermissionStatus dirPerms,
      boolean createParent)
      throws IOException {
    final long modTime = now();
    if (createParent) {
      final String parent = new Path(path).getParent().toString();
      if (!mkdirs(parent, dirPerms, true, modTime)) {
        return null;
      }
    }
    final String userName = dirPerms.getUserName();
    INodeSymlink newNode = unprotectedAddSymlink(path, target, modTime, modTime,
        new PermissionStatus(userName, null, FsPermission.getDefault()));

    if (newNode == null) {
      NameNode.stateChangeLog.info("DIR* addSymlink: failed to add " + path);
      return null;
    }

    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addSymlink: " + path + " is added");
    }
    return newNode;
  }

  /**
   * Add the specified path into the namespace. Invoked from edit log
   * processing.
   */
  INodeSymlink unprotectedAddSymlink(String path, String target, long mtime,
      long atime, PermissionStatus perm)
      throws IOException {
    final INodeSymlink symlink = new INodeSymlink(target, mtime, atime, perm);
    return addNode(path, symlink, UNKNOWN_DISK_SPACE);
  }
  
  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode)
      throws StorageException, TransactionContextException {
    // Name is cached only for files
    if (inode.isDirectory() || inode.isSymlink()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  

  public INodeDirectoryWithQuota getRootDir()
      throws StorageException, TransactionContextException {
    return INodeDirectoryWithQuota.getRootDir();
  }

  public boolean isQuotaEnabled() {
    return this.quotaEnabled;
  }
  
  //add root inode if its not there
  public static INodeDirectoryWithQuota createRootInode(
      final PermissionStatus ps, final boolean overwrite) throws IOException {
    LightWeightRequestHandler addRootINode =
        new LightWeightRequestHandler(HDFSOperationType.SET_ROOT) {
          @Override
          public Object performTask() throws IOException {
            INodeDirectoryWithQuota newRootINode = null;
            INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
            INodeDirectoryWithQuota rootInode = (INodeDirectoryWithQuota) da
                .findInodeByNameParentIdAndPartitionIdPK(INodeDirectory.ROOT_NAME,
                    INodeDirectory.ROOT_PARENT_ID, INodeDirectory.getRootDirPartitionKey());
            if (rootInode == null || overwrite == true) {
              newRootINode = INodeDirectoryWithQuota.createRootDir(ps);
              List<INode> newINodes = new ArrayList();
              newINodes.add(newRootINode);
              da.prepare(INode.EMPTY_LIST, newINodes, INode.EMPTY_LIST);

              INodeAttributes inodeAttributes =
                  new INodeAttributes(newRootINode.getId(), Long.MAX_VALUE, 1L,
                      FSDirectory.UNKNOWN_DISK_SPACE, 0L);
              INodeAttributesDataAccess ida =
                  (INodeAttributesDataAccess) HdfsStorageFactory
                      .getDataAccess(INodeAttributesDataAccess.class);
              List<INodeAttributes> attrList = new ArrayList();
              attrList.add(inodeAttributes);
              ida.prepare(attrList, null);
              LOG.info("Added new root inode");
            }
            return (Object) newRootINode;
          }
        };
    return (INodeDirectoryWithQuota) addRootINode.handle();
  }

  private INode cloneINode(final INode inode)
      throws IOException {
    INode clone = null;
    if (inode instanceof INodeDirectoryWithQuota) {
      clone = new INodeDirectoryWithQuota(
          ((INodeDirectoryWithQuota) inode).getNsQuota(),
          ((INodeDirectoryWithQuota) inode).getDsQuota(),
          (INodeDirectory) inode);
    } else if (inode instanceof INodeSymlink) {
      clone = new INodeSymlink(((INodeSymlink) inode).getLinkValue(),
          ((INodeSymlink) inode).getModificationTime(),
          ((INodeSymlink) inode).getAccessTime(),
          ((INodeSymlink) inode).getPermissionStatus());

    } else if (inode instanceof INodeFileUnderConstruction) {
      int id = ((INodeFileUnderConstruction) inode).getId();
      int pid = ((INodeFileUnderConstruction) inode).getParentId();
      byte[] name = ((INodeFileUnderConstruction) inode).getLocalNameBytes();
      short replication =
          ((INodeFileUnderConstruction) inode).getBlockReplication();
      long modificationTime =
          ((INodeFileUnderConstruction) inode).getModificationTime();
      long preferredBlockSize =
          ((INodeFileUnderConstruction) inode).getPreferredBlockSize();
      BlockInfo[] blocks = null/*BlockInfo[] blocks,*/;
      PermissionStatus permissionStatus =
          ((INodeFileUnderConstruction) inode).getPermissionStatus();
      String clientName = ((INodeFileUnderConstruction) inode).getClientName();
      String clientMachineName =
          ((INodeFileUnderConstruction) inode).getClientMachine();
      DatanodeID datanodeID =
          ((INodeFileUnderConstruction) inode).getClientNode();
      clone =
          new INodeFileUnderConstruction(name, replication, modificationTime,
              preferredBlockSize, blocks, permissionStatus, clientName,
              clientMachineName, datanodeID, id, pid);
      ((INodeFileUnderConstruction)clone).setHasBlocksNoPersistance(((INodeFileUnderConstruction)inode).hasBlocks());
    } else if (inode instanceof INodeFile) {
      clone = new INodeFile((INodeFile) inode);
    } else if (inode instanceof INodeDirectory) {
      clone = new INodeDirectory((INodeDirectory) inode);
    }
    clone.setHeaderNoPersistance(inode.getHeader());
    clone.setPartitionIdNoPersistance(inode.getPartitionId());
    clone.setLocalNameNoPersistance(inode.getLocalName());
    clone.setIdNoPersistance(inode.getId());
    clone.setParentIdNoPersistance(inode.getParentId());
    clone.setUser(inode.getUserName());
    return clone;
  }

  public boolean hasChildren(final int parentId, final boolean areChildrenRandomlyPartitioned) throws IOException {
    LightWeightRequestHandler hasChildrenHandler =
            new LightWeightRequestHandler(HDFSOperationType.HAS_CHILDREN) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
        return ida.hasChildren(parentId, areChildrenRandomlyPartitioned);
      }
    };
    return (Boolean) hasChildrenHandler.handle();
  }
}
