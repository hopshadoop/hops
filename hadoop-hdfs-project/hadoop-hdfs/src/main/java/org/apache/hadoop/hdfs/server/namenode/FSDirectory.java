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
import io.hops.common.IDsGeneratorFactory;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.resolvingcache.Cache;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.security.AccessControlException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.FsAclPermission;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

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

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING = ".reserved";
  public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
      + DOT_RESERVED_STRING;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  public final static String DOT_INODES_STRING = ".inodes";
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  private final FSNamesystem namesystem;
  private volatile boolean ready = false;
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private long yieldCount = 0; // keep track of lock yield count.
  
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

    namesystem = ns;

    createRoot(ns.createFsOwnerPermissions(new FsPermission((short) 0755)),
        false /*dont overwrite if root inode already existes*/);
    
    int configuredLimit = conf.getInt(DFSConfigKeys.DFS_LIST_LIMIT,
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit > 0 ? configuredLimit :
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    
    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    
    // filesystem limits
    this.maxComponentLength =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
            DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    Preconditions.checkArgument(maxDirItems >= 0, "Cannot set "
        + DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY
        + " to a value less than 0");
    
    int threshold =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
            DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG
        .info("Caching file names occuring more than " + threshold + " times");
    nameCache = new NameCache<>(threshold);
    
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
  INodeFile addFile(String path, PermissionStatus permissions,
      short replication, long preferredBlockSize, String clientName,
      String clientMachine, DatanodeDescriptor clientNode)
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
    INodeFile newNode = new INodeFile(IDsGeneratorFactory.getInstance().getUniqueINodeID(), permissions,
        BlockInfo.EMPTY_ARRAY, replication, modTime, modTime, preferredBlockSize, (byte) 0);
    newNode.toUnderConstruction(clientName, clientMachine, clientNode);

    boolean added = false;
    added = addINode(path, newNode);

    if (!added) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " + path);
      return null;
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + path + " is added");
    }
    return newNode;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfo addBlock(String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo targets[])
      throws QuotaExceededException, StorageException,
      TransactionContextException, IOException {
    final INode[] inodes = inodesInPath.getINodes();
    final INodeFile fileINode = inodes[inodes.length - 1].asFile();
    Preconditions.checkState(fileINode.isUnderConstruction());

    long diskspaceTobeConsumed = fileINode.getBlockDiskspace();
    //When appending to a small file stored in DB we should consider the file
    // size which was accounted for before in the inode attributes to avoid
    // over calculation of quota
    if(fileINode.isFileStoredInDB()){
      diskspaceTobeConsumed -= (fileINode.getSize() *
          fileINode.getBlockReplication());
    }
    // check quota limits and updated space consumed
    updateCount(inodesInPath, inodes.length - 1, 0, diskspaceTobeConsumed, true);

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
  void persistBlocks(String path, INodeFile file)
      throws StorageException, TransactionContextException {
    Preconditions.checkArgument(file.isUnderConstruction());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.persistBlocks: " + path + " with " +
              file.getBlocks().length +
              " blocks is persisted to the file system");
    }
  }
  
  /**
   * Persist the new block (the last block of the given file).
   */
  void persistNewBlock(String path, INodeFile file) throws IOException {
    Preconditions.checkArgument(file.isUnderConstruction());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      String block ="";
      if(file.getLastBlock()!=null){
        block = file.getLastBlock().toString();
      }
      NameNode.stateChangeLog.debug("DIR* FSDirectory.persistNewBlock: "
          + path + " with new block " + block
          + ", current total block count is " + file.getBlocks().length);
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
   * @return Whether the block exists in the corresponding file
   */
  boolean removeBlock(String path, INodeFile fileNode,
      Block block) throws IOException, StorageException {
    Preconditions.checkArgument(fileNode.isUnderConstruction());
    return unprotectedRemoveBlock(path, fileNode, block);
  }
  
  boolean unprotectedRemoveBlock(String path, INodeFile fileNode,
      Block block) throws IOException, StorageException {
    Preconditions.checkArgument(fileNode.isUnderConstruction());
    // modify file-> block and blocksMap
    boolean removed = fileNode.removeLastBlock(block);
    if (!removed) {
      return false;
    }
    getBlockManager().removeBlockFromMap(block);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.removeReplica: " + path + " with " + block +
              " block is removed from the file system");
    }

    // update space consumed
    final INodesInPath inodesInPath = getINodesInPath4Write(path, true);
    final INode[] inodes = inodesInPath.getINodes();
    updateCount(inodesInPath, inodes.length-1, 0,
        -fileNode.getPreferredBlockSize() * fileNode.getBlockReplication(),
        true);
    return true;
  }

  /**
   * @see #unprotectedRenameTo(String, String, long)
   * @deprecated Use {@link #renameTo(String, String, Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst, INode.DirCounts srcCounts, INode.DirCounts dstCounts)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    return unprotectedRenameTo(src, dst, now(), srcCounts, dstCounts);
  }

  /**
   * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
   */
  void renameTo(String src, String dst, INode.DirCounts srcCounts, INode.DirCounts dstCounts, Options.Rename... options)
      throws FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, QuotaExceededException,
      UnresolvedLinkException, IOException, StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    if (unprotectedRenameTo(src, dst, now(), srcCounts, dstCounts, options)) {
      incrDeletedFileCount(1);
    }
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
      INode.DirCounts srcCounts, INode.DirCounts dstCounts,
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
    final INodesInPath srcInodesInPath = getINodesInPath4Write(src, false);
    final INode[] srcInodes = srcInodesInPath.getINodes();
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
        dst.equals(((INodeSymlink) srcInode).getSymlinkString())) {
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
    INodesInPath dstInodesInPath = getExistingPathINodes(dstComponents);
    final INode[] dstInodes = dstInodesInPath.getINodes();
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
    verifyFsLimitsForRename(srcInodesInPath, dstInodesInPath, dstComponents[dstComponents.length-1]);
    verifyQuotaForRename(srcInodes, dstInodes, srcCounts,  dstCounts);
    INode removedSrc = removeLastINodeForRename(srcInodesInPath, srcCounts);
    if (removedSrc == null) {
      error = "Failed to rename " + src + " to " + dst +
          " because the source can not be removed";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }

    INode srcClone = removedSrc.cloneInode();

    final String srcChildName = removedSrc.getLocalName();
    String dstChildName = null;
    INode removedDst = null;
    boolean restoreSrc = true;
    boolean restoreDst = false;
    try {
      if (dstInode != null) { // dst exists remove it
        removedDst = removeLastINode(dstInodesInPath, dstCounts);
        dstChildName = removedDst.getLocalName();
        restoreDst = true;
      }

      removedSrc.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);
      // add src as dst to complete rename

      if (addLastINodeNoQuotaCheck(dstInodesInPath, removedSrc, srcCounts)) {
        restoreSrc = false;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        srcInodes[srcInodes.length - 2].asDirectory().decreaseChildrenNum();
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        srcInodes[srcInodes.length - 2].asDirectory().increaseChildrenNum();
        // update moved lease with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);

        // Collect the blocks and remove the lease for previous dst
        int filesDeleted = 0;
        if (removedDst != null) {
          INode rmdst = removedDst;
          restoreDst=false;
          BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
          filesDeleted = 1; // rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
                            // [S] as the dst dir was empty it will always return 1
                            // if the destination is file then we need to collect the blocks for it
          if(rmdst instanceof  INodeFile && !((INodeFile)rmdst).isFileStoredInDB()){
            Block [] blocks = ((INodeFile)rmdst).getBlocks();
            for(Block blk : blocks){
              collectedBlocks.addDeleteBlock(blk);
            }
          }else if(rmdst instanceof  INodeFile && ((INodeFile)rmdst).isFileStoredInDB()){
            ((INodeFile)rmdst).deleteFileDataStoredInDB();
          }
          getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
        }
  
        if(!restoreSrc && !restoreDst) {
          logMetadataEventForRename(srcClone, removedSrc);
        }
        
        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            removedSrc);
        
        return filesDeleted > 0;
      }
    } finally {
      if (restoreSrc) {
        // Rename failed - restore src
        removedSrc.setLocalNameNoPersistance(srcChildName);
        addLastINodeNoQuotaCheck(srcInodesInPath, removedSrc, srcCounts);
      }
      if (restoreDst) {
        // Rename failed - restore dst
        removedDst.setLocalNameNoPersistance(dstChildName);
        addLastINodeNoQuotaCheck(dstInodesInPath, removedDst, dstCounts);
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
  boolean unprotectedRenameTo(String src, String dst, long timestamp, INode.DirCounts srcCounts,
      INode.DirCounts dstCounts)
      throws IOException {
    final INodesInPath srcInodesInPath = getINodesInPath4Write(src, false);
    final INode[] srcInodes = srcInodesInPath.getINodes();
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
        dst.equals(((INodeSymlink) srcInode).getSymlinkString())) {
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
    INodesInPath dstInodesInPath = getExistingPathINodes(dstComponents);
    final INode[] dstInodes = dstInodesInPath.getINodes();
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
    verifyFsLimitsForRename(srcInodesInPath, dstInodesInPath, dstComponents[dstComponents.length-1]);
    verifyQuotaForRename(srcInodes, dstInodes, srcCounts, dstCounts);

    boolean added = false;
    INode srcChild = null;
    String srcChildName = null;
    try {
      // remove src
      srcChild =
          removeLastINodeForRename(srcInodesInPath, srcCounts);
      if (srcChild==null) {
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " +
                src + " to " + dst + " because the source can not be removed");
        return false;
      }


      INode srcClone = srcChild.cloneInode();

      srcChildName = srcChild.getLocalName();
      srcChild.setLocalNameNoPersistance(dstComponents[dstInodes.length - 1]);

      // add src to the destination
      added = addLastINodeNoQuotaCheck(dstInodesInPath, srcChild, srcCounts);
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* FSDirectory.unprotectedRenameTo: " + src +
                  " is renamed to " + dst);
        }
        // update modification time of dst and the parent of src
        srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
        srcInodes[srcInodes.length - 2].asDirectory().decreaseChildrenNum();
        dstInodes[dstInodes.length - 2].setModificationTime(timestamp);
        srcInodes[srcInodes.length - 2].asDirectory().increaseChildrenNum();
        // update moved leases with new filename
        getFSNamesystem().unprotectedChangeLease(src, dst);
        
        logMetadataEventForRename(srcClone, srcChild);
        
        EntityManager.snapshotMaintenance(
            HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
            srcChild);
        
        return true;
      }
    } finally {
      if (!added && srcChild != null) {
        // put it back
        srcChild.setLocalNameNoPersistance(srcChildName);
        addLastINodeNoQuotaCheck(srcInodesInPath, srcChild, srcCounts);
      }
    }
    NameNode.stateChangeLog.warn(
        "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
            " to " + dst);
    return false;
  }

  private void logMetadataEventForRename(INode srcClone, INode srcChild)
      throws TransactionContextException, StorageException {
    INode srcDataset = srcClone.getMetaEnabledParent();
    INode dstDataset = srcChild.getMetaEnabledParent();
  
    if(srcDataset == null){
      if(dstDataset == null){
        //No logging required
      }else{
        //rename from non metaEnabled directory to a metaEnabled directoy
        srcChild.logMetadataEvent(MetadataLogEntry.Operation.ADD);
      }
    }else{
      if(dstDataset == null){
        //rename from metaEnabled directory to a non metaEnabled directory
        EntityManager.add(new MetadataLogEntry(srcDataset.getId(),
            srcClone.getId(), srcClone.getPartitionId(), srcClone
            .getParentId(), srcClone.getLocalName(), srcChild
            .incrementLogicalTime(), MetadataLogEntry.Operation.DELETE));
      }else{
        //rename across datasets or the same dataset
        srcChild.logMetadataEvent(MetadataLogEntry.Operation.RENAME);
      }
    }
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
    final INodesInPath inodesInPath = getINodesInPath4Write(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    INode inode = inodes[inodes.length - 1];
    if (inode == null || !inode.isFile()) {
      return null;
    }
    INodeFile fileNode = (INodeFile) inode;
    final short oldRepl = fileNode.getBlockReplication();

    // check disk quota
    long dsDelta =
        (replication - oldRepl) * (fileNode.diskspaceConsumed() / oldRepl);
    updateCount(inodesInPath, inodes.length - 1, 0, dsDelta, true);

    fileNode.setReplication(replication);

    if (oldReplication != null) {
      oldReplication[0] = oldRepl;
    }
    return fileNode.getBlocks();
  }

  void setStoragePolicy(String src, BlockStoragePolicy policy) throws IOException {
    unprotectedSetStoragePolicy(src, policy);
  }

  void unprotectedSetStoragePolicy(String src, BlockStoragePolicy policy) throws IOException {
    final INodesInPath inodesInPath = getINodesInPath4Write(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    INode inode = inodes[inodes.length - 1];

    if (inode == null) {
      throw new FileNotFoundException("File/Directory does not exist: " + src);
    } else if (inode.isSymlink()) {
      throw new IOException("Cannot set storage policy for symlink: " + src);
    } else {
      inode.setBlockStoragePolicyID(policy.getId());
    }
  }

  /**
   * @param path the file path
   * @return the block size of the file.
   */
  long getPreferredBlockSize(String path)
      throws UnresolvedLinkException, FileNotFoundException, IOException,
      StorageException {
    return INodeFile.valueOf(getNode(path, false), path).getPreferredBlockSize();
  }

  void setPermission(String src, FsPermission permission)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    unprotectedSetPermission(src, permission);
  }

  void unprotectedSetPermission(String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    INode inode = getNode(src, true);
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
    INode inode = getNode(src, true);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    UsersGroups.addUserToGroup(username, groupname);

    if (username != null) {
      inode.setUser(username);
    }
    if (groupname != null) {
      inode.setGroup(groupname);
    }
    
    inode.logMetadataEvent(MetadataLogEntry.Operation.UPDATE);
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
    
    final INodesInPath trgINodesInPath = getINodesInPath4Write(target);
    final INode[] trgINodes = trgINodesInPath.getINodes();
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
        new ArrayList<>();
    for (INodeFile allSrcInode : allSrcInodes) {
      srcs_param.add(new INodeCandidatePrimaryKey(allSrcInode.getId()));
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
    unprotectedUpdateCount(trgINodesInPath, trgINodes.length - 1, -count, 0);
    

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
  boolean delete(String src, BlocksMapUpdateInfo collectedBlocks)
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
    final INode inode = getNode(path, false);
    if (inode == null || !inode.isDirectory()) {
      //not found or not a directory
      return false;
    }
    return ((INodeDirectory) inode).getChildrenList().size() != 0;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * <br>
   * Note: This is to be used by FSEditLog only.
   * <br>
   *
   * @param src
   *     a string representation of a path to an inode
   * @param mtime
   *     the time the inode is removed
   */
  void unprotectedDelete(String src, long mtime)
      throws UnresolvedLinkException, StorageException, IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
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
  int unprotectedDelete(String src, BlocksMapUpdateInfo collectedBlocks, long mtime)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);

    final INodesInPath inodesInPath = getINodesInPath4Write(src, false);
    final INode[] inodes = inodesInPath.getINodes();
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

    // Remove the node from the namespace
    targetNode = removeLastINode(inodesInPath);
    if (targetNode == null) {
      return 0;
    }
    // set the parent's modification time
    inodes[inodes.length - 2].setModificationTime(mtime);
    inodes[inodes.length - 2].asDirectory().decreaseChildrenNum();
    
    int filesRemoved = targetNode.collectSubtreeBlocksAndClear(collectedBlocks);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* FSDirectory.unprotectedDelete: " + src + " is removed");
    }
    return filesRemoved;
  }

  private void addMetaDataLogForDirDeletion(INode targetNode) throws TransactionContextException, StorageException {
    if (targetNode.isDirectory()) {
      List<INode> children = ((INodeDirectory) targetNode).getChildrenList();
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

  private byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != BlockStoragePolicySuite.ID_UNSPECIFIED ? inodePolicy : parentPolicy;
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
      boolean needLocation, boolean isSuperUser)
      throws UnresolvedLinkException, IOException, StorageException {
    String srcs = normalizePath(src);
    INode targetNode = getNode(srcs, true);
    if (targetNode == null) {
      return null;
    }

    byte parentStoragePolicy = isSuperUser ?
      targetNode.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
    
    if (!targetNode.isDirectory()) {
      return new DirectoryListing(new HdfsFileStatus[]{
          createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode,
              needLocation, parentStoragePolicy)}, 0);
    }
    INodeDirectory dirInode = (INodeDirectory) targetNode;
    List<INode> contents = dirInode.getChildrenList();
    int startChild = dirInode.nextChild(contents, startAfter);
    int totalNumChildren = contents.size();
    int numOfListing = Math.min(totalNumChildren - startChild, this.lsLimit);
    int locationBudget = this.lsLimit;
      int listingCnt = 0;
    HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing && locationBudget>0; i++) {
      INode cur = contents.get(startChild + i);
      byte curPolicy = isSuperUser && !cur.isSymlink() ? cur.getLocalStoragePolicyID()
          : BlockStoragePolicySuite.ID_UNSPECIFIED;
      listing[i] = createFileStatus(cur.getLocalNameBytes(), cur, needLocation, getStoragePolicyID(curPolicy,
          parentStoragePolicy));
      listingCnt++;
        if (needLocation) {
            // Once we  hit lsLimit locations, stop.
            // This helps to prevent excessively large response payloads.
            // Approximate #locations with locatedBlockCount() * repl_factor
            LocatedBlocks blks = 
                ((HdfsLocatedFileStatus)listing[i]).getBlockLocations();
            locationBudget -= (blks == null) ? 0 :
               blks.locatedBlockCount() * listing[i].getReplication();
        }
      }
      // truncate return array if necessary
      if (listingCnt < numOfListing) {
          listing = Arrays.copyOf(listing, listingCnt);
    }
    return new DirectoryListing(listing,
        totalNumChildren - startChild - listingCnt);
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
  HdfsFileStatus getFileInfo(String src, boolean resolveLink, boolean includeStoragePolicy)
      throws IOException {
    String srcs = normalizePath(src);
    INode targetNode = getNode(srcs, resolveLink);
    if (targetNode == null) {
      return null;
    } else {
      byte policyId = includeStoragePolicy && targetNode != null && !targetNode.isSymlink() ? targetNode.
          getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
      return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode, policyId);
    }
  }

  INodesInPath getExistingPathINodes(byte[][] components)
      throws UnresolvedLinkException, StorageException, TransactionContextException {
    return INodesInPath.resolve(getRootDir(), components);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src) throws UnresolvedLinkException,
      StorageException, TransactionContextException {
    return getNode(src, true);
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
   * @see INodeDirectory#getINodesInPath4Write(byte[][], INode[], boolean)
   */
  INodesInPath getINodesInPath4Write(String path)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    return getINodesInPath4Write(path, true);
  }
  
  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INodesInPath getLastINodeInPath(String src)
      throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getLastINodeInPath(src, true);
  }

  /**
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    String srcs = normalizePath(src);
    return srcs.startsWith("/") && !srcs.endsWith("/")
        && getINode4Write(srcs, false) == null;
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);
    INode node = getNode(src, false);
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
   * @throws FileNotFoundException
   *     if path does not exist.
   */
  void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
      throws QuotaExceededException, FileNotFoundException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    final INodesInPath inodesInPath = getINodesInPath4Write(path, false);
    final INode[] inodes = inodesInPath.getINodes();
    int len = inodes.length;
    if (inodes[len - 1] == null) {
      throw new FileNotFoundException("Path not found: " + path);
    }
    updateCount(inodesInPath, len - 1, nsDelta, dsDelta, true);
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
  private void updateCount(INodesInPath inodesInPath, int numOfINodes, long nsDelta,
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
    final INode[] inodes = inodesInPath.getINodes();
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
  private void updateCountNoQuotaCheck(INodesInPath inodesInPath, int numOfINodes,
      long nsDelta, long dsDelta)
      throws StorageException, TransactionContextException {
    try {
      updateCount(inodesInPath, numOfINodes, nsDelta, dsDelta, false);
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
  void unprotectedUpdateCount(INodesInPath inodesInPath, int numOfINodes, long nsDelta,
      long dsDelta) throws StorageException, TransactionContextException {
    if (!isQuotaEnabled()) {
      return;
    }
    final INode[] inodes = inodesInPath.getINodes();
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
    final int lastInodeIndex = components.length - 1;

    INodesInPath inodesInPath = getExistingPathINodes(components);
    INode[] inodes = inodesInPath.getINodes();

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
      pathbuilder.append(Path.SEPARATOR).append(names[i]);
      String cur = pathbuilder.toString();
      unprotectedMkdir(IDsGeneratorFactory.getInstance().getUniqueINodeID(), inodesInPath, i, components[i],
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

  INode unprotectedMkdir(int inodeId, String src, PermissionStatus permissions,
      long timestamp)
      throws IOException {
    byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = getExistingPathINodes(components);
    INode[] inodes = inodesInPath.getINodes();
    unprotectedMkdir(inodeId, inodesInPath, inodes.length - 1, components[inodes.length - 1],
        permissions, timestamp);
    return inodes[inodes.length - 1];
  }

  /**
   * create a directory at index pos.
   * The parent path to the directory is at [0, pos-1].
   * All ancestors exist. Newly created one stored at index pos.
   */
  private void unprotectedMkdir(long inodeId, INodesInPath inodesInPath, int pos, byte[] name,
      PermissionStatus permission, long timestamp)
      throws IOException {
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission, timestamp);
    if (addChild(inodesInPath, pos, dir, true)) {
      inodesInPath.setINode(pos, dir);
    }
  }
    
  /**
   * Add the given child to the namespace.
   * @param src The full path name of the child node.
   * @throw QuotaExceededException is thrown if it violates quota limit
   */
  private boolean addINode(String src, INode child)
      throws IOException {
    byte[][] components = INode.getPathComponents(src);
    byte[] path = components[components.length - 1];
    child.setLocalNameNoPersistance(path);
    cacheName(child);
    INodesInPath inodesInPath = getExistingPathINodes(components);
    return addLastINode(inodesInPath, child, true);
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
        final DirectoryWithQuotaFeature q =
            inodes[i].asDirectory().getDirectoryWithQuotaFeature();
        if (q != null) { // a directory with quota
          q.verifyQuota(inodes[i].asDirectory(), nsDelta, dsDelta);
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
   * directory from where node is being moved.
   * @param dstInodes
   * directory to where node is moved to.
   * @throws QuotaExceededException
   * if quota limit is exceeded.
   */
  private void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes, INode.DirCounts srcCounts,
      INode.DirCounts dstCounts) throws QuotaExceededException, StorageException, TransactionContextException {

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
    long nsDelta = srcCounts.getNsCount();
    long dsDelta = srcCounts.getDsCount();
    // Reduce the required quota by dst that is being removed
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInode != null) {
      nsDelta -= dstCounts.getNsCount();
      dsDelta -= dstCounts.getDsCount();
    }
    verifyQuota(dstInodes, dstInodes.length - 1, nsDelta, dsDelta,
        commonAncestor);
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   *
   * @param srcIIP INodesInPath containing every inode in the rename source
   * @param dstIIP INodesInPath containing every inode in the rename destination
   * @throws PathComponentTooLongException child's name is too long.
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  private void verifyFsLimitsForRename(INodesInPath srcIIP, INodesInPath dstIIP, byte[] dstChildName)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException, StorageException, TransactionContextException {
    INode[] dstInodes = dstIIP.getINodes();
    int pos = dstInodes.length - 1;
    verifyMaxComponentLength(dstChildName, dstInodes, pos);
    // Do not enforce max directory items if renaming within same directory.
    if (maxDirItems > 0 && srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
      verifyMaxDirItems(dstInodes, pos);
    }
  }
  
  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath Object either INode[] or String containing parent path
   * @param pos int position of new child in path
   * @throws PathComponentTooLongException child's name is too long.
   */
  private void verifyMaxComponentLength(byte[] childName, Object parentPath,
      int pos) throws PathComponentTooLongException {
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final String p = parentPath instanceof INode[] ? getFullPathName((INode[]) parentPath, pos - 1)
          : (String) parentPath;
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, p, DFSUtil.bytes2String(childName));
      if (ready) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
      }
    }
  }

  /**
   * Verify children size for fs limit.
   *
   * @param pathComponents INode[] containing full path of inodes to new child
   * @param pos int position of new child in pathComponents
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  private void verifyMaxDirItems(INode[] pathComponents, int pos)
      throws MaxDirectoryItemsExceededException, StorageException, TransactionContextException {

    final INodeDirectory parent = pathComponents[pos - 1].asDirectory();
    final int count = parent.getChildrenNum();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(maxDirItems, count);
      if (ready) {
        e.setPathName(getFullPathName(pathComponents, pos - 1));
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
            + e.getLocalizedMessage());
      }
    }
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
          final String p = getFullPathName((INode[]) pathComponents, pos - 1);
          throw new PathComponentTooLongException(
          maxComponentLength, length, p, child.getLocalName());
        }
      }
      if (maxDirItems != 0) {
        INodeDirectory parent = (INodeDirectory) pathComponents[pos - 1];
        int count = parent.getChildrenNum();
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
   * The same as {@link #addChild(INodesInPath, int, INode, boolean)}
   * with pos = length - 1.
   */
  private boolean addLastINode(INodesInPath inodesInPath,
      INode inode, INode.DirCounts counts, boolean checkQuota, boolean
      logMetadataEvent) throws
      QuotaExceededException, StorageException, IOException {
    final int pos = inodesInPath.getINodes().length - 1;
    return addChild(inodesInPath, pos, inode, counts, checkQuota, logMetadataEvent);
  }

  private boolean addLastINode(INodesInPath inodesInPath,
      INode inode, boolean checkQuota) throws QuotaExceededException, StorageException, IOException {
    final int pos = inodesInPath.getINodes().length - 1;
    return addChild(inodesInPath, pos, inode, checkQuota);
  }
  
  /**
   * Add a node child to the inodes at index pos.
   * Its ancestors are stored at [0, pos-1].
   * @return false if the child with this name already exists;
   *         otherwise return true;
   * @throw QuotaExceededException is thrown if it violates quota limit
   */
  private boolean addChild(INodesInPath inodesInPath, int pos,
      INode child, boolean checkQuota) throws QuotaExceededException, StorageException, IOException {
    final INode[] inodes = inodesInPath.getINodes();
    // The filesystem limits are not really quotas, so this check may appear
    // odd.  It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyFsLimits(inodes, pos, child);
    }
    
    INode.DirCounts counts = new INode.DirCounts();
    if (isQuotaEnabled()) {       //HOP
      child.spaceConsumedInTree(counts);
    }

    return addChild(inodesInPath, pos, child, counts, checkQuota);
  }
  
  private boolean addChild(INodesInPath inodesInPath, int pos, INode child,
      INode.DirCounts counts, boolean checkQuota)
      throws IOException {
    return addChild(inodesInPath, pos, child, counts, checkQuota, true);
  }
  
  private boolean addChild(INodesInPath inodesInPath, int pos, INode child,
      INode.DirCounts counts, boolean checkQuota, boolean logMetadataEvent)
      throws IOException {
    final INode[] inodes = inodesInPath.getINodes();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    if (pos == 1 && inodes[0] == getRootDir() && isReservedName(child)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + child.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    // The filesystem limits are not really quotas, so this check may appear
    // odd.  It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location becase a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      verifyFsLimits(inodes, pos, child);
    }

     updateCount(inodesInPath, pos, counts.getNsCount(), counts.getDsCount(), checkQuota);
    if (inodes[pos-1] == null) {
      throw new NullPointerException("Panic: parent does not exist");
    }
    final boolean added = ((INodeDirectory)inodes[pos-1]).addChild(child,
        true, logMetadataEvent);
    if (!added) {
      updateCount(inodesInPath, pos, -counts.getNsCount(), -counts.getDsCount(), true);
    } 

    if (added) {
      if (!child.isDirectory()) {
        INode[] pc = Arrays.copyOf(inodes, inodes.length);
        pc[pc.length - 1] = child;
        String path = getFullPathName(pc, pc.length - 1);
        Cache.getInstance().set(path, pc);
      }
    }
    //
    return added;
  }
  
  private boolean addLastINodeNoQuotaCheck(INodesInPath inodesInPath, INode child, INode.DirCounts counts)
      throws IOException {
    try {
      return addLastINode(inodesInPath, child, counts, false, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return false;
  }
  
  private INode removeLastINode(final INodesInPath inodesInPath) throws StorageException, TransactionContextException {
    final INode[] inodes = inodesInPath.getINodes();
    final int pos = inodes.length - 1;
    INode.DirCounts counts = new INode.DirCounts();
    if (isQuotaEnabled()) {
      INode nodeToBeRemored = inodes[pos];
      nodeToBeRemored.spaceConsumedInTree(counts);
    }
    return removeLastINode(inodesInPath, false, counts);
  }
  
  /**
   * Remove the last inode in the path from the namespace.
   * Count of each ancestor with quota is also updated.
   * @return the removed node; null if the removal fails.
   */
  private INode removeLastINode(final INodesInPath inodesInPath, boolean forRename,
          final INode.DirCounts counts)
      throws StorageException, TransactionContextException {
    final INode[] inodes = inodesInPath.getINodes();
    final int pos = inodes.length - 1;
    INode removedNode = null;
    if (forRename) {
      removedNode = inodes[pos];
    } else {
      removedNode = ((INodeDirectory) inodes[pos - 1])
          .removeChild(inodes[pos]);
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
      updateCountNoQuotaCheck(inodesInPath, pos,
          -counts.getNsCount() + nsDelta, -counts.getDsCount() + dsDelta);
    }
    return removedNode;
  }

  INode removeChildNonRecursively(final INodesInPath inodesInPath, int pos)
      throws StorageException, TransactionContextException {
    final INode[] inodes = inodesInPath.getINodes();
    INode removedNode = ((INodeDirectory) inodes[pos - 1])
        .removeChild(inodes[pos]);
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
        updateCountNoQuotaCheck(inodesInPath, pos, -1 + nsDelta, dsDelta);
      } else {
        INode.DirCounts counts = new INode.DirCounts();
        removedNode.spaceConsumedInTree(counts);
        updateCountNoQuotaCheck(inodesInPath, pos,
            -counts.getNsCount() + nsDelta, -counts.getDsCount() + dsDelta);
      }
    }
    return removedNode;
  }
  
  private INode removeLastINode(final INodesInPath inodesInPath, final INode.DirCounts counts)
      throws StorageException, TransactionContextException {
    return removeLastINode(inodesInPath, false, counts);
  }
  
  private INode removeLastINodeForRename(final INodesInPath inodesInPath, INode.DirCounts counts) throws
      StorageException, TransactionContextException {
    return removeLastINode(inodesInPath, true, counts);
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
    INode targetNode = getNode(srcs, false);
    if (targetNode == null) {
      throw new FileNotFoundException("File does not exist: " + srcs);
    } else {
      // Make it relinquish locks everytime contentCountLimit entries are
      // processed. 0 means disabled. I.e. blocking for the entire duration.
      ContentSummaryComputationContext cscc = new ContentSummaryComputationContext(this, getFSNamesystem(),
          contentCountLimit);
      ContentSummary cs = targetNode.computeAndConvertContentSummary(cscc);
      yieldCount += cscc.getYieldCount();
      return cs;
    }
  }
  
  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
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
    updateCountForINodeWithQuota(this, getRootDir(), new INode.DirCounts(),
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
  private static void updateCountForINodeWithQuota(FSDirectory fsd, INodeDirectory dir,
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
        updateCountForINodeWithQuota(fsd, (INodeDirectory) child, counts,
            nodesInPath);
      } else if (child.isSymlink()) {
        counts.nsCount += 1;
      } else { // reduce recursive calls
        counts.nsCount += 1;
        counts.dsCount += ((INodeFile) child).diskspaceConsumed();
      }
    }

    if (dir.isQuotaSet()) {
      final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
      if (q != null) {
        q.setSpaceConsumed(dir, counts.nsCount, counts.dsCount);
      }

      // check if quota is violated for some reason.
      final Quota.Counts oldQuota = dir.getQuotaCounts();
      if ((oldQuota.get(Quota.NAMESPACE) >= 0 && counts.nsCount > oldQuota.get(Quota.NAMESPACE)) ||
          (oldQuota.get(Quota.DISKSPACE) >= 0 && counts.dsCount > oldQuota.get(Quota.DISKSPACE))) {

        // can only happen because of a software bug. the bug should be fixed.
        StringBuilder path = new StringBuilder(512);
        for (INode n : nodesInPath) {
          path.append('/');
          path.append(n.getLocalName());
        }
        
        NameNode.LOG.warn("Quota violation in image for " + path +
            " (Namespace quota : " + oldQuota.get(Quota.NAMESPACE) +
            " consumed : " + counts.nsCount + ")" +
            " (Diskspace quota : " + oldQuota.get(Quota.DISKSPACE) +
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
   *
   * @see #unprotectedSetQuota(String, long, long)
   */
  void setQuota(String src, long nsQuota, long dsQuota, long nsCount,
      long dsCount) throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException, IOException {
    INodeDirectory dir =
        unprotectedSetQuota(src, nsQuota, dsQuota, nsCount, dsCount);
    if (dir != null) {
      // Some audit log code is missing here
    }
  }

    /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   * @returns INodeDirectory if any of the quotas have changed. null other wise.
   * @throws FileNotFoundException if the path does not exist.
   * @throws PathIsNotDirectoryException if the path is not a directory.
   * @throws QuotaExceededException
   *     if the directory tree size is
   *     greater than the given quota
   * @throws UnresolvedLinkException
   *     if a symlink is encountered in src.
   */
  INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota, long nsCount, long dsCount)
      throws FileNotFoundException, PathIsNotDirectoryException, IOException,
      QuotaExceededException, UnresolvedLinkException, StorageException, TransactionContextException {
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

    final INodesInPath inodesInPath = getINodesInPath4Write(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    INodeDirectory dirNode = INodeDirectory.valueOf(inodes[inodes.length-1], srcs);
    if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException(
          "Cannot clear namespace quota on root.");
    } else { // a directory inode
      final Quota.Counts oldQuota = dirNode.getQuotaCounts();
      final long oldNsQuota = oldQuota.get(Quota.NAMESPACE);
      final long oldDsQuota = oldQuota.get(Quota.DISKSPACE);
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
        dsQuota = oldDsQuota;
      }
      
      if (!dirNode.isRoot()) {
        dirNode.setQuota(nsQuota, nsCount, dsQuota, dsCount);
        INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
        parent.replaceChild(dirNode);
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
    unprotectedSetTimes(inode, mtime, atime, force);
  }

  boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException, AccessControlException {
    INode inode = getINode(src);
    return unprotectedSetTimes(inode, mtime, atime, force);
  }

  private boolean unprotectedSetTimes(INode inode, long mtime,
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
    createRoot(
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
      boolean needLocation, byte storagePolicy) throws IOException, StorageException {
    if (needLocation) {
      return createLocatedFileStatus(path, node, storagePolicy);
    } else {
      return createFileStatus(path, node, storagePolicy);
    }
  }

  /**
   * Create FileStatus by file INode
   */
  private HdfsFileStatus createFileStatus(byte[] path, INode node, byte storagePolicy)
      throws IOException {
     long size = 0;     // length is zero for directories

    if (node instanceof INodeFile) {
      INodeFile fileNode = (INodeFile) node;
      size = fileNode.getSize();//.computeFileSize(true);
      //size = fileNode.computeFileSize(true);
    }
    short replication = 0;
    long blocksize = 0;
    boolean isStoredInDB = false;
    if (node instanceof INodeFile) {
      INodeFile fileNode = (INodeFile) node;
      replication = fileNode.getBlockReplication();
      blocksize = fileNode.getPreferredBlockSize();
      isStoredInDB = fileNode.isFileStoredInDB();
    }
    
    int childrenNum = node.isDirectory() ? node.asDirectory().getChildrenNum() : 0;

    // TODO Add encoding status
    return new HdfsFileStatus(
        size,
        node.isDirectory(),
        replication,
        blocksize,
        node.getModificationTime(),
        node.getAccessTime(),
        getPermissionForFileStatus(node),
        node.getUserName(),
        node.getGroupName(),
        node.isSymlink() ? ((INodeSymlink) node).getSymlink() : null,
        path,
        node.getId(),
        childrenNum,
        isStoredInDB,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private HdfsLocatedFileStatus createLocatedFileStatus(byte[] path,
      INode node, byte storagePolicy) throws IOException {
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    boolean isFileStoredInDB = false;
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      isFileStoredInDB = fileNode.isFileStoredInDB();
      
      if(isFileStoredInDB){
        size = fileNode.getSize();
      } else {
        size = fileNode.computeFileSize(true);
      }
      
      replication = fileNode.getBlockReplication();
      blocksize = fileNode.getPreferredBlockSize();

      final boolean isUc = fileNode.isUnderConstruction();
      final long fileSize = isUc ? 
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;
      
      if (isFileStoredInDB) {
        loc = getFSNamesystem().getBlockManager().createPhantomLocatedBlocks(fileNode,null,isUc,false);
      } else {
        loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
            fileNode.getBlocks(), fileSize, isUc, 0L, size, false);
      }
      if (loc == null) {
        loc = new LocatedBlocks();
      }
    }

    int childrenNum = node.isDirectory() ? node.asDirectory().getChildrenNum() : 0;
    
    HdfsLocatedFileStatus status = new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
        blocksize, node.getModificationTime(),
        node.getAccessTime(), node.getFsPermission(),
        node.getUserName(), node.getGroupName(),
        node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
        node.getId(), loc, childrenNum, isFileStoredInDB, storagePolicy);
    if (loc != null) {
      CacheManager cacheManager = namesystem.getCacheManager();
      for (LocatedBlock lb: loc.getLocatedBlocks()) {
        cacheManager.setCachedLocations(lb, node.getId());
      }
    }
    return status;
  }
  
  /**
   * Returns an inode's FsPermission for use in an outbound FileStatus.  If the
   * inode has an ACL, then this method will convert to a FsAclPermission.
   *
   * @param node INode to check
   * @param snapshot int snapshot ID
   * @return FsPermission from inode, with ACL bit on if the inode has an ACL
   */
  private static FsPermission getPermissionForFileStatus(INode node) throws TransactionContextException,
      StorageException, AclException {
    FsPermission perm = node.getFsPermission();
    if (node.getAclFeature() != null) {
      perm = new FsAclPermission(perm);
    }
    return perm;
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
    long id = IDsGeneratorFactory.getInstance().getUniqueINodeID();
    INodeSymlink newNode = unprotectedAddSymlink(id, path, target, modTime, modTime,
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
  INodeSymlink unprotectedAddSymlink(long id, String path, String target, long mtime,
      long atime, PermissionStatus perm)
      throws IOException {
    final INodeSymlink symlink = new INodeSymlink(id, target, mtime, atime, perm);
    return addINode(path, symlink)? symlink: null;
  }

  void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    unprotectedModifyAclEntries(src, aclSpec);
  }

  private List<AclEntry> unprotectedModifyAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    AclStorage.validateAclSpec(aclSpec);
    INode inode = getINode(src);
    if (inode == null){
      throw new FileNotFoundException();
    }
    
    if (aclSpec.size() == 1 && aclSpec.get(0).getType().equals(AclEntryType.MASK)){
      //HOPS: We allow setting
      FsPermission fsPermission = inode.getFsPermission();
      inode.setPermission(new FsPermission(fsPermission.getUserAction(), aclSpec.get(0).getPermission(), fsPermission
          .getOtherAction()));
      return AclStorage.readINodeLogicalAcl(inode);
    }
    List<AclEntry> existingAcl;
    if (AclStorage.hasOwnAcl(inode)){
      existingAcl = AclStorage.readINodeLogicalAcl(inode);
    } else {
      existingAcl = AclStorage.getMinimalAcl(inode);
    }
    
    List<AclEntry> newAcl = AclTransformation.mergeAclEntries(existingAcl,
      aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl);
    return newAcl;
  }

  void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    unprotectedRemoveAclEntries(src, aclSpec);
  }

  private List<AclEntry> unprotectedRemoveAclEntries(String src,
      List<AclEntry> aclSpec) throws IOException {
    AclStorage.validateAclSpec(aclSpec);
  
    INode inode = getINode(src);
    if (inode == null){
      throw new FileNotFoundException();
    }
    List<AclEntry> existingAcl;
    if (AclStorage.hasOwnAcl(inode)){
      existingAcl = AclStorage.readINodeLogicalAcl(inode);
    } else {
      existingAcl = AclStorage.getMinimalAcl(inode);
    }
    List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
      existingAcl, aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl);
    return newAcl;
  }

  void removeDefaultAcl(String src) throws IOException {
    unprotectedRemoveDefaultAcl(src);
  }

  private List<AclEntry> unprotectedRemoveDefaultAcl(String src)
      throws IOException {
    INode inode = getINode(src);
    if (inode == null){
      throw new FileNotFoundException();
    }
    List<AclEntry> existingAcl;
    if (AclStorage.hasOwnAcl(inode)){
      existingAcl = AclStorage.readINodeLogicalAcl(inode);
    } else {
      existingAcl = AclStorage.getMinimalAcl(inode);
    }
    List<AclEntry> newAcl = AclTransformation.filterDefaultAclEntries(
      existingAcl);
    AclStorage.updateINodeAcl(inode, newAcl);
    return newAcl;
  }

  void removeAcl(String src) throws IOException {
    unprotectedRemoveAcl(src);
  }

  private void unprotectedRemoveAcl(String src) throws IOException {
    INode inode = getINode(src);
    if (inode == null){
      throw new FileNotFoundException();
    }
    AclStorage.removeINodeAcl(inode);
  }

  void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    unprotectedSetAcl(src, aclSpec);
  }

  List<AclEntry> unprotectedSetAcl(String src, List<AclEntry> aclSpec)
      throws IOException {
    AclStorage.validateAclSpec(aclSpec);
  
    // ACL removal is logged to edits as OP_SET_ACL with an empty list.
    if (aclSpec.isEmpty()) {
      unprotectedRemoveAcl(src);
      return AclFeature.EMPTY_ENTRY_LIST;
    }
    
    INode inode = getINode(src);
    if (inode == null){
      throw new FileNotFoundException();
    }
    List<AclEntry> existingAcl;
    if (AclStorage.hasOwnAcl(inode)){
      existingAcl = AclStorage.readINodeLogicalAcl(inode);
    } else {
      existingAcl = AclStorage.getMinimalAcl(inode);
    }
    List<AclEntry> newAcl = AclTransformation.replaceAclEntries(existingAcl, aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl);
    return newAcl;
  }

  AclStatus getAclStatus(String src) throws IOException {
    String srcs = normalizePath(src);
    INode inode = getINode(srcs);
    
    List<AclEntry> acl = AclStorage.readINodeAcl(inode);
    return new AclStatus.Builder()
        .owner(inode.getUserName()).group(inode.getGroupName())
        .stickyBit(inode.getFsPermission().getStickyBit())
        .addEntries(acl).build();
  }

  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode)
      throws StorageException, TransactionContextException {
    // Name is cached only for files
    if (!inode.isFile()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  

  /**
   * Given an INode get all the path complents leading to it from the root.
   * If an Inode corresponding to C is given in /A/B/C, the returned
   * patch components will be {root, A, B, C}
   */
  static byte[][] getPathComponents(INode inode) throws StorageException, TransactionContextException {
    List<byte[]> components = new ArrayList<byte[]>();
    components.add(0, inode.getLocalNameBytes());
    while (inode.getParent() != null) {
      components.add(0, inode.getParent().getLocalNameBytes());
      inode = inode.getParent();
    }
    return components.toArray(new byte[components.size()][]);
  }

  /**
   * @return path components for reserved path, else null.
   */
  static byte[][] getPathComponentsForReservedPath(String src) {
    return !isReservedName(src) ? null : INode.getPathComponents(src);
  }

  /** Check if a given inode name is reserved */
  public static boolean isReservedName(INode inode) {
    return CHECK_RESERVED_FILE_NAMES
            && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
  }
  /** Check if a given path is reserved */
  public static boolean isReservedName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX);
  }
  
  /**
   * Resolve the path of /.reserved/.inodes/<inodeid>/... to a regular path
   *
   * @param src path that is being processed
   * @param pathComponents path components corresponding to the path
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing upto
   * <inodeid> with the corresponding path of the inode, else the path
   * in {@code src} as is.
   * @throws FileNotFoundException if inodeid is invalid
   */
  static String resolvePath(String src, byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException, StorageException, TransactionContextException, IOException {
    if (pathComponents == null || pathComponents.length <= 3) {
      return src;
    }
    // Not /.reserved/.inodes
    if (!Arrays.equals(DOT_RESERVED, pathComponents[1])
        || !Arrays.equals(DOT_INODES, pathComponents[2])) { // Not .inodes path
      return src;
    }
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    final int id;
    try {
      id = Integer.valueOf(inodeId);
    } catch (NumberFormatException e) {
      throw new FileNotFoundException("Invalid inode path: " + src);
    }
    if (id == INode.ROOT_INODE_ID && pathComponents.length == 4) {
      return Path.SEPARATOR;
    }
    
    // Handle single ".." for NFS lookup support.
    if ((pathComponents.length > 4)
        && DFSUtil.bytes2String(pathComponents[4]).equals("..")) {
      INode parent = fsd.getParent(id, src);
      if (parent == null || parent.getId() == INode.ROOT_INODE_ID) {
        // inode is root, or its parent is root.
        return Path.SEPARATOR;
      } else {
        return fsd.getFullPathName(parent.getId(), src);
      }
    }
    
    StringBuilder path = id == INode.ROOT_INODE_ID ? new StringBuilder()
        : new StringBuilder(fsd.getFullPathName(id, src));
    for (int i = 4; i < pathComponents.length; i++) {
      path.append(Path.SEPARATOR).append(DFSUtil.bytes2String(pathComponents[i]));
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug("Resolved path is " + path);
    }
    return path.toString();
  }
  
//  @VisibleForTesting
//  INode getInode(final int id) throws IOException {
//    return (INode) (new HopsTransactionalRequestHandler(HDFSOperationType.GET_INODE) {
//      INodeIdentifier inodeIdentifier;
//
//      @Override
//      public void setUp() throws StorageException {
//        inodeIdentifier = new INodeIdentifier(id);
//      }
//
//      @Override
//      public void acquireLock(TransactionLocks locks) throws IOException {
//        LockFactory lf = LockFactory.getInstance();
//        locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED, inodeIdentifier));
//      }
//
//      @Override
//      public Object performTask() throws IOException {
//        return EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
//      }
//    }).handle();
//  }
  
  String getFullPathName(final long id, final String src) throws IOException {
    HopsTransactionalRequestHandler getFullPathNameHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_INODE) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            inodeIdentifier = new INodeIdentifier(id);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED, inodeIdentifier, true));
          }

          @Override
          public Object performTask() throws IOException {
            INode inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
            if (inode == null) {
              throw new FileNotFoundException(
                  "File for given inode path does not exist: " + src);
            }
            return inode.getFullPathName();
          }
        };
    return (String) getFullPathNameHandler.handle();
  }

  INode getParent(final long id, final String src) throws IOException {
    HopsTransactionalRequestHandler getParentHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_INODE) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            inodeIdentifier = new INodeIdentifier(id);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED, inodeIdentifier, true));
          }

          @Override
          public Object performTask() throws IOException {
            INode inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
            if (inode == null) {
              throw new FileNotFoundException(
                  "File for given inode path does not exist: " + src);
            }
            return inode.getParent();
          }
        };
    return (INode) getParentHandler.handle();
  }
  
  INode getInode(final long id) throws IOException {
    HopsTransactionalRequestHandler getInodeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_INODE) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            inodeIdentifier = new INodeIdentifier(id);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED, inodeIdentifier, true));
          }

          @Override
          public Object performTask() throws IOException {
            return EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
            
          }
        };
    return (INode) getInodeHandler.handle();
  }

  /** @return the {@link INodesInPath} containing only the last inode. */
  private INodesInPath getLastINodeInPath(String path, boolean resolveLink
  ) throws UnresolvedLinkException, StorageException, TransactionContextException {
    return INodesInPath.resolve(getRootDir(), INode.getPathComponents(path), 1,
            resolveLink);
  }

    /** @return the {@link INodesInPath} containing all inodes in the path. */
  INodesInPath getINodesInPath(String path, boolean resolveLink
  ) throws UnresolvedLinkException, StorageException, TransactionContextException {
    final byte[][] components = INode.getPathComponents(path);
    return INodesInPath.resolve(getRootDir(), components, components.length,
            resolveLink);
  }
  /** @return the last inode in the path. */
  INode getNode(String path, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getLastINodeInPath(path, resolveLink).getINode(0);
  }
  /**
   * @return the INode of the last component in src, or null if the last
   * component does not exist.
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  private INode getINode4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINodesInPath4Write(src, resolveLink).getLastINode();
  }
  /**
   * @return the INodesInPath of the components in src
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  private INodesInPath getINodesInPath4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    final byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = INodesInPath.resolve(getRootDir(), components,
            components.length, resolveLink);
    return inodesInPath;
  }

  public INodeDirectory getRootDir()
      throws StorageException, TransactionContextException {
    return INodeDirectory.getRootDir();
  }

  public boolean isQuotaEnabled() {
    return this.quotaEnabled;
  }
  
  //add root inode if its not there
  public INodeDirectory createRoot(
      final PermissionStatus ps, final boolean overwrite) throws IOException {
    LightWeightRequestHandler addRootINode =
        new LightWeightRequestHandler(HDFSOperationType.SET_ROOT) {
          @Override
          public Object performTask() throws IOException {
            INodeDirectory newRootINode = null;
            INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
            INodeDirectory rootInode = (INodeDirectory) da
                .findInodeByNameParentIdAndPartitionIdPK(INodeDirectory.ROOT_NAME,
                    INodeDirectory.ROOT_PARENT_ID, INodeDirectory.getRootDirPartitionKey());
            if (rootInode == null || overwrite == true) {
              newRootINode = INodeDirectory.createRootDir(ps);
              // Set the block storage policy to DEFAULT
              List<INode> newINodes = new ArrayList();
              newINodes.add(newRootINode);
              da.prepare(INode.EMPTY_LIST, newINodes, INode.EMPTY_LIST);

              INodeAttributes inodeAttributes =
                  new INodeAttributes(newRootINode.getId(), Long.MAX_VALUE, 1L, -1L, 0L);
              INodeAttributesDataAccess ida =
                  (INodeAttributesDataAccess) HdfsStorageFactory
                      .getDataAccess(INodeAttributesDataAccess.class);
              List<INodeAttributes> attrList = new ArrayList();
              attrList.add(inodeAttributes);
              ida.prepare(attrList, null);
              LOG.info("Added new root inode");
            }
            return newRootINode;
          }
        };
    return (INodeDirectory) addRootINode.handle();
  }

  public boolean hasChildren(final long parentId, final boolean areChildrenRandomlyPartitioned) throws IOException {
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
