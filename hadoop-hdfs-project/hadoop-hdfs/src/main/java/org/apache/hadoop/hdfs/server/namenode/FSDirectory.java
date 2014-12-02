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
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.hdfs.DFSUtil;
import static org.apache.hadoop.util.Time.now;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Both FSDirectory and FSNamesystem manage the state of the namespace.
 * FSDirectory is a pure in-memory data structure, all of whose operations
 * happen entirely in memory. In contrast, FSNamesystem persists the operations
 * to the disk.
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
public class FSDirectory implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);
  
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
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private long yieldCount = 0; // keep track of lock yield count.
  
  private boolean quotaEnabled;

  private final boolean isPermissionEnabled;
  private final String fsOwnerShortUserName;
  private final String supergroup;



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
    this.isPermissionEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.fsOwnerShortUserName =
      UserGroupInformation.getCurrentUser().getShortUserName();
    this.supergroup = conf.get(
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
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

  public FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  private BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
  
  int getLsLimit() {
    return lsLimit;
  }

  int getContentCountLimit() {
    return contentCountLimit;
  }
  
  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {

  }

  void markNameCacheInitialized() {
    nameCache.initialized();
  }
  
  /**
   * Add the given filename to the fs.
   *
   * @throws FileAlreadyExistsException
   * @throws QuotaExceededException
   * @throws UnresolvedLinkException
   */
  INodeFile addFile(String path, PermissionStatus permissions,
      short replication, long preferredBlockSize,
      String clientName, String clientMachine)
      throws IOException {

    long modTime = now();
    
    INodeFile newNode = new INodeFile(IDsGeneratorFactory.getInstance().getUniqueINodeID(), permissions,
        BlockInfo.EMPTY_ARRAY, replication, modTime, modTime, preferredBlockSize, (byte) 0);
    newNode.toUnderConstruction(clientName, clientMachine);

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
          fileINode.getFileReplication());
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
        -fileNode.getPreferredBlockSize() * fileNode.getFileReplication(),
        true);
    return true;
  }

  /**
   * This is a wrapper for resolvePath(). If the path passed
   * is prefixed with /.reserved/raw, then it checks to ensure that the caller
   * has super user has super user privileges.
   *
   * @param pc The permission checker used when resolving path.
   * @param path The path to resolve.
   * @param pathComponents path components corresponding to the path
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is. If the path refers to a path in the "raw"
   *         directory, return the non-raw pathname.
   * @throws FileNotFoundException
   * @throws AccessControlException
   */
  String resolvePath(String path, byte[][] pathComponents)
      throws FileNotFoundException, AccessControlException, IOException {
    return resolvePath(path, pathComponents, this);
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
    return unprotectedSetReplication(src, replication, oldReplication);
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
    final short oldRepl = fileNode.getFileReplication();

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
    
    if (username != null) {
      UsersGroups.addUser(username);
      inode.setUser(username);
    }
    if (groupname != null) {
      UsersGroups.addGroup(groupname);
      inode.setGroup(groupname);
    }
    
    inode.logMetadataEvent(MetadataLogEntry.Operation.UPDATE);
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
  long delete(String src, BlocksMapUpdateInfo collectedBlocks, long mtime)
      throws UnresolvedLinkException, StorageException, IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
    }
    int filesRemoved = unprotectedDelete(src, collectedBlocks, mtime);

    return filesRemoved;
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
      return -1;
    }
    if (inodes.length == 1) { // src is the root
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: " +
          "failed to remove " + src +
          " because the root is not allowed to be deleted");
      return -1;
    }

    // Add metadata log entry for all deleted childred.
    addMetaDataLogForDirDeletion(targetNode);

    // Remove the node from the namespace
    targetNode = removeLastINode(inodesInPath);
    if (targetNode == null) {
      return -1;
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

  byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != BlockStoragePolicySuite.ID_UNSPECIFIED ? inodePolicy : parentPolicy;
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
    
    if (!namesystem.isImageLoaded()) {
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
    if (!namesystem.isImageLoaded()) {
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
  void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes, INode.DirCounts srcCounts,
      INode.DirCounts dstCounts) throws QuotaExceededException, StorageException, TransactionContextException {

    if (!isQuotaEnabled()) {
      return;
    }

    if (!namesystem.isImageLoaded()) {
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
  void verifyFsLimitsForRename(INodesInPath srcIIP, INodesInPath dstIIP, byte[] dstChildName)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException, StorageException, TransactionContextException {
    INode[] dstInodes = dstIIP.getINodes();
    int pos = dstInodes.length - 1;
    verifyMaxComponentLength(dstChildName, dstInodes, pos);
    // Do not enforce max directory items if renaming within same directory.
    if (maxDirItems > 0 && !srcIIP.getINode(-2).equals(dstIIP.getINode(-2))) {
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
      if (namesystem.isImageLoaded()) {
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
      if (namesystem.isImageLoaded()) {
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
      if (namesystem.isImageLoaded()) {
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
  boolean addChild(INodesInPath inodesInPath, int pos,
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
  
  boolean addChild(INodesInPath inodesInPath, int pos, INode child,
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
  
  boolean addLastINodeNoQuotaCheck(INodesInPath inodesInPath, INode child, INode.DirCounts counts)
      throws IOException {
    try {
      return addLastINode(inodesInPath, child, counts, false, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return false;
  }
  
  INode removeLastINode(final INodesInPath inodesInPath) throws StorageException, TransactionContextException {
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
  INode removeLastINode(final INodesInPath inodesInPath, boolean forRename,
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
  
  /**
   */
  static String normalizePath(String src) {
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

  void addYieldCount(long value) {
    yieldCount += value;
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
   * @return INodeDirectory if any of the quotas have changed. null otherwise.
   *
   * @see #unprotectedSetQuota(String, long, long)
   */
  INodeDirectory setQuota(String src, long nsQuota, long dsQuota, long nsCount,
      long dsCount) throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException, IOException {
    return unprotectedSetQuota(src, nsQuota, dsQuota, nsCount, dsCount);
  }

    /**
   * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
   * Sets quota for for a directory.
   * @returns INodeDirectory if any of the quotas have changed. null otherwise.
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
         nsQuota != HdfsConstants.QUOTA_RESET) || 
        (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET && 
          dsQuota != HdfsConstants.QUOTA_RESET)) {
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
  boolean setTimes(INode inode, long mtime, long atime, boolean force)
      throws StorageException, TransactionContextException,
      AccessControlException {
    return unprotectedSetTimes(inode, mtime, atime, force);
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
    createRoot(
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        true);
    nameCache.reset();
  }

  /**
   * Add the specified path into the namespace.
   */
  INodeSymlink addSymlink(long id, String path, String target,
                          long mtime, long atime, PermissionStatus perm)
          throws UnresolvedLinkException, QuotaExceededException, IOException {
    return unprotectedAddSymlink(id, path, target, mtime, atime, perm);
  }

  INodeSymlink unprotectedAddSymlink(long id, String path, String target, long mtime,
      long atime, PermissionStatus perm)
      throws IOException {
    final INodeSymlink symlink = new INodeSymlink(id, target, mtime, atime, perm);
    return addINode(path, symlink)? symlink: null;
  }

  List<AclEntry> modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    return unprotectedModifyAclEntries(src, aclSpec);
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

  List<AclEntry> removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    return unprotectedRemoveAclEntries(src, aclSpec);
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

  List<AclEntry> removeDefaultAcl(String src) throws IOException {
    return unprotectedRemoveDefaultAcl(src);
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

  List<AclEntry> setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    return unprotectedSetAcl(src, aclSpec);
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
  INode getINode4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINodesInPath4Write(src, resolveLink).getLastINode();
  }
  /**
   * @return the INodesInPath of the components in src
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INodesInPath getINodesInPath4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    final byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = INodesInPath.resolve(getRootDir(), components,
            components.length, resolveLink);
    return inodesInPath;
  }
  
    /**
   * Get {@link INode} associated with the file / directory.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  public INode getINode4Write(String src) throws UnresolvedLinkException, StorageException, TransactionContextException{
      return getINode4Write(src, true);
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
  
  FSPermissionChecker getPermissionChecker()
    throws AccessControlException {
    try {
      return new FSPermissionChecker(fsOwnerShortUserName, supergroup,
          NameNode.getRemoteUser());
    } catch (IOException ioe) {
      throw new AccessControlException(ioe);
    }
  }

  void checkOwner(FSPermissionChecker pc, String path)
      throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, true, null, null, null, null);
  }

  void checkPathAccess(FSPermissionChecker pc, String path,
                       FsAction access)
      throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, false, null, null, access, null);
  }
  void checkParentAccess(
      FSPermissionChecker pc, String path, FsAction access)
      throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, false, null, access, null, null);
  }

  void checkAncestorAccess(
      FSPermissionChecker pc, String path, FsAction access)
      throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, false, access, null, null, null);
  }

  void checkTraverse(FSPermissionChecker pc, String path)
      throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, false, null, null, null, null);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  private void checkPermission(
    FSPermissionChecker pc, String path, boolean doCheckOwner,
    FsAction ancestorAccess, FsAction parentAccess, FsAction access,
    FsAction subAccess)
    throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, path, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false, true);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(
      FSPermissionChecker pc, String path, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean ignoreEmptyDir, boolean resolveLink)
      throws AccessControlException, UnresolvedLinkException, TransactionContextException, IOException {
    if (!pc.isSuperUser()) {
      pc.checkPermission(path, this, doCheckOwner, ancestorAccess,
          parentAccess, access, subAccess, ignoreEmptyDir, resolveLink);
    }
  }
  
  HdfsFileStatus getAuditFileInfo(String path, boolean resolveSymlink)
    throws IOException {
    return (namesystem.isAuditEnabled() && namesystem.isExternalInvocation())
      ? FSDirStatAndListingOp.getFileInfo(this, path, resolveSymlink, false) : null;
  }
  
  /**
   * Verify that parent directory of src exists.
   */
  void verifyParentDir(String src)
      throws FileNotFoundException, ParentNotDirectoryException,
             UnresolvedLinkException, StorageException, TransactionContextException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      final INode parentNode = getINode(parent.toString());
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
  }
}
