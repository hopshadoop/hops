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
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.Charsets;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.util.Time.now;
import io.hops.metadata.hdfs.dal.DirectoryWithQuotaFeatureDataAccess;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;

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
  private final long contentSleepMicroSec;
  private long yieldCount = 0; // keep track of lock yield count.
  private final int inodeXAttrsLimit; //inode xattrs max limit
  
  private boolean quotaEnabled;

  private final boolean isPermissionEnabled;
  /**
   * Support for ACLs is controlled by a configuration flag. If the
   * configuration flag is false, then the NameNode will reject all
   * ACL-related operations.
   */
  private final boolean aclsEnabled;

  // precision of access times.
  private final long accessTimePrecision;
  // whether setStoragePolicy is allowed.
  private final boolean storagePolicyEnabled;
  // whether quota by storage type is allowed
  private final boolean quotaByStorageTypeEnabled;

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
    this.aclsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LOG.info("ACLs enabled? " + aclsEnabled);
    int configuredLimit = conf.getInt(DFSConfigKeys.DFS_LIST_LIMIT,
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit > 0 ? configuredLimit :
        DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;
    
    this.accessTimePrecision = conf.getLong(
        DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

    this.storagePolicyEnabled =
        conf.getBoolean(DFS_STORAGE_POLICY_ENABLED_KEY,
                        DFS_STORAGE_POLICY_ENABLED_DEFAULT);

    this.quotaByStorageTypeEnabled =
        conf.getBoolean(DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY,
                        DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT);

    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    this.contentSleepMicroSec = conf.getLong(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT);
    
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
  
    int inodeXAttrs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);
    Preconditions.checkArgument(inodeXAttrs >= 0,
        "Cannot set a negative limit on the number of xattrs per inode (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    
    if(inodeXAttrs > XAttrStorage.getMaxNumberOfUserXAttrPerInode()){
      inodeXAttrs = XAttrStorage.getMaxNumberOfUserXAttrPerInode();
    }
    
    this.inodeXAttrsLimit = inodeXAttrs;
    
    NameNode.LOG.info("The maximum number of xattrs per inode is set to " + inodeXAttrsLimit);
    
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

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    return getBlockManager().getStoragePolicySuite();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
  boolean isAclsEnabled() {
    return aclsEnabled;
  }

  boolean isStoragePolicyEnabled() {
    return storagePolicyEnabled;
  }
  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  boolean isQuotaByStorageTypeEnabled() {
    return quotaByStorageTypeEnabled;
  }

  int getLsLimit() {
    return lsLimit;
  }

  int getContentCountLimit() {
    return contentCountLimit;
  }

  long getContentSleepMicroSec() {
    return contentSleepMicroSec;
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
  INodesInPath addFile(INodesInPath existing, String localName, PermissionStatus 
      permissions, short replication, long preferredBlockSize,
      String clientName, String clientMachine)
    throws IOException {

    long modTime = now();
    
    INodeFile newNode = new INodeFile(IDsGeneratorFactory.getInstance().getUniqueINodeID(), permissions,
        BlockInfoContiguous.EMPTY_ARRAY, replication, modTime, modTime, preferredBlockSize, (byte) 0);
    newNode.setLocalNameNoPersistance(localName.getBytes(Charsets.UTF_8));
    newNode.toUnderConstruction(clientName, clientMachine);

    INodesInPath newiip;
    newiip = addINode(existing, newNode);

    if (newiip == null) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " +
          existing.getPath() + "/" + localName);
      return null;
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + localName + " is added");
    }
    return newiip;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfoContiguous addBlock(String path, INodesInPath inodesInPath, Block block,
      DatanodeStorageInfo targets[])
      throws QuotaExceededException, StorageException,
      TransactionContextException, IOException {
    final INodeFile fileINode = inodesInPath.getLastINode().asFile();
    Preconditions.checkState(fileINode.isUnderConstruction());

    long diskspaceTobeConsumed = fileINode.getPreferredBlockSize();
    //When appending to a small file stored in DB we create a new block that will contain both the data
    //previously in the file and the appended data. We should only add the appended data to the quota
    if(fileINode.isFileStoredInDB()){
      diskspaceTobeConsumed -= fileINode.getSize();
    }
    // check quota limits and updated space consumed
    updateCount(inodesInPath, 0, diskspaceTobeConsumed,
          fileINode.getBlockReplication(), true);

    // associate new last block for the file
    BlockInfoContiguousUnderConstruction blockInfo =
        new BlockInfoContiguousUnderConstruction(block, fileINode.getId(),
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
  boolean removeBlock(String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException, StorageException {
    Preconditions.checkArgument(fileNode.isUnderConstruction());
    return unprotectedRemoveBlock(path, iip, fileNode, block);
  }
  
  boolean unprotectedRemoveBlock(String path, INodesInPath iip, INodeFile fileNode,
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
    updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
        fileNode.getBlockReplication(), true);
    return true;
  }

  /**
   * This is a wrapper for resolvePath(). If the path passed
   * is prefixed with /.reserved/raw, then it checks to ensure that the caller
   * has super user privileges.
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
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(INodesInPath inodesInPath)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    final INode inode = inodesInPath.getLastINode();
    if (inode == null || !inode.isDirectory()) {
      //not found or not a directory
      return false;
    }
    return ((INodeDirectory) inode).getChildrenList().size() != 0;
  }

  /** 
   * Check whether the filepath could be created
   */
  boolean isValidToCreate(String src, INodesInPath iip)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    String srcs = normalizePath(src);
    return srcs.startsWith("/") && !srcs.endsWith("/")
        && iip.getLastINode() == null;
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    src = normalizePath(src);
    INode node = getINode(src, false);
    return node != null && node.isDirectory();
  }

  /** Updates namespace, storagespace and typespaces consumed for all
   * directories until the parent directory of file represented by path.
   *
   * @param iip the INodesInPath instance containing all the INodes for
   *            updating quota usage
   * @param nsDelta the delta change of namespace
   * @param ssDelta the delta change of storage space consumed without replication
   * @param replication the replication factor of the block consumption change
   * @throws QuotaExceededException
   *     if the new count violates any quota limit
   * @throws FileNotFoundException
   *     if path does not exist.
   */
  void updateSpaceConsumed(INodesInPath iip, long nsDelta, long ssDelta, short replication)
    throws QuotaExceededException, FileNotFoundException,
    UnresolvedLinkException, StorageException,
    TransactionContextException {
      if (iip.getLastINode() == null) {
        throw new FileNotFoundException("Path not found: " + iip.getPath());
    }
    updateCount(iip, nsDelta, ssDelta, replication, true);
  }
  
  /**
   * Update usage count without replication factor change
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short replication,
      boolean checkQuota) throws QuotaExceededException, StorageException, TransactionContextException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
      getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta,
          replication, replication);;
    updateCount(iip, iip.length() - 1,
      new QuotaCounts.Builder().nameSpace(nsDelta).storageSpace(ssDelta * replication).
          typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }
  
  /**
   * Update usage count with replication factor change due to setReplication
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short oldRep,
      short newRep, boolean checkQuota) throws QuotaExceededException, StorageException, TransactionContextException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
        getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta, oldRep, newRep);
    updateCount(iip, iip.length() - 1,
        new QuotaCounts.Builder().nameSpace(nsDelta).
            storageSpace(ssDelta * (newRep - oldRep)).
            typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }
  
  /**
   * update count of each inode with quota
   *
   * @param inodes
   *     an array of inodes on a path
   * @param numOfINodes
   *     the number of inodes to update starting from index 0
   * @param counts the count of space/namespace/type usage to be update
   * @param checkQuota
   *     if true then check if quota is exceeded
   * @throws QuotaExceededException
   *     if the new count violates any quota limit
   */
  private void updateCount(INodesInPath iip, int numOfINodes,
      QuotaCounts counts, boolean checkQuota)
      throws QuotaExceededException, StorageException,
      TransactionContextException {
    if (!isQuotaEnabled()) {
      return;
    }
    
    if (!namesystem.isImageLoaded()) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes > iip.length()) {
      numOfINodes = iip.length();
    }
    if (checkQuota) {
      verifyQuota(iip, numOfINodes, counts, null);
    }
    INode iNode = iip.getINode(numOfINodes - 1);
    namesystem.getQuotaUpdateManager()
        .addUpdate(iNode.getId(), counts);
  }
  
  /**
   * update quota of each inode and check to see if quota is exceeded.
   * See {@link #updateCount(INodesInPath, int, QuotaCounts, boolean)}
   */
  void updateCountNoQuotaCheck(INodesInPath inodesInPath, 
      int numOfINodes, QuotaCounts counts)
      throws StorageException, TransactionContextException {
    try {
      updateCount(inodesInPath, numOfINodes, counts, false);
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
  void unprotectedUpdateCount(INodesInPath inodesInPath, 
      int numOfINodes, QuotaCounts counts) throws StorageException, TransactionContextException {
    if (!isQuotaEnabled()) {
      return;
    }
    INode iNode = inodesInPath.getINode(numOfINodes - 1);
    namesystem.getQuotaUpdateManager()
        .addUpdate(iNode.getId(), counts);
  }

  public EnumCounters<StorageType> getStorageTypeDeltas(byte storagePolicyID,
      long dsDelta, short oldRep, short newRep) {
    EnumCounters<StorageType> typeSpaceDeltas =
        new EnumCounters<StorageType>(StorageType.class);
    // Storage type and its quota are only available when storage policy is set
    if (storagePolicyID != BlockStoragePolicySuite.ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {
        List<StorageType> oldChosenStorageTypes =
            storagePolicy.chooseStorageTypes(oldRep);

        for (StorageType t : oldChosenStorageTypes) {
          if (!t.supportTypeQuota()) {
            continue;
          }
          Preconditions.checkArgument(dsDelta > 0);
          typeSpaceDeltas.add(t, -dsDelta);
        }
      }

      List<StorageType> newChosenStorageTypes =
          storagePolicy.chooseStorageTypes(newRep);

      for (StorageType t : newChosenStorageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        typeSpaceDeltas.add(t, dsDelta);
      }
    }
    return typeSpaceDeltas;
  }

  /** Return the name of the path represented by inodes at [0, pos] */
  static String getFullPathName(INode[] inodes, int pos) {
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
   * @param existing the INodesInPath containing all the ancestral INodes
   * @param child the new INode to add
   * @return a new INodesInPath instance containing the new child INode. Null
   * if the adding fails.
   * @throw QuotaExceededException is thrown if it violates quota limit
   */
  INodesInPath addINode(INodesInPath existing, INode child)
      throws IOException {
    cacheName(child);
    return addLastINode(existing, child, true);
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and storagespace to a given position.
   *  
   * @param iip INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param deltas needed namespace, storagespace and storage types
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  static void verifyQuota(INodesInPath iip, int pos, QuotaCounts deltas,
                          INode commonAncestor) throws QuotaExceededException, StorageException,
      TransactionContextException {
    if (deltas.getNameSpace() <= 0 && deltas.getStorageSpace() <= 0
        && deltas.getTypeSpaces().allLessOrEqual(0L)) {
      // if quota is being freed or not being consumed
      return;
    }
    
      // check existing components in the path
    for (int i = (pos > iip.length() ? iip.length() : pos) - 1; i >= 0; i--) {
      if (commonAncestor!=null && commonAncestor.equals(iip.getINode(i))) {
        // Moving an existing node. Stop checking for quota when common
        // ancestor is reached
        return;
      }
      final DirectoryWithQuotaFeature q = iip.getINode(i).asDirectory().getDirectoryWithQuotaFeature();
      if (q != null) { // a directory with quota
        try {
          q.verifyQuota(deltas);
        } catch (QuotaExceededException e) {
          List<INode> inodes = iip.getReadOnlyINodes();
          final String path = getFullPathName(inodes.toArray(new INode[inodes.size()]), i);
          e.setPathName(path);
          throw e;
        }
      }
    }
  }
  
  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath String containing parent path
   * @throws PathComponentTooLongException child's name is too long.
   */
  void verifyMaxComponentLength(byte[] childName, String parentPath)
      throws PathComponentTooLongException {
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, parentPath,
          DFSUtil.bytes2String(childName));
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
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  void verifyMaxDirItems(INodeDirectory parent, String parentPath)
      throws MaxDirectoryItemsExceededException, StorageException, TransactionContextException {
    if (maxDirItems <= 0) {
      return;
    }
    final int count = parent.getChildrenNum();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(maxDirItems, count);
      if (namesystem.isImageLoaded()) {
        e.setPathName(parentPath);
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
  protected <T extends INode> void verifyFsLimits(byte[] childName, String parentPath, INodeDirectory parent)
      throws FSLimitException, StorageException, TransactionContextException {
    boolean includeChildName = false;
    try {
      if (maxComponentLength != 0) {
        int length = childName.length;
        if (length > maxComponentLength) {
          includeChildName = true;
          throw new PathComponentTooLongException(maxComponentLength, length, parentPath,
              DFSUtil.bytes2String(childName));
        }
      }
      if (maxDirItems != 0) {
        int count = parent.getChildrenNum();
        if (count >= maxDirItems) {
          throw new MaxDirectoryItemsExceededException(maxDirItems, count);
        }
      }
    } catch (FSLimitException e) {
      String badPath = parentPath;
      if (includeChildName) {
        badPath += Path.SEPARATOR + childName;
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
  
  INodesInPath addLastINode(INodesInPath existing,
      INode inode, boolean checkQuota) throws QuotaExceededException, StorageException, IOException {
    assert existing.getLastINode() != null &&
        existing.getLastINode().isDirectory();
    
    QuotaCounts counts = new QuotaCounts.Builder().build();
    if (isQuotaEnabled()) {       //HOP
      inode.computeQuotaUsage(getBlockStoragePolicySuite(), counts);
    }
    return addLastINode(existing, inode, counts, checkQuota, true);
  }
  
  /**
   * Add a child to the end of the path specified by INodesInPath.
   * @return an INodesInPath instance containing the new INode
   */
  INodesInPath addLastINode(INodesInPath existing,
      INode inode, QuotaCounts counts, boolean checkQuota, boolean
      logMetadataEvent) throws
      QuotaExceededException, StorageException, IOException {
    assert existing.getLastINode() != null &&
        existing.getLastINode().isDirectory();
    
    final int pos = existing.length();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    if (pos == 1 && existing.getINode(0) == getRootDir() && isReservedName(inode)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + inode.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    final INodeDirectory parent = existing.getINode(pos - 1).asDirectory();
    // The filesystem limits are not really quotas, so this check may appear
    // odd.  It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location because a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      final String parentPath = existing.getPath();
      verifyMaxComponentLength(inode.getLocalNameBytes(), parentPath);
      verifyMaxDirItems(parent, parentPath);
    }

    updateCount(existing, pos, counts, checkQuota);
    final boolean added = parent.addChild(inode,
        true, logMetadataEvent);
    if (!added) {
      updateCountNoQuotaCheck(existing, pos, counts.negation());
      return null;
    } 

    INodesInPath iip = INodesInPath.append(existing, inode, inode.getLocalNameBytes());
    if (added) {
      if (!inode.isDirectory()) {
        List<INode> pathInodes = new ArrayList<>(pos+1);
        int i=0;
        for(INode node: existing.getReadOnlyINodes()){
          pathInodes.add(node);
          i++;
          if(i==pos){
            pathInodes.add(inode);
            break;
          }
        }
        
        String path = iip.getPath();
        Cache.getInstance().set(path, pathInodes);
      }
    }
    //
    return iip;
  }
  
  INodesInPath addLastINodeNoQuotaCheck(INodesInPath existing, INode inode, QuotaCounts counts)
      throws IOException {
    try {
      return addLastINode(existing, inode, counts, false, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return null;
  }
  
  long removeLastINode(final INodesInPath iip) throws IOException {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    if (isQuotaEnabled()) {
      iip.getLastINode().computeQuotaUsage(getBlockStoragePolicySuite(), counts);
    }
    return removeLastINode(iip, false, counts);
  }
  
  /**
   * Remove the last inode in the path from the namespace.
   * Count of each ancestor with quota is also updated.
   * @return the removed node; null if the removal fails.
   */
  long removeLastINode(final INodesInPath iip, boolean forRename,
          final QuotaCounts counts)
      throws IOException {
    final INode last = iip.getLastINode();
    final INodeDirectory parent = iip.getINode(-2).asDirectory();
    if(!forRename){
      if(!parent.removeChild(last)){
        return -1;
      }
    } else {
      ((INodeDirectory)parent).decreaseChildrenNum();
    }

    if (isQuotaEnabled()) {
      
      
      QuotaCounts outStandingDelta= new QuotaCounts.Builder().build();
      
      //apply the quota update that have not been applied yet in order for them to be forwarded to the parent eventhough
      //they will be droped due to the fact that the dir will not exist anymore when trying to apply them
      
      //if the removed Inode has quota set, counts already contains the usage that need to be removed from 
      //the parent, update not applied yet should be canceled by this update (removal will already be part off the value
      //of counts and add will be added to be removed imediatly
      if (!last.isQuotaSet()) {
        //if the removed Inode does not have quota set we need to compute the ammount of data that need to be removed
        //if children have been removed before and the removal quota update is not applied yet this need to be 
        //propagated to the parent
        List<QuotaUpdate> outstandingUpdates = (List<QuotaUpdate>) EntityManager
            .findList(QuotaUpdate.Finder.ByINodeId, last.getId());
        for (QuotaUpdate update : outstandingUpdates) {
          EnumCounters<StorageType> typeCounts = new EnumCounters<StorageType>(StorageType.class);
          for(StorageType type: StorageType.asList()){
            typeCounts.add(type, update.getTypeSpaces().get(QuotaUpdate.StorageType.valueOf(type.name())));
          }
          QuotaCounts up = new QuotaCounts.Builder().storageSpace(update.getStorageSpaceDelta()).nameSpace(update.
              getNamespaceDelta()).typeSpaces(typeCounts).build();
          outStandingDelta.add(up);
        }
      }
      outStandingDelta.add(counts.negation());
      updateCountNoQuotaCheck(iip, iip.length() - 1, outStandingDelta);
      return counts.getNameSpace();
    }
    return 1;
  }
  
  /**
   */
  static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }
  
  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
  }

  void addYieldCount(long value) {
    yieldCount += value;
  }

  boolean truncate(INodesInPath iip, long newLength,
                   BlocksMapUpdateInfo collectedBlocks,
                   long mtime, QuotaCounts delta)
      throws IOException {
    return unprotectedTruncate(iip, newLength, collectedBlocks, mtime, delta);
  }

  /**
   * Truncate has the following properties:
   * 1.) Any block deletions occur now.
   * 2.) INode length is truncated now – new clients can only read up to
   * the truncated length.
   * 3.) INode will be set to UC and lastBlock set to UNDER_RECOVERY.
   * 4.) NN will trigger DN truncation recovery and waits for DNs to report.
   * 5.) File is considered UNDER_RECOVERY until truncation recovery completes.
   * 6.) Soft and hard Lease expiration require truncation recovery to complete.
   *
   * @return true if on the block boundary or false if recovery is need
   */
  boolean unprotectedTruncate(INodesInPath iip, long newLength,
                              BlocksMapUpdateInfo collectedBlocks,
                              long mtime, QuotaCounts delta) throws IOException {
    INodeFile file = iip.getLastINode().asFile();
    
    verifyQuotaForTruncate(iip, file, newLength, delta);
    
    long remainingLength =
        file.collectBlocksBeyondMax(newLength, collectedBlocks);
    file.setModificationTime(mtime);
    // If on block boundary, then return
    return (remainingLength - newLength) == 0;
  }

  private void verifyQuotaForTruncate(INodesInPath iip, INodeFile file,
      long newLength, QuotaCounts delta) throws QuotaExceededException, 
      TransactionContextException, StorageException {
    if (!getFSNamesystem().isImageLoaded()) {
      // Do not check quota if edit log is still being processed
      return;
    }
    final long diff = file.computeQuotaDeltaForTruncate(newLength);
    final short repl = file.getBlockReplication();
    delta.addStorageSpace(diff * repl);
    final BlockStoragePolicy policy = getBlockStoragePolicySuite()
        .getPolicy(file.getStoragePolicyID());
    List<StorageType> types = policy.chooseStorageTypes(repl);
    for (StorageType t : types) {
      if (t.supportTypeQuota()) {
        delta.addTypeSpace(t, diff);
      }
    }
    if (diff > 0) {
      verifyQuota(iip, iip.length() - 1, delta, null);
    }
  }
//  /**
//   * Update the count of each directory with quota in the namespace
//   * A directory's count is defined as the total number inodes in the tree
//   * rooted at the directory.
//   * <p/>
//   * This is an update of existing state of the filesystem and does not
//   * throw QuotaExceededException.
//   */
//  void updateCountForINodeWithQuota()
//      throws StorageException, TransactionContextException {
//    // HOP If this one is used for something then we need to modify it to use the QuotaUpdateManager
//    updateCountForINodeWithQuota(this, getRootDir(), new INode.DirCounts(),
//        new ArrayList<INode>(50));
//  }
//  
//  /**
//   * Update the count of the directory if it has a quota and return the count
//   * <p/>
//   * This does not throw a QuotaExceededException. This is just an update
//   * of of existing state and throwing QuotaExceededException does not help
//   * with fixing the state, if there is a problem.
//   *
//   * @param dir
//   *     the root of the tree that represents the directory
//   * @param counts
//   *     counters for name space and disk space
//   * @param nodesInPath
//   *     INodes for the each of components in the path.
//   */
//  private static void updateCountForINodeWithQuota(FSDirectory fsd, INodeDirectory dir,
//      INode.DirCounts counts, ArrayList<INode> nodesInPath)
//      throws StorageException, TransactionContextException {
//    long parentNamespace = counts.nsCount;
//    long parentDiskspace = counts.dsCount;
//    
//    counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
//    counts.dsCount = 0L;
//    
//    /* We don't need nodesInPath if we could use 'parent' field in
//     * INode. using 'parent' is not currently recommended. */
//    nodesInPath.add(dir);
//
//    for (INode child : dir.getChildrenList()) {
//      if (child.isDirectory()) {
//        updateCountForINodeWithQuota(fsd, (INodeDirectory) child, counts,
//            nodesInPath);
//      } else if (child.isSymlink()) {
//        counts.nsCount += 1;
//      } else { // reduce recursive calls
//        counts.nsCount += 1;
//        counts.dsCount += ((INodeFile) child).diskspaceConsumed();
//      }
//    }
//
//    if (dir.isQuotaSet()) {
//      final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
//      if (q != null) {
//        q.setSpaceConsumed(dir, counts.nsCount, counts.dsCount);
//      }
//
//      // check if quota is violated for some reason.
//      final Quota.Counts oldQuota = dir.getQuotaCounts();
//      if ((oldQuota.get(Quota.NAMESPACE) >= 0 && counts.nsCount > oldQuota.get(Quota.NAMESPACE)) ||
//          (oldQuota.get(Quota.DISKSPACE) >= 0 && counts.dsCount > oldQuota.get(Quota.DISKSPACE))) {
//
//        // can only happen because of a software bug. the bug should be fixed.
//        StringBuilder path = new StringBuilder(512);
//        for (INode n : nodesInPath) {
//          path.append('/');
//          path.append(n.getLocalName());
//        }
//        
//        NameNode.LOG.warn("Quota violation in image for " + path +
//            " (Namespace quota : " + oldQuota.get(Quota.NAMESPACE) +
//            " consumed : " + counts.nsCount + ")" +
//            " (Diskspace quota : " + oldQuota.get(Quota.DISKSPACE) +
//            " consumed : " + counts.dsCount + ").");
//      }
//    }
//
//    // pop
//    nodesInPath.remove(nodesInPath.size() - 1);
//    
//    counts.nsCount += parentNamespace;
//    counts.dsCount += parentDiskspace;
//  }
  
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
   * Reset the entire namespace tree.
   */
  void reset() throws IOException {
    createRoot(
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        true);
    nameCache.reset();
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
    return src.startsWith(DOT_RESERVED_PATH_PREFIX + Path.SEPARATOR);
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
  
  static INode resolveLastINode(INodesInPath iip) throws FileNotFoundException {
    INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("cannot find " + iip.getPath());
    }
    return inode;
  }

  INodesInPath getExistingPathINodes(byte[][] components)
      throws UnresolvedLinkException, StorageException, TransactionContextException {
    return INodesInPath.resolve(getRootDir(), components, false);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INodesInPath getINodesInPath4Write(String src)
      throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINodesInPath4Write(src, true);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  public INode getINode4Write(String src) throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINodesInPath4Write(src, true).getLastINode();
  }

    /** @return the {@link INodesInPath} containing all inodes in the path. */
  public INodesInPath getINodesInPath(String path, boolean resolveLink) throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    final byte[][] components = INode.getPathComponents(path);
    return INodesInPath.resolve(getRootDir(), components, resolveLink);
  }
  /** @return the last inode in the path. */
  INode getINode(String path, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINodesInPath(path, resolveLink).getLastINode();
  }

  /**
   *  Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src) throws UnresolvedLinkException, StorageException, TransactionContextException {
    return getINode(src, true);
  }
  
  /**
   * Get {@link INode} associated with the file / directory.
   */
  INodesInPath getINodesInPath4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, StorageException, TransactionContextException {
    final byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = INodesInPath.resolve(getRootDir(), components,
            resolveLink);
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
                    HdfsConstantsClient.GRANDFATHER_INODE_ID, INodeDirectory.getRootDirPartitionKey());
            if (rootInode == null || overwrite == true) {
              newRootINode = INodeDirectory.createRootDir(ps);

              DirectoryWithQuotaFeature quotaFeature = new DirectoryWithQuotaFeature.Builder(newRootINode.getId()).
                  nameSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA).
                  storageSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA).build();
              
              DirectoryWithQuotaFeatureDataAccess ida =
                  (DirectoryWithQuotaFeatureDataAccess) HdfsStorageFactory
                      .getDataAccess(DirectoryWithQuotaFeatureDataAccess.class);
              
              newRootINode.addDirectoryWithQuotaFeature(quotaFeature);
              
              // Set the block storage policy to DEFAULT
              List<INode> newINodes = new ArrayList();
              newINodes.add(newRootINode);
              da.prepare(INode.EMPTY_LIST, newINodes, INode.EMPTY_LIST);
              
              List<DirectoryWithQuotaFeature> attrList = new ArrayList();
              attrList.add(quotaFeature);
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

  void checkOwner(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException, IOException {
    checkPermission(pc, iip, true, null, null, null, null);
  }

  void checkPathAccess(FSPermissionChecker pc, INodesInPath iip,
                       FsAction access)
      throws AccessControlException, IOException {
    checkPermission(pc, iip, false, null, null, access, null);
  }
  void checkParentAccess(
      FSPermissionChecker pc, INodesInPath iip, FsAction access)
      throws AccessControlException, IOException {
    checkPermission(pc, iip, false, null, access, null, null);
  }

  void checkAncestorAccess(
      FSPermissionChecker pc, INodesInPath iip, FsAction access)
      throws AccessControlException, IOException {
    checkPermission(pc, iip, false, access, null, null, null);
  }

  void checkTraverse(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException, IOException {
    checkPermission(pc, iip, false, null, null, null, null);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(
    FSPermissionChecker pc, INodesInPath iip, boolean doCheckOwner,
    FsAction ancestorAccess, FsAction parentAccess, FsAction access,
    FsAction subAccess)
    throws AccessControlException, UnresolvedLinkException, IOException {
    checkPermission(pc, iip, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(
      FSPermissionChecker pc, INodesInPath iip, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException, UnresolvedLinkException, TransactionContextException, IOException {
    if (!pc.isSuperUser()) {
      pc.checkPermission(iip, doCheckOwner, ancestorAccess,
          parentAccess, access, subAccess, ignoreEmptyDir);
    }
  }
  
  HdfsFileStatus getAuditFileInfo(INodesInPath iip)
    throws IOException {
    return (namesystem.isAuditEnabled() && namesystem.isExternalInvocation())
      ? FSDirStatAndListingOp.getFileInfo(this, iip, false) : null;
  }
  
  /**
   * Verify that parent directory of src exists.
   */
  void verifyParentDir(INodesInPath iip, String src)
      throws FileNotFoundException, ParentNotDirectoryException,
             StorageException, TransactionContextException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      final INode parentNode = iip.getINode(-2);
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
  }
  
  
  void removeXAttr(String src, XAttr xAttr) throws IOException {
    XAttr removedXAttr = unprotectedRemoveXAttr(src, xAttr);
    if (removedXAttr == null) {
      NameNode.stateChangeLog.info("DIR* FSDirectory.removeXAttr: XAttr " +
          XAttrHelper.getPrefixName(xAttr) +
          " does not exist on the path " + src);
    }
  }
  
  private XAttr unprotectedRemoveXAttr(String src,
      XAttr xAttr) throws IOException {
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(iip);
    XAttr attr = XAttrStorage.readINodeXAttr(inode, xAttr);
    if(attr != null){
      XAttrStorage.removeINodeXAttr(inode, xAttr);
      decrementXAttrs(inode, xAttr);
      return xAttr;
    }
    return null;
  }
  
  
  void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
      unprotectedSetXAttr(src, xAttr, flag);
  }
  
  void unprotectedSetXAttr(String src, XAttr xAttr,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    INodesInPath iip = getINodesInPath4Write(normalizePath(src), true);
    INode inode = resolveLastINode(iip);
    XAttr storedXAtttr = setINodeXAttr(inode, xAttr, flag);
    XAttrStorage.updateINodeXAttr(inode, xAttr, storedXAtttr != null);
  
  }
  
  XAttr setINodeXAttr(INode inode, XAttr xAttr,
      EnumSet<XAttrSetFlag> flag) throws QuotaExceededException, IOException {
  
    XAttr storedXAtttr = XAttrStorage.readINodeXAttr(inode, xAttr);
    XAttrSetFlag.validate(xAttr.getName(), storedXAtttr != null, flag);
    
    incrementXAttrs(inode, xAttr, storedXAtttr != null);
    
    if(isUserVisible(xAttr)) {
      if (inode.getNumUserXAttrs() > inodeXAttrsLimit) {
        throw new IOException("Cannot add additional XAttr to inode, "
            + "would exceed limit of " + inodeXAttrsLimit);
      }
    }else {
      if (inode.getNumSysXAttrs() >
          XAttrStorage.getMaxNumberOfSysXAttrPerInode()) {
        throw new IOException("Cannot add additional System XAttr to inode, "
            + "would exceed limit of " +
            XAttrStorage.getMaxNumberOfSysXAttrPerInode());
      }
    }
    
    return storedXAtttr;
  }
  
  private boolean isUserVisible(XAttr xAttr) {
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER ||
        xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED) {
      return true;
    }
    return false;
  }
  
  private void incrementXAttrs(INode inode, XAttr xAttr, boolean xAttrExists)
      throws TransactionContextException, StorageException {
    if(xAttrExists)
      return;
    
    if(isUserVisible(xAttr)) {
      inode.incrementUserXAttrs();
    }else {
      inode.incrementSysXAttrs();
    }
  }
  
  private void decrementXAttrs(INode inode, XAttr xAttr)
      throws TransactionContextException, StorageException {
    if(isUserVisible(xAttr)) {
      inode.decrementUserXAttrs();
    }else {
      inode.decrementSysXAttrs();
    }
  }
  
  List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
    INodesInPath iip = getINodesInPath(normalizePath(src), true);
    INode inode = resolveLastINode(iip);
    return XAttrStorage.readINodeXAttrs(inode, xAttrs);
  }
  
  List<XAttr> getXAttrs(String src) throws IOException {
    return getXAttrs(src, Collections.<XAttr>emptyList());
  }
}
