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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;

import static org.apache.hadoop.util.Time.now;

class FSDirRenameOp {
  public static final Log LOG = LogFactory.getLog(FSDirRenameOp.class);
  
  static boolean renameToInt(
      FSDirectory fsd, final String srcArg, final String dstArg)
      throws IOException {
    
    String src = srcArg;
    String dst = dstArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    
    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    HdfsFileStatus resultingStat = null;
    src = fsd.resolvePath(src, srcComponents, fsd);
    dst = fsd.resolvePath(dst, dstComponents, fsd);
    
     @SuppressWarnings("deprecation")
    final boolean status = renameToInternal(fsd, src, dst);
    
    return status;   
  }

  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   */
  static void verifyQuotaForRename(FSDirectory fsd,
      INode[] src, INode[] dst, INode.DirCounts srcCounts,
      INode.DirCounts dstCounts)
      throws QuotaExceededException, StorageException, TransactionContextException {
    if (!fsd.getFSNamesystem().isImageLoaded() || !fsd.isQuotaEnabled()) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode commonAncestor = null;
    for (int i = 0; src[i].equals(dst[i]); i++) {
      commonAncestor = src[i];
    }
    long nsDelta = srcCounts.getNsCount();
    long dsDelta = srcCounts.getDsCount();

    // Reduce the required quota by dst that is being removed
    INode dstInode = dst[dst.length - 1];
    if (dstInode != null) {
      nsDelta -= dstCounts.getNsCount();
      dsDelta -= dstCounts.getDsCount();
    }
    FSDirectory.verifyQuota(dst, dst.length - 1, nsDelta, dsDelta,
        commonAncestor);
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   */
  static void verifyFsLimitsForRename(FSDirectory fsd,
      INodesInPath srcIIP,
      INodesInPath dstIIP)
      throws FSLimitException.PathComponentTooLongException,
          FSLimitException.MaxDirectoryItemsExceededException,
          StorageException,
          TransactionContextException {
    byte[] dstChildName = dstIIP.getLastLocalName();
    INode[] dstInodes = dstIIP.getINodes();
    int pos = dstInodes.length - 1;
    fsd.verifyMaxComponentLength(dstChildName, dstInodes, pos);
    // Do not enforce max directory items if renaming within same directory.
    if (!srcIIP.getINode(-2).equals(dstIIP.getINode(-2))) {
      fsd.verifyMaxDirItems(dstInodes, pos);
    }
  }

  /**
   * Change a path name
   *
   * @param fsd FSDirectory
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @deprecated See {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  static boolean unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp)
      throws IOException {
    
    
    
    PathInformation srcInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INodesInPath srcIIP = srcInfo.getINodesInPath();
    final INode srcInode = srcIIP.getLastINode();
    try{
      validateRenameSource(src, srcInfo.getINodesInPath());
    }catch(IOException  ignored){
      return false;
    }
    
    PathInformation dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    if(dstInfo.isDir()){
      dst += Path.SEPARATOR + new Path(src).getName();
    }

    // validate the destination
    if (dst.equals(src)) {
      return true;
    }

    try {
      validateDestination(src, dst, srcInode);
    } catch (IOException ignored) {
      return false;
    }
    
    dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    INodesInPath dstIIP = dstInfo.getINodesInPath();
    if (dstIIP.getLastINode() != null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination " +
          "exists");
      return false;
    }
    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination's " +
          "parent does not exist");
      return false;
    }

    INode srcDataSet = getMetaEnabledParent(srcInfo.getPathInodes());
    INode dstDataSet = getMetaEnabledParent(dstInfo.getPathInodes());
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //TODO [S]  if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation.
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcCounts.nsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    srcCounts.dsCount = srcInfo.getDsCount();
    INode.DirCounts dstCounts = new INode.DirCounts();
    dstCounts.nsCount = dstInfo.getNsCount();
    dstCounts.dsCount = dstInfo.getDsCount();
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;

    String subTreeLockDst = INode.constructPath(dstInfo.getPathComponents(),
        0,  dstInfo.getNumExistingComp());
    if(subTreeLockDst.equals(INodeDirectory.ROOT_NAME)){
      subTreeLockDst = "/"; // absolute path
    }
    try {
      if (isUsingSubTreeLocks) {
        LOG.debug("Rename src: "+src+" dst: "+dst+" requires sub-tree locking mechanism");
        //checkin parentAccess is enough for this operation, no need to pass access argument to QuotaCountingFileTree
        //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
        //access:null, subAccess:null, ignoreEmptyDir:false
        srcSubTreeRoot = fsd.getFSNamesystem().lockSubtreeAndCheckOwnerAndParentPermission(src, false, 
            FsAction.WRITE, SubTreeOperation.Type.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (shouldLogSubtreeInodes(srcInfo, dstInfo, srcDataSet,
              dstDataSet, srcSubTreeRoot)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot, srcDataSet, dstDataSet);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcCounts.nsCount = srcFileTree.getNamespaceCount();
          srcCounts.dsCount = srcFileTree.getDiskspaceCount();
        }
        fsd.getFSNamesystem().delayAfterBbuildingTree("Built tree of "+ src +" for rename. ");
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
        //permission will be checked in renameToTransaction
      }

      boolean retValue = renameToTransaction(fsd, src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0L,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks, logEntries, srcIIP, dstIIP, timestamp);

      // the rename Tx has committed. it has also remove the subTreeLocks
      renameTransactionCommitted = true;

      return retValue;

    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          fsd.getFSNamesystem().unlockSubtree(src, srcSubTreeRoot.getInodeId());
        }
     }
    }
  }

  private static boolean renameToTransaction(final FSDirectory fsd, final String src, final long srcINodeID,
      final String dst, final INode.DirCounts srcCounts, final INode.DirCounts dstCounts,
      final boolean isUsingSubTreeLocks, final Collection<MetadataLogEntry> logEntries, final INodesInPath srcIIP,
      final INodesInPath dstIIP, final long timestamp) throws IOException {

    HopsTransactionalRequestHandler renameToHandler = new HopsTransactionalRequestHandler(
        isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_DEPRICATED_RENAME : HDFSOperationType.DEPRICATED_RENAME,
         src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getLegacyRenameINodeLock(
            INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src, dst)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        if (isUsingSubTreeLocks) {
          il.setIgnoredSTOInodes(srcINodeID);
        }
        locks.add(il)
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.IV, BLK.CR, BLK.ER, BLK.PE, BLK.UR));
        if (!isUsingSubTreeLocks) {
          locks.add(lf.getLeaseLock(LockType.WRITE))
              .add(lf.getLeasePathLock(LockType.READ_COMMITTED));
        } else {
          locks.add(lf.getLeaseLock(LockType.READ_COMMITTED))
              .add(lf.getLeasePathLock(LockType.READ_COMMITTED, src));
        }
        if (fsd.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src, dst));
        }
      }

      @Override
      public Object performTask() throws IOException {

        if (!isUsingSubTreeLocks) {
          if (fsd.isPermissionEnabled()) {
            FSPermissionChecker pc = fsd.getFSNamesystem().getPermissionChecker();
            // Rename does not operates on link targets
            // Do not resolveLink when checking permissions of src and dst
            // Check write access to parent of src
            fsd.getFSNamesystem().checkPermission(pc, src, false, null, FsAction.WRITE, null, null,
                false, false);
            // Check write access to ancestor of dst
            fsd.getFSNamesystem().checkPermission(pc, dst, false, FsAction.WRITE, null, null, null,
                false, false);
          }
        }
        // remove the subtree locks
        removeSubTreeLocksForRenameInternal(fsd, src, isUsingSubTreeLocks);

        for (MetadataLogEntry logEntry : logEntries) {
          EntityManager.add(logEntry);
        }

        AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime(logEntries);

        // Ensure dst has quota to accommodate rename
        verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
        verifyQuotaForRename(fsd, srcIIP.getINodes(), dstIIP.getINodes(), srcCounts, dstCounts);

        RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP, srcCounts);

        boolean added = false;
        INode srcChild = null;

        try {
          // remove src
          srcChild = removeLastINodeForRename(fsd, srcIIP, srcCounts);
          if (srcChild == null) {
            NameNode.stateChangeLog.warn(
                "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src + " to " + dst
                + " because the source can not be removed");
            return false;
          }

          INode srcClone = srcChild.cloneInode();

          srcChild.setLocalNameNoPersistance(dstIIP.getLastLocalName());

          // add src to the destination
          added = tx.addSourceToDestination();
          if (added) {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "DIR* FSDirectory.unprotectedRenameTo: " + src + " is renamed to " + dst);
            }

            tx.updateMtimeAndLease(timestamp);
            tx.updateQuotasInSourceTree();

            logMetadataEventForRename(srcClone, srcChild);

            EntityManager.snapshotMaintenance(
                HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
                srcChild);

            return true;
          }
        } finally {
          if (!added && srcChild != null) {
            tx.restoreSource();
          }
        }
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src + " to " + dst);
        return false;
      }
    };
    return (Boolean) renameToHandler.handle();
  }
  
  /**
   * The new rename which has the POSIX semantic.
   */
  static Map.Entry<BlocksMapUpdateInfo, HdfsFileStatus> renameToInt(
      FSDirectory fsd, final String srcArg, final String dstArg,
      Options.Rename... options)
      throws IOException {
    String src = srcArg;
    String dst = dstArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options -" +
          " " + src + " to " + dst);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }

    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    src = fsd.resolvePath(src, srcComponents);
    dst = fsd.resolvePath(dst, dstComponents);
    HdfsFileStatus resultingStat = renameToInternal(fsd, src, dst, collectedBlocks,
        options);

    return new AbstractMap.SimpleImmutableEntry<BlocksMapUpdateInfo,
        HdfsFileStatus>(collectedBlocks, resultingStat);
  }

  /**
   * @see #unprotectedRenameTo(FSDirectory, String, String, long,
   * org.apache.hadoop.fs.Options.Rename...)
   */
  static HdfsFileStatus renameTo(
      FSDirectory fsd, String src, String dst, long mtime,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    RenameResult ret = unprotectedRenameTo(fsd, src, dst, mtime, collectedBlocks, options);
    if (ret.filesDeleted) {
      fsd.getFSNamesystem().incrDeletedFileCount(1);
    }
    return ret.auditStat;
  }

  /**
   * Rename src to dst.
   * <br>
   * Note: This is to be used by {@link org.apache.hadoop.hdfs.server
   * .namenode.FSEditLog} only.
   * <br>
   *
   * @param fsd       FSDirectory
   * @param src       source path
   * @param dst       destination path
   * @param timestamp modification time
   * @param options   Rename options
   */
  static RenameResult unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp,
      Options.Rename... options)
      throws IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    RenameResult ret = unprotectedRenameTo(fsd, src, dst, timestamp,
        collectedBlocks, options);
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      fsd.getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
    return ret;
  }

  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics and exceptions.
   *
   * @param fsd             FSDirectory
   * @param src             source path
   * @param dst             destination path
   * @param timestamp       modification time
   * @param collectedBlocks blocks to be removed
   * @param options         Rename options
   */
  static RenameResult unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    boolean overwrite = options != null
        && Arrays.asList(options).contains(Options.Rename.OVERWRITE);

    final String error;
    PathInformation srcInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    final INodesInPath srcIIP = srcInfo.getINodesInPath();
    final INode srcInode = srcIIP.getLastINode();
    validateRenameSource(src, srcIIP);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }
    validateDestination(src, dst, srcInode);

    PathInformation dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    INodesInPath dstIIP = dstInfo.getINodesInPath();
    if (dstIIP.getINodes().length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new IOException(error);
    }

    final INode dstInode = dstIIP.getLastINode();
    
    if (dstInode != null) { // Destination exists
      validateOverwrite(fsd, src, dst, overwrite, srcInode, dstInode, dstInfo);
    }

    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new FileNotFoundException(error);
    }
    if (!dstParent.isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new ParentNotDirectoryException(error);
    }

    INode srcDataSet = getMetaEnabledParent(srcInfo.getPathInodes());
    INode dstDataSet = getMetaEnabledParent(dstInfo.getPathInodes());
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //--
    //TODO [S]  if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation.
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcCounts.nsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    srcCounts.dsCount = srcInfo.getDsCount();
    INode.DirCounts dstCounts = new INode.DirCounts();
    dstCounts.nsCount = dstInfo.getNsCount();
    dstCounts.dsCount = dstInfo.getDsCount();
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;
    String subTreeLockDst = INode.constructPath(dstInfo.getPathComponents(),
        0, dstInfo.getNumExistingComp());
    if(subTreeLockDst.equals(INodeDirectory.ROOT_NAME)){
      subTreeLockDst = "/"; // absolute path
    }
    try {
      if (isUsingSubTreeLocks) {
        LOG.debug("Rename src: " + src + " dst: " + dst + " requires sub-tree locking mechanism");
        //checkin parentAccess is enough for this operation, no need to pass access argument to QuotaCountingFileTree
        //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
        //access:null, subAccess:null, ignoreEmptyDir:false
        srcSubTreeRoot = fsd.getFSNamesystem().lockSubtreeAndCheckOwnerAndParentPermission(src, false, 
            FsAction.WRITE, SubTreeOperation.Type.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (shouldLogSubtreeInodes(srcInfo, dstInfo, srcDataSet,
              dstDataSet, srcSubTreeRoot)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot, srcDataSet, dstDataSet);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcCounts.nsCount = srcFileTree.getNamespaceCount();
          srcCounts.dsCount = srcFileTree.getDiskspaceCount();

          fsd.getFSNamesystem().delayAfterBbuildingTree("Built Tree for "+src+" for rename. ");
        }
      } else {
        //permissions are checked in the transaction
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      RenameResult ret = renameToTransaction(fsd, src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks,
              subTreeLockDst, logEntries, srcIIP, dstIIP, timestamp, options);

      renameTransactionCommitted = true;
      
      return ret;
    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          fsd.getFSNamesystem().unlockSubtree(src, srcSubTreeRoot.getInodeId());
        }
      }
    }
      
  }

  private 
  static RenameResult renameToTransaction(final FSDirectory fsd, final String src, final long srcINodeID, final String dst,
      final INode.DirCounts srcCounts, final INode.DirCounts dstCounts,
      final boolean isUsingSubTreeLocks, final String subTreeLockDst,
      final Collection<MetadataLogEntry> logEntries, final INodesInPath srcIIP,
      final INodesInPath dstIIP, final long timestamp,
      final Options.Rename... options) throws IOException {

    return (RenameResult) new HopsTransactionalRequestHandler(
        isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_RENAME : HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getRenameINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH, src, dst)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        if (isUsingSubTreeLocks) {
          il.setIgnoredSTOInodes(srcINodeID);
        }
        locks.add(il)
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.IV,
                BLK.PE, BLK.ER));
        if (fsd.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src, dst));
        }
        if (!isUsingSubTreeLocks) {
          locks.add(lf.getLeaseLock(LockType.WRITE))
              .add(lf.getLeasePathLock(LockType.READ_COMMITTED));
        } else {
          locks.add(lf.getLeaseLock(LockType.WRITE))
              .add(lf.getLeasePathLock(LockType.WRITE, src));
        }
        if (fsd.getFSNamesystem().isErasureCodingEnabled()) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, dst));
        }
        locks.add(lf.getAcesLock());

        for (Rename op : options) {
          if (op == Options.Rename.OVERWRITE) {
            locks.add(lf.getAllUsedHashBucketsLock());
          }
        }
      }

      @Override
      public Object performTask() throws IOException {
        
        if (!isUsingSubTreeLocks) {
          if (fsd.isPermissionEnabled()) {
            FSPermissionChecker pc = fsd.getFSNamesystem().getPermissionChecker();
            // Rename does not operates on link targets
            // Do not resolveLink when checking permissions of src and dst
            // Check write access to parent of src
            fsd.checkPermission(pc, src, false, null, FsAction.WRITE, null, null,
                false, false);
            // Check write access to ancestor of dst
            fsd.checkPermission(pc, dst, false, FsAction.WRITE, null, null, null,
                false, false);
          }
        }

        for (MetadataLogEntry logEntry : logEntries) {
          EntityManager.add(logEntry);
        }

        AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime(logEntries);

        for (Options.Rename op : options) {
          if (op == Rename.KEEP_ENCODING_STATUS) {
            INodesInPath srcInodesInPath = fsd.getINodesInPath(src, false);
            INode[] srcNodes = srcInodesInPath.getINodes();
            INodesInPath dstInodesInPath = fsd.getINodesInPath(dst, false);
            INode[] dstNodes = dstInodesInPath.getINodes();
            INode srcNode = srcNodes[srcNodes.length - 1];
            INode dstNode = dstNodes[dstNodes.length - 1];
            EncodingStatus status = EntityManager.find(
                EncodingStatus.Finder.ByInodeId, dstNode.getId());
            EncodingStatus newStatus = new EncodingStatus(status);
            newStatus.setInodeId(srcNode.getId(), srcNode.isInTree());
            EntityManager.add(newStatus);
            EntityManager.remove(status);
            break;
          }
        }

        removeSubTreeLocksForRenameInternal(fsd, src, isUsingSubTreeLocks);

        // Ensure dst has quota to accommodate rename
        verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
        verifyQuotaForRename(fsd, srcIIP.getINodes(), dstIIP.getINodes(), srcCounts, dstCounts);

        RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP, srcCounts);

        boolean undoRemoveSrc = true;
        INode removedSrc = removeLastINodeForRename(fsd, srcIIP, srcCounts);
        if (removedSrc == null) {
          String error = "Failed to rename " + src + " to " + dst + " because the source can not be removed";
          NameNode.stateChangeLog
              .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
          throw new IOException(error);
        }

        INode srcClone = removedSrc.cloneInode();

        String dstChildName = null;
        boolean undoRemoveDst = false;
        INode removedDst = null;
        try {
          if (dstIIP.getLastINode() != null) { // dst exists remove it
            removedDst = removeLastINode(fsd, dstIIP, dstCounts);
            dstChildName = removedDst.getLocalName();
            undoRemoveDst = true;
          }

          // add src as dst to complete rename
          if (tx.addSourceToDestination()) {
            undoRemoveSrc = false;
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "DIR* FSDirectory.unprotectedRenameTo: " + src + " is renamed to " + dst);
            }
            tx.updateMtimeAndLease(timestamp);

            // Collect the blocks and remove the lease for previous dst
            boolean filesDeleted = false;
            if (removedDst != null) {
              INode  rmdst = removedDst;
              undoRemoveDst = false;
              BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
              filesDeleted = true;
              // rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
              // [S] as the dst dir was empty it will always return 1
              // if the destination is file then we need to collect the blocks for it
              if (rmdst instanceof INodeFile && !((INodeFile) rmdst).isFileStoredInDB()) {
                Block[] blocks = ((INodeFile) rmdst).getBlocks();
                for (Block blk : blocks) {
                  collectedBlocks.addDeleteBlock(blk);
                }
              } else if (rmdst instanceof INodeFile && ((INodeFile) rmdst).isFileStoredInDB()) {
                ((INodeFile) rmdst).deleteFileDataStoredInDB();
              }
              fsd.getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
            }

            if (!undoRemoveSrc && !undoRemoveDst) {
              logMetadataEventForRename(srcClone, removedSrc);
            }

            EntityManager.snapshotMaintenance(
                HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
                removedSrc);

            tx.updateQuotasInSourceTree();
            HdfsFileStatus auditStat = fsd.getAuditFileInfo(dst, false);
            return new RenameResult(filesDeleted, auditStat);
          }
        } finally {
          if (undoRemoveSrc) {
            tx.restoreSource();
          }
          if (undoRemoveDst) {
            // Rename failed - restore dst
            removedDst.setLocalNameNoPersistance(dstChildName);
            fsd.addLastINodeNoQuotaCheck(dstIIP, removedDst, dstCounts);
          }
        }
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src + " to " + dst);
        throw new IOException("rename from " + src + " to " + dst + " failed.");

      }
    }.handle();
  }

  
  /**
   * @see #unprotectedRenameTo(FSDirectory, String, String, long)
   * @deprecated Use {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  private static boolean renameTo(
      FSDirectory fsd, String src, String dst, long mtime)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    boolean stat = false;
      stat = unprotectedRenameTo(fsd, src, dst, mtime);
    return stat;
  }

  /**
   * @deprecated See {@link #renameTo(FSDirectory, String, String, long)}
   */
  @Deprecated
  private static boolean renameToInternal(
      FSDirectory fsd, String src, String dst)
      throws IOException {

    long mtime = now();
    @SuppressWarnings("deprecation")
    final boolean stat = renameTo(fsd, src, dst, mtime);
    if (stat) {
      return true;
    }
    return false;
  }

  private static HdfsFileStatus renameToInternal(
      FSDirectory fsd, String src, String dst,
      BlocksMapUpdateInfo collectedBlocks,
      Options.Rename... options)
      throws IOException {

    long mtime = now();
    return renameTo(fsd, src, dst, mtime, collectedBlocks, options);
    
  }

  private static void validateDestination(
      String src, String dst, INode srcInode)
      throws IOException {
    String error;
    if (srcInode.isSymlink() &&
        dst.equals(srcInode.asSymlink().getSymlinkString())) {
      throw new FileAlreadyExistsException("Cannot rename symlink " + src
          + " to its target " + dst);
    }
    // dst cannot be a directory or a file under src
    if (dst.startsWith(src)
        && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
  }

  private static void validateOverwrite(FSDirectory fsd,
      String src, String dst, boolean overwrite, INode srcInode, INode dstInode, PathInformation dstInfo)
      throws IOException {
    String error;// It's OK to rename a file to a symlink and vice versa
    if (dstInode.isDirectory() != srcInode.isDirectory()) {
      error = "Source " + src + " and destination " + dst
          + " must both be directories";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    if (!overwrite) { // If destination exists, overwrite flag must be true
      error = "rename destination " + dst + " already exists";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileAlreadyExistsException(error);
    }
    short depth = (short) (INodeDirectory.ROOT_DIR_DEPTH + dstInfo.getPathInodes().length-1);
    boolean areChildrenRandomlyPartitioned = INode.isTreeLevelRandomPartitioned(depth);
    if (dstInode.isDirectory() && fsd.hasChildren(dstInode.getId(), areChildrenRandomlyPartitioned)) {
      error = "rename destination directory is not empty: " + dst;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);

    }
  }

  private static void validateRenameSource(String src, INodesInPath srcIIP)
      throws IOException {
    String error;
    final INode srcInode = srcIIP.getLastINode();
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcIIP.getINodes().length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
  }

  private static class RenameOperation {
    private final FSDirectory fsd;
    private final INodesInPath srcIIP;
    private final INodesInPath dstIIP;
    private final String src;
    private final String dst;
    private final INodeDirectory srcParent;
    private final byte[] srcChildName;
    private final Quota.Counts oldSrcCounts;
    private final INode.DirCounts srcCounts;
    private INode srcChild;

    RenameOperation(FSDirectory fsd, String src, String dst,
                    INodesInPath srcIIP, INodesInPath dstIIP, INode.DirCounts srcCounts)
        throws QuotaExceededException, IOException {
      this.fsd = fsd;
      this.srcIIP = srcIIP;
      this.dstIIP = dstIIP;
      this.src = src;
      this.dst = dst;
      srcChild = srcIIP.getLastINode();
      srcChildName = srcChild.getLocalNameBytes();
      srcParent = srcIIP.getINode(-2).asDirectory();

      oldSrcCounts = Quota.Counts.newInstance();
      this.srcCounts = srcCounts;
    }

    boolean addSourceToDestination() throws IOException {
      final INode dstParent = dstIIP.getINode(-2);
      srcChild = srcIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      srcChild.setLocalNameNoPersistance(dstChildName);
      toDst = srcChild;
      return fsd.addLastINodeNoQuotaCheck(dstIIP, toDst, srcCounts);
    }

    void updateMtimeAndLease(long timestamp) throws QuotaExceededException, IOException {
      srcParent.updateModificationTime(timestamp);
      final INode dstParent = dstIIP.getINode(-2);
      dstParent.updateModificationTime(timestamp);
      // update moved lease with new filename
      fsd.getFSNamesystem().unprotectedChangeLease(src, dst);
    }

    void restoreSource() throws QuotaExceededException, IOException {
      // Rename failed - restore src
      // put it back
      srcChild.setLocalNameNoPersistance(srcChildName);
      // srcParent is not an INodeDirectoryWithSnapshot, we only need to add
      // the srcChild back
      fsd.addLastINodeNoQuotaCheck(srcIIP, srcChild, srcCounts);
    }

    void updateQuotasInSourceTree() throws QuotaExceededException {
      // update the quota usage in src tree
      // for snapshot
    }
  }
  
  static class RenameResult {
    final boolean filesDeleted;
    final HdfsFileStatus auditStat;

    RenameResult(boolean filesDeleted, HdfsFileStatus auditStat) {
      this.filesDeleted = filesDeleted;
      this.auditStat = auditStat;
    }
  }
  
  private static INode getMetaEnabledParent(INode[] pathComponents) {
    for (INode node : pathComponents) {
      if (node != null && node.isDirectory()) {
        INodeDirectory dir = (INodeDirectory) node;
        if (dir.isMetaEnabled()) {
          return dir;
        }
      }
    }
    return null;
  }
  
  private static boolean shouldLogSubtreeInodes(PathInformation srcInfo,
      PathInformation dstInfo, INode srcDataSet, INode dstDataSet,
      INodeIdentifier srcSubTreeRoot){
    if(pathIsMetaEnabled(srcInfo.getPathInodes()) || pathIsMetaEnabled(dstInfo
        .getPathInodes())){
      if(srcDataSet == null){
        if(dstDataSet == null){
          //shouldn't happen
        }else{
          //Moving a non metaEnabled directory under a metaEnabled directory
          return true;
        }
      }else{
        if(dstDataSet == null){
          //rename a metadateEnabled directory to a non metadataEnabled
          // directory, always log the subtree except if it is a rename of the
          // dataset
          return !srcDataSet.equalsIdentifier(srcSubTreeRoot);
        }else{
          //Move from one dataset to another, always log except if it is the
          //same dataset
          return !srcDataSet.equals(dstDataSet);
        }
      }
    }
    return false;
  }
  
  private static boolean pathIsMetaEnabled(INode[] pathComponents) {
    return getMetaEnabledParent(pathComponents) == null ? false : true;
  }
  
  private static void removeSubTreeLocksForRenameInternal(FSDirectory fsd, final String src,
      final boolean isUsingSubTreeLocks)
      throws StorageException, TransactionContextException,
      UnresolvedLinkException {
    if (isUsingSubTreeLocks) {
      INode[] nodes;
      INode inode;
      if (!src.equals("/")) {
        EntityManager.remove(new SubTreeOperation(fsd.getFSNamesystem().getSubTreeLockPathPrefix(src)));
        INodesInPath inodesInPath = fsd.getINodesInPath(src, false);
        nodes = inodesInPath.getINodes();
        inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSTOLocked()) {
          inode.setSubtreeLocked(false);
          EntityManager.update(inode);
        }
      }
    }
  }
  
  private static INode removeLastINodeForRename(FSDirectory fsd, final INodesInPath inodesInPath, INode.DirCounts counts) throws
      StorageException, TransactionContextException {
    return fsd.removeLastINode(inodesInPath, true, counts);
  }
  
  private static INode removeLastINode(FSDirectory fsd, final INodesInPath inodesInPath, final INode.DirCounts counts)
      throws StorageException, TransactionContextException {
    return fsd.removeLastINode(inodesInPath, false, counts);
  }
    
  private static void logMetadataEventForRename(INode srcClone, INode srcChild)
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
}
