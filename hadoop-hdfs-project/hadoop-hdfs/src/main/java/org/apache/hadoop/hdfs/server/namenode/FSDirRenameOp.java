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

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
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
import io.hops.util.Slicer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.util.Time;

class FSDirRenameOp {
  public static final Log LOG = LogFactory.getLog(FSDirRenameOp.class);
  @Deprecated
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
    FSPermissionChecker pc = fsd.getPermissionChecker();
    
    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    HdfsFileStatus resultingStat = null;
    src = fsd.resolvePath(pc, src, srcComponents);
    dst = fsd.resolvePath(pc, dst, dstComponents);
    
     @SuppressWarnings("deprecation")
    final boolean status = renameTo(fsd, pc, src, dst);
    
    return status;   
  }

  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   */
  static void verifyQuotaForRename(FSDirectory fsd,
      INodesInPath src, INodesInPath dst, QuotaCounts srcCounts,
      QuotaCounts dstCounts)
      throws QuotaExceededException, StorageException, TransactionContextException {
    if (!fsd.getFSNamesystem().isImageLoaded() || !fsd.isQuotaEnabled()) {
      // Do not check quota if edits log is still being processed
      return;
    }
    INode commonAncestor = null;
    for (int i = 0; src.getINode(i).equals(dst.getINode(i)); i++) {
      commonAncestor = src.getINode(i);
    }
    
    final QuotaCounts delta = new QuotaCounts.Builder().quotaCount(srcCounts).build();
    
    // Reduce the required quota by dst that is being removed
    INode dstINode = dst.getLastINode();
    if (dstINode != null) {
      delta.subtract(dstCounts);
    }
    FSDirectory.verifyQuota(dst, dst.length() - 1, delta, commonAncestor);
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   */
  static void verifyFsLimitsForRename(FSDirectory fsd, INodesInPath srcIIP,
      INodesInPath dstIIP)
      throws FSLimitException.PathComponentTooLongException,
          FSLimitException.MaxDirectoryItemsExceededException,
          StorageException,
          TransactionContextException {
    byte[] dstChildName = dstIIP.getLastLocalName();
    final String parentPath = dstIIP.getParentPath();
    fsd.verifyMaxComponentLength(dstChildName, parentPath);
    // Do not enforce max directory items if renaming within same directory.
    if (!srcIIP.getINode(-2).equals(dstIIP.getINode(-2))) {
      fsd.verifyMaxDirItems(dstIIP.getINode(-2).asDirectory(), parentPath);
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
  @SuppressWarnings("deprecation")
  static boolean renameForEditLog(FSDirectory fsd, String src, String dst, long timestamp)
      throws IOException {
    
    
    
    PathInformation srcInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INodesInPath srcIIP = srcInfo.getINodesInPath();
    final INode srcInode = srcIIP.getLastINode();
    try{
      validateRenameSource(srcIIP);
    }catch(IOException  ignored){
      return false;
    }
    
    PathInformation dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    boolean updateDst = false;
    if(dstInfo.isDir()){
      dst += Path.SEPARATOR + new Path(src).getName();
      updateDst = true;
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
    
    if (updateDst) {
      dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
          false, FsAction.WRITE, null, null, null);
    }
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

    INode srcDataSet = getMetaEnabledParent(srcInfo.getINodesInPath().getReadOnlyINodes());
    INode dstDataSet = getMetaEnabledParent(dstInfo.getINodesInPath().getReadOnlyINodes());
    Collection<INodeMetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst

    QuotaCounts srcCounts = new QuotaCounts.Builder().quotaCount(srcInfo.getUsage()).build();
    QuotaCounts dstCounts = new QuotaCounts.Builder().quotaCount(dstInfo.getUsage()).build();
    
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;

    try {
      if (isUsingSubTreeLocks) {
        List<AclEntry> nearestDefaultsForSubtree =
          fsd.getFSNamesystem().calculateNearestDefaultAclForSubtree(srcInfo);
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
                    srcSubTreeRoot, srcDataSet, dstDataSet, nearestDefaultsForSubtree);
            srcFileTree.buildUp(fsd.getBlockStoragePolicySuite());
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot, nearestDefaultsForSubtree);
            srcFileTree.buildUp(fsd.getBlockStoragePolicySuite());
          }
          srcCounts = new QuotaCounts.Builder().quotaCount(srcFileTree.getQuotaCount()).build();

          if (fsd.isQuotaEnabled()) {
            //apply pending quota level by level for src folder
            FSSTOHelper.applyAllPendingQuotaInSubTree(fsd.getFSNamesystem(), srcFileTree);

            // we should apply quota updates to the destination folder in case it is
            // overwritten. However, in case of rename the folder is overwritten
            // only if the destination folder is empty. So applying prioritized
            // updates only for the destination folder is sufficient
            if (dstIIP.getLastINode() != null && dstIIP.getLastINode().isDirectory()) {
              FSSTOHelper.applyAllPendingQuotaForDirectory(fsd.getFSNamesystem(),
                dstIIP.getLastINode().getId());
            }
          }
        }
        fsd.getFSNamesystem().delayAfterBbuildingTree("Built tree of "+ src +" for rename. ");
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
        //permission will be checked in renameToTransaction
      }

      boolean retValue = renameToTransaction(fsd, src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0L,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks, timestamp);
      // the rename Tx has committed. it has also remove the subTreeLocks
      renameTransactionCommitted = true;

      addLogEntries(fsd, logEntries);

      return retValue;
    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          fsd.getFSNamesystem().unlockSubtree(src, srcSubTreeRoot.getInodeId());
        }
     }
    }
  }
  private static void addLogEntries(FSDirectory fsd, Collection<INodeMetadataLogEntry> logEntries) throws IOException {
    List<INodeMetadataLogEntry> entries = new ArrayList<>(logEntries);
    try {
      Slicer.slice(entries.size(), fsd.getFSNamesystem().getSlicerBatchSize(),
              fsd.getFSNamesystem().getSlicerNbThreads(),
             fsd.getFSNamesystem().getFSOperationsExecutor(),
              new Slicer.OperationHandler() {
                @Override
                public void handle(int startIndex, int endIndex)
                        throws Exception {
                  new HopsTransactionalRequestHandler(
                          HDFSOperationType.ADD_METADATA_LOG_ENTRIES) {
                    @Override
                    public void acquireLock(TransactionLocks locks) {
                    }

                    @Override
                    public Object performTask() throws IOException {
                      List<INodeMetadataLogEntry> subList = entries.subList(startIndex, endIndex);

                      for (INodeMetadataLogEntry logEntry : subList) {
                        EntityManager.add(logEntry);
                      }

                      AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime(subList);
                      return null;
                    }
                  }.handle();
                }
              });
    } catch (Exception ex) {
      if (ex instanceof  IOException){
        throw (IOException)ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  private static boolean renameToTransaction(final FSDirectory fsd, final String src, final long srcINodeID,
      final String dst, final QuotaCounts srcCounts, final QuotaCounts dstCounts,
      final boolean isUsingSubTreeLocks, final long timestamp) throws IOException {

    HopsTransactionalRequestHandler renameToHandler = new HopsTransactionalRequestHandler(
        isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_DEPRICATED_RENAME : HDFSOperationType.DEPRICATED_RENAME,
         src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getLegacyRenameINodeLock(
            INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src, dst)
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
          locks.add(lf.getLeaseLockAllPaths(LockType.WRITE,
                  fsd.getFSNamesystem().getLeaseCreationLockRows()))
              .add(lf.getLeasePathLock());
        } else {
          locks.add(lf.getLeaseLockAllPaths(LockType.READ_COMMITTED,
                          fsd.getFSNamesystem().getLeaseCreationLockRows()))
              .add(lf.getLeasePathLock(src)).
              add(lf.getSubTreeOpsLock(LockType.WRITE, fsd.getFSNamesystem().getSubTreeLockPathPrefix(src), false));
        }

        locks.add(lf.getEZLock());
        locks.add(lf.getXAttrLock(FSDirXAttrOp.XATTR_ENCRYPTION_ZONE));
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        INodesInPath dstIIP = fsd.getINodesInPath(dst, false);
        INodesInPath srcIIP = fsd.getINodesInPath(src, false);

        if (!isUsingSubTreeLocks) {
          if (fsd.isPermissionEnabled()) {
            FSPermissionChecker pc = fsd.getFSNamesystem().getPermissionChecker();
            // Rename does not operates on link targets
            // Do not resolveLink when checking permissions of src and dst
            // Check write access to parent of src
            fsd.checkPermission(pc, srcIIP, false, null, FsAction.WRITE, null, null,
                false);
            // Check write access to ancestor of dst
            fsd.checkPermission(pc, dstIIP, false, FsAction.WRITE, null, null, null,
                false);
          }
        }
        // remove the subtree locks
        removeSubTreeLocksForRenameInternal(fsd, src, isUsingSubTreeLocks);

        fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);
        // Ensure dst has quota to accommodate rename
        verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
        verifyQuotaForRename(fsd, srcIIP, dstIIP, srcCounts, dstCounts);

        RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP, srcCounts, dstCounts);

        boolean added = false;

        try {
          // remove src
          if (!tx.removeSrc4OldRename()) {
            return false;
          }

          // add src to the destination
          added = tx.addSourceToDestination();
          if (added) {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "DIR* FSDirectory.unprotectedRenameTo: " + src + " is renamed to " + dst);
            }

            tx.updateMtimeAndLease(timestamp);
            tx.updateQuotasInSourceTree(fsd.getBlockStoragePolicySuite());

            tx.logMetadataEvent();
            tx.snapshotMaintenance();

            return true;
          }
        } finally {
          if (!added) {
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
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    
    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    src = fsd.resolvePath(pc, src, srcComponents);
    dst = fsd.resolvePath(pc, dst, dstComponents);
    HdfsFileStatus resultingStat = renameTo(fsd, pc, src, dst, collectedBlocks, options);

    return new AbstractMap.SimpleImmutableEntry<>(
        collectedBlocks, resultingStat);
  }

  /**
   * @see {@link #unprotectedRenameTo(FSDirectory, String, String, INodesInPath,
   * INodesInPath, long, BlocksMapUpdateInfo, Options.Rename...)}
   */
  static HdfsFileStatus renameTo(FSDirectory fsd, FSPermissionChecker pc, String src,
      String dst, BlocksMapUpdateInfo collectedBlocks,
      Options.Rename... options) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    final long mtime = Time.now();
    RenameResult ret = unprotectedRenameTo(fsd, src, dst, mtime, 
        collectedBlocks, options);
    if (ret.filesDeleted) {
      FSDirDeleteOp.incrDeletedFileCount(1);
    }
    return ret.auditStat;
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
   * @return whether a file/directory gets overwritten in the dst path
   */
  static RenameResult unprotectedRenameTo(FSDirectory fsd, String src, String dst, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    boolean overwrite = options != null
        && Arrays.asList(options).contains(Options.Rename.OVERWRITE);

    final String error;
    PathInformation srcInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INodesInPath srcIIP = srcInfo.getINodesInPath();
    final INode srcInode = srcIIP.getLastINode();
    validateRenameSource(srcIIP);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }
    validateDestination(src, dst, srcInode);

    PathInformation dstInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    INodesInPath dstIIP = dstInfo.getINodesInPath();
    if (dstIIP.length() == 1) {
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

    INode srcDataSet = getMetaEnabledParent(srcInfo.getINodesInPath().getReadOnlyINodes());
    INode dstDataSet = getMetaEnabledParent(dstInfo.getINodesInPath().getReadOnlyINodes());
    Collection<INodeMetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst
    QuotaCounts srcCounts = new QuotaCounts.Builder().quotaCount(srcInfo.getUsage()).build();
    QuotaCounts dstCounts = new QuotaCounts.Builder().quotaCount(dstInfo.getUsage()).build();
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;
    try {
      if (isUsingSubTreeLocks) {
        LOG.debug("Rename src: " + src + " dst: " + dst + " requires sub-tree locking mechanism");
        //checkin parentAccess is enough for this operation, no need to pass access argument to QuotaCountingFileTree
        //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
        //access:null, subAccess:null, ignoreEmptyDir:false
        srcSubTreeRoot = fsd.getFSNamesystem().lockSubtreeAndCheckOwnerAndParentPermission(src, false, 
            FsAction.WRITE, SubTreeOperation.Type.RENAME_STO);

        if (srcSubTreeRoot != null) {
          List<AclEntry> nearestDefaultsForSubtree =
            fsd.getFSNamesystem().calculateNearestDefaultAclForSubtree(srcInfo);
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (shouldLogSubtreeInodes(srcInfo, dstInfo, srcDataSet,
              dstDataSet, srcSubTreeRoot)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot, srcDataSet, dstDataSet, nearestDefaultsForSubtree);
            srcFileTree.buildUp(fsd.getBlockStoragePolicySuite());
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(fsd.getFSNamesystem(),
                    srcSubTreeRoot, nearestDefaultsForSubtree);
            srcFileTree.buildUp(fsd.getBlockStoragePolicySuite());
          }

          if (fsd.isQuotaEnabled()) {
            //apply pending quota level by level
            FSSTOHelper.applyAllPendingQuotaInSubTree(fsd.getFSNamesystem(), srcFileTree);

            // we should apply quota updates to the destination folder in case it is
            // overwritten. However, in case of rename the folder is overwritten
            // only if the destination folder is empty. So applying prioritized
            // updates only for the destination folder is sufficient
            if(dstIIP.getLastINode() != null && dstIIP.getLastINode().isDirectory()) {
              FSSTOHelper.applyAllPendingQuotaForDirectory(fsd.getFSNamesystem(),
                dstIIP.getLastINode().getId());
            }
          }

          srcCounts = new QuotaCounts.Builder().quotaCount(srcFileTree.getQuotaCount()).build();

          fsd.getFSNamesystem().delayAfterBbuildingTree("Built Tree for "+src+" for rename. ");
        } else {
          isUsingSubTreeLocks=false;
        }
      } else {
        //permissions are checked in the transaction
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      RenameResult ret = renameToTransaction(fsd, src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks, timestamp, options);
      renameTransactionCommitted = true;

      addLogEntries(fsd, logEntries);

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
      final QuotaCounts srcCounts, final QuotaCounts dstCounts,
      final boolean isUsingSubTreeLocks, final long timestamp,
      final Options.Rename... options) throws IOException {

    return (RenameResult) new HopsTransactionalRequestHandler(
        isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_RENAME : HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getRenameINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src, dst)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        if (isUsingSubTreeLocks) {
          il.setIgnoredSTOInodes(srcINodeID);
        }
        locks.add(il)
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.IV,
                BLK.PE, BLK.ER));

        if (!isUsingSubTreeLocks) {
          locks.add(lf.getLeaseLockAllPaths(LockType.WRITE,
                  fsd.getFSNamesystem().getLeaseCreationLockRows()))
              .add(lf.getLeasePathLock());
        } else {
          locks.add(lf.getLeaseLockAllPaths(LockType.WRITE,
                  fsd.getFSNamesystem().getLeaseCreationLockRows()))
              .add(lf.getLeasePathLock(src)).
              add(lf.getSubTreeOpsLock(LockType.WRITE, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src), false));
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
        locks.add(lf.getEZLock());
        List<XAttr> xAttrsToLock = new ArrayList<>();
        xAttrsToLock.add(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO);
        xAttrsToLock.add(FSDirXAttrOp.XATTR_ENCRYPTION_ZONE);
        locks.add(lf.getXAttrLock(xAttrsToLock));
      }

      @Override
      public Object performTask() throws IOException {
        INodesInPath dstIIP = fsd.getINodesInPath(dst, false);
        INodesInPath srcIIP = fsd.getINodesInPath(src, false);
        fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);
        if (!isUsingSubTreeLocks) {
          if (fsd.isPermissionEnabled()) {
            FSPermissionChecker pc = fsd.getFSNamesystem().getPermissionChecker();
            // Rename does not operates on link targets
            // Do not resolveLink when checking permissions of src and dst
            // Check write access to parent of src
            fsd.checkPermission(pc, srcIIP, false, null, FsAction.WRITE, null, null,
                false);
            // Check write access to ancestor of dst
            fsd.checkPermission(pc, dstIIP, false, FsAction.WRITE, null, null, null,
                false);
          }
        }

        BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
        
        for (Options.Rename op : options) {
          if (op == Rename.KEEP_ENCODING_STATUS) {
            INodesInPath srcInodesInPath = fsd.getINodesInPath(src, false);
            INodesInPath dstInodesInPath = fsd.getINodesInPath(dst, false);
            INode srcNode = srcIIP.getLastINode();
            INode dstNode = dstIIP.getLastINode();
            EncodingStatus status = EntityManager.find(
                EncodingStatus.Finder.ByInodeId, dstNode.getId());
            EncodingStatus newStatus = new EncodingStatus(status);
            newStatus.setInodeId(srcNode.getId(), srcNode.isInTree());
            EntityManager.add(newStatus);
            EntityManager.remove(status);
            break;
          }
        }

        if (isUsingSubTreeLocks) {
          removeSubTreeLocksForRenameInternal(fsd, src, isUsingSubTreeLocks);
        }

        // Ensure dst has quota to accommodate rename
        verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
        verifyQuotaForRename(fsd, srcIIP, dstIIP, srcCounts, dstCounts);

        RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP, srcCounts, dstCounts);

        boolean undoRemoveSrc = true;
        tx.removeSrc();

        boolean undoRemoveDst = false;
        long removedNum = 0;
        try {
          if (dstIIP.getLastINode() != null) { // dst exists remove it
            removedNum = tx.removeDst();
            if (removedNum != -1) {  
              undoRemoveDst = true;
            }
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
            if (undoRemoveDst) {
              undoRemoveDst = false;
              if (removedNum > 0) {
                filesDeleted = tx.cleanDst(bsps);
              }
            }
            
            if (!undoRemoveSrc && !undoRemoveDst) {
              tx.logMetadataEvent();
             
            }

            tx.snapshotMaintenance();
            tx.updateQuotasInSourceTree(bsps);
            HdfsFileStatus auditStat = fsd.getAuditFileInfo(dstIIP);
            return new RenameResult(filesDeleted, auditStat);
          }
        } finally {
          if (undoRemoveSrc) {
            tx.restoreSource();
          }
          if (undoRemoveDst) { // Rename failed - restore dst
            tx.restoreDst();
          }
        }
        NameNode.stateChangeLog.warn(
            "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src + " to " + dst);
        throw new IOException("rename from " + src + " to " + dst + " failed.");

      }
    }.handle();
  }

  
  /**
   * @deprecated Use {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  private static boolean renameTo(FSDirectory fsd, FSPermissionChecker pc,
      String src, String dst) throws IOException {    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    final long mtime = Time.now();
    boolean stat = false;
      stat = renameForEditLog(fsd, src, dst, mtime);
    if (stat) {
      return true;
    }
    return false;
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
    short depth = (short) (INodeDirectory.ROOT_DIR_DEPTH + dstInfo.getINodesInPath().length()-1);
    boolean areChildrenRandomlyPartitioned = INode.isTreeLevelRandomPartitioned(depth);
    if (dstInode.isDirectory() && fsd.hasChildren(dstInode.getId(), areChildrenRandomlyPartitioned)) {
      error = "rename destination directory is not empty: " + dst;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);

    }
  }

  private static void validateRenameSource(INodesInPath srcIIP)
      throws IOException {
    String error;
    final INode srcInode = srcIIP.getLastINode();
    // validate source
    if (srcInode == null) {
      error = "rename source " + srcIIP.getPath() + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcIIP.length() == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
  }

  private static class RenameOperation {
    private final FSDirectory fsd;
    private INodesInPath srcIIP;
    private final INodesInPath srcParentIIP;
    private INodesInPath dstIIP;
    private final INodesInPath dstParentIIP;
    private final String src;
    private final String dst;
    private final INodeDirectory srcParent;
    private final byte[] srcChildName;
    private final QuotaCounts srcCounts;
    private final QuotaCounts dstCounts;
    private INode srcChild;
    private INode oldDstChild;
    private INode srcClone;

    RenameOperation(FSDirectory fsd, String src, String dst,
                    INodesInPath srcIIP, INodesInPath dstIIP, QuotaCounts srcCounts,
                    QuotaCounts dstCounts)
        throws QuotaExceededException, IOException {
      this.fsd = fsd;
      this.src = src;
      this.dst = dst;
      this.srcIIP = srcIIP;
      this.dstIIP = dstIIP;
      this.srcParentIIP = srcIIP.getParentINodesInPath();
      this.dstParentIIP = dstIIP.getParentINodesInPath();

      srcChild = this.srcIIP.getLastINode();
      if (srcChild != null) {
        srcClone = srcChild.cloneInode();
      }
      srcChildName = srcChild.getLocalNameBytes();
      srcParent = this.srcIIP.getINode(-2).asDirectory();

      this.srcCounts = srcCounts;
      this.dstCounts = dstCounts;
    }

    long removeSrc() throws IOException {
      long removedNum = fsd.removeLastINode(srcIIP, true, srcCounts);
      if (removedNum == -1) {
        String error = "Failed to rename " + src + " to " + dst +
            " because the source can not be removed";
        NameNode.stateChangeLog.warn("DIR* FSDirRenameOp.unprotectedRenameTo:" +
            error);
        throw new IOException(error);
      }
      srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1, null);
      return removedNum;
    }
        
    boolean removeSrc4OldRename() throws IOException {
      final long removedSrc = fsd.removeLastINode(srcIIP, true, srcCounts);
      if (removedSrc == -1) {
        NameNode.stateChangeLog.warn("DIR* FSDirRenameOp.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst + " because the source" +
            " can not be removed");
        return false;
      } else {
        srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1, null);
        return true;
      }
    }
    
    long removeDst() throws IOException {
      long removedNum = fsd.removeLastINode(dstIIP, false, dstCounts);
      if (removedNum != -1) {
        oldDstChild = dstIIP.getLastINode();
        dstIIP = INodesInPath.replace(dstIIP, dstIIP.length() - 1, null);
      }
      return removedNum;
    }
        
    boolean addSourceToDestination() throws IOException {
      final INode dstParent = dstParentIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      srcChild.setLocalNameNoPersistance(dstChildName);
      toDst = srcChild;
      return fsd.addLastINodeNoQuotaCheck(dstParentIIP, toDst, srcCounts) != null;
    }

    void updateMtimeAndLease(long timestamp) throws QuotaExceededException, IOException {
      srcParent.updateModificationTime(timestamp);
      final INode dstParent = dstParentIIP.getLastINode();
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
      fsd.addLastINodeNoQuotaCheck(srcParentIIP, srcChild, srcCounts);
    }

    void restoreDst() throws QuotaExceededException, IOException {
      Preconditions.checkState(oldDstChild != null);
      
      fsd.addLastINodeNoQuotaCheck(dstParentIIP, oldDstChild, dstCounts); 
    }

    boolean cleanDst(BlockStoragePolicySuite bsps)
        throws QuotaExceededException, IOException {
      Preconditions.checkState(oldDstChild != null);
      BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
      List<INode> removedINodes = new ChunkedArrayList<>();
      final boolean filesDeleted;
      
      oldDstChild.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
      filesDeleted = true;
      
      fsd.getFSNamesystem().removeLeasesAndINodes(src, removedINodes);
      fsd.getFSNamesystem().removeBlocks(collectedBlocks);
      return filesDeleted;
    }

    void updateQuotasInSourceTree(BlockStoragePolicySuite bsps) throws QuotaExceededException {
      // update the quota usage in src tree
      // for snapshot
    }
    
    public void logMetadataEvent()
        throws TransactionContextException, StorageException {
      INode srcDataset = srcClone.getMetaEnabledParent();
      INode dstDataset = srcChild.getMetaEnabledParent();

      if (srcDataset == null) {
        if (dstDataset == null) {
          //No logging required
        } else {
          //rename from non metaEnabled directory to a metaEnabled directoy
          srcChild.logMetadataEvent(INodeMetadataLogEntry.Operation.Add);
        }
      } else {
        if (dstDataset == null) {
          //rename from metaEnabled directory to a non metaEnabled directory
          EntityManager.add(new INodeMetadataLogEntry(srcDataset.getId(),
              srcClone.getId(), srcClone.getPartitionId(), srcClone
              .getParentId(), srcClone.getLocalName(), srcChild
              .incrementLogicalTime(),
              INodeMetadataLogEntry.Operation.Delete));
        } else {
          //rename across datasets or the same dataset
          //recalculate the partitionId for the new destination, for the
          // inode it is already handled using the INodePKChanged snapshot
          // maintaince command
          long partitionId = INode.calculatePartitionId(srcChild.getParentId(),
              srcChild.getLocalName(), srcChild.myDepth());
          EntityManager.add(new INodeMetadataLogEntry(dstDataset.getId(),
              srcChild.getId(), partitionId, srcChild
              .getParentId(), srcChild.getLocalName(), srcChild
              .incrementLogicalTime(),
              INodeMetadataLogEntry.Operation.Rename));
        }
      }
    }
    
    public void snapshotMaintenance() throws TransactionContextException {
      EntityManager.snapshotMaintenance(
          HdfsTransactionContextMaintenanceCmds.INodePKChanged, srcClone,
          srcChild);
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
  
  private static INode getMetaEnabledParent(List<INode> pathComponents) {
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
    if (pathIsMetaEnabled(srcInfo.getINodesInPath().getReadOnlyINodes()) || pathIsMetaEnabled(dstInfo.getINodesInPath().
        getReadOnlyINodes())) {
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
  
  private static boolean pathIsMetaEnabled(List<INode> pathComponents) {
    return getMetaEnabledParent(pathComponents) == null ? false : true;
  }
  
  private static void removeSubTreeLocksForRenameInternal(FSDirectory fsd, final String src,
      final boolean isUsingSubTreeLocks)
      throws StorageException, TransactionContextException,
      UnresolvedLinkException {
    if (isUsingSubTreeLocks) {
      if (!src.equals("/")) {
        SubTreeOperation subTreeOp = EntityManager.find(SubTreeOperation.Finder.ByPath, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src));
        EntityManager.remove(subTreeOp);
        INodesInPath inodesInPath = fsd.getINodesInPath(src, false);
        INode inode = inodesInPath.getLastINode();
        if (inode != null && inode.isSTOLocked()) {
          inode.setSubtreeLocked(false);
          EntityManager.update(inode);
        }
      }
    }
  }
    
}
