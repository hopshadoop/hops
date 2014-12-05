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
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.FsAclPermission;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(
      final FSDirectory fsd, final String srcArg, byte[] startAfterArg,
      final boolean needLocation)
    throws IOException {
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    String startAfterString = new String(startAfterArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);

    // Get file name when startAfter is an INodePath
    if (FSDirectory.isReservedName(startAfterString)) {
      byte[][] startAfterComponents = FSDirectory
          .getPathComponentsForReservedPath(startAfterString);
      try {
        String tmp = fsd.resolvePath(src, startAfterComponents, fsd);
        byte[][] regularPath = INode.getPathComponents(tmp);
        startAfterArg = regularPath[regularPath.length - 1];
      } catch (IOException e) {
        // Possibly the inode is deleted
        throw new DirectoryListingStartAfterNotFoundException(
            "Can't find startAfter " + startAfterString);
      }
    }

    final byte[] startAfter = startAfterArg;
    
    HopsTransactionalRequestHandler getListingHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_LISTING, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        if (needLocation) {
          locks.add(lf.getBlockLock()).add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.CA));
        }
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath(src, true);
        final boolean isSuperUser = pc.isSuperUser();
        if (fsd.isPermissionEnabled()) {
          if (fsd.isDir(src)) {
            fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
          } else {
            fsd.checkTraverse(pc, iip);
          }
        }

        return getListing(fsd, src, startAfter, needLocation, isSuperUser);
      }
    };
    return (DirectoryListing) getListingHandler.handle();
  }

  /**
   * Get the file info for a specific file.
   *
   * @param srcArg The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   *        if src refers to a symlink
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(
      final FSDirectory fsd, String srcArg, final boolean resolveLink)
      throws IOException {
    if (!DFSUtil.isValidName(srcArg)) {
      throw new InvalidPathException("Invalid file name: " + srcArg);
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents, fsd);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.GET_FILE_INFO,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                    .resolveSymLink(resolveLink).setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
                    .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(true);
            locks.add(il);
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            HdfsFileStatus stat;
            FSPermissionChecker pc = fsd.getPermissionChecker();
            final INodesInPath iip = fsd.getINodesInPath(src, resolveLink);
            boolean isSuperUser = true;
            if (fsd.isPermissionEnabled()) {
              fsd.checkPermission(pc, iip, false, null, null, null, null, false);
              isSuperUser = pc.isSuperUser();
            }
            return getFileInfo(fsd, src, resolveLink, isSuperUser);
          }
        }.handle();
  }

  /**
   * Returns true if the file is closed
   */
  static boolean isFileClosed(final FSDirectory fsd, String srcArg) throws IOException {
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents, fsd);
    return (boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_FILE_INFO,
        src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ,INodeResolveType.PATH, src)
                .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
                .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath(src, true);
        if (fsd.isPermissionEnabled()) {
          fsd.checkTraverse(pc, iip);
        }
        INode[] inodes = iip.getINodes();
        return !INodeFile.valueOf(inodes[inodes.length - 1],
            src).isUnderConstruction();
      }
    }.handle();    
  }

  static ContentSummary getContentSummary(
      FSDirectory fsd, String src) throws IOException {
    
    return getContentSummaryInt(fsd, src);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * We will stop when any of the following conditions is met:
   * 1) this.lsLimit files have been added
   * 2) needLocation is true AND enough files have been added such
   * that at least this.lsLimit block locations are in the response
   *
   * @param fsd FSDirectory
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  private static DirectoryListing getListing(
      FSDirectory fsd, String src, byte[] startAfter, boolean needLocation,
      boolean isSuperUser)
      throws IOException {
    String srcs = FSDirectory.normalizePath(src);

      final INodesInPath inodesInPath = fsd.getINodesInPath(srcs, true);
      final INode[] inodes = inodesInPath.getINodes();
      final INode targetNode = inodes[inodes.length - 1];
      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : BlockStoragePolicySuite
          .ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(fsd,
                HdfsFileStatus.EMPTY_NAME, targetNode, needLocation,
                parentStoragePolicy, inodesInPath)}, 0);
      }

      final INodeDirectory dirInode = targetNode.asDirectory();
      final  List<INode> contents = dirInode.getChildrenList();
      int startChild = dirInode.nextChild(contents, startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren - startChild,
          fsd.getLsLimit());
      int locationBudget = fsd.getLsLimit();
      int listingCnt = 0;
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing && locationBudget>0; i++) {
        INode cur = contents.get(startChild+i);
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            BlockStoragePolicySuite.ID_UNSPECIFIED;
        listing[i] = createFileStatus(fsd, cur.getLocalNameBytes(), cur,
            needLocation, fsd.getStoragePolicyID(curPolicy,
                parentStoragePolicy), inodesInPath);
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
      return new DirectoryListing(
          listing, totalNumChildren-startChild-listingCnt);
  }

  /** Get the file info for a specific file.
   * @param fsd FSDirectory
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   * @param isRawPath true if a /.reserved/raw pathname was passed by the user
   * @param includeStoragePolicy whether to include storage policy
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String src, boolean resolveLink,
      boolean includeStoragePolicy)
    throws IOException {
    String srcs = FSDirectory.normalizePath(src);
      final INodesInPath inodesInPath = fsd.getINodesInPath(srcs, resolveLink);
      final INode[] inodes = inodesInPath.getINodes();
      final INode i = inodes[inodes.length - 1];
      byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ?
          i.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
      return i == null ? null : createFileStatus(fsd,
          HdfsFileStatus.EMPTY_NAME, i, policyId, inodesInPath);
  }

  /**
   * create an hdfs file status from an inode
   *
   * @param fsd FSDirectory
   * @param path the local name
   * @param node inode
   * @param needLocation if block locations need to be included or not
   * @param isRawPath true if this is being called on behalf of a path in
   *                  /.reserved/raw
   * @return a file status
   * @throws java.io.IOException if any error occurs
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node, boolean needLocation,
      byte storagePolicy, INodesInPath iip)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(fsd, path, node, storagePolicy, iip);
    } else {
      return createFileStatus(fsd, path, node, storagePolicy, iip);
    }
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node, byte storagePolicy, INodesInPath iip) throws IOException {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     boolean isStoredInDB = false;
     
     if (node.isFile()) {
       final INodeFile fileNode = node.asFile();
       isStoredInDB = fileNode.isFileStoredInDB();
       size = fileNode.getSize();
       replication = fileNode.getFileReplication();
       blocksize = fileNode.getPreferredBlockSize();
     } 

     int childrenNum = node.isDirectory() ?
         node.asDirectory().getChildrenNum() : 0;

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
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        path,
        node.getId(),
        childrenNum,
        isStoredInDB,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private static HdfsLocatedFileStatus createLocatedFileStatus(
      FSDirectory fsd, byte[] path, INode node, byte storagePolicy,
      INodesInPath iip) throws IOException {
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    boolean isStoredInDB = false;
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      isStoredInDB = fileNode.isFileStoredInDB();     
      if(isStoredInDB){
        size = fileNode.getSize();
      } else {
        size = fileNode.computeFileSize(true);
      }
      replication = fileNode.getFileReplication();
      blocksize = fileNode.getPreferredBlockSize();

      final boolean isUc = fileNode.isUnderConstruction();
      final long fileSize = isUc ?
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;

      if (isStoredInDB) {
        loc = fsd.getFSNamesystem().getBlockManager().createPhantomLocatedBlocks(fileNode,null,isUc,false);
      } else {
      loc = fsd.getFSNamesystem().getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(), fileSize, isUc, 0L, size, false);
      }
      if (loc == null) {
        loc = new LocatedBlocks();
      }
    } 
    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum() : 0;

    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(),
          node.getAccessTime(),
          getPermissionForFileStatus(node),
          node.getUserName(), node.getGroupName(),
          node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
          node.getId(), loc, childrenNum, isStoredInDB, storagePolicy);
    // Set caching information for the located blocks.
    if (loc != null) {
      CacheManager cacheManager = fsd.getFSNamesystem().getCacheManager();
      for (LocatedBlock lb: loc.getLocatedBlocks()) {
        cacheManager.setCachedLocations(lb, node.getId());
      }
    }
    return status;
  }

  /**
   * Returns an inode's FsPermission for use in an outbound FileStatus.  If the
   * inode has an ACL or is for an encrypted file/dir, then this method will
   * return an FsPermissionExtension.
   *
   * @param node INode to check
   * @param snapshot int snapshot ID
   * @param isEncrypted boolean true if the file/dir is encrypted
   * @return FsPermission from inode, with ACL bit on if the inode has an ACL
   * and encrypted bit on if it represents an encrypted file/dir.
   */
  private static FsPermission getPermissionForFileStatus(
      INode node) throws IOException {
    FsPermission perm = node.getFsPermission();
    boolean hasAcl = node.getAclFeature() != null;
    if (hasAcl) {
      perm = new FsAclPermission(perm);
    }
    return perm;
  }

  private static ContentSummary getContentSummaryInt(
      FSDirectory fsd, String src) throws IOException {

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(src, pathComponents, fsd);
    PathInformation pathInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, null, null, null);
    if (pathInfo.getPathInodes()[pathInfo.getPathComponents().length - 1] == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    final INode subtreeRoot = pathInfo.getPathInodes()[pathInfo.getPathComponents().length - 1];
    final INodeAttributes subtreeAttr = pathInfo.getSubtreeRootAttributes();
    final INodeIdentifier subtreeRootIdentifier = new INodeIdentifier(subtreeRoot.getId(), subtreeRoot.getParentId(),
        subtreeRoot.getLocalName(), subtreeRoot.getPartitionId());
    subtreeRootIdentifier.setDepth(((short) (INodeDirectory.ROOT_DIR_DEPTH + pathInfo.getPathComponents().length - 1)));

    //Calcualte subtree root default ACLs to be inherited in the tree.
    List<AclEntry> nearestDefaultsForSubtree = fsd.getFSNamesystem().calculateNearestDefaultAclForSubtree(pathInfo);

    //we do not lock the subtree and we do not need to check parent access and owner, but we need to check children access
    //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:null, 
    //access:null, subAccess:FsAction.READ_EXECUTE, ignoreEmptyDir:true
    final AbstractFileTree.CountingFileTree fileTree = new AbstractFileTree.CountingFileTree(fsd.getFSNamesystem(),
        subtreeRootIdentifier, FsAction.READ_EXECUTE, true, nearestDefaultsForSubtree);
    fileTree.buildUp();

    ContentSummary cs = new ContentSummary(fileTree.getFileSizeSummary(),
        fileTree.getFileCount(), fileTree.getDirectoryCount(),
        subtreeAttr == null ? subtreeRoot.getQuotaCounts().get(Quota.NAMESPACE) : subtreeAttr.getQuotaCounts().get(
            Quota.NAMESPACE),
        fileTree.getDiskspaceCount(), subtreeAttr == null ? subtreeRoot
        .getQuotaCounts().get(Quota.DISKSPACE) : subtreeAttr.getQuotaCounts().get(Quota.DISKSPACE));
    fsd.addYieldCount(0);
    return cs;
  }
}
