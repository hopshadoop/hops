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

import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(
      final FSDirectory fsd, final String srcArg, byte[] startAfterArg,
      final boolean needLocation)
    throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    String startAfterString = new String(startAfterArg, Charsets.UTF_8);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);

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
        locks.add(lf.getEZLock());
        locks.add(lf.getXAttrLock(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO, src));
      }

      @Override
      public Object performTask() throws IOException {
        final INodesInPath iip = fsd.getINodesInPath(src, true);
        final boolean isSuperUser = pc.isSuperUser();
        if (fsd.isPermissionEnabled()) {
          if (iip.getLastINode() != null && iip.getLastINode().isDirectory()) {
            fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
          } else {
            fsd.checkTraverse(pc, iip);
          }
        }

        return getListing(fsd, iip, srcArg, src, startAfter, needLocation, isSuperUser);
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
   * @param needLocation Include {@link LocatedBlocks} in result.
   * @param needBlockToken Include block tokens in {@link LocatedBlocks}.
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(FSDirectory fsd, String srcArg,
      boolean resolveLink, boolean needLocation, boolean needBlockToken)
      throws IOException {
    if (!DFSUtil.isValidName(srcArg)) {
      throw new InvalidPathException("Invalid file name: " + srcArg);
    }
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);
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
            locks.add(lf.getEZLock());
            locks.add(lf.getXAttrLock(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO));
            if (needLocation) {
              locks.add(lf.getBlockLock());
              locks.add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.CA));
            }
          }

          @Override
          public Object performTask() throws IOException {
            HdfsFileStatus stat;
            final INodesInPath iip = fsd.getINodesInPath(src, resolveLink);
            boolean isSuperUser = true;
            if (fsd.isPermissionEnabled()) {
              fsd.checkPermission(pc, iip, false, null, null, null, null, false);
              isSuperUser = pc.isSuperUser();
            }
            return getFileInfo(fsd, src, resolveLink, FSDirectory.isReservedRawName(srcArg),
                    isSuperUser, needLocation, needBlockToken);
          }
        }.handle();
  }

  /**
   * Returns true if the file is closed
   */
  static boolean isFileClosed(final FSDirectory fsd, String srcArg) throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);
    return (boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.IS_FILE_CLOSED,
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
        final INodesInPath iip = fsd.getINodesInPath(src, true);
        if (fsd.isPermissionEnabled()) {
          fsd.checkTraverse(pc, iip);
        }
        return !INodeFile.valueOf(iip.getLastINode(), src).isUnderConstruction();
      }
    }.handle();    
  }

  static ContentSummary getContentSummary(
      FSDirectory fsd, String src) throws IOException {

    return getContentSummaryInt(fsd, src);
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ? inodePolicy :
        parentPolicy;
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
   * @param iip the INodesInPath instance containing all the INodes along the
   *            path
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  private static DirectoryListing getListing(FSDirectory fsd, INodesInPath iip,
      String srcArg, String src, byte[] startAfter, boolean needLocation,boolean isSuperUser)
      throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    final boolean isRawPath = fsd.isReservedRawName(srcArg);
    
      final INode targetNode = iip.getLastINode();
      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : HdfsConstantsClient
          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(fsd,
                HdfsFileStatus.EMPTY_NAME, targetNode, needLocation, false,
                parentStoragePolicy, isRawPath, iip)}, 0);
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
            HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        listing[i] = createFileStatus(fsd, cur.getLocalNameBytes(), cur,
            needLocation, false, getStoragePolicyID(curPolicy, parentStoragePolicy), isRawPath,
                iip);
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
   * @param includeStoragePolicy whether to include storage policy
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, INodesInPath src, boolean isRawPath,
      boolean includeStoragePolicy, boolean needLocation, boolean needBlockToken)
      throws IOException {

    final INode i = src.getLastINode();
    byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ? i.getStoragePolicyID()
        : HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    if (i == null) {
      return null;
    } else {
      if (needLocation){
        return createLocatedFileStatus(
                fsd, HdfsFileStatus.EMPTY_NAME, i, policyId, isRawPath,
                src, needBlockToken);
      } else {
        return createFileStatus(
                fsd, HdfsFileStatus.EMPTY_NAME, i, policyId, isRawPath,
                src);
      }
    }
  }

  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String src, boolean resolveLink, boolean isRawPath,
      boolean includeStoragePolicy, boolean needLocation, boolean needBlockToken)
    throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    final INodesInPath iip = fsd.getINodesInPath(srcs, resolveLink);
    return getFileInfo(fsd, iip, isRawPath, includeStoragePolicy, needLocation, needBlockToken);
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
      FSDirectory fsd, byte[] path, INode node, boolean needLocation, boolean needBlockToken,
      byte storagePolicy, boolean isRawPath, INodesInPath iip)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(fsd, path, node, storagePolicy, isRawPath, iip, needBlockToken);
    } else {
      return createFileStatus(fsd, path, node, storagePolicy, isRawPath, iip);
    }
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node, byte storagePolicy, boolean isRawPath,
      INodesInPath iip) throws
      IOException {

     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     boolean isStoredInDB = false;
     final boolean isEncrypted;

     final FileEncryptionInfo feInfo = isRawPath ? null :
         fsd.getFileEncryptionInfo(node, iip);
     
     if (node.isFile()) {
       final INodeFile fileNode = node.asFile();
       size = fileNode.getSize();
       replication = fileNode.getBlockReplication();
       blocksize = fileNode.getPreferredBlockSize();
       isEncrypted = (feInfo != null) ||
           (isRawPath && fsd.isInAnEZ(INodesInPath.fromINode(node)));
     } else {
       isEncrypted = fsd.isInAnEZ(INodesInPath.fromINode(node));
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
        getPermissionForFileStatus(node, isEncrypted),
        node.getUserName(),
        node.getGroupName(),
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        path,
        node.getId(),
        childrenNum,
        feInfo,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private static HdfsLocatedFileStatus createLocatedFileStatus(
      FSDirectory fsd, byte[] path, INode node, byte storagePolicy, boolean isRawPath,
      INodesInPath iip, boolean needBlockToken ) throws IOException {
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    boolean isStoredInDB = false;
    final boolean isEncrypted;
    final FileEncryptionInfo feInfo = isRawPath ? null :
        fsd.getFileEncryptionInfo(node, iip);
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      isStoredInDB = fileNode.isFileStoredInDB();     
      if(isStoredInDB){
        size = fileNode.getSize();
      } else {
        size = fileNode.computeFileSize();
      }
      replication = fileNode.getBlockReplication();
      blocksize = fileNode.getPreferredBlockSize();

      final boolean isUc = fileNode.isUnderConstruction();
      final long fileSize = isUc ?
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;

      if (isStoredInDB) {
        loc = fsd.getFSNamesystem().getBlockManager().createPhantomLocatedBlocks(fileNode,null,isUc,false, feInfo);
      } else {
        loc = fsd.getFSNamesystem().getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(), fileSize, isUc, 0L, size, needBlockToken, feInfo);
      }
      if (loc == null) {
        loc = new LocatedBlocks();
      }
      isEncrypted = (feInfo != null) ||
          (isRawPath && fsd.isInAnEZ(INodesInPath.fromINode(node)));
    } else {
      isEncrypted = fsd.isInAnEZ(INodesInPath.fromINode(node));
    }
    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum() : 0;

    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(),
          node.getAccessTime(),
          getPermissionForFileStatus(node, isEncrypted),
          node.getUserName(), node.getGroupName(),
          node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
          node.getId(), loc, childrenNum, feInfo, storagePolicy);
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
   * @param isEncrypted boolean true if the file/dir is encrypted
   * @return FsPermission from inode, with ACL bit on if the inode has an ACL
   * and encrypted bit on if it represents an encrypted file/dir.
   */
  private static FsPermission getPermissionForFileStatus(
      INode node, boolean isEncrypted) throws IOException {
    FsPermission perm = node.getFsPermission();
    boolean hasAcl = node.getAclFeature() != null;
    if (hasAcl || isEncrypted) {
      perm = new FsPermissionExtension(perm, hasAcl, isEncrypted);
    }
    return perm;
  }

  private static ContentSummary getContentSummaryInt(FSDirectory fsd, 
      String src) throws IOException {
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(fsd.getPermissionChecker(), src, pathComponents);
    PathInformation pathInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
        false, null, null, null, null);
    if (pathInfo.getINodesInPath().getLastINode() == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    final INode subtreeRoot = pathInfo.getINodesInPath().getLastINode();
    final QuotaCounts subtreeQuota = pathInfo.getQuota();
    final INodeIdentifier subtreeRootIdentifier = new INodeIdentifier(subtreeRoot.getId(), subtreeRoot.getParentId(),
        subtreeRoot.getLocalName(), subtreeRoot.getPartitionId());
    subtreeRootIdentifier.setDepth(((short) (INodeDirectory.ROOT_DIR_DEPTH + pathInfo.getPathComponents().length - 1)));

    //Calcualte subtree root default ACLs to be inherited in the tree.
    List<AclEntry> nearestDefaultsForSubtree = fsd.getFSNamesystem().calculateNearestDefaultAclForSubtree(pathInfo);

    byte inheritedStoragePolicy = fsd.getFSNamesystem().calculateNearestinheritedStoragePolicy(pathInfo);
    //we do not lock the subtree and we do not need to check parent access and owner, but we need to check children access
    //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:null, 
    //access:null, subAccess:FsAction.READ_EXECUTE, ignoreEmptyDir:true
    final AbstractFileTree.CountingFileTree fileTree = new AbstractFileTree.CountingFileTree(fsd.getFSNamesystem(),
        subtreeRootIdentifier, FsAction.READ_EXECUTE, true, nearestDefaultsForSubtree, inheritedStoragePolicy);
    fileTree.buildUp(fsd.getBlockStoragePolicySuite());

    ContentCounts counts = fileTree.getCounts();
    QuotaCounts q = subtreeQuota;
    if(q==null){
      q=subtreeRoot.getQuotaCounts();
    }
    ContentSummary cs = new ContentSummary.Builder().
              length(counts.getLength()).
              fileCount(counts.getFileCount() + counts.getSymlinkCount()).
              directoryCount(counts.getDirectoryCount()).
              quota(q.getNameSpace()).
              spaceConsumed(counts.getStoragespace()).
              spaceQuota(q.getStorageSpace()).
              typeConsumed(counts.getTypeSpaces()).
              typeQuota(q.getTypeSpaces().asArray()).
              build();
    
    fsd.addYieldCount(0);
    return cs;
  }
}
