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
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.security.GroupAlreadyExistsException;
import io.hops.security.UserAlreadyExistsException;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.ipc.NotALeaderException;

public class FSDirAttrOp {

  static HdfsFileStatus setPermission(
      final FSDirectory fsd, final String srcArg, final FsPermission permission)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    //we just want to lock the subtree the permission are checked in setPermissionInt
    final INodeIdentifier inode = fsd.getFSNamesystem().lockSubtreeAndCheckOwnerAndParentPermission(src, true,
        null, SubTreeOperation.Type.SET_PERMISSION_STO);
    final boolean isSTO = inode != null;
    boolean txFailed = true;
    try {
      HdfsFileStatus ret = (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_SETPERMISSION,
          src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
              .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
              .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
              .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
          if (isSTO) {
            il.setIgnoredSTOInodes(inode.getInodeId());
            locks.add(lf.getSubTreeOpsLock(TransactionLockTypes.LockType.WRITE, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src), false));
          }
          locks.add(il).add(lf.getBlockLock());
          locks.add(lf.getAcesLock());
        }

        @Override
        public Object performTask() throws IOException {
          FSPermissionChecker pc = fsd.getPermissionChecker();
          final INodesInPath iip = fsd.getINodesInPath4Write(src);
          fsd.checkOwner(pc, iip);
          unprotectedSetPermission(fsd, src, permission);
          //remove sto from
          if (isSTO) {
            INode inode = iip.getLastINode();
            if (inode != null && inode.isSTOLocked()) {
              inode.setSubtreeLocked(false);
              EntityManager.update(inode);
            }
            SubTreeOperation subTreeOp = EntityManager.find(SubTreeOperation.Finder.ByPath, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src));
            EntityManager.remove(subTreeOp);
          }
          return fsd.getAuditFileInfo(iip);
        }
      }.handle();
      txFailed = false;
      return ret;
    } finally {
      if (txFailed) {
        if (inode != null) {
          fsd.getFSNamesystem().unlockSubtree(src, inode.getInodeId());
        }
      }
    }
  }

  static HdfsFileStatus setOwner(
      final FSDirectory fsd, String srcArg, final String username, final String group)
      throws IOException {
    //only for testing STO
    fsd.getFSNamesystem().saveTimes();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    boolean txFailed = true;
    //we just want to lock the subtree the permission are checked in setOwnerSTOInt
    final INodeIdentifier inode = fsd.getFSNamesystem().lockSubtreeAndCheckOwnerAndParentPermission(src, true,
        null, SubTreeOperation.Type.SET_OWNER_STO);
    final boolean isSTO = inode != null;
    try {
      HdfsFileStatus ret
          = (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.SET_OWNER_SUBTREE, src) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                  .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
                  .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
              if (isSTO) {
                il.setIgnoredSTOInodes(inode.getInodeId());
                locks.add(lf.getSubTreeOpsLock(TransactionLockTypes.LockType.WRITE, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src), false));
              }
              locks.add(il).add(lf.getBlockLock()).add(lf.getAcesLock());
            }

            @Override
            public Object performTask() throws IOException {

              FSPermissionChecker pc = fsd.getPermissionChecker();
              final INodesInPath iip = fsd.getINodesInPath4Write(src);
              fsd.checkOwner(pc, iip);
              if (!pc.isSuperUser()) {
                if (username != null && !pc.getUser().equals(username)) {
                  throw new AccessControlException("Non-super user cannot change owner");
                }
                if (group != null && !pc.containsGroup(group)) {
                  throw new AccessControlException("User does not belong to " + group);
                }
              }
              unprotectedSetOwner(fsd, src, username, group);

              if (isSTO) {
                INodesInPath inodesInPath = fsd.getINodesInPath(src, false);
                INode inode = inodesInPath.getLastINode();
                if (inode != null && inode.isSTOLocked()) {
                  inode.setSubtreeLocked(false);
                  EntityManager.update(inode);
                }
                SubTreeOperation subTreeOp = EntityManager.find(SubTreeOperation.Finder.ByPath, fsd.
                    getFSNamesystem().getSubTreeLockPathPrefix(src));
                EntityManager.remove(subTreeOp);
              }
              return fsd.getAuditFileInfo(iip);
            }
          }.handle();
      txFailed = false;
      return ret;
    } finally {
      if (txFailed) {
        if (inode != null) {
          fsd.getFSNamesystem().unlockSubtree(src, inode.getInodeId());
        }
      }
    }
  }

  static HdfsFileStatus setTimes(
      final FSDirectory fsd, String srcArg, final long mtime, final long atime)
      throws IOException {
    if (!fsd.isAccessTimeSupported() && atime != -1) {
      throw new IOException(
          "Access time for hdfs is not configured. " +
              " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY
              + " configuration parameter.");
    }

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.SET_TIMES, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock());
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {

        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath4Write(src);
        // Write access is required to set access and modification times
        if (fsd.isPermissionEnabled()) {
          fsd.checkPathAccess(pc, iip, FsAction.WRITE);
        }
        final INode inode = iip.getLastINode();
        if (inode == null) {
          throw new FileNotFoundException("File/Directory " + src + " does not exist.");
        }
        boolean changed = unprotectedSetTimes(fsd, inode, mtime, atime, true);
        return fsd.getAuditFileInfo(iip);
      }
    }.handle();
  }

  static boolean setReplication(
      final FSDirectory fsd, final BlockManager bm, String srcArg, final short replication)
      throws IOException {
    bm.verifyReplication(srcArg, replication, null);
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    HopsTransactionalRequestHandler setReplicationHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.SET_REPLICATION,
        src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il).add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.UR, BLK.IV)).add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {

        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath4Write(src);
        if (fsd.isPermissionEnabled()) {
          fsd.checkPathAccess(pc, iip, FsAction.WRITE);
        }

        final short[] blockRepls = new short[2]; // 0: old, 1: new
        final Block[] blocks = unprotectedSetReplication(fsd, src, replication,
            blockRepls);
        final boolean isFile = blocks != null;
        INode targetNode = iip.getLastINode();
        // [s] for the files stored in the database setting the replication level does not make
        // any sense. For now we will just set the replication level as requested by the user
        if (isFile && !((INodeFile) targetNode).isFileStoredInDB()) {
          bm.setReplication(blockRepls[0], blockRepls[1], src, blocks);
        }
        return isFile;
      }
    };
    return (Boolean) setReplicationHandler.handle();
  }

  static HdfsFileStatus setStoragePolicy(
      final FSDirectory fsd, final BlockManager bm, String srcArg, final String policyName)
      throws IOException {
    if (!fsd.isStoragePolicyEnabled()) {
      throw new IOException(
          "Failed to set storage policy since "
              + DFS_STORAGE_POLICY_ENABLED_KEY + " is set to false.");
    }
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = FSDirectory.resolvePath(srcArg, pathComponents, fsd);
    
    HopsTransactionalRequestHandler setStoragePolicyHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.SET_STORAGE_POLICY, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {

        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath4Write(src);

        if (fsd.isPermissionEnabled()) {
          fsd.checkPathAccess(pc, iip, FsAction.WRITE);
        }

        // get the corresponding policy and make sure the policy name is valid
        BlockStoragePolicy policy = bm.getStoragePolicy(policyName);
        if (policy == null) {
          throw new HadoopIllegalArgumentException(
              "Cannot find a block policy with the name " + policyName);
        }
        unprotectedSetStoragePolicy(fsd, bm, iip, policy.getId());

        return fsd.getAuditFileInfo(iip);
      }
    };
    return (HdfsFileStatus) setStoragePolicyHandler.handle();
  }

  static BlockStoragePolicy[] getStoragePolicies(BlockManager bm)
      throws IOException {
    return bm.getStoragePolicies();
  }

  static long getPreferredBlockSize(final FSDirectory fsd, String srcArg)
      throws IOException {
    
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
   
    HopsTransactionalRequestHandler getPreferredBlockSizeHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_PREFERRED_BLOCK_SIZE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ_COMMITTED, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        final INodesInPath iip = fsd.getINodesInPath(src, false);
        if (fsd.isPermissionEnabled()) {
          fsd.checkTraverse(pc, iip);
        }
        return INodeFile.valueOf(iip.getLastINode(), src)
            .getPreferredBlockSize();
      }
    };
    return (Long) getPreferredBlockSizeHandler.handle();
  }

  /**
   * Set the namespace, storagespace and typespace quota for a directory.
   *
   * Note: This does not support ".inodes" relative path.
   */
  static void setQuota(final FSDirectory fsd, String srcArg, final long nsQuota, final long ssQuota,
      final StorageType type) throws IOException {
    if (fsd.isPermissionEnabled()) {
      FSPermissionChecker pc = fsd.getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }

    if (!fsd.getFSNamesystem().getNameNode().isLeader() && fsd.isQuotaEnabled()) {
      throw new NotALeaderException("Quota enabled. Delete operation can only be performed on a " + "leader namenode");
    }

    INodeIdentifier subtreeRoot = null;
    boolean removeSTOLock = false;
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents, fsd);
    try {
      PathInformation pathInfo = fsd.getFSNamesystem().getPathExistingINodesFromDB(src,
          false, null, null, null, null);
      INode lastComp = pathInfo.getINodesInPath().getLastINode();
      if (lastComp == null) {
        throw new FileNotFoundException("Directory does not exist: " + src);
      } else if (!lastComp.isDirectory()) {
        throw new FileNotFoundException(src + ": Is not a directory");
      } else if (lastComp.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
        throw new IllegalArgumentException(
            "Cannot clear namespace quota on root.");
      }

      //check if the path is root
      if (INode.getPathNames(src).length == 0) { // this method return empty array in case of
        // path = "/"
        subtreeRoot = INodeDirectory.getRootIdentifier();
        subtreeRoot.setStoragePolicy(BlockStoragePolicySuite.ID_UNSPECIFIED);
      } else {
        //this can only be called by super user we just want to lock the tree, not check needed
        subtreeRoot = fsd.getFSNamesystem().lockSubtree(src, SubTreeOperation.Type.QUOTA_STO);
        if (subtreeRoot == null) {
          // in the mean while the dir has been deleted by someone
          throw new FileNotFoundException("Directory does not exist: " + src);
        }
        removeSTOLock = true;
      }

      final AbstractFileTree.IdCollectingCountingFileTree fileTree = new AbstractFileTree.IdCollectingCountingFileTree(
          fsd.getFSNamesystem(), subtreeRoot, subtreeRoot.getStoragePolicy());
      fileTree.buildUp(fsd.getBlockStoragePolicySuite());
      Iterator<Long> idIterator = fileTree.getOrderedIds().descendingIterator();
      synchronized (idIterator) {
        fsd.getFSNamesystem().getQuotaUpdateManager().addPrioritizedUpdates(idIterator);
        try {
          idIterator.wait();
        } catch (InterruptedException e) {
          // Not sure if this can happen if we are not shutting down but we
          // need to abort in case it happens.
          throw new IOException("Operation failed due to an Interrupt");
        }
      }

      HopsTransactionalRequestHandler setQuotaHandler = new HopsTransactionalRequestHandler(HDFSOperationType.SET_QUOTA,
          src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
              .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
              .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
              .setIgnoredSTOInodes(fileTree.getSubtreeRootId().getInodeId())
              .setIgnoredSTOInodes(fileTree.getSubtreeRootId().getInodeId());
          locks.add(il).add(lf.getBlockLock());
        }

        @Override
        public Object performTask() throws IOException {

          INodeDirectory changed = unprotectedSetQuota(fsd, src, nsQuota, ssQuota, fileTree.getUsedCounts(), type);
          if (changed != null) {
            final QuotaCounts q = changed.getQuotaCounts();
          }
          return null;
        }
      };
      setQuotaHandler.handle();
    } finally {
      if (removeSTOLock) {
        fsd.getFSNamesystem().unlockSubtree(src, subtreeRoot.getInodeId());
      }
    }
  }

  static void unprotectedSetPermission(
      FSDirectory fsd, String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
             QuotaExceededException, StorageException, TransactionContextException {
    final INodesInPath inodesInPath = fsd.getINodesInPath4Write(src, true);
    final INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    inode.setPermission(permissions);
  }

  static void unprotectedSetOwner(
      FSDirectory fsd, String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, IOException {
    final INodesInPath inodesInPath = fsd.getINodesInPath4Write(src, true);
    INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }

    if (username != null) {
      try{
        UsersGroups.addUser(username); // The user may already eixst.
      } catch (UserAlreadyExistsException e){ }
      inode.setUser(username);
      inode.setUserID(UsersGroups.getUserID(username));
    }

    if (groupname != null) {
      try {
        UsersGroups.addGroup(groupname);
      } catch (GroupAlreadyExistsException e){}
      inode.setGroup(groupname);
      inode.setGroupID(UsersGroups.getGroupID(groupname));
    }
    inode.logMetadataEvent(INodeMetadataLogEntry.Operation.Update);
  }

  static boolean setTimes(
      FSDirectory fsd, INode inode, long mtime, long atime, boolean force) throws QuotaExceededException,
      TransactionContextException, StorageException {
    return unprotectedSetTimes(fsd, inode, mtime, atime, force);
  }

  static boolean unprotectedSetTimes(
      FSDirectory fsd, String src, long mtime, long atime, boolean force)
      throws UnresolvedLinkException, QuotaExceededException, TransactionContextException, StorageException {
    final INodesInPath i = fsd.getINodesInPath(src, true);
    return unprotectedSetTimes(fsd, i.getLastINode(), mtime, atime,
                               force);
  }

  /**
   * See {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String,
   *     long, long, StorageType)}
   * for the contract.
   * Sets quota for for a directory.
   * @return INodeDirectory if any of the quotas have changed. null otherwise.
   * @throws FileNotFoundException if the path does not exist.
   * @throws PathIsNotDirectoryException if the path is not a directory.
   * @throws QuotaExceededException if the directory tree size is
   *                                greater than the given quota
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  static INodeDirectory unprotectedSetQuota(
      FSDirectory fsd, String src, long nsQuota, long ssQuota, QuotaCounts fileTreeUsage, StorageType type)
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException, 
      UnsupportedActionException, StorageException, TransactionContextException {
    if (!fsd.isQuotaEnabled()) {
      return null;
    }
    // sanity check
    if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET &&
         nsQuota != HdfsConstants.QUOTA_RESET) ||
        (ssQuota < 0 && ssQuota != HdfsConstants.QUOTA_DONT_SET &&
          ssQuota != HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "ssQuota : " + nsQuota + " and " +
                                         ssQuota);
    }
    // sanity check for quota by storage type
    if ((type != null) && (!fsd.isQuotaByStorageTypeEnabled() ||
        nsQuota != HdfsConstants.QUOTA_DONT_SET)) {
      throw new UnsupportedActionException(
          "Failed to set quota by storage type because either" +
          DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY + " is set to " +
          fsd.isQuotaByStorageTypeEnabled() + " or nsQuota value is illegal " +
          nsQuota);
    }

    String srcs = FSDirectory.normalizePath(src);
    final INodesInPath iip = fsd.getINodesInPath4Write(srcs, true);
    INodeDirectory dirNode = INodeDirectory.valueOf(iip.getLastINode(), srcs);
    if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Cannot clear namespace quota on root.");
    } else { // a directory inode
      final QuotaCounts oldQuota = dirNode.getQuotaCounts();
      final long oldNsQuota = oldQuota.getNameSpace();
      final long oldSsQuota = oldQuota.getStorageSpace();

      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (ssQuota == HdfsConstants.QUOTA_DONT_SET) {
        ssQuota = oldSsQuota;
      }

      // unchanged space/namespace quota
      if (type == null && oldNsQuota == nsQuota && oldSsQuota == ssQuota) {
        return null;
      }

      // unchanged type quota
      if (type != null) {
          EnumCounters<StorageType> oldTypeQuotas = oldQuota.getTypeSpaces();
          if (oldTypeQuotas != null && oldTypeQuotas.get(type) == ssQuota) {
              return null;
          }
      }
      
      if (!dirNode.isRoot()) {
        dirNode.setQuota(fsd.getBlockStoragePolicySuite(), nsQuota, ssQuota, fileTreeUsage, type);
        INodeDirectory parent = (INodeDirectory) iip.getINode(-2);
        parent.replaceChild(dirNode); //to update db?
      }
      return dirNode;
    }
  }

  static Block[] unprotectedSetReplication(
      FSDirectory fsd, String src, short replication, short[] blockRepls)
      throws QuotaExceededException, UnresolvedLinkException, StorageException, TransactionContextException{

    final INodesInPath iip = fsd.getINodesInPath4Write(src, true);
    final INode inode = iip.getLastINode();
    if (inode == null || !inode.isFile()) {
      return null;
    }
    INodeFile file = inode.asFile();
    final short oldBR = file.getBlockReplication();
    
    // before setFileReplication, check for increasing block replication.
    // if replication > oldBR, then newBR == replication.
    // if replication < oldBR, we don't know newBR yet.
    if (replication > oldBR) {
      long dsDelta = file.storagespaceConsumed()/oldBR;
      fsd.updateCount(iip, 0L, dsDelta, oldBR, replication, true);
    }

    file.setFileReplication(replication);

    final short newBR = file.getBlockReplication();
    // check newBR < oldBR case.
    if (newBR < oldBR) {
      long dsDelta = file.storagespaceConsumed()/newBR;
      fsd.updateCount(iip, 0L, dsDelta, oldBR, newBR, true);
    }

    if (blockRepls != null) {
      blockRepls[0] = oldBR;
      blockRepls[1] = newBR;
    }
    return file.getBlocks();
  }

  static void unprotectedSetStoragePolicy(
      FSDirectory fsd, BlockManager bm, INodesInPath iip, byte policyId)
      throws IOException {
    final INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File/Directory does not exist: "
          + iip.getPath());
    }
    if (inode.isFile()) {
      inode.asFile().setStoragePolicyID(policyId);
    } else if (inode.isDirectory()) {
      setDirStoragePolicy(fsd, inode.asDirectory(), policyId);
    } else {
      throw new FileNotFoundException(iip.getPath()
          + " is not a file or directory");
    }
  }

  private static void setDirStoragePolicy(
      FSDirectory fsd, INodeDirectory inode, byte policyId) throws IOException {
    inode.setStoragePolicyID(policyId);
  }

  private static boolean unprotectedSetTimes(
      FSDirectory fsd, INode inode, long mtime, long atime, boolean force) throws QuotaExceededException,
      TransactionContextException, StorageException {
    boolean status = false;
    if (mtime != -1) {
      inode.setModificationTime(mtime);
      status = true;
    }
    if (atime != -1) {
      long inodeTime = inode.getAccessTime();

      // if the last access time update was within the last precision interval, then
      // no need to store access time
      if (atime <= inodeTime + fsd.getFSNamesystem().getAccessTimePrecision()
          && !force) {
        status =  false;
      } else {
        inode.setAccessTime(atime);
        status = true;
      }
    }
    return status;
  }
}
