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
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.FileProvenanceEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.ipc.RetriableException;

import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.RetryCacheDistributed;
import org.apache.hadoop.ipc.Server;
import static org.apache.hadoop.util.Time.now;

class FSDirDeleteOp {
  public static final Log LOG = LogFactory.getLog(FSDirDeleteOp.class);
  
  public static long BIGGEST_DELETABLE_DIR;
  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param fsd the FSDirectory instance
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param collectedBlocks Blocks under the deleted directory
   * @param removedINodes INodes that should be removed from inodeMap
   * @return the number of files that have been removed
   */
  static long delete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    final long filesRemoved;
      if (!deleteAllowed(iip, iip.getPath()) ) {
        filesRemoved = -1;
      } else {
        filesRemoved = unprotectedDelete(fsd, iip, collectedBlocks,
                                         removedINodes, mtime);
      }
    return filesRemoved;
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   *
   * @param fsn namespace
   * @param src path name to be deleted
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static boolean delete(
      final FSNamesystem fsn, String srcArg, final boolean recursive)
      throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(fsd.getPermissionChecker(), srcArg, pathComponents);

    if (!recursive) {
      // It is safe to do this as it will only delete a single file or an empty directory
      return deleteTransaction(fsn, src, recursive);
    }

    PathInformation pathInfo = fsn.getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INode pathInode = pathInfo.getINodesInPath().getLastINode();

    if (pathInode == null) {
      NameNode.stateChangeLog
          .debug("Failed to remove " + src + " because it does not exist");
      return false;
    } else if (pathInode.isRoot()) {
      NameNode.stateChangeLog.warn("Failed to remove " + src
          + " because the root is not allowed to be deleted");
      return false;
    }

    INodeIdentifier subtreeRoot = null;
    if (pathInode.isFile() || pathInode.isSymlink()) {
      return deleteTransaction(fsn, src, false);
    }

    RetryCache.CacheEntry cacheEntry = fsn.retryCacheWaitForCompletionTransactional();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
    }
    boolean ret = false;
    try {
      //if quota is enabled then only the leader namenode can delete the directory.
      //this is because before the deletion is done the quota manager has to apply all the outstanding
      //quota updates for the directory. The current design of the quota manager is not distributed.
      //HopsFS clients send the delete operations to the leader namenode if quota is enabled
      if (!fsn.isLeader()) {
        throw new QuotaUpdateException("Unable to delete the file " + src
            + " because Quota is enabled and I am not the leader");
      }

      //sub tree operation
      try {
        //once subtree is locked we still need to check all subAccess in AbstractFileTree.FileTree
        //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
        //access:null, subAccess:FsAction.ALL, ignoreEmptyDir:true
        subtreeRoot = fsn.lockSubtreeAndCheckOwnerAndParentPermission(src, false,
            FsAction.WRITE, SubTreeOperation.Type.DELETE_STO);

        List<AclEntry> nearestDefaultsForSubtree = fsn.calculateNearestDefaultAclForSubtree(pathInfo);
        AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(fsn, subtreeRoot, FsAction.ALL, true,
            nearestDefaultsForSubtree, subtreeRoot.getStoragePolicy());
        fileTree.buildUp(fsd.getBlockStoragePolicySuite());
        fsn.delayAfterBbuildingTree("Built tree for " + srcArg + " for delete op");

        if (fsd.isQuotaEnabled()) {
          Iterator<Long> idIterator = fileTree.getAllINodesIds().iterator();
          synchronized (idIterator) {
            fsn.getQuotaUpdateManager().addPrioritizedUpdates(idIterator);
            try {
              idIterator.wait();
            } catch (InterruptedException e) {
              // Not sure if this can happen if we are not shutting down but we need to abort in case it happens.
              throw new IOException("Operation failed due to an Interrupt");
            }
          }
        }

        for (int i = fileTree.getHeight(); i > 0; i--) {
          if (!deleteTreeLevel(fsn, src, fileTree.getSubtreeRoot().getId(), fileTree, i)) {
            ret = false;
            return ret;
          }
        }
      } finally {
        if (subtreeRoot != null) {
          fsn.unlockSubtree(src, subtreeRoot.getInodeId());
        }
      }
      ret = true;
      return ret;
    } finally {
      fsn.retryCacheSetStateTransactional(cacheEntry, ret);
    }
  }
  
    private static boolean deleteTreeLevel(final FSNamesystem fsn, final String subtreeRootPath, final long subTreeRootID,
      final AbstractFileTree.FileTree fileTree, int level) throws TransactionContextException, IOException {
    ArrayList<Future> barrier = new ArrayList<>();

    for (final ProjectedINode dir : fileTree.getDirsByLevel(level)) {
      if (fileTree.countChildren(dir.getId()) <= BIGGEST_DELETABLE_DIR) {
        final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
        Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
        barrier.add(f);
      } else {
        //delete the content of the directory one by one.
        for (final ProjectedINode inode : fileTree.getChildren(dir.getId())) {
          if (!inode.isDirectory()) {
            final String path = fileTree.createAbsolutePath(subtreeRootPath, inode);
            Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
            barrier.add(f);
          }
        }
        // the dir is empty now. delete it.
        final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
        Future f = multiTransactionDeleteInternal(fsn, path, subTreeRootID);
        barrier.add(f);
      }
    }

    boolean result = true;
    for (Future f : barrier) {
      try {
        if (!((Boolean) f.get())) {
          result = false;
        }
      } catch (ExecutionException e) {
        result = false;
        LOG.error("Exception was thrown during partial delete", e);
        Throwable throwable = e.getCause();
        if (throwable instanceof IOException) {
          throw (IOException) throwable; //pass the exception as is to the client
        } else {
          throw new IOException(e); //only io exception as passed to clients.
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return result;
  }
    
  private static Future multiTransactionDeleteInternal(final FSNamesystem fsn, final String src,
      final long subTreeRootId)
      throws StorageException, TransactionContextException, IOException {
    final FSDirectory fsd = fsn.getFSDirectory();

    return fsn.getFSOperationsExecutor().submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        HopsTransactionalRequestHandler deleteHandler = new HopsTransactionalRequestHandler(
            HDFSOperationType.SUBTREE_DELETE) {
          @Override
          public void acquireLock(TransactionLocks locks)
              throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, src)
                .setNameNodeID(fsn.getNamenodeId())
                .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!fsd.isQuotaEnabled())
                .setIgnoredSTOInodes(subTreeRootId);
            locks.add(il).add(lf.getLeaseLock(LockType.WRITE))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                .add(lf.getBlockLock()).add(
                lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE, BLK.IV, BLK.ER));

            locks.add(lf.getAllUsedHashBucketsLock());

            if (fsd.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src));
            }

            if (fsn.isErasureCodingEnabled()) {
              locks.add(lf.getEncodingStatusLock(true, LockType.WRITE, src));
            }
            locks.add(lf.getEZLock());
          }

          @Override
          public Object performTask() throws IOException {
            final INodesInPath iip = fsd.getINodesInPath4Write(src);
            if (!deleteInternal(fsn, src, iip)) {
              //at this point the delete op is expected to succeed. Apart from DB errors
              // this can only fail if the quiesce phase in subtree operation failed to
              // quiesce the subtree. See TestSubtreeConflicts.testConcurrentSTOandInodeOps
              throw new RetriableException("Unable to Delete path: " + src + "." + " Possible subtree quiesce failure");

            }
            return true;
          }
        };
        return (Boolean) deleteHandler.handle(this);
      }
    });
  }
    
  static boolean deleteTransaction(
      final FSNamesystem fsn, String srcArg, final boolean recursive)
      throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);

    HopsTransactionalRequestHandler deleteHandler = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src)
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il).add(lf.getLeaseLock(LockType.WRITE))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR,BLK.PE, BLK.IV,BLK.ER));
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }

        locks.add(lf.getAllUsedHashBucketsLock());

        if (fsd.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src));
        }
        if (fsn.isErasureCodingEnabled()) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
        }
        locks.add(lf.getEZLock());
      }

      @Override
      public Object performTask() throws IOException {
        RetryCache.CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return true; // Return previous response
        }
        boolean ret = false;
        try {

          final INodesInPath iip = fsd.getINodesInPath4Write(src, false);
          if (!recursive && fsd.isNonEmptyDirectory(iip)) {
            throw new PathIsNotEmptyDirectoryException(src + " is non empty");
          }
          if (fsd.isPermissionEnabled()) {
            fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                FsAction.ALL, true);
          }
          ret = deleteInternal(fsn, src, iip);
          return ret; 
        } finally {
          RetryCacheDistributed.setState(cacheEntry, ret);
        }
      }
    };
    return (Boolean) deleteHandler.handle();
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   * @param fsn namespace
   * @param src path name to be deleted
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static boolean deleteInternal(
      FSNamesystem fsn, String src, INodesInPath iip)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    
    long mtime = now();
    // Unlink the target directory from directory tree
    long filesRemoved = delete(
        fsd, iip, collectedBlocks, removedINodes, mtime);
    if (filesRemoved < 0) {
      return false;
    }
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeasesAndINodes(src, removedINodes);
    fsn.removeBlocks(collectedBlocks); // Incremental deletion of blocks
    collectedBlocks.clear();

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
                                        + src +" is removed");
    }
    return true;
  }

  static void incrDeletedFileCount(long count) {
    NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }

  private static boolean deleteAllowed(final INodesInPath iip,
      final String src) {
    if (iip.length() < 1 || iip.getLastINode() == null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: failed to remove "
                + src + " because it does not exist");
      }
      return false;
    } else if (iip.length() == 1) { // src is the root
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedDelete: failed to remove " + src +
              " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param fsd the FSDirectory instance
   * @param iip the inodes resolved from the path
   * @param collectedBlocks blocks collected from the deleted path
   * @param removedINodes inodes that should be removed from inodeMap
   * @param mtime the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */
  private static long unprotectedDelete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws IOException {

    // check if target node exists
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return -1;
    }
    
    // check if target node is the root
    if (iip.length() == 1) {
      return -1;
    }
  
    // Add metadata log entry for all deleted childred.
    addMetaDataLogForDirDeletion(targetNode, fsd.getFSNamesystem().getNamenodeId());
    
    // Remove the node from the namespace
    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return -1;
    }

    // set the parent's modification time
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime);
    if (removed == 0) {
      return 0;
    }    
            
    // collect block
    targetNode.destroyAndCollectBlocks(fsd.getBlockStoragePolicySuite(),
        collectedBlocks, removedINodes);
    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return removed;
  }
  
  private static void addMetaDataLogForDirDeletion(INode targetNode, long namenodeId) throws IOException {
    if (targetNode.isDirectory()) {
      List<INode> children = ((INodeDirectory) targetNode).getChildrenList();
      for(INode child : children){
       if(child.isDirectory()){
         addMetaDataLogForDirDeletion(child, namenodeId);
       }else{
         child.logMetadataEvent(INodeMetadataLogEntry.Operation.Delete);
         child.logProvenanceEvent(namenodeId, FileProvenanceEntry.Operation.delete());
       }
      }
    }
    targetNode.logMetadataEvent(INodeMetadataLogEntry.Operation.Delete);
    targetNode.logProvenanceEvent(namenodeId, FileProvenanceEntry.Operation.delete());
  }
}
