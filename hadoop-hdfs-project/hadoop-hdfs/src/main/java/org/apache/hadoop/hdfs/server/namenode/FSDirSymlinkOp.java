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

import io.hops.common.IDsGeneratorFactory;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import java.io.IOException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.RetryCacheDistributed;
import org.apache.hadoop.ipc.Server;

import static org.apache.hadoop.util.Time.now;

class FSDirSymlinkOp {

  static HdfsFileStatus createSymlinkInt(
      final FSNamesystem fsn, final String target, final String linkArg,
      final PermissionStatus dirPerms, final boolean createParent)
      throws IOException {
    final FSDirectory fsd = fsn.getFSDirectory();
    if (!DFSUtil.isValidName(linkArg)) {
      throw new InvalidPathException("Invalid link name: " + linkArg);
    }
    if (FSDirectory.isReservedName(target) || target.isEmpty()) {
      throw new InvalidPathException("Invalid target name: " + target);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target="
          + target + " link=" + linkArg);
    }

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(linkArg);
    final String link = fsd.resolvePath(linkArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.CREATE_SYM_LINK,
        link) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, link)
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getAcesLock());
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        final RetryCache.CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {

          FSPermissionChecker pc = fsn.getPermissionChecker();
          final INodesInPath iip = fsd.getINodesInPath4Write(link, false);
          if (!createParent) {
            fsd.verifyParentDir(iip, link);
          }
          if (!fsd.isValidToCreate(link, iip)) {
            throw new IOException(
                "failed to create link " + link + " either because the filename is invalid or the file exists");
          }
          if (fsd.isPermissionEnabled()) {
            fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
          }
          // validate that we have enough inodes.
          fsn.checkFsObjectLimit();

          // add symbolic link to namespace
          addSymlink(fsd, link, iip, target, dirPerms, createParent);

          NameNode.getNameNodeMetrics().incrCreateSymlinkOps();
          success = true;
          return fsd.getAuditFileInfo(link, false);
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
      }
    }.handle();
  }

  static INodeSymlink unprotectedAddSymlink(
      FSDirectory fsd, INodesInPath iip, long id, String target, long mtime,
      long atime, PermissionStatus perm)
      throws UnresolvedLinkException, QuotaExceededException, IOException {
    final INodeSymlink symlink = new INodeSymlink(id, target, mtime, atime,
        perm);
    return fsd.addINode(iip, symlink) ? symlink : null;
  }

  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   */
  private static INodeSymlink addSymlink(
      FSDirectory fsd, String path, INodesInPath iip, String target,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException {
    final long mtime = now();
    if (createParent) {
      INodesInPath parentIIP = iip.getParentINodesInPath();
      if (parentIIP == null || (parentIIP = FSDirMkdirOp.mkdirsRecursively(
          fsd,
          parentIIP, dirPerms, true, mtime)) == null) {
        return null;
      } else {
        iip = INodesInPath.append(parentIIP, null, iip.getLastLocalName());
      }
    }
    final String userName = dirPerms.getUserName();
    long id = IDsGeneratorFactory.getInstance().getUniqueINodeID();
    PermissionStatus perm = new PermissionStatus(
        userName, null, FsPermission.getDefault());
    INodeSymlink newNode =
        unprotectedAddSymlink(fsd, iip, id, target, mtime, mtime, perm);
    if (newNode == null) {
      NameNode.stateChangeLog.info("addSymlink: failed to add " + path);
      return null;
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("addSymlink: " + path + " is added");
    }
    return newNode;
  }
}
