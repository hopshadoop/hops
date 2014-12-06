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

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class FSDirAclOp {
  static HdfsFileStatus modifyAclEntries(
      final FSDirectory fsd, final String srcArg, final List<AclEntry> aclSpec)
      throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.MODIFY_ACL_ENTRIES) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(false);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();

        INodesInPath iip = fsd.getINodesInPath4Write(
            FSDirectory.normalizePath(src), true);
        fsd.checkOwner(pc, iip);
        AclStorage.validateAclSpec(aclSpec);
        INode inode = FSDirectory.resolveLastINode(src, iip);
        if (aclSpec.size() == 1 && aclSpec.get(0).getType().equals(AclEntryType.MASK)) {
          //HOPS: We allow setting
          FsPermission fsPermission = inode.getFsPermission();
          inode.setPermission(new FsPermission(fsPermission.getUserAction(), aclSpec.get(0).getPermission(),
              fsPermission
                  .getOtherAction()));
          return AclStorage.readINodeLogicalAcl(inode);
        }
        List<AclEntry> existingAcl;
        if (AclStorage.hasOwnAcl(inode)) {
          existingAcl = AclStorage.readINodeLogicalAcl(inode);
        } else {
          existingAcl = AclStorage.getMinimalAcl(inode);
        }
        List<AclEntry> newAcl = AclTransformation.mergeAclEntries(
            existingAcl, aclSpec);
        AclStorage.updateINodeAcl(inode, newAcl);

        return fsd.getAuditFileInfo(src, false);
      }
    }.handle();
  }

  static HdfsFileStatus removeAclEntries(
      final FSDirectory fsd, final String srcArg, final List<AclEntry> aclSpec)
      throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_ACL_ENTRIES) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        INodesInPath iip = fsd.getINodesInPath4Write(
            FSDirectory.normalizePath(src), true);
        fsd.checkOwner(pc, iip);
        AclStorage.validateAclSpec(aclSpec);
        INode inode = FSDirectory.resolveLastINode(src, iip);
        List<AclEntry> existingAcl;
        if (AclStorage.hasOwnAcl(inode)) {
          existingAcl = AclStorage.readINodeLogicalAcl(inode);
        } else {
          existingAcl = AclStorage.getMinimalAcl(inode);
        }
        List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
            existingAcl, aclSpec);
        AclStorage.updateINodeAcl(inode, newAcl);
        return fsd.getAuditFileInfo(src, false);
      }
    }.handle();
  }

  static HdfsFileStatus removeDefaultAcl(final FSDirectory fsd, final String srcArg)
      throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_DEFAULT_ACL) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        INodesInPath iip = fsd.getINodesInPath4Write(
            FSDirectory.normalizePath(src), true);
        fsd.checkOwner(pc, iip);
        INode inode = FSDirectory.resolveLastINode(src, iip);
        List<AclEntry> existingAcl;
        if (AclStorage.hasOwnAcl(inode)) {
          existingAcl = AclStorage.readINodeLogicalAcl(inode);
        } else {
          existingAcl = AclStorage.getMinimalAcl(inode);
        }
        List<AclEntry> newAcl = AclTransformation.filterDefaultAclEntries(
            existingAcl);
        AclStorage.updateINodeAcl(inode, newAcl);
        return fsd.getAuditFileInfo(src, false);
      }
    }.handle();
  }

  static HdfsFileStatus removeAcl(final FSDirectory fsd, final String srcArg)
      throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_ACL) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        INodesInPath iip = fsd.getINodesInPath4Write(src);
        fsd.checkOwner(pc, iip);
        unprotectedRemoveAcl(fsd, src);
        return fsd.getAuditFileInfo(src, false);
      }
    }.handle();
  }

  static HdfsFileStatus setAcl(
      final FSDirectory fsd, final String srcArg, final List<AclEntry> aclSpec)
      throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.SET_ACL) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        INodesInPath iip = fsd.getINodesInPath4Write(src);
        fsd.checkOwner(pc, iip);
        List<AclEntry> newAcl = unprotectedSetAcl(fsd, src, aclSpec);
        return fsd.getAuditFileInfo(src, false);
      }
    }.handle();
  }

  static AclStatus getAclStatus(
      final FSDirectory fsd, String srcArg) throws IOException {
    checkAclsConfigFlag(fsd);
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = fsd.resolvePath(srcArg, pathComponents);
    return (AclStatus) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ACL_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNamenodeId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = fsd.getPermissionChecker();
        INodesInPath iip = fsd.getINodesInPath(src, true);
        if (fsd.isPermissionEnabled()) {
          fsd.checkTraverse(pc, iip);
        }
        INode inode = FSDirectory.resolveLastINode(src, iip);
        List<AclEntry> acl = AclStorage.readINodeAcl(inode);
        return new AclStatus.Builder()
            .owner(inode.getUserName()).group(inode.getGroupName())
            .stickyBit(inode.getFsPermission().getStickyBit())
            .addEntries(acl).build();
      }
    }.handle();
  }

  static List<AclEntry> unprotectedSetAcl(
      FSDirectory fsd, String src, List<AclEntry> aclSpec)
      throws IOException {
    AclStorage.validateAclSpec(aclSpec);
    // ACL removal is logged to edits as OP_SET_ACL with an empty list.
    if (aclSpec.isEmpty()) {
      unprotectedRemoveAcl(fsd, src);
      return AclFeature.EMPTY_ENTRY_LIST;
    }

    INodesInPath iip = fsd.getINodesInPath4Write(FSDirectory.normalizePath
        (src), true);
    INode inode = FSDirectory.resolveLastINode(src, iip);
    List<AclEntry> existingAcl;
    if (AclStorage.hasOwnAcl(inode)){
      existingAcl = AclStorage.readINodeLogicalAcl(inode);
    } else {
      existingAcl = AclStorage.getMinimalAcl(inode);
    }
    List<AclEntry> newAcl = AclTransformation.replaceAclEntries(existingAcl,
      aclSpec);
    AclStorage.updateINodeAcl(inode, newAcl);
    return newAcl;
  }

  private static void checkAclsConfigFlag(FSDirectory fsd) throws AclException {
    if (!fsd.isAclsEnabled()) {
      throw new AclException(String.format(
          "The ACL operation has been rejected.  "
              + "Support for ACLs has been disabled by setting %s to false.",
          DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY));
    }
  }

  private static void unprotectedRemoveAcl(FSDirectory fsd, String src)
      throws IOException {
    INodesInPath iip = fsd.getINodesInPath4Write(
        FSDirectory.normalizePath(src), true);
    INode inode = FSDirectory.resolveLastINode(src, iip);
    AclFeature f = inode.getAclFeature();
    if (inode.getNumAces() <= 0){
      return;
    }

    FsPermission perm = inode.getFsPermission();
    List<AclEntry> featureEntries = f.getEntries();
    if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
      // Restore group permissions from the feature's entry to permission
      // bits, overwriting the mask, which is not part of a minimal ACL.
      AclEntry groupEntryKey = new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS).setType(AclEntryType.GROUP).build();
      int groupEntryIndex = Collections.binarySearch(
          featureEntries, groupEntryKey,
          AclTransformation.ACL_ENTRY_COMPARATOR);
      assert groupEntryIndex >= 0;
      FsAction groupPerm = featureEntries.get(groupEntryIndex).getPermission();
      FsPermission newPerm = new FsPermission(perm.getUserAction(), groupPerm,
          perm.getOtherAction(), perm.getStickyBit());
      inode.setPermission(newPerm);
    }

    inode.removeAclFeature();
  }
}
