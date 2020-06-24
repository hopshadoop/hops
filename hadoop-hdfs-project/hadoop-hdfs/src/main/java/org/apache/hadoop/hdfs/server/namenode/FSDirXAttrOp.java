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

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

import static io.hops.transaction.lock.LockFactory.getInstance;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;

class FSDirXAttrOp {
  private static final XAttr UNREADABLE_BY_SUPERUSER_XATTR =
      XAttrHelper.buildXAttr(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER, null);

  public static final XAttr XATTR_ENCRYPTION_ZONE =
       XAttrHelper.buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, null);
  public static final XAttr XATTR_FILE_ENCRYPTION_INFO =
       XAttrHelper.buildXAttr(CRYPTO_XATTR_FILE_ENCRYPTION_INFO, null);
  
  static HdfsFileStatus setXAttr(final FSDirectory fsd, final String srcArg,
      final String src,
      final XAttr xAttr, final EnumSet<XAttrSetFlag> flag)
      throws  IOException {
    checkXAttrsConfigFlag(fsd);
    checkXAttrSize(fsd, xAttr);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.SET_XATTR) {
      
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE, TransactionLockTypes.INodeResolveType.PATH, src)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il);
        List<XAttr> xAttrsToLock = new ArrayList<>();
        xAttrsToLock.add(xAttr);
        xAttrsToLock.add(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO);
        xAttrsToLock.add(FSDirXAttrOp.XATTR_ENCRYPTION_ZONE);
        locks.add(lf.getXAttrLock(xAttrsToLock));
        locks.add(lf.getAcesLock());
        locks.add(lf.getEZLock());
        
        if(fsd.getFSNamesystem().isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId(), Server.getRpcEpoch()));
        }
      }
      
      @Override
      public Object performTask() throws IOException {
        RetryCacheEntry cacheEntry = LightWeightCacheDistributed.get();
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null;
        }
        HdfsFileStatus stat = null;
        boolean success = false;
        try {
          stat = setXAttrInt(fsd, srcArg, src, xAttr, flag, cacheEntry != null);
          success = true;
        }finally {
          LightWeightCacheDistributed.put(null, success);
        }
        return stat;
      }
    }.handle();
  }
  
  static List<XAttr> getXAttrs(final FSDirectory fsd, final String srcArg,
      final String src, final List<XAttr> xAttrs) throws IOException {
    checkXAttrsConfigFlag(fsd);
    return (List<XAttr>) new HopsTransactionalRequestHandler(HDFSOperationType.GET_XATTRS) {
      
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ, TransactionLockTypes.INodeResolveType.PATH
            , src)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il);
        locks.add(lf.getXAttrLock(xAttrs));
        locks.add(lf.getAcesLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        return getXAttrsInt(fsd, srcArg, src, xAttrs);
      }
    }.handle();
  }
  
  static List<XAttr> listXAttrs(final FSDirectory fsd, final String srcArg,
      final String src) throws IOException {
    checkXAttrsConfigFlag(fsd);
    return (List<XAttr>) new HopsTransactionalRequestHandler(HDFSOperationType.LIST_XATTRS) {
      
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ, TransactionLockTypes.INodeResolveType.PATH
            , src)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il);
        locks.add(lf.getXAttrLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        return listXAttrsInt(fsd, srcArg, src);
      }
    }.handle();
  }
  
  static HdfsFileStatus removeXAttr(final FSDirectory fsd, final String srcArg,
      final String src, final XAttr xAttr) throws IOException {
    checkXAttrsConfigFlag(fsd);
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.GET_XATTRS){
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE, TransactionLockTypes.INodeResolveType.PATH
            , src)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());
        locks.add(il);
        List<XAttr> xAttrsToLock = new ArrayList<>();
        xAttrsToLock.add(xAttr);
        xAttrsToLock.add(FSDirXAttrOp.XATTR_FILE_ENCRYPTION_INFO);
        locks.add(lf.getXAttrLock(xAttrsToLock));
        locks.add(lf.getAcesLock());
        if(fsd.getFSNamesystem().isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId(), Server.getRpcEpoch()));
        }
        locks.add(lf.getEZLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        RetryCacheEntry cacheEntry = LightWeightCacheDistributed.get();
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        HdfsFileStatus stat;
        boolean success = false;
        try {
          stat = removeXAttrInt(fsd, srcArg, src, xAttr, cacheEntry != null);
          success = true;
        } finally {
          LightWeightCacheDistributed.put(null, success);
        }
        return stat;
      }
      
    }.handle();
  }
  
  private static HdfsFileStatus setXAttrInt(
      FSDirectory fsd, String srcArg, String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag,
      boolean logRetryCache)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    XAttrPermissionFilter.checkPermissionForApi(pc, xAttr, FSDirectory.isReservedRawName(srcArg));
    final INodesInPath iip = fsd.getINodesInPath4Write(src);
    checkXAttrChangeAccess(fsd, iip, xAttr, pc);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    unprotectedSetXAttrs(fsd, src, xAttrs, flag);
    return fsd.getAuditFileInfo(fsd.getINodesInPath(src, false));
  }

  private static List<XAttr> getXAttrsInt(FSDirectory fsd, final String srcArg,
      final String src, List<XAttr> xAttrs)
      throws IOException {
    final boolean isRawPath = FSDirectory.isReservedRawName(srcArg);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    boolean getAll = xAttrs == null || xAttrs.isEmpty();
    if (!getAll) {
      XAttrPermissionFilter.checkPermissionForApi(pc, xAttrs, isRawPath);
    }
    
    final INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsd.isPermissionEnabled()) {
      fsd.checkPathAccess(pc, iip, FsAction.READ);
    }
    List<XAttr> all = getXAttrsInt(fsd, src, xAttrs);
    List<XAttr> filteredAll = XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, isRawPath);
  
    if (getAll) {
      return filteredAll;
    }
  
    if (filteredAll == null || !iip.getLastINode().hasXAttrs()) {
      return null;
    }
  
    List<XAttr> toGet = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      boolean foundIt = false;
      for (XAttr a : filteredAll) {
        if (xAttr.getNameSpace() == a.getNameSpace()
            && xAttr.getName().equals(a.getName())) {
          toGet.add(a);
          foundIt = true;
          break;
        }
      }
      if (!foundIt) {
        throw new IOException(
            "At least one of the attributes provided was not found.");
      }
    }
    return toGet;
  }

  private static List<XAttr> listXAttrsInt(
      FSDirectory fsd, String srcArg, String src) throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsd.isPermissionEnabled()) {
      /* To access xattr names, you need EXECUTE in the owning directory. */
      fsd.checkParentAccess(pc, iip, FsAction.EXECUTE);
    }
    final List<XAttr> all = getXAttrsInt(fsd, src);
    final List<XAttr> filteredAll = XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, FSDirectory.isReservedRawName(srcArg));
    return filteredAll;
  }

  /**
   * Remove an xattr for a file or directory.
   *
   * @param src
   *          - path to remove the xattr from
   * @param xAttr
   *          - xAttr to remove
   * @throws IOException
   */
  private static HdfsFileStatus removeXAttrInt(
      FSDirectory fsd,String srcArg, String src, XAttr xAttr, boolean logRetryCache)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    XAttrPermissionFilter.checkPermissionForApi(pc, xAttr, FSDirectory.isReservedRawName(srcArg));
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    checkXAttrChangeAccess(fsd, iip, xAttr, pc);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    List<XAttr> removedXAttrs = unprotectedRemoveXAttrs(fsd, src, xAttrs);
    if (removedXAttrs != null && !removedXAttrs.isEmpty()) {
      //
    } else {
      throw new IOException(
          "No matching attributes found for remove operation");
    }
    return fsd.getAuditFileInfo(fsd.getINodesInPath(src, false));
  }
  

  static XAttr unprotectedGetXAttrByName(
      INode inode, String xAttrName)
      throws IOException {
    List<XAttr> xAttrs = unprotectedGetXAttrs(inode);
    if (xAttrs == null) {
      return null;
    }
    for (XAttr x : xAttrs) {
      if (XAttrHelper.getPrefixName(x)
          .equals(xAttrName)) {
        return x;
      }
    }
    return null;
  }
  
  private static void checkXAttrsConfigFlag(FSDirectory fsd) throws
      IOException {
    if (!fsd.isXattrsEnabled()) {
      throw new IOException(String.format(
          "The XAttr operation has been rejected.  "
              + "Support for XAttrs has been disabled by setting %s to false.",
          DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }
  
  private static void checkXAttrChangeAccess(
      FSDirectory fsd, INodesInPath iip, XAttr xAttr,
      FSPermissionChecker pc)
      throws IOException {
    if (fsd.isPermissionEnabled() && xAttr.getNameSpace() == XAttr.NameSpace
        .USER) {
      final INode inode = iip.getLastINode();
      if (inode != null &&
          inode.isDirectory() &&
          inode.getFsPermission().getStickyBit()) {
        if (!pc.isSuperUser()) {
          fsd.checkOwner(pc, iip);
        }
      } else {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
    }
  }
  
  /**
   * Verifies that the size of the name and value of an xattr is within
   * the configured limit.
   */
  public static void checkXAttrSize(FSDirectory fsd, final XAttr xAttr) {
    int nameSize = XAttrStorage.getXAttrByteSize(xAttr.getName());
    int valueSize = 0;
    if (xAttr.getValue() != null) {
      valueSize = xAttr.getValue().length;
    }
    
    if (nameSize > XAttrStorage.getMaxXAttrNameSize()) {
      throw new HadoopIllegalArgumentException(
          "The XAttr name is too big. The maximum size of the"
              + " name is " + XAttrStorage.getMaxXAttrNameSize()
              + ", but the name size is " + nameSize);
    }
    
    if (valueSize > XAttrStorage.getMaxXAttrValueSize()) {
      throw new HadoopIllegalArgumentException(
          "The XAttr value is too big. The maximum size of the"
              + " value is " + XAttrStorage.getMaxXAttrValueSize()
              + ", but the value size is " + valueSize);
    }
  
    int allowedValueSize = fsd.getXattrMaxSize() - XAttrStorage.getMaxXAttrNameSize();
    if (allowedValueSize > 0 && valueSize > allowedValueSize) {
      throw new HadoopIllegalArgumentException(
          "The XAttr value is too big. The maximum size of the"
              + " value is " + allowedValueSize
              + ", but the value size is " + valueSize);
    }
    
    int size = nameSize + valueSize;
    if (size > fsd.getXattrMaxSize()) {
      throw new HadoopIllegalArgumentException(
          "The XAttr is too big. The maximum combined size of the"
              + " name and value is " + fsd.getXattrMaxSize()
              + ", but the total size is " + size);
    }
  }
  
  static void unprotectedSetXAttrs(FSDirectory fsd, final String src,
      final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    INodesInPath iip = fsd.getINodesInPath4Write(fsd.normalizePath(src), true);
    INode inode = fsd.resolveLastINode(iip);
    setINodeXAttrs(fsd, inode, xAttrs, flag);
  }
  
  static void setINodeXAttrs(FSDirectory fsd, INode inode,
      final List<XAttr> toSet,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    
    // Check for duplicate XAttrs in toSet
    // We need to use a custom comparator, so using a HashSet is not suitable
    for (int i = 0; i < toSet.size(); i++) {
      for (int j = i + 1; j < toSet.size(); j++) {
        if (toSet.get(i).equalsIgnoreValue(toSet.get(j))) {
          throw new IOException("Cannot specify the same XAttr to be set " +
              "more than once");
        }
      }
    }
    
    // Check if the XAttr already exists to validate with the provided flag
    for (XAttr xAttr: toSet) {
      boolean exists = XAttrStorage.readINodeXAttr(inode, xAttr) != null;
      XAttrSetFlag.validate(xAttr.getName(), exists, flag);
      incrementXAttrs(fsd, inode, xAttr, exists);
      
      final String xaName = XAttrHelper.getPrefixName(xAttr);
      
      /*
       * If we're adding the encryption zone xattr, then add src to the list
       * of encryption zones.
       */
      if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
        final HdfsProtos.ZoneEncryptionInfoProto ezProto =
            HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xAttr.getValue());
        fsd.ezManager.addEncryptionZone(inode.getId(),
                                        PBHelper.convert(ezProto.getSuite()),
                                        PBHelper.convert(
                                            ezProto.getCryptoProtocolVersion()),
                                        ezProto.getKeyName());
      }
      if (!inode.isFile() && SECURITY_XATTR_UNREADABLE_BY_SUPERUSER.equals(xaName)) {
        throw new IOException("Can only set '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' on a file.");
      }
      
      XAttrStorage.updateINodeXAttr(inode, xAttr, exists, fsd.getFSNamesystem().getNamenodeId());
    }
  }
  
  private static void incrementXAttrs(FSDirectory fsd, INode inode,
      XAttr xAttr, boolean xAttrExists)
      throws IOException {
    if(xAttrExists)
      return;
    
    boolean limitsExceeded = false;
    String message = "Cannot add additional %sXAttr to inode, would exceed " +
        "limit of %d";
    
    if(isUserVisible(xAttr)) {
      if(inode.getNumUserXAttrs()== XAttrStorage.getMaxNumberOfUserXAttrPerInode()){
        limitsExceeded = true;
      }else {
        inode.incrementUserXAttrs();
        limitsExceeded = inode.getNumUserXAttrs() > fsd.getInodeXAttrsLimit();
      }
      message = String.format(message, "", fsd.getInodeXAttrsLimit());
    }else {
      if(inode.getNumSysXAttrs() == XAttrStorage.getMaxNumberOfSysXAttrPerInode()){
        limitsExceeded = true;
        message = String.format(message, "System ",
            XAttrStorage.getMaxNumberOfSysXAttrPerInode());
      }else{
        inode.incrementSysXAttrs();
      }
    }
    
    if(limitsExceeded){
      throw new IOException(message);
    }
  }
  
  private static boolean isUserVisible(XAttr xAttr) {
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER ||
        xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED ||
        xAttr.getNameSpace() == XAttr.NameSpace.PROVENANCE) {
      return true;
    }
    return false;
  }
  
  static List<XAttr> getXAttrs(INode inode) throws IOException {
    return unprotectedGetXAttrs(inode, Collections.<XAttr>emptyList());
  }
  
  private static List<XAttr> getXAttrsInt(FSDirectory fsd,
      String src) throws IOException {
    return getXAttrsInt(fsd, src, Collections.<XAttr>emptyList());
  }
  
  private static List<XAttr> getXAttrsInt(FSDirectory fsd,
      String src, List<XAttr> xAttrs) throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    INodesInPath iip = fsd.getINodesInPath(srcs, true);
    INode inode = FSDirectory.resolveLastINode(iip);
    return unprotectedGetXAttrs(inode, xAttrs);
  }
  
  private static List<XAttr> unprotectedGetXAttrs(INode inode)
      throws IOException {
    return XAttrStorage.readINodeXAttrs(inode, Collections.<XAttr>emptyList());
  }
  
  private static List<XAttr> unprotectedGetXAttrs(INode inode, List<XAttr> xAttrs)
      throws IOException {
    return XAttrStorage.readINodeXAttrs(inode, xAttrs);
  }
  
  static List<XAttr> unprotectedRemoveXAttrs(FSDirectory fsd, final String src,
      final List<XAttr> toRemove) throws IOException {
    INodesInPath iip = fsd.getINodesInPath4Write(fsd.normalizePath(src), true);
    INode inode = fsd.resolveLastINode(iip);
    List<XAttr> storedXAttrs = XAttrStorage.readINodeXAttrs(inode, toRemove);
    for(XAttr xAttr : toRemove){
      if (UNREADABLE_BY_SUPERUSER_XATTR.equalsIgnoreValue(xAttr)) {
        throw new AccessControlException("The xattr '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' can not be deleted.");
      }
    }
    for(XAttr xAttr : storedXAttrs){
      XAttrStorage.removeINodeXAttr(inode, xAttr, fsd.getFSNamesystem().getNamenodeId());
      decrementXAttrs(inode, xAttr);
    }
    
    if(!storedXAttrs.isEmpty())
      return storedXAttrs;
    return null;
  }
  
  private static void decrementXAttrs(INode inode, XAttr xAttr)
      throws TransactionContextException, StorageException {
    if(isUserVisible(xAttr)) {
      inode.decrementUserXAttrs();
    }else {
      inode.decrementSysXAttrs();
    }
  }
}
