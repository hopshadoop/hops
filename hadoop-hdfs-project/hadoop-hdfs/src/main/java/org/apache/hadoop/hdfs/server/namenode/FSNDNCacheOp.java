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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCacheDistributed;
import org.apache.hadoop.ipc.Server;

class FSNDNCacheOp {

  static CacheDirectiveInfo addCacheDirective(
      final FSNamesystem fsn, final FSDirectory fsd, final CacheManager cacheManager,
      final CacheDirectiveInfo directive, final EnumSet<CacheFlag> flags)
      throws IOException {

    cacheManager.validatePoolName(directive);
    final String path = cacheManager.validatePath(directive);
    final long id = cacheManager.getNextDirectiveId();
    HopsTransactionalRequestHandler addDirectiveHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.ADD_CACHE_DIRECTIVE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
        }
        locks.add(lf.getCachePoolLock(directive.getPool()));
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, path)
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());

        locks.add(il).
            add(lf.getCacheDirectiveLock(id)).
            add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        final RetryCacheDistributed.CacheEntryWithPayload cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.
            getRetryCache(), null);

        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return PBHelper.bytesToLong(cacheEntry.getPayload());
        }
        boolean success = false;
        Long result = null;
        try {
          final FSPermissionChecker pc = getFsPermissionChecker(fsn);

          if (directive.getId() != null) {
            throw new IOException("addDirective: you cannot specify an ID " + "for this operation.");
          }
          CacheDirectiveInfo effectiveDirective = cacheManager.addDirective(directive, pc, flags, id);
          result = effectiveDirective.getId();
          success = true;
          return effectiveDirective;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success, PBHelper.longToBytes(result));
        }
      }
    };
    return (CacheDirectiveInfo) addDirectiveHandler.handle();
  }

  static void modifyCacheDirective(
      final FSNamesystem fsn, final FSDirectory fsd, final CacheManager cacheManager, final CacheDirectiveInfo directive,
      final EnumSet<CacheFlag> flags) throws IOException {

    new HopsTransactionalRequestHandler(HDFSOperationType.MODIFY_CACHE_DIRECTIVE) {
      String path;
      List<String> pools = new ArrayList<>(2);

      @Override
      public void setUp() throws IOException {
        CacheDirectiveDataAccess da = (CacheDirectiveDataAccess) HdfsStorageFactory
            .getDataAccess(CacheDirectiveDataAccess.class);
        CacheDirective originalDirective = (CacheDirective) da.find(directive.getId());
        if (directive.getPath() != null) {
          path = directive.getPath().toString();
        } else if (originalDirective != null) {
          path = originalDirective.getPath();
        }
        if (directive.getPool() != null) {
          pools.add(directive.getPool());
        }
        if (originalDirective != null) {
          pools.add(originalDirective.getPoolName());
        }

      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        locks.add(lf.getCacheDirectiveLock(directive.getId())).
            add(lf.getCachePoolsLock(pools));
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, path)
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes())
            .skipReadingQuotaAttr(!fsd.isQuotaEnabled());

        locks.add(il).
            add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        boolean success = false;
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null;
        }

        final FSPermissionChecker pc = getFsPermissionChecker(fsn);
        try {
          cacheManager.modifyDirective(directive, pc, flags);
          success = true;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle();
  }

  static void removeCacheDirective(
      final FSNamesystem fsn, final CacheManager cacheManager, final long id)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_CACHE_DIRECTIVE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        locks.add(lf.getCacheDirectiveLock(id)).
            add(lf.getCachePoolLock(LockType.WRITE));
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null;
        }
        boolean success = false;
        try {

          final FSPermissionChecker pc = getFsPermissionChecker(fsn);

          cacheManager.removeDirective(id, pc);
          success = true;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle();

  }

  static BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      FSNamesystem fsn, CacheManager cacheManager,
      long startId, CacheDirectiveInfo filter) throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCacheDirectives(startId, filter, pc);
  }

  static CachePoolInfo addCachePool(
      final FSNamesystem fsn, final CacheManager cacheManager, final CachePoolInfo req)
      throws IOException {
    CachePoolInfo.validate(req);
    final String poolName = req.getPoolName();
    return (CachePoolInfo) new HopsTransactionalRequestHandler(HDFSOperationType.ADD_CACHE_POOL) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        locks.add(lf.getCachePoolLock(poolName));
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {
          final FSPermissionChecker pc = getFsPermissionChecker(fsn);

          if (pc != null) {
            pc.checkSuperuserPrivilege();
          }
          CachePoolInfo info = cacheManager.addCachePool(req);
          return info;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
      }
    }.handle();
  }

  static void modifyCachePool(
      final FSNamesystem fsn, final CacheManager cacheManager, final CachePoolInfo req) throws IOException {
    CachePoolInfo.validate(req);
    final String poolName = req.getPoolName();
    new HopsTransactionalRequestHandler(HDFSOperationType.MODIFY_CACHE_POOL) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        locks.add(lf.getCachePoolLock(poolName));
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {
          final FSPermissionChecker pc = getFsPermissionChecker(fsn);
          if (pc != null) {
            pc.checkSuperuserPrivilege();
          }
          cacheManager.modifyCachePool(req);
          success = true;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle();

  }

  static void removeCachePool(
      final FSNamesystem fsn, final CacheManager cacheManager, final String cachePoolName) throws IOException {
    CachePoolInfo.validateName(cachePoolName);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_CACHE_POOL) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        if (fsn.isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        locks.add(lf.getCachePoolLock(cachePoolName)).
            add(lf.getCacheDirectiveLock(cachePoolName));
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsn.getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {
          final FSPermissionChecker pc = getFsPermissionChecker(fsn);
          if (pc != null) {
            pc.checkSuperuserPrivilege();
          }
          cacheManager.removeCachePool(cachePoolName);
        } finally {
           RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle();
  }

  static BatchedListEntries<CachePoolEntry> listCachePools(
      FSNamesystem fsn, CacheManager cacheManager, String prevKey)
      throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCachePools(pc, prevKey);
  }

  private static FSPermissionChecker getFsPermissionChecker(FSNamesystem fsn)
      throws AccessControlException {
    return fsn.isPermissionEnabled() ? fsn.getPermissionChecker() : null;
  }
}
