/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.transaction.lock;

import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.ipc.RetryCache;

public class LockFactory {

  private final static LockFactory instance = new LockFactory();
  
  
  public static enum BLK {
    /**
     * Replica
     */
    RE,
    /**
     * CorruptReplica
     */
    CR,
    /**
     * ExcessReplica
     */
    ER,
    /**
     * UnderReplicated
     */
    UR,
    /**
     * ReplicaUnderConstruction
     */
    UC,
    /**
     * InvalidatedBlock
     */
    IV,
    /**
     * PendingBlock
     */
    PE
  }

  private LockFactory() {

  }

  public static LockFactory getInstance() {
    return instance;
  }

  public Lock getBlockChecksumLock(String target, int blockIndex) {
    return new BlockChecksumLock(target, blockIndex);
  }

  public Lock getBlockLock() {
    return new BlockLock();
  }
  
  public Lock getBlockLock(long blockId, INodeIdentifier inode) {
    return new BlockLock(blockId, inode);
  }

  public Lock getReplicaLock() {
    return new BlockRelatedLock(Lock.Type.Replica);
  }

  public Lock getCorruptReplicaLock() {
    return new BlockRelatedLock(Lock.Type.CorruptReplica);
  }

  public Lock getExcessReplicaLock() {
    return new BlockRelatedLock(Lock.Type.ExcessReplica);
  }

  public Lock getReplicatUnderConstructionLock() {
    return new BlockRelatedLock(Lock.Type.ReplicaUnderConstruction);
  }

  public Lock getInvalidatedBlockLock() {
    return new BlockRelatedLock(Lock.Type.InvalidatedBlock);
  }

  public Lock getUnderReplicatedBlockLock() {
    return new BlockRelatedLock(Lock.Type.UnderReplicatedBlock);
  }

  public Lock getPendingBlockLock() {
    return new BlockRelatedLock(Lock.Type.PendingBlock);
  }

  public Lock getSqlBatchedBlocksLock() {
    return new SqlBatchedBlocksLock();
  }

  public Lock getSqlBatchedReplicasLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.Replica);
  }

  public Lock getSqlBatchedCorruptReplicasLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.CorruptReplica);
  }

  public Lock getSqlBatchedExcessReplicasLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.ExcessReplica);
  }

  public Lock getSqlBatchedReplicasUnderConstructionLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.ReplicaUnderConstruction);
  }

  public Lock getSqlBatchedInvalidatedBlocksLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.InvalidatedBlock);
  }

  public Lock getSqlBatchedUnderReplicatedBlocksLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.UnderReplicatedBlock);
  }

  public Lock getSqlBatchedPendingBlocksLock() {
    return new SqlBatchedBlocksRelatedLock(Lock.Type.PendingBlock);
  }

  public Lock getIndividualBlockLock(long blockId, INodeIdentifier inode) {
    return new IndividualBlockLock(blockId, inode);
  }

  public Lock getBatchedINodesLock(List<INodeIdentifier> inodeIdentifiers) {
    return new BatchedINodeLock(inodeIdentifiers);
  }

  /**
   * @param lockType the lock to acquire
   * @param inodeIdentifier the id of the inode
   * @param readUpPathInodes if true, you'll get locks for the parent inodes too
   */
  public Lock getIndividualINodeLock(
      TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier, boolean readUpPathInodes) {
    return new IndividualINodeLock(lockType, inodeIdentifier, readUpPathInodes);
  }

  public Lock getIndividualINodeLock(
      TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier) {
    return new IndividualINodeLock(lockType, inodeIdentifier);
  }

  public Lock getINodeLock(boolean skipReadingQuotaAttr, NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      boolean ignoreLocalSubtreeLocks, String... paths) {
    return new INodeLock(lockType, resolveType, resolveLink,
        ignoreLocalSubtreeLocks, skipReadingQuotaAttr, nameNode.getId(),
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }

  public Lock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      boolean ignoreLocalSubtreeLocks, String... paths) {
    return new INodeLock(lockType, resolveType, resolveLink,
        ignoreLocalSubtreeLocks, false, nameNode.getId(),
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }

  public Lock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      String... paths) {
    return new INodeLock(false, lockType, resolveType, resolveLink,
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }

  public Lock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String... paths) {
    return new INodeLock(lockType, resolveType,
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }
  
  public Lock getINodeLock(boolean skipReadingQuotaAttr, NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, 
      String... paths) {
   return new INodeLock(lockType, resolveType, true,
        false, skipReadingQuotaAttr, nameNode.getId(),
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }
  
  public Lock getINodeLock(boolean skipReadingQuotaAttr, NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,  boolean resolveLink,
      String... paths) {
   return new INodeLock(lockType, resolveType, resolveLink,
        false, skipReadingQuotaAttr, nameNode.getId(),
        nameNode.getActiveNameNodes().getActiveNodes(), paths);
  }

  public Lock getRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, String src, String dst) {
    return new RenameINodeLock(lockType, resolveType, ignoreLocalSubtreeLocks,
        nameNode.getId(), nameNode.getActiveNameNodes().getActiveNodes(), src,
        dst);
  }

  public Lock getRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String src,
      String dst) {
    return new RenameINodeLock(lockType, resolveType,
        nameNode.getActiveNameNodes().getActiveNodes(), src, dst);
  }

  public Lock getLegacyRenameINodeLock(boolean skipReadingQuotaAttr,NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, String src, String dst) {
    return new RenameINodeLock(skipReadingQuotaAttr,lockType, resolveType, ignoreLocalSubtreeLocks,
        nameNode.getId(), nameNode.getActiveNameNodes().getActiveNodes(), src,
        dst, true);
  }

  public Lock getLeaseLock(TransactionLockTypes.LockType lockType,
      String leaseHolder) {
    return new LeaseLock(lockType, leaseHolder);
  }

  public Lock getLeaseLock(TransactionLockTypes.LockType lockType) {
    return new LeaseLock(lockType);
  }

  public Lock getLeasePathLock(TransactionLockTypes.LockType lockType,
      int expectedCount) {
    return new LeasePathLock(lockType, expectedCount);
  }

  public Lock getLeasePathLock(TransactionLockTypes.LockType lockType) {
    return new LeasePathLock(lockType);
  }
  
  public Lock getLeasePathLock(TransactionLockTypes.LockType lockType,
          String src) {
    return new LeasePathLock(lockType, src);
  }

  public Lock getNameNodeLeaseLock(TransactionLockTypes.LockType lockType) {
    return new NameNodeLeaseLock(lockType);
  }

  public Lock getQuotaUpdateLock(boolean includeChildren, String... targets) {
    return new QuotaUpdateLock(includeChildren, targets);
  }

  public Lock getQuotaUpdateLock(String... targets) {
    return new QuotaUpdateLock(targets);
  }

  public Lock getVariableLock(Variable.Finder[] finders,
      TransactionLockTypes.LockType[] lockTypes) {
    assert finders.length == lockTypes.length;
    VariablesLock lock = new VariablesLock();
    for (int i = 0; i < finders.length; i++) {
      lock.addVariable(finders[i], lockTypes[i]);
    }
    return lock;
  }

  public Lock getVariableLock(Variable.Finder finder,
      TransactionLockTypes.LockType lockType) {
    VariablesLock lock = new VariablesLock();
    lock.addVariable(finder, lockType);
    return lock;
  }

  public List<Lock> getBlockReportingLocks(long[] blockIds, int[] inodeIds, long[] unresolvedBlks, int storageId) {
    ArrayList<Lock> list = new ArrayList(3);
    list.add(new BatchedBlockLock(blockIds,inodeIds, unresolvedBlks));
    //list.add(new BatchedBlocksRelatedLock.BatchedInvalidatedBlocksLock(storageId));
    return list;
  }

  public Lock getEncodingStatusLock(TransactionLockTypes.LockType lockType,
      String... targets) {
    return new BaseEncodingStatusLock.EncodingStatusLock(lockType, targets);
  }

  public Lock getEncodingStatusLock(boolean includeChildren, TransactionLockTypes.LockType lockType,
      String... targets) {
    return new BaseEncodingStatusLock.EncodingStatusLock(includeChildren, lockType, targets);
  }
  public Lock getIndivdualEncodingStatusLock(
      TransactionLockTypes.LockType lockType, int inodeId) {
    return new BaseEncodingStatusLock.IndividualEncodingStatusLock(lockType,
        inodeId);
  }
  
  public Lock getBatchedEncodingStatusLock(
      TransactionLockTypes.LockType lockType, List<INodeIdentifier> inodeIds) {
    return new BaseEncodingStatusLock.BatchedEncodingStatusLock(lockType,inodeIds);
  }
  
  public Lock getSubTreeOpsLock(TransactionLockTypes.LockType lockType, 
          String pathPrefix) {
    return new SubTreeOpLock(lockType, pathPrefix);
  }
  
  public Lock getIndividualHashBucketLock(int storageId, int bucketId) {
    return new IndividualHashBucketLock(storageId, bucketId);
  }
  
  public Lock getLastBlockHashBucketsLock(){
    return new LastBlockReplicasHashBucketLock();
  }
  
  public Lock getRetryCacheEntryLock(byte[] clientId, int callId){
    return new RetryCacheEntryLock(clientId, callId);
  }
  
  public Lock getRetryCacheEntryLock(List<RetryCache.CacheEntry> entries){
    return new RetryCacheEntryLock(entries);
  }
  
  public Collection<Lock> getBlockRelated(BLK... relatedBlks) {
    ArrayList<Lock> list = new ArrayList();
    for (BLK b : relatedBlks) {
      switch (b) {
        case RE:
          list.add(getReplicaLock());
          break;
        case CR:
          list.add(getCorruptReplicaLock());
          break;
        case IV:
          list.add(getInvalidatedBlockLock());
          break;
        case PE:
          list.add(getPendingBlockLock());
          break;
        case UC:
          list.add(getReplicatUnderConstructionLock());
          break;
        case UR:
          list.add(getUnderReplicatedBlockLock());
          break;
        case ER:
          list.add(getExcessReplicaLock());
          break;
      }
    }
    return list;
  }
  
  public Collection<Lock> getSqlBatchedBlocksRelated(BLK... relatedBlks) {
    ArrayList<Lock> list = new ArrayList();
    for (BLK b : relatedBlks) {
      switch (b) {
        case RE:
          list.add(getSqlBatchedReplicasLock());
          break;
        case CR:
          list.add(getSqlBatchedCorruptReplicasLock());
          break;
        case IV:
          list.add(getSqlBatchedInvalidatedBlocksLock());
          break;
        case PE:
          list.add(getSqlBatchedInvalidatedBlocksLock());
          break;
        case UC:
          list.add(getSqlBatchedReplicasUnderConstructionLock());
          break;
        case UR:
          list.add(getUnderReplicatedBlockLock());
          break;
        case ER:
          list.add(getSqlBatchedExcessReplicasLock());
          break;
      }
    }
    return list;
  }

  public Lock getLastTwoBlocksLock(String src){
    return new LastTwoBlocksLock(src);
  }

  public Lock getAcesLock(){
    return new AcesLock();
  }
  
  public void setConfiguration(Configuration conf) {
    BaseINodeLock.enableSetPartitionKey(
        conf.getBoolean(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED,
            DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED_DEFAULT));
    BaseINodeLock.enableSetRandomPartitionKey(conf.getBoolean(DFSConfigKeys
        .DFS_SET_RANDOM_PARTITION_KEY_ENABLED, DFSConfigKeys
        .DFS_SET_RANDOM_PARTITION_KEY_ENABLED_DEFAULT));
    BaseINodeLock.setDefaultLockType(getPrecedingPathLockType(conf));
  }
  
  private TransactionLockTypes.INodeLockType getPrecedingPathLockType(
      Configuration conf) {
    String val = conf.get(DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE,
        DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE_DEFAULT);
    if (val.compareToIgnoreCase("READ") == 0) {
      return TransactionLockTypes.INodeLockType.READ;
    } else if (val.compareToIgnoreCase("READ_COMMITTED") == 0) {
      return TransactionLockTypes.INodeLockType.READ_COMMITTED;
    } else {
      throw new IllegalStateException(
          "Critical Parameter is not defined. Set " +
              DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE);
    }
  }
}
