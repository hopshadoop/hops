/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.junit.Test;

/**
 * This class provides tests for BlockInfoUnderConstruction class
 */
public class TestBlockInfoUnderConstruction {

  @Test
  public void testInitializeBlockRecovery() throws Exception {

    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final HeartbeatManager hm = namesystem.getBlockManager().getDatanodeManager().getHeartbeatManager();
      final String poolId = namesystem.getBlockPoolId();
      final DatanodeRegistration nodeReg1 = DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(0),
          poolId);
      final DatanodeDescriptor dd1 = NameNodeAdapter.getDatanode(namesystem, nodeReg1);
      final DatanodeRegistration nodeReg2 = DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(1),
          poolId);
      final DatanodeDescriptor dd2 = NameNodeAdapter.getDatanode(namesystem, nodeReg2);
      final DatanodeRegistration nodeReg3 = DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(2),
          poolId);
      final DatanodeDescriptor dd3 = NameNodeAdapter.getDatanode(namesystem, nodeReg3);
      DatanodeManager dn = namesystem.getBlockManager().getDatanodeManager();
      try {
        synchronized (hm) {

          DatanodeStorageInfo s1 = dd1.getStorageInfos()[0];
          DatanodeStorageInfo s2 = dd2.getStorageInfos()[0];
          DatanodeStorageInfo s3 = dd3.getStorageInfos()[0];

          dd1.isAlive = dd2.isAlive = dd3.isAlive = true;
          BlockInfoContiguousUnderConstruction blockInfo = createBlockInfoUnderConstruction(new DatanodeStorageInfo[]{s1, s2, s3});
          // Recovery attempt #1.
          long currentTime = System.currentTimeMillis();
          dd1.setLastUpdate(currentTime - 3 * 1000);
          dd2.setLastUpdate(currentTime - 1 * 1000);
          dd3.setLastUpdate(currentTime - 2 * 1000);
          initializeBlockRecovery(blockInfo, 1, dn);
          BlockInfoContiguousUnderConstruction[] blockInfoRecovery = dd2.getLeaseRecoveryCommand(1);
          assertEquals(blockInfoRecovery[0], blockInfo);

          // Recovery attempt #2.
          currentTime = System.currentTimeMillis();
          dd1.setLastUpdate(currentTime - 2 * 1000);
          dd2.setLastUpdate(currentTime - 1 * 1000);
          dd3.setLastUpdate(currentTime - 3 * 1000);
          initializeBlockRecovery(blockInfo, 2, dn);
          blockInfoRecovery = dd1.getLeaseRecoveryCommand(1);
          assertEquals(blockInfoRecovery[0], blockInfo);

          // Recovery attempt #3.
          currentTime = System.currentTimeMillis();
          dd1.setLastUpdate(currentTime - 2 * 1000);
          dd2.setLastUpdate(currentTime - 1 * 1000);
          dd3.setLastUpdate(currentTime - 3 * 1000);
          currentTime = System.currentTimeMillis();
          initializeBlockRecovery(blockInfo, 3, dn);
          blockInfoRecovery = dd3.getLeaseRecoveryCommand(1);
          assertEquals(blockInfoRecovery[0], blockInfo);

          // Recovery attempt #4.
          // Reset everything. And again pick DN with most recent heart beat.
          currentTime = System.currentTimeMillis();
          dd1.setLastUpdate(currentTime - 2 * 1000);
          dd2.setLastUpdate(currentTime - 1 * 1000);
          dd3.setLastUpdate(currentTime);
          currentTime = System.currentTimeMillis();
          initializeBlockRecovery(blockInfo, 3, dn);
          blockInfoRecovery = dd3.getLeaseRecoveryCommand(1);
          assertEquals(blockInfoRecovery[0], blockInfo);
        }
      } finally {
      }
    } finally {
      cluster.shutdown();
    }
  }

  private BlockInfoContiguousUnderConstruction createBlockInfoUnderConstruction(final DatanodeStorageInfo[] storages) throws
      IOException {
    return (BlockInfoContiguousUnderConstruction) new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier = new INodeIdentifier(3L);

      @Override
      public void setUp() throws StorageException {
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, inodeIdentifier, true))
            .add(
                lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
            .add(lf.getBlockLock(10, inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.UC,
                LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        Block block = new Block(10, 0, GenerationStamp.LAST_RESERVED_STAMP);
        EntityManager.add(new BlockInfoContiguous(block,
            inodeIdentifier != null ? inodeIdentifier.getInodeId() : BlockInfoContiguous.NON_EXISTING_ID));
        BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
            block, 3,
            HdfsServerConstants.BlockUCState.UNDER_RECOVERY, storages);
        return blockInfo;
      }

    }.handle();
  }

  private void initializeBlockRecovery(final BlockInfoContiguousUnderConstruction blockInfo, final long recoveryId,
      final DatanodeManager dn) throws
      IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier = new INodeIdentifier(3L);

      @Override
      public void setUp() throws StorageException {
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, inodeIdentifier, true))
            .add(
                lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
            .add(lf.getBlockLock(10, inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.UC,
                LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        blockInfo.initializeBlockRecovery(recoveryId, dn);
        return null;
      }

    }.handle();
  }
}
