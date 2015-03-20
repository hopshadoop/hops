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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Time;

import static org.junit.Assert.assertEquals;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestHeartbeatHandling {
  /**
   * Test if
   * {@link FSNamesystem#handleHeartbeat}
   * can pick up replication and/or invalidate requests and observes the max
   * limit
   */
  @Test
  public void testHeartbeat() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final HeartbeatManager hm = namesystem.getBlockManager().getDatanodeManager().getHeartbeatManager();
      final String poolId = namesystem.getBlockPoolId();
      final DatanodeRegistration nodeReg = DataNodeTestUtils.getDNRegistrationForBP(cluster.getDataNodes().get(0), poolId);
      final DatanodeDescriptor dd = NameNodeAdapter.getDatanode(namesystem, nodeReg);
      final String storageID = DatanodeStorage.generateUuid();
      dd.updateStorage(new DatanodeStorage(storageID));

      final int REMAINING_BLOCKS = 1;
      final int MAX_REPLICATE_LIMIT =
          conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
              2);
      final int MAX_INVALIDATE_LIMIT =
          DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT;
      final int MAX_INVALIDATE_BLOCKS =
          2 * MAX_INVALIDATE_LIMIT + REMAINING_BLOCKS;
      final int MAX_REPLICATE_BLOCKS =
          2 * MAX_REPLICATE_LIMIT + REMAINING_BLOCKS;
      final DatanodeStorageInfo[] ONE_TARGET = {dd.getStorageInfo(storageID)};

      synchronized (hm) {
        for (int i = 0; i < MAX_REPLICATE_BLOCKS; i++) {
          dd.addBlockToBeReplicated(
              new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP), ONE_TARGET);
        }
        DatanodeCommand[] cmds =
            NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
                .getCommands();
        assertEquals(1, cmds.length);
        assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
        assertEquals(MAX_REPLICATE_LIMIT,
            ((BlockCommand) cmds[0]).getBlocks().length);

        ArrayList<Block> blockList =
            new ArrayList<>(MAX_INVALIDATE_BLOCKS);
        for (int i = 0; i < MAX_INVALIDATE_BLOCKS; i++) {
          blockList.add(new Block(i, 0, GenerationStamp.LAST_RESERVED_STAMP));
        }
        dd.addBlocksToBeInvalidated(blockList);
        cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
            .getCommands();
        assertEquals(2, cmds.length);
        assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
        assertEquals(MAX_REPLICATE_LIMIT,
            ((BlockCommand) cmds[0]).getBlocks().length);
        assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
        assertEquals(MAX_INVALIDATE_LIMIT,
            ((BlockCommand) cmds[1]).getBlocks().length);

        cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
            .getCommands();
        assertEquals(2, cmds.length);
        assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
        assertEquals(REMAINING_BLOCKS,
            ((BlockCommand) cmds[0]).getBlocks().length);
        assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[1].getAction());
        assertEquals(MAX_INVALIDATE_LIMIT,
            ((BlockCommand) cmds[1]).getBlocks().length);

        cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
            .getCommands();
        assertEquals(1, cmds.length);
        assertEquals(DatanodeProtocol.DNA_INVALIDATE, cmds[0].getAction());
        assertEquals(REMAINING_BLOCKS,
            ((BlockCommand) cmds[0]).getBlocks().length);

        cmds = NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
            .getCommands();
        assertEquals(0, cmds.length);
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test if
   * {@link FSNamesystem#handleHeartbeat}
   * correctly selects data node targets for block recovery.
   */
  @Test
  public void testHeartbeatBlockRecovery() throws Exception {
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

      try {
        synchronized (hm) {
          NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem);
          NameNodeAdapter.sendHeartBeat(nodeReg2, dd2, namesystem);
          NameNodeAdapter.sendHeartBeat(nodeReg3, dd3, namesystem);

          // Test with all alive nodes.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, 0);
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, 0);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, 0);
          final DatanodeStorageInfo[] storages = {
            dd1.getStorageInfos()[0],
            dd2.getStorageInfos()[0],
            dd3.getStorageInfos()[0]};
          BlockInfoContiguousUnderConstruction blockInfo = createBlockInfoUnderConstruction(storages);
          dd1.addBlockToBeRecovered(blockInfo);
          DatanodeCommand[] cmds = NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          BlockRecoveryCommand recoveryCommand = (BlockRecoveryCommand) cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          DatanodeInfo[] recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          assertEquals(3, recoveringNodes.length);
          List<DatanodeInfo> dds = new ArrayList<>();
          dds.add(dd1);
          dds.add(dd2);
          dds.add(dd3);
          for(DatanodeInfo di: recoveringNodes){
            dds.remove(di);
          }
          assertEquals(dds.size(), 0);

          // Test with one stale node.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, 0);
          // More than the default stale interval of 30 seconds.
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, -40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, 0);
          blockInfo = createBlockInfoUnderConstruction(storages);
          dd1.addBlockToBeRecovered(blockInfo);
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          recoveryCommand = (BlockRecoveryCommand) cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          assertEquals(2, recoveringNodes.length);
          // dd2 is skipped.
          dds = new ArrayList<>();
          dds.add(dd1);
          dds.add(dd3);
          for(DatanodeInfo di: recoveringNodes){
            dds.remove(di);
          }
          assertEquals(dds.size(), 0);

          // Test with all stale node.
          DFSTestUtil.resetLastUpdatesWithOffset(dd1, - 60 * 1000);
          // More than the default stale interval of 30 seconds.
          DFSTestUtil.resetLastUpdatesWithOffset(dd2, - 40 * 1000);
          DFSTestUtil.resetLastUpdatesWithOffset(dd3, - 80 * 1000);
          blockInfo = createBlockInfoUnderConstruction(storages);
          dd1.addBlockToBeRecovered(blockInfo);
          cmds = NameNodeAdapter.sendHeartBeat(nodeReg1, dd1, namesystem).getCommands();
          assertEquals(1, cmds.length);
          assertEquals(DatanodeProtocol.DNA_RECOVERBLOCK, cmds[0].getAction());
          recoveryCommand = (BlockRecoveryCommand) cmds[0];
          assertEquals(1, recoveryCommand.getRecoveringBlocks().size());
          recoveringNodes = recoveryCommand.getRecoveringBlocks()
              .toArray(new BlockRecoveryCommand.RecoveringBlock[0])[0].getLocations();
          // Only dd1 is included since it heart beated and hence its not stale
          // when the list of recovery blocks is constructed.
          assertEquals(3, recoveringNodes.length);
          dds = new ArrayList<>();
          dds.add(dd1);
          dds.add(dd2);
          dds.add(dd3);
          for(DatanodeInfo di: recoveringNodes){
            dds.remove(di);
          }
          assertEquals(dds.size(), 0);
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
  
  /*
   * test that if a data node dies while the NN is not running the replica stored on this dn will eventually be removed
   * from the database
   */
  @Test(timeout = 120000)
  public void testDeadReplicasCleanup() throws IOException, InterruptedException, TimeoutException {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 10 * 1000); // 5 minutes
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path file1 = new Path("/test1");
      DFSTestUtil.createFile(fs, file1, 6 * 1024, (short) 3, 1L);
      //wait for the file to be replicated on all of the datanodes.
      DFSTestUtil.waitReplication(fs, file1, (short) 3);

      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor dnDescriptor = cluster.getNameNode().getNamesystem().getBlockManager().getDatanodeManager().
          getDatanode(dn.getDatanodeId());

      cluster.shutdownNameNodes();
      cluster.stopDataNode(dn.getDisplayName());

      assert getReplicasOnStorage(dnDescriptor.getStorageInfos()[0].getSid()) + getReplicasOnStorage(dnDescriptor.
          getStorageInfos()[1].getSid()) > 0;
      cluster.restartNameNode(true, false);
      assert getReplicasOnStorage(dnDescriptor.getStorageInfos()[0].getSid()) + getReplicasOnStorage(dnDescriptor.
          getStorageInfos()[1].getSid()) > 0;
      long start = Time.now();
      //wait for the namenode to cleanup replicas from the dead datanode
      while (start > Time.now() - 3 * cluster.getNameNode().getNamesystem().getBlockManager().getDatanodeManager().
          getHeartbeatExpireInterval()) {
        if (getReplicasOnStorage(dnDescriptor.getStorageInfos()[0].getSid()) + getReplicasOnStorage(dnDescriptor.
            getStorageInfos()[1].getSid()) <= 0) {
          break;
        }
        Thread.sleep(1000);
      }
      assert getReplicasOnStorage(dnDescriptor.getStorageInfos()[0].getSid()) + getReplicasOnStorage(dnDescriptor.
          getStorageInfos()[1].getSid()) <= 0;

    } finally {
      cluster.shutdown();
    }
  }

  private int getReplicasOnStorage(final int sid) throws IOException {
    return (int) new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory.getDataAccess(ReplicaDataAccess.class);
        return da.countAllReplicasForStorageId(sid);
      }
    }.handle();
  }
}
