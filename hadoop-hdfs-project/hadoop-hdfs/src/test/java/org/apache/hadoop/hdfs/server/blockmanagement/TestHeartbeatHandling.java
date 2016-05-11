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

import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static io.hops.transaction.lock.LockFactory.getInstance;
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
              new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP), ONE_TARGET);
        }
        DatanodeCommand[] cmds =
            NameNodeAdapter.sendHeartBeat(nodeReg, dd, namesystem)
                .getCommands();
        assertEquals(1, cmds.length);
        assertEquals(DatanodeProtocol.DNA_TRANSFER, cmds[0].getAction());
        assertEquals(MAX_REPLICATE_LIMIT,
            ((BlockCommand) cmds[0]).getBlocks().length);

        ArrayList<Block> blockList =
            new ArrayList<Block>(MAX_INVALIDATE_BLOCKS);
        for (int i = 0; i < MAX_INVALIDATE_BLOCKS; i++) {
          blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
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
}
