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
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class provides tests for BlockInfo class, which is used in BlocksMap.
 * The test covers BlockList.listMoveToHead, used for faster block report
 * processing in DatanodeDescriptor.reportDiff.
 */
public class TestBlockInfo {

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockInfo");

  @Before
  public void setup() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    HashBuckets.initialize(1);
  }

  @Test
  public void testAddStorage() throws Exception {
    Block block = new Block(0);
    BlockInfo blockInfo = new BlockInfo(block, 1);

    final DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo("storageID", "127.0.0.1");
    storage.setSid(0);

    boolean added = addStorage(blockInfo, storage);

    Assert.assertTrue(added);
    Assert.assertEquals(storage.getSid(), getStorageId(blockInfo));
  }

  private boolean addStorage(final BlockInfo blockInfo, final DatanodeStorageInfo storage) throws IOException {
    return (boolean) new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getIndividualBlockLock(blockInfo.getBlockId(), new INodeIdentifier(blockInfo.getInodeId())))
            .add(lf.getBlockRelated(LockFactory.BLK.RE));
      }

      @Override
      public Object performTask() throws IOException {

        return blockInfo.addStorage(storage);
      }
    }.handle();
  }

  private int getStorageId(final BlockInfo blockInfo) throws IOException {
    return (int) new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory.getDataAccess(ReplicaDataAccess.class);
        List<Replica> replicas = da.findReplicasById(blockInfo.getBlockId(), blockInfo.getInodeId());
        return replicas.get(0).getStorageId();
      }
    }.handle();
  }

  @Test
  public void testReplaceStorage() throws Exception {

    // Create two dummy storages.
    final DatanodeStorageInfo storage1 = DFSTestUtil.createDatanodeStorageInfo("storageID1", "127.0.0.1");
    storage1.setSid(1);
    storage1.getDatanodeDescriptor().getStorageInfo(storage1.getStorageID()).setSid(1);
    final DatanodeStorageInfo storage2 = new DatanodeStorageInfo(storage1.getDatanodeDescriptor(), new DatanodeStorage(
        "storageID2"));
    storage2.setSid(2);
    final int NUM_BLOCKS = 10;
    BlockInfo[] blockInfos = new BlockInfo[NUM_BLOCKS];

    // Create a few dummy blocks and add them to the first storage.
    for (int i = 0; i < NUM_BLOCKS; ++i) {
      Block block = new Block(i);
      blockInfos[i] = new BlockInfo(block, i);
      addBlockInfo(blockInfos[i]);
      addBlock(storage1,blockInfos[i]);
    }

    // Try to move one of the blocks to a different storage.
    boolean added = addBlock(storage2,blockInfos[NUM_BLOCKS / 2]);
    Assert.assertThat(added, is(false));
    Assert.assertThat(getStorageId(blockInfos[NUM_BLOCKS / 2]), is(storage2.getSid()));
  }

  private boolean addBlock(final DatanodeStorageInfo storage, final BlockInfo blk) throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(blk.getBlockId(), new INodeIdentifier(blk.getInodeId())))
            .add(lf.getBlockRelated(LockFactory.BLK.RE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return storage.addBlock(blk);
      }

    }.handle();
  }
  
  private void addBlockInfo(final BlockInfo blk) throws IOException{
    new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory.getDataAccess(BlockInfoDataAccess.class);
        List<BlockInfo> added = new ArrayList<>();
        added.add(blk);
        da.prepare(null, null, added);
        return null;
      }
    }.handle();
    
  }
}
