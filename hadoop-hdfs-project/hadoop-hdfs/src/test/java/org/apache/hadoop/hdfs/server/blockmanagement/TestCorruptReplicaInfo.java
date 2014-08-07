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

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.hops.transaction.lock.LockFactory.BLK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.junit.Test;


/**
 * This test makes sure that
 * CorruptReplicasMap::numBlocksWithCorruptReplicas and
 * CorruptReplicasMap::getCorruptReplicaBlockIds
 * return the correct values
 */
public class TestCorruptReplicaInfo {
  
  private static final Log LOG =
      LogFactory.getLog(TestCorruptReplicaInfo.class);
  
  private Map<Integer, BlockInfo> block_map = new HashMap<>();
  private BlocksMap blocksMap = new BlocksMap(null);

  // Allow easy block creation by block id
  // Return existing block if one with same block id already exists
  private BlockInfo getBlock(Integer block_id) {
    if (!block_map.containsKey(block_id)) {
      block_map
          .put(block_id, new BlockInfo(new Block(block_id, 0, 0), block_id));
    }
    
    return block_map.get(block_id);
  }
  
  @Test
  public void testCorruptReplicaInfo()
      throws IOException, InterruptedException, StorageException {
    
    HdfsStorageFactory.setConfiguration(new HdfsConfiguration());
    HdfsStorageFactory.formatStorage();

    CorruptReplicasMap crm = new CorruptReplicasMap(null);

    // Make sure initial values are returned correctly
    assertEquals("Number of corrupt blocks must initially be 0", 0, crm.size());
    assertNull("Param n cannot be less than 0",
        crm.getCorruptReplicaBlockIds(-1, null));
    assertNull("Param n cannot be greater than 100",
        crm.getCorruptReplicaBlockIds(101, null));
    long[] l = crm.getCorruptReplicaBlockIds(0, null);
    assertNotNull("n = 0 must return non-null", l);
    assertEquals("n = 0 must return an empty list", 0, l.length);

    // create a list of block_ids. A list is used to allow easy validation of the
    // output of getCorruptReplicaBlockIds
    int NUM_BLOCK_IDS = 140;
    List<Integer> block_ids = new LinkedList<>();
    for (int i = 0; i < NUM_BLOCK_IDS; i++) {
      block_ids.add(i);
    }

    DatanodeDescriptor dn1 = DFSTestUtil.getLocalDatanodeDescriptor();
    DatanodeStorage ds1 = new DatanodeStorage("storageid_1",
        DatanodeStorage.State.NORMAL, StorageType.DEFAULT);
    DatanodeStorageInfo storage1 = new DatanodeStorageInfo(dn1, ds1);

    DatanodeDescriptor dn2 = DFSTestUtil.getLocalDatanodeDescriptor();
    DatanodeStorage ds2 = new DatanodeStorage("storageid_2",
        DatanodeStorage.State.NORMAL, StorageType.DEFAULT);
    DatanodeStorageInfo storage2 = new DatanodeStorageInfo(dn2, ds2);

    addToCorruptReplicasMap(crm, getBlock(0), storage1);
    assertEquals("Number of corrupt blocks not returning correctly", 1,
        crm.size());
    addToCorruptReplicasMap(crm, getBlock(1), storage1);
    assertEquals("Number of corrupt blocks not returning correctly", 2,
        crm.size());

    addToCorruptReplicasMap(crm, getBlock(1), storage2);
    assertEquals("Number of corrupt blocks not returning correctly", 2,
        crm.size());

    removeFromCorruptReplicasMap(crm, getBlock(1));
    assertEquals("Number of corrupt blocks not returning correctly", 1,
        crm.size());

    removeFromCorruptReplicasMap(crm, getBlock(0));
    assertEquals("Number of corrupt blocks not returning correctly", 0,
        crm.size());

    for (Integer block_id : block_ids) {
      addToCorruptReplicasMap(crm, getBlock(block_id), storage1);
    }

    assertEquals("Number of corrupt blocks not returning correctly",
        NUM_BLOCK_IDS, crm.size());

    assertTrue("First five block ids not returned correctly ", Arrays
            .equals(new long[]{0, 1, 2, 3, 4},
                crm.getCorruptReplicaBlockIds(5, null)));

    LOG.info(crm.getCorruptReplicaBlockIds(10, 7L));
    LOG.info(block_ids.subList(7, 18));

    assertTrue("10 blocks after 7 not returned correctly ", Arrays
            .equals(new long[]{8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
                crm.getCorruptReplicaBlockIds(10, 7L)));

  }
  
  private void addToCorruptReplicasMap(final CorruptReplicasMap crm,
      final BlockInfo blk, final DatanodeStorageInfo storage)
      throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST_CORRUPT_REPLICA_INFO) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.CR));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        blocksMap.addBlockCollection(blk, new INodeFile(blk.getInodeId(),
            new PermissionStatus("n", "n", FsPermission.getDefault()), null, (short)1, 0, 0, 1, (byte) 0));
        crm.addToCorruptReplicasMap(blk, storage, "TEST", Reason.NONE);
        return null;
      }
    }.handle();
  }
  
  private void removeFromCorruptReplicasMap(final CorruptReplicasMap crm,
      final BlockInfo blk) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST_CORRUPT_REPLICA_INFO) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.CR));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        crm.removeFromCorruptReplicasMap(blk);
        return null;
      }
    }.handle();
  }  
}
