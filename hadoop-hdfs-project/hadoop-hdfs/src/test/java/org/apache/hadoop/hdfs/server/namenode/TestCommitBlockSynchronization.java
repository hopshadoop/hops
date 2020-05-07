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

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verify that TestCommitBlockSynchronization is idempotent.
 */
public class TestCommitBlockSynchronization {
  private static final long blockId = 100;
  private static final long length = 200;
  private static final long genStamp = 300;

  private MiniDFSCluster cluster;
  @Before
  public void setup() throws IOException{
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
  }
      
  @After
  public void shutdown(){
    cluster.shutdown();
  }
  
  private FSNamesystem makeNameSystemSpy(Block block, INodeFile file)
      throws IOException {
    Configuration conf = new Configuration();
    DatanodeStorageInfo[] targets = new DatanodeStorageInfo[0];

    FSNamesystem namesystem = new FSNamesystem(conf, cluster.getNameNode());
    namesystem.setImageLoaded(true);

    // set file's parent as root and put the file to inodeMap, so
    // FSNamesystem's isFileDeleted() method will return false on this file
    if (file.getParent() == null) {
      INodeDirectory parent = mock(INodeDirectory.class);
      parent.setLocalName(new byte[0]);
      file.setParent(parent);
    }

    FSNamesystem namesystemSpy = spy(namesystem);
    doReturn(1L).when(file).getId();
    BlockInfoContiguousUnderConstruction blockInfo = createBlockInfoUnderConstruction(targets, block, file);
    
    BlockInfoContiguousUnderConstruction mockBlockInfo = mock(BlockInfoContiguousUnderConstruction.class);
    doReturn(file).when(mockBlockInfo).getBlockCollection();
    doReturn(blockInfo.getGenerationStamp()).when(mockBlockInfo).getGenerationStamp();
    doReturn(blockInfo.getBlockRecoveryId()).when(mockBlockInfo).getBlockRecoveryId();
    doReturn(blockInfo.getBlockId()).when(mockBlockInfo).getBlockId();
    doReturn(blockInfo.getInodeId()).when(mockBlockInfo).getInodeId();
    
    doReturn(true).when(file).removeLastBlock(any(Block.class));
    doReturn(true).when(file).isUnderConstruction();
    
    doReturn(mockBlockInfo).when(namesystemSpy).getStoredBlock(any(Block.class));  
    doReturn(mockBlockInfo).when(file).getLastBlock();
    doReturn("").when(namesystemSpy).closeFileCommitBlocks(
        any(INodeFile.class),
        any(BlockInfoContiguous.class));

    return namesystemSpy;
  }
  
  private BlockInfoContiguousUnderConstruction createBlockInfoUnderConstruction(final DatanodeStorageInfo[] targets,
      final Block block, final INodeFile file) throws IOException {
    return (BlockInfoContiguousUnderConstruction) new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier = new INodeIdentifier(1L);

      @Override
      public void setUp() throws StorageException {
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, inodeIdentifier, true))
            .add(
                lf.getLeaseLockAllPaths(TransactionLockTypes.LockType.WRITE))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))

            .add(lf.getBlockLock(10, inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.UC,
                LockFactory.BLK.UR));
        EntityManager.snapshotMaintenance
                (HdfsTransactionContextMaintenanceCmds.EmptyFile, inodeIdentifier.getInodeId());

      }

      @Override
      public Object performTask() throws IOException {


        BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction(
        block, 1, HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, targets);
        blockInfo.setBlockCollection(file);
        blockInfo.setGenerationStamp(genStamp);
        blockInfo.initializeBlockRecovery(genStamp, cluster.getNamesystem().getBlockManager().getDatanodeManager());
        return blockInfo;
      }

    }.handle();
  }
  
  private INodeFile mockFileUnderConstruction() {
    INodeFile file = mock(INodeFile.class);
    return file;
  }
  
  @Test
  public void testCommitBlockSynchronization() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock(null ,block);
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false,
        false, newTargets, null);

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, false, newTargets, null);

    // Simulate 'completing' the block.
    BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, 1);
    setBlockCollectionAndGenerationStamp(completedBlockInfo, file);
    BlockInfoContiguous mockCompletedBlockInfo = mock(BlockInfoContiguous.class);
    doReturn(file).when(mockCompletedBlockInfo).getBlockCollection();
    doReturn(completedBlockInfo.getGenerationStamp()).when(mockCompletedBlockInfo).getGenerationStamp();
    doReturn(completedBlockInfo.isComplete()).when(mockCompletedBlockInfo).isComplete();
    
    doReturn(mockCompletedBlockInfo).when(namesystemSpy)
        .getStoredBlock(any(Block.class));
    doReturn(mockCompletedBlockInfo).when(file).getLastBlock();

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, false, newTargets, null);
  }

    private BlockInfoContiguousUnderConstruction setBlockCollectionAndGenerationStamp(final BlockInfoContiguous completedBlockInfo,
        final INodeFile file) throws IOException {
    return (BlockInfoContiguousUnderConstruction) new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier = new INodeIdentifier(1L);

      @Override
      public void setUp() throws StorageException {
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, inodeIdentifier, true))
            .add(
                lf.getLeaseLockAllPaths(TransactionLockTypes.LockType.WRITE))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
            .add(lf.getBlockLock(10, inodeIdentifier))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.UC,
                LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        completedBlockInfo.setBlockCollection(file);
        completedBlockInfo.setGenerationStamp(genStamp);

        return null;
      }

    }.handle();
  }
    
  @Test
  public void testCommitBlockSynchronization2() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock(null, block);
    
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false,
        false, newTargets, null);

    // Make sure the call fails if the generation stamp does not match
    // the block recovery ID.
    try {
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp - 1, length, false, false, newTargets, null);
      fail("Failed to get expected IOException on generation stamp/" +
           "recovery ID mismatch");
    } catch (IOException ioe) {
      // Expected exception.
    }
  }

  @Test
  public void testCommitBlockSynchronizationWithDelete() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeDescriptor[] targets = new DatanodeDescriptor[0];
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock(null, block);
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp, length, false,
          true, newTargets, null);

    // Simulate removing the last block from the file.
    doReturn(false).when(file).removeLastBlock(any(Block.class));

    // Repeat the call to make sure it does not throw
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, false, true, newTargets, null);
  }

  @Test
  public void testCommitBlockSynchronizationWithClose() throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeDescriptor[] targets = new DatanodeDescriptor[0];
    DatanodeID[] newTargets = new DatanodeID[0];

    ExtendedBlock lastBlock = new ExtendedBlock(null, block);
      namesystemSpy.commitBlockSynchronization(
          lastBlock, genStamp, length, true,
          false, newTargets, null);

    // Repeat the call to make sure it returns true
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);

    BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, 1);
    setBlockCollectionAndGenerationStamp(completedBlockInfo, file);
    BlockInfoContiguous mockCompletedBlockInfo = mock(BlockInfoContiguous.class);
    doReturn(file).when(mockCompletedBlockInfo).getBlockCollection();
    doReturn(completedBlockInfo.getGenerationStamp()).when(mockCompletedBlockInfo).getGenerationStamp();
    doReturn(completedBlockInfo.isComplete()).when(mockCompletedBlockInfo).isComplete();
    
    doReturn(mockCompletedBlockInfo).when(namesystemSpy)
        .getStoredBlock(any(Block.class));
    doReturn(mockCompletedBlockInfo).when(file).getLastBlock();

    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);
  }
  
  @Test
  public void testCommitBlockSynchronizationWithCloseAndNonExistantTarget()
      throws IOException {
    INodeFile file = mockFileUnderConstruction();
    Block block = new Block(blockId, length, genStamp);
    FSNamesystem namesystemSpy = makeNameSystemSpy(block, file);
    DatanodeID[] newTargets = new DatanodeID[]{
        new DatanodeID("0.0.0.0", "nonexistantHost", "1", 0, 0, 0, 0)};

    ExtendedBlock lastBlock = new ExtendedBlock(null, block);
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true,
        false, newTargets, null);

    // Repeat the call to make sure it returns true
    namesystemSpy.commitBlockSynchronization(
        lastBlock, genStamp, length, true, false, newTargets, null);
  }
}
