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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test if FSDataset#append, writeToRbw, and writeToTmp
 */
public class TestWriteToReplica {

  final private static int FINALIZED = 0;
  final private static int TEMPORARY = 1;
  final private static int RBW = 2;
  final private static int RWR = 3;
  final private static int RUR = 4;
  final private static int NON_EXISTENT = 5;
  
  // test close
  @Test
  public void testClose() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet =
          (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      
      ExtendedBlock[] blocks = setup(bpid, dataSet);

      // test close
      testClose(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }

  // test append
  @Test
  public void testAppend() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet =
          (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, dataSet);

      // test append
      testAppend(bpid, dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }

  // test writeToRbw
  @Test
  public void testWriteToRbw() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet =
          (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, dataSet);

      // test writeToRbw
      testWriteToRbw(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }
  
  // test writeToTemporary
  @Test
  public void testWriteToTemporary() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet =
          (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);

      // set up replicasMap
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock[] blocks = setup(bpid, dataSet);

      // test writeToTemporary
      testWriteToTemporary(dataSet, blocks);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Generate testing environment and return a collection of blocks
   * on which to run the tests.
   *
   * @param bpid
   *     Block pool ID to generate blocks for
   * @param dataSet
   *     Namespace in which to insert blocks
   * @return Contrived blocks for further testing.
   * @throws IOException
   */
  private ExtendedBlock[] setup(String bpid, FsDatasetImpl dataSet)
      throws IOException {
    // setup replicas map
    
    ExtendedBlock[] blocks =
        new ExtendedBlock[]{
            new ExtendedBlock(bpid, 1, 1, 2001, Block.NON_EXISTING_BUCKET_ID),
            new ExtendedBlock(bpid, 2, 1, 2002, Block.NON_EXISTING_BUCKET_ID),
            new ExtendedBlock(bpid, 3, 1, 2003, Block.NON_EXISTING_BUCKET_ID),
            new ExtendedBlock(bpid, 4, 1, 2004, Block.NON_EXISTING_BUCKET_ID),
            new ExtendedBlock(bpid, 5, 1, 2005, Block.NON_EXISTING_BUCKET_ID),
            new ExtendedBlock(bpid, 6, 1, 2006, Block.NON_EXISTING_BUCKET_ID)};

    ReplicaMap replicasMap = dataSet.volumeMap;
    FsVolumeImpl vol = (FsVolumeImpl) dataSet.volumes
        .getNextVolume(StorageType.DEFAULT, 0).getVolume();
    ReplicaInfo replicaInfo = new FinalizedReplica(
        blocks[FINALIZED].getLocalBlock(), vol, vol.getCurrentDir().getParentFile());
    replicasMap.add(bpid, replicaInfo);
    replicaInfo.getBlockFile().createNewFile();
    replicaInfo.getMetaFile().createNewFile();
    
    replicasMap.add(bpid, new ReplicaInPipeline(
        blocks[TEMPORARY].getBlockId(),
        blocks[TEMPORARY].getGenerationStamp(),
        blocks[TEMPORARY].getCloudBucketID(), vol,
        vol.createTmpFile(bpid, blocks[TEMPORARY].getLocalBlock()).getParentFile(), 0));
    
    replicaInfo = new ReplicaBeingWritten(blocks[RBW].getLocalBlock(), vol,
        vol.createRbwFile(bpid, blocks[RBW].getLocalBlock()).getParentFile(), null);
    replicasMap.add(bpid, replicaInfo);
    replicaInfo.getBlockFile().createNewFile();
    replicaInfo.getMetaFile().createNewFile();
    
    replicasMap.add(bpid,
        new ReplicaWaitingToBeRecovered(blocks[RWR].getLocalBlock(), vol,
            vol.createRbwFile(bpid, blocks[RWR].getLocalBlock())
                .getParentFile()));
    replicasMap.add(bpid, new ReplicaUnderRecovery(
        new FinalizedReplica(blocks[RUR].getLocalBlock(), vol,
            vol.getCurrentDir().getParentFile()), 2007));
    
    return blocks;
  }
  
  private void testAppend(String bpid, FsDatasetImpl dataSet,
      ExtendedBlock[] blocks) throws IOException {
    long newGS = blocks[FINALIZED].getGenerationStamp() + 1;
    final FsVolumeImpl v = (FsVolumeImpl) dataSet.volumeMap
        .get(bpid, blocks[FINALIZED].getLocalBlock()).getVolume();
    long available = v.getCapacity() - v.getDfsUsed();
    long expectedLen = blocks[FINALIZED].getNumBytes();
    try {
      v.decDfsUsed(bpid, -available);
      blocks[FINALIZED].setNumBytes(expectedLen + 100);
      dataSet.append(blocks[FINALIZED], newGS, expectedLen);
      Assert.fail(
          "Should not have space to append to an RWR replica" + blocks[RWR]);
    } catch (DiskOutOfSpaceException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Insufficient space for appending to "));
    }
    v.decDfsUsed(bpid, available);
    blocks[FINALIZED].setNumBytes(expectedLen);

    newGS = blocks[RBW].getGenerationStamp() + 1;
    dataSet.append(blocks[FINALIZED], newGS,
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);

    try {
      dataSet
          .append(blocks[TEMPORARY], blocks[TEMPORARY].getGenerationStamp() + 1,
              blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have appended to a temporary replica " +
          blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[TEMPORARY],
          e.getMessage());
    }

    try {
      dataSet.append(blocks[RBW], blocks[RBW].getGenerationStamp() + 1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RBW replica" + blocks[RBW]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RBW],
          e.getMessage());
    }

    try {
      dataSet.append(blocks[RWR], blocks[RWR].getGenerationStamp() + 1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RWR],
          e.getMessage());
    }

    try {
      dataSet.append(blocks[RUR], blocks[RUR].getGenerationStamp() + 1,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RUR],
          e.getMessage());
    }

    try {
      dataSet.append(blocks[NON_EXISTENT],
          blocks[NON_EXISTENT].getGenerationStamp(),
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have appended to a non-existent replica " +
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertEquals(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + blocks[NON_EXISTENT],
          e.getMessage());
    }
    
    newGS = blocks[FINALIZED].getGenerationStamp() + 1;
    dataSet.recoverAppend(blocks[FINALIZED], newGS,
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);
    
    try {
      dataSet.recoverAppend(blocks[TEMPORARY],
          blocks[TEMPORARY].getGenerationStamp() + 1,
          blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have appended to a temporary replica " +
          blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    newGS = blocks[RBW].getGenerationStamp() + 1;
    dataSet.recoverAppend(blocks[RBW], newGS, blocks[RBW].getNumBytes());
    blocks[RBW].setGenerationStamp(newGS);

    try {
      dataSet.recoverAppend(blocks[RWR], blocks[RWR].getGenerationStamp() + 1,
          blocks[RBW].getNumBytes());
      Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverAppend(blocks[RUR], blocks[RUR].getGenerationStamp() + 1,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverAppend(blocks[NON_EXISTENT],
          blocks[NON_EXISTENT].getGenerationStamp(),
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have appended to a non-existent replica " +
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
  }

  private void testClose(FsDatasetImpl dataSet, ExtendedBlock[] blocks)
      throws IOException {
    long newGS = blocks[FINALIZED].getGenerationStamp() + 1;
    dataSet.recoverClose(blocks[FINALIZED], newGS,
        blocks[FINALIZED].getNumBytes());  // successful
    blocks[FINALIZED].setGenerationStamp(newGS);
    
    try {
      dataSet.recoverClose(blocks[TEMPORARY],
          blocks[TEMPORARY].getGenerationStamp() + 1,
          blocks[TEMPORARY].getNumBytes());
      Assert.fail("Should not have recovered close a temporary replica " +
          blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    newGS = blocks[RBW].getGenerationStamp() + 1;
    dataSet.recoverClose(blocks[RBW], newGS, blocks[RBW].getNumBytes());
    blocks[RBW].setGenerationStamp(newGS);

    try {
      dataSet.recoverClose(blocks[RWR], blocks[RWR].getGenerationStamp() + 1,
          blocks[RBW].getNumBytes());
      Assert
          .fail("Should not have recovered close an RWR replica" + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverClose(blocks[RUR], blocks[RUR].getGenerationStamp() + 1,
          blocks[RUR].getNumBytes());
      Assert
          .fail("Should not have recovered close an RUR replica" + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
    }

    try {
      dataSet.recoverClose(blocks[NON_EXISTENT],
          blocks[NON_EXISTENT].getGenerationStamp(),
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail("Should not have recovered close a non-existent replica " +
          blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
  }
  
  private void testWriteToRbw(FsDatasetImpl dataSet, ExtendedBlock[] blocks)
      throws IOException {
    try {
      dataSet.recoverRbw(blocks[FINALIZED],
          blocks[FINALIZED].getGenerationStamp() + 1, 0L,
          blocks[FINALIZED].getNumBytes());
      Assert.fail(
          "Should not have recovered a finalized replica " + blocks[FINALIZED]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(
          e.getMessage().startsWith(ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[FINALIZED]);
      Assert.fail("Should not have created a replica that's already " +
          "finalized " + blocks[FINALIZED]);
    } catch (ReplicaAlreadyExistsException e) {
    }

    try {
      dataSet.recoverRbw(blocks[TEMPORARY],
          blocks[TEMPORARY].getGenerationStamp() + 1, 0L,
          blocks[TEMPORARY].getNumBytes());
      Assert.fail(
          "Should not have recovered a temporary replica " + blocks[TEMPORARY]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(
          e.getMessage().startsWith(ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as " +
          "temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }

    dataSet.recoverRbw(blocks[RBW], blocks[RBW].getGenerationStamp() + 1, 0L,
        blocks[RBW].getNumBytes());  // expect to be successful
    
    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[RWR], blocks[RWR].getGenerationStamp() + 1, 0L,
          blocks[RWR].getNumBytes());
      Assert.fail("Should not have recovered a RWR replica " + blocks[RWR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(
          e.getMessage().startsWith(ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
          "recovered " + blocks[RWR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[RUR], blocks[RUR].getGenerationStamp() + 1, 0L,
          blocks[RUR].getNumBytes());
      Assert.fail("Should not have recovered a RUR replica " + blocks[RUR]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(
          e.getMessage().startsWith(ReplicaNotFoundException.NON_RBW_REPLICA));
    }

    try {
      dataSet.createRbw(StorageType.DEFAULT, blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.recoverRbw(blocks[NON_EXISTENT],
          blocks[NON_EXISTENT].getGenerationStamp() + 1, 0L,
          blocks[NON_EXISTENT].getNumBytes());
      Assert.fail(
          "Cannot recover a non-existent replica " + blocks[NON_EXISTENT]);
    } catch (ReplicaNotFoundException e) {
      Assert.assertTrue(e.getMessage()
          .contains(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
    }
    
    dataSet.createRbw(StorageType.DEFAULT, blocks[NON_EXISTENT]);
  }
  
  private void testWriteToTemporary(FsDatasetImpl dataSet,
      ExtendedBlock[] blocks) throws IOException {
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[FINALIZED]);
      Assert.fail("Should not have created a temporary replica that was " +
          "finalized " + blocks[FINALIZED]);
    } catch (ReplicaAlreadyExistsException e) {
    }

    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[TEMPORARY]);
      Assert.fail("Should not have created a replica that had created as" +
          "temporary " + blocks[TEMPORARY]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RBW]);
      Assert.fail("Should not have created a replica that had created as RBW " +
          blocks[RBW]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RWR]);
      Assert.fail("Should not have created a replica that was waiting to be " +
          "recovered " + blocks[RWR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[RUR]);
      Assert.fail("Should not have created a replica that was under recovery " +
          blocks[RUR]);
    } catch (ReplicaAlreadyExistsException e) {
    }
    
    dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]);

    try {
      dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]);
      Assert.fail("Should not have created a replica that had already been "
          + "created " + blocks[NON_EXISTENT]);
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains(blocks[NON_EXISTENT].getBlockName()));
      Assert.assertTrue(e instanceof ReplicaAlreadyExistsException);
    }

    long newGenStamp = blocks[NON_EXISTENT].getGenerationStamp() * 10;
    blocks[NON_EXISTENT].setGenerationStamp(newGenStamp);
    try {
      ReplicaInPipelineInterface replicaInfo =
          dataSet.createTemporary(StorageType.DEFAULT, blocks[NON_EXISTENT]).getReplica();
      Assert.assertTrue(replicaInfo.getGenerationStamp() == newGenStamp);
      Assert.assertTrue(
          replicaInfo.getBlockId() == blocks[NON_EXISTENT].getBlockId());
    } catch (ReplicaAlreadyExistsException e) {
      Assert.fail("createRbw() Should have removed the block with the older "
          + "genstamp and replaced it with the newer one: " + blocks[NON_EXISTENT]);
    }
  }
  
  /**
   * This is a test to check the replica map before and after the datanode 
   * quick restart (less than 5 minutes)
   * @throws Exception
   */
  @Test
  public  void testReplicaMapAfterDatanodeRestart() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(2))
        .build();
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);
      
      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.
          getFSDataset(dn);
      ReplicaMap replicaMap = dataSet.volumeMap;
      
      List<FsVolumeImpl> volumes = dataSet.getVolumes();
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong", 2, volumes.size());
      //HOPS: in hops we have the same blockpool for all the NN so we should only have one blockpool.
      ArrayList<String> bpList = new ArrayList<String>(Arrays.asList(
          cluster.getNamesystem(0).getBlockPoolId()));
      
      Assert.assertTrue("Cluster should have 1 block pool", 
          bpList.size() == 1);
      
      createReplicas(bpList, volumes, replicaMap);
      ReplicaMap oldReplicaMap = new ReplicaMap(this);
      oldReplicaMap.addAll(replicaMap);
      
      cluster.restartDataNode(0);
      cluster.waitActive();
      dn = cluster.getDataNodes().get(0);
      dataSet = (FsDatasetImpl) dn.getFSDataset();
      testEqualityOfReplicaMap(oldReplicaMap, dataSet.volumeMap, bpList);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Compare the replica map before and after the restart
   **/
  private void testEqualityOfReplicaMap(ReplicaMap oldReplicaMap, ReplicaMap 
      newReplicaMap, List<String> bpidList) {
    // Traversing through newReplica map and remove the corresponding 
    // replicaInfo from oldReplicaMap.
    for (String bpid: bpidList) {
      for (ReplicaInfo info: newReplicaMap.replicas(bpid)) {
        assertNotNull("Volume map before restart didn't contain the "
            + "blockpool: " + bpid, oldReplicaMap.replicas(bpid));
        
        ReplicaInfo oldReplicaInfo = oldReplicaMap.get(bpid, 
            info.getBlockId());
        // Volume map after restart contains a blockpool id which 
        assertNotNull("Old Replica Map didnt't contain block with blockId: " +
            info.getBlockId(), oldReplicaInfo);
        
        ReplicaState oldState = oldReplicaInfo.getState();
        // Since after restart, all the RWR, RBW and RUR blocks gets 
        // converted to RWR
        if (info.getState() == ReplicaState.RWR) {
           if (oldState == ReplicaState.RWR || oldState == ReplicaState.RBW 
               || oldState == ReplicaState.RUR) {
             oldReplicaMap.remove(bpid, oldReplicaInfo);
           }
        } else if (info.getState() == ReplicaState.FINALIZED && 
            oldState == ReplicaState.FINALIZED) {
          oldReplicaMap.remove(bpid, oldReplicaInfo);
        }
      }
    }
    
    // We don't persist the ReplicaInPipeline replica
    // and if the old replica map contains any replica except ReplicaInPipeline
    // then we didn't persist that replica
    for (String bpid: bpidList) {
      for (ReplicaInfo replicaInfo: oldReplicaMap.replicas(bpid)) {
        if (replicaInfo.getState() != ReplicaState.TEMPORARY) {
          Assert.fail("After datanode restart we lost the block with blockId: "
              +  replicaInfo.getBlockId());
        }
      }
    }
  }

  private void createReplicas(List<String> bpList, List<FsVolumeImpl> volumes,
      ReplicaMap volumeMap) throws IOException {
    Assert.assertTrue("Volume map can't be null" , volumeMap != null);
    
    // Here we create all different type of replicas and add it
    // to volume map. 
    // Created all type of ReplicaInfo, each under Blkpool corresponding volume
    long id = 1; // This variable is used as both blockId and genStamp
    for (String bpId: bpList) {
      for (FsVolumeImpl volume: volumes) {
        ReplicaInfo finalizedReplica = new FinalizedReplica(id, 1, id,
           Block.NON_EXISTING_BUCKET_ID, volume,
            DatanodeUtil.idToBlockDir(volume.getFinalizedDir(bpId), id));
        volumeMap.add(bpId, finalizedReplica);
        id++;
        
        ReplicaInfo rbwReplica = new ReplicaBeingWritten(id, 1, id,
                Block.NON_EXISTING_BUCKET_ID, volume, volume.getRbwDir(bpId), null, 100);
        volumeMap.add(bpId, rbwReplica);
        id++;

        ReplicaInfo rwrReplica = new ReplicaWaitingToBeRecovered(id, 1, id,
                Block.NON_EXISTING_BUCKET_ID, volume, volume.getRbwDir(bpId));
        volumeMap.add(bpId, rwrReplica);
        id++;
        
        ReplicaInfo ripReplica = new ReplicaInPipeline(id, id, Block.NON_EXISTING_BUCKET_ID,
                volume, volume.getTmpDir(bpId), 0);
        volumeMap.add(bpId, ripReplica);
        id++;
      }
    }
    
    for (String bpId: bpList) {
      for (ReplicaInfo replicaInfo: volumeMap.replicas(bpId)) {
        File parentFile = replicaInfo.getBlockFile().getParentFile();
        if (!parentFile.exists()) {
          if (!parentFile.mkdirs()) {
            throw new IOException("Failed to mkdirs " + parentFile);
          }
        }
        replicaInfo.getBlockFile().createNewFile();
        replicaInfo.getMetaFile().createNewFile();
      }
    }
  }
}
