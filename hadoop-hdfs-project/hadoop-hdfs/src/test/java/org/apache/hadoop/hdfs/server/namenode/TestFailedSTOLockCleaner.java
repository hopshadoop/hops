/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestFailedSTOLockCleaner extends TestCase {

  private static final Log LOG =
          LogFactory.getLog(TestFailedSTOLockCleaner.class);

  @Test
  public void testSTOCleanup() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    long stoCleanDelay = 1000;
    try {
      Logger.getRootLogger().setLevel(Level.WARN);
      Logger.getLogger(TestFailedSTOLockCleaner.class).setLevel(Level.ALL);
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY,
              stoCleanDelay);
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(3))
              .numDataNodes(1).build();
      cluster.waitActive();


      final DistributedFileSystem dfs = cluster.getFileSystem(0);
      final FSNamesystem namesystem = cluster.getNamesystem(0);

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/C"), FsPermission.getDefault());

      namesystem.lockSubtree("/A/C", SubTreeOperation.Type.NA);
      namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);

      assertEquals("On going subtree ops table", 2, countOnGoingSTOs());
      assertEquals("Locked Inodes", 2, countLockedINodes());


      cluster.restartNameNode(0);

      long leadercheckInterval =
              conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
                      DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
      int missedHeartBeatThreshold =
              conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
                      DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
      long delay = (stoCleanDelay + (leadercheckInterval * (missedHeartBeatThreshold + 1)) + 3000);
      FSNamesystem.LOG.debug("Testing STO: waiting for " + delay + ". After this the locks should have been reclaimed");
      Thread.sleep(stoCleanDelay + (leadercheckInterval * (missedHeartBeatThreshold + 1)) + 3000);

      assertEquals("On going subtree ops table", 0, countOnGoingSTOs());
      assertEquals("Locked Inodes", 0, countLockedINodes());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testListingDeadOps() throws IOException {
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatHdfsStorage();

    OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
            .getDataAccess(OngoingSubTreeOpsDataAccess.class);

    List<SubTreeOperation> stos = new ArrayList<>();
    int NUM_NNs = 10;
    int OPS_PER_NN = 3;
    for (int i = 0; i < NUM_NNs; i++) {
      for (int j = 0; j < OPS_PER_NN; j++) {
        SubTreeOperation sto = new SubTreeOperation("/A/" + i + "/" + j, 0, i, SubTreeOperation.Type.NA,
                i /*creation time*/, "user_nn_" + i, 0);
        stos.add(sto);
      }
    }

    da.prepare(Collections.EMPTY_LIST, stos, Collections.EMPTY_LIST);

    int ALIVE_NNS_COUNT = 3;
    long aliveNNsIDs[] = new long[ALIVE_NNS_COUNT];
    for (int i = 0; i < ALIVE_NNS_COUNT; i++) {
      aliveNNsIDs[i] = i;
    }

    // ---------------------------------------------------------
    // test correct no of dead ops are returned based on time
    for (int i = 0; i < NUM_NNs; i++) {
      List<SubTreeOperation> ret = (List<SubTreeOperation>) da.allDeadOperations(aliveNNsIDs, i + 1);
      LOG.info(" i = "+i+" Ret = "+ret.size());
      if(i < ALIVE_NNS_COUNT){
        assert ret.size() == 0;
      } else {
        assert ret.size() == ((i - ALIVE_NNS_COUNT + 1) * OPS_PER_NN);
      }
    }

    List<SubTreeOperation> ret = (List<SubTreeOperation>) da.allDeadOperations(aliveNNsIDs, 0);
    LOG.info("Dead Operations " + ret.size());
    assert ret.size() == 0;

    ret = (List<SubTreeOperation>) da.allDeadOperations(aliveNNsIDs, NUM_NNs );
    LOG.info("Dead Operations " + ret.size());
    assert ret.size() == (NUM_NNs * OPS_PER_NN) - (ALIVE_NNS_COUNT * OPS_PER_NN);

    for (SubTreeOperation sto : ret) {
      assertTrue("Operation " + sto.getPath() + " NN ID: " + sto.getNameNodeId(),
              sto.getNameNodeId() >= ALIVE_NNS_COUNT);
      LOG.info("Dead Operation " + sto.getPath() + " NN ID: " + sto.getNameNodeId());
    }

    // ---------------------------------------------------------
    // test correct no of slow ops are returned
    // slow ops belong to alive NNs

    ret = (List<SubTreeOperation>) da.allSlowActiveOperations(aliveNNsIDs, 0);
    LOG.info("size --> "+ret.size());
    assert ret.size() == 0;

    ret = (List<SubTreeOperation>) da.allSlowActiveOperations(aliveNNsIDs, ALIVE_NNS_COUNT);
    assert ret.size() == ALIVE_NNS_COUNT * OPS_PER_NN;
    for (SubTreeOperation sto : ret) {
      assertTrue("Operation " + sto.getPath() + " NN ID: " + sto.getNameNodeId(),
              sto.getNameNodeId() < ALIVE_NNS_COUNT);
      LOG.info("Dead Operation " + sto.getPath() + " NN ID: " + sto.getNameNodeId());
    }
  }

  @Test
  public void testOperationRetry() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    long stoCleanDelay = 10000;
    try {
      Logger.getRootLogger().setLevel(Level.ALL);
      Logger.getLogger(TestFailedSTOLockCleaner.class).setLevel(Level.ALL);
      Logger.getLogger(MDCleaner.class).setLevel(Level.ALL);
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY,
              stoCleanDelay);
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(1))
              .numDataNodes(1).build();
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem(0);
      final FSNamesystem namesystem = cluster.getNamesystem(0);

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());

      namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);


      assertEquals("On going subtree ops table", 1, countOnGoingSTOs());
      assertEquals("Locked Inodes", 1, countLockedINodes());

      long startTime = System.currentTimeMillis();
      cluster.getNameNode(0).getLeaderElectionInstance().relinquishCurrentIdInNextRound();

      // after stoCleanDelay time the locks will be cleaned and rename will succeed

      try {
        dfs.rename(new Path("/A/B"), new Path("/A/B_new"));
        assert (System.currentTimeMillis() - startTime) > stoCleanDelay;
      } catch (Exception e) {
        fail("No Exception was expected");
      }

      assertEquals("On going subtree ops table", 0, countOnGoingSTOs());
      assertEquals("Locked Inodes", 0, countLockedINodes());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testUnlockMultipleTimes() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    long stoCleanDelay = 10000;
    try {
      Logger.getRootLogger().setLevel(Level.WARN);
      Logger.getLogger(TestFailedSTOLockCleaner.class).setLevel(Level.ALL);
      Logger.getLogger(MDCleaner.class).setLevel(Level.ALL);
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY,
              stoCleanDelay);
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(1))
              .numDataNodes(1).build();
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem(0);
      final FSNamesystem namesystem = cluster.getNamesystem(0);

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());

      INodeIdentifier lockedInodeID = namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);

      try{ // before it cause NPE
        namesystem.unlockSubtree("/A/B", lockedInodeID.getInodeId());
        namesystem.unlockSubtree("/A/B", lockedInodeID.getInodeId());
        namesystem.unlockSubtree("/A/B", lockedInodeID.getInodeId());
      } catch (Exception e ){
        fail();
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSlowOpCleanup() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    long stoCleanDelay = 10000;
    try {
      Logger.getRootLogger().setLevel(Level.WARN);
      Logger.getLogger(TestFailedSTOLockCleaner.class).setLevel(Level.ALL);
      Logger.getLogger(MDCleaner.class).setLevel(Level.ALL);
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY, stoCleanDelay);
      conf.setLong(DFSConfigKeys.DFS_SUBTREE_CLEAN_SLOW_OPS_LOCKS_DELAY_KEY, stoCleanDelay);
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(1))
              .numDataNodes(1).build();
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem(0);
      final FSNamesystem namesystem = cluster.getNamesystem(0);

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());

      namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);

      try{
        namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);
        fail();
      } catch (Exception e ){
      }

      Thread.sleep(stoCleanDelay * 2);

      try{
        namesystem.lockSubtree("/A/B", SubTreeOperation.Type.NA);
      } catch (Exception e ){
        fail(e.toString());
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static int countOnGoingSTOs() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allOps().size();
              }
            };
    return (Integer) subTreeLockChecker.handle();
  }

  public static int countLockedINodes() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);
                return ida.countSubtreeLockedInodes();
              }
            };
    return (Integer) subTreeLockChecker.handle();
  }
}

