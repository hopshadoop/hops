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
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;

import java.io.IOException;

public class TestFailedSTOLockCleaner extends TestCase {


  @Test
  public void testSTOCleanup() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    long stoCleanDelay=1000;
    try {
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
      long delay = (stoCleanDelay + (leadercheckInterval * (missedHeartBeatThreshold+1)) + 3000);
      FSNamesystem.LOG.debug("Testing STO: waiting for "+delay+". After this the locks should have been reclaimed");
      Thread.sleep(stoCleanDelay + (leadercheckInterval * (missedHeartBeatThreshold+1)) + 3000);

      assertEquals("On going subtree ops table", 0, countOnGoingSTOs());
      assertEquals("Locked Inodes", 0, countLockedINodes());

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
    return (Integer)subTreeLockChecker.handle();
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
    return (Integer)subTreeLockChecker.handle();
  }
}
