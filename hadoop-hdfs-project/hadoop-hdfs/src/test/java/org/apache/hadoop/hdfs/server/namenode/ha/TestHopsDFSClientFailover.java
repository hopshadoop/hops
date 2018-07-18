/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;
import org.apache.log4j.Level;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.Log;
import static org.junit.Assert.fail;

/**
 * This test makes sure that when
 * {@link DFSConfigKeys#DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY} is set,
 * DFSClient instances can still be created within NN/DN (e.g., the fs instance
 * used by the trash emptier thread in NN)
 */
public class TestHopsDFSClientFailover {

  public static final Log LOG =
          LogFactory.getLog(TestHopsDFSClientFailover.class);

  private static void initLoggers() {
    ((Log4JLogger) HopsRandomStickyFailoverProxyProvider.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) TestHopsDFSClientFailover.LOG).getLogger().setLevel(Level.ALL);
  }

  @Test
  public void testHopsRandomStickyFailoverProxyProvider() throws Exception {
    initLoggers();
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, /*default
    45*/ 0);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 0);
    conf.set(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_KEY,"1000,2");

    long leadercheckInterval =
            conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
                    DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
            conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
                    DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    long delay = (leadercheckInterval * (missedHeartBeatThreshold + 1)) + 3000;

    try {
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(2)).numDataNodes(1).build();
      cluster.waitActive();

      FileSystem fs = cluster.getFileSystem(0);
      fs.mkdirs(new Path("/test"));
      LOG.debug("Killing namenode "+cluster.getNameNode(0).getNameNodeAddress());
      cluster.shutdownNameNode(0);

      fs.mkdirs(new Path("/test2"));
      cluster.restartNameNode(0, true);
      LOG.debug("Restarting namenode "+cluster.getNameNode(0).getNameNodeAddress());
      LOG.debug("Killing namenode "+cluster.getNameNode(1).getNameNodeAddress());
      cluster.shutdownNameNode(1);

      fs.mkdirs(new Path("/test3"));
    } catch (Exception e) {
      e.printStackTrace();
      fail("No exception expected "+e);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testHopsLeaderFailoverProxyProvider() throws Exception {
    MiniDFSCluster cluster = null;
    final int numNN = 4;
    Configuration conf = new HdfsConfiguration();
    long leadercheckInterval =
            conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
                    DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
            conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
                    DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    long delay = (leadercheckInterval * (missedHeartBeatThreshold + 1)) + 3000;

    try {
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(numNN)).numDataNodes(0).build();
      cluster.waitActive();

      //by default quota is enabled and quota enabled delete operations are performed on the
      // leader namenode.

      FileSystem fs = cluster.getFileSystem(0);
      fs.mkdirs(new Path("/test/test1"));
      fs.mkdirs(new Path("/test/test2"));
      fs.mkdirs(new Path("/test/test3"));
      fs.mkdirs(new Path("/test/test4"));


      fs.delete(new Path("/test/test1"), true);
      for (int i = 0; i < numNN; i++) {
        if (cluster.getNameNode(i).isLeader()) {
          cluster.getNameNode(i).getLeaderElectionInstance().relinquishCurrentIdInNextRound();
        }
      }

      Thread.sleep(delay);

      fs.delete(new Path("/test/test2"), true);

    } catch (Exception e) {
      fail("No exception expected " + e);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
