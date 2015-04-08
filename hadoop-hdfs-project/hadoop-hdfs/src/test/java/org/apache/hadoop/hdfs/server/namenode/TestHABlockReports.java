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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.MetricsAsserts;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

public class TestHABlockReports extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestHABlockReports.class);

  Configuration conf = new HdfsConfiguration();
  MiniDFSCluster cluster = null;
  int NUM_NAMENODES = 2;
  int NUM_DATANODES = 3;


  public void setconfig() {
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, /*default 15*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY, /*default 10*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY, /*default 500*/ 500);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY, /*default 15000*/1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY, /*default 0*/ 0);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            /*default 0*/0);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, /*default
    45*/ 2);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 1);
    conf.set(HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY, "1000,2");
  }

  /**
   * Testing block reporting in a multi-namenode setup. When a new
   * namenode is started then the datanodes connect to the new namenode.
   * However, there is no need to send complete block report to the new
   * namenode because the existing namenodes have already processed the block
   * reports from the datanodes.
   */
  @Test(timeout = 900000)
  public void testMultiNNBlockReports() throws IOException, TimeoutException, InterruptedException {

    final int NN1 = 0, NN2 = 1;
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }

    try {
      setconfig();
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
              .numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      Thread.sleep(10000); // make sure that all block reports are in

      assertEquals("Wrong number of block reports", 6, getTotalBRSent(cluster));

      LOG.info("Cluster started. Total Block reports: " + getTotalBRSent(cluster));

      //kill one namenode
      cluster.shutdownNameNode(NN2); // now the cluster is running with 1 NN
      LOG.info("Second namenode is killed");

      // create some files and change the state of the datanodes
      DistributedFileSystem fs = cluster.getFileSystem(0);
      for (int i = 0; i < 5; i++) {
        FSDataOutputStream out = fs.create(new Path("/test" + i));
        out.write(i);
        out.close();
      }

      LOG.info("Created some files to change the state of the datanodes");

      assertEquals("Wrong number of block reports", 6, getTotalBRSent(cluster));

      cluster.restartNameNode(NN2); // now the cluster is running with 1 NN
      cluster.waitActive();

      Thread.sleep(10000);

      LOG.info("Restarted the second namenode. Total Block reports: " + getTotalBRSent(cluster));
      assertEquals("Wrong number of block reports", 6, getTotalBRSent(cluster));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  //
  @Test(timeout = 900000)
  public void testAllDeadNamenodes() throws IOException, TimeoutException, InterruptedException {
    final int NN1 = 0, NN2 = 1;
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }

    try {
      setconfig();
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
              .numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      Thread.sleep(10000); // make sure that all block reports are in

      assertEquals("Wrong number of block reports", 6, getTotalBRSent(cluster));
      LOG.info("Cluster started. Total Block reports: " + getTotalBRSent(cluster));

      cluster.shutdownNameNode(NN1);
      cluster.shutdownNameNode(NN2);

      Thread.sleep(10000); // make sure that all block reports are in

      cluster.restartNameNode(NN1, false);
      cluster.restartNameNode(NN2, false);
      cluster.getNameNode(0).metrics.incrAddBlockOps();
      cluster.waitActive();

      Thread.sleep(10000); // make sure that all block reports are in

      // 3 more block reports are received from the 3 datanodes
      assertEquals("Wrong number of block reports", 12, getTotalBRSent(cluster));
      LOG.info("Cluster started. Total Block reports: " + getTotalBRSent(cluster));


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

  }

  private long getTotalBRSent(MiniDFSCluster cluster) {
    long counter = 0;
    List<DataNode> datanodes = cluster.getDataNodes();
    for (DataNode dn : datanodes) {
      MetricsRecordBuilder rb = MetricsAsserts.getMetrics(dn.getMetrics().name());
      counter += MetricsAsserts.getLongCounter("BlockReportCount", rb);
    }
    return counter;
  }
}