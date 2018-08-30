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
package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.Assert;
import org.junit.Test;


/**
 * This class tests rolling upgrade.
 */
public class TestRollingUpgrade {
  private static final Log LOG = LogFactory.getLog(TestRollingUpgrade.class);

  public static void runCmd(DFSAdmin dfsadmin, boolean success,
      String... args) throws  Exception {
    if (success) {
      Assert.assertEquals(0, dfsadmin.run(args));
    } else {
      Assert.assertTrue(dfsadmin.run(args) != 0);
    }
  }

  /**
   * Test DFSAdmin Upgrade Command.
   */
  @Test
  public void testDFSAdminRollingUpgradeCommands() throws Exception {
    // start a cluster 
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");
      final Path baz = new Path("/baz");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        final DFSAdmin dfsadmin = new DFSAdmin(conf);
        dfs.mkdirs(foo);

        //illegal argument "abc" to rollingUpgrade option
        runCmd(dfsadmin, false, "-rollingUpgrade", "abc");

        //query rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade");

        //start rolling upgrade
//        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        runCmd(dfsadmin, true, "-rollingUpgrade", "prepare");
//        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

        //query rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade", "query");

        dfs.mkdirs(bar);
        
        //finalize rolling upgrade
        runCmd(dfsadmin, true, "-rollingUpgrade", "finalize");

        dfs.mkdirs(baz);

        runCmd(dfsadmin, true, "-rollingUpgrade");

        // All directories created before upgrade, when upgrade in progress and
        // after upgrade finalize exists
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));

      }

      // Ensure directories exist after restart
      cluster.restartNameNode();
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));
      }
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }

  @Test
  public void testDFSAdminDatanodeUpgradeControlCommands() throws Exception {
    // start a cluster
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final DFSAdmin dfsadmin = new DFSAdmin(conf);
      DataNode dn = cluster.getDataNodes().get(0);

      // check the datanode
      final String dnAddr = dn.getDatanodeId().getIpcAddr(false);
      final String[] args1 = {"-getDatanodeInfo", dnAddr};
      Assert.assertEquals(0, dfsadmin.run(args1));

      // issue shutdown to the datanode.
      final String[] args2 = {"-shutdownDatanode", dnAddr, "upgrade" };
      Assert.assertEquals(0, dfsadmin.run(args2));

      // the datanode should be down.
      Thread.sleep(2000);
      Assert.assertFalse("DataNode should exit", dn.isDatanodeUp());

      // ping should fail.
      Assert.assertEquals(-1, dfsadmin.run(args1));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  @Test (timeout = 300000)
  public void testQueryAfterRestart() throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

//      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      // start rolling upgrade
      dfs.rollingUpgrade(RollingUpgradeAction.PREPARE);

      cluster.restartNameNodes();
      dfs.rollingUpgrade(RollingUpgradeAction.QUERY);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
