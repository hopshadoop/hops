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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test checks correctness of port usage by hdfs components:
 * NameNode, DataNode, SecondaryNamenode and BackupNode.
 * <p/>
 * The correct behavior is:<br>
 * - when a specific port is provided the server must either start on that port
 * or fail by throwing {@link java.net.BindException}.<br>
 * - if the port = 0 (ephemeral) then the server should choose
 * a free port and start on it.
 */
public class TestHDFSServerPorts {
  public static final Log LOG = LogFactory.getLog(TestHDFSServerPorts.class);
  
  // reset default 0.0.0.0 addresses in order to avoid IPv6 problem
  static final String THIS_HOST = getFullHostName() + ":0";
  
  private static final File TEST_DATA_DIR = PathUtils.getTestDir(TestHDFSServerPorts.class);
  
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  Configuration config;
  File hdfsDir;

  /**
   * Attempt to determine the fully qualified domain name for this host
   * to compare during testing.
   * <p/>
   * This is necessary because in order for the BackupNode test to correctly
   * work, the namenode must have its http server started with the fully
   * qualified address, as this is the one the backupnode will attempt to start
   * on as well.
   *
   * @return Fully qualified hostname, or 127.0.0.1 if can't determine
   */
  public static String getFullHostName() {
    try {
      return DNS.getDefaultHost("default");
    } catch (UnknownHostException e) {
      LOG.warn("Unable to determine hostname.  May interfere with obtaining " +
          "valid test results.");
      return "127.0.0.1";
    }
  }
  
  public NameNode startNameNode() throws IOException {
    return startNameNode(false);
  }

  /**
   * Start the namenode.
   */
  public NameNode startNameNode(boolean withService) throws IOException {
    hdfsDir = new File(TEST_DATA_DIR, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    config = new HdfsConfiguration();
    FileSystem.setDefaultUri(config, "hdfs://" + THIS_HOST);
    if (withService) {
      NameNode.setServiceAddress(config, THIS_HOST);
    }
    config.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
    DFSTestUtil.formatNameNode(config);

    String[] args = new String[]{};
    // NameNode will modify config with the ports it bound to
    return NameNode.createNameNode(args, config);
  }
  
  /**
   * Start the datanode.
   */
  public DataNode startDataNode(int index, Configuration config) 
  throws IOException {
    File dataNodeDir = new File(TEST_DATA_DIR, "data-" + index);
    config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataNodeDir.getPath());

    String[] args = new String[]{};
    // NameNode will modify config with the ports it bound to
    return DataNode.createDataNode(args, config);
  }

  /**
   * Stop the datanode.
   */
  public void stopDataNode(DataNode dn) {
    if (dn != null) {
      dn.shutdown();
    }
  }

  public void stopNameNode(NameNode nn) {
    if (nn != null) {
      nn.stop();
    }
  }

  public Configuration getConfig() {
    return this.config;
  }

  /**
   * Check whether the namenode can be started.
   */
  private boolean canStartNameNode(Configuration conf) throws IOException {
    NameNode nn2 = null;
    try {
      nn2 = NameNode.createNameNode(new String[]{}, conf);
    } catch (IOException e) {
      if (e instanceof java.net.BindException) {
        return false;
      }
      throw e;
    } finally {
      stopNameNode(nn2);
    }
    return true;
  }

  /**
   * Check whether the datanode can be started.
   */
  private boolean canStartDataNode(Configuration conf) throws IOException {
    DataNode dn = null;
    try {
      dn = DataNode.createDataNode(new String[]{}, conf);
    } catch (IOException e) {
      if (e instanceof java.net.BindException) {
        return false;
      }
      throw e;
    } finally {
      if (dn != null) {
        dn.shutdown();
      }
    }
    return true;
  }

  @Test(timeout = 300000)
  public void testNameNodePorts() throws Exception {
    runTestNameNodePorts(false);
    runTestNameNodePorts(true);
  }

  /**
   * Verify namenode port usage.
   */
  public void runTestNameNodePorts(boolean withService) throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode(withService);

      // start another namenode on the same port
      Configuration conf2 = new HdfsConfiguration(config);
      DFSTestUtil.formatNameNode(conf2);
      boolean started = canStartNameNode(conf2);
      assertFalse(started); // should fail

      // start on a different main port
      FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
      started = canStartNameNode(conf2);
      assertFalse(started); // should fail again

      // reset conf2 since NameNode modifies it
      FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
      // different http port
      conf2.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
      started = canStartNameNode(conf2);

      if (withService) {
        assertFalse("Should've failed on service port", started);

        // reset conf2 since NameNode modifies it
        FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
        conf2.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
        // Set Service address      
        conf2
            .set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, THIS_HOST);
        started = canStartNameNode(conf2);
      }
      assertTrue(started);
    } finally {
      stopNameNode(nn);
    }
  }

  /**
   * Verify datanode port usage.
   */
  @Test(timeout = 300000)
  public void testDataNodePorts() throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode();

      // start data-node on the same port as name-node
      Configuration conf2 = new HdfsConfiguration(config);
      conf2.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
          new File(hdfsDir, "data").getPath());
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
          FileSystem.getDefaultUri(config).getAuthority());
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, THIS_HOST);
      boolean started = canStartDataNode(conf2);
      assertFalse(started); // should fail

      // bind http server to the same port as name-node
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
          config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
      started = canStartDataNode(conf2);
      assertFalse(started); // should fail

      // both ports are different from the name-node ones
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, THIS_HOST);
      started = canStartDataNode(conf2);
      assertTrue(started); // should start now
    } finally {
      stopNameNode(nn);
    }
  }
}
