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

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.net.BindException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests the validation of the configuration object when passed
 * to the NameNode
 */
public class TestValidateConfigurationSettings {

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Tests setting the rpc port to the same as the web port to test that
   * an exception
   * is thrown when trying to re-use the same port
   */
  @Test(expected = BindException.class)
  public void testThatMatchingRPCandHttpPortsThrowException()   //HOP: fails in the master branch
      throws IOException {

    Configuration conf = new HdfsConfiguration();
    // set both of these to port 9000, should fail
    FileSystem.setDefaultUri(conf, "hdfs://localhost:9000");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "127.0.0.1:9000");
    DFSTestUtil.formatNameNode(conf);
    new NameNode(conf);
  }

  /**
   * Tests setting the rpc port to a different as the web port that an
   * exception is NOT thrown
   */
  @Test
  public void testThatDifferentRPCandHttpPortsAreOK()       //HOP: fails in the master branch
      throws IOException {

    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:8000");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "127.0.0.1:9000");
    DFSTestUtil.formatNameNode(conf);
    NameNode nameNode = new NameNode(conf); // should be OK!
    nameNode.stop();
  }
}
