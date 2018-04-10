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
package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSAclBaseTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests ACL APIs via WebHDFS.
 */
public class TestWebHDFSAcl extends FSAclBaseTest {

  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = WebHdfsTestUtil.createConf();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf);//, WebHdfsFileSystem.SCHEME);
    assertTrue(fs instanceof WebHdfsFileSystem);
  }

  /**
   * We need to skip this test on WebHDFS, because WebHDFS currently cannot
   * resolve symlinks.
   */
  @Override
  @Test
  @Ignore
  public void testDefaultAclNewSymlinkIntermediate() {
  }
}
