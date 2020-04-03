/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the cd Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Test;

import java.io.IOException;
import static org.junit.Assert.fail;


/**
 * This class tests various cases during file creation.
 */
public class TestFormatAll {

  @Test
  public void testFormatAlll() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    ExitUtil.disableSystemExit();
    String[] argv = {"-formatAll"};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testCreateAfterFormat() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    try {
      NameNode.formatAll(conf);
    } catch (Exception e) {
      fail();
    }

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.create(new Path("/testfile")).close();

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
