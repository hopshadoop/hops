/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the cd Apache License, Version 2.0 (the
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.junit.Assert.fail;

public class TestUnicode {
  @Test
  public void testDelete() throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
            .newInstance(fs.getUri(), fs.getConf());
    try {

      try {
        fs.mkdirs(new Path("/bär")); // ä is unicode here
        fail("FS operation was expected to fail");
      } catch (IOException e){
      }

      try {
        fs.mkdirs(new Path("/bär")); // ä is latin
      } catch (IOException e){
        fail("FS operation was not expected to fail");
      }

      try {
        NameNodeRpcServer.checkCollation("!\"#$%&'()*+,-./0123456789:;" +
                "<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz" +
                "{|}~¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ");
      } catch (IOException e){
        fail("FS operation was not expected to fail");
      }

      try {
        NameNodeRpcServer.checkCollation("测试字符串");
        fail("FS operation was expected to fail");
      } catch (IOException e){
      }

    } finally {
      cluster.shutdown();
    }

  }
}

