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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test to make sure that Append properly logs the right
 * things to the edit log, such that files aren't lost or truncated
 * on restart.
 */
public class TestFileAppendRestart {
  private static final int BLOCK_SIZE = 4096;

  private void writeAndAppend(FileSystem fs, Path p, int lengthForCreate,
      int lengthForAppend) throws IOException {
    // Creating a file with 4096 blockSize to write multiple blocks
    FSDataOutputStream stream =
        fs.create(p, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
    try {
      AppendTestUtil.write(stream, 0, lengthForCreate);
      stream.close();
      
      stream = fs.append(p);
      AppendTestUtil.write(stream, lengthForCreate, lengthForAppend);
      stream.close();
    } finally {
      IOUtils.closeStream(stream);
    }
    
    int totalLength = lengthForCreate + lengthForAppend;
    assertEquals(totalLength, fs.getFileStatus(p).getLen());
  }

  /**
   * Regression test for HDFS-2991. Creates and appends to files
   * where blocks start/end on block boundaries.
   */
  @Test
  public void testAppendRestart() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 0);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      Path p1 = new Path("/block-boundaries");
      writeAndAppend(fs, p1, BLOCK_SIZE, BLOCK_SIZE);

      Path p2 = new Path("/not-block-boundaries");
      writeAndAppend(fs, p2, BLOCK_SIZE / 2, BLOCK_SIZE);

      cluster.restartNameNode();
      
      AppendTestUtil.check(fs, p1, 2 * BLOCK_SIZE);
      AppendTestUtil.check(fs, p2, 3 * BLOCK_SIZE / 2);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeStream(stream);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test to append to the file, when one of datanode in the existing pipeline
   * is down.
   *
   * @throws Exception
   */
  @Test
  public void testAppendWithPipelineRecovery() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FSDataOutputStream out = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).manageDataDfsDirs(true)
          .manageNameDfsDirs(true).numDataNodes(4)
          .racks(new String[] { "/rack1", "/rack1", "/rack2", "/rack2" })
          .build();
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test1");

      out = fs.create(path, true, BLOCK_SIZE, (short) 3, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, 1024);
      out.close();

      cluster.stopDataNode(3);
      out = fs.append(path);
      AppendTestUtil.write(out, 1024, 1024);
      out.close();

      cluster.restartNameNode(true);
      AppendTestUtil.check(fs, path, 2048);
    } finally {
      IOUtils.closeStream(out);
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }
}
