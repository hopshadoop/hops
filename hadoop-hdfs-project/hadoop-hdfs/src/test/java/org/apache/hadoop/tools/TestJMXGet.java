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

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;


/**
 * Startup and checkpoint tests
 */
public class TestJMXGet {

  private Configuration config;
  private MiniDFSCluster cluster;

  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;

  private void writeFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }


  @Before
  public void setUp() throws Exception {
    config = new HdfsConfiguration();
  }

  /**
   * clean up
   */
  @After
  public void tearDown() throws Exception {
    if (cluster.isClusterUp()) {
      cluster.shutdown();
    }

    File data_dir = new File(cluster.getDataDirectory());
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException(
          "Could not delete hdfs directory in tearDown '" + data_dir + "'");
    }
  }

  /**
   * test JMX connection to NameNode..
   *
   * @throws Exception
   */
  @Test
  public void testNameNode() throws Exception {
    int numDatanodes = 2;
    cluster =
        new MiniDFSCluster.Builder(config).numDataNodes(numDatanodes).build();
    cluster.waitActive();

    writeFile(cluster.getFileSystem(), new Path("/test1"), 2);

    JMXGet jmx = new JMXGet();
    //jmx.setService("*"); // list all hadoop services
    //jmx.init();
    //jmx = new JMXGet();
    jmx.init(); // default lists namenode mbeans only
    Thread.sleep(25000);

    //get some data from different source
    assertEquals(numDatanodes,
        Integer.parseInt(jmx.getValue("NumLiveDataNodes")));
    assertGauge("CorruptBlocks", Long.parseLong(jmx.getValue("CorruptBlocks")),
        getMetrics("FSNamesystem"));

//    https://issues.apache.org/jira/browse/HDFS-10270
//    assertEquals(numDatanodes,
//        Integer.parseInt(jmx.getValue("NumOpenConnections")));

    cluster.shutdown();
  }

  /**
   * test JMX connection to DataNode..
   *
   * @throws Exception
   */
  @Test
  public void testDataNode() throws Exception {
    int numDatanodes = 2;
    cluster =
        new MiniDFSCluster.Builder(config).numDataNodes(numDatanodes).build();
    cluster.waitActive();

    writeFile(cluster.getFileSystem(), new Path("/test"), 2);

    JMXGet jmx = new JMXGet();
    //jmx.setService("*"); // list all hadoop services
    //jmx.init();
    //jmx = new JMXGet();
    jmx.setService("DataNode");
    jmx.init();
    Thread.sleep(15000);
    assertEquals(fileSize, Integer.parseInt(jmx.getValue("BytesWritten")));

    cluster.shutdown();
  }
}
