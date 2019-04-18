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
package io.hops.erasure_coding;

import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import org.junit.Assert;

public class TestMapReduceEncodingManager extends MrClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestLocalEncodingManagerImpl.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private Configuration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");
  private final Path parityFile = new Path("/parity/test_file");

  public TestMapReduceEncodingManager() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setLong(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, Util.JSON_CODEC_ARRAY);
    conf.setBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY, 1);
    numDatanode = 15; // Sometimes a node gets excluded during the test
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Test
  public void testRaidFiles() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile, false);

    List<Report> reports;
    while ((reports = encodingManager.computeReports()).size() > 0) {
      Assert.assertNotSame("Encoding Assert.failed.", Report.Status.FAILED,
          reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    FileStatus parityStatus = dfs.getFileStatus(parityFile);
    Assert.assertEquals(parityStatus.getLen(), 6 * DFS_TEST_BLOCK_SIZE);
    try {
      FSDataInputStream in = dfs.open(parityFile);
      byte[] buff = new byte[6 * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity Assert.failed", e);
      Assert.fail("Parity could not be read.");
    }
  }

  @Test
  public void testFailover() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(mrCluster.getConfig());

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(mrCluster.getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile, false);

    MapReduceEncodingManager recoveredManager =
        new MapReduceEncodingManager(mrCluster.getConfig());
    List<Report> reports = recoveredManager.computeReports();
    Assert.assertEquals(1, reports.size());
    Assert.assertNotSame("Encoding Assert.failed.", Report.Status.FAILED,
        reports.get(0).getStatus());

    while ((reports = recoveredManager.computeReports()).size() > 0) {
      Assert.assertNotSame("Encoding Assert.failed.", Report.Status.FAILED,
          reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    FileStatus parityStatus = dfs.getFileStatus(parityFile);
    Assert.assertEquals(parityStatus.getLen(), 6 * DFS_TEST_BLOCK_SIZE);
    try {
      FSDataInputStream in = dfs.open(parityFile);
      byte[] buff = new byte[6 * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity Assert.failed", e);
      Assert.fail("Parity could not be read.");
    }
  }
}
