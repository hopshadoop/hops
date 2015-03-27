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
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeUtil;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

@Ignore
public class TestErasureCodingManagerEndless extends ClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestErasureCodingManagerEndless.class);

  private static final int NUMBER_OF_DATANODES = 16;
  private static final int TEST_STRIPE_LENGTH = 10;
  private static final int TEST_PARITY_LENGTH = 6;
  private static final int TEST_STRIPE_COUNT = 2;
  private static final int TEST_BLOCK_COUNT =
      TEST_STRIPE_LENGTH * TEST_STRIPE_COUNT;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");

  public TestErasureCodingManagerEndless() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, Util.JSON_CODEC_ARRAY);
    conf.setBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.ENCODING_MANAGER_CLASSNAME_KEY,
        DFSConfigKeys.DEFAULT_ENCODING_MANAGER_CLASSNAME);
    conf.set(DFSConfigKeys.BLOCK_REPAIR_MANAGER_CLASSNAME_KEY,
        DFSConfigKeys.DEFAULT_BLOCK_REPAIR_MANAGER_CLASSNAME);
    conf.setInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 10 * 1000);
    conf.setInt("dfs.blockreport.intervalMsec", 60 * 1000);
    conf.setInt(DFSConfigKeys.REPAIR_DELAY_KEY, 2 * 60 * 1000);
    conf.setInt(DFSConfigKeys.PARITY_REPAIR_DELAY_KEY, 2 * 60 * 1000);
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConfig())
        .numDataNodes(NUMBER_OF_DATANODES).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    FileSystem fs = getFileSystem();
    FileStatus[] files = fs.globStatus(new Path("/*"));
    for (FileStatus file : files) {
      fs.delete(file.getPath(), true);
    }
  }

  @Ignore
  public void endlessSourceTest() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();

    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);

    while (!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    EncodingStatus status = dfs.getEncodingStatus(testFile.toUri().getPath());
    Path parityPath = new Path("/parity/" + status.getParityFileName());
    FileStatus parityStatus = dfs.getFileStatus(parityPath);
    assertEquals(parityStatus.getLen(),
        TEST_STRIPE_COUNT * TEST_PARITY_LENGTH * DFS_TEST_BLOCK_SIZE);
    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity failed", e);
      fail("Parity could not be read.");
    }

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    LOG.info("Loosing block " + lb.toString());

    EncodingStatus lastStatus = null;
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Was interrupted", e);
      }
      EncodingStatus status2 =
          dfs.getEncodingStatus(testFile.toUri().getPath());
      if (status2.equals(lastStatus) == false) {
        LOG.info("New status is " + status2.getStatus());
        lastStatus = status2;
      }
    }
  }

  @Ignore
  public void endlessParityTest() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();

    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);

    while (!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    EncodingStatus status = dfs.getEncodingStatus(testFile.toUri().getPath());
    Path parityPath = new Path("/parity/" + status.getParityFileName());
    FileStatus parityStatus = dfs.getFileStatus(parityPath);
    assertEquals(parityStatus.getLen(),
        TEST_STRIPE_COUNT * TEST_PARITY_LENGTH * DFS_TEST_BLOCK_SIZE);
    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity failed", e);
      fail("Parity could not be read.");
    }

    int blockToLoose = new Random(seed)
        .nextInt((int) (parityStatus.getLen() / parityStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient()
        .getLocatedBlocks(parityPath.toUri().getPath(), 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    LOG.info("Loosing block " + lb.toString());

    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
      fail("Successfully read parity file which should have been broken.");
    } catch (BlockMissingException e) {
    }

    EncodingStatus lastStatus = null;
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Was interrupted", e);
      }
      EncodingStatus status2 =
          dfs.getEncodingStatus(testFile.toUri().getPath());
      if (status2.equals(lastStatus) == false) {
        LOG.info("New status is " + status2);
        lastStatus = status2;
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
  }
}