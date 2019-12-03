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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ErasureCodingFileSystem;
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
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class TestErasureCodingManager extends ClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestErasureCodingManager.class);

  private static final int NUMBER_OF_DATANODES = 20;
  private static final int TEST_STRIPE_LENGTH = 10;
  private static final int TEST_PARITY_LENGTH = 6;
  private static final int TEST_STRIPE_COUNT = 2;
  private static final int TEST_BLOCK_COUNT =
      TEST_STRIPE_LENGTH * TEST_STRIPE_COUNT;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");
  private DistributedFileSystem dfs;

  public TestErasureCodingManager() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setLong(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, Util.JSON_CODEC_ARRAY);
    conf.setBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.ENCODING_MANAGER_CLASSNAME_KEY,
        DFSConfigKeys.DEFAULT_ENCODING_MANAGER_CLASSNAME);
    conf.set(DFSConfigKeys.BLOCK_REPAIR_MANAGER_CLASSNAME_KEY,
        DFSConfigKeys.DEFAULT_BLOCK_REPAIR_MANAGER_CLASSNAME);
    conf.setInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 20 * 1000);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10 * 1000);
    conf.setInt(DFSConfigKeys.REPAIR_DELAY_KEY, 10 * 1000);
    conf.setInt(DFSConfigKeys.PARITY_REPAIR_DELAY_KEY, 10 * 1000);
    conf.setClass("fs.hdfs.impl", ErasureCodingFileSystem.class,
        FileSystem.class); // Make sure it works with ecfs
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY, 1);
  }

  protected Configuration getConfig() {
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConfig())
        .numDataNodes(NUMBER_OF_DATANODES).build();
    cluster.waitActive();

    fs = FileSystem.get(conf);
    dfs = (DistributedFileSystem)
        ((ErasureCodingFileSystem) fs).getFileSystem();
    FileStatus[] files = fs.globStatus(new Path("/*"));
    for (FileStatus file : files) {
      fs.delete(file.getPath(), true);
    }
  }

  @Test(timeout = 120000)
  public void testEncoding() throws IOException, InterruptedException {
    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);

    EncodingStatus status;
    while (!(status = dfs.getEncodingStatus(testFile.toUri().getPath()))
        .isEncoded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    Assert.assertEquals(status.getParityStatus(), EncodingStatus.ParityStatus.HEALTHY);
    Path parityPath = new Path(conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER), status.getParityFileName());
    Assert.assertTrue(dfs.exists(parityPath));
    Assert.assertFalse(status.getRevoked());
    Assert.assertEquals(policy, status.getEncodingPolicy());
  }

  @Test(timeout = 120000)
  public void testLateEncoding() throws IOException {
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    dfs.encodeFile(testFile.toUri().getPath(), policy);

    EncodingStatus encodingStatus;
    while (!(encodingStatus =
        dfs.getEncodingStatus(testFile.toUri().getPath())).isEncoded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    FileStatus fileStatus = dfs.getFileStatus(testFile);
    Path parityFile = new Path(
        conf.get(DFSConfigKeys.PARITY_FOLDER,
            DFSConfigKeys.DEFAULT_PARITY_FOLDER),
        encodingStatus.getParityFileName());
    FileStatus parityStatus = dfs.getFileStatus(parityFile);
    BlockLocation[] blockLocations = dfs.getFileBlockLocations(fileStatus, 0,
        TEST_STRIPE_LENGTH * DFS_TEST_BLOCK_SIZE);
    BlockLocation[] parityBlockLocations = dfs.getFileBlockLocations(
        parityStatus, 0, TEST_PARITY_LENGTH * DFS_TEST_BLOCK_SIZE);

    Set<String> set = new HashSet<String>();
    for (BlockLocation blockLocation : blockLocations) {
      String host = blockLocation.getNames()[0];
      if (set.contains(host)) {
        Assert.fail("Duplicated location "
            + Arrays.toString(blockLocation.getNames()));
      }
      set.add(host);
    }
    for (BlockLocation blockLocation : parityBlockLocations) {
      String host = blockLocation.getNames()[0];
      if (set.contains(host)) {
        Assert.fail("Duplicated location "
            + Arrays.toString(blockLocation.getNames()));
      }
      set.add(host);
    }
  }

  @Test(timeout = 120000)
  public void testRevoke() throws IOException, InterruptedException {
    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);

    EncodingStatus status;
    while (!(status = dfs.getEncodingStatus(testFile.toUri().getPath()))
        .isEncoded()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    dfs.revokeEncoding(testFile.toUri().getPath(), (short) 2);
    while (dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    Path parityPath = new Path(conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER), status.getParityFileName());
    Assert.assertFalse(dfs.exists(parityPath));
    Assert.assertEquals(2, dfs.getFileStatus(testFile).getReplication());
  }

  @Test(timeout = 150000)
  public void testDelete() throws IOException, InterruptedException {
    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);

    EncodingStatus status;
    while (!(status = dfs.getEncodingStatus(testFile.toUri().getPath()))
        .isEncoded()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    dfs.delete(testFile, false);

    Thread.sleep(2 * conf.getInt(DFSConfigKeys.RECHECK_INTERVAL_KEY,
        DFSConfigKeys.DEFAULT_RECHECK_INTERVAL));

    Path parityPath = new Path(conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER), status.getParityFileName());
    Assert.assertFalse(dfs.exists(parityPath));
  }

  @Test(timeout = 240000)
  public void testSourceRepair() throws IOException, InterruptedException {
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

    Thread.sleep(2 * conf.getLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, DFSConfigKeys
        .DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT));

    EncodingStatus status = dfs.getEncodingStatus(testFile.toUri().getPath());
    Path parityPath = new Path("/parity/" + status.getParityFileName());
    FileStatus parityStatus = dfs.getFileStatus(parityPath);
    Assert.assertEquals(TEST_STRIPE_COUNT * TEST_PARITY_LENGTH * DFS_TEST_BLOCK_SIZE, parityStatus.getLen());
    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity failed", e);
      Assert.fail("Parity could not be read.");
    }

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(cluster, lb);
    LOG.info("Losing block " + lb.toString());

    Thread.sleep(2 * conf.getLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT) +
        2 * conf.getInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 0));

    while (true) {
      Thread.sleep(10000);
      EncodingStatus status2 = dfs.getEncodingStatus(
          testFile.toUri().getPath());
      LOG.info("Current status is " + status2.getStatus());
      if (status2.getStatus() == EncodingStatus.Status.ENCODED) {
        break;
      }
    }

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      Assert.fail("Repair failed. Missing a block.");
    }
  }

  @Test(timeout = 240000)
  public void testParityRepair() throws IOException, InterruptedException {
    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);

    while (!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Wait for encoding thread was interrupted.");
      }
    }

    Thread.sleep(2 * conf.getLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT));

    EncodingStatus status = dfs.getEncodingStatus(testFile.toUri().getPath());
    Path parityPath = new Path(conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER), status.getParityFileName());
    FileStatus parityStatus = dfs.getFileStatus(parityPath);
    Assert.assertEquals(parityStatus.getLen(),
        TEST_STRIPE_COUNT * TEST_PARITY_LENGTH * DFS_TEST_BLOCK_SIZE);
    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading parity failed", e);
      Assert.fail("Parity could not be read.");
    }

    int blockToLoose = new Random(seed)
        .nextInt((int) (parityStatus.getLen() / parityStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient()
        .getLocatedBlocks(parityPath.toUri().getPath(), 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(cluster, lb);
    LOG.info("Losing block " + lb.toString());

    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
      Assert.fail("Successfully read parity file which should have been broken.");
    } catch (BlockMissingException e) {
    }

    Thread.sleep(2 * conf.getLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT) +
        2 * conf.getInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 0));

    while (true) {
      Thread.sleep(10000);
      EncodingStatus status2 = dfs.getEncodingStatus(
          testFile.toUri().getPath());
      LOG.info("Current status is " + status2);
      if (status2.getParityStatus() == EncodingStatus.ParityStatus.HEALTHY) {
        break;
      }
    }

    try {
      FSDataInputStream in = dfs.open(parityPath);
      byte[] buff = new byte[TEST_STRIPE_COUNT * TEST_PARITY_LENGTH *
          DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      Assert.fail("Repair failed. Missing a block.");
    }
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
  }
}