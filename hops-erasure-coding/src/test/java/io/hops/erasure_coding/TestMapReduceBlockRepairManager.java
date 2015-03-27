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
import org.apache.hadoop.hdfs.TestDfsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

public class TestMapReduceBlockRepairManager extends ClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestLocalEncodingManagerImpl.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");
  private final Path parityFile = new Path("/parity/test_file");

  public TestMapReduceBlockRepairManager() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, Util.JSON_CODEC_ARRAY);
    numDatanode = 16;
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Test
  public void testBlockRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);

    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile);

    // Busy waiting until the encoding is done
    while (encodingManager.computeReports().size() > 0) {
      ;
    }

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks =
        new LocatedBlocks(0, false, lostBlocks, null, true);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
    LOG.info("Loosing block " + lb.toString());
    getCluster().triggerBlockReports();

    MapReduceBlockRepairManager repairManager =
        new MapReduceBlockRepairManager(conf);
    repairManager.repairSourceBlocks("src", testFile, parityFile);

    while (true) {
      List<Report> reports = repairManager.computeReports();
      if (reports.size() == 0) {
        break;
      }
      LOG.info(reports.get(0).getStatus());
      System.out.println("WAIT");
      Thread.sleep(1000);
    }

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      fail("Repair failed. Missing a block.");
    }
  }

  @Test
  public void testCorruptedRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);

    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile);

    // Busy waiting until the encoding is done
    while (encodingManager.computeReports().size() > 0) {
      ;
    }

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks =
        new LocatedBlocks(0, false, lostBlocks, null, true);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
    LOG.info("Loosing block " + lb.toString());
    getCluster().triggerBlockReports();

    dfs.getClient().addBlockChecksum(testFile.toUri().getPath(),
        (int) (lb.getStartOffset() / lb.getBlockSize()), 0);

    MapReduceBlockRepairManager repairManager =
        new MapReduceBlockRepairManager(conf);
    repairManager.repairSourceBlocks("src", testFile, parityFile);

    while (true) {
      List<Report> reports = repairManager.computeReports();
      if (reports.size() == 0) {
        break;
      }
      LOG.info(reports.get(0).getStatus());
      System.out.println("WAIT");
      Thread.sleep(1000);
    }

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
      fail("Repair succeeded with bogus checksum.");
    } catch (BlockMissingException e) {
    }
  }
}