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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
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
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import org.junit.Assert;

public class TestMapReduceBlockRepairManager extends MrClusterTest {

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
  public void testBlockRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();

    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile, false);

    // Busy waiting until the encoding is done
    List<Report> reports;
    while ((reports = encodingManager.computeReports()).size() > 0) {
      Assert.assertNotSame(Report.Status.FAILED, reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    addEncodingStatus(testFile, policy);

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LOG.info("Losing block " + lb.toString());
    getCluster().triggerBlockReports();

    MapReduceBlockRepairManager repairManager =
        new MapReduceBlockRepairManager(conf);
    repairManager.repairSourceBlocks("src", testFile, parityFile);

    while ((reports = repairManager.computeReports()).size() > 0) {
      Assert.assertNotSame("Repair Assert.failed.", Report.Status.FAILED,
          reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      Assert.fail("Repair Assert.failed. Missing a block.");
    }
  }

  @Test
  public void testCorruptedRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();

    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile, false);

    // Busy waiting until the encoding is done
    List<Report> reports;
    while ((reports = encodingManager.computeReports()).size() > 0) {
      Assert.assertNotSame(Report.Status.FAILED, reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    addEncodingStatus(testFile, policy);

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    final LocatedBlock lb = dfs.getClient()
        .getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LOG.info("Losing block " + lb.toString());
    getCluster().triggerBlockReports();

    final long inodeId = io.hops.TestUtil.getINodeId(cluster.getNameNode(),
        testFile);
    new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        BlockChecksumDataAccess da = (BlockChecksumDataAccess)
            HdfsStorageFactory.getDataAccess(BlockChecksumDataAccess.class);
        da.update(new BlockChecksum(inodeId,
            (int) (lb.getStartOffset() / lb.getBlockSize()), 0));
        return null;
      }
    }.handle();

    MapReduceBlockRepairManager repairManager =
        new MapReduceBlockRepairManager(conf);
    repairManager.repairSourceBlocks("src", testFile, parityFile);

    Report lastReport = null;
    while ((reports = repairManager.computeReports()).size() > 0) {
      Thread.sleep(1000);
      lastReport = reports.get(0);
    }
    Assert.assertEquals(Report.Status.FAILED, lastReport.getStatus());

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
      Assert.fail("Repair succeeded with bogus checksum.");
    } catch (BlockMissingException e) {
    }
  }

  @Test
  public void testFailover() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    MapReduceEncodingManager encodingManager =
        new MapReduceEncodingManager(conf);

    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    encodingManager.encodeFile(policy, testFile, parityFile, false);

    // Busy waiting until the encoding is done
    List<Report> reports;
    while ((reports = encodingManager.computeReports()).size() > 0) {
      Assert.assertNotSame(Report.Status.FAILED, reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    addEncodingStatus(testFile, policy);

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LOG.info("Losing block " + lb.toString());
    getCluster().triggerBlockReports();

    MapReduceBlockRepairManager repairManager =
        new MapReduceBlockRepairManager(mrCluster.getConfig());
    repairManager.repairSourceBlocks("src", testFile, parityFile);

    MapReduceBlockRepairManager recoverdManager =
        new MapReduceBlockRepairManager(mrCluster.getConfig());

    reports = recoverdManager.computeReports();
    Assert.assertEquals(1, reports.size());
    Assert.assertNotSame("Repair Assert.failed.", Report.Status.FAILED,
        reports.get(0).getStatus());

    while ((reports = recoverdManager.computeReports()).size() > 0) {
      Assert.assertNotSame("Repair Assert.failed.", Report.Status.FAILED,
          reports.get(0).getStatus());
      Thread.sleep(1000);
    }

    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      Assert.fail("Repair Assert.failed. Missing a block.");
    }
  }

  private void addEncodingStatus(Path src, EncodingPolicy policy)
      throws IOException {
    final EncodingStatus status = new EncodingStatus(EncodingStatus.Status.ENCODED);
    status.setEncodingPolicy(policy);
    status.setInodeId(io.hops.TestUtil.getINodeId(cluster.getNameNode(), src));
    new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        EncodingStatusDataAccess da = (EncodingStatusDataAccess)
            HdfsStorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        da.add(status);
        return null;
      }
    }.handle();
  }
}