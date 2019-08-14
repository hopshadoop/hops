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
package org.apache.hadoop.fs;

import io.hops.erasure_coding.ClusterTest;
import io.hops.erasure_coding.Codec;
import io.hops.erasure_coding.TestLocalEncodingManagerImpl;
import io.hops.erasure_coding.Util;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestDfsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import org.junit.Assert;

public class TestErasureCodingFileSystem extends ClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestLocalEncodingManagerImpl.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");

  public TestErasureCodingFileSystem() {
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
    conf.setInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 10000);
    conf.setInt(DFSConfigKeys.REPAIR_DELAY_KEY, 100 * 60 * 60 * 1000);
    conf.setClass("fs.hdfs.impl", ErasureCodingFileSystem.class,
        FileSystem.class);
    numDatanode = 16;
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Test
  public void testSetAsDefaultFS() throws IOException {
    Assert.assertEquals(ErasureCodingFileSystem.class,
        FileSystem.get(conf).getClass());
  }

  @Test
  public void testReadBrokenFile() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem)
        ((ErasureCodingFileSystem) getFileSystem()).getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);

    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);

    // Busy waiting until the encoding is done
    while (!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      Thread.sleep(1000);
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
        new LocatedBlocks(0, false, lostBlocks, null, true, null);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
    LOG.info("Losing block " + lb.toString());
    getCluster().triggerBlockReports();

    ErasureCodingFileSystem ecfs = (ErasureCodingFileSystem) getFileSystem();
    NameNode nameNode = getCluster().getNameNode();
    ecfs.initialize(nameNode.getUri(nameNode.getServiceRpcAddress()), conf);
    try {
      FSDataInputStream in = ecfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      Assert.fail("Repair failed. Missing a block.");
    }
  }

  @Test
  public void testCorruptRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem)
        ((ErasureCodingFileSystem) getFileSystem()).getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);

    Codec.initializeCodecs(getConfig());
    EncodingPolicy policy = new EncodingPolicy("src", (short) 1);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE, policy);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);

    // Busy waiting until the encoding is done
    while (!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      Thread.sleep(1000);
    }

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    final LocatedBlock lb = dfs.getClient()
        .getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks =
        new LocatedBlocks(0, false, lostBlocks, null, true, null);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
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

    ErasureCodingFileSystem ecfs = (ErasureCodingFileSystem) getFileSystem();
    NameNode nameNode = getCluster().getNameNode();
    ecfs.initialize(nameNode.getUri(nameNode.getServiceRpcAddress()), conf);
    try {
      FSDataInputStream in = ecfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
      Assert.fail("Read succeeded with bogus checksum");
    } catch (BlockMissingException e) {
    }
  }
}