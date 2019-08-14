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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestDfsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.BlockReconstructor;
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
import org.junit.Before;

public class TestBlockReconstructor extends ClusterTest {

  public static final Log LOG = LogFactory.getLog(TestBlockReconstructor.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");
  private final Path testParityFile = new Path("/raidsrc/test_file");
  private final Codec codec = Util.getCodec(Util.Codecs.SRC);

  public TestBlockReconstructor() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.setInt(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    numDatanode = 16;
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);
    Assert.assertTrue(
        Util.encodeFile(getConfig(), dfs, codec, testFile, testParityFile));
  }

  @Test(timeout = 30000)
  public void testSourceBlockRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);

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

    LocatedBlocks missingBlocks =
        new LocatedBlocks(testFileStatus.getLen(), false,
            new ArrayList<LocatedBlock>(), null, true, null);
    missingBlocks.getLocatedBlocks().add(lb);
    BlockReconstructor blockReconstructor = new BlockReconstructor(conf);
    Decoder decoder = new Decoder(conf, Util.getCodec(Util.Codecs.SRC));
    blockReconstructor
        .processFile(testFile, testParityFile, missingBlocks, decoder, null);

    // Block is recovered to the same data node so no need to wait for the block report
    try {
      FSDataInputStream in = dfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading failed", e);
      Assert.fail("Repair failed. Missing a block.");
    }
  }

  @Test
  public void testParityBlockRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);
    FileStatus parityFileStatus = dfs.getFileStatus(testParityFile);

    String path = parityFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt(
        (int) (parityFileStatus.getLen() / parityFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE)
        .get(blockToLoose);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks =
        new LocatedBlocks(0, false, lostBlocks, null, true, null);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);

    LocatedBlocks missingBlocks =
        new LocatedBlocks(parityFileStatus.getLen(), false,
            new ArrayList<LocatedBlock>(), null, true, null);
    missingBlocks.getLocatedBlocks().add(lb);
    BlockReconstructor blockReconstructor = new BlockReconstructor(conf);
    Decoder decoder = new Decoder(conf, Util.getCodec(Util.Codecs.SRC));
    blockReconstructor
        .processParityFile(testFile, testParityFile, missingBlocks, decoder,
            null);

    // Block is recovered to the same data node so no need to wait for the block report
    try {
      FSDataInputStream in = dfs.open(testParityFile);
      byte[] buff = new byte[DFS_TEST_BLOCK_SIZE * codec.parityLength];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      LOG.error("Reading failed", e);
      Assert.fail("Repair failed. Missing a block.");
    }
  }
}
