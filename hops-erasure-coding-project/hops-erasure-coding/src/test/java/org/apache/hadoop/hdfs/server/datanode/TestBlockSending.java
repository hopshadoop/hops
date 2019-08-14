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
package org.apache.hadoop.hdfs.server.datanode;

import io.hops.erasure_coding.ClusterTest;
import io.hops.erasure_coding.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestDfsClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class TestBlockSending extends ClusterTest {

  public static final Log LOG = LogFactory.getLog(TestBlockSending.class);

  private static final int TEST_BLOCK_COUNT = 10;
  private static final int BLOCK_TO_RESEND = 5;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("test_file");

  public TestBlockSending() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setLong(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Test
  public void testBlockSending() throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    TestDfsClient testDfsClient = new TestDfsClient(getConfig());
    testDfsClient.injectIntoDfs(dfs);
    Util.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT,
        DFS_TEST_BLOCK_SIZE);

    FileStatus status = dfs.getFileStatus(testFile);
    LocatedBlock lb = dfs.getClient()
        .getLocatedBlocks(status.getPath().toUri().getPath(), 0, Long.MAX_VALUE)
        .get(0);
    DataNodeUtil.loseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks =
        new LocatedBlocks(0, false, lostBlocks, null, true, null);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
    LOG.info("Losing block " + lb.toString());

    HdfsDataOutputStream out = dfs.sendBlock(status.getPath(), lb, null, null);
    out.write(Util.randomBytes(seed,
        conf.getInt(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE)), 0,
        DFS_TEST_BLOCK_SIZE);
    out.close();
    ExtendedBlock extendedBlock = new ExtendedBlock(lb.getBlock());
    extendedBlock.setBlockId(lb.getBlock().getBlockId());
    int number = getCluster().getAllBlockFiles(extendedBlock).length;
    Assert.assertEquals(conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
        number);
  }
}
