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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

public class TestLocalEncodingManagerImpl extends ClusterTest {

  public static final Log LOG =
      LogFactory.getLog(TestLocalEncodingManagerImpl.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private HdfsConfiguration conf;
  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("test_file");
  private final Path parityFile = new Path("/parity/test_file");

  public TestLocalEncodingManagerImpl() {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    numDatanode = 16;
  }

  @Override
  protected Configuration getConfig() {
    return conf;
  }

  @Test
  public void testRaidFiles() throws IOException {
    for(Util.Codecs codec: Util.Codecs.values()){
      testRaidFilesForCodec(codec);
    }
  }
  
  private void testRaidFilesForCodec(Util.Codecs codecName) throws IOException{
    DistributedFileSystem dfs = (DistributedFileSystem) getFileSystem();
    FSDataOutputStream stm = dfs.create(testFile);
    byte[] buffer =
        Util.randomBytes(seed, TEST_BLOCK_COUNT, DFS_TEST_BLOCK_SIZE);
    stm.write(buffer, 0, buffer.length);
    stm.close();

    LocalEncodingManager encodingManager = new LocalEncodingManager(conf);

    Codec codec = Util.getCodec(codecName);
    BaseEncodingManager.Statistics stats = new BaseEncodingManager.Statistics();
    assertTrue(encodingManager.doFileRaid(conf, testFile, parityFile, codec,
        stats, RaidUtils.NULL_PROGRESSABLE, 1, 1, false));
    try {
      dfs.open(parityFile);
      dfs.delete(parityFile, false);
    } catch (IOException e) {
      fail("Couldn't open parity file under given path.");
    }
  }
}
