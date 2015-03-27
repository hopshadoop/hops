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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

public abstract class BasicClusterTestCase extends TestCase {

  protected static final int DFS_TEST_BLOCK_SIZE = 4 * 1024;

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private Configuration conf;

  protected BasicClusterTestCase() {
    this(new HdfsConfiguration());
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    conf.setBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 10000);
  }

  protected BasicClusterTestCase(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(getConf()
        .getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
            DFSConfigKeys.DFS_REPLICATION_DEFAULT)).build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
  }

  @Override
  public void tearDown() throws Exception {
    FileStatus[] files = dfs.globStatus(new Path("/*"));
    for (FileStatus file : files) {
      dfs.delete(file.getPath(), true);
    }
    dfs.close();
    cluster.shutdown();
  }

  public DistributedFileSystem getDfs() {
    return dfs;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }
}

