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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public abstract class ClusterTest extends TestCase {

  protected static final int DFS_TEST_BLOCK_SIZE = 4 * 1024;

  protected int numDatanode = 3;
  protected MiniDFSCluster cluster;
  protected FileSystem fs;

  protected abstract Configuration getConfig();

  @Override
  public void setUp() throws Exception {
    Configuration conf = getConfig();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanode)
        .build();
    cluster.waitActive();
    fs = FileSystem.get(conf);
  }

  @Override
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }
}

