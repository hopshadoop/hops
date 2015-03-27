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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class TestDfsClient extends DFSClient {

  private LocatedBlocks missingLocatedBlocks;

  public TestDfsClient(Configuration conf) throws IOException {
    super(conf);
  }

  public TestDfsClient(InetSocketAddress address, Configuration conf)
      throws IOException {
    super(address, conf);
  }

  public TestDfsClient(URI nameNodeUri, Configuration conf) throws IOException {
    super(nameNodeUri, conf);
  }

  public TestDfsClient(URI nameNodeUri, Configuration conf,
      FileSystem.Statistics stats) throws IOException {
    super(nameNodeUri, conf, stats);
  }

  @Override
  public LocatedBlocks getMissingLocatedBlocks(String src) throws IOException {
    return missingLocatedBlocks;
  }

  @Override
  public LocatedBlock getRepairedBlockLocations(String sourcePath,
      String parityPath, LocatedBlock block, boolean isParity)
      throws IOException {
    return block;
  }

  public void setMissingLocatedBlocks(LocatedBlocks missingLocatedBlocks) {
    this.missingLocatedBlocks = missingLocatedBlocks;
  }

  public void injectIntoDfs(DistributedFileSystem dfs) {
    dfs.dfs = this;
  }
}
