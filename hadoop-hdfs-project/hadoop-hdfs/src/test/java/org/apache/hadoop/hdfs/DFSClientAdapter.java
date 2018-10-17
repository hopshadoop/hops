/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class DFSClientAdapter {
  public static DFSClient getDFSClient(DistributedFileSystem dfs) {
    return dfs.dfs;
  }
  
  public static void setDFSClient(DistributedFileSystem dfs, DFSClient client) {
    dfs.dfs = client;
  }
  
  public static void stopLeaseRenewer(DistributedFileSystem dfs)
      throws IOException {
    try {
      dfs.dfs.getLeaseRenewer().interruptAndJoin();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  public static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) throws IOException {
    return DFSClient.callGetBlockLocations(namenode, src, start, length);
  }
  
  public static ClientProtocol getNamenode(DFSClient client) throws IOException {
    return client.getNamenode();
  }

  public static DFSClient getClient(DistributedFileSystem dfs)
      throws IOException {
    return dfs.dfs;
  }

  public static ExtendedBlock getPreviousBlock(DFSClient client, String file) {
    return client.getPreviousBlock(file);
  }

  public static long getFileId(DFSOutputStream out) {
    return out.getFileId();
  }
  
  public static DistributedFileSystem newDistributedFileSystem(
      Configuration conf, ClientProtocol namenode, Collection<ClientProtocol> allNamenodes)
      throws IOException {
    DistributedFileSystem dfs = new DistributedFileSystem();
    dfs.dfs = new DFSClient(null, namenode, conf, null);
    dfs.dfs.setNamenodes(allNamenodes);
    return dfs;
  }
}
