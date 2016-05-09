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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.MD5Hash;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests the creation of files with block-size
 * smaller than the default buffer size of 4K.
 */
public class TestBlockCheckSum {



  /**
   * Tests small block size in in DFS.
   */
  @Test
  public void testSmallBlock() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, "1024");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("/file.data");
      FSDataOutputStream out = fileSys.create(file1);
      byte data[] = new byte[1024];
      Random rand = new Random(System.currentTimeMillis());
      for(int i = 0 ; i < 1024; i++){
        data[i] = (byte)rand.nextInt(255);
      }
      out.write(data);
      out.close();

      FSDataInputStream  in = fileSys.open(file1);
      byte readData[] = new byte[1024];
      in.read(readData, 0, 1024);
      Path file2 = new Path("/file.data");
      out = fileSys.create(file2);
      out.write(readData);
      out.close();


      MD5Hash hash1 = fileSys.getClient().getFileBlockChecksum(file1.toString(), 0);
      MD5Hash hash2 = fileSys.getClient().getFileBlockChecksum(file2.toString(), 0);

      if(!hash1.equals(hash2)){
       fail("Hash did not match. Expecting: "+hash2.toString()+" Got: "+hash1.toString());
      }
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

}
