/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.StorageType.*;
import static org.junit.Assert.assertTrue;

public class TestFileCreateWithPolicies {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;

  static final int numDatanodes = 4;
  static final int storagesPerDN = 3;
  static final StorageType[][] storageTypes = {
      {DISK, DISK, DISK}, // sid 0,1,2
      {DISK, DISK, SSD}, // sid 3,4,5
      {DISK, DISK, SSD}, // sid 6,7,8
      {SSD, SSD, SSD}, // sid 9,10,11
  };

  static final Log LOG = LogFactory.getLog(TestFileCreateWithPolicies.class);

  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl) throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");

    FSDataOutputStream stm = fileSys.create(name, true, 4096, (short) repl, blockSize);

    return stm;
  }

  public static HdfsDataOutputStream create(DistributedFileSystem dfs,
      Path name, int repl) throws IOException {
    return (HdfsDataOutputStream) createFile(dfs, name, repl);
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  public static void writeFile(FSDataOutputStream stm, int size)
      throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }

  @Test
  public void testSimple() throws IOException {
    Configuration conf = new HdfsConfiguration();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .storagesPerDatanode(storagesPerDN)
        .storageTypes(storageTypes)
        .build();

    DistributedFileSystem dfs = cluster.getFileSystem();

    String[] policyNames = {
        HdfsConstants.ALLSSD_STORAGE_POLICY_NAME,
        HdfsConstants.WARM_STORAGE_POLICY_NAME,
        HdfsConstants.ONESSD_STORAGE_POLICY_NAME
    };

    LOG.debug("arrived here");

    try {
      for(int i = 0; i < 10; i++) {
        FSNamesystem.LOG.debug("Starting file iteration " + i);
        // create a new file in home directory. Do not close it.
        Path file = new Path("/file_" + i + ".dat");
        FSDataOutputStream stm = createFile(dfs, file, 3);

        // Set the policy to all ssd
        dfs.setReplication(file, (short) 4);
        dfs.setStoragePolicy(file, policyNames[i % policyNames.length]);

        // verify that file exists in FS namespace
        assertTrue(file + " should be a file", dfs.getFileStatus(file).isFile());

        // write to file
        byte[] buffer = AppendTestUtil.randomBytes(seed, fileSize);
        stm.write(buffer, 0, fileSize);

        stm.close();

        FSNamesystem.LOG.debug("Finished file iteration " + i);
      }
    } finally {
      cluster.shutdown();
    }
  }
}




























