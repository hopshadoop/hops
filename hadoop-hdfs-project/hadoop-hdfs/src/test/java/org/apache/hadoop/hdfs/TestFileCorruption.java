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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A JUnit test for corrupted file handling.
 */
public class TestFileCorruption {
  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger()
        .setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
  }

  static Log LOG = ((Log4JLogger) NameNode.stateChangeLog);

  /**
   * check if DFS can handle corrupted blocks properly
   */
  @Test
  public void testFileCorruption() throws Exception {
    MiniDFSCluster cluster = null;
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFileCorruption").
        setNumFiles(20).build();
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      // Now deliberately remove the blocks
      File storageDir = cluster.getInstanceStorageDir(2, 0);
      String bpid = cluster.getNamesystem().getBlockPoolId();
      File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir",
          (blocks != null) && (blocks.length > 0));
      for (File block : blocks) {
        if (!block.getName().startsWith("blk_")) {
          continue;
        }
        System.out
            .println("Deliberately removing file " + block.getName());
        assertTrue("Cannot remove file.", block.delete());
      }
      assertTrue("Corrupted replicas not handled properly.",
          util.checkFiles(fs, "/srcdat"));
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * check if local FS can handle corrupted blocks properly
   */
  @Test
  public void testLocalFileCorruption() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Path file = new Path(System.getProperty("test.build.data"), "corruptFile");
    FileSystem fs = FileSystem.getLocal(conf);
    DataOutputStream dos = fs.create(file);
    dos.writeBytes("original bytes");
    dos.close();
    // Now deliberately corrupt the file
    dos = new DataOutputStream(new FileOutputStream(file.toString()));
    dos.writeBytes("corruption");
    dos.close();
    // Now attempt to read the file
    DataInputStream dis = fs.open(file, 512);
    try {
      System.out.println("A ChecksumException is expected to be logged.");
      dis.readByte();
    } catch (ChecksumException ignore) {
      //expect this exception but let any NPE get thrown
    }
    fs.delete(file, true);
  }
  
  /**
   * Test the case that a replica is reported corrupt while it is not
   * in blocksMap. Make sure that ArrayIndexOutOfBounds does not thrown.
   * See Hadoop-4351.
   * <p/>
   * TODO HOPS This test fails as it tries to remove a non-existing replica.
   * Calling findAndMarkBlockAsCorrupt from a DataNode that does not store
   * any replica for this specific block will lead to a tuple did not exist
   * exception. The reason for this is that BlockManager.removeStoredBlock
   * is called with a node that does not store a replica and hence the delete
   * will not be able to succeed during commit.
   */
  @Test
  public void testArrayOutOfBoundsException() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      final Path FILE_PATH = new Path("/tmp.txt");
      final long FILE_LEN = 1L;
      DFSTestUtil.createFile(fs, FILE_PATH, FILE_LEN, (short) 2, 1L);
      
      // get the block
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      File storageDir = cluster.getInstanceStorageDir(0, 0);
      File dataDir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      ExtendedBlock blk = getBlock(bpid, dataDir);
      if (blk == null) {
        storageDir = cluster.getInstanceStorageDir(0, 1);
        dataDir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
        blk = getBlock(bpid, dataDir);
      }
      assertFalse(blk == null);

      // start a third datanode
      cluster.startDataNodes(conf, 1, true, null, null);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);
      DataNode dataNode = datanodes.get(2);
      
      // report corrupted block by the third datanode
      DatanodeRegistration dnR = DataNodeTestUtils
          .getDNRegistrationForBP(dataNode, blk.getBlockPoolId());
      FSNamesystem ns = cluster.getNamesystem();
      cluster.getNamesystem().getBlockManager()
          .findAndMarkBlockAsCorrupt(blk, new DatanodeInfo(dnR), "TEST");

      // open the file
      fs.open(FILE_PATH);
      
      //clean up
      fs.delete(FILE_PATH, false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    
  }
  
  private ExtendedBlock getBlock(String bpid, File dataDir) {
    assertTrue("data directory does not exist", dataDir.exists());
    File[] blocks = dataDir.listFiles();
    assertTrue("Blocks do not exist in dataDir",
        (blocks != null) && (blocks.length > 0));

    int idx = 0;
    String blockFileName = null;
    for (; idx < blocks.length; idx++) {
      blockFileName = blocks[idx].getName();
      if (blockFileName.startsWith("blk_") &&
          !blockFileName.endsWith(".meta")) {
        break;
      }
    }
    if (blockFileName == null) {
      return null;
    }
    long blockId = Long.parseLong(blockFileName.substring("blk_".length()));
    long blockTimeStamp = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    for (idx = 0; idx < blocks.length; idx++) {
      String fileName = blocks[idx].getName();
      if (fileName.startsWith(blockFileName) && fileName.endsWith(".meta")) {
        int startIndex = blockFileName.length() + 1;
        int endIndex = fileName.length() - ".meta".length();
        blockTimeStamp =
            Long.parseLong(fileName.substring(startIndex, endIndex));
        break;
      }
    }
    return new ExtendedBlock(bpid, blockId, blocks[idx].length(),
        blockTimeStamp);
  }
}
