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
package org.apache.hadoop.hdfs.server.namenode.ha;

import io.hops.metadata.HdfsVariables;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.SafeModeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;


/**
 * Tests that exercise safemode in an HA cluster.
 */
public class TestHASafeMode {
  private static final Log LOG = LogFactory.getLog(TestHASafeMode.class);
  private static final int BLOCK_SIZE = 1024;
  private NameNode nn0;
  private NameNode nn1;
  private FileSystem fs;
  private MiniDFSCluster cluster;
  
  static {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }
  
  @Before
  public void setupCluster() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(2))
      .numDataNodes(3)
      .waitSafeMode(false)
      .build();
    cluster.waitActive();
    
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);

    fs = cluster.getFileSystem(0);
    
    cluster.waitActive();
  }
  
  @After
  public void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Make sure the client retries when the active NN is in safemode
   */
  @Test (timeout=300000)
  public void testClientRetrySafeMode() throws Exception {
    final Map<Path, Boolean> results = Collections
        .synchronizedMap(new HashMap<Path, Boolean>());
    final Path test = new Path("/test");
    // let nn0 enter safemode
    NameNodeAdapter.enterSafeMode(nn0, false);
    NameNodeAdapter.enterSafeMode(nn1, false);
    SafeModeInfo safeMode = nn0.getNamesystem().getSafeModeInfoForTests();
    Whitebox.setInternalState(safeMode, "extension", Integer.valueOf(30000));
    HdfsVariables.setSafeModeInfo(safeMode, safeMode.getReached());
    LOG.info("enter safemode");
    Thread testThread = new Thread() {
      @Override
      public void run() {
        try {
          boolean mkdir = fs.mkdirs(test);
          LOG.info("mkdir finished, result is " + mkdir);
          synchronized (TestHASafeMode.this) {
            results.put(test, mkdir);
            TestHASafeMode.this.notifyAll();
          }
        } catch (Exception e) {
          LOG.info("Got Exception while calling mkdir", e);
        }
      }
    };
    testThread.setName("test");
    testThread.start();
    
    // make sure the client's call has actually been handled by the active NN
    assertFalse("The directory should not be created while NN in safemode",
        fs.exists(test));
    
    Thread.sleep(1000);
    // let nn0 leave safemode
    NameNodeAdapter.leaveSafeMode(nn0);
    LOG.info("leave safemode");
    
    synchronized (this) {
      while (!results.containsKey(test)) {
        this.wait();
      }
      assertTrue(results.get(test));
    }
  }
  
  

  /**
   * Print a big banner in the test log to make debug easier.
   */
  static void banner(String string) {
    LOG.info("\n\n\n\n================================================\n" +
        string + "\n" +
        "==================================================\n\n");
  }
  
  /** Test NN crash and client crash/stuck immediately after block allocation */
  @Test(timeout = 100000)
  public void testOpenFileWhenNNAndClientCrashAfterAddBlock() throws Exception {
    cluster.getConfiguration(0).set(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "1.0f");
    String testData = "testData";
    // to make sure we write the full block before creating dummy block at NN.
    cluster.getConfiguration(0).setInt("io.bytes.per.checksum",
        testData.length());
    cluster.getConfiguration(1).setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, //"io.bytes.per.checksum",
        testData.length());
    cluster.restartNameNode(0);
    cluster.restartNameNode(1);
    try {
      cluster.waitActive();
//      cluster.transitionToActive(0);
//      cluster.transitionToStandby(1);
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getNewFileSystemInstance(0);
      String pathString = "/tmp1.txt";
      Path filePath = new Path(pathString);
      FSDataOutputStream create = dfs.create(filePath,
          FsPermission.getDefault(), true, 1024, (short) 3, testData.length(),
          null);
      create.write(testData.getBytes());
      create.hflush();
      long fileId = ((DFSOutputStream) create.
          getWrappedStream()).getFileId();
      FileStatus fileStatus = dfs.getFileStatus(filePath);
      DFSClient client = DFSClientAdapter.getClient(dfs);
      // add one dummy block at NN, but not write to DataNode
      ExtendedBlock previousBlock =
          DFSClientAdapter.getPreviousBlock(client, fileId);
      DFSClientAdapter.getNamenode(client).addBlock(
          pathString,
          client.getClientName(),
          new ExtendedBlock(previousBlock),
          new DatanodeInfo[0],
          DFSClientAdapter.getFileId((DFSOutputStream) create
              .getWrappedStream()), null);
      cluster.restartNameNode(0, true);
      cluster.restartNameNode(1, true);
      cluster.restartDataNode(0);
      cluster.waitActive();
      // let the block reports be processed.
      Thread.sleep(2000);
      FSDataInputStream is = dfs.open(filePath);
      is.close();
      dfs.recoverLease(filePath);// initiate recovery
      //in hops the expected block are stored in DB so the new NN should get the info
      //even if one NN crash. As a result it should eventually complete the blocks on the DN.
      for(int i=0; i<10; i++){
        if(dfs.recoverLease(filePath)){
          break;
        }
        Thread.sleep(1000);
      }
      assertTrue("Recovery also should be success", dfs.recoverLease(filePath));
    } finally {
      cluster.shutdown();
    }
  }

}
