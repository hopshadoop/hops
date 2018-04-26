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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


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
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
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

}
