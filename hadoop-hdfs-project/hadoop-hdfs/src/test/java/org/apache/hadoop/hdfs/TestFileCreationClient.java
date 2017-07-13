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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestFileCreationClient {
  static final String DIR =
      "/" + TestFileCreationClient.class.getSimpleName() + "/";

  {
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger()
        .setLevel(Level.ALL);
    ((Log4JLogger) InterDatanodeProtocol.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Test lease recovery Triggered by DFSClient.
   */
  @Test
  public void testClientTriggeredLeaseRecovery() throws Exception {
    final int REPLICATION = 3;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION).build();

    try {
      final FileSystem fs = cluster.getFileSystem();
      final Path dir = new Path("/wrwelkj");
      
      SlowWriter[] slowwriters = new SlowWriter[10];
      for (int i = 0; i < slowwriters.length; i++) {
        slowwriters[i] = new SlowWriter(fs, new Path(dir, "file" + i));
      }

      try {
        for (SlowWriter slowwriter : slowwriters) {
          slowwriter.start();
        }

        Thread.sleep(1000);                       // let writers get started

        //stop a datanode, it should have least recover.
        cluster.stopDataNode(AppendTestUtil.nextInt(REPLICATION));
        
        //let the slow writer writes a few more seconds
        System.out.println("Wait a few seconds");
        Thread.sleep(5000);
      } finally {
        for (SlowWriter slowwriter1 : slowwriters) {
          if (slowwriter1 != null) {
            slowwriter1.running = false;
            slowwriter1.interrupt();
          }
        }
        for (SlowWriter slowwriter : slowwriters) {
          if (slowwriter != null) {
            slowwriter.join();
          }
        }
      }

      //Verify the file
      System.out.println("Verify the file");
      for (SlowWriter slowwriter : slowwriters) {
        System.out.println(slowwriter.filepath + ": length=" +
            fs.getFileStatus(slowwriter.filepath).getLen());
        FSDataInputStream in = null;
        try {
          in = fs.open(slowwriter.filepath);
          for (int j = 0, x; (x = in.read()) != -1; j++) {
            assertEquals(j, x);
          }
        } finally {
          IOUtils.closeStream(in);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static class SlowWriter extends Thread {
    final FileSystem fs;
    final Path filepath;
    boolean running = true;
    
    SlowWriter(FileSystem fs, Path filepath) {
      super(SlowWriter.class.getSimpleName() + ":" + filepath);
      this.fs = fs;
      this.filepath = filepath;
    }

    @Override
    public void run() {
      FSDataOutputStream out = null;
      int i = 0;
      try {
        out = fs.create(filepath);
        for (; running; i++) {
          System.out.println(getName() + " writes " + i);
          out.write(i);
          out.hflush();
          sleep(100);
        }
      } catch (Exception e) {
        System.out.println(getName() + " dies: e=" + e);
      } finally {
        System.out.println(getName() + ": i=" + i);
        IOUtils.closeStream(out);
      }
    }
  }
}
