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
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestReplaceDatanodeOnFailure {
  static final Log LOG = AppendTestUtil.LOG;

  static final String DIR =
      "/" + TestReplaceDatanodeOnFailure.class.getSimpleName() + "/";
  static final short REPLICATION = 3;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";

  {
    ((Log4JLogger) DataTransferProtocol.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Test DEFAULT ReplaceDatanodeOnFailure policy.
   */
  @Test
  public void testDefaultPolicy() throws Exception {
    final ReplaceDatanodeOnFailure p = ReplaceDatanodeOnFailure.DEFAULT;

    final DatanodeInfo[] infos = new DatanodeInfo[5];
    final DatanodeInfo[][] datanodes = new DatanodeInfo[infos.length + 1][];
    datanodes[0] = new DatanodeInfo[0];
    for (int i = 0; i < infos.length; ) {
      infos[i] = DFSTestUtil.getLocalDatanodeInfo(50020 + i);
      i++;
      datanodes[i] = new DatanodeInfo[i];
      System.arraycopy(infos, 0, datanodes[i], 0, datanodes[i].length);
    }

    final boolean[] isAppend = {true, true, false, false};
    final boolean[] isHflushed = {true, false, true, false};

    for (short replication = 1; replication <= infos.length; replication++) {
      for (int nExistings = 0; nExistings < datanodes.length; nExistings++) {
        final DatanodeInfo[] existings = datanodes[nExistings];
        Assert.assertEquals(nExistings, existings.length);
  
        for (boolean anIsAppend : isAppend) {
          for (boolean anIsHflushed : isHflushed) {
            final int half = replication / 2;
            final boolean enoughReplica = replication <= nExistings;
            final boolean noReplica = nExistings == 0;
            final boolean replicationL3 = replication < 3;
            final boolean existingsLEhalf = nExistings <= half;
            final boolean isAH = anIsAppend || anIsHflushed;
      
            final boolean expected;
            if (enoughReplica || noReplica || replicationL3) {
              expected = false;
            } else {
              expected = isAH || existingsLEhalf;
            }
      
            final boolean computed =
                p.satisfy(replication, existings, anIsAppend, anIsHflushed);
            try {
              Assert.assertEquals(expected, computed);
            } catch (AssertionError e) {
              final String s =
                  "replication=" + replication + "\nnExistings =" + nExistings +
                      "\nisAppend   =" + anIsAppend + "\nisHflushed =" +
                      anIsHflushed;
              throw new RuntimeException(s, e);
            }
          }
        }
      }
    }
  }

  /**
   * Test replace datanode on failure.
   */
  @Test
  public void testReplaceDatanodeOnFailure() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    
    //always replace a datanode
    ReplaceDatanodeOnFailure.ALWAYS.write(conf);

    final String[] racks = new String[REPLICATION];
    Arrays.fill(racks, RACK0);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).racks(racks).numDataNodes(REPLICATION)
            .build();

    try {
      final DistributedFileSystem fs =
          (DistributedFileSystem) cluster.getFileSystem();
      final Path dir = new Path(DIR);
      
      final SlowWriter[] slowwriters = new SlowWriter[10];
      for (int i = 1; i <= slowwriters.length; i++) {
        //create slow writers in different speed
        slowwriters[i - 1] =
            new SlowWriter(fs, new Path(dir, "file" + i), i * 200L);
      }

      for (SlowWriter s : slowwriters) {
        s.start();
      }

      // Let slow writers write something.
      // Some of them are too slow and will be not yet started. 
      sleepSeconds(1);

      //start new datanodes
      cluster.startDataNodes(conf, 2, true, null, new String[]{RACK1, RACK1});
      //stop an old datanode
      cluster.stopDataNode(AppendTestUtil.nextInt(REPLICATION));
      
      //Let the slow writer writes a few more seconds
      //Everyone should have written something.
      sleepSeconds(5);

      //check replication and interrupt.
      for (SlowWriter s : slowwriters) {
        s.checkReplication();
        s.interruptRunning();
      }

      //close files
      for (SlowWriter s : slowwriters) {
        s.joinAndClose();
      }

      //Verify the file
      LOG.info("Verify the file");
      for (SlowWriter slowwriter : slowwriters) {
        LOG.info(slowwriter.filepath + ": length=" +
            fs.getFileStatus(slowwriter.filepath).getLen());
        FSDataInputStream in = null;
        try {
          in = fs.open(slowwriter.filepath);
          for (int j = 0, x; (x = in.read()) != -1; j++) {
            Assert.assertEquals(j, x);
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

  static void sleepSeconds(final int waittime) throws InterruptedException {
    LOG.info("Wait " + waittime + " seconds");
    Thread.sleep(waittime * 1000L);
  }

  static class SlowWriter extends Thread {
    final Path filepath;
    final HdfsDataOutputStream out;
    final long sleepms;
    private volatile boolean running = true;
    
    SlowWriter(DistributedFileSystem fs, Path filepath, final long sleepms)
        throws IOException {
      super(SlowWriter.class.getSimpleName() + ":" + filepath);
      this.filepath = filepath;
      this.out = (HdfsDataOutputStream) fs.create(filepath, REPLICATION);
      this.sleepms = sleepms;
    }

    @Override
    public void run() {
      int i = 0;
      try {
        sleep(sleepms);
        for (; running; i++) {
          LOG.info(getName() + " writes " + i);
          out.write(i);
          out.hflush();
          sleep(sleepms);
        }
      } catch (InterruptedException e) {
        LOG.info(getName() + " interrupted:" + e);
      } catch (IOException e) {
        throw new RuntimeException(getName(), e);
      } finally {
        LOG.info(getName() + " terminated: i=" + i);
      }
    }

    void interruptRunning() {
      running = false;
      interrupt();
    }

    void joinAndClose() throws InterruptedException {
      LOG.info(getName() + " join and close");
      join();
      IOUtils.closeStream(out);
    }

    void checkReplication() throws IOException {
      Assert.assertEquals(REPLICATION, out.getCurrentBlockReplication());
    }
  }

  @Test
  public void testAppend() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final short REPLICATION = (short) 3;
    
    Assert.assertEquals(ReplaceDatanodeOnFailure.DEFAULT,
        ReplaceDatanodeOnFailure.get(conf));

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

    try {
      final DistributedFileSystem fs =
          (DistributedFileSystem) cluster.getFileSystem();
      final Path f = new Path(DIR, "testAppend");
      
      {
        LOG.info("create an empty file " + f);
        fs.create(f, REPLICATION).close();
        final FileStatus status = fs.getFileStatus(f);
        Assert.assertEquals(REPLICATION, status.getReplication());
        Assert.assertEquals(0L, status.getLen());
      }
      
      
      final byte[] bytes = new byte[1000];
      {
        LOG.info("append " + bytes.length + " bytes to " + f);
        final FSDataOutputStream out = fs.append(f);
        out.write(bytes);
        out.close();

        final FileStatus status = fs.getFileStatus(f);
        Assert.assertEquals(REPLICATION, status.getReplication());
        Assert.assertEquals(bytes.length, status.getLen());
      }

      {
        LOG.info("append another " + bytes.length + " bytes to " + f);
        try {
          final FSDataOutputStream out = fs.append(f);
          out.write(bytes);
          out.close();

          Assert.fail();
        } catch (IOException ioe) {
          LOG.info("This exception is expected", ioe);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
