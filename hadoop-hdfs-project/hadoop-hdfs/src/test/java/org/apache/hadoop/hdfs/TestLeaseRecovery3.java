/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the cd Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * This class tests various cases during file creation.
 */
public class TestLeaseRecovery3 {

  @Test
  public void testFileOverwrite() throws Exception {

    Logger.getRootLogger().setLevel(Level.INFO);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    DistributedFileSystem newdfs = cluster.getFileSystem();

    try {

      // a client creats multiple files but fails and leaves the
      // files in under construction state
      newdfs.mkdirs(new Path("/dir"));
      for (int i = 0; i < 3; i++) {
        Path file = new Path("/dir/file" + i);
        FSDataOutputStream out = newdfs.create(file, (short) 1);
        //out.close();
      }
      //kill file client
      newdfs.getClient().getLeaseRenewer().interruptAndJoin();
      newdfs.getClient().abort();
      newdfs.close();


      // a new client overwrites these files.
      // This leades to exception like "Trying to access database . . .

      newdfs = (DistributedFileSystem) FileSystem
              .newInstance(cluster.getURI(), conf);
      for (int i = 0; i < 3; i++) {
        Path file = new Path("/dir/file" + i);
        FSDataOutputStream out = newdfs.create(file, (short) 1);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testFileOverwriteSimple() throws Exception {

    Logger.getRootLogger().setLevel(Level.INFO);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    DistributedFileSystem newdfs = cluster.getFileSystem();

    try {

      // a client creats multiple files but fails and leaves the
      // files in under construction state
      newdfs.mkdirs(new Path("/dir"));
      for (int i = 0; i < 3; i++) {
        Path file = new Path("/dir/file" + i);
        FSDataOutputStream out = newdfs.create(file, (short) 1);
//        out.close();
      }
      //kill file client
      newdfs.getClient().getLeaseRenewer().interruptAndJoin();
      newdfs.getClient().abort();
      newdfs.close();
      //three open leases

      newdfs = (DistributedFileSystem) FileSystem
              .newInstance(cluster.getURI(), conf);
      for (int i = 0; i < 3; i++) {
        Path file = new Path("/dir/file" + i);
        FSDataOutputStream out = newdfs.create(file, (short) 1);
        out.close();
      }
      newdfs.getClient().getLeaseRenewer().interruptAndJoin();
      newdfs.getClient().abort();
      newdfs.close();


    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      cluster.shutdown();
    }

  }
}
