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

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  /**
   * [HOPS-1590] When a directory is renamed such that the directory name is a
   * prefix of any of its immediate siblings then it may corrupt the leases
   * For example we have following namespace
   *    /a/file1
   *    /a/file2
   *    /aa/file1
   *    /aa/file2
   * Assume all the about files are open and the clients hold leases for these files. If you try
   * to rename `/a` to `/a-new`, the rename operation will the also rename the leases for
   * the files in the `/aa` directory. This is because the string comparison to
   * find active leases in a directory is broken.
   * @throws Exception
   */
  @Test
  public void testLeaseRecoveryWithRenameOp() throws Exception {

    Logger.getRootLogger().setLevel(Level.INFO);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    // set the hard limit to be 1 second
    long SHORT_LEASE_PERIOD = 1000L;
    long LONG_LEASE_PERIOD = 30 * 60 * SHORT_LEASE_PERIOD;
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    DistributedFileSystem newdfs = (DistributedFileSystem)cluster.getNewFileSystemInstance(0);

    try {

      // a client creats multiple files but fails and leaves the
      // files in under construction state
      newdfs.mkdirs(new Path("/dir1"));
      List<FSDataOutputStream> outs1 = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        Path file = new Path("/dir1/file" + i);
        FSDataOutputStream out = newdfs.create(file, (short) 1);
        outs1.add(out);
      }

      newdfs.mkdirs(new Path("/dir11"));
      DistributedFileSystem newdfs2 = (DistributedFileSystem)cluster.getNewFileSystemInstance(0);
      List<FSDataOutputStream> outs2 = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        Path file = new Path("/dir11/file" + i);
        FSDataOutputStream out = newdfs2.create(file, (short) 1);
        outs2.add(out);
      }

      //rename
      newdfs2.rename(new Path("/dir1" ),new Path("/dir111"));

      //kill file client
      newdfs.getClient().getLeaseRenewer().interruptAndJoin();
      newdfs.getClient().abort();
      newdfs.close();

      newdfs2.getClient().getLeaseRenewer().interruptAndJoin();
      newdfs2.getClient().abort();
      newdfs2.close();

      Thread.sleep(10*1000);

      //make sure that all files are closed.
      checkOpenFiles();

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      cluster.shutdown();
    }

  }

  public static int checkOpenFiles() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException, IOException {
                int count = 0;
                INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);

                List<INode> inodes = ida.allINodes();

                if (inodes != null && !inodes.isEmpty()) {
                  for (INode inode : inodes) {
                    if (inode.isUnderConstruction()) {
                      throw new IllegalStateException("INode id: "+inode.getId()+" is not closed.");
                    }
                  }
                }

                return count;
              }
            };
    return (Integer) subTreeLockChecker.handle();
  }
}
