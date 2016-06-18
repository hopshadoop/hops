/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.log4j.Level;
import org.junit.Ignore;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;

/**
 * This test case tests the high availability of reads when ever reader
 * namenodes goes down and even if all the readers go down (except the writer)
 * It also tests if the system can be made highly available for reads even if
 * the writer is down
 */
@Ignore(value = "The design of this test needs to be reconsidered. " +
    "It fails most of the times because of race conditions.")
public class TestHARead extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestHARead.class);

  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger()
        .setLevel(Level.ALL);
  }

  final Path dir = new Path("/test/HA/");

  int list(FileSystem fs) throws IOException {
    int totalFiles = 0;
    for (FileStatus s : fs.listStatus(dir)) {
      FileSystem.LOG.info("" + s.getPath());
      totalFiles++;
    }
    return totalFiles;
  }

  static void createFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("something");
    a_out.close();
  }

  @Test
  public void testHighAvailability() throws IOException {
    Configuration conf = new HdfsConfiguration();

    // Create cluster with 3 readers and 1 writer
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(4)).numDataNodes(2)
        .format(true).build();
    cluster.waitActive();

    try {

      // Get the filesystem and create a directory
      FileSystem fs = cluster.getFileSystem(0);

      // Write operation should work since we have one writer
      assertTrue(fs.mkdirs(dir));

      // Write operation - Create a file and write something to it
      Path file1 = new Path(dir, "file1");
      createFile(fs, file1);

      // Read operation - The file should exist.
      assertTrue(fs.exists(file1));

      // Read operation - List files in this directory
      assertEquals(1, list(fs));

      // Read operation - Get file status
      FileStatus fileStatus = fs.listStatus(dir)[0];

      // Read operation - Get block locations
      assertNotSame(0, fs.getFileBlockLocations(file1, 0, 1).length);

      // Now we kill all namenodes except the last two
      cluster.getNameNode(0).stop();
      cluster.getNameNode(1).stop();

      // Now lets read again - These operations should be possible
      assertTrue(fs.exists(file1));

      // Writer operation - concat files
      Path file2 = new Path(dir, "file2");
      createFile(fs, file2);
      assertTrue(fs.exists(file2));
      Path file3 = new Path(dir, "file3");
      createFile(fs, file3);
      assertTrue(fs.exists(file3));
      Path file4 = new Path(dir, "file4");

      // Read operation - list files (3 files created now under this directory)
      assertEquals(3, list(fs));

      // Write operation - rename
      // [S] commented out because rename is not yet supported
      // ((DistributedFileSystem) fs).rename(file1, file4);

      // Kill another namenode
      cluster.getNameNode(2).stop();

      // Read operation - File status
      fs.getFileStatus(file2);

      // Write operation - Delete
      assertTrue(fs.delete(dir, true));

    } catch (IOException ex) {
      // In case we have any connectivity issues here, there is a problem
      // All connectivitiy issues are handled in the above piece of code
      LOG.error(ex);
      ex.printStackTrace();
      assertFalse("Cannot be any connectivity issues",
          ex instanceof ConnectException);
      fail();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}