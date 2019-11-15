/*
 * Copyright (C) 2019 LogicalClocks.
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
package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.security.UsersGroups;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;
import static org.junit.Assert.*;

public class TestSmallFilesDiskFull {

  private static final Log LOG =
          LogFactory.getLog(TestSmallFilesDiskFull.class);

  /**
   * The small files should spill on datanodes disks when the database is full.
   * In the metadata-dal-impl project the default size for the disk data is
   * set to 2G. This test writes small files of total size of 2.1G. It checks
   * to make sure that some file have spilled to the datanodes disks.
   * This test may take long time on slow machines
   * <p>
   * NOTE: This test should run in the end. It calls truncate to free up the
   * extents using the truncate command that changes the schema. Subsequent
   * tests will fails.
   *
   * @throws IOException
   */
  @Test(timeout = 900000) // 30 mins
  public void TestWriteMaxSpillToDN() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int MAX_SMALL_FILE_SIZE = 1024 * 1024 * 10;  //default allocated disk
      // space is 128 MB. ~ 12 small files of size 10 MB can fit there
      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024 * 128);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      int count = 13;
      int i = 0;
      try {
        for (i = 0; i < count; i++) {
          writeFile(dfs, "/dir/file" + i, MAX_SMALL_FILE_SIZE);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed after creating " + i + " files");
      }

      try {
        for (i = 0; i < count; i++) {
          verifyFile(dfs, "/dir/file" + i, MAX_SMALL_FILE_SIZE);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to verify files");
      }

      int dbFiles = countDBFiles();
      assertTrue("Count of db file should be more than 0 and less than " + count,
              dbFiles > 0 && dbFiles < count);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @AfterClass
  /** force format the database to release the extents
   *
   */
  public static void Cleanup() throws IOException {
    TestSmallFilesCreation.TestZLastTestCleanUp();
  }
}
