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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;

public class TestSmallFilesCreation {

  private static final Log LOG =
          LogFactory.getLog(TestSmallFilesCreation.class);

  /**
   * Simple read and write test
   *
   * @throws IOException
   */
  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";


      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);

      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Write large file and make sure that it is stored on the datanodes
   *
   * @throws IOException
   */
  @Test
  public void TestWriteLargeFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int FILE_SIZE = MAX_SMALL_FILE_SIZE + 1;

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME, FILE_SIZE);

      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * if the file is small but the client calls flush method before the
   * close operation. The file will be flushed to datanodes rather than the database. This is because the
   * final size of the file is not known before the file is closed.
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHflush() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeData(out, 0, INMEMORY_BUCKET_SIZE);
      out.flush();
      out.hflush();
      out.close();


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * if the file is small but the client calls sync ethod before the
   * close operation the save the file on the datanodes. This is because the
   * final size of the file is not known before the file is closed
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHsync() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeData(out, 0, INMEMORY_BUCKET_SIZE);
      out.hsync();
      out.close();

      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * delete file stored in the database
   *
   * @throws IOException
   */
  @Test
  public void TestDeleteSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      dfs.delete(new Path(FILE_NAME1));
      dfs.delete(new Path(FILE_NAME2));
      dfs.delete(new Path(FILE_NAME3));
      dfs.delete(new Path(FILE_NAME4));

      assertTrue("Expecting 0 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  test mv smallfile smallfile_new
   */
  @Test
  public void TestRenameSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME1 + "1"));
      dfs.rename(new Path(FILE_NAME2), new Path(FILE_NAME2 + "1"));
      dfs.rename(new Path(FILE_NAME3), new Path(FILE_NAME3 + "1"));
      dfs.rename(new Path(FILE_NAME4), new Path(FILE_NAME4 + "1"));

      verifyFile(dfs, FILE_NAME1 + "1", INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2 + "1", ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3 + "1", ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME4 + "1", MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  replace in-memory file with another in-memory file
  replace in-memory file with another on-disk file
  replace on-disk file with another on-disk file
   */
  @Test
  public void TestRenameSmallFiles2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      // replace in-memory file with an other in-memory file
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 2", countInMemoryDBFiles() == 2);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, INMEMORY_BUCKET_SIZE);

      // replace in-memory file with on-disk file
      // create on-disk small file
      writeFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);

      //replace on disk file with another ondsik file
      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 2", countAllOnDiskDBFiles() == 2);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*

   */
  @Test
  public void TestRenameSmallFiles3() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      FSNamesystem namesystem = cluster.getNamesystem();

      /*create smallfile1
      create largefile1
      mv smallfile1 largefile1
      test largefile is deleted*/
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE + 1);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 1 block but found: " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 0 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 0);
      dfs.delete(new Path(FILE_NAME2));
      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      /*create smallfile1
      create largefile1
      mv largefile1 smallfile1
      test smallfile is deleted*/
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE + 1);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 1 block but found: " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.rename(new Path(FILE_NAME2), new Path(FILE_NAME1), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 1 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.delete(new Path(FILE_NAME1));
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 0 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Delete a directory that has both small and large files
 */
  @Test
  public void TestDelete1() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      for (int i = 0; i < 5; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, INMEMORY_BUCKET_SIZE);
      }

      for (int i = 5; i < 10; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, ONDISK_SMALL_BUCKET_SIZE);
      }

      for (int i = 10; i < 15; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, ONDISK_MEDIUM_BUCKET_SIZE);
      }

      for (int i = 15; i < 20; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, MAX_SMALL_FILE_SIZE);
      }

      for (int i = 20; i < 25; i++) {
        writeFile(dfs, "/dir/file2" + i, MAX_SMALL_FILE_SIZE + 1);
      }

      assertTrue("Count of db file should be 10", countDBFiles() == 20);
      assertTrue("Expecting 5 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 5);

      dfs.delete(new Path("/dir"), true);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 0 block but foudn " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  appending large amount to data to a file stored in the database.
  the file should migrate to the datanodes
 */
  @Test
  public void TestAppendMigrateToDataNodes() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);

      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_BUCKET_SIZE, ONDISK_SMALL_BUCKET_SIZE - INMEMORY_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskSmallDBFiles() == 1);


      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_SMALL_BUCKET_SIZE, ONDISK_MEDIUM_BUCKET_SIZE - ONDISK_SMALL_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_MEDIUM_BUCKET_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_MEDIUM_BUCKET_SIZE, MAX_SMALL_FILE_SIZE - ONDISK_MEDIUM_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskMediumDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, MAX_SMALL_FILE_SIZE, 1);
      out.close();
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 1 block but foudn " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE + 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  during append if sync or flush is called then store the file on the datanodes
  */
  @Test
  public void TestAppendSync() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_BUCKET_SIZE, 1);
      out.hflush();
      writeData(out, INMEMORY_BUCKET_SIZE + 1, 1);
      out.close();

      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE + 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
   Test appending to a file stored in the database
  */
  @Test
  public void TestAppend() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);

      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);

      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, MAX_SMALL_FILE_SIZE, 1024);
      out.close();

      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE + 1024);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Test overwrite
  overwrite in-memory file with another in-memory file
  overwrite on-disk file with another on-disk file
  overwrite in-memory file with on-disk file and visa versa
 */
  @Test
  public void TestOverwrite() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      // overwrite in-memory file with another in-memory file
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, ONDISK_MEDIUM_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_MEDIUM_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskSmallDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_MEDIUM_BUCKET_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME2, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 2", countAllOnDiskDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      assertTrue("Count of db file should be 0", countDBFiles() == 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Test overwrite

 */
  @Test
  public void TestOverwrite2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Expecting 0 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      assertTrue("Expecting 0 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * write small file using hdfs like client.
   * the file should be stored on the DNs
   *
   * @throws IOException
   */
  @Test
  public void TestHdfsCompatibility1() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      conf.setBoolean(DFSConfigKeys.DFS_FORCE_CLIENT_TO_WRITE_SMALL_FILES_TO_DISK_KEY, true);

      FileSystem hdfsClient = FileSystem.newInstance(conf);

      writeFile(hdfsClient, FILE_NAME2, INMEMORY_BUCKET_SIZE);

      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);

      verifyFile(hdfsClient, FILE_NAME1, INMEMORY_BUCKET_SIZE);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Testing append using HDFS client.
  When a HDFS client tries to append to small file that is stored in the database then
  the file is first move to the datanodes and then the append is performed. HopsFS is
  responsible for seemlessly moving the small files to the datanodes.
   */
  @Test
  public void TestHdfsCompatibility2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      conf.setBoolean(DFSConfigKeys.DFS_FORCE_CLIENT_TO_WRITE_SMALL_FILES_TO_DISK_KEY, true);
      FileSystem hdfsClient = FileSystem.newInstance(conf);

      FSDataOutputStream out = hdfsClient.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_BUCKET_SIZE, ONDISK_SMALL_BUCKET_SIZE - INMEMORY_BUCKET_SIZE);
      out.close();

      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      verifyFile(hdfsClient, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void TestSmallFilesWithNoDataNodes() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestSmallFilesReplication() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      final int dbReplication = 2;
      conf.setInt(DFSConfigKeys.DFS_DB_REPLICATION_FACTOR, dbReplication);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();

      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);

      dfs.setReplication(new Path(FILE_NAME1), (short) 10);
      
      assertTrue("Replication factor for small files should be always set to " +
          "be db replication factor",
          dfs.getFileStatus(new Path(FILE_NAME1)).getReplication() == dbReplication);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestSmallFileByteBufferReader() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);

      FSDataInputStream read = dfs.open(new Path(FILE_NAME1));
      ByteBuffer buf = ByteBuffer.allocate(MAX_SMALL_FILE_SIZE);
      while (buf.hasRemaining()) {
        int readCount = read.read(buf);
        if (readCount == -1) {
          // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
          // that has more remaining than the amount of data in the stream.
          throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

  }

  @Test
  public void TestSmallFileListing() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final String BASE_DIR = "/dir";
      final String FILE_NAME1 = BASE_DIR + "/TEST-FLIE1";
      final String FILE_NAME2 = BASE_DIR + "/TEST-FLIE2";
      final String FILE_NAME3 = BASE_DIR + "/TEST-FLIE3";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, MAX_SMALL_FILE_SIZE);

      verifyFile(dfs, FILE_NAME3, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);


      BlockLocation[] loc = dfs.getFileBlockLocations(new Path(FILE_NAME1), 0, Long.MAX_VALUE);
      loc = dfs.getFileBlockLocations(new Path(FILE_NAME2), 0, Long.MAX_VALUE);
      loc = dfs.getFileBlockLocations(new Path(FILE_NAME3), 0, Long.MAX_VALUE);

      LocatedFileStatus fs = null;
      RemoteIterator<LocatedFileStatus> itr = null;

      assertTrue("Expecting 3 files", dfs.listStatus(new Path(BASE_DIR)).length == 3);
      FileStatus[] fss = dfs.listStatus(new Path(FILE_NAME1));
      assertTrue("Small file size did not match. Expecting " + ONDISK_SMALL_BUCKET_SIZE + " Got " + fss[0].getLen(),
              fss[0].getLen() == ONDISK_SMALL_BUCKET_SIZE);


      fss = dfs.listStatus(new Path(FILE_NAME2));
      assertTrue("Small file size did not match. Expecting " + ONDISK_MEDIUM_BUCKET_SIZE + " Got " + fss[0].getLen(),
              fss[0].getLen() == ONDISK_MEDIUM_BUCKET_SIZE);

      fss = dfs.listStatus(new Path(FILE_NAME3));
      assertTrue("Small file size did not match. Expecting " + MAX_SMALL_FILE_SIZE + " Got " + fss[0].getLen(),
              fss[0].getLen() == MAX_SMALL_FILE_SIZE);


      itr = dfs.listFiles(new Path(BASE_DIR), true);
      while (itr.hasNext()) {
        fs = itr.next();
        System.out.println("File: " + fs.getPath() + ", Size: " + fs.getLen() + ", Loc: " + Arrays.toString(fs.getBlockLocations()));
      }

      itr = dfs.listFiles(new Path(FILE_NAME1), true);
      fs = itr.next();
      assertTrue("Small file size did not match. Expecting " + ONDISK_SMALL_BUCKET_SIZE + " Got " + fs.getLen(),
              fs.getLen() == ONDISK_SMALL_BUCKET_SIZE);

      itr = dfs.listFiles(new Path(FILE_NAME2), true);
      fs = itr.next();
      assertTrue("Small file size did not match. Expecting " + ONDISK_MEDIUM_BUCKET_SIZE + " Got " + fs.getLen(),
              fs.getLen() == ONDISK_MEDIUM_BUCKET_SIZE);

      itr = dfs.listFiles(new Path(FILE_NAME3), true);
      fs = itr.next();
      assertTrue("Small file size did not match. Expecting " + MAX_SMALL_FILE_SIZE + " Got " + fs.getLen(),
              fs.getLen() == MAX_SMALL_FILE_SIZE);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestReconfiguration1() throws IOException {
    int INMEMORY_BUCKET_SIZE = 0;
    final String FILE_NAME0 = "/dir/TEST-FLIE0";
    final String FILE_NAME1 = "/dir/TEST-FLIE1";
    final String FILE_NAME2 = "/dir/TEST-FLIE2";
    final String FILE_NAME3 = "/dir/TEST-FLIE3";
    final int firstConfig = 64 * 1024;
    final int secondConfig = 64 * 1024 * 2;
    final int thirdConfig = 32 * 1024;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, firstConfig); // 64 KB files
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      int maxSmallFile = FSNamesystem.getMaxSmallFileSize();
      assertTrue(maxSmallFile == firstConfig);

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs, FILE_NAME0, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME0, INMEMORY_BUCKET_SIZE);

      writeFile(dfs, FILE_NAME1, maxSmallFile);
      verifyFile(dfs, FILE_NAME1, maxSmallFile);

      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }

      try {
        Configuration conf = new HdfsConfiguration();
        final int BLOCK_SIZE = 1024 * 1024;
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, secondConfig);
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(false).build();
        cluster.waitActive();

        DistributedFileSystem dfs = cluster.getFileSystem();

        verifyFile(dfs, FILE_NAME1, firstConfig);

        int maxSmallFile = FSNamesystem.getMaxSmallFileSize();
        assertTrue(maxSmallFile == secondConfig);

        writeFile(dfs, FILE_NAME2, secondConfig);
        verifyFile(dfs, FILE_NAME2, secondConfig);

        assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
        assertTrue("Expecting 2 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 2);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
        assertTrue("Expecting 2 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 2);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      } finally {
        if (cluster != null) {
          cluster.shutdown();
        }
      }


      try {
        Configuration conf = new HdfsConfiguration();
        final int BLOCK_SIZE = 1024 * 1024;
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, thirdConfig);
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(false).build();
        cluster.waitActive();

        DistributedFileSystem dfs = cluster.getFileSystem();

        verifyFile(dfs, FILE_NAME1, firstConfig);
        verifyFile(dfs, FILE_NAME2, secondConfig);

        int maxSmallFile = FSNamesystem.getMaxSmallFileSize();
        assertTrue(maxSmallFile == thirdConfig);

        writeFile(dfs, FILE_NAME3, thirdConfig);
        verifyFile(dfs, FILE_NAME3, thirdConfig);

        assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
        assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
        assertTrue("Expecting 3 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 3);

        //all files except FILE_NAME0 should move to DN after append
        append(dfs, FILE_NAME0, INMEMORY_BUCKET_SIZE, 1);
        verifyFile(dfs, FILE_NAME0, INMEMORY_BUCKET_SIZE + 1);

        append(dfs, FILE_NAME1, firstConfig, 1);
        verifyFile(dfs, FILE_NAME1, firstConfig + 1);

        append(dfs, FILE_NAME2, secondConfig, 1);
        verifyFile(dfs, FILE_NAME2, secondConfig + 1);

        append(dfs, FILE_NAME3, thirdConfig, 1);
        verifyFile(dfs, FILE_NAME3, thirdConfig + 1);

        assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
        assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
        assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

        dfs.delete(new Path(FILE_NAME0));
        dfs.delete(new Path(FILE_NAME1));
        dfs.delete(new Path(FILE_NAME2));
        dfs.delete(new Path(FILE_NAME3));

        assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
        assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      } finally {
        if (cluster != null) {
          cluster.shutdown();
        }
      }
    }
  }

  void append(DistributedFileSystem dfs, String name, int existingSize, int newSize) throws IOException {
    FSDataOutputStream out = dfs.append(new Path(name));
    writeData(out, existingSize, newSize);
    out.close();
  }

  @Test
  public void TestConcat() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      Path paths[] = new Path[4];
      for(int i = 0; i < 4; i++){
        paths[i] = new Path("/dir/TEST-FLIE"+i);
      }

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      writeFile(dfs,  paths[0].toString(), INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, paths[0].toString(), INMEMORY_BUCKET_SIZE);
      writeFile(dfs,  paths[1].toString(), ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, paths[1].toString(), ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs,  paths[2].toString(), ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, paths[2].toString(), ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs,  paths[3].toString(), MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, paths[3].toString(), MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      //combine these files

      Path merged = new Path("/dir/merged");
      writeFile(dfs, merged.toString(), 0);
      try {
        dfs.concat(merged, paths);
      } catch (IOException e){
        if (!e.getMessage().contains("stored in DB")){
          throw e;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestTruncate() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024 * 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      Path file = new Path("/dir/file");

      writeFile(dfs,  file.toString(), MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, file.toString(), MAX_SMALL_FILE_SIZE);

      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      assert dfs.truncate(file, ONDISK_MEDIUM_BUCKET_SIZE) == true;
      verifyFile(dfs, file.toString(), ONDISK_MEDIUM_BUCKET_SIZE);

      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

      assert dfs.truncate(file, ONDISK_SMALL_BUCKET_SIZE) == true;
      verifyFile(dfs, file.toString(), ONDISK_SMALL_BUCKET_SIZE);

      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

      assert dfs.truncate(file, INMEMORY_BUCKET_SIZE) == true;
      verifyFile(dfs, file.toString(), INMEMORY_BUCKET_SIZE);

      Thread.sleep(1000);
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);
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
  public static void TestZLastTestCleanUp() throws IOException {
    String[] argv = {"-format", "-force"};
    ExitUtil.disableSystemExit();
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    UsersGroups.createSyncRow();
    try {
      NameNode.createNameNode(argv, conf);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitUtil.ExitException e) {
    }
  }
}
