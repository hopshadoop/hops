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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdfs.TestSmallFilesCreation.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHopsFSCloudFileCreation {

  static final Log LOG = LogFactory.getLog(TestHopsFSCloudFileCreation.class);
  @Rule
  public TestName name = new TestName();


  /**
   * Simple read and write test
   *
   * @throws IOException
   */
  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int FILESIZE = 2 * BLKSIZE;

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, FILE_NAME1, FILESIZE);
      verifyFile(dfs, FILE_NAME1, FILESIZE);

      CloudTestHelper.checkMetaDataMatch(conf);

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
  public void TestEnv() {
    String path = System.getenv("AWS_CREDENTIAL_PROFILES_FILE");

    assertTrue("Unit test could not read AWS credential file", path != null);
  }

  @Test
  public void TestListing() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_FILES = 1;
      final int NUM_DN = 5;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < NUM_FILES; i++) {
        writeFile(dfs, "/dir/file" + i, FILESIZE);
      }

      CloudTestHelper.checkMetaDataMatch(conf);

      FileStatus[] filesStatus = dfs.listStatus(new Path("/dir"));

      assert filesStatus.length == NUM_FILES;

      RemoteIterator<LocatedFileStatus> locatedFilesStatus = dfs.listLocatedStatus(new Path("/dir"));

      System.out.println("");

      int count = 0;
      while (locatedFilesStatus.hasNext()) {
        LocatedFileStatus locFileStatus = locatedFilesStatus.next();

        assert locFileStatus.getBlockLocations().length == BLK_PER_FILE;

        count += locFileStatus.getBlockLocations().length;
      }

      assert count == NUM_FILES * BLK_PER_FILE;


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
  public void TestDeleteFile() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, "/dir/file", FILESIZE);

      CloudTestHelper.checkMetaDataMatch(conf);

      dfs.delete(new Path("/dir/file"), true);

      Thread.sleep(10000);

      CloudTestHelper.checkMetaDataMatch(conf);
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
  public void TestDeleteDir() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.mkdirs(new Path("/dir/dir1"));
      dfs.mkdirs(new Path("/dir/dir2"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 5; i++) {
        String file = "/dir/dir1/dir-dir1-file-" + i;
        writeFile(dfs, file, FILESIZE);
        verifyFile(dfs, file, FILESIZE);
      }

      for (int i = 0; i < 5; i++) {
        String file = "/dir/dir2/dir-dir2-file-" + i;
        writeFile(dfs, file, FILESIZE);
        verifyFile(dfs, file, FILESIZE);
      }

      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == 10 * 3;

      dfs.delete(new Path("/dir"), true);

      // sleep to make sure that the objects from the cloud storage
      // have been deleted
      Thread.sleep(20000);

      CloudTestHelper.checkMetaDataMatch(conf);

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
  public void TestRename() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 5; i++) {
        writeFile(dfs, "/dir/file-" + i, FILESIZE);
      }

      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == 5 * 3;

      for (int i = 0; i < 5; i++) {
        dfs.rename(new Path("/dir/file-" + i), new Path("/dir/file-new-" + i));
      }

      for (int i = 0; i < 5; i++) {
        verifyFile(dfs, "/dir/file-new-" + i, FILESIZE);
      }

      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == 5 * 3;

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
  public void TestOverWrite() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, "/dir/file1", FILESIZE);

      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == BLK_PER_FILE;

      // now overrite this file
      writeFile(dfs, "/dir/file1", FILESIZE);
      assert CloudTestHelper.findAllBlocks().size() == BLK_PER_FILE;

      Thread.sleep(20000); //wait so that cloud bocks are deleted

      CloudTestHelper.checkMetaDataMatch(conf);

      writeFile(dfs, "/dir/file2", FILESIZE);

      assert CloudTestHelper.findAllBlocks().size() == 2 * BLK_PER_FILE;
      CloudTestHelper.checkMetaDataMatch(conf);

      dfs.rename(new Path("/dir/file1"), new Path("/dir/file2"), Options.Rename.OVERWRITE);
      assert CloudTestHelper.findAllBlocks().size() == 1 * BLK_PER_FILE;

      Thread.sleep(20000); //wait so that cloud bocks are deleted
      CloudTestHelper.checkMetaDataMatch(conf);

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
  public void TestAppend() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 1;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      int initialSize = FSNamesystem.getMaxSmallFileSize() + 1;
      writeFile(dfs, "/dir/file1", initialSize);  // write to cloud

      CloudTestHelper.checkMetaDataMatch(conf);

      final int APPEND_SIZE = 512;
      final int TIMES = 10;
      //append 1 byte ten times
      for (int i = 0; i < TIMES; i++) {
        FSDataOutputStream out = dfs.append(new Path("/dir/file1"));
        HopsFSCloudTestHelper.writeData(out, initialSize + APPEND_SIZE * i, APPEND_SIZE);
        out.close();
      }

      verifyFile(dfs, "/dir/file1", initialSize + APPEND_SIZE * TIMES);

      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

      dfs.delete(new Path("/dir/file1"), true);
      Thread.sleep(20000);
      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == 0;

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  // store small files in DB and large files in cloud
  @Test
  public void TestSmallAndLargeFiles() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";
      final String FILE_NAME5 = "/dir/TEST-FLIE5";
      final String FILE_NAME6 = "/dir/TEST-FLIE6";
      final String FILE_NAME7 = "/dir/TEST-FLIE7";
      final String FILE_NAME8 = "/dir/TEST-FLIE8";
      final String FILE_NAME9 = "/dir/TEST-FLIE9";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");


      //write small files
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      //validate
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);


      //now write large files. these should be stored in the cloud
      writeFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1); // 1 block
      verifyFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1);

      writeFile(dfs, FILE_NAME6, 10 * BLKSIZE);  // 10 blocks
      verifyFile(dfs, FILE_NAME6, 10 * BLKSIZE);
      assert CloudTestHelper.findAllBlocks().size() == 11;  // 11 blocks so far
      CloudTestHelper.checkMetaDataMatch(conf);


      dfs.mkdirs(new Path("/dir2"));
      dfs.setStoragePolicy(new Path("/dir"), "DB");
      writeFile(dfs, FILE_NAME7, MAX_SMALL_FILE_SIZE);  // goes to DB
      verifyFile(dfs, FILE_NAME7, MAX_SMALL_FILE_SIZE);

      writeFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE + 1);  // goes to DNs disks. 1 blk
      verifyFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE + 1);

      writeFile(dfs, FILE_NAME9, BLKSIZE * 10); // goes to DNs disks. 10 blks. total of 22 blks so far
      verifyFile(dfs, FILE_NAME9, BLKSIZE * 10);

      assert CloudTestHelper.findAllBlocks().size() == 22;
//      assert CloudTestHelper.getAllCloudBlocks(cloud).size() == 11;


      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 4 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 4);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 2 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 2);

      dfs.delete(new Path("/dir"), true);
      dfs.delete(new Path("/dir2"), true);
      Thread.sleep(20000);
      CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == 0;


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
  public void TestSetReplication() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();

      final int S = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      writeFile(dfs, FILE_NAME2, BLKSIZE);

      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);

      dfs.setReplication(new Path(FILE_NAME1), (short) 10);
      dfs.setReplication(new Path(FILE_NAME2), (short) 10);

      if (dfs.getFileStatus(new Path(FILE_NAME1)).getReplication() != 10) {
        fail("Unable to set replication for a small file");
      }

      if (dfs.getFileStatus(new Path(FILE_NAME2)).getReplication() != 10) {
        fail("Unable to set replication for a large file");
      }

      assert CloudTestHelper.checkMetaDataMatch(conf);

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
  public void TestSmallFileHsync() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    TestHsyncORFlush(true);
  }

  @Test
  public void TestSmallFileHflush() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    TestHsyncORFlush(false);
  }

  public void TestHsyncORFlush(boolean hsync) throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);


      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      final int TIMES = 10;
      final int SYNC_SIZE = 1024;
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      HopsFSCloudTestHelper.writeData(out, 0, SYNC_SIZE);

      for (int i = 1; i <= TIMES; i++) {
        if (hsync) {
          out.hsync();   // syncs and closes a block
        } else {
          out.hflush();
        }
        HopsFSCloudTestHelper.writeData(out, i * SYNC_SIZE, 1024);  //written to new block
      }
      out.close();

      verifyFile(dfs, FILE_NAME, SYNC_SIZE * (TIMES + 1));

      assert CloudTestHelper.checkMetaDataMatch(conf);
      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

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
  public void TestDataRace() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final FSNamesystem fsNamesystem = cluster.getNameNode().getNamesystem();
      final FSNamesystem fsNamesystemSpy = Mockito.spy(fsNamesystem);
      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      rpcServer.setFSNamesystem(fsNamesystemSpy);

      Answer delayer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("Delaying the FSYNC a bit to create a race condition");
          Thread.sleep(2000);
          return invocationOnMock.callRealMethod();
        }
      };

      Mockito.doAnswer(delayer).when(fsNamesystemSpy).fsync(anyString(), anyLong(), anyString(),
              anyLong());

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");


      final int TIMES = 20;
      final int SYNC_SIZE = 1024;
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      HopsFSCloudTestHelper.writeData(out, 0, SYNC_SIZE);

      for (int i = 1; i <= TIMES; i++) {
        ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
                .of(HdfsDataOutputStream.SyncFlag.END_BLOCK));
        HopsFSCloudTestHelper.writeData(out, i * SYNC_SIZE, 1024);  //written to new block
      }
      out.close();

      verifyFile(dfs, FILE_NAME, SYNC_SIZE * (TIMES + 1));

      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

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
  public void TestConcat() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      Path paths[] = new Path[5];
      for (int i = 0; i < paths.length; i++) {
        paths[i] = new Path("/dir/TEST-FLIE" + i);
        writeFile(dfs, paths[i].toString(), BLKSIZE);
      }

      //combine these files
      int targetFileSize = 0;
      Path merged = new Path("/dir/merged");
      writeFile(dfs, merged.toString(), targetFileSize);

      dfs.concat(merged, paths);

      verifyFile(dfs, merged.toString(), BLKSIZE * paths.length + targetFileSize);

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
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      Path path = new Path("/dir/file");
      writeFile(dfs, path.toString(), BLKSIZE * 2);

      boolean isReady = dfs.truncate(path, BLKSIZE + BLKSIZE / 2);

      if (!isReady) {
        TestFileTruncate.checkBlockRecovery(path, dfs);
      }

      verifyFile(dfs, path.toString(), BLKSIZE + BLKSIZE / 2);

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
  public void TestTruncateSlowIncrementalBR() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    final Logger logger = Logger.getRootLogger();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final FSNamesystem fsNamesystem = cluster.getNameNode().getNamesystem();
      final FSNamesystem fsNamesystemSpy = Mockito.spy(fsNamesystem);
      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      rpcServer.setFSNamesystem(fsNamesystemSpy);

      Answer delayer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("Delaying the incremental block report so that" +
                  " sync / close file does not succeed on the first try");
          Thread.sleep(1000);
          return invocationOnMock.callRealMethod();
        }
      };

      Mockito.doAnswer(delayer).when(fsNamesystemSpy).
              processIncrementalBlockReport(any(DatanodeRegistration.class),
                      any(StorageReceivedDeletedBlocks.class));
      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      final LogVerificationAppender appender1 = new LogVerificationAppender();
      logger.addAppender(appender1);
      writeFile(dfs, FILE_NAME, BLKSIZE * 2);
      verifyFile(dfs, FILE_NAME, BLKSIZE * 2);
      assertTrue(getExceptionCount(appender1.getLog(), NotReplicatedYetException.class) != 0);

      LOG.info("HopsFS-Cloud. Truncating file");
      final LogVerificationAppender appender2 = new LogVerificationAppender();
      logger.addAppender(appender2);
      boolean isReady = dfs.truncate(new Path(FILE_NAME), BLKSIZE + (BLKSIZE / 2));
      if (!isReady) {
        TestFileTruncate.checkBlockRecovery(new Path(FILE_NAME), dfs);
      }
      assertTrue(getExceptionCount(appender2.getLog(), NotReplicatedYetException.class) != 0);

      LOG.info("HopsFS-Cloud. Truncate op has completed");

      verifyFile(dfs, FILE_NAME, BLKSIZE + BLKSIZE / 2);

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
  Check that upon format the default storage policy for the root
  is set to CLOUD
   */
  @Test
  public void TestFormat() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      BlockStoragePolicy policy = dfs.getStoragePolicy(new Path("/"));
      assertTrue("Expected: "+ HdfsConstants.CLOUD_STORAGE_POLICY_NAME+" Got: "+policy.getName(),
              policy.getName().compareTo("CLOUD") == 0);

      writeFile(dfs, "/file1", BLKSIZE);
      policy = dfs.getStoragePolicy(new Path("/file1"));
      assertTrue("Expected: "+ HdfsConstants.CLOUD_STORAGE_POLICY_NAME+" Got: "+policy.getName(),
              policy.getName().compareTo("CLOUD") == 0);


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  int getExceptionCount(List<LoggingEvent> log, Class e) {
    int count = 0;
    for (int i = 0; i < log.size(); i++) {
      if (log.get(i).getMessage().toString().contains(e.getCanonicalName())) {
        count++;
      }
    }
    return count;
  }

  private void setRandomBucketPrefix(Configuration conf) {
    Date date = new Date();
    String prefix =
            "unittesting." + name.getMethodName() + "." + date.getHours() + date.getMinutes() + date.getSeconds();
    conf.set(DFSConfigKeys.S3_BUCKET_PREFIX, prefix.toLowerCase());
  }

  @Test
  public void TestZDeleteAllBuckets() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
  }
}
