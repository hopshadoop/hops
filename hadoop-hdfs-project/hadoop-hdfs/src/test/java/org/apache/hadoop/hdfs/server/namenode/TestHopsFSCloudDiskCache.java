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
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static org.apache.hadoop.hdfs.TestSmallFilesCreation.verifyFile;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHopsFSCloudDiskCache {

  static final Log LOG = LogFactory.getLog(TestHopsFSCloudDiskCache.class);
  @Rule
  public TestName name = new TestName();

  @Test
  public void TestDiskCache() throws IOException {

    Logger.getLogger(ProvidedBlocksCacheCleaner.class).setLevel(Level.DEBUG);

    HopsFSCloudTestHelper.purgeS3();
    final Logger logger = Logger.getRootLogger();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 1;
      final int BLKSIZE = 1 * 1024 * 1024;
//      final int BLKSIZE =  65 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY, 90);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_BATCH_SIZE_KEY, 1);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_CHECK_INTERVAL_KEY, 1000);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_WAIT_KEY, 10000);  //The cached block
      // has to be atlease 10 sec old before it can be deleted

      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      FsVolumeImpl v =
              ((CloudFsDatasetImpl)cluster.getDataNodes().get(0).getFSDataset()).getCloudVolume();
      BlockPoolSlice slice = v.getBlockPoolSlice(cluster.getNamesystem(0).getBlockPoolId());
      ProvidedBlocksCacheCleaner cleaner = slice.getProvidedBlocksCacheCleaner();
      ProvidedBlocksCacheDiskUtilization uti = cleaner.getDiskUtilizationCalc();
      ProvidedBlocksCacheDiskUtilization mockedCalc = Mockito.spy(uti);
      cleaner.setDiskUtilizationMock(mockedCalc);

      LOG.info("HopsFS-Cloud. Setting new mock obj with fixed disk utilization of 0%");
      Answer diskUtilization = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("HopsFS-Cloud. Mocked. Cache disk utilization is 0%");
          return  new Double(0);
        }
      };
      Mockito.doAnswer(diskUtilization).when(mockedCalc).getDiskUtilization();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/dir"));
      String file1 = "/dir/file";
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      int totalBlks = 10;
      LOG.info("HopsFS-Cloud. Writing File1");
      HopsFSCloudTestHelper.writeFile(dfs, file1, BLKSIZE * totalBlks);
      assert cleaner.getCachedFilesCount() == totalBlks * 2; // blocks + meta files

      diskUtilization = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("HopsFS-Cloud. Mocked. Cache disk utilization is 90%");
          return  new Double(90); //disk it full cleaner will empty the cache to make room for
          // new blocks;
        }
      };

      Mockito.doAnswer(diskUtilization).when(mockedCalc).getDiskUtilization();
      cleaner.setDiskUtilizationMock(mockedCalc);
      LOG.info("HopsFS-Cloud. Setting new mock obj with fixed disk utilization of 90%");

      String file2 = "/dir/file2";
      HopsFSCloudTestHelper.writeFile(dfs, file2, BLKSIZE * totalBlks);

      Thread.sleep(20000); // wait for the cleaner to remove cached blocks

      assert cleaner.getCachedFilesCount() == 0;

      LOG.info("HopsFS-Cloud. Reading File.");
      verifyFile(dfs, file1, BLKSIZE * totalBlks);
      verifyFile(dfs, file2, BLKSIZE * totalBlks);

      Thread.sleep(20000); // wait for the cleaner to remove cached blocks
      assert cleaner.getCachedFilesCount() == 0;

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
  make sure that when a file is read multiple time it is redirected to
  same datanode that contains the block in its cache
   */
  @Test
  public void TestDiskCache2() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
    final Logger logger = Logger.getRootLogger();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 1 * 1024 * 1024;
//      final int BLKSIZE =  65 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_BATCH_SIZE_KEY, 1);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_CHECK_INTERVAL_KEY, 1000);

      // DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY to high number to
      // prevent cache cleaner from deleting block.
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY, 99);
      setRandomBucketPrefix(conf);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(HopsFSCloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      for (int i = 0; i < NUM_DN; i++) {
        CloudFsDatasetImpl data = (CloudFsDatasetImpl) cluster.getDataNodes().get(i).getFSDataset();
        CloudPersistenceProvider cloud = data.getCloudConnector();

        final CloudPersistenceProvider cloudMock = Mockito.spy(cloud);
        data.installMockCloudConnector(cloudMock);

        Answer checker = new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            String error = "Every thing should have been read from the cache.";
            LOG.error(error);
            throw new IllegalStateException(error);
          }
        };

        Mockito.doAnswer(checker).when(cloudMock).downloadObject(anyShort(), anyString(),
                (File) anyObject());
      }

      int totalBlks = 10;
      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/dir"));
      String file = "/dir/file";
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");
      HopsFSCloudTestHelper.writeFile(dfs, file, BLKSIZE * totalBlks);

      LOG.info("HopsFS-Cloud. Reading File.");
      verifyFile(dfs, file, BLKSIZE * totalBlks);

      LOG.info("HopsFS-Cloud. Reading File Again");
      verifyFile(dfs, file, BLKSIZE * totalBlks);

      LOG.info("HopsFS-Cloud. " + conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));

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
  make sure that block delete requests are send to datanodes that
  store cached copies of the blocks
   */
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

      for(int i = 0; i < 10; i++){
        HopsFSCloudTestHelper.writeFile(dfs, "/dir/file"+i, FILESIZE);
      }

      CloudTestHelper.checkMetaDataMatch(conf);

      dfs.delete(new Path("/dir"), true);

      Thread.sleep(10000);

      CloudTestHelper.checkMetaDataMatch(conf);

      for(DataNode dn: cluster.getDataNodes()){
        CloudFsDatasetImpl data = (CloudFsDatasetImpl)dn.getFSDataset();
        CloudFsVolumeImpl vol = (CloudFsVolumeImpl)data.getCloudVolume();
        File dir = vol.getCacheDir(cluster.getNamesystem().getBlockPoolId());
        String files[] = dir.list();
        if ( files.length != 0 ){
          LOG.info("HopsFS-Cloud. Cached Files : "+ Arrays.toString(files));
          fail();
        }
        assert vol.getBlockPoolSlice(cluster.getNamesystem().getBlockPoolId()).
                getProvidedBlocksCacheCleaner().getCachedFilesCount() == 0;
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
  public void TestZDeleteAllBuckets() throws IOException {
    HopsFSCloudTestHelper.purgeS3();
  }

  private void setRandomBucketPrefix(Configuration conf) {
    Date date = new Date();
    String prefix =
            "unittesting." + name.getMethodName() + "." + date.getHours() + date.getMinutes() + date.getSeconds();
    conf.set(DFSConfigKeys.S3_BUCKET_PREFIX, prefix.toLowerCase());
  }
}
