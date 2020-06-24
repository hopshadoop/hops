package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.ipc.ClientId;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestNamenodeRetryCacheCleaner {
  public static final Log LOG = LogFactory.getLog(TestNamenodeRetryCacheCleaner.class);

  @Test
  public void testCleanerFewEntries() throws Exception {
    testcleaner(10);
  }

  @Test
  public void testCleanerToneOfEntries() throws Exception {
    testcleaner(10000);
  }

  public void testcleaner(int entriesPerEpoch) throws Exception {

    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger(FSNamesystem.RetryCacheCleaner.class).setLevel(Level.ALL);
    Logger.getLogger(TestNamenodeRetryCacheCleaner.class).setLevel(Level.ALL);
    byte[] CLIENT_ID = ClientId.getClientId();
    int callId = 1;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY, 5000);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
            .newInstance(fs.getUri(), fs.getConf());
    try {
      long startEpoch = System.currentTimeMillis() / 1000;
      int totalEpochs = 5; // >= 1
      int span = 5; //sec
      for (long epoch = startEpoch; epoch < (startEpoch + (totalEpochs * span)); epoch += span) {
        List<RetryCacheEntry> entries = new ArrayList<>(entriesPerEpoch);
        for (int i = 0; i < entriesPerEpoch; i++) {
          entries.add(new RetryCacheEntry(CLIENT_ID, callId++, epoch));
        }
        addCacheEntry(entries);
      }

      assert countEntries() == totalEpochs * entriesPerEpoch;

      Thread.sleep((span*1000)/2);
      for (int i = 1; i <= totalEpochs; i++) {
        Thread.sleep(span * 1000);
        assert countEntries() == (totalEpochs - i)  * entriesPerEpoch;
        LOG.info("Epoch "+i+" cleared");
      }


    } catch (Exception e) {
      fail(e.toString());
    } finally {
      cluster.shutdown();
    }
  }

  private void addCacheEntry(List<RetryCacheEntry> entries) throws IOException {
    new LightWeightRequestHandler(
            HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        RetryCacheEntryDataAccess da = (RetryCacheEntryDataAccess) HdfsStorageFactory
                .getDataAccess(RetryCacheEntryDataAccess.class);
        da.prepare(Collections.EMPTY_LIST, entries);
        return null;
      }
    }.handle();
  }

  private int countEntries() throws IOException {
    return ((int) new LightWeightRequestHandler(
            HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        RetryCacheEntryDataAccess da = (RetryCacheEntryDataAccess) HdfsStorageFactory
                .getDataAccess(RetryCacheEntryDataAccess.class);
        return da.count();
      }
    }.handle());
  }

  @Test
  public void testCacheEnabled() throws Exception {
   simpleTest(true);
  }

  @Test
  public void testCacheDisabled() throws Exception {
    simpleTest(false);
  }

  public void simpleTest(boolean  enableCache) throws Exception {
    long BlockSize = 1024;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, enableCache);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY, 5000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 0);
    conf.set(HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,"1000,2");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
            .newInstance(fs.getUri(), fs.getConf());
    try {
      DFSTestUtil.runOperations(cluster, dfs, conf, BlockSize, 0);
    } catch (Exception e) {
      fail(e.toString());
    } finally {
      cluster.shutdown();
    }
  }
}
