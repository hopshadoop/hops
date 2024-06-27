/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the cd Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs;

import io.hops.leaderElection.HdfsLeDescriptorFactory;
import io.hops.leaderElection.LeaderElection;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.Slicer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RetryCacheCleaner;
import org.apache.hadoop.util.Daemon;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRetryCacheCleaner {

  static final Log LOG = LogFactory.getLog(TestRetryCacheCleaner.class);

  Random rand = new Random(System.currentTimeMillis());
  Configuration conf;
  LeaderElection leaderElection;
  final int DELETE_BATCH_SIZE = 200;

  @Test
  public void testRetryCleaner() throws Exception {
    try {
      final int TOTAL_EPOCHS = 10;
      final int ROWS_PER_EPOCH = 600;
      Logger.getRootLogger().setLevel(Level.INFO);
      Logger.getLogger(RetryCacheCleaner.class).setLevel(Level.ALL);

      // setup conf and leader election
      setup();
      long entryExpiryMillis = conf.getLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
              DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);

      // set last epoch
      long epoch = (System.currentTimeMillis() - entryExpiryMillis) / 1000;
      HdfsVariables.setRetryCacheCleanerEpoch(epoch - 1);

      for (int i = 0; i < TOTAL_EPOCHS; i++) {
        insertTestData(epoch + i, ROWS_PER_EPOCH);
      }
      Thread.sleep(TOTAL_EPOCHS * 1000);

      Daemon retryCacheCleanerThread = new Daemon(new RetryCacheCleaner(conf, leaderElection));
      retryCacheCleanerThread.setName("Retry Cache Cleaner");
      retryCacheCleanerThread.start();


      Thread.sleep((TOTAL_EPOCHS + 10) * 1000);
      ((RetryCacheCleaner) retryCacheCleanerThread.getRunnable()).stopMonitor();
      retryCacheCleanerThread.interrupt();

      //make sure all rows are deleted
      assertTrue("Did not clean up all rows", countRows() == 0);
      try {
      } catch (Exception e) {
        fail(e.getMessage());
      }

    } finally {
      cleanup();
    }
  }

  @Test
  public void testRetryCleanerFastCleanup() throws Exception {
    try {

      final int TOTAL_EPOCHS_SEC = 100;
      final int ROWS_PER_EPOCH = 600;
      Logger.getRootLogger().setLevel(Level.INFO);
      Logger.getLogger(RetryCacheCleaner.class).setLevel(Level.ALL);

      // setup conf and leader election
      setup();
      long entryExpiryMillis = conf.getLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
              DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);

      // set last epoch
      long epoch = (System.currentTimeMillis() - entryExpiryMillis - TOTAL_EPOCHS_SEC * 1000) / 1000;
      HdfsVariables.setRetryCacheCleanerEpoch(epoch - 1);

      for (int i = 0; i < TOTAL_EPOCHS_SEC; i++) {
        insertTestData(epoch + i, ROWS_PER_EPOCH);
      }

      Thread.sleep(1000);
      // all rows are expired now

      Daemon retryCacheCleanerThread = new Daemon(new RetryCacheCleaner(conf, leaderElection));
      retryCacheCleanerThread.setName("Retry Cache Cleaner");
      retryCacheCleanerThread.start();


      // delete TOTAL_EPOCHS_SEC (100) in 5 secs
      Thread.sleep(5 * 1000);
      ((RetryCacheCleaner) retryCacheCleanerThread.getRunnable()).stopMonitor();
      retryCacheCleanerThread.interrupt();

      //make sure all rows are deleted
      assertTrue("Did not clean up all rows", countRows() == 0);
      try {
      } catch (Exception e) {
        fail(e.getMessage());
      }

    } finally {
      cleanup();
    }
  }

  public void cleanup() throws InterruptedException {
    if (conf != null) {
      conf = null;
    }
    if (leaderElection != null) {
      leaderElection.stopElectionThread();
      while (!leaderElection.isStopped()) {
        Thread.sleep(10);
      }
    }
  }

  public int countRows() throws Exception {
    return (int) (new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        RetryCacheEntryDataAccess da = (RetryCacheEntryDataAccess) HdfsStorageFactory
                .getDataAccess(RetryCacheEntryDataAccess.class);
        return da.count();
      }
    }.handle());
  }

  public void setup() throws Exception {


    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_DELETE_BATCH_SIZE_KEY, DELETE_BATCH_SIZE);

    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    long leadercheckInterval =
            conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
                    DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
            conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
                    DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    int leIncrement = conf.getInt(DFSConfigKeys.DFS_LEADER_TP_INCREMENT_KEY,
            DFSConfigKeys.DFS_LEADER_TP_INCREMENT_DEFAULT);
    String httpAddress = "127.0.0.1:8020";
    String rpcAddresses = "127.0.0.1:8020";
    leaderElection =
            new LeaderElection(new HdfsLeDescriptorFactory(), leadercheckInterval,
                    missedHeartBeatThreshold, leIncrement, httpAddress,
                    rpcAddresses, (byte) conf.getInt(DFSConfigKeys.DFS_LOCATION_DOMAIN_ID,
                    DFSConfigKeys.DFS_LOCATION_DOMAIN_ID_DEFAULT));
    leaderElection.start();
  }

  private void insertTestData(long epoch, int count) throws Exception {

    int maxDML = 500;
    int maxInsertThreads = 10;

    String clientIDStr = Integer.toString(rand.nextInt());
    ExecutorService executorService = Executors.newFixedThreadPool(maxInsertThreads);

    Slicer.slice(count, maxDML, 10, executorService,
            new Slicer.OperationHandler() {
              @Override
              public void handle(int startIndex, int endIndex) throws Exception {
                final List<RetryCacheEntry> entries = new ArrayList(maxDML);
                for (int i = startIndex; i < endIndex; i++) {
                  entries.add(new RetryCacheEntry(clientIDStr.getBytes(), i, epoch));
                }
                new LightWeightRequestHandler(HDFSOperationType.TEST) {
                  @Override
                  public Object performTask() throws IOException {
                    RetryCacheEntryDataAccess da = (RetryCacheEntryDataAccess) HdfsStorageFactory
                            .getDataAccess(RetryCacheEntryDataAccess.class);
                    da.prepare(Collections.EMPTY_LIST, entries);
                    return null;
                  }
                }.handle();
              }
            });
    LOG.info("Inserted data for epoch: " + epoch);
  }
}
