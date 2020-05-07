/*
 * Copyright 2018 Apache Software Foundation.
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
package io.hops.transaction.lock;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;

public class TestInodeLock {

  public static final Log LOG = LogFactory.getLog(TestInodeLock.class);

  @Test
  public void testInodeLockWithWrongPath() throws IOException {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final MiniDFSCluster clusterFinal = cluster;
      final DistributedFileSystem hdfs = cluster.getFileSystem();

      hdfs.mkdirs(new Path("/tmp"));
      DFSTestUtil.createFile(hdfs, new Path("/tmp/f1"), 0, (short) 1, 0);

      new HopsTransactionalRequestHandler(
              HDFSOperationType.TEST) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED,
                  TransactionLockTypes.INodeResolveType.PATH, new String[]{"/tmp/f1", "/tmp/f2"})
                  .setNameNodeID(clusterFinal.getNameNode().getId())
                  .setActiveNameNodes(clusterFinal.getNameNode().getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(true);
          locks.add(il);
        }

        @Override
        public Object performTask() throws IOException {
          return null;
        }
      }.handle();

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Hops-1564
   */
  @Test
  public void testInodeAndBlockLock() throws IOException {
    Logger.getRootLogger().setLevel(Level.INFO);
    final Configuration conf = new Configuration();
    final int BLOCK_SIZE = 1024 * 1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).build();
      cluster.waitActive();
      final MiniDFSCluster clusterFinal = cluster;
      final DistributedFileSystem hdfs = cluster.getFileSystem();

      hdfs.mkdirs(new Path("/tmp"));

      final int BLOCKS_PER_FILE = 10;
      final int NO_OF_FILES = 10;
      for (int i = 0; i < NO_OF_FILES; i++) {
        DFSTestUtil.createFile(hdfs, new Path("/tmp/file" + i), BLOCKS_PER_FILE * BLOCK_SIZE, (short) 3, 0);
      }

      int count = (int) new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          HdfsStorageFactory.getConnector().writeLock();
          ReplicaUnderConstructionDataAccess da = (ReplicaUnderConstructionDataAccess) HdfsStorageFactory
                  .getDataAccess(ReplicaUnderConstructionDataAccess.class);
          return da.countAll();
        }
      }.performTask();
      assert count == 0;

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Test to check that taking lock on a row using PK and FTIS works
   */
  @Test
  public void testInodeAndBlockLock1() throws IOException {
    final Configuration conf = new Configuration();
    final int BLOCK_SIZE = 1024 * 1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      final MiniDFSCluster clusterFinal = cluster;
      final DistributedFileSystem hdfs = cluster.getFileSystem();

      hdfs.mkdirs(new Path("/tmp"));

      final int BLOCKS_PER_FILE = 1;
      final int NO_OF_FILES = 1;
      for (int i = 0; i < NO_OF_FILES; i++) {
        DFSTestUtil.createFile(hdfs, new Path("/tmp/file" + i), BLOCKS_PER_FILE * BLOCK_SIZE, (short) 3, 0);
      }

      InodeLockThread worker1 = new InodeLockThread("file0", new Long(2), new Long(2), 1000);
      InodeLockThread worker2 = new InodeLockThread(new Long(3), 1000);

      long startTime = System.currentTimeMillis();
      Thread thread1 = new Thread(worker1);
      thread1.start();

      Thread.sleep(250);

      Thread thread2 = new Thread(worker2);
      thread2.start();

      thread1.join();
      thread2.join();

      long endTime = System.currentTimeMillis();

      assert (endTime - startTime) > 2000;


    } catch (InterruptedException e) {
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  class InodeLockThread implements Runnable {
    private Long id;
    private Long pid;
    private String name;
    private Long partID;
    private long sleep;

    public InodeLockThread(Long id, long sleep) {
      this.id = id;
      this.sleep = sleep;
    }

    public InodeLockThread(String name, Long pid, Long partID, long sleep) {
      this.name = name;
      this.pid = pid;
      this.partID = partID;
      this.sleep = sleep;
    }

    @Override
    public void run() {
      try {
        new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            try {
              INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                      .getDataAccess(INodeDataAccess.class);
              EntityManager.writeLock();
              LOG.info("TID: " + Thread.currentThread().getId() + " Connector " + HdfsStorageFactory.getConnector());
              if (id != null) {
                assert da.findInodeByIdFTIS(id) != null;
                LOG.info("Locked using FTIS");
                Thread.sleep(sleep);
                LOG.info("Locked using FTIS Returning");
              } else {
                assert da.findInodeByNameParentIdAndPartitionIdPK(name, pid, partID) != null;
                LOG.info("Locked using PK");
                Thread.sleep(sleep);
                LOG.info("Locked using PK Returning");
              }
            } catch (InterruptedException e){

            }
          }

          @Override
          public Object performTask() throws IOException {
            return null;
          }
        }.handle();
      } catch (IOException e){
      }
    }
  }
}
