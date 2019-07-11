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
package io.hops.transaction;

import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTransaction {

  static MiniDFSCluster cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 10);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .storagesPerDatanode(1)
        .build();

    cluster.waitActive();

  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDB() throws IOException {
    HdfsStorageFactory.formatStorage();
    new HopsTransactionalRequestHandler(
        HDFSOperationType.CHOOSE_UNDER_REPLICATED_BLKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.ReplicationIndex,
            TransactionLockTypes.LockType.WRITE));
      }
      final AtomicBoolean firstTime = new AtomicBoolean(true);

      @Override
      public Object performTask() throws StorageException, IOException {
        LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
          @Override
          public Object performTask() throws IOException {
            if (firstTime.getAndSet(false)) {
              throw new TransientStorageException();
            }
            return null;
          }
        };
        setRMDTMasterKeyHandler.handle();
        return null;
      }
    }.handle();
  }
}
