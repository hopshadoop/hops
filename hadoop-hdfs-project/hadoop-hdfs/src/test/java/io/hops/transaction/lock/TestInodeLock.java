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

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestInodeLock {

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
}
