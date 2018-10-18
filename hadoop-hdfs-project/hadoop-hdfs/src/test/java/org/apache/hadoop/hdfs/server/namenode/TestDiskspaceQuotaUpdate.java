/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.TransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import static org.junit.Assert.assertEquals;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDiskspaceQuotaUpdate {

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 1;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test if the quota can be correctly updated for append
   */
  @Test
  public void testUpdateQuotaForAppend() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(dfs, bar, BLOCKSIZE, REPLICATION, 0L);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    // append half of the block data
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE / 2);
    
    //let time to the quota manager to update the quota
    Thread.sleep(1000);

//    INodeDirectory fooNode = getINode4Write(foo);
    Quota.Counts quota = getSpaceConsumed(foo);
//        fooNode.getDirectoryWithQuotaFeature()
//        .getSpaceConsumed(fooNode);
    long ns = quota.get(Quota.NAMESPACE);
    long ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, ds);

    // append another block
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
    quota = getSpaceConsumed(foo);
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION, ds);
  }

  /**
   * Test if the quota can be correctly updated when file length is updated
   * through fsync
   */
  @Test
  public void testUpdateQuotaForFSync() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(dfs, bar, BLOCKSIZE, REPLICATION, 0L);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    FSDataOutputStream out = dfs.append(bar);
    out.write(new byte[BLOCKSIZE / 4]);
    ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
        .of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
//    INodeDirectory fooNode = getINode4Write(foo);
    Quota.Counts quota = getSpaceConsumed(foo);
//        fooNode.getDirectoryWithQuotaFeature()
//        .getSpaceConsumed(fooNode);
    long ns = quota.get(Quota.NAMESPACE);
    long ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals(BLOCKSIZE * 2 * REPLICATION, ds); // file is under construction

    out.write(new byte[BLOCKSIZE / 4]);
    out.close();

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
//    fooNode = getINode4Write(foo);
//    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed(fooNode);
    quota = getSpaceConsumed(foo);
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns);
    assertEquals((BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, ds);

    // append another block
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);
    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
//    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed(fooNode);
    quota = getSpaceConsumed(foo);
    ns = quota.get(Quota.NAMESPACE);
    ds = quota.get(Quota.DISKSPACE);
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION, ds);
  }

  Quota.Counts getSpaceConsumed(final Path foo) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, foo.toString());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.READ))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
        return fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed(fooNode);
      }
    };
    return (Quota.Counts) handler.handle();
  }

//  INodeDirectory getINode4Write(final Path foo) throws IOException {
//    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
//
//      @Override
//      public void acquireLock(TransactionLocks locks) throws IOException {
//        LockFactory lf = getInstance();
//        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
//            TransactionLockTypes.INodeResolveType.PATH, foo.toString());
//        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.READ))
//            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
//            .add(lf.getBlockLock())
//            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
//      }
//
//      @Override
//      public Object performTask() throws IOException {
//        return fsdir.getINode4Write(foo.toString()).asDirectory();
//      }
//    };
//    return (INodeDirectory) handler.handle();
//  }
}
