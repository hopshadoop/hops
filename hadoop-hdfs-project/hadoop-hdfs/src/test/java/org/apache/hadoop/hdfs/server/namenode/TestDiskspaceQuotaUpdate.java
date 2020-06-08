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
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDiskspaceQuotaUpdate {

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 4;
  static final long seed = 0L;
  private static final Path dir = new Path("/TestQuotaUpdate");
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;
  private int leaseCreationLockRows;
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    leaseCreationLockRows = conf.getInt(DFSConfigKeys.DFS_LEASE_CREATION_LOCKS_COUNT_KEY,
            DFSConfigKeys.DFS_LEASE_CREATION_LOCKS_COUNT_DEFAULT);
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
   * Test if the quota can be correctly updated for create file
   */
  @Test (timeout=60000)
  public void testQuotaUpdateWithFileCreate() throws Exception  {
    final Path foo = new Path(dir, "foo");
    Path createdFile = new Path(foo, "created_file.data");
    dfs.mkdirs(foo);
    dfs.setQuota(foo, Long.MAX_VALUE-1, Long.MAX_VALUE-1);
    long fileLen = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
        fileLen, BLOCKSIZE, REPLICATION, seed);
    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    QuotaCounts cnt = getSpaceConsumed(foo);
    assertEquals(2, cnt.getNameSpace());
    assertEquals(fileLen * REPLICATION, cnt.getStorageSpace());
  }


  /**
   * Test if the quota can be correctly updated for append
   */
  @Test //(timeout=60000)
  public void testUpdateQuotaForAppend() throws Exception {
    final Path foo = new Path(dir ,"foo");
    final Path bar = new Path(foo, "bar");
    long currentFileLen = BLOCKSIZE;
    DFSTestUtil.createFile(dfs, bar, currentFileLen, REPLICATION, seed);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    // append half of the block data, the previous file length is at block
    // boundary
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE / 2);
    currentFileLen += (BLOCKSIZE / 2);
    
    //let time to the quota manager to update the quota
    Thread.sleep(1000);

    QuotaCounts quota = getSpaceConsumed(foo);
    long ns = quota.getNameSpace();
    long ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    ContentSummary c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);

    // append another block, the previous file length is not at block boundary
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);
    currentFileLen += BLOCKSIZE;

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
    quota = getSpaceConsumed(foo);
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);
    // append several blocks
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE * 3 + BLOCKSIZE / 8);
    currentFileLen += (BLOCKSIZE * 3 + BLOCKSIZE / 8);
    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    quota = getSpaceConsumed(foo);
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(currentFileLen * REPLICATION, ds);
    c = dfs.getContentSummary(foo);
    assertEquals(c.getSpaceConsumed(), ds);
  }

  /**
   * Test if the quota can be correctly updated when file length is updated
   * through fsync
   */
  @Test (timeout=60000)
  public void testUpdateQuotaForFSync() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(dfs, bar, BLOCKSIZE, REPLICATION, 0L);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);

    FSDataOutputStream out = dfs.append(bar);
    out.write(new byte[BLOCKSIZE / 4]);
    ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
//    INodeDirectory fooNode = getINode4Write(foo);
    QuotaCounts quota = getSpaceConsumed(foo);
//        fooNode.getDirectoryWithQuotaFeature()
//        .getSpaceConsumed(fooNode);
    long ns = quota.getNameSpace();
    long ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals(BLOCKSIZE * 2 * REPLICATION, ds); // file is under construction

    out.write(new byte[BLOCKSIZE / 4]);
    out.close();

    //let time to the quota manager to update the quota
    Thread.sleep(1000);
//    fooNode = getINode4Write(foo);
//    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed(fooNode);
    quota = getSpaceConsumed(foo);
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns);
    assertEquals((BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION, ds);

    // append another block
    DFSTestUtil.appendFile(dfs, bar, BLOCKSIZE);
    //let time to the quota manager to update the quota
    Thread.sleep(1000);
    
//    quota = fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed(fooNode);
    quota = getSpaceConsumed(foo);
    ns = quota.getNameSpace();
    ds = quota.getStorageSpace();
    assertEquals(2, ns); // foo and bar
    assertEquals((BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION, ds);
  }

  QuotaCounts getSpaceConsumed(final Path foo) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, foo.toString());
        locks.add(il).add(lf.getLeaseLockAllPaths(TransactionLockTypes.LockType.READ, leaseCreationLockRows))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory fooNode = fsdir.getINode4Write(foo.toString()).asDirectory();
        assertTrue(fooNode.isQuotaSet());
        return fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
      }
    };
    return (QuotaCounts) handler.handle();
  }

  INodeFile getINodeFile(final Path foo) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, foo.toString());
        locks.add(il).add(lf.getLeaseLockAllPaths(TransactionLockTypes.LockType.READ, leaseCreationLockRows))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        return fsdir.getINode(foo.toString()).asFile();
      }
    };
    return (INodeFile) handler.handle();
  }

  /**
   * Test append over storage quota does not mark file as UC or create lease
   */
  @Test //(timeout=60000)
  public void testAppendOverStorageQuota() throws Exception {
    final Path dir = new Path("/TestAppendOverQuota");
    final Path file = new Path(dir, "file");

    // create partial block file
    dfs.mkdirs(dir);
    DFSTestUtil.createFile(dfs, file, BLOCKSIZE/2, REPLICATION, seed);

    // lower quota to cause exception when appending to partial block
    dfs.setQuota(dir, Long.MAX_VALUE - 1, 1);
    final long spaceUsed = getSpaceConsumed(dir).getStorageSpace();
    try {
      DFSTestUtil.appendFile(dfs, file, BLOCKSIZE);
      Assert.fail("append didn't fail");
    } catch (DSQuotaExceededException e) {
      // ignore
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getINodeFile(file);
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", TestLeaseManager.getLeaseByPath(cluster.getNamesystem().
        getLeaseManager(), file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = getSpaceConsumed(dir).getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    dfs.recoverLease(file);
    cluster.restartNameNodes();
  }

  /**
   * Test append over a specific type of storage quota does not mark file as
   * UC or create a lease
   */
  @Test (timeout=60000)
  public void testAppendOverTypeQuota() throws Exception {
    final Path dir = new Path("/TestAppendOverTypeQuota");
    final Path file = new Path(dir, "file");

    // create partial block file
    dfs.mkdirs(dir);
    // set the storage policy on dir
    dfs.setStoragePolicy(dir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    DFSTestUtil.createFile(dfs, file, BLOCKSIZE/2, REPLICATION, seed);

    // set quota of SSD to 1L
    dfs.setQuotaByStorageType(dir, StorageType.SSD, 1L);
    final long spaceUsed = getSpaceConsumed(dir).getStorageSpace();
    try {
      DFSTestUtil.appendFile(dfs, file, BLOCKSIZE);
      Assert.fail("append didn't fail");
    } catch (QuotaByStorageTypeExceededException e) {
      //ignore
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getINodeFile(file);
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", TestLeaseManager.getLeaseByPath(cluster.getNamesystem().
        getLeaseManager(), file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = getSpaceConsumed(dir).getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    dfs.recoverLease(file);
    cluster.restartNameNodes();
  }

  /**
   * Test truncate over quota does not mark file as UC or create a lease
   */
  @Test (timeout=60000)
  public void testTruncateOverQuota() throws Exception {
    final Path dir = new Path("/TestTruncateOverquota");
    final Path file = new Path(dir, "file");

    // create partial block file
    dfs.mkdirs(dir);
    DFSTestUtil.createFile(dfs, file, BLOCKSIZE/2, REPLICATION, seed);

    // lower quota to cause exception when appending to partial block
    dfs.setQuota(dir, Long.MAX_VALUE - 1, 1);
    final long spaceUsed = getSpaceConsumed(dir).getStorageSpace();
    try {
      dfs.truncate(file, BLOCKSIZE / 2 - 1);
      Assert.fail("truncate didn't fail");
    } catch (RemoteException e) {
      assertTrue(e.getClassName().contains("DSQuotaExceededException"));
    }

    // check that the file exists, isn't UC, and has no dangling lease
    INodeFile inode = getINodeFile(file);
    Assert.assertNotNull(inode);
    Assert.assertFalse("should not be UC", inode.isUnderConstruction());
    Assert.assertNull("should not have a lease", TestLeaseManager.getLeaseByPath(cluster.getNamesystem().
        getLeaseManager(), file.toString()));
    // make sure the quota usage is unchanged
    final long newSpaceUsed = getSpaceConsumed(dir).getStorageSpace();
    assertEquals(spaceUsed, newSpaceUsed);
    // make sure edits aren't corrupted
    dfs.recoverLease(file);
    cluster.restartNameNodes();
  }
}
