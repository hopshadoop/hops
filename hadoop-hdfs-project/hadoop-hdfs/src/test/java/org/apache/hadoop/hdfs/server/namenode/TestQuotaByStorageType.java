/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
  import static org.junit.Assert.assertEquals;
  import static org.junit.Assert.fail;

  import org.apache.commons.logging.Log;
  import org.apache.commons.logging.LogFactory;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.ContentSummary;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.fs.StorageType;
  import org.apache.hadoop.hdfs.DFSConfigKeys;
  import org.apache.hadoop.hdfs.DFSTestUtil;
  import org.apache.hadoop.hdfs.DistributedFileSystem;
  import org.apache.hadoop.hdfs.MiniDFSCluster;
  import org.apache.hadoop.hdfs.protocol.HdfsConstants;
  import org.apache.hadoop.test.GenericTestUtils;
  import org.junit.After;
import static org.junit.Assert.assertTrue;
  import org.junit.Before;
  import org.junit.Test;

  import java.io.IOException;

public class TestQuotaByStorageType {

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 3;
  private static final long seed = 0L;
  private static final Path dir = new Path("/TestQuotaByStorageType");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;
  private FSNamesystem fsn;

  protected static final Log LOG = LogFactory.getLog(TestQuotaByStorageType.class);

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

    // Setup a 3-node cluster and configure
    // each node with 1 SSD and 1 DISK without capacity limitation
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(REPLICATION)
        .storageTypes(new StorageType[]{StorageType.SSD, StorageType.DEFAULT})
        .build();
    cluster.waitActive();

    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();
    fsn = cluster.getNamesystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateOneSSD() throws Exception {
    testQuotaByStorageTypeWithFileCreateCase(
        HdfsConstants.ONESSD_STORAGE_POLICY_NAME,
        StorageType.SSD,
        (short)1);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateAllSSD() throws Exception {
    testQuotaByStorageTypeWithFileCreateCase(
        HdfsConstants.ALLSSD_STORAGE_POLICY_NAME,
        StorageType.SSD,
        (short)3);
  }

  void testQuotaByStorageTypeWithFileCreateCase(
      String storagePolicy, StorageType storageType, short replication) throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to storagePolicy
    dfs.setStoragePolicy(foo, storagePolicy);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, storageType, BLOCKSIZE * 10);

    INode fnode = getINode4Write(foo);

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    //let time to quotamanager to update quota
    Thread.sleep(1000);
    // Verify space consumed and remaining quota
    long storageTypeConsumed = getSpaceConsumed(foo).getTypeSpaces().get(storageType);
    assertEquals(file1Len * replication, storageTypeConsumed);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateAppend() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    INode fnode = getINode4Write(foo);

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    // Verify space consumed and remaining quota
    long ssdConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // append several blocks
    int appendLen = BLOCKSIZE * 2;
    DFSTestUtil.appendFile(dfs, createdFile1, appendLen);
    file1Len += appendLen;

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    ssdConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateDelete() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 10);
    INode fnode = getINode4Write(foo);

    // Create file of size 2.5 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    // Verify space consumed and remaining quota
    long storageTypeConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, storageTypeConsumed);

    // Delete file and verify the consumed space of the storage type is updated
    dfs.delete(createdFile1, false);
    
    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    storageTypeConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(0, storageTypeConsumed);

    QuotaCounts counts = computeQuotaUsage(foo);
    String subTree = dumpTreeRecursively(foo);
    assertEquals(subTree, 0,
            counts.getTypeSpaces().get(StorageType.SSD));

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), 0);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), 0);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), 0);
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateRename() throws Exception {
    final Path foo = new Path(dir, "foo");
    dfs.mkdirs(foo);
    Path createdFile1foo = new Path(foo, "created_file1.data");

    final Path bar = new Path(dir, "bar");
    dfs.mkdirs(bar);
    Path createdFile1bar = new Path(bar, "created_file1.data");

    // set storage policy on directory "foo" and "bar" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setStoragePolicy(bar, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    dfs.setQuotaByStorageType(bar, StorageType.SSD, BLOCKSIZE * 2);

    INode fnode = getINode4Write(foo);

    // Create file of size 3 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 3;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1foo, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    // Verify space consumed and remaining quota
    long ssdConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // move file from foo to bar
    try {
      dfs.rename(createdFile1foo, createdFile1bar);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  
  /**
   * Test if the quota can be correctly updated for create file even
   * QuotaByStorageTypeExceededException is thrown
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeExceptionWithFileCreate() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);

    INode fnode = getINode4Write(foo);

    // Create the 1st file of size 2 * BLOCKSIZE under directory "foo" and expect no exception
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);
    
    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    long currentSSDConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, currentSSDConsumed);

    // Create the 2nd file of size 1.5 * BLOCKSIZE under directory "foo" and expect no exception
    Path createdFile2 = new Path(foo, "created_file2.data");
    long file2Len = BLOCKSIZE + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile2, bufLen, file2Len, BLOCKSIZE, REPLICATION, seed);
    
    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    currentSSDConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);

    assertEquals(file1Len + file2Len, currentSSDConsumed);

    // Create the 3rd file of size BLOCKSIZE under directory "foo" and expect quota exceeded exception
    Path createdFile3 = new Path(foo, "created_file3.data");
    long file3Len = BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile3, bufLen, file3Len, BLOCKSIZE, REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);

      //let time for the quota monitor to apply quota
      Thread.sleep(1000);
      currentSSDConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
      assertEquals(file1Len + file2Len, currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOffChildOff() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // Create file of size 2.5 * BLOCKSIZE under child directory.
    // Since both parent and child directory do not have SSD quota set,
    // expect succeed without exception
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
        REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(5000);
    // Verify SSD usage at the root level as both parent/child don't have DirectoryWithQuotaFeature
    //INode fnode = fsdir.getINode4Write("/");
    long ssdConsumed = getSpaceConsumed(new Path("/")).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOffChildOn() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(child, StorageType.SSD, 2 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Since child directory have SSD quota of 2 * BLOCKSIZE,
    // expect an exception when creating files under child directory.
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    try {
      DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
          REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOnChildOff() throws Exception {
    short replication = 1;
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(parent, StorageType.SSD, 3 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Verify parent Quota applies
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
        replication, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(5000);
    INode fnode = getINode4Write(parent);
    long currentSSDConsumed = getSpaceConsumed(parent).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, currentSSDConsumed);

    // Create the 2nd file of size BLOCKSIZE under child directory and expect quota exceeded exception
    Path createdFile2 = new Path(child, "created_file2.data");
    long file2Len = BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile2, bufLen, file2Len, BLOCKSIZE, replication, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
      //let time for the quota monitor to apply quota
      Thread.sleep(5000);
      currentSSDConsumed = getSpaceConsumed(parent).getTypeSpaces().get(StorageType.SSD);
      assertEquals(file1Len, currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeParentOnChildOn() throws Exception {
    final Path parent = new Path(dir, "parent");
    final Path child = new Path(parent, "child");
    dfs.mkdirs(parent);
    dfs.mkdirs(child);

    dfs.setStoragePolicy(parent, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(parent, StorageType.SSD, 2 * BLOCKSIZE);
    dfs.setQuotaByStorageType(child, StorageType.SSD, 3 * BLOCKSIZE);

    // Create file of size 2.5 * BLOCKSIZE under child directory
    // Verify parent Quota applies
    Path createdFile1 = new Path(child, "created_file1.data");
    long file1Len = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    int bufLen = BLOCKSIZE / 16;
    try {
      DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE,
          REPLICATION, seed);
      fail("Should have failed with QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithTraditionalQuota() throws Exception {
    final Path foo = new Path(dir, "foo");
    dfs.mkdirs(foo);

    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 10);
    dfs.setQuota(foo, Long.MAX_VALUE - 1, REPLICATION * BLOCKSIZE * 10);

    INode fnode = getINode4Write(foo);

    Path createdFile = new Path(foo, "created_file.data");
    long fileLen = BLOCKSIZE * 2 + BLOCKSIZE / 2;
    DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
        fileLen, BLOCKSIZE, REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    QuotaCounts cnt = getSpaceConsumed(foo);
    assertEquals(2, cnt.getNameSpace());
    assertEquals(fileLen * REPLICATION, cnt.getStorageSpace());

    dfs.delete(createdFile, true);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    QuotaCounts cntAfterDelete = getSpaceConsumed(foo);
    assertEquals(1, cntAfterDelete.getNameSpace());
    assertEquals(0, cntAfterDelete.getStorageSpace());

    // Validate the computeQuotaUsage()
    QuotaCounts counts = computeQuotaUsage(foo);
    String subtree = dumpTreeRecursively(foo);
    assertEquals(subtree, 1,
        counts.getNameSpace());
    assertEquals(subtree, 0,
        counts.getStorageSpace());
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * exceeded. expect DSQuotaExceededException is thrown as we check traditional
   * space quota first and then storage type quota.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException1()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        4 * REPLICATION, 4, 5, REPLICATION);
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * SSD quota is exceeded but traditional space quota is not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException2()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        5 * REPLICATION, 4, 5, REPLICATION);
  }

  /**
   * Both traditional space quota and the storage type quota for SSD are set and
   * traditional space quota is exceeded but SSD quota is not exceeded.
   */
  @Test(timeout = 60000)
  public void testQuotaByStorageTypeAndTraditionalQuotaException3()
      throws Exception {
    testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
        4 * REPLICATION, 5, 5, REPLICATION);
  }

  private void testQuotaByStorageTypeOrTraditionalQuotaExceededCase(
      long storageSpaceQuotaInBlocks, long ssdQuotaInBlocks,
      long testFileLenInBlocks, short replication) throws Exception {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path testDir = new Path(dir, METHOD_NAME);

    dfs.mkdirs(testDir);
    dfs.setStoragePolicy(testDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    final long ssdQuota = BLOCKSIZE * ssdQuotaInBlocks;
    final long storageSpaceQuota = BLOCKSIZE * storageSpaceQuotaInBlocks;

    dfs.setQuota(testDir, Long.MAX_VALUE - 1, storageSpaceQuota);
    dfs.setQuotaByStorageType(testDir, StorageType.SSD, ssdQuota);

    INode testDirNode = getINode4Write(testDir);

    Path createdFile = new Path(testDir, "created_file.data");
    long fileLen = testFileLenInBlocks * BLOCKSIZE;

    try {
      DFSTestUtil.createFile(dfs, createdFile, BLOCKSIZE / 16,
          fileLen, BLOCKSIZE, replication, seed);      
      //let time for the quota monitor to apply quota
      Thread.sleep(1000);
      // Create the 2nd file of size 1.5 * BLOCKSIZE under directory "foo" and expect no exception
      Path createdFile2 = new Path(testDir, "created_file2.data");
      DFSTestUtil.createFile(dfs, createdFile2, BLOCKSIZE / 16,
          fileLen, BLOCKSIZE, replication, seed);
      fail("Should have failed with DSQuotaExceededException or " +
          "QuotaByStorageTypeExceededException ");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
      Thread.sleep(5000);
      long currentSSDConsumed = getSpaceConsumed(testDir).getTypeSpaces().get(StorageType.SSD);
      //our quota mechanism may not detect when one file being writen is too long (because of assynchrony)
      //but it should detect it for the second file so the total ammount of data should be at most the size
      //of the first file.
      assertTrue("SSD consumed to high " + currentSSDConsumed, fileLen >= currentSSDConsumed);
    }
  }

  @Test(timeout = 60000)
  public void testQuotaByStorageTypeWithFileCreateTruncate() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // set quota by storage type on directory "foo"
    dfs.setQuotaByStorageType(foo, StorageType.SSD, BLOCKSIZE * 4);
    INode fnode = getINode4Write(foo);

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    // Verify SSD consumed before truncate
    long ssdConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(file1Len, ssdConsumed);

    // Truncate file to 1 * BLOCKSIZE
    int newFile1Len = BLOCKSIZE * 1;
    dfs.truncate(createdFile1, newFile1Len);

    //let time for the quota monitor to apply quota
    Thread.sleep(1000);
    // Verify SSD consumed after truncate
    ssdConsumed = getSpaceConsumed(foo).getTypeSpaces().get(StorageType.SSD);
    assertEquals(newFile1Len, ssdConsumed);

    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), newFile1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), newFile1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), newFile1Len * 2);
  }
  
  INodeDirectory getINode4Write(final Path foo) throws IOException {
    return getINode4Write(foo, true);
  }

  INodeDirectory getINode4Write(final Path foo, final boolean isQuotaset) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, foo.toString());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.READ))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        INode fnode = fsdir.getINode4Write(foo.toString()).asDirectory();
        assertTrue(fnode.isDirectory());
        assertTrue(fnode.isQuotaSet()==isQuotaset);
        return fnode;
      }
    };
    return (INodeDirectory) handler.handle();
  }
  
  QuotaCounts getSpaceConsumed(final Path foo) throws IOException {
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
        assertTrue(fooNode.isQuotaSet());
        return fooNode.getDirectoryWithQuotaFeature().getSpaceConsumed();
      }
    };
    return (QuotaCounts) handler.handle();
  }
  
  QuotaCounts computeQuotaUsage(final Path foo) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, foo.toString());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.READ))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory fnode = fsdir.getINode4Write(foo.toString()).asDirectory();
        QuotaCounts counts = new QuotaCounts.Builder().build();
        fnode.computeQuotaUsage(fsn.getBlockManager().getStoragePolicySuite(), counts);
        
        
        return counts;
      }
    };
    return (QuotaCounts) handler.handle();
  }
  
  String dumpTreeRecursively(final Path foo) throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, foo.toString());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.READ))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED, foo.toString()))
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory fnode = fsdir.getINode4Write(foo.toString()).asDirectory();
        return fnode.dumpTreeRecursively().toString();
      }
    };
    return (String) handler.handle();
  }

  @Test(timeout = 60000)
  public void testContentSummaryWithoutQuotaByStorageType() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    // set storage policy on directory "foo" to ONESSD
    dfs.setStoragePolicy(foo, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    INode fnode = getINode4Write(foo, false);

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify getContentSummary without any quota set
    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    assertEquals(cs.getTypeConsumed(StorageType.SSD), file1Len);
    assertEquals(cs.getTypeConsumed(StorageType.DISK), file1Len * 2);
  }

  @Test(timeout = 60000)
  public void testContentSummaryWithoutStoragePolicy() throws Exception {
    final Path foo = new Path(dir, "foo");
    Path createdFile1 = new Path(foo, "created_file1.data");
    dfs.mkdirs(foo);

    INode fnode = getINode4Write(foo, false);

    // Create file of size 2 * BLOCKSIZE under directory "foo"
    long file1Len = BLOCKSIZE * 2;
    int bufLen = BLOCKSIZE / 16;
    DFSTestUtil.createFile(dfs, createdFile1, bufLen, file1Len, BLOCKSIZE, REPLICATION, seed);

    // Verify getContentSummary without any quota set
    // Expect no type quota and usage information available
    ContentSummary cs = dfs.getContentSummary(foo);
    assertEquals(cs.getSpaceConsumed(), file1Len * REPLICATION);
    for (StorageType t : StorageType.values()) {
      assertEquals(cs.getTypeConsumed(t), 0);
      assertEquals(cs.getTypeQuota(t), -1);
    }
  }
}
