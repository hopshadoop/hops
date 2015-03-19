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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import org.apache.hadoop.HadoopIllegalArgumentException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;

public class TestFileTruncate {
  static {
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.ALL);
  }
  static final Log LOG = LogFactory.getLog(TestFileTruncate.class);
  static final int BLOCK_SIZE = 4;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 3;
  static final int SUCCESS_ATTEMPTS = 300;
  static final int RECOVERY_ATTEMPTS = 600;
  static final long SLEEP = 100L;

  static final long LOW_SOFTLIMIT = 100L;
  static final long LOW_HARDLIMIT = 200L;
  static final int SHORT_HEARTBEAT = 1;

  static Configuration conf;
  static MiniDFSCluster cluster;
  static DistributedFileSystem fs;

  @BeforeClass
  public static void startUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, SHORT_HEARTBEAT);
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
        .format(true)
        .numDataNodes(DATANODE_NUM)
        .nameNodePort(NameNode.DEFAULT_PORT)
        .waitSafeMode(true)
        .build();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if(fs != null)      fs.close();
    if(cluster != null) cluster.shutdown();
  }

  /**
   * Truncate files of different sizes byte by byte.
   */
  @Test
  public void testBasicTruncate() throws IOException {
    int startingFileSize = 3 * BLOCK_SIZE;

    Path parent = new Path("/test");
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    for (int fileLength = startingFileSize; fileLength > 0;
                                            fileLength -= BLOCK_SIZE - 1) {
      for (int toTruncate = 0; toTruncate <= fileLength; toTruncate++) {
        final Path p = new Path(parent, "testBasicTruncate" + fileLength);
        writeContents(contents, fileLength, p);

        int newLength = fileLength - toTruncate;
        boolean isReady = fs.truncate(p, newLength);
        LOG.info("fileLength=" + fileLength + ", newLength=" + newLength
            + ", toTruncate=" + toTruncate + ", isReady=" + isReady);

        assertEquals("File must be closed for zero truncate"
            + " or truncating at the block boundary",
            isReady, toTruncate == 0 || newLength % BLOCK_SIZE == 0);
        if (!isReady) {
          checkBlockRecovery(p);
        }

        ContentSummary cs = fs.getContentSummary(parent);
        assertEquals("Bad disk space usage",
            cs.getSpaceConsumed(), newLength * REPLICATION);
        // validate the file content
        checkFullFile(p, newLength, contents);
      }
    }
    fs.delete(parent, true);
  }

  /** Truncate the same file multiple times until its size is zero. */
  @Test
  public void testMultipleTruncate() throws IOException {
    Path dir = new Path("/testMultipleTruncate");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[100 * BLOCK_SIZE];
    DFSUtil.getRandom().nextBytes(data);
    writeContents(data, data.length, p);

    for(int n = data.length; n > 0; ) {
      final int newLength = DFSUtil.getRandom().nextInt(n);
      final boolean isReady = fs.truncate(p, newLength);
      LOG.info("newLength=" + newLength + ", isReady=" + isReady);
      assertEquals("File must be closed for truncating at the block boundary",
          isReady, newLength % BLOCK_SIZE == 0);
      assertEquals("Truncate is not idempotent",
          isReady, fs.truncate(p, newLength));
      if (!isReady) {
        checkBlockRecovery(p);
      }
      checkFullFile(p, newLength, data);
      n = newLength;
    }

    fs.delete(dir, true);
  }

  /**
   * Truncate files and then run other operations such as
   * rename, set replication, set permission, etc.
   */
  @Test
  public void testTruncateWithOtherOperations() throws IOException {
    Path dir = new Path("/testTruncateOtherOperations");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[2 * BLOCK_SIZE];

    DFSUtil.getRandom().nextBytes(data);
    writeContents(data, data.length, p);

    final int newLength = data.length - 1;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    fs.setReplication(p, (short)(REPLICATION - 1));
    fs.setPermission(p, FsPermission.createImmutable((short)0444));

    final Path q = new Path(dir, "newFile");
    fs.rename(p, q);

    checkBlockRecovery(q);
    checkFullFile(q, newLength, data);

    cluster.restartNameNode();
    checkFullFile(q, newLength, data);

    fs.delete(dir, true);
  }

  /**
   * Failure / recovery test for truncate.
   * In this failure the DNs fail to recover the blocks and the NN triggers
   * lease recovery.
   * File stays in RecoveryInProgress until DataNodes report recovery.
   */
  @Test
  public void testTruncateFailure() throws IOException {
    int startingFileSize = 2 * BLOCK_SIZE + BLOCK_SIZE / 2;
    int toTruncate = 1;

    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path dir = new Path("/dir");
    final Path p = new Path(dir, "testTruncateFailure");
    {
      FSDataOutputStream out = fs.create(p, false, BLOCK_SIZE, REPLICATION,
          BLOCK_SIZE);
      out.write(contents, 0, startingFileSize);
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail on open file.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      } finally {
        out.close();
      }
    }

    {
      FSDataOutputStream out = fs.append(p);
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail for append.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      } finally {
        out.close();
      }
    }

    try {
      fs.truncate(p, -1);
      fail("Truncate must fail for a negative new length.");
    } catch (HadoopIllegalArgumentException expected) {
      GenericTestUtils.assertExceptionContains(
          "Cannot truncate to a negative file size", expected);
    }

    try {
      fs.truncate(p, startingFileSize + 1);
      fail("Truncate must fail for a larger new length.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "Cannot truncate to a larger file size", expected);
    }

    try {
      fs.truncate(dir, 0);
      fail("Truncate must fail for a directory.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "Path is not a file", expected);
    }

    try {
      fs.truncate(new Path(dir, "non-existing"), 0);
      fail("Truncate must fail for a non-existing file.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "File does not exist", expected);
    }

    
    fs.setPermission(p, FsPermission.createImmutable((short)0664));
    {
      final UserGroupInformation fooUgi = 
          UserGroupInformation.createUserForTesting("foo", new String[]{"foo"});
      try {
        final FileSystem foofs = DFSTestUtil.getFileSystemAs(fooUgi, conf);
        foofs.truncate(p, 0);
        fail("Truncate must fail for no WRITE permission.");
      } catch (Exception expected) {
        GenericTestUtils.assertExceptionContains(
            "Permission denied", expected);
      }
    }

    cluster.shutdownDataNodes();
    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(LOW_SOFTLIMIT, LOW_HARDLIMIT);

    int newLength = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLength);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    {
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail since a trancate is already in pregress.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      }
    }

    boolean recoveryTriggered = false;
    for(int i = 0; i < RECOVERY_ATTEMPTS; i++) {
      String leaseHolder =
          NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(),
          p.toUri().getPath());
      if(leaseHolder.equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) {
        recoveryTriggered = true;
        break;
      }
      try { Thread.sleep(SLEEP); } catch (InterruptedException ignored) {}
    }
    assertThat("lease recovery should have occurred in ~" +
        SLEEP * RECOVERY_ATTEMPTS + " ms.", recoveryTriggered, is(true));
    cluster.startDataNodes(conf, DATANODE_NUM, true,
        StartupOption.REGULAR, null);
    cluster.waitActive();

    checkBlockRecovery(p);

    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(HdfsConstants.LEASE_SOFTLIMIT_PERIOD,
            HdfsConstants.LEASE_HARDLIMIT_PERIOD);

    checkFullFile(p, newLength, contents);
    fs.delete(p, false);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * dn0 is shutdown before truncate and restart after truncate successful.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesRestart() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path parent = new Path("/test");
    final Path p = new Path(parent, "testTruncateWithDataNodesRestart");

    writeContents(contents, startingFileSize, p);
    LocatedBlock oldBlock = getLocatedBlocks(p).getLastLocatedBlock();

    int dn = 0;
    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    cluster.getDataNodes().get(dn).shutdown();
    try {
      boolean isReady = fs.truncate(p, newLength);
      assertFalse(isReady);
    } finally {
      cluster.restartDataNode(dn, true, true);
      cluster.waitActive();
    }
    checkBlockRecovery(p);

    LocatedBlock newBlock = getLocatedBlocks(p).getLastLocatedBlock();
    /*
     * For non copy-on-truncate, the truncated block id is the same, but the 
     * GS should increase.
     * The truncated block will be replicated to dn0 after it restarts.
     */
    assertEquals(newBlock.getBlock().getBlockId(), 
        oldBlock.getBlock().getBlockId());
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    // Wait replicas come to 3
    DFSTestUtil.waitReplication(fs, p, REPLICATION);
    // Old replica is disregarded and replaced with the truncated one
    assertEquals(cluster.getBlockFile(dn, newBlock.getBlock()).length(), 
        newBlock.getBlockSize());
    assertTrue(cluster.getBlockMetadataFile(dn, 
        newBlock.getBlock()).getName().endsWith(
            newBlock.getBlock().getGenerationStamp() + ".meta"));

    // Validate the file
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));
    checkFullFile(p, newLength, contents);

    fs.delete(parent, true);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * dn0, dn1 are restarted immediately after truncate.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesRestartImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path parent = new Path("/test");
    final Path p = new Path(parent, "testTruncateWithDataNodesRestartImmediately");

    writeContents(contents, startingFileSize, p);
    LocatedBlock oldBlock = getLocatedBlocks(p).getLastLocatedBlock();

    int dn0 = 0;
    int dn1 = 1;
    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    cluster.restartDataNode(dn0, true, true);
    cluster.restartDataNode(dn1, true, true);
    cluster.waitActive();
    checkBlockRecovery(p);

    LocatedBlock newBlock = getLocatedBlocks(p).getLastLocatedBlock();
    /*
     * For non copy-on-truncate, the truncated block id is the same, but the 
     * GS should increase.
     */
    assertEquals(newBlock.getBlock().getBlockId(), 
        oldBlock.getBlock().getBlockId());
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    // Wait replicas come to 3
    DFSTestUtil.waitReplication(fs, p, REPLICATION);
    // Old replica is disregarded and replaced with the truncated one on dn0
    assertEquals(cluster.getBlockFile(dn0, newBlock.getBlock()).length(), 
        newBlock.getBlockSize());
    assertTrue(cluster.getBlockMetadataFile(dn0, 
        newBlock.getBlock()).getName().endsWith(
            newBlock.getBlock().getGenerationStamp() + ".meta"));

    // Old replica is disregarded and replaced with the truncated one on dn1
    assertEquals(cluster.getBlockFile(dn1, newBlock.getBlock()).length(), 
        newBlock.getBlockSize());
    assertTrue(cluster.getBlockMetadataFile(dn1, 
        newBlock.getBlock()).getName().endsWith(
            newBlock.getBlock().getGenerationStamp() + ".meta"));

    // Validate the file
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));
    checkFullFile(p, newLength, contents);

    fs.delete(parent, true);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * shutdown the datanodes immediately after truncate.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesShutdownImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path parent = new Path("/test");
    final Path p = new Path(parent, "testTruncateWithDataNodesShutdownImmediately");

    writeContents(contents, startingFileSize, p);

    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    cluster.shutdownDataNodes();
    cluster.setDataNodesDead();
    try {
      for(int i = 0; i < SUCCESS_ATTEMPTS && cluster.isDataNodeUp(); i++) {
        Thread.sleep(SLEEP);
      }
      assertFalse("All DataNodes should be down.", cluster.isDataNodeUp());
      LocatedBlocks blocks = getLocatedBlocks(p);
      assertTrue(blocks.isUnderConstruction());
    } finally {
      cluster.startDataNodes(conf, DATANODE_NUM, true,
          StartupOption.REGULAR, null);
      cluster.waitActive();
    }
    checkBlockRecovery(p);

    fs.delete(parent, true);
  }

  /**
   * Check truncate recovery.
   */
  @Test
  public void testTruncateRecovery() throws IOException {
    FSNamesystem fsn = cluster.getNamesystem();
    String client = "client";
    String clientMachine = "clientMachine";
    Path parent = new Path("/test");
    String src = "/test/testTruncateRecovery";
    Path srcPath = new Path(src);

    byte[] contents = AppendTestUtil.initBuffer(BLOCK_SIZE);
    writeContents(contents, BLOCK_SIZE, srcPath);

    INodesInPath iip = getINodesInPath(src, fsn, cluster);
    INodeFile file = iip.getLastINode().asFile();
    long initialGenStamp = getLastBlock(file, fsn).getGenerationStamp();
    // Test that prepareFileForTruncate sets up in-place truncate.
    Block oldBlock = getLastBlock(file, fsn);
    Block truncateBlock = prepareFileForTruncate(file, iip, client, clientMachine, fsn); 
    // In-place truncate uses old block id with new genStamp.
    assertThat(truncateBlock.getBlockId(),
        is(equalTo(oldBlock.getBlockId())));
    assertThat(truncateBlock.getNumBytes(),
        is(oldBlock.getNumBytes()));
    assertThat(truncateBlock.getGenerationStamp(),
        is(oldBlock.getGenerationStamp()+1));
    assertThat(getLastBlock(file, fsn).getBlockUCState(),
        is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
    long blockRecoveryId = ((BlockInfoContiguousUnderConstruction) getLastBlock(file, fsn))
        .getBlockRecoveryId();
    assertThat(blockRecoveryId, is(initialGenStamp + 1));

    fs.delete(parent, true);
  }

  public INodesInPath getINodesInPath(final String src, final FSNamesystem fsn, final MiniDFSCluster cluster) throws IOException {
    HopsTransactionalRequestHandler getInodeHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_INODE) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ, TransactionLockTypes.INodeResolveType.PATH, src)
                .setNameNodeID(cluster.getNameNode().getId()).setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        return fsn.getFSDirectory().getINodesInPath4Write(src, true);

      }
    };
    return (INodesInPath) getInodeHandler.handle();
  }
   
  private BlockInfoContiguous getLastBlock(final INodeFile inode, final FSNamesystem fsn) throws IOException{
    return (BlockInfoContiguous) new HopsTransactionalRequestHandler(HDFSOperationType.TRUNCATE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, inode.getId())
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock()).add(
            lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.PE, LockFactory.BLK.UR, LockFactory.BLK.UC, LockFactory.BLK.IV));
        locks.add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
              .add(lf.getLeasePathLock(TransactionLockTypes.LockType.WRITE));
        
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        return inode.getLastBlock();
        
      }
    }.handle();
  }
  
  private Block prepareFileForTruncate(final INodeFile inode, final INodesInPath iip, final String client, final String clientMachine, final FSNamesystem fsn) throws IOException{
    return (Block) new HopsTransactionalRequestHandler(HDFSOperationType.TRUNCATE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, inode.getId())
            .setNameNodeID(fsn.getNamenodeId())
            .setActiveNameNodes(fsn.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock()).add(
            lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.PE, LockFactory.BLK.UR, LockFactory.BLK.UC, LockFactory.BLK.IV));
        locks.add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE, client))
              .add(lf.getLeasePathLock(TransactionLockTypes.LockType.WRITE));
        
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        return fsn.prepareFileForTruncate(iip, client, clientMachine, 1, null);
      }
    }.handle();
  }
 
  @Test
  public void testTruncate4Symlink() throws IOException {
    final int fileLength = 3 * BLOCK_SIZE;

    final Path parent = new Path("/test");
    fs.mkdirs(parent);
    final byte[] contents = AppendTestUtil.initBuffer(fileLength);
    final Path file = new Path(parent, "testTruncate4Symlink");
    writeContents(contents, fileLength, file);

    final Path link = new Path(parent, "link");
    fs.createSymlink(file, link, false);

    final int newLength = fileLength/3;
    boolean isReady = fs.truncate(link, newLength);

    Assert.assertTrue("Recovery is not expected.", isReady);

    FileStatus fileStatus = fs.getFileStatus(file);
    assertThat(fileStatus.getLen(), is((long) newLength));

    ContentSummary cs = fs.getContentSummary(parent);
    assertEquals("Bad disk space usage",
        cs.getSpaceConsumed(), newLength * REPLICATION);
    // validate the file content
    checkFullFile(file, newLength, contents);

    fs.delete(parent, true);
  }

  static void writeContents(byte[] contents, int fileLength, Path p)
      throws IOException {
    FSDataOutputStream out = fs.create(p, true, BLOCK_SIZE, REPLICATION,
        BLOCK_SIZE);
    out.write(contents, 0, fileLength);
    out.close();
  }

  static void checkBlockRecovery(Path p) throws IOException {
    checkBlockRecovery(p, fs);
  }

  public static void checkBlockRecovery(Path p, DistributedFileSystem dfs)
      throws IOException {
    boolean success = false;
    for(int i = 0; i < SUCCESS_ATTEMPTS; i++) {
      LocatedBlocks blocks = getLocatedBlocks(p, dfs);
      boolean noLastBlock = blocks.getLastLocatedBlock() == null;
      if(!blocks.isUnderConstruction() &&
          (noLastBlock || blocks.isLastBlockComplete())) {
        success = true;
        break;
      }
      try { Thread.sleep(SLEEP); } catch (InterruptedException ignored) {}
    }
    assertThat("inode should complete in ~" + SLEEP * SUCCESS_ATTEMPTS + " ms.",
        success, is(true));
  }

  static LocatedBlocks getLocatedBlocks(Path src) throws IOException {
    return getLocatedBlocks(src, fs);
  }

  static LocatedBlocks getLocatedBlocks(Path src, DistributedFileSystem dfs)
      throws IOException {
    return dfs.getClient().getLocatedBlocks(src.toString(), 0, Long.MAX_VALUE);
  }
  
  static void assertFileLength(Path file, long length) throws IOException {
    byte[] data = DFSTestUtil.readFileBuffer(fs, file);
    assertEquals("Wrong data size in snapshot.", length, data.length);
  }

  static void checkFullFile(Path p, int newLength, byte[] contents)
      throws IOException {
    AppendTestUtil.checkFullFile(fs, p, newLength, contents, p.toString());
  }

}
