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
package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.ShortCircuitCache;
import org.apache.hadoop.hdfs.client.ShortCircuitReplica;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockReaderLocal {
  private static TemporarySocketDirectory sockDir;
  
  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }
  
  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }
  
  public static void assertArrayRegionsEqual(byte []buf1, int off1, byte []buf2,
      int off2, int len) {
    for (int i = 0; i < len; i++) {
      if (buf1[off1 + i] != buf2[off2 + i]) {
        Assert.fail("arrays differ at byte " +  i + ". " + 
          "The first array has " + (int)buf1[off1 + i] + 
          ", but the second array has " + (int)buf2[off2 + i]);
      }
    }
  }

  /**
   * Similar to IOUtils#readFully(). Reads bytes in a loop.
   *
   * @param reader           The BlockReaderLocal to read bytes from
   * @param buf              The ByteBuffer to read into
   * @param off              The offset in the buffer to read into
   * @param len              The number of bytes to read.
   * 
   * @throws IOException     If it could not read the requested number of bytes
   */
  private static void readFully(BlockReaderLocal reader,
      ByteBuffer buf, int off, int len) throws IOException {
    int amt = len;
    while (amt > 0) {
      buf.limit(off + len);
      buf.position(off);
      long ret = reader.read(buf);
      if (ret < 0) {
        throw new EOFException( "Premature EOF from BlockReaderLocal " +
            "after reading " + (len - amt) + " byte(s).");
      }
      amt -= ret;
      off += ret;
    }
  }

  private static class BlockReaderLocalTest {
    final static int TEST_LENGTH = 12345;
    final static int BYTES_PER_CHECKSUM = 512;

    public void setConfiguration(HdfsConfiguration conf) {
      // default: no-op
    }
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
      // default: no-op
    }
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      // default: no-op
    }
  }
  
  public void runBlockReaderLocalTest(BlockReaderLocalTest test,
      boolean checksum, long readahead) throws IOException {
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, !checksum);
    conf.setLong(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");
    conf.setLong(DFSConfigKeys.DFS_CLIENT_CACHE_READAHEAD, readahead);
    test.setConfiguration(conf);
    FileInputStream dataIn = null, metaIn = null;
    final Path TEST_PATH = new Path("/a");
    final long RANDOM_SEED = 4567L;
    BlockReaderLocal blockReaderLocal = null;
    FSDataInputStream fsIn = null;
    byte original[] = new byte[BlockReaderLocalTest.TEST_LENGTH];
    
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          BlockReaderLocalTest.TEST_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      IOUtils.readFully(fsIn, original, 0,
          BlockReaderLocalTest.TEST_LENGTH);
      fsIn.close();
      fsIn = null;
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      File dataFile = MiniDFSCluster.getBlockFile(0, block);
      File metaFile = MiniDFSCluster.getBlockMetadataFile(0, block);

      DatanodeID datanodeID = cluster.getDataNodes().get(0).getDatanodeId();
      ShortCircuitCache shortCircuitCache =
          ClientContext.getFromConf(conf).getShortCircuitCache();
      cluster.shutdown();
      cluster = null;
      test.setup(dataFile, checksum);
      FileInputStream streams[] = {
          new FileInputStream(dataFile),
          new FileInputStream(metaFile)
      };
      dataIn = streams[0];
      metaIn = streams[1];
      ExtendedBlockId key = new ExtendedBlockId(block.getBlockId(), block.getBlockPoolId());
      ShortCircuitReplica replica = new ShortCircuitReplica(
          key, dataIn, metaIn, shortCircuitCache, Time.now());
      blockReaderLocal = new BlockReaderLocal.Builder(
              new DFSClient.Conf(conf)).
          setFilename(TEST_PATH.getName()).
          setBlock(block).
          setShortCircuitReplica(replica).
          setDatanodeID(datanodeID).
          setCachingStrategy(new CachingStrategy(false, readahead)).
          setVerifyChecksum(checksum).
          build();
      dataIn = null;
      metaIn = null;
      test.doTest(blockReaderLocal, original);
      // BlockReaderLocal should not alter the file position.
      Assert.assertEquals(0, streams[0].getChannel().position());
      Assert.assertEquals(0, streams[1].getChannel().position());
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
      if (dataIn != null) dataIn.close();
      if (metaIn != null) metaIn.close();
      if (blockReaderLocal != null) blockReaderLocal.close();
    }
  }
  
  private static class TestBlockReaderLocalImmediateClose 
      extends BlockReaderLocalTest {
  }
  
  @Test
  public void testBlockReaderLocalImmediateClose() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), true, 0);
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), false, 0);
  }
  
  private static class TestBlockReaderSimpleReads 
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 512);
      assertArrayRegionsEqual(original, 0, buf, 0, 512);
      reader.readFully(buf, 512, 512);
      assertArrayRegionsEqual(original, 512, buf, 512, 512);
      reader.readFully(buf, 1024, 513);
      assertArrayRegionsEqual(original, 1024, buf, 1024, 513);
      reader.readFully(buf, 1537, 514);
      assertArrayRegionsEqual(original, 1537, buf, 1537, 514);
      // Readahead is always at least the size of one chunk in this test.
      Assert.assertTrue(reader.getMaxReadaheadLength() >=
          BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    }
  }
  
  @Test
  public void testBlockReaderSimpleReads() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderSimpleReadsShortReadahead() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true,
        BlockReaderLocalTest.BYTES_PER_CHECKSUM - 1);
  }

  @Test
  public void testBlockReaderSimpleReadsNoChecksum() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), false,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderSimpleReadsNoReadahead() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true, 0);
  }

  @Test
  public void testBlockReaderSimpleReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), false, 0);
  }
  
  private static class TestBlockReaderLocalArrayReads2 
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 10);
      assertArrayRegionsEqual(original, 0, buf, 0, 10);
      reader.readFully(buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf, 10, 100);
      reader.readFully(buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf, 110, 700);
      reader.readFully(buf, 810, 1); // from offset 810 to offset 811
      reader.readFully(buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf, 811, 5);
      reader.readFully(buf, 816, 900); // skip from offset 816 to offset 1716
      reader.readFully(buf, 1716, 5);
      assertArrayRegionsEqual(original, 1716, buf, 1716, 5);
    }
  }
  
  @Test
  public void testBlockReaderLocalArrayReads2() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(),
        true, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(),
        false, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(), true, 0);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(), false, 0);
  }

  private static class TestBlockReaderLocalByteBufferReads 
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      readFully(reader, buf, 0, 10);
      assertArrayRegionsEqual(original, 0, buf.array(), 0, 10);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }
  
  @Test
  public void testBlockReaderLocalByteBufferReads()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
        true, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferReads(),
        false, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }
  
  @Test
  public void testBlockReaderLocalByteBufferReadsNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
        false, 0);
  }

  /**
   * Test reads that bypass the bounce buffer (because they are aligned
   * and bigger than the readahead).
   */
  private static class TestBlockReaderLocalByteBufferFastLaneReads 
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      ByteBuffer buf = ByteBuffer.allocateDirect(TEST_LENGTH);
      readFully(reader, buf, 0, 5120);
      buf.flip();
      assertArrayRegionsEqual(original, 0,
          DFSTestUtil.asArray(buf), 0,
          5120);
      reader.skip(1537);
      readFully(reader, buf, 0, 1);
      buf.flip();
      assertArrayRegionsEqual(original, 6657,
          DFSTestUtil.asArray(buf), 0,
          1);
      reader.setMlocked(true);
      readFully(reader, buf, 0, 5120);
      buf.flip();
      assertArrayRegionsEqual(original, 6658,
          DFSTestUtil.asArray(buf), 0,
          5120);
      reader.setMlocked(false);
      readFully(reader, buf, 0, 513);
      buf.flip();
      assertArrayRegionsEqual(original, 11778,
          DFSTestUtil.asArray(buf), 0,
          513);
      reader.skip(3);
      readFully(reader, buf, 0, 50);
      buf.flip();
      assertArrayRegionsEqual(original, 12294,
          DFSTestUtil.asArray(buf), 0,
          50);
    }
  }
  
  @Test
  public void testBlockReaderLocalByteBufferFastLaneReads()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        true, 2 * BlockReaderLocalTest.BYTES_PER_CHECKSUM);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferFastLaneReads(),
        false, 2 * BlockReaderLocalTest.BYTES_PER_CHECKSUM);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        false, 0);
  }

  private static class TestBlockReaderLocalReadCorruptStart
      extends BlockReaderLocalTest {
    boolean usingChecksums = false;
    @Override
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
      RandomAccessFile bf = null;
      this.usingChecksums = usingChecksums;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      if (usingChecksums) {
        try {
          reader.readFully(buf, 0, 10);
          Assert.fail("did not detect corruption");
        } catch (IOException e) {
          // expected
        }
      } else {
        reader.readFully(buf, 0, 10);
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalReadCorruptStart()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorruptStart(), true,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }
  
  private static class TestBlockReaderLocalReadCorrupt
      extends BlockReaderLocalTest {
    boolean usingChecksums = false;
    @Override
    public void setup(File blockFile, boolean usingChecksums) 
        throws IOException {
      RandomAccessFile bf = null;
      this.usingChecksums = usingChecksums;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.seek(1539);
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      try {
        reader.readFully(buf, 0, 10);
        assertArrayRegionsEqual(original, 0, buf, 0, 10);
        reader.readFully(buf, 10, 100);
        assertArrayRegionsEqual(original, 10, buf, 10, 100);
        reader.readFully(buf, 110, 700);
        assertArrayRegionsEqual(original, 110, buf, 110, 700);
        reader.skip(1); // skip from offset 810 to offset 811
        reader.readFully(buf, 811, 5);
        assertArrayRegionsEqual(original, 811, buf, 811, 5);
        reader.readFully(buf, 816, 900);
        if (usingChecksums) {
          // We should detect the corruption when using a checksum file.
          Assert.fail("did not detect corruption");
        }
      } catch (ChecksumException e) {
        if (!usingChecksums) {
          Assert.fail("didn't expect to get ChecksumException: not " +
              "using checksums.");
        }
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalReadCorrupt()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), true,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), false,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), true, 0);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), false, 0);
  }

  private static class TestBlockReaderLocalWithMlockChanges
      extends BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
    }
    
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      reader.skip(1);
      readFully(reader, buf, 1, 9);
      assertArrayRegionsEqual(original, 1, buf.array(), 1, 9);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      reader.setMlocked(true);
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.setMlocked(false);
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }

  @Test
  public void testBlockReaderLocalWithMlockChanges()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        true, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        false, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        false, 0);
  }

  private static class TestBlockReaderLocalOnFileWithoutChecksum
      extends BlockReaderLocalTest {
    @Override
    public void setConfiguration(HdfsConfiguration conf) {
      conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "NULL");
    }

    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      Assert.assertTrue(!reader.getVerifyChecksum());
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      reader.skip(1);
      readFully(reader, buf, 1, 9);
      assertArrayRegionsEqual(original, 1, buf.array(), 1, 9);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      reader.setMlocked(true);
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.setMlocked(false);
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }
  
  private static class TestBlockReaderLocalReadZeroBytes
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte emptyArr[] = new byte[0];
      Assert.assertEquals(0, reader.read(emptyArr, 0, 0));
      ByteBuffer emptyBuf = ByteBuffer.wrap(emptyArr);
      Assert.assertEquals(0, reader.read(emptyBuf));
      reader.skip(1);
      Assert.assertEquals(0, reader.read(emptyArr, 0, 0));
      Assert.assertEquals(0, reader.read(emptyBuf));
      reader.skip(BlockReaderLocalTest.TEST_LENGTH - 1);
      Assert.assertEquals(-1, reader.read(emptyArr, 0, 0));
      Assert.assertEquals(-1, reader.read(emptyBuf));
    }
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        true, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        false, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        false, 0);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytes()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        true, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        false, DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        false, 0);
  }
  
  @Test(timeout=60000)
  public void TestStatisticsForShortCircuitLocalRead() throws Exception {
    testStatistics(true);
  }

  @Test(timeout=60000)
  public void TestStatisticsForLocalRead() throws Exception {
    testStatistics(false);
  }
  
  private void testStatistics(boolean isShortCircuit) throws Exception {
    Assume.assumeTrue(DomainSocket.getLoadingFailureReason() == null);
    HdfsConfiguration conf = new HdfsConfiguration();
    TemporarySocketDirectory sockDir = null;
    if (isShortCircuit) {
      DFSInputStream.tcpReadsDisabledForTesting = true;
      sockDir = new TemporarySocketDirectory();
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(), "TestStatisticsForLocalRead.%d.sock").
          getAbsolutePath());
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
      DomainSocket.disableBindPathValidation();
    } else {
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, false);
    }
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final long RANDOM_SEED = 4567L;
    FSDataInputStream fsIn = null;
    byte original[] = new byte[BlockReaderLocalTest.TEST_LENGTH];
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          BlockReaderLocalTest.TEST_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      IOUtils.readFully(fsIn, original, 0,
          BlockReaderLocalTest.TEST_LENGTH);
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH, 
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH, 
          dfsIn.getReadStatistics().getTotalLocalBytesRead());
      if (isShortCircuit) {
        Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH, 
            dfsIn.getReadStatistics().getTotalShortCircuitBytesRead());
      } else {
        Assert.assertEquals(0,
            dfsIn.getReadStatistics().getTotalShortCircuitBytesRead());
      }
      fsIn.close();
      fsIn = null;
    } finally {
      DFSInputStream.tcpReadsDisabledForTesting = false;
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
      if (sockDir != null) sockDir.close();
    }
  }
}
