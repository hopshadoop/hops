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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

/**
 * This class tests the client connection caching in a single node
 * mini-cluster.
 */
public class TestConnCache {
  static final Log LOG = LogFactory.getLog(TestConnCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;
  final static int CACHE_SIZE = 4;
  final static long CACHE_EXPIRY_MS = 200;
  static Configuration conf = null;
  static MiniDFSCluster cluster = null;
  static FileSystem fs = null;
  static SocketCache cache;

  static final Path testFile = new Path("/testConnCache.dat");
  static byte authenticData[] = null;

  static BlockReaderTestUtil util = null;


  /**
   * A mock Answer to remember the BlockReader used.
   * <p/>
   * It verifies that all invocation to DFSInputStream.getBlockReader()
   * use the same socket.
   */
  private class MockGetBlockReader implements Answer<RemoteBlockReader2> {
    public RemoteBlockReader2 reader = null;
    private Socket sock = null;

    @Override
    public RemoteBlockReader2 answer(InvocationOnMock invocation)
        throws Throwable {
      RemoteBlockReader2 prevReader = reader;
      reader = (RemoteBlockReader2) invocation.callRealMethod();
      if (sock == null) {
        sock = reader.dnSock;
      } else if (prevReader != null) {
        assertSame("DFSInputStream should use the same socket", sock,
            reader.dnSock);
      }
      return reader;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int REPLICATION_FACTOR = 1;

    /* create a socket cache. There is only one socket cache per jvm */
    cache = SocketCache.getInstance(CACHE_SIZE, CACHE_EXPIRY_MS);

    util = new BlockReaderTestUtil(REPLICATION_FACTOR);
    cluster = util.getCluster();
    conf = util.getConf();
    fs = cluster.getFileSystem();

    authenticData = util.writeFile(testFile, FILE_SIZE / 1024);
  }


  /**
   * (Optionally) seek to position, read and verify data.
   * <p/>
   * Seek to specified position if pos is non-negative.
   */
  private void pread(DFSInputStream in, long pos, byte[] buffer, int offset,
      int length) throws IOException {
    assertTrue("Test buffer too small", buffer.length >= offset + length);

    if (pos >= 0) {
      in.seek(pos);
    }

    LOG.info("Reading from file of size " + in.getFileLength() +
        " at offset " + in.getPos());

    while (length > 0) {
      int cnt = in.read(buffer, offset, length);
      assertTrue("Error in read", cnt > 0);
      offset += cnt;
      length -= cnt;
    }

    // Verify
    for (int i = 0; i < length; ++i) {
      byte actual = buffer[i];
      byte expect = authenticData[(int) pos + i];
      assertEquals("Read data mismatch at file offset " + (pos + i) +
              ". Expects " + expect + "; got " + actual, actual, expect);
    }
  }

  /**
   * Test the SocketCache itself.
   */
  @Test
  public void testSocketCache() throws Exception {
    // Make a client
    InetSocketAddress nnAddr =
        new InetSocketAddress("localhost", cluster.getNameNodePort());
    DFSClient client = new DFSClient(nnAddr, conf);

    // Find out the DN addr
    LocatedBlock block = client.getNamenode()
        .getBlockLocations(testFile.toString(), 0, FILE_SIZE).getLocatedBlocks()
        .get(0);
    DataNode dn = util.getDataNode(block);
    InetSocketAddress dnAddr = dn.getXferAddress();


    // Make some sockets to the DN
    Socket[] dnSockets = new Socket[CACHE_SIZE];
    for (int i = 0; i < dnSockets.length; ++i) {
      dnSockets[i] = client.socketFactory
          .createSocket(dnAddr.getAddress(), dnAddr.getPort());
    }


    // Insert a socket to the NN
    Socket nnSock = new Socket(nnAddr.getAddress(), nnAddr.getPort());
    cache.put(nnSock, null);
    assertSame("Read the write", nnSock, cache.get(nnAddr).sock);
    cache.put(nnSock, null);

    // Insert DN socks
    for (Socket dnSock : dnSockets) {
      cache.put(dnSock, null);
    }

    assertEquals("NN socket evicted", null, cache.get(nnAddr));
    assertTrue("Evicted socket closed", nnSock.isClosed());

    // Lookup the DN socks
    for (Socket dnSock : dnSockets) {
      assertEquals("Retrieve cached sockets", dnSock, cache.get(dnAddr).sock);
      dnSock.close();
    }

    assertEquals("Cache is empty", 0, cache.size());
  }


  /**
   * Test the SocketCache expiry.
   * Verify that socket cache entries expire after the set
   * expiry time.
   */
  @Test
  public void testSocketCacheExpiry() throws Exception {
    // Make a client
    InetSocketAddress nnAddr =
        new InetSocketAddress("localhost", cluster.getNameNodePort());
    DFSClient client = new DFSClient(nnAddr, conf);

    // Find out the DN addr
    LocatedBlock block = client.getNamenode()
        .getBlockLocations(testFile.toString(), 0, FILE_SIZE).getLocatedBlocks()
        .get(0);
    DataNode dn = util.getDataNode(block);
    InetSocketAddress dnAddr = dn.getXferAddress();


    // Make some sockets to the DN and put in cache
    Socket[] dnSockets = new Socket[CACHE_SIZE];
    for (int i = 0; i < dnSockets.length; ++i) {
      dnSockets[i] = client.socketFactory
          .createSocket(dnAddr.getAddress(), dnAddr.getPort());
      cache.put(dnSockets[i], null);
    }

    // Client side still has the sockets cached
    assertEquals(CACHE_SIZE, client.socketCache.size());

    //sleep for a second and see if it expired
    Thread.sleep(CACHE_EXPIRY_MS + 1000);
    
    // Client side has no sockets cached
    assertEquals(0, client.socketCache.size());

    //sleep for another second and see if 
    //the daemon thread runs fine on empty cache
    Thread.sleep(CACHE_EXPIRY_MS + 1000);
  }


  /**
   * Read a file served entirely from one DN. Seek around and read from
   * different offsets. And verify that they all use the same socket.
   *
   * @throws java.io.IOException
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testReadFromOneDN() throws IOException {
    LOG.info("Starting testReadFromOneDN()");
    DFSClient client = new DFSClient(
        new InetSocketAddress("localhost", cluster.getNameNodePort()), conf);
    DFSInputStream in = spy(client.open(testFile.toString()));
    LOG.info("opened " + testFile.toString());

    byte[] dataBuf = new byte[BLOCK_SIZE];

    MockGetBlockReader answer = new MockGetBlockReader();
    Mockito.doAnswer(answer).when(in)
        .getBlockReader((InetSocketAddress) Matchers.anyObject(),
            (DatanodeInfo) Matchers.anyObject(), Matchers.anyString(),
            (LocatedBlock) Matchers.anyObject(),
            (Token<BlockTokenIdentifier>) Matchers.anyObject(),
            Matchers.anyLong(), Matchers.anyLong(), Matchers.anyInt(),
            Matchers.anyBoolean(), Matchers.anyString());

    // Initial read
    pread(in, 0, dataBuf, 0, dataBuf.length);
    // Read again and verify that the socket is the same
    pread(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length);
    pread(in, 1024, dataBuf, 0, dataBuf.length);
    pread(in, -1, dataBuf, 0, dataBuf.length);            // No seek; just read
    pread(in, 64, dataBuf, 0, dataBuf.length / 2);

    in.close();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdown();
  }
}
