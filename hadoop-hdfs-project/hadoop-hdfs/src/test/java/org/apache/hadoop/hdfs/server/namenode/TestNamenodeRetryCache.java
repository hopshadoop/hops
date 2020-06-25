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

import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.LightWeightCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ensuring the namenode retry cache works correctly for
 * non-idempotent requests.
 * 
 * Retry cache works based on tracking previously received request based on the
 * ClientId and CallId received in RPC requests and storing the response. The
 * response is replayed on retry when the same request is received again.
 * 
 * The test works by manipulating the Rpc {@link Server} current RPC call. For
 * testing retried requests, an Rpc callId is generated only once using
 * {@link #newCall()} and reused for many method calls. For testing non-retried
 * request, a new callId is generated using {@link #newCall()}.
 */
public class TestNamenodeRetryCache {
  private static final byte[] CLIENT_ID = ClientId.getClientId();
  private static MiniDFSCluster cluster;
  private static NamenodeProtocols nnRpc;
  private static final FsPermission perm = FsPermission.getDefault();
  private static DistributedFileSystem filesystem;
  private static int callId = 100;
  private static Configuration conf;
  private static final int BlockSize = 512;
  private static final int expirityTime = 30000;
  
  /** Start a cluster */
  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY, 60000);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    nnRpc = cluster.getNameNode().getRpcServer();
    filesystem = cluster.getFileSystem();
  }
  
  /** Cleanup after the test 
   * @throws IOException 
   * @throws UnresolvedLinkException 
   * @throws SafeModeException 
   * @throws AccessControlException */
  @After
  public void cleanup() throws IOException {
    cluster.shutdown();
  }
  
  public static void incrementCallId() {
    callId++;
  }
  
  /** Set the current Server RPC call */
  public static void newCall() {
    Server.Call call = new Server.Call(++callId, 1, null, null,
        RpcKind.RPC_PROTOCOL_BUFFER, CLIENT_ID, Client.getRpcEpochSec());
    Server.getCurCall().set(call);
  }
  
  public static void resetCall() {
    Server.Call call = new Server.Call(RpcConstants.INVALID_CALL_ID, 1, null,
        null, RpcKind.RPC_PROTOCOL_BUFFER, RpcConstants.DUMMY_CLIENT_ID,
            RpcConstants.INVALID_EPOCH);
    Server.getCurCall().set(call);
  }
  
  private void concatSetup(String file1, String file2) throws Exception {
    DFSTestUtil.createFile(filesystem, new Path(file1), BlockSize, (short)1, 0L);
    DFSTestUtil.createFile(filesystem, new Path(file2), BlockSize, (short)1, 0L);
  }
  
  /**
   * Tests for concat call
   */
  @Test
  public void testConcat() throws Exception {
    resetCall();
    String file1 = "/testNamenodeRetryCache/testConcat/file1";
    String file2 = "/testNamenodeRetryCache/testConcat/file2";
    
    // Two retried concat calls succeed
    concatSetup(file1, file2);
    newCall();
    nnRpc.concat(file1, new String[]{file2});
    nnRpc.concat(file1, new String[]{file2});
    nnRpc.concat(file1, new String[]{file2});
    
    // A non-retried concat request fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      nnRpc.concat(file1, new String[]{file2});
      Assert.fail("testConcat - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
  
  /**
   * Tests for delete call
   */
  @Test
  public void testDelete() throws Exception {
    String dir = "/testNamenodeRetryCache/testDelete";
    // Two retried calls to create a non existent file
    newCall();
    nnRpc.mkdirs(dir, perm, true);
    newCall();
    Assert.assertTrue(nnRpc.delete(dir, false));
    Assert.assertTrue(nnRpc.delete(dir, false));
    Assert.assertTrue(nnRpc.delete(dir, false));
    
    // non-retried call fails and gets false as return
    newCall();
    Assert.assertFalse(nnRpc.delete(dir, false));
  }
  
  /**
   * Test for createSymlink
   */
  @Test
  public void testCreateSymlink() throws Exception {
    String target = "/testNamenodeRetryCache/testCreateSymlink/target";
    
    // Two retried symlink calls succeed
    newCall();
    nnRpc.createSymlink(target, "/a/b", perm, true);
    nnRpc.createSymlink(target, "/a/b", perm, true);
    nnRpc.createSymlink(target, "/a/b", perm, true);
    
    // non-retried call fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      nnRpc.createSymlink(target, "/a/b", perm, true);
      Assert.fail("testCreateSymlink - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
  
  /**
   * Test for create file
   */
  @Test
  public void testCreate() throws Exception {
    String src = "/testNamenodeRetryCache/testCreate/file";
    // Two retried calls succeed
    newCall();
        HdfsFileStatus status = nnRpc.create(src, perm, "holder",
      new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true,
      (short) 1, BlockSize, null, null);
    Assert.assertEquals(status.getFileId(), nnRpc.create(src, perm, 
        "holder", new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
        true, (short) 1, BlockSize, null, null).getFileId());
    Assert.assertEquals(status.getFileId(), nnRpc.create(src, perm,
        "holder", new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
        true, (short) 1, BlockSize, null, null).getFileId());
    
    // A non-retried call fails
    newCall();
    try {
      nnRpc.create(src, perm, "holder", 
          new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 1, BlockSize, null, null);
      Assert.fail("testCreate - expected exception is not thrown");
    } catch (IOException e) {
      // expected
    }
  }
  
  /**
   * Test for rename1
   */
  @Test
  public void testAppend() throws Exception {
    String src = "/testNamenodeRetryCache/testAppend/src";
    resetCall();
    // Create a file with partial block
    DFSTestUtil.createFile(filesystem, new Path(src), 128, (short)1, 0L);
    
    // Retried append requests succeed
    newCall();
    LastBlockWithStatus b = nnRpc.append(src, "holder",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND)));
    Assert.assertEquals(b.getLastBlock().getBlock().getBlockId(), nnRpc.append(src, "holder",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND))).getLastBlock().getBlock().getBlockId());
    Assert.assertEquals(b.getLastBlock().getBlock().getBlockId(), nnRpc.append(src, "holder",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND))).getLastBlock().getBlock().getBlockId());
    
    // non-retried call fails
    newCall();
    try {
      nnRpc.append(src, "holder",
          new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND)));
      Assert.fail("testAppend - expected exception is not thrown");
    } catch (Exception e) {
      // Expected
    }
  }
  
  /**
   * Test for rename1
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testRename1() throws Exception {
    String src = "/testNamenodeRetryCache/testRename1/src";
    String target = "/testNamenodeRetryCache/testRename1/target";
    resetCall();
    nnRpc.mkdirs(src, perm, true);
    
    // Retried renames succeed
    newCall();
    Assert.assertTrue(nnRpc.rename(src, target));
    Assert.assertTrue(nnRpc.rename(src, target));
    Assert.assertTrue(nnRpc.rename(src, target));
    
    // A non-retried request fails
    newCall();
    Assert.assertFalse(nnRpc.rename(src, target));
  }
  
  /**
   * Test for rename2
   */
  @Test
  public void testRename2() throws Exception {
    String src = "/testNamenodeRetryCache/testRename2/src";
    String target = "/testNamenodeRetryCache/testRename2/target";
    resetCall();
    nnRpc.mkdirs(src, perm, true);
    
    // Retried renames succeed
    newCall();
    nnRpc.rename2(src, target, Rename.NONE);
    nnRpc.rename2(src, target, Rename.NONE);
    nnRpc.rename2(src, target, Rename.NONE);
    
    // A non-retried request fails
    newCall();
    try {
      nnRpc.rename2(src, target, Rename.NONE);
      Assert.fail("testRename 2 expected exception is not thrown");
    } catch (IOException e) {
      // expected
    }
  }
  
  /**
   * Make sure a retry call does not hang because of the exception thrown in the
   * first call.
   */
  @Test(timeout = 60000)
  public void testUpdatePipelineWithFailOver() throws Exception {
    cluster.shutdown();
    nnRpc = null;
    filesystem = null;
    cluster = new MiniDFSCluster.Builder(conf).nnTopology(
        MiniDFSNNTopology.simpleHOPSTopology(2)).numDataNodes(1).build();
    cluster.waitActive();
    NamenodeProtocols ns0 = cluster.getNameNodeRpc(0);
    ExtendedBlock oldBlock = new ExtendedBlock();
    ExtendedBlock newBlock = new ExtendedBlock();
    DatanodeID[] newNodes = new DatanodeID[2];
    String[] newStorages = new String[2];
    
    cluster.shutdownNameNode(0);
    cluster.shutdownNameNode(1);
    
    newCall();
    try {
      ns0.updatePipeline("testClient", oldBlock, newBlock, newNodes, newStorages);
      Assert.fail("Expect StandbyException from the updatePipeline call");
    } catch (IOException e) {
      // expected, since in the beginning both nn are in standby state
      GenericTestUtils.assertExceptionContains(
          "does not exist or is not under Construction", e);
    }
    
    cluster.restartNameNodes();
    try {
      ns0.updatePipeline("testClient", oldBlock, newBlock, newNodes, newStorages);
    } catch (IOException e) {
      // ignore call should not hang.
    }
  }
  
  /**
   * After run a set of operations, restart NN and check if the retry cache has
   * been rebuilt based on the editlog.
   */
  @Test
  public void testRetryCacheRebuild() throws Exception {
    DFSTestUtil.runOperations(cluster, filesystem, conf, BlockSize, 0);
    
    FSNamesystem namesystem = cluster.getNamesystem();
    List<RetryCacheEntry> cacheSet = namesystem.getCacheSet();
    assertEquals(22, cacheSet.size());
    
    Map<RetryCacheEntry, RetryCacheEntry> oldEntries =
        new HashMap<>();
    Iterator<RetryCacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      RetryCacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // restart NameNode
    cluster.restartNameNode();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    
    // check retry cache
    assertTrue(namesystem.hasRetryCache());
    cacheSet = namesystem.getCacheSet();
    assertEquals(22, cacheSet.size());
    iter = cacheSet.iterator();
    FSNamesystem.LOG.info("Checking: "+ Arrays.toString(oldEntries.keySet().toArray()));
    while (iter.hasNext()) {
      RetryCacheEntry entry = iter.next();
      FSNamesystem.LOG.info("Checking "+entry);
      assertTrue(oldEntries.containsKey(entry));
    }
  }
  
  @Test
  public void testRetryCacheCleaning() throws Exception {
    DFSTestUtil.runOperations(cluster, filesystem, conf, BlockSize, 0);
    
    FSNamesystem namesystem = cluster.getNamesystem();
    List<RetryCacheEntry> cacheSet = namesystem.getCacheSet();
    assertEquals(22, cacheSet.size());
    
    Map<RetryCacheEntry, RetryCacheEntry> oldEntries =
        new HashMap<>();
    Iterator<RetryCacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      RetryCacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // restart NameNode
    cluster.restartNameNode();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    
    Thread.sleep(expirityTime*3);
    
    // check retry cache
    assertTrue(namesystem.hasRetryCache());
    cacheSet = namesystem.getCacheSet();
    assertEquals(0, cacheSet.size());
  }
  
 
  @Test
  public void testSetXAttr() throws Exception {
    resetCall();
    String src = "/testNamenodeRetryCache/testSetXAttr/src";
    // Create a file with partial block
    DFSTestUtil.createFile(filesystem, new Path(src), 128, (short)1, 0L);
  
    XAttr xAttr = XAttrHelper.buildXAttr("user.test", "test".getBytes());
    
    newCall();
    nnRpc.setXAttr(src, xAttr, EnumSet.of(XAttrSetFlag.CREATE));
    nnRpc.setXAttr(src, xAttr, EnumSet.of(XAttrSetFlag.CREATE));
    nnRpc.setXAttr(src, xAttr, EnumSet.of(XAttrSetFlag.CREATE));
    
    // A non-retried setXAttr request fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      nnRpc.setXAttr(src, xAttr, EnumSet.of(XAttrSetFlag.CREATE));
      Assert.fail("testSetXAttr - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
  
  @Test
  public void testRemoveXAttr() throws Exception {
    resetCall();
    String src = "/testNamenodeRetryCache/testRemoveXAttr/src";
    // Create a file with partial block
    DFSTestUtil.createFile(filesystem, new Path(src), 128, (short)1, 0L);
    XAttr xAttr = XAttrHelper.buildXAttr("user.test", "test".getBytes());
    
    filesystem.setXAttr(new Path(src), XAttrHelper.getPrefixName(xAttr),
        xAttr.getValue());
    
    newCall();
    nnRpc.removeXAttr(src, xAttr);
    nnRpc.removeXAttr(src, xAttr);
    nnRpc.removeXAttr(src, xAttr);
    
    // A non-retried removeXAttr request fails
    newCall();
    try {
      // Second non-retry call should fail with an exception
      nnRpc.removeXAttr(src, xAttr);
      Assert.fail("testRemoveXAttr - expected exception is not thrown");
    } catch (IOException e) {
      // Expected
    }
  }
}
