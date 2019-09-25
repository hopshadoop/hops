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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;

import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verify that incremental block reports are generated in response to
 * block additions/deletions.
 */
public class TestIncrementalBlockReports {
  public static final Log LOG = LogFactory.getLog(TestIncrementalBlockReports.class);

  private static final short DN_COUNT = 1;
  private static final long DUMMY_BLOCK_ID = 5678;
  private static final long DUMMY_BLOCK_LENGTH = 1024 * 1024;
  private static final long DUMMY_BLOCK_GENSTAMP = 1000;
  private static final short NON_EXISTING_BUCKET_ID  = Block.NON_EXISTING_BUCKET_ID;

  private MiniDFSCluster cluster = null;
  private DistributedFileSystem fs;
  private Configuration conf;
  private NameNode singletonNn;
  private DataNode singletonDn;
  private BPOfferService bpos;    // BPOS to use for block injection.
  private BPServiceActor actor;   // BPSA to use for block injection.
  private String storageUuid;     // DatanodeStorage to use for block injection.

  @Before
  public void startCluster() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DN_COUNT).build();
    fs = cluster.getFileSystem();
    singletonNn = cluster.getNameNode();
    singletonDn = cluster.getDataNodes().get(0);
    bpos = singletonDn.getAllBpOs().get(0);
    actor = bpos.getBPServiceActors().get(0);
    storageUuid = singletonDn.getFSDataset().getVolumes().get(0).getStorageID();
  }

  private static Block getDummyBlock() {
    return new Block(DUMMY_BLOCK_ID, DUMMY_BLOCK_LENGTH, DUMMY_BLOCK_GENSTAMP, NON_EXISTING_BUCKET_ID);
  }

  /**
   * Inject a fake 'received' block into the BPServiceActor state.
   */
  private void injectBlockReceived() {
    ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(
        getDummyBlock(), BlockStatus.RECEIVED_BLOCK , null);
    bpos.notifyNamenodeBlockInt(rdbi, storageUuid, true);
  }

  /**
   * Inject a fake 'deleted' block into the BPServiceActor state.
   */
  private void injectBlockDeleted() {
    ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(
        getDummyBlock(), BlockStatus.DELETED_BLOCK, null);
    bpos.notifyNamenodeDeletedBlockInt(rdbi, bpos.getDataNode().getFSDataset().getStorage(storageUuid));
  }

  /**
   * Spy on calls from the DN to the NN.
   * @return spy object that can be used for Mockito verification.
   */
  DatanodeProtocolClientSideTranslatorPB spyOnDnCallsToNn() {
    return DataNodeTestUtils.spyOnBposToNN(singletonDn, singletonNn);
  }

  /**
   * Ensure that an IBR is generated immediately for a block received by
   * the DN.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test (timeout=60000)
  public void testReportBlockReceived() throws InterruptedException, IOException {
    try {
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockReceived();

      // Sleep for a very short time, this is necessary since the IBR is
      // generated asynchronously.
      Thread.sleep(2000);

      // Ensure that the received block was reported immediately.
      Mockito.verify(nnSpy, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));
    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Ensure that a delayed IBR is generated for a block deleted on the DN.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test (timeout=60000)
  public void testReportBlockDeleted() throws InterruptedException, IOException {
    try {
      // Trigger a block report to reset the IBR timer.
      DataNodeTestUtils.triggerBlockReport(singletonDn);

      // Spy on calls from the DN to the NN
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockDeleted();

      // Sleep for a very short time since IBR is generated
      // asynchronously.
      Thread.sleep(2000);

      // Ensure that no block report was generated immediately.
      // Deleted blocks are reported when the IBR timer elapses.
      Mockito.verify(nnSpy, times(0)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

      // Trigger a heartbeat, this also triggers an IBR.
      DataNodeTestUtils.triggerHeartbeat(singletonDn);
      Thread.sleep(2000);

      // Ensure that the deleted block is reported.
      Mockito.verify(nnSpy, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Add a received block entry and then replace it. Ensure that a single
   * IBR is generated and that pending receive request state is cleared.
   * This test case verifies the failure in HDFS-5922.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test (timeout=60000)
  public void testReplaceReceivedBlock() throws InterruptedException, IOException {
    try {
      // Spy on calls from the DN to the NN
      DatanodeProtocolClientSideTranslatorPB nnSpy = spyOnDnCallsToNn();
      injectBlockReceived();
      injectBlockReceived();    // Overwrite the existing entry.

      // Sleep for a very short time since IBR is generated
      // asynchronously.
      Thread.sleep(2000);

      // Ensure that the received block is reported.
      Mockito.verify(nnSpy, atLeastOnce()).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));

      // Ensure that no more IBRs are pending.
      assertFalse(bpos.hasPendingIBR());

    } finally {
      cluster.shutdown();
      cluster = null;
    }
  }
}
