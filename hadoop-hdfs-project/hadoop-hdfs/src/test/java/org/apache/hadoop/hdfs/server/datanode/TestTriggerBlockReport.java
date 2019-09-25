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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test manually requesting that the DataNode send a block report.
 */
public final class TestTriggerBlockReport {
  private void testTriggerBlockReport(boolean incremental) throws Exception {
    Configuration conf = new HdfsConfiguration();

    // Set a really long value for dfs.blockreport.intervalMsec and
    // dfs.heartbeat.interval, so that incremental block reports and heartbeats
    // won't be sent during this test unless they're triggered
    // manually.
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10800000L);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1080L);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    Thread.sleep(10000);
    DatanodeProtocolClientSideTranslatorPB spy =
        DataNodeTestUtils.spyOnBposToNN(
            cluster.getDataNodes().get(0), cluster.getNameNode());
    DFSTestUtil.createFile(fs, new Path("/abc"), 16, (short) 1, 1L);

    // We should get 1 incremental block report.
    Mockito.verify(spy, timeout(60000).times(1)).blockReceivedAndDeleted(
        any(DatanodeRegistration.class),
        anyString(),
        any(StorageReceivedDeletedBlocks[].class));

    // We should not receive any more incremental or incremental block reports,
    // since the interval we configured is so long.
    for (int i = 0; i < 3; i++) {
      Thread.sleep(10);
      Mockito.verify(spy, times(0)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          Mockito.<BlockReportContext>anyObject());
      Mockito.verify(spy, times(1)).blockReceivedAndDeleted(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageReceivedDeletedBlocks[].class));
    }

    // Create a fake block deletion notification on the DataNode.
    // This will be sent with the next incremental block report.
    
    DataNode datanode = cluster.getDataNodes().get(0);
    BPOfferService service =
        datanode.getAllBpOs().get(0);
    String storageUuid =
        datanode.getFSDataset().getVolumes().get(0).getStorageID();
    ExtendedBlock rdbi = new ExtendedBlock(service.getBlockPoolId(), 5678, 512,
            1000, Block.NON_EXISTING_BUCKET_ID);
    service.notifyNamenodeDeletedBlock(rdbi, storageUuid);

    // Manually trigger a block report.
    datanode.triggerBlockReport(
        new BlockReportOptions.Factory().
            setIncremental(incremental).
            build()
    );

    // triggerBlockReport returns before the block report is
    // actually sent.  Wait for it to be sent here.
    if (incremental) {
      Mockito.verify(spy, timeout(60000).times(2)).
          blockReceivedAndDeleted(
              any(DatanodeRegistration.class),
              anyString(),
              any(StorageReceivedDeletedBlocks[].class));
    } else {
      Mockito.verify(spy, timeout(60000)).blockReport(
          any(DatanodeRegistration.class),
          anyString(),
          any(StorageBlockReport[].class),
          Mockito.<BlockReportContext>anyObject());
    }

    cluster.shutdown();
  }

  @Test
  public void testTriggerFullBlockReport() throws Exception {
    testTriggerBlockReport(false);
  }

  @Test
  public void testTriggerIncrementalBlockReport() throws Exception {
    testTriggerBlockReport(true);
  }
}
