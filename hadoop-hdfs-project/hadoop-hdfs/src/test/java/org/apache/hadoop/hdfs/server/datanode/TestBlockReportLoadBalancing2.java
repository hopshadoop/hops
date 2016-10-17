/*
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
package org.apache.hadoop.hdfs.server.datanode;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.metadata.HdfsStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.BRTrackingService;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 * @author salman
 */
public class TestBlockReportLoadBalancing2 {

  public static final Log LOG = LogFactory.getLog(TestBlockReportLoadBalancing2.class);

  @Before
  public void startUpCluster() throws IOException {
  }

  @After
  public void shutDownCluster() throws IOException {
  }

  @Test
  public void TestClusterWithDataNodes()
          throws Exception {
    final int NN_COUNT = 2;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 10000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TW = 10;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1;

    final int BLOCK_SIZE = 1024;
    final int NUM_BLOCKS = 10;
    final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE;

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    LOG.debug(METHOD_NAME+" Starting test");

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE, DFS_BR_LB_TIME_WINDOW_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY,0);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3000);
    HdfsStorageFactory.setConfiguration(conf);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true)
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .numDataNodes(1).build();
    cluster.waitActive();
    LOG.debug(METHOD_NAME+" Cluster is up");
    FileSystem fs = cluster.getNewFileSystemInstance(0);

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath, FILE_SIZE, (short)1, 0);
    LOG.debug(METHOD_NAME+" Files are created");

    final List<Long>  history = new ArrayList<Long>();
    GenericTestUtils.DelayAnswer responder = new GenericTestUtils.DelayAnswer(LOG) {
      @Override
      protected Object passThrough(InvocationOnMock invocation)
              throws Throwable {
        try {
          LOG.debug("Block report received");
          synchronized (this){
            history.add(System.currentTimeMillis());
          }
          return super.passThrough(invocation);
        } finally {
        }
      }
    };

    NameNode nn0 = cluster.getNameNode(0);
    NameNode nn1 = cluster.getNameNode(1);
    // Set up a spy
    DataNode dn0 = cluster.getDataNodes().get(0);

    DatanodeProtocol spy0 = DataNodeTestUtils.spyOnBposToNN(dn0, nn0);
    DatanodeProtocol spy1 = DataNodeTestUtils.spyOnBposToNN(dn0, nn1);

    Mockito.doAnswer(responder).when(spy0)
              .blockReport(Mockito.<DatanodeRegistration>anyObject(),
                      Mockito.anyString(), Mockito.<StorageBlockReport[]>anyObject());
    Mockito.doAnswer(responder).when(spy1)
            .blockReport(Mockito.<DatanodeRegistration>anyObject(),
                    Mockito.anyString(), Mockito.<StorageBlockReport[]>anyObject());


    responder.waitForCall();
    responder.proceed();

    //here we have one datanode storing 10 blocks
    //block report load balancing will ensure that at most 10 blocks are processed in 10 secs
    //run the system for 32 secs.
    //in the history list expect 3 almost ten second long gaps
    //
    Thread.sleep(32*1000);
    int counter = 0;
    LOG.debug("All No "+Arrays.toString(history.toArray()));
    long previousTime=history.remove(0);
    for(Long time : history){
      long diff =time -previousTime;
      if(diff >= DFS_BR_LB_TIME_WINDOW_SIZE){
        counter++;
      }
      LOG.debug("Diff "+diff);
      previousTime=time;
    }
    assertTrue("Expecting 3. Got "+counter, counter==3);
  }

  private static ActiveNode assignWork(SortedActiveNodeList nnList, BRTrackingService service, long blks){
    try
    {
     return service.assignWork(nnList,blks);
    }catch (Exception e){
      return null;
    }
  }
}
