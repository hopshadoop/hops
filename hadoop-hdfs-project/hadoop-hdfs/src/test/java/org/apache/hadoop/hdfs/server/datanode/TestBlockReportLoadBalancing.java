/*
 * Copyright 2015 Apache Software Foundation.
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
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.metadata.HdfsStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BRTrackingService;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 *
 * @author salman
 */
public class TestBlockReportLoadBalancing {

  public static final Log LOG = LogFactory.getLog(TestBlockReportLoadBalancing.class);

  @Before
  public void startUpCluster() throws IOException {
  }

  @After
  public void shutDownCluster() throws IOException {
  }


  @Test
  public void TestBRTrackingService_01() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TU = 100000;
    final long DFS_BR_LB_UPDATE_THRESHOLD_TIME = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<ActiveNode>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService( DFS_BR_LB_UPDATE_THRESHOLD_TIME, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 0.8));
      assertTrue("Unable to assign work", an != null);
    }

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 0.2));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    ActiveNode an = service.assignWork(nnList, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = service.assignWork(nnList, 1);
    assertTrue("More work should not have been assigned", an == null);


    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);


    for (int i = 0; i < NN_COUNT; i++) {
      an = service.assignWork(nnList, DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail

    an = service.assignWork(nnList, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = service.assignWork(nnList, 1);
    assertTrue("More work should not have been assigned", an == null);
  }

  @Test
  //add and remove namenodes
  public void TestBRTrackingService_02() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TU = 100000;
    final long DFS_BR_LB_UPDATE_THRESHOLD_TIME = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<ActiveNode>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService(DFS_BR_LB_UPDATE_THRESHOLD_TIME, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 1));
      assertTrue("Unable to assign work", an != null);
    }


    // more work assignment should fail
    ActiveNode an = service.assignWork(nnList, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = service.assignWork(nnList, 1);
    assertTrue("More work should not have been assigned", an == null);


    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);


    // kill a NN 
    list.remove(0);
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < NN_COUNT; i++) {
      an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 1));
      assertTrue("Unable to assign work", an != null);
    }


    // more work assignment should fail
    an = service.assignWork(nnList, 10000);
    assertTrue("More work should not have been assigned", an == null);


    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    // add more namenodes 
    for (int i = NN_COUNT; i < 2 * NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < NN_COUNT; i++) {
      an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 0.25));
      assertTrue("Unable to assign work", an != null);
    }

    for (int i = 0; i < NN_COUNT; i++) {
      an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 0.75));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = service.assignWork(nnList, 1);
    assertTrue("More work should not have been assigned", an == null);
  }

  @Test
  public void TestCommandLine() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TU = 100000;
    final long DFS_BR_LB_UPDATE_THRESHOLD_TIME = 1000;

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<ActiveNode>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService( DFS_BR_LB_UPDATE_THRESHOLD_TIME, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 1));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    ActiveNode an = service.assignWork(nnList, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = service.assignWork(nnList, 1);
    assertTrue("More work should not have been assigned", an == null);

    String[] argv = {"-setBlkRptProcessSize", NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 2 + ""};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals("setBlkRptProcessSize command should succeed", 0, e.status);
    }

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = service.assignWork(nnList, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 2));
      assertTrue("Unable to assign work", an != null);
    }
  }

  @Test
  public void TestClusterWithOutDataNodes() throws IOException, InterruptedException {

    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TU = 100000;
    final long DFS_BR_LB_UPDATE_THRESHOLD_TIME = 1000;

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT * DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE, DFS_BR_LB_TIME_WINDOW_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_UPDATE_THRESHOLD_TIME,DFS_BR_LB_UPDATE_THRESHOLD_TIME);



    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .format(true).numDataNodes(0).build();
    cluster.waitActive();


    ActiveNode an = null;
    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
      assertTrue("Unable to assign work", an != null);
    }


    // more work assignment should fail
    an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    assertTrue("More work should not have been assigned", an == null);

    String[] argv = {"-setBlkRptProcessSize", NN_COUNT * DFS_BR_LB_MAX_BLK_PER_NN_PER_TU * 2 + ""};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals("setBlkRptProcessSize command should succeed", 0, e.status);
    }

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU*2);
      assertTrue("Unable to assign work", an != null);
    }

    // now kill some namenodes
    cluster.shutdownNameNode(NN_COUNT-1);
    cluster.shutdownNameNode(NN_COUNT-2);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU*0.5));
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU));
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU*0.5));
      assertTrue("Unable to assign work", an != null);
    }


    an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TU*2);
    assertTrue("More work should not have been assigned", an == null);
  }

  @Test
  public void TestClusterWithDataNodes()
          throws Exception {
    final int NN_COUNT = 2;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 10000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TU = 10;
    final long DFS_BR_LB_UPDATE_THRESHOLD_TIME = 1000;

    final int BLOCK_SIZE = 1024;
    final int NUM_BLOCKS = 10;
    final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE;

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, DFS_BR_LB_MAX_BLK_PER_NN_PER_TU);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE, DFS_BR_LB_TIME_WINDOW_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_UPDATE_THRESHOLD_TIME,DFS_BR_LB_UPDATE_THRESHOLD_TIME);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY,0);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3000);


    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .format(true).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem fs = cluster.getNewFileSystemInstance(0);


    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    DFSTestUtil.createFile(fs, filePath, FILE_SIZE, (short)1, 0);


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

    //here we have two datanodes storing 10 blocks
    //block report load balancing will ensure that at most 10 blocks are processed in 10 secs
    //run the system for 32 secs.
    //in the history list expect 3 almost ten second long gaps
    //
    Thread.sleep(32*1000);
    int counter = 0;
    long previousTime=history.get(0);
    LOG.debug("All No "+Arrays.toString(history.toArray()));
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

}
