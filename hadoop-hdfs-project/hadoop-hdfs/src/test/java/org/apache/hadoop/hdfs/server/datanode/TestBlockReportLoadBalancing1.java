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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import static org.junit.Assert.*;

/**
 *
 * @author salman
 */
public class TestBlockReportLoadBalancing1 {

  public static final Log LOG = LogFactory.getLog(TestBlockReportLoadBalancing1.class);
  private MiniDFSCluster cluster;

  @Before
  public void startUpCluster() throws IOException {
  }

  @After
  public void shutDownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
      Thread.sleep(1000);
    }
  }


  @Test
  public void TestBRTrackingService_01() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TW = 100000;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService( DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = assignWork(nnList, service,(long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 0.8));
      assertTrue("Unable to assign work", an != null);
    }

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = assignWork(nnList, service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 0.2));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    ActiveNode an = assignWork(nnList, service, 10000);
    assertTrue("More work should not have been assigned", an==null);

    an = assignWork(nnList, service, 1);
    assertTrue("More work should not have been assigned", an==null);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);


    for (int i = 0; i < NN_COUNT; i++) {
      an = assignWork(nnList, service, DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, 10000);
    assertTrue("More work should not have been assigned", an==null);

    an = assignWork(nnList, service, 1);
    assertTrue("More work should not have been assigned", an==null);
  }

  @Test
  //add and remove namenodes
  public void TestBRTrackingService_02() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TW = 100000;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService(DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = assignWork(nnList, service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 1));
      assertTrue("Unable to assign work", an != null);
    }


    // more work assignment should fail
    ActiveNode an = assignWork(nnList, service, 10000);
    assertTrue("More work should not have been assigned", an==null);

    an = assignWork(nnList, service, 1);
    assertTrue("More work should not have been assigned", an==null);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    // kill a NN
    list.remove(0);
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < NN_COUNT; i++) {
      an = assignWork(nnList, service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 1));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, 1);
    assertTrue("More work should not have been assigned", an==null);


    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    // add more namenodes 
    for (int i = NN_COUNT; i < 2 * NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < NN_COUNT; i++) {
      an = assignWork(nnList, service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 0.25));
      assertTrue("Unable to assign work", an != null);
    }

    for (int i = 0; i < NN_COUNT; i++) {
      an = assignWork(nnList, service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 0.75));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, 1);
    assertTrue("More work should not have been assigned", an==null);

  }

  @Test
  public void TestCommandLine() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TW = 100000;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());

    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0.0:10000");
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService( DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNode an = assignWork(nnList,service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 1));
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    ActiveNode an = assignWork(nnList,service, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = assignWork(nnList,service, 1);
    assertTrue("More work should not have been assigned", an == null);

    String[] argv = {"-setBlkRptProcessSize", NN_COUNT*DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 2 + ""};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals("setBlkRptProcessSize command should succeed", 0, e.status);
    }

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = assignWork(nnList,service, (long) (DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 2));
      assertTrue("Unable to assign work", an != null);
    }
  }

  @Test
  public void TestClusterWithOutDataNodes() throws IOException, InterruptedException {

    final int NN_COUNT = 5;
    final long DFS_BR_LB_TIME_WINDOW_SIZE = 5000;
    final long DFS_BR_LB_MAX_BLK_PER_NN_PER_TW = 100000;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW, NN_COUNT * DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE, DFS_BR_LB_TIME_WINDOW_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);



    cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .format(true).numDataNodes(0).build();
    cluster.waitActive();


    ActiveNode an = null;
    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
      assertTrue("Unable to assign work", an != null);
    }


    // more work assignment should fail
    try {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW);
      fail("More work should not have been assigned");
    }catch(BRLoadBalancingException e){
    }

    String[] argv = {"-setBlkRptProcessSize", NN_COUNT * DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 2 + ""};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals("setBlkRptProcessSize command should succeed", 0, e.status);
    }

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW*2);
      assertTrue("Unable to assign work", an != null);
    }

    // now kill some namenodes
    cluster.shutdownNameNode(NN_COUNT-1);
    cluster.shutdownNameNode(NN_COUNT-2);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_TIME_WINDOW_SIZE);

    for (int i = 0; i < NN_COUNT; i++) {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW*0.5));
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW));
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport((long)(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW*0.5));
      assertTrue("Unable to assign work", an != null);
    }


    try {
      an = cluster.getNameNode(0).getNextNamenodeToSendBlockReport(DFS_BR_LB_MAX_BLK_PER_NN_PER_TW * 2);
      fail("More work should not have been assigned");
    }catch(BRLoadBalancingException e){

    }

    cluster.shutdown();
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
