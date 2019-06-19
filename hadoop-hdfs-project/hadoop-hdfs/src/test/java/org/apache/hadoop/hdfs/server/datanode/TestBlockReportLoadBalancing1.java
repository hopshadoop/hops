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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingNonLeaderException;
import org.apache.hadoop.hdfs.server.blockmanagement.BRLoadBalancingOverloadException;
import org.apache.hadoop.hdfs.server.blockmanagement.BRTrackingService;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBlockReportLoadBalancing1 {

  public static final Log LOG = LogFactory.getLog(TestBlockReportLoadBalancing1.class);

  @Test
  public void TestBRTrackingService_01() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = NN_COUNT;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i,
              "0.0.0."+i+":10000" /*httpAddress*/, "0.0.0.0", 0);
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService(DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,
            DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN, DFS_BR_LB_MAX_BR_PROCESSING_TIME);

    String dnAddress = "";
    ActiveNode an = null;
    for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * NN_COUNT; i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_MAX_BR_PROCESSING_TIME);


    for (int i = 0; i < NN_COUNT * DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN; i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);
  }

  @Test
  //add and remove namenodes
  public void TestBRTrackingService_02() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = NN_COUNT;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0."+i+":10000"
              , "", 0);
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService(DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,
            DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN, DFS_BR_LB_MAX_BR_PROCESSING_TIME);

    ActiveNode an = null;
    String dnAddress = "";
    for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * NN_COUNT; i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(DFS_BR_LB_MAX_BR_PROCESSING_TIME);

    // kill a NN
    list.remove(0);
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * (nnList.size()); i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);


    Thread.sleep(DFS_BR_LB_MAX_BR_PROCESSING_TIME);

    // add more namenodes
    for (int i = NN_COUNT; i < 2 * NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0."+i+":10000", "", 0);
      list.add(anode);
    }
    nnList = new SortedActiveNodeListPBImpl(list);

    for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * nnList.size(); i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);

  }

  @Test
  public void TestCommandLine() throws IOException, InterruptedException {
    final int NN_COUNT = 5;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = NN_COUNT;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();


    List<ActiveNode> list = new ArrayList<>();
    for (int i = 0; i < NN_COUNT; i++) {
      ActiveNodePBImpl anode = new ActiveNodePBImpl(i, "host", "localhost", i, "0.0.0."+i+":10000"
              , "", 0);
      list.add(anode);
    }

    SortedActiveNodeListPBImpl nnList = new SortedActiveNodeListPBImpl(list);
    BRTrackingService service = new BRTrackingService(DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,
            DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN, DFS_BR_LB_MAX_BR_PROCESSING_TIME);


    ActiveNode an = null;
    String dnAddress = "";
    for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * nnList.size(); i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    // more work assignment should fail
    an = assignWork(nnList, service, dnAddress, 10000);
    assertTrue("More work should not have been assigned", an == null);

    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);

    LOG.info("Chainging the number of concurrent block reports");

    String[] argv = {"-concurrentBlkReports", DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * 2 + ""};
    try {
      NameNode.createNameNode(argv, conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals("concurrentBlkReports command should succeed", 0, e.status);
    }

    // sleep. All history will be cleared after that and more work can be  assigned
    Thread.sleep(Math.max(DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD));

    for (int i = 0; i < (DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * 2)*nnList.size(); i++) {
      dnAddress = "0.0.0.0:" + i;
      an = assignWork(nnList, service, dnAddress, 10000);
      assertTrue("Unable to assign work", an != null);
    }

    an = assignWork(nnList, service, dnAddress, 1);
    assertTrue("More work should not have been assigned", an == null);
  }

  @Test
  public void TestClusterDataNodes() throws IOException, InterruptedException {

    int NN_COUNT = 5;
    int DN_COUNT = 10;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = 1;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);

    MiniDFSCluster cluster = null;
    try {

      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
              .format(true).numDataNodes(DN_COUNT).build();
      cluster.waitActive();


      Thread.sleep(10000);

      ActiveNode an = null;
      for (int i = 0; i < DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN * NN_COUNT; i++) {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration);
        LOG.info("Assigned work for datanode " + cluster.getDataNodes().get(i).getAllBpOs().get(0)
                .bpRegistration.getXferAddr());
        assertTrue("Unable to assign work", an != null);
      }

      // more work assignment should fail
      try {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(0).getAllBpOs().get(0).bpRegistration);
        fail("More work should not have been assigned");
      } catch (BRLoadBalancingOverloadException e) {
      } catch (BRLoadBalancingNonLeaderException e) {
      }

      String[] argv = {"-concurrentBlkReports", 2 + ""};
      try {
        NameNode.createNameNode(argv, conf);
      } catch (ExitUtil.ExitException e) {
        assertEquals("concurrentBlkReports command should succeed", 0, e.status);
      }

      // sleep. All history will be cleared after that and more work can be  assigned
      Thread.sleep(Math.max(DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD));

      for (int i = 0; i < 2 *NN_COUNT; i++) {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(10000,
                cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration);
        assertTrue("Unable to assign work", an != null);
      }

      // now kill some namenodes
      cluster.shutdownNameNode(NN_COUNT - 1);
      cluster.shutdownNameNode(NN_COUNT - 2);

      // sleep. All history will be cleared after that and more work can be  assigned
      Thread.sleep(DFS_BR_LB_MAX_BR_PROCESSING_TIME);

      for (int i = 0; i < 2  *(NN_COUNT-2); i++) {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration);
        assertTrue("Unable to assign work", an != null);
      }

      try {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(0).getAllBpOs().get(0).bpRegistration);
        fail("More work should not have been assigned");

      } catch (BRLoadBalancingOverloadException e) {
      } catch (BRLoadBalancingNonLeaderException e) {
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private NameNode getLeader(MiniDFSCluster cluster, int NN_COUNT) {
    NameNode leader = null;
    for (int i = 0; i < NN_COUNT; i++) {
      leader = cluster.getNameNode(i);
      if (leader.isLeader()) {
        break;
      }
    }
    return leader;
  }

  @Test
  public void TestUnregisteredDataNodesReport() throws IOException, InterruptedException {

    int NN_COUNT = 2;
    int DN_COUNT = 1;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = NN_COUNT;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);

    MiniDFSCluster cluster = null;
    try {

      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
              .numDataNodes(DN_COUNT).format(true).build();
      cluster.waitActive();

      DatanodeRegistration dnr = new DatanodeRegistration(new DatanodeID("test:5050"),
              new StorageInfo(0, 0, "", 0, HdfsServerConstants.NodeType.DATA_NODE, "")
              , ExportedBlockKeys.DUMMY_KEYS, "");

      try {
        cluster.getNameNode(0).getNextNamenodeToSendBlockReport(1000, dnr);
        fail("should throw a ProcessReport from dead or unregistred node exception");
      } catch (IOException ex) {
        assertTrue("wrong error message " + ex.getMessage(),
                ex.getMessage().contains("ProcessReport from dead or unregistered node"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static ActiveNode assignWork(SortedActiveNodeList nnList,
                                       BRTrackingService service,
                                       String dnAddress, long blks) {
    try {
      return service.assignWork(nnList, dnAddress, blks);
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  public void TestClusterMultiNNBR() throws IOException, InterruptedException {

    int NN_COUNT = 5;
    int DN_COUNT = 10;
    final long DFS_BR_LB_MAX_BR_PROCESSING_TIME = 5 * 1000;
    final long DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN = 2;
    final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD = 1000;


    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_CONCURRENT_BR_PER_NN, DFS_BR_LB_MAX_CONCURRENT_BLK_REPORTS_PER_NN);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, DFS_BR_LB_MAX_BR_PROCESSING_TIME);
    conf.setLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD, DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD);

    MiniDFSCluster cluster = null;
    try {

      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
              .format(true).numDataNodes(DN_COUNT).build();
      cluster.waitActive();


      Thread.sleep(10000);

      ActiveNode an = null;
      for (int i = 0; i < DN_COUNT; i++) {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration);
        LOG.info("Assigned work for datanode " + cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration.getXferAddr());
        assertTrue( an != null);
      }

      // more work assignment should fail
      try {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(0).getAllBpOs().get(0).bpRegistration);
        fail("More work should not have been assigned");
      } catch (BRLoadBalancingOverloadException e) {
      }

      for (int i = 0; i < DN_COUNT; i++) {
        //broadcast that the br is completed
        String dnAddress = cluster.getDataNodes().get(i).getAllBpOs().get(0)
                .bpRegistration.getXferAddr();

        for(int n = 0; n < NN_COUNT; n++){
          NameNode nn = cluster.getNameNode(n);
          nn.getBRTrackingService().blockReportCompleted(dnAddress);
        }
      }

      for (int i = 0; i < DN_COUNT; i++) {
        an = getLeader(cluster, NN_COUNT).getNextNamenodeToSendBlockReport(1000,
                cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration);
        LOG.info("Assigned work for datanode " + cluster.getDataNodes().get(i).getAllBpOs().get(0).bpRegistration.getXferAddr());
        assertTrue( an != null);
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
