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
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommission {
  public static final Log LOG = LogFactory.getLog(TestDecommission.class);
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
  static final int BLOCKREPORT_INTERVAL_MSEC = 1000; //block report in msec
  static final int NAMENODE_REPLICATION_INTERVAL = 1; //replication interval

  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;
  FileSystem localFileSys;
  Configuration conf;
  MiniDFSCluster cluster = null;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir,
        System.getProperty("test.build.data", "target/test/data") +
            "/work-dir/decommission");
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");
    
    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, NAMENODE_REPLICATION_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_FETCHER_BUCKETS_PER_THREAD, DFSConfigKeys.DFS_NUM_BUCKETS_DEFAULT);

    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);
  }
  
  @After
  public void teardown() throws IOException {
    cleanupFile(localFileSys, excludeFile.getParent());
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private void writeConfigFile(Path name, ArrayList<String> nodes)
      throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);
    
    if (nodes != null) {
      for (String node : nodes) {
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void writeFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    
    // Added this delay as the minimum replication is set to 1
    // some times it  happens as we manage to create a file
    // and the data node that is going to be decommissioned
    // does not yet have the replica ( its in flight )
    // if you decommission before it has the replica then
    // the replication manager will not know about it and this test
    // fails
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      Logger.getLogger(TestDecommission.class.getName())
          .log(Level.SEVERE, null, ex);
    }

    LOG.info("Created file " + name + " with " + repl + " replicas.");
  }

  /**
   * Verify that the number of replicas are as expected for each block in
   * the given file.
   * For blocks with a decommissioned node, verify that their replication
   * is 1 more than what is specified.
   * For blocks without decommissioned nodes, verify their replication is
   * equal to what is specified.
   *
   * @param downnode
   *     - if null, there is no decommissioned node for this file.
   * @return - null if no failure found, else an error message string.
   */
  private String checkFile(FileSystem fileSys, Path name, int repl,
      String downnode, int numDatanodes) throws IOException {
    boolean isNodeDown = (downnode != null);
    // need a raw stream
    assertTrue("Not HDFS:" + fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream) fileSys.open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) { // for each replica
        if (isNodeDown && nodes[j].getXferAddr().equals(downnode)) {
          hasdown++;
          //Downnode must actually be decommissioned
          if (!nodes[j].isDecommissioned()) {
            return "For block " + blk.getBlock() + " replica on " +
                nodes[j] + " is given as downnode, " +
                "but is not decommissioned";
          }
          //Decommissioned node (if any) should only be last node in list.
          if (j != nodes.length - 1) {
            return "For block " + blk.getBlock() + " decommissioned node " +
                nodes[j] + " was not last node in list: " + (j + 1) + " of " +
                nodes.length;
          }
          LOG.info("Block " + blk.getBlock() + " replica on " +
              nodes[j] + " is decommissioned.");
        } else {
          //Non-downnodes must not be decommissioned
          if (nodes[j].isDecommissioned()) {
            return "For block " + blk.getBlock() + " replica on " +
                nodes[j] + " is unexpectedly decommissioned";
          }
        }
      }

      LOG.info("Block " + blk.getBlock() + " has " + hasdown +
          " decommissioned replica.");
      if (Math.min(numDatanodes, repl + hasdown) != nodes.length) {
        return "Wrong number of replicas for block " + blk.getBlock() +
            ": " + nodes.length + ", expected " +
            Math.min(numDatanodes, repl + hasdown);
      }
    }
    return null;
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * decommission the DN at index dnIndex or one random node if dnIndex is set
   * to -1 and wait for the node to reach the given {@code waitForState}.
   */
  private DatanodeInfo decommissionNode(int nnIndex,
      String datanodeUuid, ArrayList<DatanodeInfo> decommissionedNodes, AdminStates waitForState) throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly unless the caller specifies one.
    //
    int index = 0;
        if (datanodeUuid == null) {
      boolean found = false;
      while (!found) {
        index = myrand.nextInt(info.length);
        if (!info[index].isDecommissioned()) {
          found = true;
        }
      }
    } else {
      // The caller specifies a DN
      for (; index < info.length; index++) {
        if (info[index].getDatanodeUuid().equals(datanodeUuid)) {
          break;
        }
      }
      if (index == info.length) {
        throw new IOException("invalid datanodeUuid " + datanodeUuid);
      }
    }
    String nodename = info[index].getXferAddr();
    LOG.info("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<>();
    if (decommissionedNodes != null) {
      for (DatanodeInfo dn : decommissionedNodes) {
        nodes.add(dn.getName());
      }
    }
    nodes.add(nodename);
    writeConfigFile(excludeFile, nodes);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    DatanodeInfo ret = NameNodeAdapter
        .getDatanode(cluster.getNamesystem(nnIndex), info[index]);
    waitNodeState(ret, waitForState);
    return ret;
  }

  /* Ask a specific NN to stop decommission of the datanode and wait for each
   * to reach the NORMAL state.
   */
  private void recomissionNode(int nnIndex, DatanodeInfo decommissionedNode) throws IOException {
    LOG.info("Recommissioning node: " + decommissionedNode);
    writeConfigFile(excludeFile, null);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    waitNodeState(decommissionedNode, AdminStates.NORMAL);

  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(DatanodeInfo node, AdminStates state) {
    boolean done = state == node.getAdminState();
    while (!done) {
      LOG.info("Waiting for node " + node + " to change state to " + state +
          " current state: " + node.getAdminState());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
      } catch (InterruptedException e) {
        // nothing
      }
      done = state == node.getAdminState();
    }
    LOG.info("node " + node + " reached the state " + state);
  }
  
  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn, Configuration conf)
      throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
  }
  
  /* Validate cluster has expected number of datanodes */
  private static void validateCluster(DFSClient client, int numDNs)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDNs, info.length);
  }
  
  /**
   * Start a MiniDFSCluster
   *
   * @throws IOException
   */
  private void startCluster(int numNameNodes, int numDatanodes,
      Configuration conf) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(numNameNodes))
        .numDataNodes(numDatanodes).build();
    cluster.waitActive();
    for (int i = 0; i < numNameNodes; i++) {
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      validateCluster(client, numDatanodes);
    }
    DFSTestUtil.createRootFolder();
  }

  static void refreshNodes(final FSNamesystem ns, final Configuration conf)
      throws IOException {
    ns.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }
  
  private void verifyStats(NameNode namenode, FSNamesystem fsn,
      DatanodeInfo node, boolean decommissioning)
      throws InterruptedException, IOException {
    // Do the stats check over 10 iterations
    for (int i = 0; i < 10; i++) {
      long[] newStats = namenode.getRpcServer().getStats();

      // For decommissioning nodes, ensure capacity of the DN is no longer
      // counted. Only used space of the DN is counted in cluster capacity
      assertEquals(newStats[0],
          decommissioning ? node.getDfsUsed() : node.getCapacity());

      // Ensure cluster used capacity is counted for both normal and
      // decommissioning nodes
      assertEquals(newStats[1], node.getDfsUsed());

      // For decommissioning nodes, remaining space from the DN is not counted
      assertEquals(newStats[2], decommissioning ? 0 : node.getRemaining());

      // Ensure transceiver count is same as that DN
      assertEquals(fsn.getTotalLoad(), node.getXceiverCount());
      
      Thread.sleep(HEARTBEAT_INTERVAL * 1000); // Sleep heart beat interval
    }
  }

  /**
   * Tests decommission for non federated cluster
   */
  @Test(timeout=360000)
  public void testDecommission() throws IOException {
    testDecommission(1, 6);
  }
  
  /**
   * Tests decommission with replicas on the target datanode cannot be migrated
   * to other datanodes and satisfy the replication factor. Make sure the
   * datanode won't get stuck in decommissioning state.
   */
  @Test(timeout = 360000)
  public void testDecommission2() throws IOException {
    LOG.info("Starting test testDecommission");
    int numNamenodes = 1;
    int numDatanodes = 4;
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    startCluster(numNamenodes, numDatanodes, conf);

    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList = new ArrayList<ArrayList<DatanodeInfo>>(
        numNamenodes);
    namenodeDecomList.add(0, new ArrayList<DatanodeInfo>(numDatanodes));

    Path file1 = new Path("testDecommission2.dat");
    int replicas = 4;

    // Start decommissioning one namenode at a time
    ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(0);
    FileSystem fileSys = cluster.getFileSystem(0);
    FSNamesystem ns = cluster.getNamesystem(0);

    writeFile(fileSys, file1, replicas);

    int deadDecomissioned = ns.getNumDecomDeadDataNodes();
    int liveDecomissioned = ns.getNumDecomLiveDataNodes();

    // Decommission one node. Verify that node is decommissioned.
    DatanodeInfo decomNode = decommissionNode(0, null, decommissionedNodes,
        AdminStates.DECOMMISSIONED);
    decommissionedNodes.add(decomNode);
    assertEquals(deadDecomissioned, ns.getNumDecomDeadDataNodes());
    assertEquals(liveDecomissioned + 1, ns.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertNull(checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
        numDatanodes));
    cleanupFile(fileSys, file1);

    // Restart the cluster and ensure recommissioned datanodes
    // are allowed to register with the namenode
    cluster.shutdown();
    startCluster(1, 4, conf);
    cluster.shutdown();
  }
  
  /**
   * Tests recommission for non federated cluster
   */
  @Test(timeout=360000)
  public void testRecommission() throws IOException {
    testRecommission(1, 6);
  }

  /**
   * Test decommission for federeated cluster
   */
  @Ignore
  public void testDecommissionFederation() throws IOException {
    testDecommission(2, 2);
  }
  
  private void testDecommission(int numNamenodes, int numDatanodes)
      throws IOException {
    LOG.info("Starting test testDecommission");
    startCluster(numNamenodes, numDatanodes, conf);

    DFSTestUtil.createRootFolder();
    
    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
        new ArrayList<>(numNamenodes);
    for (int i = 0; i < numNamenodes; i++) {
      namenodeDecomList.add(i, new ArrayList<DatanodeInfo>(numDatanodes));
    }
    Path file1 = new Path("testDecommission.dat");
    for (int iteration = 0; iteration < numDatanodes - 1; iteration++) {
      int replicas = numDatanodes - iteration - 1;
      
      // Start decommissioning one namenode at a time
      for (int i = 0; i < numNamenodes; i++) {
        ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(i);
        FileSystem fileSys = cluster.getFileSystem(i);
        FSNamesystem ns = cluster.getNamesystem(i);
        
        writeFile(fileSys, file1, replicas);
        int deadDecomissioned = ns.getNumDecomDeadDataNodes();
        int liveDecomissioned = ns.getNumDecomLiveDataNodes();

        // Decommission one node. Verify that node is decommissioned.
        DatanodeInfo decomNode = decommissionNode(i, null, decommissionedNodes,
            AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(decomNode);
        assertEquals(deadDecomissioned, ns.getNumDecomDeadDataNodes());
        assertEquals(liveDecomissioned + 1, ns.getNumDecomLiveDataNodes());
        
        // Ensure decommissioned datanode is not automatically shutdown
        DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
        assertEquals("All datanodes must be alive", numDatanodes,
            client.datanodeReport(DatanodeReportType.LIVE).length);
        // wait for the block to be replicated
        int tries = 0;
        while (tries++ < 20) {
          try {
            Thread.sleep(1000);
            if (checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
                numDatanodes) == null) {
              break;
            }
          } catch (InterruptedException ie) {
          }
        }
        assertTrue("Checked if block was replicated after decommission, tried "
            + tries + " times.", tries < 20);
        cleanupFile(fileSys, file1);
      }
    }

    // Restart the cluster and ensure decommissioned datanodes
    // are allowed to register with the namenode
    cluster.shutdown();

    startCluster(numNamenodes, numDatanodes, conf);
    cluster.shutdown();
  }


  private void testRecommission(int numNamenodes, int numDatanodes)
      throws IOException {
    LOG.info("Starting test testRecommission");

    startCluster(numNamenodes, numDatanodes, conf);

    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
        new ArrayList<>(numNamenodes);
    for (int i = 0; i < numNamenodes; i++) {
      namenodeDecomList.add(i, new ArrayList<DatanodeInfo>(numDatanodes));
    }
    Path file1 = new Path("testDecommission.dat");
    int replicas = numDatanodes - 1;

    for (int i = 0; i < numNamenodes; i++) {
      ArrayList<DatanodeInfo> decommissionedNodes = namenodeDecomList.get(i);
      FileSystem fileSys = cluster.getFileSystem(i);
      writeFile(fileSys, file1, replicas);

      // Decommission one node. Verify that node is decommissioned.
      DatanodeInfo decomNode =
          decommissionNode(i, null, decommissionedNodes, AdminStates.DECOMMISSIONED);
      decommissionedNodes.add(decomNode);

      // Ensure decommissioned datanode is not automatically shutdown
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      assertEquals("All datanodes must be alive", numDatanodes,
          client.datanodeReport(DatanodeReportType.LIVE).length);
      int tries =0;
      // wait for the block to be replicated
      while (tries++ < 20) {
        try {
          Thread.sleep(1000);
          if (checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
              numDatanodes) == null) {
            break;
          }
        } catch (InterruptedException ie) {
        }
      }
      assertTrue("Checked if block was replicated after decommission, tried "
          + tries + " times.", tries < 20);

      // stop decommission and check if the new replicas are removed
      recomissionNode(0, decomNode);
      // wait for the block to be deleted
      tries = 0;
      while (tries++ < 20) {
        try {
          Thread.sleep(1000);
          if (checkFile(fileSys, file1, replicas, null, numDatanodes) == null) {
            break;
          }
        } catch (InterruptedException ie) {
        }
      }
      cleanupFile(fileSys, file1);
      assertTrue("Checked if node was recommissioned " + tries + " times.",
          tries < 20);
      LOG.info("tried: " + tries + " times before recommissioned");
    }
    cluster.shutdown();
  }
  
  /**
   * Tests cluster storage statistics during decommissioning for non
   * federated cluster
   */
  @Test(timeout=360000)
  public void testClusterStats() throws Exception {
    testClusterStats(1);
  }
  
  /**
   * Tests cluster storage statistics during decommissioning for
   * federated cluster
   */
  @Ignore // HOP federation not supported
  public void testClusterStatsFederation() throws Exception {
    testClusterStats(3);
  }
  
  public void testClusterStats(int numNameNodes)
      throws IOException, InterruptedException {
    LOG.info("Starting test testClusterStats");
    int numDatanodes = 1;
    startCluster(numNameNodes, numDatanodes, conf);
    
    for (int i = 0; i < numNameNodes; i++) {
      FileSystem fileSys = cluster.getFileSystem(i);
      Path file = new Path("testClusterStats.dat");
      writeFile(fileSys, file, 1);
      
      FSNamesystem fsn = cluster.getNamesystem(i);
      NameNode namenode = cluster.getNameNode(i);
      DatanodeInfo downnode =
          decommissionNode(i, null, null, AdminStates.DECOMMISSION_INPROGRESS);
      // Check namenode stats for multiple datanode heartbeats
      verifyStats(namenode, fsn, downnode, true);
      
      // Stop decommissioning and verify stats
      writeConfigFile(excludeFile, null);
      refreshNodes(fsn, conf);
      DatanodeInfo ret = NameNodeAdapter.getDatanode(fsn, downnode);
      waitNodeState(ret, AdminStates.NORMAL);
      verifyStats(namenode, fsn, ret, false);
    }
  }
  
  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a non
   * federated cluster.
   */
  @Test(timeout=360000)
  public void testHostsFile() throws IOException, InterruptedException {
    // Test for a single namenode cluster
    testHostsFile(1);
  }
  
  /**
   * Test host/include file functionality. Only datanodes
   * in the include file are allowed to connect to the namenode in a
   * federated cluster.
   */
  @Ignore //hop federation not supported
  public void testHostsFileFederation()
      throws IOException, InterruptedException {
    // Test for 3 namenode federated cluster
    testHostsFile(3);
  }
  
  public void testHostsFile(int numNameNodes)
      throws IOException, InterruptedException {
    int numDatanodes = 1;
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(numNameNodes))
        .numDataNodes(numDatanodes).setupHostsFile(true).build();
    cluster.waitActive();
    
    // Now empty hosts file and ensure the datanode is disallowed
    // from talking to namenode, resulting in it's shutdown.
    ArrayList<String> list = new ArrayList<>();
    final String bogusIp = "127.0.30.1";
    list.add(bogusIp);
    writeConfigFile(hostsFile, list);
    
    for (int j = 0; j < numNameNodes; j++) {
      refreshNodes(cluster.getNamesystem(j), conf);
      
      DFSClient client = getDfsClient(cluster.getNameNode(j), conf);
      DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
      for (int i = 0; i < 5 && info.length != 0; i++) {
        LOG.info("Waiting for datanode to be marked dead");
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
        info = client.datanodeReport(DatanodeReportType.LIVE);
      }
      assertEquals("Number of live nodes should be 0", 0, info.length);
      
      // Test that non-live and bogus hostnames are considered "dead".
      // The dead report should have an entry for (1) the DN  that is
      // now considered dead because it is no longer allowed to connect
      // and (2) the bogus entry in the hosts file (these entries are
      // always added last)
      info = client.datanodeReport(DatanodeReportType.DEAD);
      assertEquals("There should be 2 dead nodes", 2, info.length);
      DatanodeID id = cluster.getDataNodes().get(0).getDatanodeId();
      assertEquals(id.getHostName(), info[0].getHostName());
      assertEquals(bogusIp, info[1].getHostName());
    }
  }

  @Test(timeout=120000)
  public void testDecommissionWithOpenfile() throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithOpenfile");
    
    //At most 4 nodes will be decommissioned
    startCluster(1, 7, conf);
        
    FileSystem fileSys = cluster.getFileSystem(0);
    FSNamesystem ns = cluster.getNamesystem(0);
    
    String openFile = "/testDecommissionWithOpenfile.dat";
           
    writeFile(fileSys, new Path(openFile), (short)3);   
    // make sure the file was open for write
    FSDataOutputStream fdos =  fileSys.append(new Path(openFile)); 
    
    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(cluster.getNameNode(0), openFile, 0, fileSize);
              
    DatanodeInfo[] dnInfos4LastBlock = lbs.getLastLocatedBlock().getLocations();
    DatanodeInfo[] dnInfos4FirstBlock = lbs.get(0).getLocations();
    
    ArrayList<String> nodes = new ArrayList<String>();
    ArrayList<DatanodeInfo> dnInfos = new ArrayList<DatanodeInfo>();
   
    DatanodeManager dm = ns.getBlockManager().getDatanodeManager();
    for (DatanodeInfo datanodeInfo : dnInfos4FirstBlock) {
      DatanodeInfo found = datanodeInfo;
      for (DatanodeInfo dif: dnInfos4LastBlock) {
        if (datanodeInfo.equals(dif)) {
         found = null;         
        }
      }
      if (found != null) {
        nodes.add(found.getXferAddr());
        dnInfos.add(dm.getDatanode(found));
      }
    }
    //decommission one of the 3 nodes which have last block
    nodes.add(dnInfos4LastBlock[0].getXferAddr());
    dnInfos.add(dm.getDatanode(dnInfos4LastBlock[0]));
    
    writeConfigFile(excludeFile, nodes);
    refreshNodes(ns, conf);  
    for (DatanodeInfo dn : dnInfos) {
      waitNodeState(dn, AdminStates.DECOMMISSIONED);
    }           

    fdos.close();
  }
  
  /**
   * Tests restart of namenode while datanode hosts are added to exclude file
   **/
  @Test(timeout=360000)
  public void testDecommissionWithNamenodeRestart()throws IOException, InterruptedException {
    LOG.info("Starting test testDecommissionWithNamenodeRestart");
    int numNamenodes = 1;
    int numDatanodes = 1;
    int replicas = 1;
    
    startCluster(numNamenodes, numDatanodes, conf);
    Path file1 = new Path("testDecommission.dat");
    FileSystem fileSys = cluster.getFileSystem();
    writeFile(fileSys, file1, replicas);
        
    DFSClient client = getDfsClient(cluster.getNameNode(), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    DatanodeID excludedDatanodeID = info[0];
    String excludedDatanodeName = info[0].getXferAddr();
    writeConfigFile(excludeFile, new ArrayList<String>(Arrays.asList(excludedDatanodeName)));
    
    //Add a new datanode to cluster
    cluster.startDataNodes(conf, 1, true, null, null, null, null);
    numDatanodes+=1;
    
    assertEquals("Number of datanodes should be 2 ", 2, cluster.getDataNodes().size());
    //Restart the namenode
    cluster.restartNameNode();
    DatanodeInfo datanodeInfo = NameNodeAdapter.getDatanode(
        cluster.getNamesystem(), excludedDatanodeID);
    waitNodeState(datanodeInfo, AdminStates.DECOMMISSIONED);
    
    // Ensure decommissioned datanode is not automatically shutdown
    assertEquals("All datanodes must be alive", numDatanodes, 
        client.datanodeReport(DatanodeReportType.LIVE).length);
    // wait for the block to be replicated
    int tries = 0;
    while (tries++ < 20) {
      try {
        Thread.sleep(1000);
        if (checkFile(fileSys, file1, replicas, datanodeInfo.getXferAddr(),
            numDatanodes) == null) {
          break;
        }
      } catch (InterruptedException ie) {
      }
    }
    assertTrue("Checked if block was replicated after decommission, tried "
        + tries + " times.", tries < 20);
    cleanupFile(fileSys, file1);
    // Restart the cluster and ensure recommissioned datanodes
    // are allowed to register with the namenode
    cluster.shutdown();
    startCluster(numNamenodes, numDatanodes, conf);
    cluster.shutdown();
  }
  
  /**
   * Test using a "registration name" in a host include file.
   *
   * Registration names are DataNode names specified in the configuration by
   * dfs.datanode.hostname.  The DataNode will send this name to the NameNode
   * as part of its registration.  Registration names are helpful when you
   * want to override the normal first result of DNS resolution on the
   * NameNode.  For example, a given datanode IP may map to two hostnames,
   * and you may want to choose which hostname is used internally in the
   * cluster.
   *
   * It is not recommended to use a registration name which is not also a
   * valid DNS hostname for the DataNode.  See HDFS-5237 for background.
   */
  @Test(timeout=360000)
  public void testIncludeByRegistrationName() throws IOException,
      InterruptedException {
    Configuration hdfsConf = new Configuration(conf);
    // Any IPv4 address starting with 127 functions as a "loopback" address
    // which is connected to the current host.  So by choosing 127.0.0.100
    // as our registration name, we have chosen a name which is also a valid
    // way of reaching the local DataNode we're going to start.
    // Typically, a registration name would be a hostname, but we don't want
    // to deal with DNS in this test.
    final String registrationName = "127.0.0.100";
    final String nonExistentDn = "127.0.0.10";
    hdfsConf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, registrationName);
    cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(1).checkDataNodeHostConfig(true)
        .setupHostsFile(true).build();
    cluster.waitActive();
    // Set up an includes file that doesn't have our datanode.
    ArrayList<String> nodes = new ArrayList<String>();
    nodes.add(nonExistentDn);
    writeConfigFile(hostsFile,  nodes);
    refreshNodes(cluster.getNamesystem(0), hdfsConf);
    // Wait for the DN to be marked dead.
    DFSClient client = getDfsClient(cluster.getNameNode(0), hdfsConf);
    while (true) {
      DatanodeInfo info[] = client.datanodeReport(DatanodeReportType.DEAD);
      if (info.length == 1) {
        break;
      }
      LOG.info("Waiting for datanode to be marked dead");
      Thread.sleep(HEARTBEAT_INTERVAL * 1000);
    }
    // Use a non-empty include file with our registration name.
    // It should work.
    int dnPort = cluster.getDataNodes().get(0).getXferPort();
    nodes = new ArrayList<String>();
    nodes.add(registrationName + ":" + dnPort);
    writeConfigFile(hostsFile,  nodes);
    refreshNodes(cluster.getNamesystem(0), hdfsConf);
    cluster.restartDataNode(0);
    // Wait for the DN to come back.
    while (true) {
      DatanodeInfo info[] = client.datanodeReport(DatanodeReportType.LIVE);
      if (info.length == 1) {
        Assert.assertFalse(info[0].isDecommissioned());
        Assert.assertFalse(info[0].isDecommissionInProgress());
        assertEquals(registrationName, info[0].getHostName());
        break;
      }
      LOG.info("Waiting for datanode to come back");
      Thread.sleep(HEARTBEAT_INTERVAL * 1000);
    }
  }
}
