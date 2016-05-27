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
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
        4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
        NAMENODE_REPLICATION_INTERVAL);

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
      for (Iterator<String> it = nodes.iterator(); it.hasNext(); ) {
        String node = it.next();
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
    HdfsDataInputStream dis =
        (HdfsDataInputStream) ((DistributedFileSystem) fileSys).open(name);
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
   * decommission one random node and wait for each to reach the
   * given {@code waitForState}.
   */
  private DatanodeInfo decommissionNode(int nnIndex,
      ArrayList<DatanodeInfo> decommissionedNodes, AdminStates waitForState)
      throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly.
    //
    int index = 0;
    boolean found = false;
    while (!found) {
      index = myrand.nextInt(info.length);
      if (!info[index].isDecommissioned()) {
        found = true;
      }
    }
    String nodename = info[index].getXferAddr();
    LOG.info("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>();
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

  /* stop decommission of the datanode and wait for each to reach the NORMAL state */
  private void recomissionNode(DatanodeInfo decommissionedNode)
      throws IOException {
    LOG.info("Recommissioning node: " + decommissionedNode);
    writeConfigFile(excludeFile, null);
    refreshNodes(cluster.getNamesystem(), conf);
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
  @Test
  public void testDecommission() throws IOException {
    testDecommission(1, 6);
  }
  
  /**
   * Tests recommission for non federated cluster
   */
  @Test
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
    
    ArrayList<ArrayList<DatanodeInfo>> namenodeDecomList =
        new ArrayList<ArrayList<DatanodeInfo>>(numNamenodes);
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
        writeFile(fileSys, file1, replicas);
        
        // Decommission one node. Verify that node is decommissioned.
        DatanodeInfo decomNode = decommissionNode(i, decommissionedNodes,
            AdminStates.DECOMMISSIONED);
        decommissionedNodes.add(decomNode);
        
        // Ensure decommissioned datanode is not automatically shutdown
        DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
        assertEquals("All datanodes must be alive", numDatanodes,
            client.datanodeReport(DatanodeReportType.LIVE).length);
        assertNull(checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
            numDatanodes));
        cleanupFile(fileSys, file1);
      }
    }

    // Restart the cluster and ensure recommissioned datanodes
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
        new ArrayList<ArrayList<DatanodeInfo>>(numNamenodes);
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
          decommissionNode(i, decommissionedNodes, AdminStates.DECOMMISSIONED);
      decommissionedNodes.add(decomNode);

      // Ensure decommissioned datanode is not automatically shutdown
      DFSClient client = getDfsClient(cluster.getNameNode(i), conf);
      assertEquals("All datanodes must be alive", numDatanodes,
          client.datanodeReport(DatanodeReportType.LIVE).length);
      assertNull(checkFile(fileSys, file1, replicas, decomNode.getXferAddr(),
          numDatanodes));

      // stop decommission and check if the new replicas are removed
      recomissionNode(decomNode);
      // wait for the block to be deleted
      int tries = 0;
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
  @Test
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
          decommissionNode(i, null, AdminStates.DECOMMISSION_INPROGRESS);
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
  @Test
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
    ArrayList<String> list = new ArrayList<String>();
    final String badHostname = "BOGUSHOST";
    list.add(badHostname);
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
      assertEquals(badHostname, info[1].getHostName());
    }
  }
}
