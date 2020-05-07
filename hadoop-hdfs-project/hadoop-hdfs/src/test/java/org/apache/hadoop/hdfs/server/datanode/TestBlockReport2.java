/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.collect.Lists;
import io.hops.metadata.hdfs.entity.HashBucket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.Bucket;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test simulates a variety of situations when blocks are being
 * intentionally corrupted, unexpectedly modified, and so on before a block
 * report is happening
 */
public class TestBlockReport2 {
  public static final Logger LOG = LoggerFactory.getLogger(TestBlockReport2.class);
  static final int BLOCK_SIZE = 1024;
  static final int NUM_BLOCKS = 20;
  static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE;
  private static final int RAND_LIMIT = 2000;
  private static final long DN_RESCAN_INTERVAL = 5000;
  private static final int FILE_START = 0;

  static {
    initLoggers();
  }

  Random rand = new Random(RAND_LIMIT);

  private static void initLoggers() {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  private static void setConfiguration(Configuration conf, int numBuckets) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
            DN_RESCAN_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NUM_BUCKETS_KEY, numBuckets);
  }

  private void waitForTempReplica(MiniDFSCluster cluster, Block bl, int DN_N1)
          throws IOException {
    final boolean tooLongWait = false;
    final int TIMEOUT = 40000;

    if (LOG.isDebugEnabled()) {
      LOG.info("Wait for datanode " + DN_N1 + " to appear");
    }
    while (cluster.getDataNodes().size() <= DN_N1) {
      waitTil(20);
    }
    if (LOG.isDebugEnabled()) {
      LOG.info("Total number of DNs " + cluster.getDataNodes().size());
    }
    cluster.waitActive();

    // Look about specified DN for the replica of the block from 1st DN
    final DataNode dn1 = cluster.getDataNodes().get(DN_N1);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    Replica r = DataNodeTestUtils.fetchReplicaInfo(dn1, bpid, bl.getBlockId());
    long start = Time.now();
    int count = 0;
    while (r == null) {
      waitTil(5);
      r = DataNodeTestUtils.fetchReplicaInfo(dn1, bpid, bl.getBlockId());
      long waiting_period = Time.now() - start;
      if (count++ % 100 == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.info("Has been waiting for " + waiting_period + " ms.");
        }
      }
      if (waiting_period > TIMEOUT) {
        assertTrue("Was waiting too long to get ReplicaInfo from a datanode",
                tooLongWait);
      }
    }

    HdfsServerConstants.ReplicaState state = r.getState();
    if (LOG.isDebugEnabled()) {
      LOG.info("Replica state before the loop " + state.getValue());
    }
    start = Time.now();
    while (state != HdfsServerConstants.ReplicaState.TEMPORARY) {
      waitTil(5);
      state = r.getState();
      if (LOG.isDebugEnabled()) {
        LOG.info("Keep waiting for " + bl.getBlockName() +
                " is in state " + state.getValue());
      }
      if (Time.now() - start > TIMEOUT) {
        assertTrue("Was waiting too long for a replica to become TEMPORARY",
                tooLongWait);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.info("Replica state after the loop " + state.getValue());
    }
  }

  private void startDNandWait(MiniDFSCluster cluster, int count)
          throws IOException, InterruptedException, TimeoutException {
    if (LOG.isDebugEnabled()) {
      LOG.info("Before next DN start: " + cluster.getDataNodes().size());
    }
    int expectedDatanodes = count + cluster.getDataNodes().size();
    cluster.startDataNodes(cluster.getConfiguration(0), 1, true, null, null);
    cluster.waitClusterUp();
    ArrayList<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(datanodes.size(), expectedDatanodes);

    if (LOG.isDebugEnabled()) {
      int lastDn = datanodes.size() - 1;
      LOG.info("New datanode " +
              cluster.getDataNodes().get(lastDn).getDisplayName() +
              " has been started");
    }
  }

  private void waitReplication(MiniDFSCluster cluster, short replication,
                               Path filePath) throws IOException, TimeoutException, InterruptedException {
    DFSTestUtil.waitReplication(cluster.getFileSystem(), filePath, replication);
  }

  private void append(MiniDFSCluster cluster, final Path filePath) throws IOException {
    LOG.info("Appending to " + filePath);
    DFSTestUtil.appendFile(cluster.getFileSystem(), filePath,
            "appended-string");
  }

  private ArrayList<Block> prepareForRide(MiniDFSCluster cluster, final Path
          filePath, short replication, int numBlocks) throws
          IOException {

    LOG.info("Creatig File " + filePath);
    DFSTestUtil.createFile(cluster.getFileSystem(), filePath, numBlocks * BLOCK_SIZE, replication,
            rand.nextLong());

    return locatedToBlocks(cluster.getNameNodeRpc()
            .getBlockLocations(filePath.toString(), FILE_START, numBlocks * BLOCK_SIZE)
            .getLocatedBlocks(), null);
  }

  private void printStats(MiniDFSCluster cluster) throws IOException {
    BlockManagerTestUtil.updateState(cluster.getNamesystem().getBlockManager());
    if (LOG.isDebugEnabled()) {
      LOG.info("Missing " + cluster.getNamesystem().getMissingBlocksCount());
      LOG.info(
              "Corrupted " + cluster.getNamesystem().getCorruptReplicaBlocks());
      LOG.info("Under-replicated " + cluster.getNamesystem().
              getUnderReplicatedBlocks());
      LOG.info("Pending delete " + cluster.getNamesystem().
              getPendingDeletionBlocks());
      LOG.info("Pending replications " + cluster.getNamesystem().
              getPendingReplicationBlocks());
      LOG.info("Excess " + cluster.getNamesystem().getExcessBlocks());
      LOG.info("Total " + cluster.getNamesystem().getBlocksTotal());
    }
  }

  private ArrayList<Block> locatedToBlocks(final List<LocatedBlock> locatedBlks,
                                           List<Integer> positionsToRemove) {
    ArrayList<Block> newList = new ArrayList<>();
    for (int i = 0; i < locatedBlks.size(); i++) {
      if (positionsToRemove != null && positionsToRemove.contains(i)) {
        if (LOG.isDebugEnabled()) {
          LOG.info(i + " block to be omitted");
        }
        continue;
      }
      newList.add(new Block(locatedBlks.get(i).getBlock().getLocalBlock()));
    }
    return newList;
  }

  private void waitTil(long waitPeriod) {
    try { //Wait til next re-scan
      Thread.sleep(waitPeriod);
    } catch (InterruptedException e) {
      LOG.info(e.toString(),e);
    }
  }

  private List<File> findAllFiles(File top, FilenameFilter mask) {
    if (top == null) {
      return null;
    }
    ArrayList<File> ret = new ArrayList<>();
    for (File f : top.listFiles()) {
      if (f.isDirectory()) {
        ret.addAll(findAllFiles(f, mask));
      } else if (mask.accept(f, f.getName())) {
        ret.add(f);
      }
    }
    return ret;
  }

  private void corruptBlockLen(final Block block) throws IOException {
    if (block == null) {
      throw new IOException("Block isn't suppose to be null");
    }
    long oldLen = block.getNumBytes();
    long newLen = oldLen - rand.nextLong();
    assertTrue("Old and new length shouldn't be the same",
            block.getNumBytes() != newLen);
    block.setNumBytesNoPersistance(newLen);
    if (LOG.isDebugEnabled()) {
      LOG.info("Length of " + block.getBlockName() +
              " is changed to " + newLen + " from " + oldLen);
    }
  }

  private void corruptBlockGS(final Block block) throws IOException {
    if (block == null) {
      throw new IOException("Block isn't suppose to be null");
    }
    long oldGS = block.getGenerationStamp();
    long newGS = oldGS - rand.nextLong();
    assertTrue("Old and new GS shouldn't be the same",
            block.getGenerationStamp() != newGS);
    block.setGenerationStampNoPersistance(newGS);
    if (LOG.isDebugEnabled()) {
      LOG.info("Generation stamp of " + block.getBlockName() +
              " is changed to " + block.getGenerationStamp() + " from " + oldGS);
    }
  }

  private Block findBlock(MiniDFSCluster cluster, Path path, long size) throws
          IOException {
    Block ret;
    List<LocatedBlock> lbs = cluster.getNameNodeRpc()
            .getBlockLocations(path.toString(), FILE_START, size)
            .getLocatedBlocks();
    LocatedBlock lb = lbs.get(lbs.size() - 1);

    // Get block from the first DN
    ret = cluster.getDataNodes().get(0).
            data.getStoredBlock(lb.getBlock().getBlockPoolId(),
            lb.getBlock().getBlockId());
    return ret;
  }

  /**
   * In case of no error or concurrent writes the hashes of the
   * buckets on the datanode side and namenode side should match
   *
   * @throws IOException
   */
  @Test
  public void blockReport_01() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      cluster.waitActive();

      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      for (int i = 0; i < 1; i++) {
        int numBlocks = 1;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);
      }


      //make sure that all incremental block reports are processed
      Thread.sleep(10000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


      LOG.info("Deleting all files");
      for (int i = 0; i < 1; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        fs.delete(filePath, false);
      }

      //make sure that the blocks are deleted from the disk
      Thread.sleep(30000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      LOG.info(e.toString(),e);
      fail(e.toString());
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * testing effect of append operations on br
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_02() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      for (int i = 0; i < 5; i++) {
        int numBlocks = 3;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);
      }

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      // append data
      for (int i = 0; i < 5; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        append(cluster, filePath);
      }

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(),e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * flush does not have any effect on blk reporting
   * however calling hflush invalidates the bucket. When hflush is called the
   * datanode updates the length of the block, however the corresponding changes
   * on the namenode side are not applied. After the block is closed there should
   * be no difference in block hashes.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_03() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    final int NUM_FILES = 5;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      FSDataOutputStream outs[] = new FSDataOutputStream[5];
      for (int i = 0; i < NUM_FILES; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        LOG.info("Creating file: " + filePath);
        outs[i] = fs.create(filePath, REPLICATION);
        //write half a block
        byte data[] = new byte[BLOCK_SIZE / 2];
        outs[i].write(data);
        outs[i].flush(); //does not have any impact on block reports
        LOG.info("Flushed half a block");
      }

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      //write more data but do not fill the entire block
      for (int i = 0; i < NUM_FILES; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        byte data[] = new byte[BLOCK_SIZE / 2 - 1];
        outs[i].write(data);
        outs[i].hflush();
        LOG.info("HFlushed half a block -1 ");
      }

      Thread.sleep(50000); //wait for all incremental block reports

      matchDNandNNState(0, NUM_DATANODES, cluster, NUM_FILES*REPLICATION, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, NUM_FILES*REPLICATION, numBuckets);

      //fill the block
      for (int i = 0; i < NUM_FILES; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        byte data[] = new byte[BLOCK_SIZE+1];
        outs[i].write(data);
        outs[i].hflush();
        outs[i].close();
        LOG.info("HFlushed 1 byte. The block is complete and file is closed.");
      }

      //make sure that all incremental block reports are processed
      LOG.info("Sleeping to make sure that all the incremental BR are " +
              "received");
      Thread.sleep(10000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(),e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * Test hard lease recovery
   */
  @Ignore // Think this case can't be handled by bucket report
  @Test
  public void blockReport_04() throws Exception {
    blockReprot_hardlease(true);
  }

  @Ignore // Think this case can't be handled by bucket report
  @Test
  public void blockReport_05() throws Exception {
    blockReprot_hardlease(false);
  }

  public void blockReprot_hardlease(boolean hflush) throws Exception {
    //create a file
    DistributedFileSystem dfs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final long SHORT_LEASE_PERIOD = 1000L;
    final long LONG_LEASE_PERIOD = 60 * 60 * SHORT_LEASE_PERIOD;
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      dfs = cluster.getFileSystem();

      String filestr = "/hardLeaseRecovery";
      LOG.info("filestr=" + filestr);
      Path filepath = new Path(filestr);
      FSDataOutputStream stm = dfs.create(filepath);
      assertTrue(dfs.getClient().exists(filestr));

      // write bytes into the file.
      //int size = AppendTestUtil.nextInt(FILE_SIZE);
      int size = (int) (BLOCK_SIZE * 1.5); // write 1.5 blocks
      byte[] buffer = new byte[FILE_SIZE];
      LOG.info("size=" + size);
      stm.write(buffer, 0, BLOCK_SIZE + 1);

      //one complete block is writing and only one byte is written to the
      // second block
      Thread.sleep(5000);
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      stm.write(buffer, 0, (int) (BLOCK_SIZE / 2 - 1));

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      // hflush file
      if (hflush) {
        LOG.info("hflush");
        stm.hflush();
      }

      // kill the lease renewal thread
      LOG.info("leasechecker.interruptAndJoin()");
      dfs.getClient().getLeaseRenewer().interruptAndJoin();

      // set the hard limit to be 1 second
      cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);

      // wait for lease recovery to complete
      LocatedBlocks locatedBlocks;
      do {
        Thread.sleep(SHORT_LEASE_PERIOD);
        locatedBlocks = dfs.getClient().getLocatedBlocks(filestr, 0L, size);
      } while (locatedBlocks.isUnderConstruction());
      assertEquals(size, locatedBlocks.getFileLength());

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      // make sure that the writer thread gets killed
      try {
        stm.write('b');
        stm.close();
        fail("Writer thread should have been killed");
      } catch (IOException e) {
        LOG.info(e.toString(), e);
      }

      // verify data
      AppendTestUtil.LOG
              .info("File size is good. Now validating sizes from datanodes...");
      AppendTestUtil.checkFullFile(dfs, filepath, size, buffer, filestr);
    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      dfs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  /**
   * concat should not have any effect on the BR
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_06() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      int numFiles = 5;
      Path paths[] = new Path[numFiles - 1];
      Path dst = null;
      for (int i = 0; i < numFiles; i++) {
        int numBlocks = 3;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);

        if (i == (numFiles - 1)) {
          dst = filePath;
        } else {
          paths[i] = filePath;
        }
      }

      fs.concat(dst, paths);

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      LOG.info(e.toString(), e);
      fail(e.toString());
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  /**
   * testing chaning the replication of file
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_07() throws IOException, InterruptedException {
    blockReportReplication((short) 3, (short) 6);
  }

  @Ignore  //Don't know why this doesn't work
  @Test
  public void blockReport_08() throws IOException, InterruptedException {
    //for some reason reducing the replicaton doe snot work well
    //see testblockswithnotenoughracks.testreducereplicationwith...
    blockReportReplication((short) 6, (short) 3);
  }

  public void blockReportReplication(short replication, short change)
          throws
          IOException,
          InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 6;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      int numFiles = 5;
      int numBlocks = 3;
      for (int i = 0; i < numFiles; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, replication, numBlocks);
      }

      Thread.sleep(10000);

      int totalReplicas = numFiles * numBlocks * replication;
      int dnBlocks = countDNBlocks(cluster);
      int nnBlocks = countNNBlocks(0, cluster);
      assertTrue("Number of blocks do not match, DN Blocks: " + dnBlocks + " NN " +
                      "Blocks: " + nnBlocks + " Both should be equal to: " + totalReplicas,
              dnBlocks == totalReplicas &&
                      nnBlocks == totalReplicas);

      //change replication
      for (int i = 0; i < numFiles; i++) {
        Path filePath = new Path(baseName + "/" + i + ".dat");
        fs.setReplication(filePath, change);
      }

      //make sure that the replication is
      Thread.sleep(60000);

      totalReplicas = numFiles * numBlocks * change;
      dnBlocks = countDNBlocks(cluster);
      nnBlocks = countNNBlocks(0, cluster);
      assertTrue("Number of blocks do not match, DN Blocks: " + dnBlocks + " NN " +
                      "Blocks: " + nnBlocks + " Both should be equal to: " + totalReplicas,
              dnBlocks == totalReplicas &&
                      nnBlocks == totalReplicas);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);
    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  @Test
  public void blockReport_09() throws IOException, InterruptedException {
    concurrentWrites(1 /*threads*/,
            (short) 1 /*replication*/,
            10 /*numDataNodes*/,
            0 /*threshold*/);
  }

  @Test
  public void blockReport_10() throws IOException, InterruptedException {
    concurrentWrites(5 /*threads*/,
            (short) 3 /*replication*/,
            10 /*numDataNodes*/,
            0 /*threshold*/);
  }

  public void concurrentWrites(int numThreads, short replication, int
          numDataNodes, int threashold) throws IOException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(numDataNodes).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, numDataNodes, cluster, 0, numBuckets);
      sendAndCheckBR(0, numDataNodes, cluster, poolId, 0, numBuckets);

      SomeWorkload threads[] = new SomeWorkload[numThreads];
      for (int i = 0; i < numThreads; i++) {
        Path filePath = new Path(baseName + "/test" + i + ".dat");
        threads[i] = new SomeWorkload(cluster, BLOCK_SIZE, replication, i);
        threads[i].start();
      }

      Thread.sleep(60 * 1000);

      for (int i = 0; i < numThreads; i++) {
        threads[i].stopIt();
      }

      //some blocks are deleted. wait for some time so that the
      //blocks are all removed from the datanodes
      Thread.sleep(30000);
      matchDNandNNState(0, numDataNodes, cluster, 0, numBuckets);
      sendAndCheckBR(0, numDataNodes, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * Testing BR and cluster restart
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_11() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      for (int i = 0; i < 5; i++) {
        int numBlocks = 3;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);
      }

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      LOG.info("Before corrupting hashes ");
      
      corruptNNHashes(0, cluster);

      LOG.info("Restarting Cluster");
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).format
              (false).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      Thread.sleep(5000);
      LOG.info("After restart comparing states");
      //after initial BR
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * Testing BR and cluster restart
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Ignore //This test doesn't make sense
  @Test
  public void blockReport_12() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      for (int i = 0; i < 5; i++) {
        int numBlocks = 3;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);
      }

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      corruptNNHashes(0, cluster);

      matchDNandNNState(0, NUM_DATANODES, cluster, NUM_DATANODES * numBuckets, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, NUM_DATANODES * numBuckets, numBuckets);


      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }


  /**
   * Testing reconfiguration
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Ignore
  // Hash buckets do not match report after sending. Either they contain non-finalized blocks
  // or there is some logical problem with letting the full block report "reset" hashes.
  @Test
  public void blockReport_13() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    try {
      Configuration conf = new Configuration();
      int numBuckets = 5;
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();


      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      for (int i = 0; i < 5; i++) {
        int numBlocks = 3;
        Path filePath = new Path(baseName + "/" + i + ".dat");
        prepareForRide(cluster, filePath, REPLICATION, numBlocks);
      }

      //make sure that all incremental block reports are processed
      Thread.sleep(5000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      deleteHashes(0, cluster);
      //Increase the number of bucktes
      cluster.shutdown();
      conf = new Configuration();
      numBuckets = 10;
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (false).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      matchDNandNNState(0, NUM_DATANODES, cluster, NUM_DATANODES * numBuckets, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, NUM_DATANODES * numBuckets, numBuckets);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      deleteHashes(0, cluster);
      //Decrease the number of buckets
      cluster.shutdown();
      conf = new Configuration();
      numBuckets = 3;
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (false).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      matchDNandNNState(0, NUM_DATANODES, cluster, NUM_DATANODES * numBuckets, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, NUM_DATANODES * numBuckets, numBuckets);


      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  private void checkStats(ReportStatistics stats, int numBuckets) {
    assertEquals("No buckets should have mismatched ", 0, numBuckets - stats
            .getNumBucketsMatching());
  }

  private void matchDNandNNState(int nnId, int numDataNodes, MiniDFSCluster
          cluster, int tolerance, int numBuckets) throws IOException {

    int mismatchCount = 0;

    for (int i = 0; i < numDataNodes; i++) {
      LOG.info("DataNode Index: " + i);
      DataNode dn = cluster.getDataNodes().get(i);
      Map<DatanodeStorage, BlockReport> storageReports = getDNBR(cluster, dn, numBuckets);

      for (Map.Entry<DatanodeStorage, BlockReport> entry : storageReports.entrySet()) {
        BlockReport value = entry.getValue();
        List<byte[]> dnHashes = new ArrayList<>();
        for (Bucket bucket : value.getBuckets()) {
          dnHashes.add(bucket.getHash());
        }

        DatanodeStorageInfo storage = cluster.getNamesystem().getBlockManager().getDatanodeManager().getDatanode(dn.getDatanodeId()).getStorageInfo(entry.getKey().getStorageID());
        List<HashBucket> storageHashes = getStorageHashes(storage);
        LOG.warn("Total buckets "+storageHashes.size());

        assertFalse("More buckets on NN than on DN. might indicate configuration issue.", storageHashes.size() > dnHashes.size());

        if (storageHashes.size() != dnHashes.size()) {
          LOG.info("Number of hashes on NN doesn't match DN. This should only be the case before first report.");
        }

        LOG.warn("numBuckets size "+numBuckets);
        byte[][] nnHashes = new byte[HashBuckets.HASH_LENGTH][numBuckets];
        for (HashBucket storageHash : storageHashes) {
          LOG.warn("adding "+storageHash.getBucketId()+", "+ storageHash.getHash());
          nnHashes[storageHash.getBucketId()] =  storageHash.getHash();
        }

        StringBuilder sb = new StringBuilder();
        for(byte[] hash : dnHashes){
          sb.append(HashBuckets.hashToString(hash)).append(", ");
        }
        LOG.info("DN Hash: " + sb);
        sb = new StringBuilder();
        for(byte[] hash : nnHashes){
          sb.append(HashBuckets.hashToString(hash)).append(", ");
        }
        LOG.info("NN Hash: " + sb);

        for (int j = 0; j < numBuckets; j++) {
          byte[] dnHash = dnHashes.get(j);
          byte[] nnHash = nnHashes[j];
          if (!HashBuckets.hashEquals(dnHash,nnHash)) {
            mismatchCount++;
          }
        }
      }
    }

    if (mismatchCount > tolerance) {
      String msg = "The Hashes Did not match. Mismatched Hashes: " + mismatchCount + " " +
              "Tolerance: " + tolerance;
      LOG.info(msg);
      fail(msg);
    }
  }

  private void corruptNNHashes(int nnId, MiniDFSCluster cluster) throws IOException {
    for (DataNode dn : cluster.getDataNodes()) {
      BlockManager bm = cluster.getNamesystem(nnId).getBlockManager();
      DatanodeDescriptor dnd = bm.getDatanodeManager().getDatanode(dn.getDatanodeId());
      for (DatanodeStorageInfo storageInfo : dnd.getStorageInfos()) {
        HashBuckets.getInstance().corruptHashBuckets(storageInfo);
      }
    }
  }

  private void deleteHashes(int nnId, MiniDFSCluster cluster) throws IOException {
    for (DataNode dn : cluster.getDataNodes()) {
      BlockManager bm = cluster.getNamesystem(nnId).getBlockManager();
      DatanodeDescriptor dnd = bm.getDatanodeManager().getDatanode(dn.getDatanodeId());
      DatanodeStorageInfo[] storageInfos = dnd.getStorageInfos();
      for (DatanodeStorageInfo storageInfo : dnd.getStorageInfos()) {
        HashBuckets.getInstance().deleteHashBuckets(storageInfo);
      }
    }
  }

  private static Map<DatanodeStorage, BlockReport> getDNBR(MiniDFSCluster cluster, DataNode dn,
                                               int numBuckets) {
    String poolId = cluster.getNamesystem().getBlockPoolId();
    Map<DatanodeStorage, BlockReport> br = dn.getFSDataset().getBlockReports(poolId);
    for (BlockReport reportedBlocks : br.values()) {
      assertEquals("Wrong number of buckets read for DN: " + dn, numBuckets,
              reportedBlocks.getBuckets().length);

    }
    return br;
  }

  public static void sendAndCheckBR(int nnId, int numDataNodes,
                              MiniDFSCluster cluster, String poolId,
                              int tolerance, int numBuckets) throws IOException {
    int mismatched = 0;
    for (int i = 0; i < numDataNodes; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      Map<DatanodeStorage, BlockReport> dnBr = getDNBR(cluster, dn, numBuckets);
      BlockManager bm = cluster.getNamesystem(nnId).getBlockManager();
      for (Map.Entry<DatanodeStorage, BlockReport> datanodeStorageBlockReportEntry : dnBr.entrySet()) {

        DatanodeDescriptor datanode = cluster.getNamesystem().getBlockManager().getDatanodeManager().getDatanode(dn.getDatanodeId());
        DatanodeStorageInfo storageInfo = datanode.getStorageInfo(datanodeStorageBlockReportEntry.getKey().getStorageID());

        List<Integer> mismatchingbuckets = bm.checkHashes(dn.getDatanodeId(),
                datanodeStorageBlockReportEntry.getKey(), datanodeStorageBlockReportEntry.getValue());
        BPOfferService.removeMatchingBuckets(mismatchingbuckets, datanodeStorageBlockReportEntry.getValue());

        BlockManager.ReportStatistics stats = bm.processReport(storageInfo,
                datanodeStorageBlockReportEntry.getValue());
        mismatched += (numBuckets - stats.numBucketsMatching);

      }
    }

    if (mismatched > tolerance) {
      String msg = "BR Buckets mismatched : " + mismatched + " Tolerance: " +
              tolerance;
      LOG.info(msg);
      fail(msg);
    }
  }

  private int countDNBlocks(MiniDFSCluster cluster)
          throws IOException {
    int count = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      String poolId = cluster.getNamesystem().getBlockPoolId();
      Map<DatanodeStorage, BlockReport> blockReports = dn.getFSDataset().getBlockReports(poolId);
      for (BlockReport reportedBlocks : blockReports.values()) {
        count += reportedBlocks.getNumberOfBlocks();
      }
    }
    return count;
  }

  private int countNNBlocks(int nnId, MiniDFSCluster cluster) throws IOException {

    int count = 0;
    BlockManager bm = cluster.getNamesystem(nnId).getBlockManager();
    DatanodeManager dnm = bm.getDatanodeManager();
    for (DataNode dn : cluster.getDataNodes()) {
      count += dnm.getDatanode(dn.getDatanodeId()).numBlocks();
    }
    return count;
  }

  private List<HashBucket> getStorageHashes(DatanodeStorageInfo storage) throws IOException {
    List<HashBucket> namenodeHashes = HashBuckets.getInstance().getBucketsForStorage(storage);

    boolean hasIncorrectStorageBucket = false;
    for (HashBucket namenodeHash : namenodeHashes) {
      if (namenodeHash.getStorageId() != storage.getSid()) {
        hasIncorrectStorageBucket = true;
        break;
      }
    }
    assertFalse("HashBuckets.getBucketForStorage() returned incorrect storage hash", hasIncorrectStorageBucket);

    return namenodeHashes;
  }


  private class MyFileFilter implements FilenameFilter {
    private String nameToAccept = "";
    private boolean all = false;

    public MyFileFilter(String nameToAccept, boolean all) {
      if (nameToAccept == null) {
        throw new IllegalArgumentException("Argument isn't suppose to be null");
      }
      this.nameToAccept = nameToAccept;
      this.all = all;
    }

    @Override
    public boolean accept(File file, String s) {
      if (all) {
        return s != null && s.startsWith(nameToAccept);
      } else {
        return s != null && s.equals(nameToAccept);
      }
    }
  }

  private class SomeWorkload extends Thread {
    MiniDFSCluster cluster;
    boolean stop = false;
    int blockSize;
    short replication;
    List<Path> paths = new ArrayList<Path>();
    final int tid;
    int counter = 0;
    Random rand = new Random(System.currentTimeMillis());

    public SomeWorkload(MiniDFSCluster cluster, int blockSize, short replication, int tid) {
      this.cluster = cluster;
      this.replication = replication;
      this.blockSize = blockSize;
      this.tid = tid;
    }

    public void stopIt() {
      stop = true;
    }

    @Override
    public void run() {
      try {

        while (!stop) {
          int choice = rand.nextInt(3);
          switch (choice) {
            case 0: // create
              Path filePath = new Path("/" + tid + "/" + counter++ + ".bin");
              FSDataOutputStream out = cluster.getFileSystem().create(filePath, replication);
              byte buffer[] = new byte[blockSize];
              out.write(buffer);
              out.close();
              paths.add(filePath);
              break;
            case 1: // delete
              if (paths.size() > 0) {
                Path path = paths.remove(0);
                cluster.getFileSystem().delete(path, true);
              }
              break;
            case 2: // some thing else
              if (paths.size() > 0) {
                Path path = paths.get(0);
                cluster.getFileSystem().setReplication(path,
                        (short) (replication + 1));
              }
              break;
            default:
              throw new UnsupportedOperationException("FIX ME");
          }
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        LOG.info(e.toString(), e);
        fail("Failed to start BlockChecker: " + e);
      }
    }
  }

  /**
   * test overwrite file on creation
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_14() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      cluster.waitActive();

      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      int numBlocks = 1;
      Path filePath = new Path(baseName + "/" + "file.dat");
      prepareForRide(cluster, filePath, REPLICATION, numBlocks);

      //make sure that all incremental block reports are processed
      Thread.sleep(10000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      filePath = new Path(baseName + "/" + "file.dat");
      FSDataOutputStream out = fs.create(filePath, true);
      out.write(1);
      out.close();

      //make sure that the blocks are deleted from the disk
      Thread.sleep(30000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  /**
   * test overwrite file on rename
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void blockReport_15() throws IOException, InterruptedException {
    DistributedFileSystem fs = null;
    MiniDFSCluster cluster = null;
    final int NUM_DATANODES = 5;
    final short REPLICATION = 3;
    String poolId = null;
    final String baseName = "/dir";
    final int numBuckets = 5;
    try {
      Configuration conf = new Configuration();
      setConfiguration(conf, numBuckets);
      cluster = new MiniDFSCluster.Builder(conf).format
              (true).numDataNodes(NUM_DATANODES).build();
      fs = (DistributedFileSystem) cluster.getFileSystem();

      cluster.waitActive();

      final String METHOD_NAME = GenericTestUtils.getMethodName();
      LOG.info("Running test " + METHOD_NAME);

      // empty block reports should match
      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      int numBlocks = 1;
      Path filePath1 = new Path(baseName + "/" + "file1.dat");
      Path filePath2 = new Path(baseName + "/" + "file2.dat");
      prepareForRide(cluster, filePath1, REPLICATION, numBlocks);
      prepareForRide(cluster, filePath2, REPLICATION, numBlocks);

      //make sure that all incremental block reports are processed
      Thread.sleep(10000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);

      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);

      fs.rename(filePath1, filePath2, Options.Rename.OVERWRITE);

      //make sure that the blocks are deleted from the disk
      Thread.sleep(30000);

      matchDNandNNState(0, NUM_DATANODES, cluster, 0, numBuckets);
      sendAndCheckBR(0, NUM_DATANODES, cluster, poolId, 0, numBuckets);


    } catch (Exception e) {
      fail(e.toString());
      LOG.info(e.toString(), e);
    } finally {
      fs.close();
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }

  @Test
  public void blockReport_16() throws IOException, InterruptedException {
    short replicas = (short) 1;
    Configuration conf = new HdfsConfiguration();
    final int numBuckets = 5;
    setConfiguration(conf, numBuckets);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
            DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags =
            EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);
    final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE + 1;
    final String fileName = "hflushtest.dat";
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).format(true).numDataNodes(replicas).build();

    cluster.waitActive();

    Thread.sleep(10000);

    DistributedFileSystem fileSystem = cluster.getFileSystem();

    Path testFile = new Path("/testfile");
    try {
      FSDataOutputStream out = fileSystem.create(testFile, replicas);
      BRTestWriter writer = new BRTestWriter(out);
      Thread t = new Thread(writer);
      t.start();

      cluster.getDataNodes().get(0).scheduleAllBlockReport(0);

      Thread.sleep(15000);

      writer.stop();
      t.join();
      out.close();

      // open file and read all blocks
      FSDataInputStream in = fileSystem.open(testFile);
      byte[] block = AppendTestUtil.initBuffer(BLOCK_SIZE);
      for (int i = 0; i < writer.blocksWritten; i++) {
        in.read(block);
      }
      in.close();

    } finally {
      fileSystem.close();
      cluster.shutdown();
    }
  }


  private class BRTestWriter implements Runnable {
    FSDataOutputStream out;
    boolean stop = false;
    byte[] block;
    int blocksWritten = 0;
    BRTestWriter(FSDataOutputStream out){
      this.out  = out;
      block = AppendTestUtil.initBuffer(BLOCK_SIZE);
    }

    public void stop(){
      stop = true;
    }
    @Override
    public void run() {
      try {
        while (!stop) {
          out.write(block);
          out.hflush();
          blocksWritten++;
        }
      }catch (IOException e){
       LOG.info(e.toString(),e);
      }
    }
  }
}
