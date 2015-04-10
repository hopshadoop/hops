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



import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.doAnswer;

/**
 * Race between two threads simultaneously calling
 * FSNamesystem.getAdditionalBlock().
 */
public class TestAddBlockRetry {
  public static final Log LOG = LogFactory.getLog(TestAddBlockRetry.class);

  private static final short REPLICATION = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Retry addBlock() while another thread is in chooseTarget(). See HDFS-4452.
   * <p/>
   * HOPS This test is not applicable/fixable for the following reasons Two
   * treads calling the getAdditionalBlock simultaneously will only proceed if
   * they are operating on different indoes. In case of same inode the locking
   * system will serialize the operations because we take write lock on the
   * inode.
   * <p/>
   * This test tries to starts multiple tx in the same threads. The underlying
   * layer
   * complains that a thread can have only one active tx at any time. we tried
   * to
   * fix this issue by creating threads in the test-case but it leads to
   * deadlocks.
   */
  @Ignore
  public void testRetryAddBlockWhileInChooseTarget() throws Exception {
    final String src = "/testRetryAddBlockWhileInChooseTarget";

    final FSNamesystem ns = cluster.getNamesystem();
    final NamenodeProtocols nn = cluster.getNameNodeRpc();

    // create file
    nn.create(src, FsPermission.getFileDefault(), "clientName",
        new EnumSetWritable<>(EnumSet.of(CreateFlag.CREATE)), true,
        (short) 3, 1024);

    // start first addBlock()
    LOG.info("Starting first addBlock for " + src);
    final LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_ADDITIONAL_BLOCK, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();

        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, src);
        locks.add(il)
            .add(lf.getLastTwoBlocksLock(src));

        locks.add(lf.getLeaseLock(TransactionLockTypes.LockType.READ, "clientName"))
            .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
            .add(lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.ER, LockFactory.BLK.UC));
      }

      @Override
      public Object performTask() throws IOException {
        DatanodeStorageInfo targets[] = ns.getNewBlockTargets(
            src, INode.ROOT_PARENT_ID, "clientName",
            null, null, null, onRetryBlock);
        assertNotNull("Targets must be generated", targets);

        // run second addBlock()
        LOG.info("Starting second addBlock for " + src);
        nn.addBlock(src, "clientName", null, null,
            INode.ROOT_PARENT_ID, null);
        assertTrue("Penultimate block must be complete",
            checkFileProgress(src, false));
        LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
        assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
        LocatedBlock lb2 = lbs.get(0);
        assertEquals("Wrong replication", REPLICATION, lb2.getLocations().length);

        // continue first addBlock()
        LocatedBlock newBlock = ns.storeAllocatedBlock(
            src, INode.ROOT_PARENT_ID, "clientName", null, targets);
        assertEquals("Blocks are not equal", lb2.getBlock(), newBlock.getBlock());

        // check locations
        lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
        assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
        LocatedBlock lb1 = lbs.get(0);
        assertEquals("Wrong replication", REPLICATION, lb1.getLocations().length);
        assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
        return null;
      }
    }.handle();
  }

  boolean checkFileProgress(String src, boolean checkall) throws IOException {
    final FSNamesystem ns = cluster.getNamesystem();
    return ns.checkFileProgress(src, ns.dir.getINode(src).asFile(), checkall);
  }

  /*
   * Since NameNode will not persist any locations of the block, addBlock()
   * retry call after restart NN should re-select the locations and return to
   * client. refer HDFS-5257
   */
  @Test
  public void testAddBlockRetryShouldReturnBlockWithLocations()
      throws Exception {
    final String src = "/testAddBlockRetryShouldReturnBlockWithLocations";
    NamenodeProtocols nameNodeRpc = cluster.getNameNodeRpc();
    // create file
    nameNodeRpc.create(src, FsPermission.getFileDefault(), "clientName",
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true,
        (short) 3, 1024);
    // start first addBlock()
    LOG.info("Starting first addBlock for " + src);
    LocatedBlock lb1 = nameNodeRpc.addBlock(src, "clientName", null, null,
        INode.ROOT_PARENT_ID, null);
    assertTrue("Block locations should be present",
        lb1.getLocations().length > 0);

    cluster.restartNameNode();
    nameNodeRpc = cluster.getNameNodeRpc();
    LocatedBlock lb2 = nameNodeRpc.addBlock(src, "clientName", null, null,
        INode.ROOT_PARENT_ID, null);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
    assertTrue("Wrong locations with retry", lb2.getLocations().length > 0);
  }
}
