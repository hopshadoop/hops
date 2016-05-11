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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.base.Preconditions;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.Daemon;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.hops.transaction.lock.LockFactory.BLK;

public class BlockManagerTestUtil {
  public static void setNodeReplicationLimit(final BlockManager blockManager,
      final int limit) {
    blockManager.maxReplicationStreams = limit;
  }

  /**
   * @return the datanode descriptor for the given the given dnUuid.
   */
  public static DatanodeDescriptor getDatanode(final FSNamesystem ns,
      final String dnUuid) {
    return ns.getBlockManager().getDatanodeManager().getDatanodeByUuid(dnUuid);
  }


  /**
   * Refresh block queue counts on the name-node.
   */
  public static void updateState(final BlockManager blockManager)
      throws IOException {
    blockManager.updateState();
  }

  /**
   * @return a tuple of the replica state (number racks, number live
   * replicas, and number needed replicas) for the given block.
   */
  public static int[] getReplicaInfo(final FSNamesystem namesystem,
      final Block b) throws IOException {
    final BlockManager bm = namesystem.getBlockManager();
    return (int[]) new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException, IOException {
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(b.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        return new int[]{getNumberOfRacks(bm, b),
            bm.countNodes(b).liveReplicas(),
            bm.neededReplications.contains(bm.getStoredBlock(b)) ? 1 : 0};
      }
    }.handle(namesystem);
  }

  /**
   * @return the number of racks over which a given block is replicated
   * decommissioning/decommissioned nodes are not counted. corrupt replicas
   * are also ignored
   */
  private static int getNumberOfRacks(final BlockManager blockManager,
      final Block b) throws StorageException, TransactionContextException {
    final Set<String> rackSet = new HashSet<String>(0);
    final Collection<DatanodeDescriptor> corruptNodes =
        getCorruptReplicas(blockManager)
            .getNodes(blockManager.blocksMap.getStoredBlock(b));
    for (Iterator<DatanodeDescriptor> it =
             blockManager.blocksMap.nodeIterator(b); it.hasNext(); ) {
      DatanodeDescriptor cur = it.next();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
        if ((corruptNodes == null) || !corruptNodes.contains(cur)) {
          String rackName = cur.getNetworkLocation();
          if (!rackSet.contains(rackName)) {
            rackSet.add(rackName);
          }
        }
      }
    }
    return rackSet.size();
  }

  /**
   * @param blockManager
   * @return replication monitor thread instance from block manager.
   */
  public static Daemon getReplicationThread(final BlockManager blockManager) {
    return blockManager.replicationThread;
  }
  
  /**
   * @param blockManager
   * @return corruptReplicas from block manager
   */
  public static CorruptReplicasMap getCorruptReplicas(
      final BlockManager blockManager) {
    return blockManager.corruptReplicas;
    
  }

  /**
   * @param blockManager
   * @return computed block replication and block invalidation work that can be
   * scheduled on data-nodes.
   * @throws IOException
   */
  public static int getComputedDatanodeWork(final BlockManager blockManager)
      throws IOException {
    return blockManager.computeDatanodeWork();
  }
  
  public static int computeInvalidationWork(BlockManager bm)
      throws IOException {
    return bm.computeInvalidateWork(Integer.MAX_VALUE);
  }
  
  /**
   * Compute all the replication and invalidation work for the
   * given BlockManager.
   * <p/>
   * This differs from the above functions in that it computes
   * replication work for all DNs rather than a particular subset,
   * regardless of invalidation/replication limit configurations.
   * <p/>
   * NB: you may want to set
   * {@link DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY} to
   * a high value to ensure that all work is calculated.
   */
  public static int computeAllPendingWork(BlockManager bm) throws IOException {
    int work = computeInvalidationWork(bm);
    work += bm.computeReplicationWork(Integer.MAX_VALUE);
    return work;
  }

  /**
   * Ensure that the given NameNode marks the specified DataNode as
   * entirely dead/expired.
   *
   * @param nn
   *     the NameNode to manipulate
   * @param dnName
   *     the name of the DataNode
   */
  public static void noticeDeadDatanode(NameNode nn, String dnName)
      throws IOException {
    FSNamesystem namesystem = nn.getNamesystem();
    DatanodeManager dnm = namesystem.getBlockManager().getDatanodeManager();
    HeartbeatManager hbm = dnm.getHeartbeatManager();
    DatanodeDescriptor[] dnds = hbm.getDatanodes();
    DatanodeDescriptor theDND = null;
    for (DatanodeDescriptor dnd : dnds) {
      if (dnd.getXferAddr().equals(dnName)) {
        theDND = dnd;
      }
    }
    Assert.assertNotNull("Could not find DN with name: " + dnName, theDND);

    synchronized (hbm) {
      theDND.setLastUpdate(0);
      hbm.heartbeatCheck();
    }
  }
  
  /**
   * Change whether the block placement policy will prefer the writer's
   * local Datanode or not.
   *
   * @param prefer
   */
  public static void setWritingPrefersLocalNode(BlockManager bm,
      boolean prefer) {
    BlockPlacementPolicy bpp = bm.getBlockPlacementPolicy();
    Preconditions.checkState(bpp instanceof BlockPlacementPolicyDefault,
        "Must use default policy, got %s", bpp.getClass());
    ((BlockPlacementPolicyDefault) bpp).setPreferLocalNode(prefer);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, boolean initializeStorage) {
    return getDatanodeDescriptor(ipAddr, rackLocation,
        initializeStorage? new DatanodeStorage(DatanodeStorage.generateUuid()): null);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage) {
    return getDatanodeDescriptor(ipAddr, rackLocation, storage, "host");
  }

  /**
   * Call heartbeat check function of HeartbeatManager
   *
   * @param bm
   *     the BlockManager to manipulate
   */
  public static void checkHeartbeat(BlockManager bm) throws IOException {
    bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
  }

  /**
   * Call heartbeat check function of HeartbeatManager and get
   * under replicated blocks count within write lock to make sure
   * computeDatanodeWork doesn't interfere.
   * @param bm the BlockManager to manipulate
   * @return the number of under replicated blocks
   */
  public static int checkHeartbeatAndGetUnderReplicatedBlocksCount(BlockManager bm)
      throws IOException {
    bm.getDatanodeManager().getHeartbeatManager().heartbeatCheck();
    return bm.getUnderReplicatedNotMissingBlocks();
  }

  public static DatanodeStorageInfo updateStorage(DatanodeDescriptor dn,
      DatanodeStorage s) {
    return dn.updateStorage(s);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, DatanodeStorage storage, String hostname) {
    DatanodeDescriptor dn = DFSTestUtil.getDatanodeDescriptor(ipAddr,
        DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT, rackLocation, hostname);
    if (storage != null) {
      dn.updateStorage(storage);
    }
    return dn;
  }

  public static DatanodeStorageInfo newDatanodeStorageInfo(
      DatanodeDescriptor dn, DatanodeStorage s) {
    return new DatanodeStorageInfo(dn, s);
  }

  public static StorageReport[] getStorageReportsForDatanode(
      DatanodeDescriptor dnd) {
    ArrayList<StorageReport> reports = new ArrayList<StorageReport>();
    for (DatanodeStorageInfo storage : dnd.getStorageInfos()) {
      DatanodeStorage dns = new DatanodeStorage(
          storage.getStorageID(), storage.getState(), storage.getStorageType());
      StorageReport report = new StorageReport(
          dns ,false, storage.getCapacity(),
          storage.getDfsUsed(), storage.getRemaining(), storage.getBlockPoolUsed());
      reports.add(report);
    }
    return reports.toArray(StorageReport.EMPTY_ARRAY);
  }
}
