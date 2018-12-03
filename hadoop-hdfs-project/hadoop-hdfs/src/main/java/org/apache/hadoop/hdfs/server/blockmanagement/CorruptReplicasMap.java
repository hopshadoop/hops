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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.entity.CorruptReplica;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor>
 */

@InterfaceAudience.Private
public class CorruptReplicasMap {
  
  /** The corruption reason code */
  public static enum Reason {
    NONE,                // not specified.
    ANY,                 // wildcard reason
    GENSTAMP_MISMATCH,   // mismatch in generation stamps
    SIZE_MISMATCH,       // mismatch in sizes
    INVALID_STATE,       // invalid state
    CORRUPTION_REPORTED  // client or datanode reported the corruption
  }

  private final DatanodeManager datanodeMgr;
  private static final Log LOG =
      LogFactory.getLog(CorruptReplicasMap.class);
  
  public CorruptReplicasMap(DatanodeManager datanodeMgr) {
    this.datanodeMgr = datanodeMgr;
  }
  
  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   * @param reason a textual reason (for logging purposes)
   * @param reasonCode the enum representation of the reason
   */
  public void addToCorruptReplicasMap(BlockInfo blk, DatanodeStorageInfo storage,
      String reason, Reason reasonCode) throws StorageException, TransactionContextException {
    Map <DatanodeDescriptor, Reason> nodes = getNodesMap(blk);
    String reasonText;
    if (reason != null) {
      reasonText = " because " + reason;
    } else {
      reasonText = "";
    }

    if(storage == null) {
      LOG.warn("Attempted to set block " + blk.getBlockId() + " as corrupt on" +
          " non-existing storage (null)" + reason);
      return;
    }
    
    if (!nodes.keySet().contains(storage.getDatanodeDescriptor())) {
      NameNode.blockStateChangeLog
          .info("BLOCK NameSystem.addToCorruptReplicasMap: " +
              blk.getBlockName() + " added as corrupt on " + storage +
              " by " + Server.getRemoteIp() + reasonText);
    } else {
      NameNode.blockStateChangeLog
          .info("BLOCK NameSystem.addToCorruptReplicasMap: " +
              "duplicate requested for " + blk.getBlockName() +
              " to add as corrupt on " + storage +
              " by " + Server.getRemoteIp() + reasonText);
    }
    // Add the node or update the reason.
    addCorruptReplicaToDB(new CorruptReplica(storage.getSid(),
          blk.getBlockId(), blk.getInodeId(), reasonCode.name()));
  }

  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk
   *     Block to be removed
   */
  void removeFromCorruptReplicasMap(BlockInfo blk)
      throws StorageException, TransactionContextException {
    Collection<CorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    if (corruptReplicas != null) {
      for (CorruptReplica cr : corruptReplicas) {
        removeCorruptReplicaFromDB(cr);
      }
    }
  }

  /**
   * Remove the block at the given datanode from CorruptBlockMap
   * @param blk block to be removed
   * @param datanode datanode where the block is located
   * @return true if the removal is successful;
   * false if the replica is not in the map
   */
  boolean removeFromCorruptReplicasMap(BlockInfo blk, DatanodeDescriptor
      datanode) throws IOException {
    return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
  }
  
  boolean removeFromCorruptReplicasMap(BlockInfo blk, DatanodeDescriptor datanode,
      Reason reason) throws StorageException, TransactionContextException {
    Collection<CorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    Map <DatanodeDescriptor, Reason> datanodes = getNodesMap(corruptReplicas);
    if (datanodes==null)
      return false;
    
    // if reasons can be compared but don't match, return false.
    Reason storedReason = datanodes.get(datanode);
    if (reason != Reason.ANY && storedReason != null &&
        reason != storedReason) {
      return false;
    }

    if (datanodes.containsKey(datanode)) { // remove the replicas
      Set<Integer> sids = new HashSet<>();
      for(DatanodeStorageInfo storage: datanode.getStorageInfos()){
        sids.add(storage.getSid());
      }
      for(CorruptReplica c : corruptReplicas){
        if(sids.contains(c.getStorageId())){
          removeCorruptReplicaFromDB(new CorruptReplica(c.getStorageId(), blk
            .getBlockId(), blk.getInodeId(), c.getReason()));
        }
      }
      return true;
    }
    return false;
  
  }

  void forceRemoveFromCorruptReplicasMap(BlockInfo blk, int sid) throws StorageException, TransactionContextException {
    Collection<CorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    if (corruptReplicas == null) {
      return;
    }
    for (CorruptReplica c : corruptReplicas) {
      if (c.getStorageId() == sid) {
        removeCorruptReplicaFromDB(new CorruptReplica(c.getStorageId(), blk
            .getBlockId(), blk.getInodeId(), c.getReason()));
      }
    }
  }

  /**
   * Get Nodes which have corrupt replicas of Block
   *
   * @param blk
   *     Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Collection<DatanodeDescriptor> getNodes(BlockInfo blk) throws StorageException, TransactionContextException {
    // HOPS datanodeMgr is null in some tests
    if (datanodeMgr == null) {
      return new ArrayList<>();
    }

    Collection<CorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    Collection<DatanodeDescriptor> dnds = new TreeSet<DatanodeDescriptor>();
    if (corruptReplicas != null) {
      for (CorruptReplica cr : corruptReplicas) {
        DatanodeDescriptor dn = datanodeMgr.getDatanodeBySid(cr.getStorageId());

        if (dn != null) {
          dnds.add(dn);
        }
      }
    }
    return dnds;
  }

    /**
   * Get Nodes which have corrupt replicas of Block
   *
   * @param blk
   *     Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Map<DatanodeDescriptor, Reason> getNodesMap(BlockInfo blk) throws StorageException, TransactionContextException {
    // HOPS datanodeMgr is null in some tests
    if (datanodeMgr == null) {
      return new TreeMap<>();
    }

    Collection<CorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    return getNodesMap(corruptReplicas);
  }
  
  Map<DatanodeDescriptor, Reason> getNodesMap(Collection<CorruptReplica> corruptReplicas) throws StorageException, TransactionContextException {
    // HOPS datanodeMgr is null in some tests
    if (datanodeMgr == null) {
      return new TreeMap<>();
    }

    Map<DatanodeDescriptor, Reason> dnds = new TreeMap<>();
    if (corruptReplicas != null) {
      for (CorruptReplica cr : corruptReplicas) {
        DatanodeDescriptor dn = datanodeMgr.getDatanodeBySid(cr.getStorageId());
        if (dn != null) {
          dnds.put(dn, Reason.valueOf(cr.getReason()));
        }
      }
    }
    return dnds;
  }
  
  /**
   * Check if replica belonging to Datanode is corrupt
   *
   * @param blk
   *     Block to check
   * @param node
   *     DatanodeDescriptor which holds the replica
   * @return true if replica is corrupt, false if does not exists in this map
   */
  boolean isReplicaCorrupt(BlockInfo blk, DatanodeDescriptor node)
      throws StorageException, TransactionContextException {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  int numCorruptReplicas(BlockInfo blk)
      throws StorageException, TransactionContextException {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  
  int size() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_CORRUPT_REPLICAS) {
      @Override
      public Object performTask() throws IOException {
        CorruptReplicaDataAccess da =
            (CorruptReplicaDataAccess) HdfsStorageFactory
                .getDataAccess(CorruptReplicaDataAccess.class);
        return da.countAllUniqueBlk();
      }
    }.handle();
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks
   *     Number of block ids to return.
   *     0 <= numExpectedBlocks <= 100
   * @param startingBlockId
   *     Block id from which to start. If null, start at
   *     beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   */
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks, Long startingBlockId)
      throws IOException {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    List<Long> sortedIds = new ArrayList<>();
    
    Collection<CorruptReplica> corruptReplicas = getAllCorruptReplicas();
    if (corruptReplicas != null) {
      for (CorruptReplica replica : corruptReplicas) {
        sortedIds.add(replica.getBlockId());
      }
    }
    
    Iterator<Long> blockIt = sortedIds.iterator();
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Long bid = blockIt.next();
        if (bid == startingBlockId) {
          isBlockFound = true;
          break;
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<>();

    // append up to numExpectedBlocks blockIds to our list
    for (int i = 0; i < numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }
  
  private Collection<CorruptReplica> getCorruptReplicas(BlockInfo blk)
      throws StorageException, TransactionContextException {
    return EntityManager
        .findList(CorruptReplica.Finder.ByBlockIdAndINodeId, blk.getBlockId(),
            blk.getInodeId());
  }

  private Collection<CorruptReplica> getAllCorruptReplicas()
      throws IOException {
    return (Collection<CorruptReplica>) new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_CORRUPT_REPLICAS) {
      @Override
      public Object performTask() throws IOException {
        CorruptReplicaDataAccess crDa =
            (CorruptReplicaDataAccess) HdfsStorageFactory
                .getDataAccess(CorruptReplicaDataAccess.class);
        return crDa.findAll();
      }
    }.handle();
  }
  
  
  private void addCorruptReplicaToDB(CorruptReplica cr)
      throws StorageException, TransactionContextException {
    EntityManager.add(cr);
  }

  private void removeCorruptReplicaFromDB(CorruptReplica cr)
      throws StorageException, TransactionContextException {
    EntityManager.remove(cr);
  }
}
