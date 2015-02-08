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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

/**
 * ************************************************
 * PendingReplicationBlocks does the bookkeeping of all
 * blocks that are getting replicated.
 * <p/>
 * It does the following:
 * 1)  record blocks that are getting replicated at this instant.
 * 2)  a coarse grain timer to track age of replication request
 * 3)  a thread that periodically identifies replication-requests
 * that never made it.
 * <p/>
 * *************************************************
 */
class PendingReplicationBlocks {
  private static final Logger LOG = BlockManager.LOG;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private static long timeout = 5 * 60 * 1000;

  PendingReplicationBlocks(long timeoutPeriod) {
    if (timeoutPeriod > 0) {
      this.timeout = timeoutPeriod;
    }
  }

  void start() {
    
  }

  /**
   * Add a block to the list of pending Replications
   * @param block The corresponding block
   * @param targets The DataNodes where replicas of the block should be placed
   */
  void increment(BlockInfoContiguous block, DatanodeDescriptor[] targets)
      throws StorageException, TransactionContextException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found == null) {
      addPendingBlockInfo(
          new PendingBlockInfo(block.getBlockId(), block.getInodeId(), now(),
              targets));
    } else {
      found.incrementReplicas(targets);
      found.setTimeStamp(now());
      updatePendingBlockInfo(found);
    }
  }

  /**
   * One replication request for this block has finished.
   * Decrement the number of pending replication requests
   * for this block.
   */
  void decrement(BlockInfoContiguous block, DatanodeDescriptor dn)
      throws StorageException, TransactionContextException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null && !isTimedout(found)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing pending replication for " + block);
      }
      if (found.decrementReplicas(dn)) {
        removePendingBlockInfo(found, dn);
      } 
    }
  }

  /**
   * Remove the record about the given block from pendingReplications.
   *
   * @param block
   *     The given block whose pending replication requests need to be
   *     removed
   */
  void remove(BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null) {
      removePendingBlockInfo(found);
    }
  }

  public void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_PENDING_REPL_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        PendingBlockDataAccess da = (PendingBlockDataAccess) HdfsStorageFactory
            .getDataAccess(PendingBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  /**
   * The total number of blocks that are undergoing replication
   */
  int size() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_ALL_VALID_PENDING_REPL_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        PendingBlockDataAccess da = (PendingBlockDataAccess) HdfsStorageFactory
            .getDataAccess(PendingBlockDataAccess.class);
        return da.countValidPendingBlocks(getTimeLimit());
      }
    }.handle();
  }

  /**
   * How many copies of this block is pending replication?
   */
  int getNumReplicas(BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null && !isTimedout(found)) {
      return found.getNumReplicas();
    }
    return 0;
  }

  /**
   * Returns a list of blocks that have timed out their
   * replication requests. Returns null if no blocks have
   * timed out.
   */
  long[] getTimedOutBlocks() throws IOException {
    List<PendingBlockInfo> timedOutItems =
        (List<PendingBlockInfo>) new LightWeightRequestHandler(
            HDFSOperationType.GET_TIMED_OUT_PENDING_BLKS) {
          @Override
          public Object performTask() throws StorageException, IOException {
            long timeLimit = getTimeLimit();
            PendingBlockDataAccess da =
                (PendingBlockDataAccess) HdfsStorageFactory
                    .getDataAccess(PendingBlockDataAccess.class);
            List<PendingBlockInfo> timedoutPendings =
                (List<PendingBlockInfo>) da.findByTimeLimitLessThan(timeLimit);
            if (timedoutPendings == null || timedoutPendings.size() <= 0) {
              return null;
            }
            return timedoutPendings;
          }
        }.handle();
    if (timedOutItems == null) {
      return null;
    }
    Collection<PendingBlockInfo> filterd =
        Collections2.filter(timedOutItems, new Predicate<PendingBlockInfo>() {

          @Override
          public boolean apply(PendingBlockInfo t) {
            return t != null;
          }
        });

    long[] blockIdArr = new long[filterd.size()];
    int i = 0;
    for (PendingBlockInfo p : filterd) {
      blockIdArr[i] = p.getBlockId();
      i++;
    }

    return blockIdArr;
  }

  /*
   * Shuts down the pending replication monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
  }

  private boolean isTimedout(PendingBlockInfo pendingBlock) {
    if (getTimeLimit() > pendingBlock.getTimeStamp()) {
      return true;
    }
    return false;
  }

  private static long getTimeLimit() {
    return now() - timeout;
  }

  private PendingBlockInfo getPendingBlock(BlockInfoContiguous block)
      throws StorageException, TransactionContextException {
    return EntityManager
        .find(PendingBlockInfo.Finder.ByBlockIdAndINodeId, block.getBlockId(),
            block.getInodeId());
  }

  private List<PendingBlockInfo> getAllPendingBlocks()
      throws StorageException, TransactionContextException {
    return (List<PendingBlockInfo>) EntityManager
        .findList(PendingBlockInfo.Finder.All);
  }

  private BlockInfoContiguous getBlockInfo(PendingBlockInfo pendingBlock)
      throws StorageException, TransactionContextException {
    return EntityManager
        .find(BlockInfoContiguous.Finder.ByBlockIdAndINodeId, pendingBlock.getBlockId());
  }

  private void addPendingBlockInfo(PendingBlockInfo pbi)
      throws StorageException, TransactionContextException {
    EntityManager.add(pbi);
  }

  private void updatePendingBlockInfo(PendingBlockInfo pbi)
      throws StorageException, TransactionContextException {
    EntityManager.update(pbi);
  }

  private void removePendingBlockInfo(PendingBlockInfo pbi, DatanodeDescriptor dn)
      throws StorageException, TransactionContextException {
    PendingBlockInfo toRemove = new PendingBlockInfo(pbi.getBlockId(), pbi.getInodeId(), pbi.getTimeStamp(), dn.
        getDatanodeUuid());
    EntityManager.remove(toRemove);
  }
  
  private void removePendingBlockInfo(PendingBlockInfo pbi)
      throws StorageException, TransactionContextException {
    EntityManager.remove(pbi);
  }
}
