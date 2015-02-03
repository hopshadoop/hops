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

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.entity.UnderReplicatedBlock;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Keep prioritized queues of under replicated blocks.
 * Blocks have replication priority, with priority {@link
 * #QUEUE_HIGHEST_PRIORITY}
 * indicating the highest priority.
 * </p>
 * Having a prioritised queue allows the {@link BlockManager} to select
 * which blocks to replicate first -it tries to give priority to data
 * that is most at risk or considered most valuable.
 * <p/>
 * <p/>
 * The policy for choosing which priority to give added blocks
 * is implemented in {@link #getPriority(Block, int, int, int)}.
 * </p>
 * <p>The queue order is as follows:</p>
 * <ol>
 * <li>{@link #QUEUE_HIGHEST_PRIORITY}: the blocks that must be replicated
 * first. That is blocks with only one copy, or blocks with zero live
 * copies but a copy in a node being decommissioned. These blocks
 * are at risk of loss if the disk or server on which they
 * remain fails.</li>
 * <li>{@link #QUEUE_VERY_UNDER_REPLICATED}: blocks that are very
 * under-replicated compared to their expected values. Currently
 * that means the ratio of the ratio of actual:expected means that
 * there is <i>less than</i> 1:3.</li>. These blocks may not be at risk,
 * but they are clearly considered "important".
 * <li>{@link #QUEUE_UNDER_REPLICATED}: blocks that are also under
 * replicated, and the ratio of actual:expected is good enough that
 * they do not need to go into the {@link #QUEUE_VERY_UNDER_REPLICATED}
 * queue.</li>
 * <li>{@link #QUEUE_REPLICAS_BADLY_DISTRIBUTED}: there are as least as
 * many copies of a block as required, but the blocks are not adequately
 * distributed. Loss of a rack/switch could take all copies off-line.</li>
 * <li>{@link #QUEUE_WITH_CORRUPT_BLOCKS} This is for blocks that are corrupt
 * and for which there are no-non-corrupt copies (currently) available.
 * The policy here is to keep those corrupt blocks replicated, but give
 * blocks that are not corrupt higher priority.</li>
 * </ol>
 */
class UnderReplicatedBlocks implements Iterable<Block> {
  /**
   * The total number of queues : {@value}
   */
  static final int LEVEL = 5;
  /**
   * The queue with the highest priority: {@value}
   */
  static final int QUEUE_HIGHEST_PRIORITY = 0;
  /**
   * The queue for blocks that are way below their expected value : {@value}
   */
  static final int QUEUE_VERY_UNDER_REPLICATED = 1;
  /**
   * The queue for "normally" under-replicated blocks: {@value}
   */
  static final int QUEUE_UNDER_REPLICATED = 2;
  /**
   * The queue for blocks that have the right number of replicas,
   * but which the block manager felt were badly distributed: {@value}
   */
  static final int QUEUE_REPLICAS_BADLY_DISTRIBUTED = 3;
  /**
   * The queue for corrupt blocks: {@value}
   */
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;

  /** Create an object. */
  UnderReplicatedBlocks() {
  }

  /**
   * Empty the queues.
   */
  void clear() throws IOException {
    new LightWeightRequestHandler(
        HDFSOperationType.DEL_ALL_UNDER_REPLICATED_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  /**
   * Return the total number of under replication blocks
   */
  int size() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_ALL_UNDER_REPLICATED_BLKS) {

      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * Return the number of under replication blocks excluding corrupt blocks
   */
  int getUnderReplicatedBlockCount() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_UNDER_REPLICATED_BLKS_LESS_THAN_LVL4) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        return da.countLessThanALevel(QUEUE_WITH_CORRUPT_BLOCKS);
      }
    }.handle();
  }

  /**
   * Return the number of corrupt blocks
   */
  int getCorruptBlockSize() throws IOException {
    return count(QUEUE_WITH_CORRUPT_BLOCKS);
  }

  /** Return the number of corrupt blocks with replication factor 1 */
  synchronized int getCorruptReplOneBlockSize() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_CORRUPT_REPL_ONE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        return da.countReplOneBlocks(QUEUE_WITH_CORRUPT_BLOCKS);
      }
    }.handle();
  }
  
  /**
   * Check if a block is in the neededReplication queue
   */
  boolean contains(BlockInfo block)
      throws StorageException, TransactionContextException {
    return getUnderReplicatedBlock(block) != null;
  }

  /**
   * Return the priority of a block
   *
   * @param block
   *     a under replicated block
   * @param curReplicas
   *     current number of replicas of the block
   * @param expectedReplicas
   *     expected number of replicas of the block
   * @return the priority for the blocks, between 0 and ({@link #LEVEL}-1)
   */
  private int getPriority(Block block, int curReplicas,
      int decommissionedReplicas, int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    if (curReplicas >= expectedReplicas) {
      // Block has enough copies, but not enough racks
      return QUEUE_REPLICAS_BADLY_DISTRIBUTED;
    } else if (curReplicas == 0) {
      // If there are zero non-decommissioned replicas but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      //all we have are corrupt blocks
      return QUEUE_WITH_CORRUPT_BLOCKS;
    } else if (curReplicas == 1) {
      //only on replica -risk of loss
      // highest priority
      return QUEUE_HIGHEST_PRIORITY;
    } else if ((curReplicas * 3) < expectedReplicas) {
      //there is less than a third as many blocks as requested;
      //this is considered very under-replicated
      return QUEUE_VERY_UNDER_REPLICATED;
    } else {
      //add to the normal queue for under replicated blocks
      return QUEUE_UNDER_REPLICATED;
    }
  }

  /**
   * add a block to a under replication queue according to its priority
   *
   * @param block
   *     a under replication block
   * @param curReplicas
   *     current number of replicas of the block
   * @param decomissionedReplicas
   *     the number of decommissioned replicas
   * @param expectedReplicas
   *     expected number of replicas of the block
   * @return true if the block was added to a queue.
   */
  boolean add(BlockInfo block, int curReplicas, int decomissionedReplicas,
      int expectedReplicas)
      throws StorageException, TransactionContextException {
    assert curReplicas >= 0 : "Negative replicas!";
    int priLevel = getPriority(block, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    if(add(block, priLevel, expectedReplicas)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.add: {}"
              + " has only {} replicas and need {} replicas so is added to" +
              " neededReplications at priority level {}", block, curReplicas,
          expectedReplicas, priLevel);
      return true;
    }
    return false;
  }

  /** remove a block from a under replication queue */
  synchronized boolean remove(BlockInfo block, 
                              int oldReplicas, 
                              int decommissionedReplicas,
                              int oldExpectedReplicas) throws IOException {
    int priLevel = getPriority(block, oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    boolean removedBlock = remove(block, priLevel);
    return removedBlock;
  }

  /**
   * Remove a block from the under replication queues.
   * <p/>
   * The priLevel parameter is a hint of which queue to query
   * first: if negative or &gt;= {@link #LEVEL} this shortcutting
   * is not attmpted.
   * <p/>
   * If the block is not found in the nominated queue, an attempt is made to
   * remove it from all queues.
   * <p/>
   * <i>Warning:</i> This is not a synchronized method.
   *
   * @param block
   *     block to remove
   * @param priLevel
   *     expected privilege level
   * @return true if the block was found and removed from one of the priority
   * queues
   */
  boolean remove(BlockInfo block, int priLevel)
      throws StorageException, TransactionContextException {
    UnderReplicatedBlock urb = getUnderReplicatedBlock(block);
    if (priLevel >= 0 && priLevel < LEVEL && remove(urb)) {
      NameNode.blockStateChangeLog.debug(
        "BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block {}" +
            " from priority queue {}", block, urb.getLevel());
      return true;
    }
    return false;
  }

  /**
   * Recalculate and potentially update the priority level of a block.
   * <p/>
   * If the block priority has changed from before an attempt is made to
   * remove it from the block queue. Regardless of whether or not the block
   * is in the block queue of (recalculate) priority, an attempt is made
   * to add it to that queue. This ensures that the block will be
   * in its expected priority queue (and only that queue) by the end of the
   * method call.
   *
   * @param block
   *     a under replicated block
   * @param curReplicas
   *     current number of replicas of the block
   * @param decommissionedReplicas
   *     the number of decommissioned replicas
   * @param curExpectedReplicas
   *     expected number of replicas of the block
   * @param curReplicasDelta
   *     the change in the replicate count from before
   * @param expectedReplicasDelta
   *     the change in the expected replica count from before
   */
  void update(BlockInfo block, int curReplicas, int decommissionedReplicas,
      int curExpectedReplicas, int curReplicasDelta, int expectedReplicasDelta)
      throws StorageException, TransactionContextException {
    int oldReplicas = curReplicas - curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, decommissionedReplicas,
        curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, decommissionedReplicas,
        oldExpectedReplicas);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " +
          block +
          " curReplicas " + curReplicas +
          " curExpectedReplicas " + curExpectedReplicas +
          " oldReplicas " + oldReplicas +
          " oldExpectedReplicas  " + oldExpectedReplicas +
          " curPri  " + curPri +
          " oldPri  " + oldPri);
    }
    if(oldPri != curPri) {
      remove(block, oldPri);
    }
    if(add(block, curPri, curExpectedReplicas, true)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.update: {} has only {} " +
              "replicas and needs {} replicas so is added to " +
              "neededReplications at priority level {}", block, curReplicas,
          curExpectedReplicas, curPri);
    }
  }
  
  /**
   * Get a list of block lists to be replicated. The index of block lists
   * represents its replication priority. Replication index will be tracked for
   * each priority list separately in priorityToReplIdx map. Iterates through
   * all priority lists and find the elements after replication index. Once the
   * last priority lists reaches to end, all replication indexes will be set to
   * 0 and start from 1st priority list to fulfill the blockToProces count.
   *
   * @param blocksToProcess
   *     - number of blocks to fetch from underReplicated blocks.
   * @return Return a list of block lists to be replicated. The block list index
   * represents its replication priority.
   */
  private List<List<Block>> chooseUnderReplicatedBlocksInt(int blocksToProcess)
      throws IOException {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate = new ArrayList<>(LEVEL);
    for (int i = 0; i < LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
    }
    if (size() == 0) { // There are no blocks to collect.
      return blocksToReplicate;
    }
    
    List<Integer> priorityToReplIdx = getReplicationIndex();
    List<List<Block>> priorityQueuestmp = createPrioriryQueue();
    
    int blockCount = 0;
    blocksToProcess = Math.min(blocksToProcess, size());
    
    for (int priority = 0; priority < LEVEL; priority++) {
      // Go through all blocks that need replications with current priority.
      Integer replIndex = priorityToReplIdx.get(priority);
      
      if (blockCount == blocksToProcess) {
        break;  // break if already expected blocks are obtained
      }
      
      int remainingblksToProcess = blocksToProcess - blockCount;
      List<UnderReplicatedBlock> urbs =
          getUnderReplicatedBlocks(priority, replIndex, remainingblksToProcess);
      addBlocksInPriorityQueues(urbs, priorityQueuestmp);
      
      List<Block> blks = priorityQueuestmp.get(priority);
      blocksToReplicate.get(priority).addAll(blks);
      blockCount += blks.size();
      replIndex += blks.size();
      
      if (priority == LEVEL - 1 && count(priority) <= replIndex) {
        // reset all priorities replication index to 0 because there is no
        // recently added blocks in any list.
        for (int i = 0; i < LEVEL; i++) {
          priorityToReplIdx.set(i, 0);
        }
        break;
      }
      priorityToReplIdx.set(priority, replIndex);
    }
    setReplicationIndex(priorityToReplIdx);
    return blocksToReplicate;
  }

  /**
   * returns an iterator of all blocks in a given priority queue
   */
  BlockIterator iterator(final int level) {
    try {
      return (BlockIterator) new HopsTransactionalRequestHandler(
          HDFSOperationType.UNDER_REPLICATED_BLKS_ITERATOR) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
        }

        @Override
        public Object performTask() throws StorageException, IOException {
          return new BlockIterator(fillPriorityQueues(level), level);
        }
      }.handle();
    } catch (IOException ex) {
      BlockManager.LOG
          .error("Error while filling the priorityQueues from db", ex);
      return null;
    }
  }

  /**
   * return an iterator of all the under replication blocks
   */
  @Override
  public BlockIterator iterator() {
    try {
      return (BlockIterator) new HopsTransactionalRequestHandler(
          HDFSOperationType.UNDER_REPLICATED_BLKS_ITERATOR) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
        }
        
        @Override
        public Object performTask() throws StorageException, IOException {
          return new BlockIterator(fillPriorityQueues());
        }
      }.handle();
    } catch (IOException ex) {
      BlockManager.LOG
          .error("Error while filling the priorityQueues from db", ex);
      return null;
    }
  }

  /**
   * An iterator over blocks.
   */
  class BlockIterator implements Iterator<Block> {
    private int level;
    private boolean isIteratorForLevel = false;
    private final List<Iterator<Block>> iterators =
        new ArrayList<>();
    
    /**
     * Construct an iterator over all queues.
     */
    private BlockIterator(List<List<Block>> priorityQueuestmp) {
      level = 0;
      synchronized (iterators) {
        for (int i = 0; i < LEVEL; i++) {
          iterators.add(priorityQueuestmp.get(i).iterator());
        }
      }
    }

    /**
     * Constrict an iterator for a single queue level
     *
     * @param l
     *     the priority level to iterate over
     */
    private BlockIterator(List<List<Block>> priorityQueuestmp, int l) {
      level = l;
      isIteratorForLevel = true;
      synchronized (iterators) {
        iterators.add(priorityQueuestmp.get(level).iterator());
      }
    }

    private void update() {
      if (isIteratorForLevel) {
        return;
      }
      synchronized (iterators) {
        while (level < LEVEL - 1 && !iterators.get(level).hasNext()) {
          level++;
        }
      }
    }

    @Override
    public Block next() {
      if (isIteratorForLevel) {
        synchronized (iterators) {
          return iterators.get(0).next();
        }
      }
      update();
      synchronized (iterators) {
        return iterators.get(level).next();
      }
    }

    @Override
    public boolean hasNext() {
      if (isIteratorForLevel) {
        synchronized (iterators) {
          return iterators.get(0).hasNext();
        }
      }
      update();
      synchronized (iterators) {
        return iterators.get(level).hasNext();
      }
    }

    @Override
    public void remove() {
      if (isIteratorForLevel) {
        synchronized (iterators) {
          iterators.get(0).remove();
        }
      } else {
        synchronized (iterators) {
          iterators.get(level).remove();
        }
      }
    }

    int getPriority() {
      return level;
    }
  }

  /**
   * This method is to decrement the replication index for the given priority
   *
   * @param priority
   *     - int priority level
   */
  public void decrementReplicationIndex(int priority)
      throws StorageException, TransactionContextException {
    List<Integer> priorityToReplIdx = getReplicationIndex();
    Integer replIdx = priorityToReplIdx.get(priority);
    replIdx = replIdx <= 0 ? 0 : (replIdx - 1);
    priorityToReplIdx.set(priority, replIdx);
    setReplicationIndex(priorityToReplIdx);
  }

  public List<List<Block>> chooseUnderReplicatedBlocks(
      final int blocksToProcess) throws IOException {
    return (List<List<Block>>) new HopsTransactionalRequestHandler(
        HDFSOperationType.CHOOSE_UNDER_REPLICATED_BLKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.ReplicationIndex,
            TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return chooseUnderReplicatedBlocksInt(blocksToProcess);
      }
    }.handle();
  }
  
  private boolean remove(UnderReplicatedBlock urb)
      throws StorageException, TransactionContextException {
    if (urb != null) {
      removeUnderReplicatedBlock(urb);
      return true;
    }
    return false;
  }

  // return true if it does not exist other wise return false
  private boolean add(BlockInfo block, int priLevel, int expectedReplicas, boolean update)
      throws StorageException, TransactionContextException {
    UnderReplicatedBlock urb = getUnderReplicatedBlock(block);
    if (urb == null) {
      addUnderReplicatedBlock(
          new UnderReplicatedBlock(priLevel, block.getBlockId(),
              block.getInodeId(), expectedReplicas));
      return true;
    }
    if(update){
      addUnderReplicatedBlock(
          new UnderReplicatedBlock(priLevel, block.getBlockId(),
              block.getInodeId(), expectedReplicas));
    }
    return false;
  }
  
  private boolean add(BlockInfo block, int priLevel, int expectedReplicas)
      throws StorageException, TransactionContextException {
   return add(block, priLevel, expectedReplicas, false);
  }
  
  private List<List<Block>> fillPriorityQueues() throws IOException {
    return fillPriorityQueues(-1);
  }
  
  private List<List<Block>> fillPriorityQueues(int level) throws IOException {
    List<List<Block>> priorityQueuestmp = createPrioriryQueue();
    List<UnderReplicatedBlock> allUrb = getUnderReplicatedBlocks(level);
    if (!allUrb.isEmpty()) {
      addBlocksInPriorityQueues(allUrb, priorityQueuestmp);
    }
    return priorityQueuestmp;
  }
  
  private List<List<Block>> createPrioriryQueue() {
    List<List<Block>> priorityQueuestmp = new ArrayList<>();
    for (int i = 0; i < LEVEL; i++) {
      priorityQueuestmp.add(new ArrayList<Block>());
    }
    return priorityQueuestmp;
  }
  

  private List<UnderReplicatedBlock> getUnderReplicatedBlocks(final int level)
      throws IOException {
    return (List<UnderReplicatedBlock>) new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_UNDER_REPLICATED_BLKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        if (level == -1) {
          return da.findAll();
        } else {
          return da.findByLevel(level);
        }
        
      }
    }.handle();
  }
  
  private List<UnderReplicatedBlock> getUnderReplicatedBlocks(final int level,
      final int offset, final int count) throws IOException {
    return (List<UnderReplicatedBlock>) new LightWeightRequestHandler(
        HDFSOperationType.GET_UNDER_REPLICATED_BLKS_By_LEVEL_LIMITED) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        return da.findByLevel(level, offset, count);

      }
    }.handle();
  }

  private void addBlocksInPriorityQueues(
      final List<UnderReplicatedBlock> allUrb,
      final List<List<Block>> priorityQueuestmp) throws IOException {
    final long[] blockIds = new long[allUrb.size()];
    final long[] inodeIds = new long[allUrb.size()];
    final HashMap<Long, UnderReplicatedBlock> allUrbHashMap =
        new HashMap<>();
    for (int i = 0; i < allUrb.size(); i++) {
      UnderReplicatedBlock b = allUrb.get(i);
      blockIds[i] = b.getBlockId();
      inodeIds[i] = b.getInodeId();
      allUrbHashMap.put(b.getBlockId(), b);
    }

    // use lightweight transaction handler here and it should work

    new LightWeightRequestHandler(HDFSOperationType.GET_BLOCKS) {
      
      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfoDataAccess bda = (BlockInfoDataAccess) HdfsStorageFactory
            .getDataAccess(BlockInfoDataAccess.class);
        List<BlockInfo> blks = bda.findByIds(blockIds, inodeIds);
        for (BlockInfo blk : blks) {
          UnderReplicatedBlock urb = allUrbHashMap.remove(blk.getBlockId());
          assert urb.getInodeId() == blk.getInodeId();
          priorityQueuestmp.get(urb.getLevel()).add(blk);
        }
        
        //HOP[M]: allUrb should contains the list of underreplicatedblocks that doesn't have any block attached to 
        // so it's safe to delete these blocks without taking anylocks
        Collection<UnderReplicatedBlock> toRemove = allUrbHashMap.values();
        if (!toRemove.isEmpty()) {
          UnderReplicatedBlockDataAccess uda =
              (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                  .getDataAccess(UnderReplicatedBlockDataAccess.class);
          uda.prepare(toRemove, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        }
        return null;
      }
    }.handle();
  }
  
  int count(final int level) throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_UNDER_REPLICATED_BLKS_AT_LVL) {
      @Override
      public Object performTask() throws StorageException, IOException {
        UnderReplicatedBlockDataAccess da =
            (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                .getDataAccess(UnderReplicatedBlockDataAccess.class);
        return da.countByLevel(level);
      }
    }.handle();
  }
  
  private UnderReplicatedBlock getUnderReplicatedBlock(BlockInfo blk)
      throws StorageException, TransactionContextException {
    return EntityManager
        .find(UnderReplicatedBlock.Finder.ByBlockIdAndINodeId, blk.getBlockId(),
            blk.getInodeId());
  }

  private void addUnderReplicatedBlock(UnderReplicatedBlock urb)
      throws StorageException, TransactionContextException {
    EntityManager.add(urb);
  }

  private void removeUnderReplicatedBlock(UnderReplicatedBlock urb)
      throws StorageException, TransactionContextException {
    EntityManager.remove(urb);
  }

  private List<Integer> getReplicationIndex()
      throws StorageException, TransactionContextException {
    return HdfsVariables.getReplicationIndex();
  }

  private void setReplicationIndex(List<Integer> replicationIndex)
      throws StorageException, TransactionContextException {
    HdfsVariables.setReplicationIndex(replicationIndex);
  }
  
}
