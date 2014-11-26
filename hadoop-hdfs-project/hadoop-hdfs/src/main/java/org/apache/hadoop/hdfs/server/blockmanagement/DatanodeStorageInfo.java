/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.Slicer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

/**
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final Log LOG = LogFactory.getLog(DatanodeStorageInfo.class);
  
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  private static int BLOCKITERATOR_BATCH_SIZE = 10000;
  
  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  static Iterable<StorageType> toStorageTypes(final Iterable<DatanodeStorageInfo> infos) {
    return new Iterable<StorageType>() {
      @Override
      public Iterator<StorageType> iterator() {
        return new Iterator<StorageType>() {
          final Iterator<DatanodeStorageInfo> i = infos.iterator();
          @Override
          public boolean hasNext() {return i.hasNext();}
          @Override
          public StorageType next() {return i.next().getStorageType();}
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  public static DatanodeInfo[] toDatanodeInfos(DatanodeStorageInfo[] storages) {
    return toDatanodeInfos(Arrays.asList(storages));
  }

  public static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo>
      storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  private final DatanodeDescriptor dn;
  private final String storageID;
  private StorageType storageType;
  private State state;
  private long capacity;
  private long dfsUsed;
  private volatile long remaining;
  private long blockPoolUsed;
  private int sid = -1;

  /** The number of block reports received */
  private int blockReportCount = 0;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  private boolean blockContentsStale = true;

  public DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
  }

  int getBlockReportCount() {
    return blockReportCount;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
  }

  /**
   * Set the capacity of this storage
   * @param capacity
   * @param dfsUsed
   * @param remaining
   */
  @VisibleForTesting
  public void setUtilization(long capacity, long dfsUsed, long remaining) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
  }

  /**
   * Iterates over the list of blocks belonging to the Storage.
   */
  public Iterator<BlockInfo> getBlockIterator() throws IOException {
    return new BlockIterator();
  }
      
  private class BlockIterator implements Iterator<BlockInfo> {    
    private Iterator<BlockInfo> blocks = Collections.EMPTY_LIST.iterator();
    long index = 0;
    
    public BlockIterator(){
      update();
    }
    
    @Override
    public boolean hasNext() {
      if(!blocks.hasNext()){
        update();
      }
      return blocks.hasNext();
    }

    @Override
    public BlockInfo next() {
      if(!blocks.hasNext()){
        update();
      }
      return blocks.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove unsupported.");
    }

    private void update(){
      try{
      while(!blocks.hasNext() && hasBlocksWithIdGreaterThan(index)){
        blocks = getStorageBlockInfos(index, BLOCKITERATOR_BATCH_SIZE).iterator();
        index = index+BLOCKITERATOR_BATCH_SIZE;
      }
      }catch(IOException ex){
        LOG.warn(ex,ex);
      }
    }
    
  }

  private List<BlockInfo> getStorageBlockInfos(final long from, final int size) throws IOException {
    final int sid = this.sid;

    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        boolean transactionActive = connector.isTransactionActive();
        try {
          if (!transactionActive) {
            connector.beginTransaction();
          }

          BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
              .getDataAccess(BlockInfoDataAccess.class);
          return da.findBlockInfosByStorageId(sid, from, size);
        } finally {
          if (!transactionActive) {
            connector.commit();
          }
        }
      }
    };
    return (List<BlockInfo>) findBlocksHandler.handle();
  }
  
  private boolean hasBlocksWithIdGreaterThan(final long from) throws IOException {
    final int sid = this.sid;

    LightWeightRequestHandler hasBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        boolean transactionActive = connector.isTransactionActive();
        try {
          if (!transactionActive) {
            connector.beginTransaction();
          }

          ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
              .getDataAccess(ReplicaDataAccess.class);
          return da.hasBlocksWithIdGreaterThan(sid, from);
        } finally {
          if (!transactionActive) {
            connector.commit();
          }
        }
      }
    };
    return (boolean) hasBlocksHandler.handle();
  }
    
  public void setState(State s) {
    this.state = s;
  }
  
  boolean areBlocksOnFailedStorage() throws IOException {
    return getState() == State.FAILED && numBlocks() != 0;
  }
  
  public State getState() {
    return this.state;
  }
  
  public String getStorageID() {
    return storageID;
  }

  public void setSid(int sid) {
    this.sid = sid;
  }

  public int getSid() {
    return this.sid;
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  public AddBlockResult addBlock(BlockInfo b)
      throws TransactionContextException, StorageException {
    // First check whether the block belongs to a different storage
    // on the same DN.
    AddBlockResult result = AddBlockResult.ADDED;
    Integer otherStorage = b.getReplicatedOnDatanode(this.getDatanodeDescriptor());
    if(otherStorage!=null){
      if (otherStorage != this.sid) {
        // The block belongs to a different storage. Remove it first.
        b.removeReplica(otherStorage);
        result = AddBlockResult.REPLACED;
      } else {
        // The block is already associated with this storage.
        return AddBlockResult.ALREADY_EXIST;
      }
    }
    b.addStorage(this);
    return result;
  }

  public boolean removeBlock(BlockInfo b)
      throws TransactionContextException, StorageException {
    return b.removeReplica(this) != null;
  }

  public int numBlocks() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.COUNT_REPLICAS_ON_NODE) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaDataAccess.class);
        return da.countAllReplicasForStorageId(getSid());
      }
    }.handle();
  }
  
  public void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /** Increment the number of blocks scheduled for each given storage */
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }
  
  @Override
  public int hashCode() {
    return storageID.hashCode();
  }
  
  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }

  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed);
  }

  public Map<Long, Long> getAllStorageReplicas(int numBuckets, int nbThreads, int bucketPerThread,
      ExecutorService executor) throws IOException {
    return DatanodeStorageInfo.getAllStorageReplicas(numBuckets, sid, nbThreads, bucketPerThread, executor);
  }
  
  public static Map<Long, Long> getAllStorageReplicas(int numBuckets, final int sid, int nbThreads, int bucketPerThread,
      ExecutorService executor) throws IOException {
    final Map<Long, Long> result = new ConcurrentHashMap<>();

    try {
      Slicer.slice(numBuckets, bucketPerThread, nbThreads, executor,
          new Slicer.OperationHandler() {

        @Override
        public void handle(final int startIndex, final int endIndex) throws Exception {
          LOG.info("get blocks in buckets " + startIndex + " to " + endIndex + " for storage " + sid);
          final List<Integer> buckets = new ArrayList<>();
          for (int i = startIndex; i < endIndex; i++) {
            buckets.add(i);
          }
          LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
              HDFSOperationType.GET_ALL_STORAGE_BLOCKS_IDS) {
            @Override
            public Object performTask() throws StorageException, IOException {
              ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
                  .getDataAccess(ReplicaDataAccess.class);
              return da.findBlockAndInodeIdsByStorageIdAndBucketIds(sid, buckets);
            }
          };
          result.putAll((Map<Long, Long>) findBlocksHandler.handle());
        }
      });
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
    return result;
  }

  public Map<Long, Long> getAllStorageReplicasInBuckets(
          final List<Integer> mismatchedBuckets) throws IOException {
    LightWeightRequestHandler findReplicasHandler = new
        LightWeightRequestHandler
            (HDFSOperationType.GET_ALL_STORAGE_BLOCKS_IN_BUCKETS) {
      @Override
      public Object performTask() throws IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaDataAccess.class);
        return da.findBlockAndInodeIdsByStorageIdAndBucketIds(sid,
            mismatchedBuckets);
      }
    };
    return (Map<Long,Long>) findReplicasHandler.handle();
  }

  public Map<Long,Long> getAllStorageInvalidatedReplicasWithGenStamp() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_BLOCKS_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockBySidUsingMySQLServer(getSid());
      }
    };
    return (Map<Long, Long>) findBlocksHandler.handle();
  }

  @VisibleForTesting
  public void setUtilizationForTesting(long capacity, long dfsUsed,
      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  /** @return the first {@link DatanodeStorageInfo} corresponding to
   *          the given datanode
   */
  static DatanodeStorageInfo getDatanodeStorageInfo(
      final Iterable<DatanodeStorageInfo> infos,
      final DatanodeDescriptor datanode) {
    if (datanode == null) {
      return null;
    }
    for(DatanodeStorageInfo storage : infos) {
      if (storage.getDatanodeDescriptor() == datanode) {
        return storage;
      }
    }
    return null;
  }

  static enum AddBlockResult {
    ADDED, REPLACED, ALREADY_EXIST;
  }
}
