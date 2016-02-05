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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

/**
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  static Iterable<StorageType> toStorageTypes(
      final Iterable<DatanodeStorageInfo> infos) {
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

  private final DatanodeDescriptor dn;
  private final String storageID;
  private final StorageType storageType;
  private State state;
  private long capacity;
  private long dfsUsed;
  private long remaining;
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

  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
  }

  boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  // TODO add a call to this method in the DatanodeDescriptor
  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  // TODO add a call to this method in the BlockManager
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
  public void setUtilization(long capacity, long dfsUsed, long remaining) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
  }

  /**
   * Iterates over the list of blocks belonging to the Storage.
   */
  public Iterator<BlockInfo> getBlockIterator() throws IOException {
    return getAllStorageBlockInfos().iterator();
  }

  private List<BlockInfo> getAllStorageBlockInfos() throws IOException {
    final int sid = this.sid;

    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
            .getDataAccess(BlockInfoDataAccess.class);
        HdfsStorageFactory.getConnector().beginTransaction();
        List<BlockInfo> list = da.findBlockInfosByStorageId(sid);
        HdfsStorageFactory.getConnector().commit();
        return list;
      }
    };
    return (List<BlockInfo>) findBlocksHandler.handle();
  }
  
  public void setState(State s) {
    this.state = s;
    
    // TODO: if goes to failed state cleanup the block list
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

  public boolean addBlock(BlockInfo b) {
    try {
      return b.addReplica(this) != null;
    } catch (StorageException e) {
      // TODO how should we handle these exceptions?
      e.printStackTrace();
    } catch (TransactionContextException e) {
      // TODO how should we handle these exceptions?
      e.printStackTrace();
    }
    return false;
  }

  public boolean removeBlock(BlockInfo b) {
//    blockList = b.listRemove(blockList, this);
//    return b.removeStorage(this);
    try {
      return b.removeReplica(this) != null;
    } catch (StorageException e) {
      // TODO how should we handle these exceptions?
      e.printStackTrace();
    } catch (TransactionContextException e) {
      // TODO how should we handle these exceptions?
      e.printStackTrace();
    }
    return false;
  }

  public int numBlocks() {
    int numBlocks = -1;

    try {
      numBlocks = (Integer) new LightWeightRequestHandler(
          HDFSOperationType.COUNT_REPLICAS_ON_NODE) {
        @Override
        public Object performTask() throws StorageException, IOException {
          ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
              .getDataAccess(ReplicaDataAccess.class);
          return da.countAllReplicasForStorageId(getSid());
        }
      }.handle();
    } catch (IOException e) {
      // TODO again: what do we do with the exceptions? (/where)
      e.printStackTrace();
    }

    return numBlocks;
  }
  
  public void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    remaining = r.getRemaining();
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
  public String toString() {
    return getDatanodeDescriptor().toString() + "[" + storageType + "]" +
        storageID + ":" + state;
  }

  public Map<Long, Integer> getAllStorageReplicas() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_BLOCKS_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
            .getDataAccess(ReplicaDataAccess.class);
        return da.findBlockAndInodeIdsByStorageId(getSid());
      }
    };
    return (Map<Long, Integer>) findBlocksHandler.handle();
  }

  public Map<Long,Long> getAllStorageInvalidatedReplicasWithGenStamp() throws IOException {
    LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(
        HDFSOperationType.GET_ALL_STORAGE_BLOCKS_IDS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InvalidateBlockDataAccess da =
            (InvalidateBlockDataAccess) HdfsStorageFactory
                .getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockByStorageIdUsingMySQLServer(getSid());
      }
    };
    return (Map<Long, Long>) findBlocksHandler.handle();
  }
}