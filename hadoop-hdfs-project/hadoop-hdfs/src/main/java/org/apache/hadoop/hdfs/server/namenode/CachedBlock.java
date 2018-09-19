/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;

/**
 * Represents a cached block.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class CachedBlock {
  
  public enum Type {
      INIT,  //not attributed to any datanode
      PENDING_CACHED, //waiting to be cached on the datanode
      CACHED, //cached on the datanode
      PENDING_UNCACHED //waiting to be uncached on the datanode
    }
    
  /**
   * Block id.
   */
  private final long blockId;
  private final long inodeId;

  /**
   * Bit 15: Mark
   * Bit 0-14: cache replication factor.
   */
  private short replicationAndMark;

  Map<Type, Set<DatanodeDescriptor>> datanodesMap = new HashMap<>();

  public CachedBlock(long blockId, long inodeId, short replication, boolean mark){
    this.blockId = blockId;
    this.inodeId = inodeId;
    setReplicationAndMark(replication, mark);
  }

  public CachedBlock(long blockId, long inodeId, short replicationAndMark) throws TransactionContextException,
      StorageException {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.replicationAndMark = replicationAndMark;
  }

  public CachedBlock(long blockId, long inodeId, short replicationAndMark, Type type, DatanodeDescriptor datanode) throws TransactionContextException,
      StorageException {
    this.blockId = blockId;
    this.inodeId = inodeId;
    this.replicationAndMark = replicationAndMark;
    if(!type.equals(Type.INIT)){
      Set<DatanodeDescriptor> node = new HashSet<>();
      node.add(datanode);
      datanodesMap.put(type, node);
    }
  }

  public CachedBlock(long blockId, long inodeId, short replication, boolean mark, DatanodeDescriptor datanode, Type type)
      throws TransactionContextException, StorageException {
    this.blockId = blockId;
    this.inodeId = inodeId;
    setReplicationAndMark(replication, mark);
    Set<DatanodeDescriptor> node = new HashSet<>();
    node.add(datanode);
    datanodesMap.put(type, node);
  }

  public long getBlockId() {
    return blockId;
  }

  public long getInodeId() {
    return inodeId;
  }
  
  @Override
  public int hashCode() {
    return (int) (blockId ^ (blockId >>> 32));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CachedBlock other = (CachedBlock) o;
    return other.blockId == blockId;
  }

  public void setReplicationAndMark(short replication, boolean mark){
    assert replication >= 0;
    replicationAndMark = (short) ((replication << 1) | (mark ? 0x1 : 0x0));
  }

  public boolean getMark() {
    return ((replicationAndMark & 0x1) != 0);
  }

  public short getReplication() {
    return (short) (replicationAndMark >>> 1);
  }

  /**
   * Get a list of the datanodes which this block is cached,
   * planned to be cached, or planned to be uncached on.
   *
   * @param type If null, this parameter is ignored.
   * If it is non-null, we match only datanodes which
   * have it on this list.
   * See {@link DatanodeDescriptor#CachedBlocksList#Type}
   * for a description of all the lists.
   *
   * @return The list of datanodes. Modifying this list does not
   * alter the state of the CachedBlock.
   */
  public List<DatanodeDescriptor> getDatanodes(Type type) {
    Set<DatanodeDescriptor> nodes = datanodesMap.get(type);
    if (nodes == null) {
      nodes = new HashSet<DatanodeDescriptor>();
    }
    return new ArrayList<DatanodeDescriptor>(nodes);
  }

  public void addDatanode(DatanodeDescriptor datanode, String type) {
    addDatanode(datanode, Type.valueOf(type));
  }
  
  public void addDatanode(DatanodeDescriptor datanode, Type type) {
    Set<DatanodeDescriptor> nodes = datanodesMap.get(type);
    if (nodes == null) {
      nodes = new HashSet<DatanodeDescriptor>();
      datanodesMap.put(type, nodes);
    }
    nodes.add(datanode);
  }

  @Override
  public String toString() {
    return new StringBuilder().append("{").
        append("blockId=").append(blockId).append(", ").
        append("replication=").append(getReplication()).append(", ").
        append("mark=").append(getMark()).append("}").
        toString();
  }

  public void switchPendingCachedToCached(DatanodeDescriptor datanode) throws TransactionContextException, StorageException {
    if(datanodesMap.get(Type.PENDING_CACHED)!= null && datanodesMap.get(Type.PENDING_CACHED).remove(datanode)){
      addDatanode(datanode, Type.CACHED);
      EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), Type.CACHED.name(), replicationAndMark));
    }
  }
  
  public void switchPendingUncachedToCached(DatanodeDescriptor datanode) throws TransactionContextException, StorageException {
    assert datanodesMap.get(Type.PENDING_UNCACHED).remove(datanode);
    addDatanode(datanode, Type.CACHED);
    EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), Type.CACHED.name(), replicationAndMark));
  }
  
  public boolean setPendingUncached(DatanodeDescriptor datanode) throws TransactionContextException, StorageException{
    Set<DatanodeDescriptor> nodes = datanodesMap.get(Type.PENDING_UNCACHED);
    if (nodes == null) {
      nodes = new HashSet<DatanodeDescriptor>();
      datanodesMap.put(Type.PENDING_UNCACHED, nodes);
    }
    if(!nodes.add(datanode)){
      return false;
    }
    EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), Type.PENDING_UNCACHED.name(), replicationAndMark));
    return true;
  }
  
  public boolean addPendingCached(DatanodeDescriptor datanode) throws TransactionContextException, StorageException{
    Set<DatanodeDescriptor> nodes = datanodesMap.get(Type.PENDING_CACHED);
    if (nodes == null) {
      nodes = new HashSet<DatanodeDescriptor>();
      datanodesMap.put(Type.PENDING_CACHED, nodes);
    }
    if(!nodes.add(datanode)){
      return false;
    }
    EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), Type.PENDING_CACHED.name(), replicationAndMark));
    return true;
  }
  
  public boolean isCached(){
    if(datanodesMap.get(Type.CACHED) != null && !datanodesMap.get(Type.CACHED).isEmpty()){
      return true;
    }
    if(datanodesMap.get(Type.PENDING_UNCACHED) != null && !datanodesMap.get(Type.PENDING_UNCACHED).isEmpty()){
      return true;
    }
    return false;
  }
  
  
  public void save() throws TransactionContextException, StorageException {
    EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
        "", Type.INIT.name(), replicationAndMark));
    for (Type type : datanodesMap.keySet()) {
      for (DatanodeDescriptor datanode : datanodesMap.get(type)) {
        EntityManager.update(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), type.name(), replicationAndMark));
      }
    }
  }
  
  public void remove() throws TransactionContextException, StorageException {
    for (Type type : datanodesMap.keySet()) {
      for (DatanodeDescriptor datanode : datanodesMap.get(type)) {
        EntityManager.remove(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            datanode.getDatanodeUuid(), type.name(), replicationAndMark));
      }
    }
    EntityManager.remove(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
            "", null, replicationAndMark));
  }
  
  public void remove(DatanodeDescriptor datanode) throws TransactionContextException, StorageException {
    EntityManager.remove(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
        datanode.getDatanodeUuid(), null, replicationAndMark));
  }
  
  public void removePending(DatanodeDescriptor datanode) throws TransactionContextException, StorageException {
    EntityManager.remove(new io.hops.metadata.hdfs.entity.CachedBlock(blockId, inodeId,
        datanode.getDatanodeUuid(), Type.PENDING_CACHED.name(), replicationAndMark));
  }
  
  public static Collection<CachedBlock> getAll(DatanodeManager datanodeManager) throws TransactionContextException, StorageException{
    return toHops(EntityManager.findList(
        io.hops.metadata.hdfs.entity.CachedBlock.Finder.All, null), datanodeManager);
  }
  
  static public Collection<CachedBlock> toHops(Collection<io.hops.metadata.hdfs.entity.CachedBlock> dbBlocks,
      DatanodeManager datanodeManager) throws TransactionContextException, StorageException {
    if (dbBlocks == null) {
      return new ArrayList<CachedBlock>();
    }
    Map<Long, CachedBlock> result = new HashMap<>();
    for (io.hops.metadata.hdfs.entity.CachedBlock dalBlock : dbBlocks) {
      if(dalBlock.getStatus().equals("")){
        continue;
      }
      CachedBlock block = result.get(dalBlock.getBlockId());
      if (block == null) {
        DatanodeDescriptor datanode = null;
        if(!CachedBlock.Type.valueOf(dalBlock.getStatus()).equals(CachedBlock.Type.INIT)){
          datanode = datanodeManager.getDatanodeByUuid(dalBlock.getDatanodeId());
        }
        result.put(dalBlock.getBlockId(), new CachedBlock(dalBlock.getBlockId(), dalBlock.getInodeId(), dalBlock.
            getReplicationAndMark(), CachedBlock.Type.valueOf(dalBlock.getStatus()), datanode));
      } else if(!CachedBlock.Type.valueOf(dalBlock.getStatus()).equals(CachedBlock.Type.INIT)) {
        block.addDatanode(datanodeManager.getDatanodeByUuid(dalBlock.getDatanodeId()), dalBlock.
            getStatus());
      }
    }
    return result.values();
  }
}
