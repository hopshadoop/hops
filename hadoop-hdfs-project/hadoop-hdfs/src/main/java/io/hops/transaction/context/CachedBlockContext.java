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
package io.hops.transaction.context;

import com.google.common.collect.HashBiMap;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.CachedBlockDataAccess;
import io.hops.metadata.hdfs.entity.CachedBlock;
import io.hops.transaction.lock.TransactionLocks;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

public class CachedBlockContext extends BaseReplicaContext<BlockPK.CachedBlockPK, CachedBlock> {

  private CachedBlockDataAccess<CachedBlock> dataAccess;

  Map<Long, Map<BlockPK.CachedBlockPK, CachedBlock>> blocksToReplicas = new HashMap<>();
  Map<Integer, Map<BlockPK.CachedBlockPK, CachedBlock>> inodesToReplicas = new HashMap<>();
  Map<String, Map<BlockPK.CachedBlockPK, CachedBlock>> datanodeToReplicas = new HashMap<>();
  
  public CachedBlockContext(CachedBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public CachedBlock find(FinderType<CachedBlock> finder, Object... params) throws TransactionContextException,
      StorageException {
    CachedBlock.Finder iFinder = (CachedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdInodeIdAndDatanodeId:
        return findByPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<CachedBlock> findList(FinderType<CachedBlock> finder, Object... params) throws
      TransactionContextException, StorageException {
    CachedBlock.Finder iFinder = (CachedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndInodeId:
        return findByBlockIdAndInodeId(iFinder, params);
      case ByInodeId:
        return findByINodeId(iFinder, params);
      case ByInodeIds:
        return findByINodeIds(iFinder, params);
      case ByBlockIdsAndINodeIds:
        return findBatch(iFinder, params);
      case ByDatanodeId:
        return findByDatanodeId(iFinder, params);
      case All:
        return findAll(iFinder, params);
      case ByDatanodeAndTypes:
        return findByDatanodeAndTypes(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  BlockPK.CachedBlockPK getKey(CachedBlock cachedBlock) {
    return new BlockPK.CachedBlockPK(cachedBlock.getBlockId(), cachedBlock.getInodeId(), cachedBlock.getDatanodeId());
  }

  private CachedBlock findByPK(CachedBlock.Finder iFinder, Object[] params) throws StorageCallPreventedException,
      StorageException {
    final BlockPK.CachedBlockPK pk = (BlockPK.CachedBlockPK) params[0];

    CachedBlock result = null;

    if (contains(pk)) {
      result = get(pk);
      hit(iFinder, result, "id", pk);
    } else {
      aboutToAccessStorage(iFinder, params);
      result = dataAccess.find(pk.getBlockId(), pk.getInodeId(), pk.getDatanodeId());
      gotFromDB(pk, result);
      miss(iFinder, result, "Id", pk);
    }

    return result;
  }

  private List<CachedBlock> findByBlockIdAndInodeId(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final long inodeId = (long) params[1];
    List<CachedBlock> results = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      results = getByBlock(blockId);
      hit(iFinder, results, "bid", blockId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findCachedBlockById(blockId);
      gotFromDB(new BlockPK.CachedBlockPK(blockId, inodeId), results);
      miss(iFinder, results, "bid", blockId);
    }
    return results;
  }

  private List<CachedBlock> findByINodeId(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];
    List<CachedBlock> results = null;
    if (containsByINode(inodeId)) {
      results = getByINode(inodeId);
      hit(iFinder, results, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findCachedBlockByINodeId(inodeId);
      gotFromDB(new BlockPK.CachedBlockPK(inodeId), results);
      miss(iFinder, results, "inodeid", inodeId);
    }
    return results;
  }

  private List<CachedBlock> findByINodeIds(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    long[] ids = (long[]) params[0];
    aboutToAccessStorage(iFinder, params);
    List<CachedBlock> results = dataAccess.findCachedBlockByINodeIds(ids);
    gotFromDB(BlockPK.CachedBlockPK.getKeys(ids), results);
    miss(iFinder, results, "inodeIds", Arrays.toString(ids));
    return results;
  }

  private List<CachedBlock> findBatch(CachedBlock.Finder bFinder, Object[] params) throws TransactionContextException,
      StorageException {
    List<CachedBlock> result = null;
    final long[] blockIds = (long[]) params[0];
    final long[] inodeIds = (long[]) params[1];
    final DatanodeID datanodeId = (DatanodeID) params[2];
    aboutToAccessStorage(bFinder, params);
    result = dataAccess.findByIds(blockIds, inodeIds, datanodeId.getDatanodeUuid());
    miss(bFinder, result, "BlockIds", Arrays.toString(blockIds), "InodeIds",
        Arrays.toString(inodeIds), "datanodeId", datanodeId);
    for (CachedBlock block : result) {
      gotFromDB(block);
    }
    for (int i = 0; i < blockIds.length; i++) {
      BlockPK.CachedBlockPK pk = new BlockPK.CachedBlockPK(blockIds[i], inodeIds[i], datanodeId.getDatanodeUuid());
      if (!contains(pk)) {
        gotFromDB(pk, (CachedBlock) null);
      }
    }
    return result;
  }

  private List<CachedBlock> findByDatanodeId(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final DatanodeID datanodeId = (DatanodeID) params[0];
    List<CachedBlock> results = null;
    if (containsByDatanode(datanodeId.getDatanodeUuid())) {
      results = getByDatanode(datanodeId.getDatanodeUuid());
      hit(iFinder, results, "datanodeId", datanodeId);
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findCachedBlockByDatanodeId(datanodeId.getDatanodeUuid());
      gotFromDB(new BlockPK.CachedBlockPK(datanodeId.getDatanodeUuid()), results);
      miss(iFinder, results, "datanodeId", datanodeId);
    }
    return results;
  }
  
  boolean hasAll=false;
  
  private Collection<CachedBlock> findAll(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    Collection<CachedBlock> results = null;
    if (hasAll) {
      results = getAll();
      hit(iFinder, results, "all");
    } else {
      aboutToAccessStorage(iFinder, params);
      results = dataAccess.findAll();
      hasAll=true;
      gotFromDB(results);
      miss(iFinder, results, "all");
    }
    return results;
  }
  
  private List<CachedBlock> findByDatanodeAndTypes(CachedBlock.Finder iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final String datanodeId = (String) params[0];
    final org.apache.hadoop.hdfs.server.namenode.CachedBlock.Type type = (org.apache.hadoop.hdfs.server.namenode.CachedBlock.Type) params[1];
    List<CachedBlock> results = null;
    if (preventStorageCalls()) {
      results = getByDatanodeAndType(datanodeId, type.toString());
      hit(iFinder, results, "datanodeId", datanodeId, "type", type.toString());
    } else {
      throw new StorageCallPreventedException("[" + iFinder + "] Trying " +
          "to access storage while it should allways be called after a more general lock");
    }
    return results;
  }
  
  @Override
  public void remove(CachedBlock entity) throws TransactionContextException {
    super.remove(entity);
    BlockPK.CachedBlockPK key = getKey(entity);
    Map<BlockPK.CachedBlockPK, CachedBlock> entityMap = datanodeToReplicas.get(key.getDatanodeId());
    if (entityMap != null) {
      entityMap.remove(key);
    }
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    datanodeToReplicas.clear();
  }

  @Override
  protected void addInternal(BlockPK.CachedBlockPK key, CachedBlock entity) {
    super.addInternal(key, entity);
    Map<BlockPK.CachedBlockPK, CachedBlock> entityMap;
    if (key.hasDatanodeId()) {
      entityMap = datanodeToReplicas.get(key.getDatanodeId());
      if (entityMap == null) {
        entityMap = new HashMap<>();
        datanodeToReplicas.put(key.getDatanodeId(), entityMap);
      }
      entityMap.put(key, entity);
    }
  }
  
  final void gotFromDB(BlockPK.CachedBlockPK key, List<CachedBlock> entities) {
    super.gotFromDB(key, entities);
    if (key.hasDatanodeId()) {
      Map<BlockPK.CachedBlockPK, CachedBlock> entityMap = datanodeToReplicas.get(key.getDatanodeId());
      if (entityMap == null) {
        datanodeToReplicas.put(key.getDatanodeId(), null);
      }
    }
  }
  
  final boolean containsByDatanode(String datanodeId) {
    return datanodeToReplicas.containsKey(datanodeId);
  }

  final List<CachedBlock> getByDatanode(String datanodeId) {
    Map<BlockPK.CachedBlockPK, CachedBlock> entityMap = datanodeToReplicas.get(datanodeId);
    if (entityMap == null) {
      return null;
    }
    return new ArrayList<>(entityMap.values());
  }

  final List<CachedBlock> getByDatanodeAndType(String datanodeId, String type) {
    Map<BlockPK.CachedBlockPK, CachedBlock> entityMap = datanodeToReplicas.get(datanodeId);
    if (entityMap == null) {
      return null;
    }
    List<CachedBlock> result = new ArrayList<>();
    for(CachedBlock block: entityMap.values()){
      if(block.getStatus().equals(type)){
        result.add(block);
      }
    }
    return result;
  }
  
  @Override
  CachedBlock cloneEntity(CachedBlock cachedBlock) {
    return cloneEntity(cachedBlock, cachedBlock.getInodeId());
  }

  @Override
  CachedBlock cloneEntity(CachedBlock cachedBlock, long inodeId) {
    return new CachedBlock(cachedBlock.getBlockId(), inodeId, cachedBlock.getDatanodeId(), cachedBlock.getStatus(),
        cachedBlock.getReplicationAndMark());
  }
}
