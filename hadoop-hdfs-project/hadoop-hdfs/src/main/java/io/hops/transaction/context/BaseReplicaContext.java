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
package io.hops.transaction.context;

import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


abstract class BaseReplicaContext<Key extends BlockPK, Entity>
    extends BaseEntityContext<Key, Entity> {

  private Map<Long, Map<Key, Entity>> blocksToReplicas =
      new HashMap<>();

  private Map<Long, Map<Key, Entity>> inodesToReplicas =
      new HashMap<>();

  @Override
  public void update(Entity entity) throws TransactionContextException {
    super.update(entity);
    addInternal(entity);
  }

  @Override
  public void remove(Entity entity) throws TransactionContextException {
    super.remove(entity);
    BlockPK key = getKey(entity);
    Map<Key, Entity> entityMap = blocksToReplicas.get(key.getBlockId());
    if (entityMap != null) {
      entityMap.remove(key);
    }

    entityMap = inodesToReplicas.get(key.getInodeId());
    if (entityMap != null) {
      entityMap.remove(key);
    }
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    blocksToReplicas.clear();
    inodesToReplicas.clear();
  }

  @Override
  final void gotFromDB(Key entityKey, Entity entity) {
    super.gotFromDB(entityKey, entity);
    addInternal(entityKey, entity);
  }

  private void addInternal(Entity entity) {
    addInternal(getKey(entity), entity);
  }

  protected void addInternal(Key key, Entity entity) {
    Map<Key, Entity> entityMap;
    if (key.hasBlockId()) {
      entityMap = blocksToReplicas.get(key.getBlockId());
      if (entityMap == null) {
        entityMap = new HashMap<>();
        blocksToReplicas.put(key.getBlockId(), entityMap);
      }
      entityMap.put(key, entity);
    }
    if (key.hasINodeId()) {
      entityMap = inodesToReplicas.get(key.getInodeId());
      if (entityMap == null) {
        entityMap = new HashMap<>();
        inodesToReplicas.put(key.getInodeId(), entityMap);
      }
      entityMap.put(key, entity);
    }
  }

  final void gotFromDB(BlockPK key, List<Entity> entities) {
    if (key.hasBlockId()) {
      Map<Key, Entity> entityMap = blocksToReplicas.get(key.getBlockId());
      if (entityMap == null) {
        blocksToReplicas.put(key.getBlockId(), null);
      }
    }
    if (key.hasINodeId()) {
      Map<Key, Entity> entityMap = inodesToReplicas.get(key.getInodeId());
      if (entityMap == null) {
        inodesToReplicas.put(key.getInodeId(), null);
      }
    }
    if (entities != null) {
      for (Entity entity : entities) {
        gotFromDB(entity);
      }
    }
  }

  final void gotFromDB(List<Key> keys, List<Entity> entities) {
    if (entities != null) {
      for (Entity entity : entities) {
        Key key = getKey(entity);
        gotFromDB(key, entity);
        keys.remove(key);
      }
    }
    for (Key key : keys) {
      gotFromDB(key, (Entity) null);
    }
  }

  final boolean containsByBlock(long blockId) {
    return blocksToReplicas.containsKey(blockId);
  }

  final boolean containsByINode(long inodeId) {
    return inodesToReplicas.containsKey(inodeId);
  }


  final List<Entity> getByBlock(long blockId) {
    Map<Key, Entity> entityMap = blocksToReplicas.get(blockId);
    if (entityMap == null) {
      return null;
    }
    return new ArrayList<>(entityMap.values());
  }

  final List<Entity> getByINode(long inodeId) {
    Map<Key, Entity> entityMap = inodesToReplicas.get(inodeId);
    if (entityMap == null) {
      return null;
    }
    return new ArrayList<>(entityMap.values());
  }


  @Override
  public final void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds =
        (HdfsTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        // need to update the rows with updated inodeId or partKey
        checkForSnapshotChange();
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        checkForSnapshotChange();
        INodeCandidatePrimaryKey trg_param =
            (INodeCandidatePrimaryKey) params[0];
        List<INodeCandidatePrimaryKey> srcs_param =
            (List<INodeCandidatePrimaryKey>) params[1];
        List<BlockInfo> oldBlks = (List<BlockInfo>) params[2];
        updateReplicas(trg_param, srcs_param);
        break;

      case BlockDoesNotExist:
        blockDoesNotExist((Long) params[0], (Long) params[1]);
        break;

      case EmptyFile:
        emptyFile((Long) params[0]);
        break;
    }
  }

  private void checkForSnapshotChange() {
    if (snapshotChanged())
    // during
    // the
    // tx no
    // replica
    // should have been changed
    {// renaming to existing file will put replicas in the deleted list
      throw new IllegalStateException(
          "No replica should have been changed during the Tx ( " +
              this.getClass() + ")");
    }
  }

  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty() ||
        !getRemoved().isEmpty();
  }

  private void updateReplicas(INodeCandidatePrimaryKey trg_param,
      List<INodeCandidatePrimaryKey> toBeDeletedSrcs)
      throws TransactionContextException {

    toBeDeletedSrcs.remove(trg_param);
    for (INodeCandidatePrimaryKey src : toBeDeletedSrcs) {
      List<Entity> replicas = getByINode(src.getInodeId());
      if (replicas == null) {
        continue;
      }
      for (Entity replica : replicas) {
        Entity toBeDeleted = cloneEntity(replica);
        Entity toBeAdded = cloneEntity(replica, trg_param.getInodeId());
        Key toBeDeletedKey = getKey(toBeDeleted);
        Key toBeAddedKey = getKey(toBeAdded);

        remove(toBeDeleted);
        if(isLogDebugEnabled()) {
          log("snapshot-maintenance-removed-replica", "bid",
                  toBeDeletedKey.getBlockId(), "inodeId",
                  toBeDeletedKey.getInodeId());
        }
        add(toBeAdded);
        if(isLogDebugEnabled()) {
          log("snapshot-maintenance-added-replica", "bid",
                  toBeAddedKey.getBlockId(), "inodeId", toBeAddedKey.getInodeId());
        }
      }
    }
  }

  private void blockDoesNotExist(long blockId, long inodeId){
    emptyFile(inodeId);
    if (!containsByBlock(blockId)) {
      blocksToReplicas.put(blockId, null);
    }
  }

  private void emptyFile(long inodeId){
    if(!containsByINode(inodeId)){
      inodesToReplicas.put(inodeId, null);
    }
  }

  abstract Entity cloneEntity(Entity entity);

  abstract Entity cloneEntity(Entity entity, long inodeId);
}
