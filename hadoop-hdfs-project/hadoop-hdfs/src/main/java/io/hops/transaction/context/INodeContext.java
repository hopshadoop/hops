/*
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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.hops.exception.LockUpgradeException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.lock.BaseINodeLock;
import io.hops.transaction.lock.Lock;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class INodeContext extends BaseEntityContext<Integer, INode> {

  protected final static Log LOG = LogFactory.getLog(INodeContext .class);

  private final INodeDataAccess<INode> dataAccess;

  private final Map<String, INode> inodesNameParentIndex =
      new HashMap<>();
  private final Map<Integer, List<INode>> inodesParentIndex =
      new HashMap<>();
  private final List<INode> renamedInodes = new ArrayList<>();

  public INodeContext(INodeDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodesNameParentIndex.clear();
    inodesParentIndex.clear();
    renamedInodes.clear();
  }

  @Override
  public INode find(FinderType<INode> finder, Object... params)
      throws TransactionContextException, StorageException {
    INode.Finder iFinder = (INode.Finder) finder;
    switch (iFinder) {
      case ByINodeIdFTIS:
        return findByInodeIdFTIS(iFinder, params);
      case ByNameParentIdAndPartitionId:
        return findByNameParentIdAndPartitionIdPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<INode> findList(FinderType<INode> finder, Object... params)
      throws TransactionContextException, StorageException {
    INode.Finder iFinder = (INode.Finder) finder;
    switch (iFinder) {
      case ByParentIdFTIS:
        return findByParentIdFTIS(iFinder, params);
      case ByParentIdAndPartitionId:
        return findByParentIdAndPartitionIdPPIS(iFinder,params);
      case ByNamesParentIdsAndPartitionIds:
        return findBatch(iFinder, params);
      case ByNamesParentIdsAndPartitionIdsCheckLocal:
        return findBatchWithLocalCacheCheck(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void remove(INode iNode) throws TransactionContextException {
    super.remove(iNode);
    inodesNameParentIndex.remove(iNode.nameParentKey());
    if (isLogDebugEnabled()) {
      log("removed-inode", "id", iNode.getId(), "name", iNode.getLocalName(), "parent_id", iNode.getParentId(),
              "partition_id", iNode.getPartitionId());
    }
  }

  @Override
  public void update(INode iNode) throws TransactionContextException {
    super.update(iNode);
    inodesNameParentIndex.put(iNode.nameParentKey(), iNode);
    if(isLogDebugEnabled()) {
      log("updated-inode", "id", iNode.getId(), "name", iNode.getLocalName(), "parent_id", iNode.getParentId(),
              "partition_id", iNode.getPartitionId());
    }
  }

  @Override
  public void prepare(TransactionLocks lks)
      throws TransactionContextException, StorageException {

    // if the list is not empty then check for the lock types
    // lock type is checked after when list length is checked
    // because some times in the tx handler the acquire lock
    // function is empty and in that case tlm will throw
    // null pointer exceptions
    Collection<INode> removed = getRemoved();
    Collection<INode> added = new ArrayList<>(getAdded());
    added.addAll(renamedInodes);
    Collection<INode> modified = getModified();

    if (lks.containsLock(Lock.Type.INode)) {
      BaseINodeLock hlk = (BaseINodeLock) lks.getLock(Lock.Type.INode);
      if (!removed.isEmpty()) {
        for (INode inode : removed) {
          TransactionLockTypes.INodeLockType lock =
              hlk.getLockedINodeLockType(inode);
          if (lock != null &&
              lock != TransactionLockTypes.INodeLockType.WRITE && lock !=
              TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
            throw new LockUpgradeException(
                "Trying to remove inode id=" + inode.getId() +
                    " acquired lock was " + lock);
          }
        }
      }

      if (!modified.isEmpty()) {
        for (INode inode : modified) {
          TransactionLockTypes.INodeLockType lock =
              hlk.getLockedINodeLockType(inode);
          if (lock != null &&
              lock != TransactionLockTypes.INodeLockType.WRITE && lock !=
              TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
            throw new LockUpgradeException(
                "Trying to update inode id=" + inode.getId() +
                    " acquired lock was " + lock);
          }
        }
      }
    }


    dataAccess.prepare(removed, added, modified);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds =
        (HdfsTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        super.remove(inodeBeforeChange);
        try {
          inodeAfterChange.setPartitionIdNoPersistance(INode.calculatePartitionId(inodeAfterChange.getParentId(),inodeAfterChange
              .getLocalName(), inodeAfterChange.myDepth()));
        } catch (StorageException e) {
          throw new TransactionContextException(e);
        }
        renamedInodes.add(inodeAfterChange);
        if (isLogDebugEnabled()) {
          log("removed-inode-snapshot-maintenance", "id", inodeBeforeChange.getId(), "name",
                  inodeBeforeChange.getLocalName(), "parent_id", inodeBeforeChange.getParentId(), "partition_id", inodeBeforeChange
                          .getPartitionId());
          log("added-inode-snapshot-maintenance", "id",
                  inodeAfterChange.getId(), "name", inodeAfterChange.getLocalName(),
                  "parent_id", inodeAfterChange.getParentId(), "partition_id", inodeAfterChange.getPartitionId());
        }
        break;
      case Concat:
        // do nothing
        // why? files y and z are merged into file x.
        // all the blocks will be added to file x and the inodes y and z will be deleted.
        // Inode deletion is handled by the concat function
        break;
    }
  }

  @Override
  Integer getKey(INode iNode) {
    return iNode.getId();
  }


  private INode findByInodeIdFTIS(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    INode result = null;
    final Integer inodeId = (Integer) params[0];
    if (contains(inodeId)) {
      result = get(inodeId);
      if(result!=null) {
        hit(inodeFinder, result, "id", inodeId, "name", result.getLocalName(), "parent_id", result.getParentId(),
            "partition_id", result.getPartitionId());
      }else{
        hit(inodeFinder, result, "id", inodeId);
      }
    } else {
      aboutToAccessStorage(inodeFinder, params);
      result = dataAccess.findInodeByIdFTIS(inodeId);
      gotFromDB(inodeId, result);
      if (result != null) {
        inodesNameParentIndex.put(result.nameParentKey(), result);
        miss(inodeFinder, result, "id", inodeId, "name", result.getLocalName(), "parent_id", result.getParentId(),
          "partition_id", result.getPartitionId());
      }else {
        miss(inodeFinder, result, "id");
      }
    }
    return result;
  }

  private INode findByNameParentIdAndPartitionIdPK(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {

    INode result = null;
    final String name = (String) params[0];
    final Integer parentId = (Integer) params[1];
    final Integer partitionId = (Integer) params[2];
    Integer possibleInodeId = null;
    if (params.length == 4) {
      possibleInodeId = (Integer) params[3];
    }
    final String nameParentKey = INode.nameParentKey(parentId, name);

    if (inodesNameParentIndex.containsKey(nameParentKey)) {
      result = inodesNameParentIndex.get(nameParentKey);
      if (!preventStorageCalls() &&
          (currentLockMode.get() == LockMode.WRITE_LOCK)) {
        //trying to upgrade lock. re-read the row from DB
        aboutToAccessStorage(inodeFinder, params);
        result = dataAccess.findInodeByNameParentIdAndPartitionIdPK(name, parentId, partitionId);
        gotFromDBWithPossibleInodeId(result, possibleInodeId);
        inodesNameParentIndex.put(nameParentKey, result);
        missUpgrade(inodeFinder, result, "name", name, "parent_id", parentId, "partition_id", partitionId);
      } else {
        hit(inodeFinder, result, "name", name, "parent_id", parentId, "partition_id", partitionId);
      }
    } else {
      if (!isNewlyAdded(parentId) && !containsRemoved(parentId, name)) {
        if (canReadCachedRootINode(name, parentId)) {
          result = RootINodeCache.getRootINode();
          LOG.debug("Reading root inode from the cache. "+result);
       } else {
          aboutToAccessStorage(inodeFinder, params);
          result = dataAccess.findInodeByNameParentIdAndPartitionIdPK(name, parentId, partitionId);
        }
        gotFromDBWithPossibleInodeId(result, possibleInodeId);
        inodesNameParentIndex.put(nameParentKey, result);
        miss(inodeFinder, result, "name", name, "parent_id", parentId, "partition_id", partitionId,
            "possible_inode_id",possibleInodeId);
      }
    }
    return result;
  }

  private List<INode> findByParentIdFTIS(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final Integer parentId = (Integer) params[0];
    List<INode> result = null;
    if (inodesParentIndex.containsKey(parentId)) {
      result = inodesParentIndex.get(parentId);
      hit(inodeFinder, result, "parent_id", parentId );
    } else {
      aboutToAccessStorage(inodeFinder, params);
      result = syncInodeInstances(
          dataAccess.findInodesByParentIdFTIS(parentId));
      inodesParentIndex.put(parentId, result);
      miss(inodeFinder, result, "parent_id", parentId);
    }
    return result;
  }

  private List<INode> findByParentIdAndPartitionIdPPIS(INode.Finder inodeFinder, Object[] params)
          throws TransactionContextException, StorageException {
    final Integer parentId = (Integer) params[0];
    final Integer partitionId = (Integer) params[1];
    List<INode> result = null;
    if (inodesParentIndex.containsKey(parentId)) {
      result = inodesParentIndex.get(parentId);
      hit(inodeFinder, result, "parent_id", parentId, "partition_id",partitionId);
    } else {
      aboutToAccessStorage(inodeFinder, params);
      result = syncInodeInstances(
              dataAccess.findInodesByParentIdAndPartitionIdPPIS(parentId, partitionId));
      inodesParentIndex.put(parentId, result);
      miss(inodeFinder, result, "parent_id", parentId, "partition_id",partitionId);
    }
    return result;
  }

  private List<INode> findBatch(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final String[] names = (String[]) params[0];
    final int[] parentIds = (int[]) params[1];
    final int[] partitionIds = (int[]) params[2];
    return findBatch(inodeFinder, names, parentIds, partitionIds);
  }

  private List<INode> findBatchWithLocalCacheCheck(INode.Finder inodeFinder,
      Object[] params)
      throws TransactionContextException, StorageException {
    final String[] names = (String[]) params[0];
    final int[] parentIds = (int[]) params[1];
    final int[] partitionIds = (int[]) params[2];

    List<String> namesRest = Lists.newArrayList();
    List<Integer> parentIdsRest = Lists.newArrayList();
    List<Integer> partitionIdsRest = Lists.newArrayList();
    List<Integer> unpopulatedIndeces = Lists.newArrayList();

    List<INode> result = new ArrayList<>(Collections.<INode>nCopies(names
        .length, null));

    for(int i=0; i<names.length; i++){
      final String nameParentKey = INode.nameParentKey(parentIds[i], names[i]);
      INode node = inodesNameParentIndex.get(nameParentKey);
      if(node != null){
        result.set(i, node);
        hit(inodeFinder, node, "name", names[i], "parent_id", parentIds[i], "partition_id", partitionIds[i]);
      }else{
        namesRest.add(names[i]);
        parentIdsRest.add(parentIds[i]);
        partitionIdsRest.add(partitionIds[i]);
        unpopulatedIndeces.add(i);
      }
    }

    if(unpopulatedIndeces.isEmpty()){
      return result;
    }

    if(unpopulatedIndeces.size() == names.length){
      return findBatch(inodeFinder, names, parentIds, partitionIds);
    }else{
      List<INode> batch = findBatch(inodeFinder,
              namesRest.toArray(new String[namesRest.size()]),
              Ints.toArray(parentIdsRest),
              Ints.toArray(partitionIdsRest));
      Iterator<INode> batchIterator = batch.listIterator();
      for(Integer i : unpopulatedIndeces){
        if(batchIterator.hasNext()){
          result.set(i, batchIterator.next());
        }
      }
      return result;
    }
  }

  private List<INode> findBatch(INode.Finder inodeFinder, String[] names,
                                int[] parentIds, int[] partitionIds) throws StorageException {
    INode rootINode = null;
    boolean addCachedRootInode = false;
    if (canReadCachedRootINode(names[0], parentIds[0])) {
      rootINode = RootINodeCache.getRootINode();
      if (rootINode != null) {
        if(names[0] == INodeDirectory.ROOT_NAME && parentIds[0] == INodeDirectory.ROOT_PARENT_ID){
          LOG.debug("Reading root inode from the cache "+rootINode);
          //remove root from the batch operation. Cached root inode will be added later to the results
          names = Arrays.copyOfRange(names, 1, names.length);
          parentIds = Arrays.copyOfRange(parentIds, 1, parentIds.length);
          partitionIds = Arrays.copyOfRange(partitionIds, 1, partitionIds.length);
          addCachedRootInode=true;
        }
      }
    }

    List<INode> batch = dataAccess.getINodesPkBatched(names, parentIds, partitionIds);
    miss(inodeFinder, batch, "names", Arrays.toString(names), "parent_ids",
            Arrays.toString(parentIds), "partition_ids", Arrays.toString(partitionIds));
    if (rootINode != null && addCachedRootInode) {
      batch.add(0, rootINode);
    }
    return syncInodeInstances(batch);
  }

  private List<INode> syncInodeInstances(List<INode> newInodes) {
    List<INode> finalList = new ArrayList<>(newInodes.size());
    
    for (INode inode : newInodes) {
      if (isRemoved(inode.getId())) {
        continue;
      }

      gotFromDB(inode);
      finalList.add(inode);

      String key = inode.nameParentKey();
      if (inodesNameParentIndex.containsKey(key)) {
        if (inodesNameParentIndex.get(key) == null) {
          inodesNameParentIndex.put(key, inode);
        }
      } else {
        inodesNameParentIndex.put(key, inode);
      }
    }
    Collections.sort(finalList, INode.Order.ByName);
    return finalList;
  }

  private boolean containsRemoved(final Integer parentId, final String name) {
    return contains(new Predicate<ContextEntity>() {
      @Override
      public boolean apply(ContextEntity input) {
        INode iNode = input.getEntity();
        return input.getState() == State.REMOVED &&
            iNode.getParentId() == parentId &&
            iNode.getLocalName().equals(name);
      }
    });
  }

  private void gotFromDBWithPossibleInodeId(INode result,
      Integer possibleInodeId) {
    if (result == null && possibleInodeId != null) {
      gotFromDB(possibleInodeId, result);
    } else {
      gotFromDB(result);
    }
  }

  private boolean canReadCachedRootINode(String name, int parentId) {
    if (name.equals(INodeDirectory.ROOT_NAME) && parentId == INodeDirectory.ROOT_PARENT_ID) {
      if (RootINodeCache.isRootInCache() && currentLockMode.get() == LockMode.READ_COMMITTED) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }
}
