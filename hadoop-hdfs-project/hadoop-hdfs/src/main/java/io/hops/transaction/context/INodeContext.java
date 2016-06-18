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
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class INodeContext extends BaseEntityContext<Integer, INode> {

  private final INodeDataAccess<INode> dataAccess;

  private final Map<String, INode> inodesNameParentIndex =
      new HashMap<String, INode>();
  private final Map<Integer, List<INode>> inodesParentIndex =
      new HashMap<Integer, List<INode>>();
  private final List<INode> renamedInodes = new ArrayList<INode>();

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
      case ByINodeId:
        return findByInodeId(iFinder, params);
      case ByNameAndParentId:
        return findByNameAndParentId(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<INode> findList(FinderType<INode> finder, Object... params)
      throws TransactionContextException, StorageException {
    INode.Finder iFinder = (INode.Finder) finder;
    switch (iFinder) {
      case ByParentId:
        return findByParentId(iFinder, params);
      case ByNamesAndParentIds:
        return findBatch(iFinder, params);
      case ByNamesAndParentIdsCheckLocal:
        return findBatchWithLocalCacheCheck(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void remove(INode iNode) throws TransactionContextException {
    super.remove(iNode);
    inodesNameParentIndex.remove(iNode.nameParentKey());
    log("removed-inode", "id", iNode.getId(), "name", iNode.getLocalName());
  }

  @Override
  public void update(INode iNode) throws TransactionContextException {
    super.update(iNode);
    inodesNameParentIndex.put(iNode.nameParentKey(), iNode);
    log("updated-inode", "id", iNode.getId(), "name", iNode.getLocalName());
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
    Collection<INode> added = new ArrayList<INode>(getAdded());
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
        renamedInodes.add(inodeAfterChange);
        log("snapshot-maintenance-inode-pk-change", "Before inodeId",
            inodeBeforeChange.getId(), "name", inodeBeforeChange.getLocalName(),
            "pid", inodeBeforeChange.getParentId(), "After inodeId",
            inodeAfterChange.getId(), "name", inodeAfterChange.getLocalName(),
            "pid", inodeAfterChange.getParentId());
        log("snapshot-maintenance-removed-inode", "name",
            inodeBeforeChange.getLocalName(), "inodeId",
            inodeBeforeChange.getId(), "pid", inodeBeforeChange.getParentId());
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


  private INode findByInodeId(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    INode result = null;
    final Integer inodeId = (Integer) params[0];
    if (contains(inodeId)) {
      result = get(inodeId);
      hit(inodeFinder, result, "id", inodeId);
    } else {
      aboutToAccessStorage(inodeFinder, params);
      result = dataAccess.indexScanfindInodeById(inodeId);
      gotFromDB(inodeId, result);
      if (result != null) {
        inodesNameParentIndex.put(result.nameParentKey(), result);
      }
      miss(inodeFinder, result, "id", inodeId);
    }
    return result;
  }

  private INode findByNameAndParentId(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {

    INode result = null;
    final String name = (String) params[0];
    final Integer parentId = (Integer) params[1];
    Integer possibleInodeId = null;
    if (params.length == 3) {
      possibleInodeId = (Integer) params[2];
    }
    final String nameParentKey = INode.nameParentKey(parentId, name);

    if (inodesNameParentIndex.containsKey(nameParentKey)) {
      result = inodesNameParentIndex.get(nameParentKey);
      if (!preventStorageCalls() &&
          (currentLockMode.get() == LockMode.WRITE_LOCK)) {
        //trying to upgrade lock. re-read the row from DB
        aboutToAccessStorage(inodeFinder, params);
        result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
        gotFromDBWithPossibleInodeId(result, possibleInodeId);
        inodesNameParentIndex.put(nameParentKey, result);
        missUpgrade(inodeFinder, result, "name", name, "pid", parentId);
      } else {
        hit(inodeFinder, result, "name", name, "pid", parentId);
      }

    } else {
      if (!isNewlyAdded(parentId) && !containsRemoved(parentId, name)) {
        aboutToAccessStorage(inodeFinder, params);
        result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
        gotFromDBWithPossibleInodeId(result, possibleInodeId);
        inodesNameParentIndex.put(nameParentKey, result);
        miss(inodeFinder, result, "name", name, "pid", parentId);
      }
    }
    return result;
  }

  private List<INode> findByParentId(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final Integer parentId = (Integer) params[0];
    List<INode> result = null;
    if (inodesParentIndex.containsKey(parentId)) {
      result = inodesParentIndex.get(parentId);
      hit(inodeFinder, result, "pid", parentId);
    } else {
      aboutToAccessStorage(inodeFinder, params);
      result = syncInodeInstances(
          dataAccess.indexScanFindInodesByParentId(parentId));
      inodesParentIndex.put(parentId, result);
      miss(inodeFinder, result, "pid", parentId);
    }
    return result;
  }

  private List<INode> findBatch(INode.Finder inodeFinder, Object[] params)
      throws TransactionContextException, StorageException {
    final String[] names = (String[]) params[0];
    final int[] parentIds = (int[]) params[1];
    return findBatch(inodeFinder, names, parentIds);
  }

  private List<INode> findBatchWithLocalCacheCheck(INode.Finder inodeFinder,
      Object[] params)
      throws TransactionContextException, StorageException {
    final String[] names = (String[]) params[0];
    final int[] parentIds = (int[]) params[1];

    List<String> namesRest = Lists.newArrayList();
    List<Integer> parentIdsRest = Lists.newArrayList();
    List<Integer> unpopulatedIndeces = Lists.newArrayList();

    List<INode> result = new ArrayList<INode>(Collections.<INode>nCopies(names
        .length, null));

    for(int i=0; i<names.length; i++){
      final String nameParentKey = INode.nameParentKey(parentIds[i], names[i]);
      INode node = inodesNameParentIndex.get(nameParentKey);
      if(node != null){
        result.set(i, node);
        hit(inodeFinder, node, "name", names[i], "pid", parentIds[i]);
      }else{
        namesRest.add(names[i]);
        parentIdsRest.add(parentIds[i]);
        unpopulatedIndeces.add(i);
      }
    }

    if(unpopulatedIndeces.isEmpty()){
      return result;
    }

    if(unpopulatedIndeces.size() == names.length){
      return findBatch(inodeFinder, names, parentIds);
    }else{
      List<INode> batch = findBatch(inodeFinder, namesRest.toArray(new
          String[namesRest.size()]), Ints.toArray(parentIdsRest));
      Iterator<INode> batchIterator = batch.listIterator();
      for(Integer i : unpopulatedIndeces){
        result.set(i, batchIterator.next());
      }
      return result;
    }
  }

  private List<INode> findBatch(INode.Finder inodeFinder, String[] names,
      int[] parentIds) throws StorageException {
    List<INode> batch = dataAccess.getINodesPkBatched(names, parentIds);
    miss(inodeFinder, batch, "name", Arrays.toString(names), "pid",
        Arrays.toString(parentIds));
    return syncInodeInstances(batch);
  }

  private List<INode> syncInodeInstances(List<INode> newInodes) {
    List<INode> finalList = new ArrayList<INode>(newInodes.size());

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
    //Collections.sort(finalList, INode.Order.ByName);
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
}
