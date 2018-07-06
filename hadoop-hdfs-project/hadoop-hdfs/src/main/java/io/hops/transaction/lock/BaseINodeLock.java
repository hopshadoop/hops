/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.lock;

import com.google.common.collect.Iterables;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.resolvingcache.Cache;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

public abstract class BaseINodeLock extends Lock {
  private final Map<INode, TransactionLockTypes.INodeLockType>
      allLockedInodesInTx;
  private final ResolvedINodesMap resolvedINodesMap;
  private boolean isPartitionKeyAlreaySet = false;
  private boolean enableHierarchicalLocking = false;

  protected static boolean setPartitionKeyEnabled = false;

  protected static boolean setRandomParitionKeyEnabled = false;

  protected static Random rand = new Random(System.currentTimeMillis());

  static void enableSetPartitionKey(boolean enable) {
    setPartitionKeyEnabled = enable;
  }

  static void enableSetRandomPartitionKey(boolean enable) {
    setRandomParitionKeyEnabled = enable;
  }

  protected BaseINodeLock() {
    this.allLockedInodesInTx =
        new HashMap<>();
    this.resolvedINodesMap = new ResolvedINodesMap();
  }

  private class ResolvedINodesMap {
    private final Map<String, PathRelatedINodes> pathToPathINodes =
        new HashMap<>();
    private final Collection<INode> individualInodes = new ArrayList<>();

    private class PathRelatedINodes {
      private List<INode> pathINodes;
      private List<INode> childINodes;
    }

    private PathRelatedINodes getWithLazyInit(String path) {
      if (!pathToPathINodes.containsKey(path)) {
        PathRelatedINodes pathRelatedINodes = new PathRelatedINodes();
        pathToPathINodes.put(path, pathRelatedINodes);
        return pathRelatedINodes;
      }
      return pathToPathINodes.get(path);
    }

    private void putPathINodes(String path, List<INode> iNodes) {
      PathRelatedINodes pathRelatedINodes = getWithLazyInit(path);
      pathRelatedINodes.pathINodes = iNodes;
    }

    private void putChildINodes(String path, List<INode> iNodes) {
      PathRelatedINodes pathRelatedINodes = getWithLazyInit(path);
      pathRelatedINodes.childINodes = iNodes;
    }

    private void putIndividualINode(INode iNode) {
      individualInodes.add(iNode);
    }

    private void putIndividualINodes(List<INode> iNodes) {
      for(INode iNode: iNodes){
        individualInodes.add(iNode);
      }
    }

    private List<INode> getPathINodes(String path) {
      PathRelatedINodes pri = pathToPathINodes.get(path);
      return pri.pathINodes;
    }

    private final int countResolvedFilesStoredInDB() {
      return fileCount(true);
    }

    private final int countResolvedFilesStoredOnDataNodes() {
      return fileCount(false);
    }

    private final int fileCount(boolean isStoredInDB) {
      int count = 0;
      for (INode inode : this.getAll()) {
        if (inode instanceof INodeFile) {
          INodeFile file = (INodeFile) inode;
          if (isStoredInDB && file.isFileStoredInDB()) {
            count++;
          } else if (!isStoredInDB && !file.isFileStoredInDB()) {
            count++;
          }
        }
      }
      return count;
    }

    private List<INode> getChildINodes(String path) {
      return pathToPathINodes.get(path).childINodes;
    }

    public Iterable<INode> getAll() {
      Iterable iterable = null;
      for (PathRelatedINodes pathRelatedINodes : pathToPathINodes.values()) {
        List<INode> pathINodes =
            pathRelatedINodes.pathINodes == null ? Collections.EMPTY_LIST :
                pathRelatedINodes.pathINodes;
        List<INode> childINodes =
            pathRelatedINodes.childINodes == null ? Collections.EMPTY_LIST :
                pathRelatedINodes.childINodes;
        if (iterable == null) {
          iterable = Iterables.concat(pathINodes, childINodes);
        } else {
          iterable = Iterables.concat(iterable, pathINodes, childINodes);
        }
      }
      if (iterable == null) {
        iterable = Collections.EMPTY_LIST;
      }
      return Iterables.concat(iterable, individualInodes);
    }
  }

  Iterable<INode> getAllResolvedINodes() {
    return resolvedINodesMap.getAll();
  }

  void addPathINodesAndUpdateResolvingCache(String path, List<INode> iNodes) {
    addPathINodes(path, iNodes);
    updateResolvingCache(path, iNodes);
  }

  void updateResolvingCache(String path, List<INode> iNodes){
    Cache.getInstance().set(path, iNodes);
  }

  void updateResolvingCache(INode inode){
    Cache.getInstance().set(inode);
  }

  void addPathINodes(String path, List<INode> iNodes) {
    resolvedINodesMap.putPathINodes(path, iNodes);
  }

  void addChildINodes(String path, List<INode> iNodes) {
    resolvedINodesMap.putChildINodes(path, iNodes);
  }

  void addIndividualINode(INode iNode) {
    resolvedINodesMap.putIndividualINode(iNode);
  }

  void addIndividualINodes(List<INode> iNodes) {
    resolvedINodesMap.putIndividualINodes(iNodes);
  }

  List<INode> getPathINodes(String path) {
    return resolvedINodesMap.getPathINodes(path);
  }

  INode getTargetINode(String path) {
    List<INode> list = resolvedINodesMap.getPathINodes(path);
    return list.get(list.size() - 1);
  }

  List<INode> getChildINodes(String path) {
    return resolvedINodesMap.getChildINodes(path);
  }

  public TransactionLockTypes.INodeLockType getLockedINodeLockType(
      INode inode) {
    return allLockedInodesInTx.get(inode);
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, String name,
      int parentId, int partitionId, int possibleINodeId)
      throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode = EntityManager
        .find(INode.Finder.ByNameParentIdAndPartitionId, name, parentId, partitionId, possibleINodeId);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, String name,
      int parentId, int partitionId) throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode =
        EntityManager.find(INode.Finder.ByNameParentIdAndPartitionId, name, parentId, partitionId);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, int id)
      throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected List<INode> find(TransactionLockTypes.INodeLockType lock,
      String[] names, int[] parentIds, int[] partitionIds, boolean checkLocalCache)
      throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    List<INode> inodes = (List<INode>) EntityManager.findList(checkLocalCache ? INode.Finder
                .ByNamesParentIdsAndPartitionIdsCheckLocal : INode.Finder
                .ByNamesParentIdsAndPartitionIds, names, parentIds, partitionIds);
    if(inodes != null) {
      for (INode inode : inodes) {
        addLockedINodes(inode, lock);
      }
    }
    return inodes;
  }

  protected void addLockedINodes(INode inode,
      TransactionLockTypes.INodeLockType lock) {
    if (inode == null) {
      return;
    }
    TransactionLockTypes.INodeLockType oldLock = allLockedInodesInTx.get(inode);
    if (oldLock == null || oldLock.compareTo(lock) < 0) {
      allLockedInodesInTx.put(inode, lock);
    }
  }

  protected void setINodeLockType(TransactionLockTypes.INodeLockType lock)
      throws StorageException {
    switch (lock) {
      case WRITE:
      case WRITE_ON_TARGET_AND_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }

  protected void acquireINodeAttributes()
      throws StorageException, TransactionContextException {
    List<INodeCandidatePrimaryKey> pks =
        new ArrayList<>();
    for (INode inode : getAllResolvedINodes()) {
      if ((inode instanceof INodeDirectory) && ((INodeDirectory) inode).isWithQuota()) {
        INodeCandidatePrimaryKey pk =
            new INodeCandidatePrimaryKey(inode.getId());
        pks.add(pk);
      }
    }
    acquireLockList(DEFAULT_LOCK_TYPE, INodeAttributes.Finder.ByINodeIds, pks);
  }

  protected void setPartitioningKey(Integer partitionId)
          throws StorageException, TransactionContextException {
    if (setPartitionKeyEnabled && partitionId != null && !isPartitionKeyAlreaySet ) {
      //set partitioning key
      Object[] key = new Object[3];
      key[0] = partitionId;
      key[1] = 0;
      key[2] = "";
      EntityManager.setPartitionKey(INodeDataAccess.class, key);
      isPartitionKeyAlreaySet=true; //to avoid setting partition key multiple times. It can happen during rename
      LOG.debug("Setting PartitionKey to be " + partitionId);
    } else {
      LOG.debug("Transaction PartitionKey is not Set");
    }
  }

  @Override
  protected final Type getType() {
    return Type.INode;
  }

  protected static boolean isStoredInDB(INode inode){
    if(inode instanceof  INodeFile){
      INodeFile file = (INodeFile)inode;
      return file.isFileStoredInDB();
    }else{
      return false;
    }
  }
  
  protected List<INode> readUpInodes(INode leaf)
      throws StorageException, TransactionContextException {
    LinkedList<INode> pathInodes = new LinkedList<>();
    pathInodes.add(leaf);
    INode curr = leaf;
    while (curr.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      curr = find(TransactionLockTypes.INodeLockType.READ_COMMITTED,
          curr.getParentId());
      if(curr==null){
        break;
      }
      pathInodes.addFirst(curr);
    }
    return pathInodes;
  }

  public BaseINodeLock enableHierarchicalLocking(boolean val){
    this.enableHierarchicalLocking = val;
    return this;
  }

  public TransactionLockTypes.INodeLockType getDefaultInodeLockType(){
    if(enableHierarchicalLocking){
      return TransactionLockTypes.INodeLockType.READ;
    }else{
      return TransactionLockTypes.INodeLockType.READ_COMMITTED;
    }
  }
}
