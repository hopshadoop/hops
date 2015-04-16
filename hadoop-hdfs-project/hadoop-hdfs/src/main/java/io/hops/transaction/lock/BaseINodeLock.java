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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.hops.common.INodeResolver;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.exception.TransientStorageException;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.resolvingcache.Cache;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import static io.hops.transaction.lock.Lock.LOG;
import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
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
  
  List<INode> getTargetINodes() {
    List<INode> targetInodes =
        Lists.newArrayListWithExpectedSize(resolvedINodesMap.pathToPathINodes.size());
    for(String path : resolvedINodesMap.pathToPathINodes.keySet()) {
      List<INode> list = resolvedINodesMap.getPathINodes(path);
      targetInodes.add(list.get(list.size() - 1));
    }
    targetInodes.addAll(resolvedINodesMap.individualInodes);
    return targetInodes;
  }
  
  List<INode> getChildINodes(String path) {
    return resolvedINodesMap.getChildINodes(path);
  }

  public TransactionLockTypes.INodeLockType getLockedINodeLockType(
      INode inode) {
    return allLockedInodesInTx.get(inode);
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, String name,
      long parentId, long partitionId, long possibleINodeId)
      throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode = EntityManager
        .find(INode.Finder.ByNameParentIdAndPartitionId, name, parentId, partitionId, possibleINodeId);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, String name,
      long parentId, long partitionId) throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode =
        EntityManager.find(INode.Finder.ByNameParentIdAndPartitionId, name, parentId, partitionId);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, long id)
      throws StorageException, TransactionContextException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, id);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected List<INode> find(TransactionLockTypes.INodeLockType lock,
      String[] names, long[] parentIds, long[] partitionIds, boolean checkLocalCache)
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
    acquireLockList(DEFAULT_LOCK_TYPE, DirectoryWithQuotaFeature.Finder.ByINodeIds, pks);
  }

  protected void setPartitioningKey(Long partitionId)
          throws StorageException, TransactionContextException {
    if (setPartitionKeyEnabled && partitionId != null && !isPartitionKeyAlreaySet ) {
      //set partitioning key
      Object[] key = new Object[3];
      key[0] = partitionId;
      key[1] = 0L;
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
    while (curr.getParentId() != HdfsConstantsClient.GRANDFATHER_INODE_ID) {
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

  protected abstract class CacheResolver {

    abstract List<INode> fetchINodes(final TransactionLockTypes.INodeLockType lockType, String path, boolean resolveLink)
        throws IOException;

    abstract List<INode> fetchINodes(final TransactionLockTypes.INodeLockType lockType, long inodeId) throws IOException;

    protected int verifyINodesFull(final List<INode> inodes, final String[] names, final long[] parentIds,
        final long[] inodeIds) throws IOException {
      int index = -1;
      if (names.length == parentIds.length) {
        if (inodes.size() == names.length) {
          index = verifyINodesPartial(inodes, names, parentIds, inodeIds);
        }
      }
      return index;
    }

    protected int verifyINodesPartial(final List<INode> inodes, final String[] names, final long[] parentIds,
        final long[] inodeIds) throws IOException {
      int index = (int) StatUtils.min(new double[]{inodes.size(), inodeIds.length, parentIds.length, names.length});
      for (int i = 0; i < index; i++) {
        INode inode = inodes.get(i);
        boolean noChangeInInodes = inode != null && inode.getLocalName().equals(names[i]) && inode.getParentId()
            == parentIds[i] && inode.getId() == inodeIds[i];
        if (!noChangeInInodes) {
          index = i;
          break;
        }
      }
      return index;
    }

    protected int reverseVerifyINodesPartial(final List<INode> inodes, final String[] names, final long[] parentIds,
        final long[] inodeIds) throws IOException {
      int maxIndex = (int) StatUtils.min(new double[]{inodes.size(), inodeIds.length, parentIds.length, names.length});
      for (int i = 1; i <= maxIndex; i++) {
        INode inode = inodes.get(inodes.size() - i);
        boolean noChangeInInodes = inode != null && inode.getLocalName().equals(names[names.length - i]) && inode.
            getParentId() == parentIds[parentIds.length - i] && inode.getId() == inodeIds[inodeIds.length - i];
        if (!noChangeInInodes) {
          return i;
        }
      }
      return maxIndex + 1;
    }

    protected long[] getParentIds(long[] inodeIds) {
      return getParentIds(inodeIds, false);
    }

    protected long[] getParentIds(long[] inodeIds, boolean partial) {
      long[] parentIds = new long[partial ? inodeIds.length + 1 : inodeIds.length];
      parentIds[0] = HdfsConstantsClient.GRANDFATHER_INODE_ID;
      System.arraycopy(inodeIds, 0, parentIds, 1, (partial ? inodeIds.length
          : inodeIds.length - 1));
      return parentIds;
    }

    protected void setPartitionKey(long[] inodeIds, long parentIds[], long partitionIds[], boolean partial)
        throws TransactionContextException, StorageException {
      Long partId = null;
      if (partial) {
        if (setRandomParitionKeyEnabled && partId == null) {
          LOG.debug("Setting Random PartitionKey");
          partId = Math.abs(rand.nextLong());
        }
      } else {
        partId = inodeIds[inodeIds.length - 1];
      }
      setPartitioningKey(partId);
    }
  }

  protected class PathResolver extends CacheResolver {

    @Override
    List<INode> fetchINodes(final TransactionLockTypes.INodeLockType lockType, String path, boolean resolveLink) throws
        IOException {
      long[] inodeIds = Cache.getInstance().get(path);
      if (inodeIds != null) {
        final String[] names = INode.getPathNames(path);
        final boolean partial = names.length > inodeIds.length;

        final long[] parentIds = getParentIds(inodeIds, partial);
        final long[] partitionIds = new long[parentIds.length];

        short depth = INodeDirectory.ROOT_DIR_DEPTH;
        partitionIds[0] = INodeDirectory.getRootDirPartitionKey();
        for (int i = 1; i < partitionIds.length; i++) {
          depth++;
          partitionIds[i] = INode.calculatePartitionId(parentIds[i], names[i], depth);
        }

        setPartitionKey(inodeIds, parentIds, partitionIds, partial);

        List<INode> inodes = readINodesWhileRespectingLocks(lockType, path, names,
            parentIds, partitionIds, resolveLink);
        if (inodes != null && !inodes.isEmpty()) {
          final int verifiedInode = verifyINodesPartial(inodes, names,
              parentIds, inodeIds);

          int diff = inodes.size() - verifiedInode;
          while (diff > 0) {
            INode node = inodes.remove(inodes.size() - 1);
            if (node != null) {
              Cache.getInstance().delete(node);
            }
            diff--;
          }

          if (verifiedInode <= 1) {
            return null;
          }

          tryResolvingTheRest(lockType, path, inodes, resolveLink);
          return inodes;
        }
      }
      return null;
    }

    INode lockInode(final TransactionLockTypes.INodeLockType lockType, long inodeId) throws IOException {
      setINodeLockType(lockType);
      INode targetInode = INodeUtil.getNode(inodeId, true);
      setINodeLockType(getDefaultInodeLockType());
      if (targetInode == null) {
        //we throw a LeaseExpiredException as this should be called only when we try to edit an open file
        //and the exception normaly sent if the file does not exist. This is to be compatible with apache client.
        throw new LeaseExpiredException("No lease on inode: " + inodeId + ": File does not exist. ");
      }
      Cache.getInstance().set(targetInode);
      return targetInode;
    }

    List<INode> lockInodeAndParent(final TransactionLockTypes.INodeLockType lockType, long inodeId) throws IOException {
      List<INode> inodes = new LinkedList<>();
      INodeIdentifier targetIdentifier = Cache.getInstance().get(inodeId);
      if (targetIdentifier == null) {
        INode targetInode = INodeUtil.getNode(inodeId, false);
        if (targetInode == null) {
          //we throw a LeaseExpiredException as this should be called only when we try to edit an open file
          //and the exception normaly sent if the file does not exist. This is to be compatible with apache client.
          throw new LeaseExpiredException("No lease on " + inodeId + ": File does not exist. ");
        }
        targetIdentifier = new INodeIdentifier(targetInode.getId(), targetInode.getParentId(), targetInode.
            getLocalName(), targetInode.getPartitionId());
      }

      if (targetIdentifier.getInodeId() == INode.ROOT_INODE_ID) {
        inodes.add(lockInode(lockType, inodeId));
        return inodes;
      }
      setINodeLockType(TransactionLockTypes.INodeLockType.WRITE);
      INode parentInode = INodeUtil.getNode(targetIdentifier.getPid(), true);
      if (parentInode == null) {
        //the cached inode does not match the one in the DB remove from cache and retry transaction
        Cache.getInstance().delete(targetIdentifier);
        throw new TransientStorageException("wrong inode in the cache");
      }
      inodes.add(parentInode);
      Cache.getInstance().set(parentInode);
      INode targetINode = lockInode(lockType, inodeId);
      if (targetINode.getParentId() != parentInode.getId()) {
        //the cached inode didn't match the one in the DB, it has now been overwriten by the one in the db, retry transaction
        throw new TransientStorageException("wrong inode in the cache");
      }
      inodes.add(targetINode);
      return inodes;
    }

    void getPathInodes(long parentId, Map<Long, INode> alreadyFetchedInodes) throws IOException {
      if (parentId == HdfsConstantsClient.GRANDFATHER_INODE_ID) {
        return;
      }
      INodeIdentifier cur = Cache.getInstance().get(parentId);
      if (cur == null) {
        INode inode = INodeUtil.getNode(parentId, true);
        if (inode == null) {
          return;
        }
        Cache.getInstance().set(inode);
        alreadyFetchedInodes.put(inode.getId(), inode);
        cur = new INodeIdentifier(inode.getId(), inode.getParentId(), inode.getLocalName(), inode.getPartitionId());
      }
      Map<Long, INodeIdentifier> inodeIdentifiers = new HashMap<>();
      while (cur != null) {
        inodeIdentifiers.put(cur.getInodeId(), cur);
        parentId = cur.getPid();
        cur = Cache.getInstance().get(parentId);
      }

      for (INodeIdentifier identifier : inodeIdentifiers.values()) {
        if (alreadyFetchedInodes.containsKey(identifier.getInodeId())) {
          inodeIdentifiers.remove(identifier);
        }
      }

      if (!inodeIdentifiers.isEmpty()) {
        long[] inodeIds = new long[inodeIdentifiers.size()];
        final String[] names = new String[inodeIdentifiers.size()];
        final long[] parentIds = new long[inodeIdentifiers.size()];
        final long[] partitionIds = new long[inodeIdentifiers.size()];

        int i = 0;
        for (INodeIdentifier inode : inodeIdentifiers.values()) {
          inodeIds[i] = inode.getInodeId();
          names[i] = inode.getName();
          parentIds[i] = inode.getPid();
          partitionIds[i] = inode.getPartitionId();
          i++;
        }

        List<INode> inodesFound = find(getDefaultInodeLockType(), names, parentIds, partitionIds, true);
        for (INode inode : inodesFound) {
          INodeIdentifier identifier = inodeIdentifiers.get(inode.getId());
          if (identifier != null && inode.getLocalName().equals(identifier.getName()) && inode.getParentId()
              == identifier.getPid()) {
            inodeIdentifiers.remove(inode.getId());
            alreadyFetchedInodes.put(inode.getId(), inode);
            Cache.getInstance().set(inode);
          }
        }
        for (INodeIdentifier identifier : inodeIdentifiers.values()) {
          //these identifier are in the cache but do not match the DB remove from cache and get from DB
          Cache.getInstance().delete(identifier);
          getPathInodes(identifier.getInodeId(), alreadyFetchedInodes);
        }
      }
      if (parentId != HdfsConstantsClient.GRANDFATHER_INODE_ID) {
        getPathInodes(parentId, alreadyFetchedInodes);
      }
    }

    @Override
    List<INode> fetchINodes(final TransactionLockTypes.INodeLockType lockType, long inodeId) throws IOException {
      List<INode> inodes = new LinkedList<>();
      //get the inode and its parrent from the cache
      if (TransactionLockTypes.impliesParentWriteLock(lockType)) {
        inodes.addAll(lockInodeAndParent(lockType, inodeId));
      } else {
        inodes.add(lockInode(lockType, inodeId));
      }

      //we are now sure that nothing should rewrite the parents path, build it
      Map<Long, INode> parentsMap = new HashMap<>();
      getPathInodes(inodes.get(0).getParentId(), parentsMap);
      INode cur = parentsMap.get(inodes.get(0).getParentId());
      while (cur != null) {
        inodes.add(0, cur);
        cur = parentsMap.get(cur.getParentId());
      }
      return inodes;
    }

    protected void tryResolvingTheRest(final TransactionLockTypes.INodeLockType lockType, String path,
        List<INode> inodes, boolean resolveLink)
        throws TransactionContextException, UnresolvedPathException, StorageException {
      int offset = inodes.size();

      resolveRestOfThePath(lockType, path, inodes, resolveLink);
      addPathINodesWithOffset(path, inodes, offset);
    }

    private void addPathINodesWithOffset(String path, List<INode> inodes, int offset) {
      addPathINodes(path, inodes);
      if (offset == 0) {
        updateResolvingCache(path, inodes);
      } else {
        if (offset == inodes.size()) {
          return;
        }
        List<INode> newInodes = inodes.subList(offset, inodes.size());
        String[] newPath = Arrays.copyOfRange(INode.getPathNames(path), offset,
            inodes.size());
        updateResolvingCache(
            Joiner.on(Path.SEPARATOR_CHAR).join(newPath), newInodes);
      }
    }

    protected List<INode> readINodesWhileRespectingLocks(final TransactionLockTypes.INodeLockType lockType,
        final String path,
        final String[] names, final long[] parentIds, final long[] partitionIds, boolean resolveLink)
        throws TransactionContextException, StorageException,
        UnresolvedPathException {
      int rowsToReadWithDefaultLock = names.length;
      if (!lockType.equals(getDefaultInodeLockType())) {
        if (lockType.equals(
            TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT)) {
          rowsToReadWithDefaultLock -= 2;
        } else {
          rowsToReadWithDefaultLock -= 1;
        }
      }

      rowsToReadWithDefaultLock = Math.min(rowsToReadWithDefaultLock,
          parentIds.length);

      List<INode> inodes = null;
      if (rowsToReadWithDefaultLock > 0) {
        inodes = find(getDefaultInodeLockType(),
            Arrays.copyOf(names, rowsToReadWithDefaultLock),
            Arrays.copyOf(parentIds, rowsToReadWithDefaultLock),
            Arrays.copyOf(partitionIds, rowsToReadWithDefaultLock), true);
      }

      if (inodes != null) {
        for (INode inode : inodes) {
          addLockedINodes(inode, getDefaultInodeLockType());
        }
      }

      if (rowsToReadWithDefaultLock == names.length) {
        return inodes;
      }

      boolean partialPath = parentIds.length < names.length;

      if (inodes != null && !partialPath) {
        resolveRestOfThePath(lockType, path, inodes, resolveLink);
      }
      return inodes;
    }

    protected void resolveRestOfThePath(final TransactionLockTypes.INodeLockType lockType, String path,
        List<INode> inodes, boolean resolveLink)
        throws StorageException, TransactionContextException,
        UnresolvedPathException {
      byte[][] components = INode.getPathComponents(path);
      INode currentINode = inodes.get(inodes.size() - 1);
      INodeResolver resolver = new INodeResolver(components, currentINode, resolveLink, true,
          inodes.size() - 1);
      while (resolver.hasNext()) {
        TransactionLockTypes.INodeLockType currentINodeLock = identifyLockType(lockType, resolver.getCount() + 1,
            components);
        setINodeLockType(currentINodeLock);
        currentINode = resolver.next();
        if (currentINode != null) {
          addLockedINodes(currentINode, currentINodeLock);
          inodes.add(currentINode);
        }
      }
    }
  }

  protected TransactionLockTypes.INodeLockType identifyLockType(final TransactionLockTypes.INodeLockType lockType,
      int count,
      byte[][] components) {
    TransactionLockTypes.INodeLockType lkType;
    if (isTarget(count, components)) {
      lkType = lockType;
    } else if (isParent(count, components) && TransactionLockTypes.impliesParentWriteLock(lockType)) {
      lkType = TransactionLockTypes.INodeLockType.WRITE;
    } else {
      lkType = getDefaultInodeLockType();
    }
    return lkType;
  }

  protected boolean isTarget(int count, byte[][] components) {
    return count == components.length - 1;
  }

  protected boolean isParent(int count, byte[][] components) {
    return count == components.length - 2;
  }

  protected List<INode> resolveUsingCache(final TransactionLockTypes.INodeLockType lockType, long inodeId) throws
      IOException {
    CacheResolver cacheResolver = getCacheResolver();
    return cacheResolver.fetchINodes(lockType, inodeId);
  }

  private CacheResolver instance = null;

  protected CacheResolver getCacheResolver() {
    if (instance == null) {
      instance = new PathResolver();
    }
    return instance;
  }
}
