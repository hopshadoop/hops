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
import io.hops.common.INodeResolver;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.resolvingcache.Cache;
import io.hops.resolvingcache.OptimalMemcache;
import io.hops.resolvingcache.PathMemcache;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.ipc.RetriableException;

import java.io.IOException;
import java.util.*;

public class INodeLock extends BaseINodeLock {
  
  private final TransactionLockTypes.INodeLockType lockType;
  private final TransactionLockTypes.INodeResolveType resolveType;
  private boolean resolveLink;
  protected final String[] paths;
  private Collection<Integer> ignoredSTOInodes;
  protected boolean skipReadingQuotaAttr;
  protected long namenodeId;
  protected Collection<ActiveNode> activeNamenodes;

  INodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String... paths) {
    super();
    this.lockType = lockType;
    this.resolveType = resolveType;
    this.resolveLink = false;
    this.activeNamenodes = null;
    this.ignoredSTOInodes = new ArrayList<Integer>();
    this.namenodeId = -1;
    this.paths = paths;
    this.skipReadingQuotaAttr = false;
  }

  public INodeLock setIgnoredSTOInodes(int inodeID) {
    this.ignoredSTOInodes.add(inodeID);
    return this;
  }

  public INodeLock setNameNodeID(long nnID) {
    this.namenodeId = nnID;
    return this;
  }

  public INodeLock setActiveNameNodes(Collection<ActiveNode> activeNamenodes){
    this.activeNamenodes = activeNamenodes;
    return this;
  }

  public INodeLock skipReadingQuotaAttr(boolean val){
    this.skipReadingQuotaAttr = val;
    return this;
  }

  public INodeLock resolveSymLink(boolean resolveLink) {
    this.resolveLink = resolveLink;
    return this;
  }

  private CacheResolver instance = null;

  private CacheResolver getCacheResolver(){
    if(instance == null){
      if(Cache.getInstance() instanceof OptimalMemcache){
        instance = new OptimalPathResolver();
      } else if(Cache.getInstance() instanceof PathMemcache){
        instance = new FullPathResolver();
      }else {
        instance = new PartialPathResolver();
      }
    }
    return instance;
  }

  private abstract class CacheResolver {
    abstract List<INode> fetchINodes(String path) throws IOException;

    protected int verifyINodesFull(final List<INode> inodes, final String[]
        names, final int[] parentIds, final int[] inodeIds) throws IOException {
      int index = -1;
      if (names.length == parentIds.length) {
        if (inodes.size() == names.length) {
          index = verifyINodesPartial(inodes, names, parentIds, inodeIds);
        }
      }
      return index;
    }

    protected int verifyINodesPartial(final List<INode> inodes, final String[]
        names, final int[] parentIds, final int[] inodeIds) throws IOException {
      int index = (int)StatUtils.min(new double[]{inodes.size(), inodeIds
          .length, parentIds.length, names.length});
      for (int i = 0; i < index; i++) {
        INode inode = inodes.get(i);
        boolean noChangeInInodes =
            inode != null && inode.getLocalName().equals(names[i]) &&
                inode.getParentId() == parentIds[i] &&
                inode.getId() == inodeIds[i];
        if (!noChangeInInodes) {
          index = i;
          break;
        }
      }
      return index;
    }

    protected int[] getParentIds(int[] inodeIds) {
      return getParentIds(inodeIds, false);
    }

    protected int[] getParentIds(int[] inodeIds, boolean partial) {
      int[] parentIds = new int[partial ? inodeIds.length + 1 : inodeIds.length];
      parentIds[0] = INodeDirectory.ROOT_PARENT_ID;
      System.arraycopy(inodeIds, 0, parentIds, 1, (partial ? inodeIds.length
          : inodeIds.length - 1));
      return parentIds;
    }

    protected void setPartitionKey(int[] inodeIds, int parentIds[], int partitionIds[], boolean partial)
            throws TransactionContextException, StorageException {
      Integer partId = null;
      if(partial){
        if (setRandomParitionKeyEnabled && partId == null) {
          LOG.debug("Setting Random PartitionKey");
          partId = Math.abs(rand.nextInt());
        }
      }else{
          partId = inodeIds[inodeIds.length - 1];
       }
      setPartitioningKey(partId);
    }
  }

  private class FullPathResolver extends CacheResolver {

    @Override
    List<INode> fetchINodes(String path) throws IOException {
      int[] inodeIds = Cache.getInstance().get(path);
      if (inodeIds != null) {
        final String[] names = INode.getPathNames(path);
        final boolean partial = names.length > inodeIds.length;

        final int[] parentIds = getParentIds(inodeIds);
        final int[] partitionIds = new int[parentIds.length];

        short depth = INodeDirectory.ROOT_DIR_DEPTH;
        partitionIds[0] = INodeDirectory.getRootDirPartitionKey();
        for(int i = 1; i < partitionIds.length; i++){
          depth++;
          partitionIds[i] = INode.calculatePartitionId(parentIds[i], names[i], depth);
        }

        setPartitionKey(inodeIds,parentIds,partitionIds,partial);

        List<INode> inodes = readINodesWhileRespectingLocks(path,names,
            parentIds,partitionIds);
        if (inodes != null) {
          if (verifyINodes(inodes, names, parentIds, inodeIds)) {
            addPathINodes(path, inodes);
            return inodes;
          } else {
            Cache.getInstance().delete(path);
          }
        }
      }
      return null;
    }

    private boolean verifyINodes(List<INode> inodes, String[] names,
        int[] parentIds, int[] inodeIds) throws IOException {
      return verifyINodesFull(inodes, names, parentIds, inodeIds) == inodes
          .size();
    }

    protected List<INode> readINodesWhileRespectingLocks(final String path,
        final String[] names, final int[] parentIds, final int[] partitionIds)
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

      if(inodes != null) {
        for (INode inode : inodes) {
          addLockedINodes(inode, getDefaultInodeLockType());
        }
      }
      
      if(rowsToReadWithDefaultLock == names.length){
        return inodes;
      }
  
      boolean partialPath = parentIds.length < names.length;

      if (inodes != null && !partialPath) {
        resolveRestOfThePath(path, inodes);
      }
      return inodes;
    }

    protected void resolveRestOfThePath(String path, List<INode> inodes)
        throws StorageException, TransactionContextException,
        UnresolvedPathException {
      byte[][] components = INode.getPathComponents(path);
      INode currentINode = inodes.get(inodes.size() - 1);
      INodeResolver resolver =
          new INodeResolver(components, currentINode, resolveLink, true,
              inodes.size() - 1);
      while (resolver.hasNext()) {
        TransactionLockTypes.INodeLockType currentINodeLock =
            identifyLockType(resolver.getCount() + 1, components);
        setINodeLockType(currentINodeLock);
        currentINode = resolver.next();
        if (currentINode != null) {
          addLockedINodes(currentINode, currentINodeLock);
          inodes.add(currentINode);
        }
      }
    }
  }

  private class PartialPathResolver extends FullPathResolver {

    @Override
    List<INode> fetchINodes(String path) throws
        IOException {
      int[] inodeIds = Cache.getInstance().get(path);
      if (inodeIds != null) {
        final String[] names = INode.getPathNames(path);
        final boolean partial = names.length > inodeIds.length;
        final int[] parentIds = getParentIds(inodeIds, partial);
        final int[] partitionIds = new int[parentIds.length];

        short depth = INodeDirectory.ROOT_DIR_DEPTH;
        partitionIds[0] = INodeDirectory.getRootDirPartitionKey();
        for(int i = 1; i < partitionIds.length;i++){
          depth++;
          partitionIds[i] = INode.calculatePartitionId(parentIds[i], names[i], depth);
        }

        setPartitionKey(inodeIds, parentIds, partitionIds, partial);


        List<INode> inodes = readINodesWhileRespectingLocks(path, names,
            parentIds, partitionIds);
        if (inodes != null && !inodes.isEmpty()) {
          final int unverifiedInode = verifyINodesPartial(inodes, names,
              parentIds, inodeIds);

          int diff = inodes.size() - unverifiedInode;
          while (diff > 0){
            INode node = inodes.remove(inodes.size() - 1);
            if(node!=null){
              Cache.getInstance().delete(node);
            }
            diff--;
          }

          if(unverifiedInode <= 1)
            return null;

          tryResolvingTheRest(path, inodes, inodes.size() - unverifiedInode);
          return inodes;
        }
      }
      return null;
    }

    protected void tryResolvingTheRest(String path, List<INode> inodes, int
        diff)
        throws TransactionContextException, UnresolvedPathException,
        StorageException {
      int offset = inodes.size();

      resolveRestOfThePath(path, inodes);

      addPathINodesWithOffset(path, inodes, offset);
    }


    private void addPathINodesWithOffset(String path, List<INode> inodes, int
        offset){
      addPathINodes(path, inodes);
      if(offset == 0){
        updateResolvingCache(path, inodes);
      }else {
        if(offset == inodes.size()){
          return;
        }
        List<INode> newInodes = inodes.subList(offset, inodes.size());
        String[] newPath = Arrays.copyOfRange(INode.getPathNames(path), offset,
            inodes.size());
        updateResolvingCache(
            Joiner.on(Path.SEPARATOR_CHAR).join(newPath), newInodes);
      }
    }
  }

  private class OptimalPathResolver extends PartialPathResolver{
    @Override
    protected void tryResolvingTheRest(String path, List<INode> inodes,
        int diff)
        throws TransactionContextException, UnresolvedPathException,
        StorageException {

      resolveRestOfThePath(path, inodes);

      addPathINodes(path, inodes);

      if(diff > 1){
        Cache.getInstance().delete(path);
        updateResolvingCache(path, inodes);
      }else{
        updateResolvingCache(inodes.get(inodes.size() - 1));
      }
    }
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    /*
     * Needs to be sorted in order to avoid deadlocks. Otherwise one transaction
     * could acquire path0 and path1 in the given order while another one does
     * it in the opposite order, more precisely path1, path0, what could cause
     * a dealock situation.
     */
    Arrays.sort(paths);
    acquireINodeLocks();
    if(!skipReadingQuotaAttr){
      acquireINodeAttributes();
    }
  }

  protected void acquireINodeLocks() throws IOException {
    if (!resolveType.equals(TransactionLockTypes.INodeResolveType.PATH) &&
            !resolveType.equals(
                    TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) &&
            !resolveType.equals(
                    TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY)) {
      throw new IllegalArgumentException("Unknown type " + resolveType.name());
    }

    for (String path : paths) {
      List<INode> resolvedINodes = null;
      if (getDefaultInodeLockType() == TransactionLockTypes.INodeLockType.READ_COMMITTED) {
        //Batching only works in READ_COMITTED mode. If locking is enabled then it can lead to deadlocks.
        resolvedINodes = resolveUsingMemcache(path);
      }

      if (resolvedINodes == null) {
        // path not found in the cache
        // set random partition key if enabled
        if (setRandomParitionKeyEnabled) {
          setPartitioningKey(rand.nextInt());
        }
        resolvedINodes = acquireINodeLockByPath(path);
        addPathINodesAndUpdateResolvingCache(path, resolvedINodes);
      }

      if (resolvedINodes.size() > 0) {
        INode lastINode = resolvedINodes.get(resolvedINodes.size() - 1);
        if (resolveType ==
                TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
          List<INode> children = findImmediateChildren(lastINode);
          addChildINodes(path, children);
        } else if (resolveType ==
                TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY) {
          List<INode> children = findChildrenRecursively(lastINode);
          addChildINodes(path, children);
        }
      }
    }
  }

  private List<INode> resolveUsingMemcache(String path) throws IOException {
    CacheResolver memcacheResolver = getCacheResolver();
    if(memcacheResolver == null)
      return null;
    List<INode> resolvedINodes = memcacheResolver.fetchINodes(path);
    if (resolvedINodes != null) {
      for (INode iNode : resolvedINodes) {
        if(iNode!=null){
          checkSubtreeLock(iNode);
        }
      }
      handleLockUpgrade(resolvedINodes, INode.getPathComponents(path), path);
    }
    return resolvedINodes;
  }

  private List<INode> acquireINodeLockByPath(String path)
      throws UnresolvedPathException, StorageException, RetriableException,
      TransactionContextException {
    List<INode> resolvedINodes = new ArrayList<>();
    byte[][] components = INode.getPathComponents(path);

    INode currentINode;
    if (isRootTarget(components)) {
      resolvedINodes.add(acquireLockOnRoot(lockType));
      return resolvedINodes;
    } else if (isRootParent(components) &&
        TransactionLockTypes.impliesParentWriteLock(this.lockType)) {
      currentINode = acquireLockOnRoot(lockType);
    } else {
      currentINode = acquireLockOnRoot(getDefaultInodeLockType());
    }
    resolvedINodes.add(currentINode);

    INodeResolver resolver =
        new INodeResolver(components, currentINode, resolveLink, true);
    while (resolver.hasNext()) {
      TransactionLockTypes.INodeLockType currentINodeLock =
          identifyLockType(resolver.getCount() + 1, components);
      setINodeLockType(currentINodeLock);
      currentINode = resolver.next();
      if (currentINode != null) {
        addLockedINodes(currentINode, currentINodeLock);
        checkSubtreeLock(currentINode);
        resolvedINodes.add(currentINode);
      }
    }

    handleLockUpgrade(resolvedINodes, components, path);
    return resolvedINodes;
  }

  private boolean isRootTarget(byte[][] components) {
    return isTarget(0, components);
  }

  private boolean isRootParent(byte[][] components) {
    return isParent(0, components);
  }

  private TransactionLockTypes.INodeLockType identifyLockType(int count,
      byte[][] components) {
    TransactionLockTypes.INodeLockType lkType;
    if (isTarget(count, components)) {
      lkType = this.lockType;
    } else if (isParent(count, components) &&
        TransactionLockTypes.impliesParentWriteLock(this.lockType)) {
      lkType = TransactionLockTypes.INodeLockType.WRITE;
    } else {
      lkType = getDefaultInodeLockType();
    }
    return lkType;
  }

  private boolean isTarget(int count, byte[][] components) {
    return count == components.length - 1;
  }

  private boolean isParent(int count, byte[][] components) {
    return count == components.length - 2;
  }

  private void checkSubtreeLock(INode iNode) throws RetriableException {
    if (SubtreeLockHelper.isSTOLocked(iNode.isSTOLocked(),
            iNode.getSTOLockOwner(), activeNamenodes)) {
      if (!ignoredSTOInodes.contains(iNode.getId())) {
        throw new RetriableException("The subtree "+iNode.getLocalName() +" is locked by " +
                "Namenode Id: " + iNode.getSTOLockOwner() + ". Active Namenodes are: "+activeNamenodes);
      } else {
        LOG.debug("Ignoring subtree lock for inode id: "+iNode.getId());
      }
    }
  }

  private void handleLockUpgrade(List<INode> resolvedINodes,
      byte[][] components, String path)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    // TODO Handle the case that predecessing nodes get deleted before locking
    // lock upgrade if the path was not fully resolved
    if (resolvedINodes.size() != components.length) {
      // path was not fully resolved
      INode inodeToReread = null;
      if (lockType ==
          TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
        if (resolvedINodes.size() <= components.length - 2) {
          inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
        }
      } else if (lockType == TransactionLockTypes.INodeLockType.WRITE) {
        inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
      }

     if (inodeToReread != null) {
        int partitionIdOfINodeToBeReRead = INode.calculatePartitionId(inodeToReread.getParentId(), inodeToReread
        .getLocalName(), inodeToReread.myDepth());
        INode inode = find(lockType, inodeToReread.getLocalName(),
            inodeToReread.getParentId(), partitionIdOfINodeToBeReRead);
        if (inode != null) {
          // re-read after taking write lock to make sure that no one has created the same inode.
          addLockedINodes(inode, lockType);
          String existingPath = buildPath(path, resolvedINodes.size());
          List<INode> rest =
              acquireLockOnRestOfPath(lockType, inode, path, existingPath,
                  false);
          resolvedINodes.addAll(rest);
        }
      }
    }
  }

  private List<INode> acquireLockOnRestOfPath(
      TransactionLockTypes.INodeLockType lock, INode baseInode, String fullPath,
      String prefix, boolean resolveLink)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    List<INode> resolved = new ArrayList<>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    INodeResolver resolver =
        new INodeResolver(fullComps, baseInode, resolveLink, true,
            prefixComps.length - 1);
    while (resolver.hasNext()) {
      setINodeLockType(lock);
      INode current = resolver.next();
      if (current != null) {
        addLockedINodes(current, lock);
        resolved.add(current);
      }
    }
    return resolved;
  }

  private List<INode> findImmediateChildren(INode lastINode)
      throws StorageException, TransactionContextException {
    List<INode> children = new ArrayList<>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        setINodeLockType(TransactionLockTypes.INodeLockType.READ_COMMITTED); //if the parent is locked then taking lock on all children is not necessary
        children.addAll(((INodeDirectory) lastINode).getChildrenList());
      }
    }
    return children;
  }

  private List<INode> findChildrenRecursively(INode lastINode)
      throws StorageException, TransactionContextException {
    LinkedList<INode> children = new LinkedList<>();
    LinkedList<INode> unCheckedDirs = new LinkedList<>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        unCheckedDirs.add(lastINode);
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        setINodeLockType(TransactionLockTypes.INodeLockType.READ_COMMITTED); //locking the parent is sufficient
        List<INode> clist = ((INodeDirectory) next).getChildrenList();
        unCheckedDirs.addAll(clist);
        children.addAll(clist);
      }
    }
    LOG.debug("Added " + children.size() + " children.");
    return children;
  }

  private INode acquireLockOnRoot(TransactionLockTypes.INodeLockType lock)
      throws StorageException, TransactionContextException {
    LOG.debug("Acquiring " + lock + " on the root node");
    return find(lock, INodeDirectory.ROOT_NAME, INodeDirectory.ROOT_PARENT_ID, INodeDirectory.getRootDirPartitionKey());
  }

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);

    for (int i = 0; i < Math.min(components.length, size); i++) {
      if (i == 0) {
        builder.append("/");
      } else {
        if (i != 1) {
          builder.append("/");
        }
        builder.append(DFSUtil.bytes2String(components[i]));
      }
    }

    return builder.toString();
  }
  
  protected INode find(String name, int parentId, int partitionId)
      throws StorageException, TransactionContextException {
    return find(lockType, name, parentId, partitionId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("INodeLock {");
    if(paths != null && paths.length > 0){
      sb.append("paths=");
      sb.append(Arrays.toString(paths));
      sb.append(", ");
    }
    if(lockType != null){
      sb.append("lockType=");
      sb.append(lockType);
      sb.append(", ");
    }

    sb.append("}");
    return sb.toString();
  }
}
