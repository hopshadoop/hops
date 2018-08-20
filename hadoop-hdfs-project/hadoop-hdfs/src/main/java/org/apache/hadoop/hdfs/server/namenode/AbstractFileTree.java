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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.security.UsersGroups;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.SubtreeLockHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@VisibleForTesting
abstract class AbstractFileTree {
  public static final Log LOG = LogFactory.getLog(AbstractFileTree.class);
  
  private final FSNamesystem namesystem;
  private final FSPermissionChecker fsPermissionChecker;
  private final INodeIdentifier subtreeRootId;
  private ConcurrentLinkedQueue<Future> activeCollectors = new ConcurrentLinkedQueue<>();

  public INodeIdentifier getSubtreeRootId() {
    return subtreeRootId;
  }

  private final FsAction subAccess;
  private volatile IOException exception;
  private List<AclEntry> subtreeRootDefaultEntries;
  
  public static class BuildingUpFileTreeFailedException extends IOException {
    BuildingUpFileTreeFailedException(String message) {
      super(message);
    }
  }
  
  private class ChildCollector implements Runnable {
    private final ProjectedINode parent;
    private final short depth; //this is the depth of the inode in the file system tree
    private final int level;
    private List<AclEntry> inheritedDefaultsAsAccess;
    
    private ChildCollector(ProjectedINode parent, short depth, int level, List<AclEntry> inheritedDefaultsAsAccess) {
      this.parent = parent;
      this.level = level;
      this.depth = depth;
      this.inheritedDefaultsAsAccess = inheritedDefaultsAsAccess;
    }
    
    @Override
    public void run() {
      LightWeightRequestHandler handler =
          new LightWeightRequestHandler(HDFSOperationType.GET_CHILD_INODES) {
            @Override
            public Object performTask() throws StorageException, IOException {
              INodeDataAccess<INode> dataAccess =
                  (INodeDataAccess) HdfsStorageFactory
                      .getDataAccess(INodeDataAccess.class);
              List<ProjectedINode> children = Collections.EMPTY_LIST;
              if (INode.isTreeLevelRandomPartitioned(depth)) {
                children = dataAccess.findInodesFTISTx(parent.getId(),
                        EntityContext.LockMode.READ_COMMITTED);
              } else {
                //then the partitioning key is the parent id
                children = dataAccess.findInodesPPISTx(parent.getId(), parent.getId(),
                        EntityContext.LockMode.READ_COMMITTED);
              }

              //locking with FTIS and PPIS is not a good idea. See JIRA HOPS-458
              //using batch operations to lock the children
              lockInodesUsingBatchOperation(children, dataAccess);

              Map<ProjectedINode, List<AclEntry>> acls = new HashMap<>();
              for (ProjectedINode child : children) {
                if (namesystem.isPermissionEnabled() && subAccess != null) {
                  List<AclEntry> inodeAclNoTransaction = INodeUtil.getInodeOwnAclNoTransaction(child);
                  acls.put(child, inodeAclNoTransaction);

                  if (inodeAclNoTransaction.isEmpty()){
                    checkAccess(child, subAccess, asAccessEntries(inheritedDefaultsAsAccess));
                  } else {
                    checkAccess(child, subAccess, inodeAclNoTransaction);
                  }
                }
                addChildNode(parent, level, child);
              }
  
              if (exception != null) {
                return null;
              }
  
              for (ProjectedINode child : children) {
                List<ActiveNode> activeNamenodes = namesystem.getNameNode().
                    getActiveNameNodes().getActiveNodes();
                if (SubtreeLockHelper.isSTOLocked(child.isSubtreeLocked(),
                    child.getSubtreeLockOwner(), activeNamenodes)) {
                  exception = new RetriableException("The subtree: "+child.getName()
                      +" is locked by Namenode: "+child.getSubtreeLockOwner()+"."+
                          " Active Namenodes: "+activeNamenodes);
                  return null;
                }

                List<AclEntry> newDefaults = filterAccessEntries(acls.get(child));
                if (child.isDirectory()) {
                  collectChildren(child, ((short) (depth + 1)), level + 1, newDefaults.isEmpty()
                      ? inheritedDefaultsAsAccess : newDefaults);
                }
              }
              return null;
            }
          };
      
      try {
        handler.handle(this);
      } catch (IOException e) {
        setExceptionIfNull(e);
      }
    }

    int batchIndex = 0;
    final int BATCHSIZE = 10000;

    private class InodesBatch {
      int[] partitionIDs = null;
      String[] names = null;
      int[] pids = null;
    }

    boolean getBatch(List<ProjectedINode> children, InodesBatch inodesBatch) {

      if (batchIndex >= children.size()) {
        return false;
      }

      int remaining = (children.size() - batchIndex);
      if (remaining > BATCHSIZE) {
        remaining = BATCHSIZE;
      } else {
        remaining = (children.size() - batchIndex);
      }

      inodesBatch.names = new String[remaining];
      inodesBatch.pids = new int[remaining];
      inodesBatch.partitionIDs = new int[remaining];

      for (int i = 0; i < remaining; i++) {
        ProjectedINode inode = children.get(batchIndex++);
        inodesBatch.names[i] = inode.getName();
        inodesBatch.pids[i] = inode.getParentId();
        inodesBatch.partitionIDs[i] = inode.getPartitionId();
      }
      return true;
    }

    void lockInodesUsingBatchOperation(List<ProjectedINode> children, INodeDataAccess<INode> dataAccess) throws StorageException {
      InodesBatch batch = new InodesBatch();
      while (getBatch(children, batch)) {
        dataAccess.lockInodesUsingPkBatchTx(batch.names, batch.pids, batch.partitionIDs,
                EntityContext.LockMode.WRITE_LOCK);
      }
    }
  }
  
  public AbstractFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId)
      throws AccessControlException {
    this(namesystem, subtreeRootId, null, null);
  }
  
  public AbstractFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId,
      FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries) throws AccessControlException {
    this.namesystem = namesystem;
    this.fsPermissionChecker = namesystem.getPermissionChecker();
    this.subtreeRootId = subtreeRootId;
    this.subAccess = subAccess;
    this.subtreeRootDefaultEntries = subtreeRootDefaultEntries;
  }
  
  private void checkAccess(INode node, FsAction action,
      List<AclEntry> aclEntries)
      throws IOException {
    if (!fsPermissionChecker.isSuperUser() && node.isDirectory()) {
      fsPermissionChecker.check(node, action, aclEntries);
    }
  }
  
  private void checkAccess(ProjectedINode node, FsAction action, List<AclEntry> aclEntries)
      throws IOException {
    if (!fsPermissionChecker.isSuperUser() && node.isDirectory()) {
      node.setUserName(UsersGroups.getUser(node.getUserID()));
      node.setGroupName(UsersGroups.getGroup(node.getGroupID()));
      fsPermissionChecker.check(node, action, aclEntries);
    }
  }
  
  public void buildUp() throws IOException {
    INode subtreeRoot = readSubtreeRoot();
    if (!subtreeRoot.isDirectory()) {
      return;
    }
    
    
    collectChildren(newProjectedInode(subtreeRoot, 0), subtreeRootId.getDepth(), 2, subtreeRootDefaultEntries);
    while (true) {
      try {
        Future future = activeCollectors.poll();
        if (future == null) {
          break;
        }
        future.get();
      } catch (InterruptedException e) {
        LOG.info("FileTree builder was interrupted");
        throw new BuildingUpFileTreeFailedException(
            "Building the up the file tree was interrupted.");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          throw new RuntimeException(e.getCause());
        } else {
          // This should not happen as it is a Runnable
          String message = "FileTree.buildUp received an unexpected execution exception";
          LOG.warn( message, e);
          throw new RetriableException(message);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }
  
  protected synchronized void setExceptionIfNull(IOException e) {
    if (exception == null) {
      exception = e;
    }
  }
  
  protected abstract void addSubtreeRoot(ProjectedINode node);
  
  protected abstract void addChildNode(ProjectedINode parent, int level, ProjectedINode child);
  
  private INode readSubtreeRoot() throws IOException {
    return (INode) new LightWeightRequestHandler(
        HDFSOperationType.GET_SUBTREE_ROOT) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess<INode> dataAccess =
            (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
        // No need to acquire a lock as the locking flag was already set
        INode subtreeRoot = null;
        subtreeRoot = dataAccess.findInodeByNameParentIdAndPartitionIdPK(subtreeRootId.getName(), subtreeRootId
            .getPid(), subtreeRootId.getPartitionId());
        if (subtreeRoot == null) {
          throw new BuildingUpFileTreeFailedException(
              "Subtree root does not exist");
        }
  
        List<AclEntry> inodeOwnAclNoTransaction = INodeUtil.getInodeOwnAclNoTransaction(subtreeRoot);
        if (namesystem.isPermissionEnabled() && subAccess != null) {
          if (inodeOwnAclNoTransaction.isEmpty()){
            
            checkAccess(subtreeRoot, subAccess, asAccessEntries(subtreeRootDefaultEntries));
          } else {
            checkAccess(subtreeRoot, subAccess, inodeOwnAclNoTransaction);
          }
        }
  
        long size = 0;
        if (subtreeRoot.isFile()) {
          size = ((INodeFile) subtreeRoot).getSize();
        }
        
        ProjectedINode pin = AbstractFileTree.newProjectedInode(subtreeRoot, size);
  
        addSubtreeRoot(pin);
        return subtreeRoot;
      }
    }.handle(this);
  }
  
  private void collectChildren(ProjectedINode parent, short depth, int level, List<AclEntry> inheritedDefaults) {
    activeCollectors.add(namesystem.getSubtreeOperationsExecutor().
        submit(new ChildCollector(parent, depth, level, inheritedDefaults)));
  }
  
  /**
   * This method is for testing only! Do not rely on it.
   *
   * @param path
   *     The path of the subtree
   * @return A FileTree instance reprecenting the path
   * @throws io.hops.exception.StorageException
   * @throws org.apache.hadoop.hdfs.protocol.UnresolvedPathException
   */
  @VisibleForTesting
  static CountingFileTree createCountingFileTreeFromPath(FSNamesystem namesystem,
      String path) throws StorageException, UnresolvedPathException,
      TransactionContextException, AccessControlException {
    LinkedList<INode> nodes = new LinkedList<>();
    INodeUtil.resolvePathWithNoTransaction(path, false, nodes);
    INodeIdentifier rootId = new INodeIdentifier(
        nodes.getLast().getId(),
        nodes.getLast().getParentId(),
        nodes.getLast().getLocalName(),
        nodes.getLast().getPartitionId());
    rootId.setDepth((short) (INodeDirectory.ROOT_DIR_DEPTH + (nodes.size() - 1)));
    return new CountingFileTree(namesystem, rootId);
  }
  
  @VisibleForTesting
  static class CountingFileTree extends AbstractFileTree {
    private final AtomicLong fileCount = new AtomicLong(0);
    private final AtomicLong directoryCount = new AtomicLong(0);
    private final AtomicLong diskspaceCount = new AtomicLong(0);
    private final AtomicLong fileSizeSummary = new AtomicLong(0);
    
    public CountingFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId)
        throws AccessControlException {
      super(namesystem, subtreeRootId);
    }
    
    public CountingFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId,
        FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries) throws AccessControlException {
      super(namesystem, subtreeRootId, subAccess, subtreeRootDefaultEntries);
    }
    
    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      addNode(node);
    }
    
    @Override
    protected void addChildNode(ProjectedINode parent, int level, ProjectedINode node) {
      addNode(node);
    }
    
    protected void addNode(final ProjectedINode node) {
      if (node.isDirectory()) {
        directoryCount.addAndGet(1);
      } else if (node.isSymlink()) {
        fileCount.addAndGet(1);
      } else {
        fileCount.addAndGet(1);
        diskspaceCount.addAndGet(node.getFileSize() * INode.HeaderFormat.getReplication(node.getHeader()));
        fileSizeSummary.addAndGet(node.getFileSize());
      }
    }
    
    long getNamespaceCount() {
      return directoryCount.get() + fileCount.get();
    }
    
    long getDiskspaceCount() {
      return diskspaceCount.get();
    }
    
    long getFileSizeSummary() {
      return fileSizeSummary.get();
    }
    
    public long getFileCount() {
      return fileCount.get();
    }
    
    public long getDirectoryCount() {
      return directoryCount.get();
    }
  }
  
  @VisibleForTesting
  static class QuotaCountingFileTree extends AbstractFileTree {
    private final AtomicLong namespaceCount = new AtomicLong(0);
    private final AtomicLong diskspaceCount = new AtomicLong(0);
  
    public QuotaCountingFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId)
        throws AccessControlException {
      super(namesystem, subtreeRootId);
    }
  
    public QuotaCountingFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId,
        FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries) throws AccessControlException {
      super(namesystem, subtreeRootId, subAccess, subtreeRootDefaultEntries);
    }
  
    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      addNode(node);
    }
  
    @Override
    protected void addChildNode(ProjectedINode parent, int level, ProjectedINode node) {
      if (!parent.isDirWithQuota())
        addNode(node);
    }
  
    protected void addNode(final ProjectedINode node) {
      if (node.isDirWithQuota()) {
        LightWeightRequestHandler handler = new LightWeightRequestHandler(
            HDFSOperationType.GET_SUBTREE_ATTRIBUTES) {
          @Override
          public Object performTask() throws StorageException, IOException {
            INodeAttributesDataAccess<INodeAttributes> dataAccess =
                (INodeAttributesDataAccess) HdfsStorageFactory
                    .getDataAccess(INodeAttributesDataAccess.class);
            INodeAttributes attributes =
                dataAccess.findAttributesByPk(node.getId());
            namespaceCount.addAndGet(attributes.getNsCount());
            diskspaceCount.addAndGet(attributes.getDiskspace());
            return null;
          }
        };
  
        try {
          handler.handle();
        } catch (IOException e) {
          setExceptionIfNull(e);
        }
      } else {
        namespaceCount.addAndGet(1);
        if (!node.isDirectory() && !node.isSymlink()) {
          diskspaceCount.addAndGet(node.getFileSize() * INode.HeaderFormat.getReplication(node.getHeader()));
        }
      }
    }
  
    long getNamespaceCount() {
      return namespaceCount.get();
    }
  
    long getDiskspaceCount() {
      return diskspaceCount.get();
    }
  
  }
    static class LoggingQuotaCountingFileTree extends QuotaCountingFileTree {
      private ConcurrentLinkedQueue<MetadataLogEntry> metadataLogEntries =
          new ConcurrentLinkedQueue<>();
      private final INode srcDataset;
      private final INode dstDataset;
      
      public LoggingQuotaCountingFileTree(
          FSNamesystem namesystem, INodeIdentifier subtreeRootId, INode srcDataset,
          INode dstDataset) throws AccessControlException {
        super(namesystem, subtreeRootId);
        this.srcDataset = srcDataset;
        this.dstDataset = dstDataset;
      }
      
      public LoggingQuotaCountingFileTree(
          FSNamesystem namesystem, INodeIdentifier subtreeRootId,
          FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries, INode srcDataset,
          INode dstDataset) throws AccessControlException {
        super(namesystem, subtreeRootId, subAccess, subtreeRootDefaultEntries);
        this.srcDataset = srcDataset;
        this.dstDataset = dstDataset;
      }
      
      @Override
      protected void addChildNode(ProjectedINode parent, int level, ProjectedINode node) {
        if(srcDataset == null){
          if(dstDataset == null){
            //Do nothing as non of the directories are metaEnabled
          }else{
            //Moving a non metaEnabled directory under a metaEnabled directory
            metadataLogEntries.add(new MetadataLogEntry(dstDataset.getId(),
                node.getId(), node.getPartitionId(), node.getParentId(), node
                .getName(), node.incrementLogicalTime(), MetadataLogEntry.Operation
                .ADD));
          }
        }else{
          if(dstDataset == null){
            //rename a metadateEnabled directory to a non metadataEnabled
            // directory
            metadataLogEntries.add(new MetadataLogEntry(srcDataset.getId(),
                node.getId(), node.getPartitionId(), node.getParentId(), node
                .getName(), node.incrementLogicalTime(), MetadataLogEntry.Operation
                .DELETE));
          }else{
            //Move from one dataset to another
            metadataLogEntries.add(new MetadataLogEntry(dstDataset.getId(),
                node.getId(), node.getPartitionId(), node.getParentId(), node
                .getName(), node.incrementLogicalTime(), MetadataLogEntry
                .Operation.CHANGEDATASET));
          }
        }
        
        super.addChildNode(parent, level, node);
      }
      
      public Collection<MetadataLogEntry> getMetadataLogEntries() {
        return metadataLogEntries;
      }
      
      static void updateLogicalTime(final Collection<MetadataLogEntry> logEntries)
          throws IOException {
        new LightWeightRequestHandler(HDFSOperationType.UPDATE_LOGICAL_TIME) {
          @Override
          public Object performTask() throws IOException {
            INodeDataAccess<INode> dataAccess =
                (INodeDataAccess) HdfsStorageFactory
                    .getDataAccess(INodeDataAccess.class);
            dataAccess.updateLogicalTime(logEntries);
            return null;
          }
        }.handle();
      }
    }
    
    @VisibleForTesting
    public static class FileTree extends AbstractFileTree {
      public static final int ROOT_LEVEL = 1;
      
      private final SetMultimap<Integer, ProjectedINode> inodesByParent;
      private final SetMultimap<Integer, ProjectedINode> inodesByLevel;
      private final SetMultimap<Integer, ProjectedINode> dirsByLevel;
      private final ConcurrentHashMap<Integer, ProjectedINode> inodesById =
          new ConcurrentHashMap<>();
      
      public FileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId)
          throws AccessControlException {
        this(namesystem, subtreeRootId, null, null);
      }
      
      public FileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId,
          FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries) throws AccessControlException {
        super(namesystem, subtreeRootId, subAccess, subtreeRootDefaultEntries);
        HashMultimap<Integer, ProjectedINode> parentMap = HashMultimap.create();
        inodesByParent = Multimaps.synchronizedSetMultimap(parentMap);
        HashMultimap<Integer, ProjectedINode> levelMap = HashMultimap.create();
        inodesByLevel = Multimaps.synchronizedSetMultimap(levelMap);
        HashMultimap<Integer, ProjectedINode> dirsLevelMap = HashMultimap.create();
        dirsByLevel = Multimaps.synchronizedSetMultimap(dirsLevelMap);
      }
      
      
      @Override
      protected void addSubtreeRoot(ProjectedINode node) {
        inodesByLevel.put(ROOT_LEVEL, node);
        inodesById.put(node.getId(), node);
        dirsByLevel.put(ROOT_LEVEL, node);
      }
      
      @Override
      protected void addChildNode(ProjectedINode parent, int level, ProjectedINode node) {
        inodesByParent.put(parent.getId(), node);
        inodesByLevel.put(level, node);
        inodesById.put(node.getId(), node);
        if (node.isDirectory()) {
          dirsByLevel.put(level, node);
        }
      }
      
      public Collection<ProjectedINode> getAll() {
        return inodesByLevel.values();
      }
      
      public Set<Integer> getAllINodesIds() {
        return inodesById.keySet();
      }
      
      public Collection<ProjectedINode> getAllChildren() {
        return inodesByParent.values();
      }
      
      public ProjectedINode getSubtreeRoot() {
        return inodesByLevel.get(ROOT_LEVEL).iterator().next();
      }
      
      public int getHeight() {
        return inodesByLevel.keySet().size();
      }
      
      public Collection<ProjectedINode> getChildren(int inodeId) {
        return inodesByParent.get(inodeId);
      }
      
      public Collection<ProjectedINode> getInodesByLevel(int level) {
        return inodesByLevel.get(level);
      }
      
      public Collection<ProjectedINode> getDirsByLevel(int level) {
        return dirsByLevel.get(level);
      }
      
      public int countChildren(int inodeId) {
        return getChildren(inodeId).size();
      }
      
      public ProjectedINode getInodeById(int id) {
        return inodesById.get(id);
      }
      
      public boolean isNonEmptyDirectory() {
        return getSubtreeRoot().isDirectory() &&
            !getChildren(getSubtreeRoot().getId()).isEmpty();
      }
      
      public String createAbsolutePath(String subtreeRootPath,
          ProjectedINode inode) {
        StringBuilder builder = new StringBuilder();
        while (inode.equals(getSubtreeRoot()) == false) {
          builder.insert(0, inode.getName());
          builder.insert(0, "/");
          inode = getInodeById(inode.getParentId());
        }
        builder.insert(0, subtreeRootPath);
        return builder.toString();
      }
    }
    
    static class IdCollectingCountingFileTree extends CountingFileTree {
      private LinkedList<Integer> ids = new LinkedList<>();
      private List<Integer> synchronizedList = Collections.synchronizedList(ids);
      
      public IdCollectingCountingFileTree(FSNamesystem namesystem, INodeIdentifier subtreeRootId)
          throws AccessControlException {
        super(namesystem, subtreeRootId);
      }
      
      public IdCollectingCountingFileTree(FSNamesystem namesystem,
          INodeIdentifier subtreeRootId, FsAction subAccess, List<AclEntry> subtreeRootDefaultEntries) throws
          AccessControlException {
        super(namesystem, subtreeRootId, subAccess, subtreeRootDefaultEntries);
      }
      
      @Override
      protected void addSubtreeRoot(ProjectedINode node) {
        synchronizedList.add(node.getId());
        super.addSubtreeRoot(node);
      }
      
      @Override
      protected void addChildNode(ProjectedINode parent, int level, ProjectedINode node) {
        synchronizedList.add(node.getId());
        super.addChildNode(parent, level, node);
      }
      
      /**
       * @return A list that guarantees to includes parents before their children.
       */
      public LinkedList<Integer> getOrderedIds() {
        return ids;
      }
    }
    
    /**
     * This method id for testing only! Do not rely on it.
     *
     * @param path
     *     The path of the subtree
     * @return A FileTree instance reprecenting the path
     * @throws io.hops.exception.StorageException
     * @throws UnresolvedPathException
     */
    @VisibleForTesting
    static FileTree createFileTreeFromPath(FSNamesystem namesystem, String path)
        throws StorageException, UnresolvedPathException,
        TransactionContextException, AccessControlException {
      LinkedList<INode> nodes = new LinkedList<>();
      INodeUtil.resolvePathWithNoTransaction(path, false, nodes );
      INodeIdentifier rootId = new INodeIdentifier(
          nodes.getLast().getId(),
          nodes.getLast().getParentId(),
          nodes.getLast().getLocalName(),
          nodes.getLast().getPartitionId());
      rootId.setDepth((short) (INodeDirectory.ROOT_DIR_DEPTH + (nodes.size() - 1)));
      return new FileTree(namesystem, rootId);
    }
    
  
  private static List<AclEntry> asAccessEntries(List<AclEntry> defaults) {
    List<AclEntry> accessEntries = new ArrayList<>();
    for (AclEntry defaultEntry : defaults) {
      accessEntries.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS).setType(defaultEntry.getType()).setName
          (defaultEntry.getName()).setPermission(defaultEntry.getPermission()).build());
    }
    return accessEntries;
  }
  
  private static List<AclEntry> filterAccessEntries(List<AclEntry> maybeAccess){
      if (maybeAccess == null){
        return new ArrayList<>();
      }
      List<AclEntry> onlyDefaults = new ArrayList<>();
      for (AclEntry entry : maybeAccess){
        if (entry.getScope().equals(AclEntryScope.DEFAULT)) {
          onlyDefaults.add(entry);
        }
      }
      return onlyDefaults;
  }
  private static List<AclEntry> filterDefaultEntries(List<AclEntry> maybeDefault){
      if (maybeDefault == null){
        return new ArrayList<>();
      }
      List<AclEntry> onlyDefaults = new ArrayList<>();
      for (AclEntry entry : maybeDefault){
        if (entry.getScope().equals(AclEntryScope.ACCESS)) {
          onlyDefaults.add(entry);
        }
      }
      return onlyDefaults;
  }
  
  private static ProjectedINode newProjectedInode(INode from, long size) {
    boolean dirWithQuota = false;
    if (from instanceof INodeDirectory && ((INodeDirectory) from).isWithQuota()) {
      dirWithQuota = true;
    }
    ProjectedINode result = new ProjectedINode(from.getId(),
        from.getParentId(),
        from.getLocalName(),
        from.getPartitionId(),
        from instanceof INodeDirectory,
        from.getFsPermissionShort(),
        from.getUserID(),
        from.getGroupID(),
        from.getHeader(),
        from.isSymlink(),
        dirWithQuota,
        from.isUnderConstruction(),
        from.isSTOLocked(),
        from.getSTOLockOwner(),
        size,
        from.getLogicalTime(),
        from.getLocalStoragePolicyID(),
        from.getNumAces());
    return result;
  }
}

