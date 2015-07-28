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
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.SubtreeLockHelper;
import io.hops.transaction.lock.SubtreeLockedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@VisibleForTesting
abstract class AbstractFileTree {
  public static final Log LOG = LogFactory.getLog(AbstractFileTree.class);

  private final FSNamesystem namesystem;
  private final int subtreeRootId;
  private final List<Future> activeCollectors = new ArrayList<Future>();
  private final FsAction subAccess;
  private volatile IOException exception;

  public static class BuildingUpFileTreeFailedException extends IOException {

    public BuildingUpFileTreeFailedException() {
    }

    public BuildingUpFileTreeFailedException(String message) {
      super(message);
    }

    public BuildingUpFileTreeFailedException(String message, Throwable cause) {
      super(message, cause);
    }

    public BuildingUpFileTreeFailedException(Throwable cause) {
      super(cause);
    }
  }

  private class ChildCollector implements Runnable {
    private final int parentId;
    private final int level;
    private boolean quotaEnabledBranch;

    private ChildCollector(int parentId, int level,
        boolean quotaEnabledBranch) {
      this.parentId = parentId;
      this.level = level;
      this.quotaEnabledBranch = quotaEnabledBranch;
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
              List<ProjectedINode> children = dataAccess
                  .findInodesForSubtreeOperationsWithWriteLock(parentId);
              for (ProjectedINode child : children) {
                if (namesystem.isPermissionEnabled() && subAccess != null) {
                  checkAccess(child, subAccess);
                }
                addChildNode(level, child, quotaEnabledBranch);
              }

              if (exception != null) {
                return null;
              }

              for (ProjectedINode inode : children) {
                List<ActiveNode> activeNamenodes = namesystem.getNameNode().
                    getActiveNameNodes().getActiveNodes();
                if (SubtreeLockHelper.isSubtreeLocked(inode.isSubtreeLocked(),
                    inode.getSubtreeLockOwner(), activeNamenodes)) {
                  exception = new SubtreeLockedException(inode.getName(),
                      activeNamenodes);
                  return null;
                }
                if (inode.isDirectory()) {
                  synchronized (activeCollectors) {
                    collectChildren(inode.getId(), level + 1,
                        inode.isDirWithQuota());
                  }
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
  }

  public AbstractFileTree(FSNamesystem namesystem, int subtreeRootId) {
    this(namesystem, subtreeRootId, null);
  }

  public AbstractFileTree(FSNamesystem namesystem, int subtreeRootId,
      FsAction subAccess) {
    this.namesystem = namesystem;
    this.subtreeRootId = subtreeRootId;
    this.subAccess = subAccess;
  }

  private void checkAccess(INode node, FsAction action)
      throws AccessControlException {
    FSPermissionChecker pc = namesystem.getPermissionChecker();
    if (!pc.isSuperUser() && node.isDirectory()) {
      pc.check(node, action);
    }
  }

  private void checkAccess(ProjectedINode node, FsAction action)
      throws IOException {
    FSPermissionChecker pc = namesystem.getPermissionChecker();
    if (!pc.isSuperUser() && node.isDirectory()) {
      pc.check(node, action);
    }
  }

  public int getSubtreeRootId() {
    return subtreeRootId;
  }

  public void buildUp() throws IOException {
    INode subtreeRoot = readSubtreeRoot();
    if (subtreeRoot.isDirectory() == false) {
      return;
    }

    boolean quotaEnabled =
        subtreeRoot instanceof INodeDirectoryWithQuota ? true : false;
    collectChildren(subtreeRootId, 2, quotaEnabled);
    while (true) {
      Future future;
      synchronized (activeCollectors) {
        if (activeCollectors.size() == 0) {
          break;
        }
        future = activeCollectors.remove(0);
      }
      try {
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
          LOG.warn(
              "FileTree.buildUp received an unexpected execution exception",
              e);
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

  protected abstract void addChildNode(int level, ProjectedINode node,
      boolean quotaEnabledBranch);

  private INode readSubtreeRoot() throws IOException {
    return (INode) new LightWeightRequestHandler(
        HDFSOperationType.GET_SUBTREE_ROOT) {
      @Override
      public Object performTask() throws StorageException, IOException {
        INodeDataAccess<INode> dataAccess =
            (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
        // No need to acquire a lock as the locking flag was already set
        INode subtreeRoot = dataAccess.indexScanfindInodeById(subtreeRootId);
        if (subtreeRoot == null) {
          throw new BuildingUpFileTreeFailedException(
              "Subtree root does not exist");
        }

        if (namesystem.isPermissionEnabled() && subAccess != null) {
          checkAccess(subtreeRoot, subAccess);
        }

        DataOutputBuffer permissions = new DataOutputBuffer();
        subtreeRoot.getPermissionStatus().write(permissions);
        long size = 0;
        if(subtreeRoot.isFile()){
            size = ((INodeFile)subtreeRoot).getSize();
        }

        addSubtreeRoot(
            new ProjectedINode(subtreeRoot.getId(), subtreeRoot.getParentId(),
                subtreeRoot.getLocalName(), permissions.getData(),
                subtreeRoot instanceof INodeFile ?
                    ((INodeFile) subtreeRoot).getHeader() : 0,
                subtreeRoot.isSymlink(),
                subtreeRoot instanceof INodeDirectoryWithQuota ? true : false,
                subtreeRoot.isUnderConstruction(),
                subtreeRoot.isSubtreeLocked(),
                subtreeRoot.getSubtreeLockOwner(),size));
        return subtreeRoot;
      }
    }.handle(this);
  }

  private void collectChildren(int parentId, int level,
      boolean quotaEnabledBranch) {
    activeCollectors.add(namesystem.getSubtreeOperationsExecutor().
        submit(new ChildCollector(parentId, level, quotaEnabledBranch)));
  }

  /**
   * This method id for testing only! Do not rely on it.
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
      TransactionContextException {
    LinkedList<INode> nodes = new LinkedList<INode>();
    boolean[] fullyResovled = new boolean[1];
    INodeUtil.resolvePathWithNoTransaction(path, false, nodes, fullyResovled);
    return new CountingFileTree(namesystem, nodes.getLast().getId());
  }

  @VisibleForTesting
  static class CountingFileTree extends AbstractFileTree {
    private final AtomicLong fileCount = new AtomicLong(0);
    private final AtomicLong directoryCount = new AtomicLong(0);
    private final AtomicLong diskspaceCount = new AtomicLong(0);
    private final AtomicLong fileSizeSummary = new AtomicLong(0);

    public CountingFileTree(FSNamesystem namesystem, int subtreeRootId) {
      this(namesystem, subtreeRootId, null);
    }

    public CountingFileTree(FSNamesystem namesystem, int subtreeRootId,
        FsAction subAccess) {
      super(namesystem, subtreeRootId, subAccess);
    }

    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      addNode(node);
    }

    @Override
    protected void addChildNode(int level, ProjectedINode node,
        boolean quotaEnabledBranch) {
      addNode(node);
    }

    protected void addNode(final ProjectedINode node) {
      if (node.isDirectory()) {
        directoryCount.addAndGet(1);
      } else if (node.isSymlink()) {
        fileCount.addAndGet(1);
      } else {
        fileCount.addAndGet(1);
        diskspaceCount.addAndGet(node.getFileSize()*INodeFile.extractBlockReplication(node.getHeader()));
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

    public QuotaCountingFileTree(FSNamesystem namesystem, int subtreeRootId) {
      super(namesystem, subtreeRootId);
    }

    public QuotaCountingFileTree(FSNamesystem namesystem, int subtreeRootId,
        FsAction subAccess) {
      super(namesystem, subtreeRootId, subAccess);
    }

    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      addNode(node);
    }

    @Override
    protected void addChildNode(int level, ProjectedINode node,
        boolean quotaEnabledBranch) {
      if (!quotaEnabledBranch) {
        addNode(node);
      }
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
          diskspaceCount.addAndGet(node.getFileSize()* INodeFile.extractBlockReplication(node.getHeader()));
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
    private LinkedList<MetadataLogEntry> metadataLogEntries =
        new LinkedList<MetadataLogEntry>();
    private final INode srcDataset;
    private final INode dstDataset;
    public LoggingQuotaCountingFileTree(
        FSNamesystem namesystem, int subtreeRootId, INode srcDataset,
        INode dstDataset) {
      super(namesystem, subtreeRootId);
      this.srcDataset = srcDataset;
      this.dstDataset = dstDataset;
    }

    public LoggingQuotaCountingFileTree(
        FSNamesystem namesystem, int subtreeRootId,
        FsAction subAccess, INode srcDataset,
        INode dstDataset) {
      super(namesystem, subtreeRootId, subAccess);
      this.srcDataset = srcDataset;
      this.dstDataset = dstDataset;
    }

    @Override
    protected void addChildNode(int level, ProjectedINode node,
        boolean quotaEnabledBranch) {
      if (srcDataset != null) {
        metadataLogEntries.add(new MetadataLogEntry(srcDataset.getId(),
            node.getId(), MetadataLogEntry.Operation.DELETE));
      }
      if (dstDataset != null) {
        metadataLogEntries.add(new MetadataLogEntry(dstDataset.getId(),
            node.getId(), MetadataLogEntry.Operation.ADD));
      }
      super.addChildNode(level, node, quotaEnabledBranch);
    }

    public Collection<MetadataLogEntry> getMetadataLogEntries() {
      return metadataLogEntries;
    }
  }

  @VisibleForTesting
  static class FileTree extends AbstractFileTree {
    public static final int ROOT_LEVEL = 1;

    private final SetMultimap<Integer, ProjectedINode> inodesByParent;
    private final SetMultimap<Integer, ProjectedINode> inodesByLevel;
    private final ConcurrentHashMap<Integer, ProjectedINode> inodesById =
        new ConcurrentHashMap<Integer, ProjectedINode>();

    public FileTree(FSNamesystem namesystem, int subtreeRootId) {
      this(namesystem, subtreeRootId, null);
    }

    public FileTree(FSNamesystem namesystem, int subtreeRootId,
        FsAction subAccess) {
      super(namesystem, subtreeRootId, subAccess);
      HashMultimap<Integer, ProjectedINode> parentMap = HashMultimap.create();
      inodesByParent = Multimaps.synchronizedSetMultimap(parentMap);
      HashMultimap<Integer, ProjectedINode> levelMap = HashMultimap.create();
      inodesByLevel = Multimaps.synchronizedSetMultimap(levelMap);
    }

    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      inodesByLevel.put(ROOT_LEVEL, node);
      inodesById.put(node.getId(), node);
    }

    @Override
    protected void addChildNode(int level, ProjectedINode node,
        boolean quotaEnabledBranch) {
      inodesByParent.put(node.getParentId(), node);
      inodesByLevel.put(level, node);
      inodesById.put(node.getId(), node);
    }

    public Collection<ProjectedINode> getAll() {
      return inodesByLevel.values();
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
    private LinkedList<Integer> ids = new LinkedList<Integer>();
    private List<Integer> synchronizedList = Collections.synchronizedList(ids);

    public IdCollectingCountingFileTree(FSNamesystem namesystem, int subtreeRootId) {
      super(namesystem, subtreeRootId);
    }

    public IdCollectingCountingFileTree(FSNamesystem namesystem,
        int subtreeRootId, FsAction subAccess) {
      super(namesystem, subtreeRootId, subAccess);
    }

    @Override
    protected void addSubtreeRoot(ProjectedINode node) {
      synchronizedList.add(node.getId());
      super.addSubtreeRoot(node);
    }

    @Override
    protected void addChildNode(int level, ProjectedINode node,
        boolean quotaEnabledBranch) {
      synchronizedList.add(node.getId());
      super.addChildNode(level, node, quotaEnabledBranch);
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
      TransactionContextException {
    LinkedList<INode> nodes = new LinkedList<INode>();
    boolean[] fullyResovled = new boolean[1];
    INodeUtil.resolvePathWithNoTransaction(path, false, nodes, fullyResovled);
    return new FileTree(namesystem, nodes.getLast().getId());
  }
}
