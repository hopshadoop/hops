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
package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.transaction.context.EntityContext;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class INodeDALAdaptor
    extends DalAdaptor<org.apache.hadoop.hdfs.server.namenode.INode, INode>
    implements INodeDataAccess<org.apache.hadoop.hdfs.server.namenode.INode> {

  private INodeDataAccess<INode> dataAccess;

  public INodeDALAdaptor(INodeDataAccess<INode> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.INode findInodeByIdFTIS(
      int inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findInodeByIdFTIS(inodeId));
  }

  @Override
  public Collection<org.apache.hadoop.hdfs.server.namenode.INode> findInodesByIdsFTIS(int[] inodeId) throws
      StorageException {
    return convertDALtoHDFS(dataAccess.findInodesByIdsFTIS(inodeId));
  }
  
  @Override
  public List<org.apache.hadoop.hdfs.server.namenode.INode> findInodesByParentIdFTIS(
      int parentId) throws StorageException {
    List<org.apache.hadoop.hdfs.server.namenode.INode> list =
        (List) convertDALtoHDFS(
            dataAccess.findInodesByParentIdFTIS(parentId));
    Collections
        .sort(list, org.apache.hadoop.hdfs.server.namenode.INode.Order.ByName);
    return list;
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.namenode.INode> findInodesByParentIdAndPartitionIdPPIS(
          int parentId, int partitionId) throws StorageException {
    List<org.apache.hadoop.hdfs.server.namenode.INode> list =
            (List) convertDALtoHDFS(
                    dataAccess.findInodesByParentIdAndPartitionIdPPIS(parentId, partitionId));
    Collections
            .sort(list, org.apache.hadoop.hdfs.server.namenode.INode.Order.ByName);
    return list;
  }

  @Override
  public List<ProjectedINode> findInodesPPISTx(
          int parentId, int partitionId, EntityContext.LockMode lock) throws StorageException {
    List<ProjectedINode> list =
            dataAccess.findInodesPPISTx(parentId, partitionId, lock);
    Collections.sort(list);
    return list;
  }

  @Override
  public List<ProjectedINode> findInodesFTISTx(
      int parentId, EntityContext.LockMode lock) throws StorageException {
    List<ProjectedINode> list =
        dataAccess.findInodesFTISTx(parentId, lock);
    Collections.sort(list);
    return list;
  }

  public List<org.apache.hadoop.hdfs.server.namenode.INode> lockInodesUsingPkBatchTx(
          String[] names, int[] parentIds, int[] partitionIds, EntityContext.LockMode lock)
          throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.namenode.INode>) convertDALtoHDFS(
            dataAccess.lockInodesUsingPkBatchTx(names, parentIds, partitionIds, lock));
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.INode findInodeByNameParentIdAndPartitionIdPK(
      String name, int parentId, int partitionId) throws StorageException {
    return convertDALtoHDFS(
        dataAccess.findInodeByNameParentIdAndPartitionIdPK(name, parentId, partitionId));
  }

  @Override
  public List<org.apache.hadoop.hdfs.server.namenode.INode> getINodesPkBatched(
      String[] names, int[] parentIds, int[] partitionIds) throws StorageException {
    return (List<org.apache.hadoop.hdfs.server.namenode.INode>) convertDALtoHDFS(
        dataAccess.getINodesPkBatched(names, parentIds, partitionIds));
  }

  @Override
  public void prepare(
      Collection<org.apache.hadoop.hdfs.server.namenode.INode> removed,
      Collection<org.apache.hadoop.hdfs.server.namenode.INode> newed,
      Collection<org.apache.hadoop.hdfs.server.namenode.INode> modified)
      throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed),
        convertHDFStoDAL(modified));
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();

  }

  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId)
      throws StorageException {
    return dataAccess.getAllINodeFiles(startId, endId);
  }
  
  @Override
  public boolean haveFilesWithIdsGreaterThan(long id) throws StorageException {
    return dataAccess.haveFilesWithIdsGreaterThan(id);
  }
  
  @Override
  public boolean haveFilesWithIdsBetween(long startId, long endId)
      throws StorageException {
    return dataAccess.haveFilesWithIdsBetween(startId, endId);
  }
  
  @Override
  public long getMinFileId() throws StorageException {
    return dataAccess.getMinFileId();
  }

  @Override
  public long getMaxFileId() throws StorageException {
    return dataAccess.getMaxFileId();
  }

  @Override
  public int countAllFiles() throws StorageException {
    return dataAccess.countAllFiles();
  }
  
  @Override
  public boolean hasChildren(int parentId, boolean areChildrenRandomlyPartitioned ) throws StorageException {
    return dataAccess.hasChildren(parentId, areChildrenRandomlyPartitioned);
  }
  
  //Only for testing
  @Override
  public List<org.apache.hadoop.hdfs.server.namenode.INode> allINodes() throws StorageException {
    return (List) convertDALtoHDFS(dataAccess.allINodes());
  }

  @Override
  public void updateLogicalTime(Collection<MetadataLogEntry> logEntries)
      throws StorageException {
    dataAccess.updateLogicalTime(logEntries);
  }

  @Override
  public INode convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.namenode.INode inode)
      throws StorageException {
    INode hopINode = null;
    if (inode != null) {
      hopINode = new INode();
      hopINode.setAccessTime(inode.getAccessTime());
      hopINode.setGroupID(inode.getGroupID());
      hopINode.setId(inode.getId());
      hopINode.setIsDir(inode.isDirectory());
      hopINode.setPartitionId(inode.getPartitionId());
      hopINode.setLogicalTime(inode.getLogicalTime());
      hopINode.setModificationTime(inode.getModificationTime());
      hopINode.setName(inode.getLocalName());
      hopINode.setParentId(inode.getParentId());
      hopINode.setPermission(inode.getFsPermission().toShort());
      if(!inode.isSymlink()){
        hopINode.setStoragePolicy(inode.getLocalStoragePolicyID());
      }
      hopINode.setSubtreeLocked(inode.isSubtreeLocked());
      hopINode.setSubtreeLockOwner(inode.getSubtreeLockOwner());
      hopINode.setUserID(inode.getUserID());

      if (inode.isDirectory()) {
        hopINode.setUnderConstruction(false);
        hopINode.setDirWithQuota(inode instanceof INodeDirectoryWithQuota);
        hopINode.setMetaEnabled(((INodeDirectory) inode).isMetaEnabled());
        hopINode.setChildrenNum(((INodeDirectory) inode).getChildrenNum());
      }
      if (inode instanceof INodeFile) {
        if (inode instanceof INodeFileUnderConstruction) {
          INodeFileUnderConstruction infuc = (INodeFileUnderConstruction) inode;
          hopINode.setClientName(infuc.getClientName());
          hopINode.setClientMachine(infuc.getClientMachine());
          hopINode.setClientNode(infuc.getClientNode() == null ? null : infuc.getClientNode().getXferAddr());
        }
        hopINode.setUnderConstruction(inode.isUnderConstruction());
        hopINode.setDirWithQuota(false);
        hopINode.setHeader(((INodeFile) inode).getHeader());
        hopINode.setGenerationStamp(((INodeFile) inode).getGenerationStamp());
        hopINode.setFileSize(((INodeFile) inode).getSize());
        hopINode.setFileStoredInDB(((INodeFile)inode).isFileStoredInDB());
      }
      if (inode instanceof INodeSymlink) {
        hopINode.setUnderConstruction(false);
        hopINode.setDirWithQuota(false);

        String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
        hopINode.setSymlink(linkValue);
      }
    }
    hopINode.setHeader(inode.getHeader());
    hopINode.setNumAces(inode.getNumAces());
    return hopINode;
  }

  @Override
  public org.apache.hadoop.hdfs.server.namenode.INode convertDALtoHDFS(
      INode hopINode) throws StorageException {
    try{
      org.apache.hadoop.hdfs.server.namenode.INode inode = null;
      if (hopINode != null) {
        PermissionStatus ps = new PermissionStatus(null, null, new FsPermission
            (hopINode.getPermission()));
        if (hopINode.isDirectory()) {
          if (hopINode.isDirWithQuota()) {
            inode = new INodeDirectoryWithQuota(hopINode.getId(), hopINode.getName(), ps, true);
          } else {
            String iname = (hopINode.getName().length() == 0) ? INodeDirectory.ROOT_NAME : hopINode.getName();
            inode = new INodeDirectory(hopINode.getId(), iname, ps, true);
          }

          inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          inode.setModificationTimeNoPersistance(hopINode.getModificationTime());
          ((INodeDirectory) inode).setMetaEnabled(hopINode.isMetaEnabled());
          ((INodeDirectory) inode).setChildrenNum(hopINode.getChildrenNum());
        } else if (hopINode.getSymlink() != null) {
          inode = new INodeSymlink(hopINode.getId(), hopINode.getSymlink(),
              hopINode.getModificationTime(), hopINode.getAccessTime(), ps, true);
        } else {
          if (hopINode.isUnderConstruction()) {
            DatanodeID dnID = (hopINode.getClientNode() == null || hopINode.getClientNode().isEmpty()) ? null
                : new DatanodeID(hopINode.getClientNode());
            inode = new INodeFileUnderConstruction(hopINode.getId(), ps,
                org.apache.hadoop.hdfs.server.namenode.INode.HeaderFormat.getReplication(hopINode.getHeader()),
                org.apache.hadoop.hdfs.server.namenode.INode.HeaderFormat.getPreferredBlockSize(hopINode.getHeader()),
                hopINode.getModificationTime(), hopINode.getClientName(),
                hopINode.getClientMachine(), dnID, hopINode.getStoragePolicy(), true);

            inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          } else {
            inode = new INodeFile(hopINode.getId(), ps, hopINode.getHeader(),
                hopINode.getModificationTime(), hopINode.getAccessTime(), hopINode.isFileStoredInDB(),
                hopINode.getStoragePolicy(), true);
          }
          ((INodeFile) inode).setGenerationStampNoPersistence(
              hopINode.getGenerationStamp());
          ((INodeFile) inode).setSizeNoPersistence(hopINode.getFileSize());
          
          ((INodeFile) inode).setFileStoredInDBNoPersistence(hopINode.isFileStoredInDB());
        }
        inode.setLocalNameNoPersistance(hopINode.getName());
        inode.setParentIdNoPersistance(hopINode.getParentId());
        inode.setSubtreeLocked(hopINode.isSubtreeLocked());
        inode.setSubtreeLockOwner(hopINode.getSubtreeLockOwner());
        inode.setUserIDNoPersistance(hopINode.getUserID());
        inode.setGroupIDNoPersistance(hopINode.getGroupID());
        inode.setHeaderNoPersistance(hopINode.getHeader());
        inode.setPartitionIdNoPersistance(hopINode.getPartitionId());
        inode.setLogicalTimeNoPersistance(hopINode.getLogicalTime());
        inode.setBlockStoragePolicyIDNoPersistance(hopINode.getStoragePolicy());
        inode.setNumAcesNoPersistence(hopINode.getNumAces());
      }
      return inode;
    } catch (IOException ex) {
      throw new StorageException(ex);
    }
  }
}
