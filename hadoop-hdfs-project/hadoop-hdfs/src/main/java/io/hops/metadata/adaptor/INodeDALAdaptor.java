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
import io.hops.metadata.hdfs.entity.ProjectedINode;
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
  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockPPIS(
          int parentId, int partitionId) throws StorageException {
    List<ProjectedINode> list =
            dataAccess.findInodesForSubtreeOperationsWithWriteLockPPIS(parentId, partitionId);
    Collections.sort(list);
    return list;
  }

  @Override
  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockFTIS(
      int parentId) throws StorageException {
    List<ProjectedINode> list =
        dataAccess.findInodesForSubtreeOperationsWithWriteLockFTIS(parentId);
    Collections.sort(list);
    return list;
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
    List<org.apache.hadoop.hdfs.server.namenode.INode> list =
        (List) convertDALtoHDFS(
            dataAccess.allINodes());
    return list;
  }

  @Override
  public INode convertHDFStoDAL(
      org.apache.hadoop.hdfs.server.namenode.INode inode)
      throws StorageException {
    INode hopINode = null;
    if (inode != null) {
      hopINode = new INode();
      hopINode.setModificationTime(inode.getModificationTime());
      hopINode.setAccessTime(inode.getAccessTime());
      hopINode.setName(inode.getLocalName());

      hopINode.setUserID(inode.getUserID());
      hopINode.setGroupID(inode.getGroupID());
      hopINode.setPermission(inode.getFsPermission().toShort());
      hopINode.setParentId(inode.getParentId());
      hopINode.setId(inode.getId());
      hopINode.setIsDir(inode.isDirectory());
      hopINode.setPartitionId(inode.getPartitionId());

      if (inode.isDirectory()) {
        hopINode.setUnderConstruction(false);
        hopINode.setDirWithQuota(inode instanceof INodeDirectoryWithQuota ?
            true : false);
        hopINode.setMetaEnabled(((INodeDirectory) inode).isMetaEnabled());
      }
      if (inode instanceof INodeFile) {
        hopINode
            .setUnderConstruction(inode.isUnderConstruction() ? true : false);
        hopINode.setDirWithQuota(false);
        if (inode instanceof INodeFileUnderConstruction) {
          hopINode.setClientName(
              ((INodeFileUnderConstruction) inode).getClientName());
          hopINode.setClientMachine(
              ((INodeFileUnderConstruction) inode).getClientMachine());
          hopINode.setClientNode(
              ((INodeFileUnderConstruction) inode).getClientNode() == null ?
                  null : ((INodeFileUnderConstruction) inode).getClientNode()
                  .getXferAddr());
        }
        hopINode.setGenerationStamp(((INodeFile) inode).getGenerationStamp());
        hopINode.setFileSize(((INodeFile) inode).getSize());
      }
      if (inode instanceof INodeSymlink) {
        hopINode.setUnderConstruction(false);
        hopINode.setDirWithQuota(false);

        String linkValue =
            DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
        hopINode.setSymlink(linkValue);
      }
      hopINode.setSubtreeLocked(inode.isSubtreeLocked());
      hopINode.setSubtreeLockOwner(inode.getSubtreeLockOwner());
    }
    hopINode.setHeader(inode.getHeader());
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
            inode = new INodeDirectoryWithQuota(hopINode.getName(), ps);
          } else {
            String iname =
                (hopINode.getName().length() == 0) ? INodeDirectory.ROOT_NAME :
                    hopINode.getName();
            inode = new INodeDirectory(iname, ps);
          }

          inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          inode.setModificationTimeNoPersistance(hopINode.getModificationTime());
          ((INodeDirectory) inode).setMetaEnabled(hopINode.isMetaEnabled());
        } else if (hopINode.getSymlink() != null) {
          inode = new INodeSymlink(hopINode.getSymlink(),
              hopINode.getModificationTime(), hopINode.getAccessTime(), ps);
        } else {
          if (hopINode.isUnderConstruction()) {
            DatanodeID dnID = (hopINode.getClientNode() == null ||
                hopINode.getClientNode().isEmpty()) ? null :
                new DatanodeID(hopINode.getClientNode());

            inode = new INodeFileUnderConstruction(ps,
                INodeFile.getBlockReplication(hopINode.getHeader()),
                INodeFile.getPreferredBlockSize(hopINode.getHeader()),
                hopINode.getModificationTime(), hopINode.getClientName(),
                hopINode.getClientMachine(), dnID);

            inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          } else {
            inode = new INodeFile(ps, hopINode.getHeader(),
                hopINode.getModificationTime(), hopINode.getAccessTime());
          }
          ((INodeFile) inode).setGenerationStampNoPersistence(
              hopINode.getGenerationStamp());
          ((INodeFile) inode).setSizeNoPersistence(hopINode.getFileSize());
          ((INodeFile) inode).setHasBlocksNoPersistance(INodeFile.hasBlocks(hopINode.getHeader()));
        }
        inode.setIdNoPersistance(hopINode.getId());
        inode.setLocalNameNoPersistance(hopINode.getName());
        inode.setParentIdNoPersistance(hopINode.getParentId());
        inode.setSubtreeLocked(hopINode.isSubtreeLocked());
        inode.setSubtreeLockOwner(hopINode.getSubtreeLockOwner());
        inode.setUserIDNoPersistance(hopINode.getUserID());
        inode.setGroupIDNoPersistance(hopINode.getGroupID());
        inode.setHeaderNoPersistance(hopINode.getHeader());
        inode.setPartitionIdNoPersistance(hopINode.getPartitionId());
      }
      return inode;
    }catch (IOException ex){
      throw new StorageException(ex);
    }
  }
}
