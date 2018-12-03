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
package io.hops.common;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.adaptor.INodeDALAdaptor;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.metadata.hdfs.entity.BlockLookUp;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.Slicer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAclHelper;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import static org.apache.hadoop.hdfs.server.namenode.INode.EMPTY_LIST;

public class INodeUtil {
  private final static Log LOG = LogFactory.getLog(INodeUtil.class);

  public static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  public static INode getNode(byte[] name, long parentId, long partitionId, boolean transactional)
      throws StorageException, TransactionContextException {
    String nameString = DFSUtil.bytes2String(name);
    if (transactional) {
      return EntityManager
          .find(INode.Finder.ByNameParentIdAndPartitionId, nameString, parentId, partitionId);
    } else {
      return findINodeWithNoTransaction(nameString, parentId, partitionId);
    }
  }

  public static INode getNode(long inodeId, boolean transactional)
      throws StorageException, TransactionContextException {
    if (transactional) {
      return EntityManager
          .find(INode.Finder.ByINodeIdFTIS, inodeId);
    } else {
      return findINodeWithNoTransaction(inodeId);
    }
  }
  
  private static INode findINodeWithNoTransaction(String name, long parentId, long partitionId)
      throws StorageException {
    LOG.debug(String
        .format("Read inode with no transaction by parent_id=%d, name=%s, partitionId=%s",
            parentId, name, parentId));
    INodeDataAccess<INode> da = (INodeDataAccess) HdfsStorageFactory
        .getDataAccess(INodeDataAccess.class);
    return da.findInodeByNameParentIdAndPartitionIdPK(name, parentId, partitionId);
  }
  
  private static INode findINodeWithNoTransaction(long inodeId)
      throws StorageException {
    LOG.debug(String.format("Read inode with no transaction by inodeId=%d", inodeId));
    INodeDataAccess<INode> da = (INodeDataAccess) HdfsStorageFactory
        .getDataAccess(INodeDataAccess.class);
    return da.findInodeByIdFTIS(inodeId);
  }

  public static boolean resolvePathWithNoTransaction(String path,
      boolean resolveLink, LinkedList<INode> preTxResolvedINodes)
      throws UnresolvedPathException, StorageException,
      TransactionContextException {
    preTxResolvedINodes.clear();

    byte[][] components = INode.getPathComponents(path);
    INode curNode = getRoot();
    preTxResolvedINodes.add(curNode);

    if (components.length == 1) {
      return false;
    }

    INodeResolver resolver =
        new INodeResolver(components, curNode, resolveLink, false);
    while (resolver.hasNext()) {
      curNode = resolver.next();
      if (curNode != null) {
        preTxResolvedINodes.add(curNode);
      }
    }
    return  preTxResolvedINodes.size() == components.length;
  }

  public static void findPathINodesById(long inodeId, boolean inTree,
      LinkedList<INode> preTxResolvedINodes, boolean[] isPreTxPathFullyResolved)
      throws StorageException {
    if (inTree) {
      INode inode = indexINodeScanById(inodeId);
      if (inode == null) {
        isPreTxPathFullyResolved[0] = false;
        return;
      }
      preTxResolvedINodes.add(inode);
      readFromLeafToRoot(inode, preTxResolvedINodes);
    }
    isPreTxPathFullyResolved[0] = true;
    //reverse the list
    int firstCounter = 0;
    int lastCounter = preTxResolvedINodes.size() - 1 - firstCounter;
    INode firstNode = null;
    INode lastNode = null;
    while (firstCounter < (preTxResolvedINodes.size() / 2)) {
      firstNode = preTxResolvedINodes.get(firstCounter);
      lastNode = preTxResolvedINodes.get(lastCounter);
      preTxResolvedINodes.remove(firstCounter);
      preTxResolvedINodes.add(firstCounter, lastNode);
      preTxResolvedINodes.remove(lastCounter);
      preTxResolvedINodes.add(lastCounter, firstNode);
      firstCounter++;
      lastCounter = preTxResolvedINodes.size() - 1 - firstCounter;
    }
  }

  public static Set<String> findPathsByLeaseHolder(String holder)
      throws StorageException {
    HashSet<String> paths = new HashSet<>();
    LeaseDataAccess<Lease> lda = (LeaseDataAccess) HdfsStorageFactory
        .getDataAccess(LeaseDataAccess.class);
    Lease rcLease = lda.findByPKey(holder,Lease.getHolderId(holder));
    if (rcLease == null) {
      return paths;
    }
    LeasePathDataAccess pda = (LeasePathDataAccess) HdfsStorageFactory
        .getDataAccess(LeasePathDataAccess.class);
    Collection<LeasePath> rclPaths = pda.findByHolderId(rcLease.getHolderID());
    for (LeasePath lp : rclPaths) {
      paths.add(lp.getPath());
    }
    return paths;
  }

  private static INode getRoot()
      throws StorageException, TransactionContextException {
    return getNode(INodeDirectory.ROOT_NAME.getBytes(),
        INodeDirectory.ROOT_PARENT_ID, INodeDirectory.getRootDirPartitionKey(), false);
  }

  public static INode indexINodeScanById(long id) throws StorageException {
    LOG.debug(String.format("Read inode with no transaction by id=%d", id));
    INodeDataAccess<INode> da = (INodeDataAccess) HdfsStorageFactory
        .getDataAccess(INodeDataAccess.class);
    return da.findInodeByIdFTIS(id);
  }

  //puts the indoes in the list in reverse order
  private static void readFromLeafToRoot(INode inode, LinkedList<INode> list)
      throws StorageException {
    INode temp = inode;
    while (temp != null &&
        temp.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      temp = indexINodeScanById(
          temp.getParentId()); // all upper components are dirs
      if (temp != null) {
        list.add(temp);
      }
    }
  }

  public static INodeIdentifier resolveINodeFromBlockID(final long bid)
      throws StorageException {
    INodeIdentifier inodeIdentifier;
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
        HDFSOperationType.RESOLVE_INODE_FROM_BLOCKID) {
      @Override
      public Object performTask() throws IOException {

        BlockLookUpDataAccess<BlockLookUp> da =
            (BlockLookUpDataAccess) HdfsStorageFactory
                .getDataAccess(BlockLookUpDataAccess.class);
        BlockLookUp blu = da.findByBlockId(bid);
        if (blu == null) {
          return null;
        }
        INodeIdentifier inodeIdent = new INodeIdentifier(blu.getInodeId());
        INodeDALAdaptor ida = (INodeDALAdaptor) HdfsStorageFactory
            .getDataAccess(INodeDataAccess.class);
        INode inode = ida.findInodeByIdFTIS(blu.getInodeId());
        if (inode != null) {
          inodeIdent.setName(inode.getLocalName());
          inodeIdent.setPid(inode.getParentId());
          inodeIdent.setPartitionId(inode.getPartitionId());
        }
        return inodeIdent;
      }
    };
    try {
      inodeIdentifier = (INodeIdentifier) handler.handle();
    } catch (IOException ex) {
      LOG.error("Could not resolve iNode from blockId (blockid=" + bid + ")");
      throw new StorageException(ex.getMessage());
    }
    return inodeIdentifier;
  }
  
  
  public static INodeIdentifier resolveINodeFromBlock(final Block b)
      throws StorageException {
    if (b instanceof BlockInfo || b instanceof BlockInfoUnderConstruction) {
      INodeIdentifier inodeIden =
          new INodeIdentifier(((BlockInfo) b).getInodeId());
      INodeDALAdaptor ida = (INodeDALAdaptor) HdfsStorageFactory
          .getDataAccess(INodeDataAccess.class);
      INode inode = ida.findInodeByIdFTIS(((BlockInfo) b).getInodeId());
      if (inode != null) {
        inodeIden.setName(inode.getLocalName());
        inodeIden.setPid(inode.getParentId());
        inodeIden.setPartitionId(inode.getPartitionId());
      }
      return inodeIden;
    } else {
      return resolveINodeFromBlockID(b.getBlockId());
    }
  }
  
  public static Map<Long, List<Long>> getINodeIdsForBlockIds(final List<Long> blockIds, int batchSize, int nbThreads, 
      ExecutorService executor) throws IOException {
    final Map<Long, List<Long>> inodeIds = new HashMap<>();

    try {
      Slicer.slice(blockIds.size(), batchSize, nbThreads, executor,
          new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex) throws Exception {
          List<Long> ids = blockIds.subList(startIndex, endIndex);
          final long[] array = new long[ids.size()];
          int i = 0;
          for (long blockId : ids) {
            array[i] = blockId;
            i++;
          }
          LightWeightRequestHandler handler = new LightWeightRequestHandler(
              HDFSOperationType.RESOLVE_INODES_FROM_BLOCKIDS) {
            @Override
            public Object performTask() throws IOException {
              boolean transactionActive = connector.isTransactionActive();
              try {

                if (!transactionActive) {
                  connector.beginTransaction();
                }
                BlockLookUpDataAccess<BlockLookUp> da = (BlockLookUpDataAccess) HdfsStorageFactory
                    .getDataAccess(BlockLookUpDataAccess.class);
                Map<Long, List<Long>> inodeIds = da.getINodeIdsForBlockIds(array);
                return inodeIds;
              } finally {
                if (!transactionActive) {
                  connector.commit();
                }
              }
            }
          };
          try {
            Map<Long, List<Long>> map = (Map<Long, List<Long>>) handler.handle();
            synchronized(inodeIds){
              for(long inodeId: map.keySet()){
                List<Long> blockIds = inodeIds.get(inodeId);
                if(blockIds==null){
                  blockIds= new ArrayList<>();
                  inodeIds.put(inodeId, blockIds);
                }
                blockIds.addAll(map.get(inodeId));
              }
            }
          } catch (IOException ex) {
            LOG.error("Could not resolve iNode from blockId (blockid=" + Arrays.toString(array) + ")");
            throw new StorageException(ex.getMessage());
          }
        }
      });
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return inodeIds;
  }
  
  public static List<INodeIdentifier> resolveINodesFromIds(final List<Long> inodeIds) throws StorageException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
        HDFSOperationType.RESOLVE_INODES_FROM_IDS) {
      @Override
      public Object performTask() throws IOException {
        boolean transactionActive = connector.isTransactionActive();
        try {
          if (!transactionActive) {
            connector.beginTransaction();
          }
          INodeDALAdaptor ida = (INodeDALAdaptor) HdfsStorageFactory
              .getDataAccess(INodeDataAccess.class);
          long[] ids = new long[inodeIds.size()];
          int i = 0;
          for (long id : inodeIds) {
            ids[i] = id;
            i++;
          }
          List<INodeIdentifier> result = new ArrayList<>(inodeIds.size());
          Collection<INode> inodes = ida.findInodesByIdsFTIS(ids);
          if (inodes == null) {
            return result;
          }
          for (INode inode : inodes) {
            INodeIdentifier inodeIdent = new INodeIdentifier(inode.getId());
            inodeIdent.setName(inode.getLocalName());
            inodeIdent.setPid(inode.getParentId());
            inodeIdent.setPartitionId(inode.getPartitionId());
            result.add(inodeIdent);
          }
          return result;

        } finally {
          if (!transactionActive) {
            connector.commit();
          }
        }
      }
    };
    try {
      return (List<INodeIdentifier>) handler.handle();
    } catch (IOException ex) {
      LOG.error("Could not resolve iNode from their Id");
      throw new StorageException(ex.getMessage());
    }
  }
  
  public static long[] resolveINodesFromBlockIds(final long[] blockIds)
      throws StorageException {
    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(HDFSOperationType.GET_INODEIDS_FOR_BLKS) {
          @Override
          public Object performTask() throws IOException {
            BlockLookUpDataAccess<BlockLookUp> da =
                (BlockLookUpDataAccess) HdfsStorageFactory
                    .getDataAccess(BlockLookUpDataAccess.class);
            return da.findINodeIdsByBlockIds(blockIds);
          }
        };
    try {
      return (long[]) handler.handle();
    } catch (IOException ex) {
      throw new StorageException(ex.getMessage());
    }
  }
 
  public static INodeIdentifier resolveINodeFromId(final long id)
      throws StorageException {
    INodeIdentifier inodeIdentifier;

    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(HDFSOperationType.RESOLVE_INODE_FROM_ID) {

          @Override
          public Object performTask() throws StorageException, IOException {
            INodeDALAdaptor ida = (INodeDALAdaptor) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);
            INode inode = ida.findInodeByIdFTIS(id);
            INodeIdentifier inodeIdent = new INodeIdentifier(id);
            if (inode != null) {
              inodeIdent.setName(inode.getLocalName());
              inodeIdent.setPid(inode.getParentId());
              inodeIdent.setPartitionId(inode.getPartitionId());
            }
            return inodeIdent;
          }
        };

    try {
      inodeIdentifier = (INodeIdentifier) handler.handle();
    } catch (IOException ex) {
      throw new StorageException(ex.getMessage());
    }
    return inodeIdentifier;
  }

  public static String constructPath(List<INode> pathINodes) {
    StringBuilder builder = new StringBuilder();
    for (INode node : pathINodes) {
      if (node.isDirectory()) {
        builder.append(node.getLocalName() + "/");
      } else {
        builder.append(node.getLocalName());
      }
    }
    return builder.toString();
  }
  
  public static List<AclEntry> getInodeOwnAclNoTransaction(INode inode) throws AclException, StorageException {
  
    AceDataAccess<Ace> aceDataAccess =
        (AceDataAccess) HdfsStorageFactory.getDataAccess(AceDataAccess.class);
    int numAces = inode.getNumAces();
    if (numAces == 0){
      return new ArrayList<>();
    }
    int[] aceIndices = new int[numAces];
    for (int i = 0; i < aceIndices.length; i++) {
      aceIndices[i] = i;
    }
    List<Ace> acesByPKBatched = aceDataAccess.getAcesByPKBatched(inode.getId(), aceIndices);
    return INodeAclHelper.convert(acesByPKBatched);
  }
  
  public static List<AclEntry> getInodeOwnAclNoTransaction(ProjectedINode child) throws StorageException, AclException {
     AceDataAccess<Ace> aceDataAccess =
        (AceDataAccess) HdfsStorageFactory.getDataAccess(AceDataAccess.class);
    int numAces = child.getNumAces();
    if (numAces == 0){
      return new ArrayList<>();
    }
    int[] aceIndices = new int[numAces];
    for (int i = 0; i < aceIndices.length; i++) {
      aceIndices[i] = i;
    }
    List<Ace> acesByPKBatched = aceDataAccess.getAcesByPKBatched(child.getId(), aceIndices);
    return INodeAclHelper.convert(acesByPKBatched);
    
  }
  
  /**
   * @return an empty list if the children list is null;
   * otherwise, return the children list.
   * The returned list should not be modified.
   */
  static public List<INode> getChildrenListNotTransactional(long inodeId, int depth)
      throws StorageException, TransactionContextException {
    List<INode> children = getChildrenNotTransactional(inodeId, depth);
    return children == null ? EMPTY_LIST : children;
  }

  /**
   * @return the children list which is possibly null.
   */
  static private List<INode> getChildrenNotTransactional(long inodeId, int depth)
      throws StorageException, TransactionContextException {

    INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
    short childrenDepth = ((short) (depth + 1));
    if (INode.isTreeLevelRandomPartitioned(childrenDepth)) {
      return (List<INode>) da.findInodesByParentIdFTIS(inodeId);
    } else {
      return (List<INode>) da.findInodesByParentIdAndPartitionIdPPIS(inodeId, inodeId/*
       * partition id for all the childred is the parent id
       */);
    }
  }
}
