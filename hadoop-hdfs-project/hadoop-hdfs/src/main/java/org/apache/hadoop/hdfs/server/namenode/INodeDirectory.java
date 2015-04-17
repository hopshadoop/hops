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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.transaction.EntityManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;


/**
 * Directory INode class.
 */
public class INodeDirectory extends INodeWithAdditionalFields {
  
  /**
   * Cast INode to INodeDirectory.
   */
  public static INodeDirectory valueOf(INode inode, Object path
      ) throws FileNotFoundException, PathIsNotDirectoryException {
    if (inode == null) {
      throw new FileNotFoundException("Directory does not exist: "
          + DFSUtil.path2String(path));
    }
    if (!inode.isDirectory()) {
      throw new PathIsNotDirectoryException(DFSUtil.path2String(path));
    }
    return (INodeDirectory) inode;
  }

  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  public final static String ROOT_NAME = "";

  public static final long ROOT_DIR_PARTITION_KEY = HdfsConstantsClient.GRANDFATHER_INODE_ID;
  public static final short ROOT_DIR_DEPTH =0;

  private boolean metaEnabled;

  private int childrenNum;
  
  public INodeDirectory(long id, String name, PermissionStatus permissions)
      throws IOException {
    
    super(id, name, permissions);
  }
  
  public INodeDirectory(long id, String name, PermissionStatus permissions, boolean inTree)
      throws IOException {
    super(id, name, permissions, inTree);
  }

  public INodeDirectory(long id, PermissionStatus permissions, long mTime)
      throws IOException {
    super(id, permissions, mTime, 0);
  }

  /**
   * constructor
   */
  INodeDirectory(long id, byte[] name, PermissionStatus permissions, long mtime)
      throws IOException {
    super(id, name, permissions, mtime, 0L, false);
  }
  
  INodeDirectory(INodeDirectory other) throws IOException {
    this(other, true);
  }
  
  /**
   * copy constructor
   *
   * @param other
   */
  INodeDirectory(INodeDirectory other, boolean copyFeatures)
      throws IOException {
    super(other);
    //HOP: FIXME: Mahmoud: the new directory has the same id as the "other"
    // directory so we don't need to notify the children of the directory change
    if (copyFeatures) {
      this.features = other.features;
    }
    this.metaEnabled = other.isMetaEnabled();
  }
  
  /**
   * @return true unconditionally.
   */
  @Override
  public final boolean isDirectory() {
    return true;
  }

  public static INodeDirectory createRootDir(PermissionStatus permissions) throws IOException {
    final INodeDirectory newRootINode = new INodeDirectory(ROOT_INODE_ID, ROOT_NAME, permissions);
    newRootINode.inTree();
    newRootINode.setParentIdNoPersistance(HdfsConstantsClient.GRANDFATHER_INODE_ID);
    newRootINode.setPartitionIdNoPersistance(getRootDirPartitionKey());
    return newRootINode;
  }
  
  public static INodeDirectory getRootDir()
    throws StorageException, TransactionContextException {
    INode inode = EntityManager.find(Finder.ByINodeIdFTIS, ROOT_INODE_ID);
    return (INodeDirectory) inode;
  }

  void setQuota(BlockStoragePolicySuite bsps, long nsQuota, long ssQuota, QuotaCounts c, StorageType type) 
    throws StorageException, TransactionContextException {
    DirectoryWithQuotaFeature quota = getDirectoryWithQuotaFeature();
    if (quota != null) {
      // already has quota; so set the quota to the new values
      if (type != null) {
        quota.setQuota(ssQuota, type);
      } else {
        quota.setQuota(nsQuota, ssQuota);
      }
    } else {
      DirectoryWithQuotaFeature.Builder builder =
          new DirectoryWithQuotaFeature.Builder(this.getId()).nameSpaceQuota(nsQuota);
      if (type != null) {
        builder.typeQuota(type, ssQuota);
      } else {
        builder.storageSpaceQuota(ssQuota);
      }
      addDirectoryWithQuotaFeature(builder.build(true)).setSpaceConsumed(c);
    
    }
  }

  @Override
  public QuotaCounts getQuotaCounts() throws StorageException, TransactionContextException {
    final DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
    return q != null ? q.getQuota() : super.getQuotaCounts();
  }
  
  public void addSpaceConsumed(QuotaCounts counts, boolean verify)
    throws QuotaExceededException, StorageException, TransactionContextException {
    DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
    if (q != null) {
      q.addSpaceConsumed(this, counts);
    } else {
      addSpaceConsumed2Parent(counts, verify);
    }
  }
  
  /**
   * If the directory contains a {@link DirectoryWithQuotaFeature}, return it;
   * otherwise, return null.
   */
  public final DirectoryWithQuotaFeature getDirectoryWithQuotaFeature() throws TransactionContextException, StorageException {
    int i=0;
    for (Feature f : features) {
      if (f instanceof DirectoryWithQuotaFeature) {
        DirectoryWithQuotaFeature feature = EntityManager.find(DirectoryWithQuotaFeature.Finder.ByINodeId, this.getId());
        features[i]=feature;
        return feature;
      }
      i++;
    }
    return null;
  }
  
  /** Is this directory with quota? */
  public final boolean isWithQuota() {
    for (Feature f : features) {
      if (f instanceof DirectoryWithQuotaFeature) {
        return true;
      }
    } 
    return false;
  }
  
  public DirectoryWithQuotaFeature addDirectoryWithQuotaFeature(
      DirectoryWithQuotaFeature q)
    throws StorageException, TransactionContextException {
    Preconditions.checkState(!isWithQuota(), "Directory is already with quota");
    addFeature(q);
    return q;
  }
  
  /**
   * @return this object.
   */
  @Override
  public final INodeDirectory asDirectory() {
    return this;
  }

  public boolean isMetaEnabled() {
    return metaEnabled;
  }

  public void setMetaEnabled(boolean metaEnabled) {
    this.metaEnabled = metaEnabled;
  }

  public boolean removeChild(INode node)
      throws IOException {
    INode existingInode = getChildINode(node.getLocalNameBytes());
    if (existingInode != null) {
      remove(existingInode);
      decreaseChildrenNum();
      return true;
    }
    return false;
  }

  /**
   * Replace a child that has the same name as newChild by newChild.
   *
   * @param newChild
   *     Child node to be added
   */
  void replaceChild(INode newChild)
      throws StorageException, TransactionContextException {
    //HOP: Mahmoud: equals based on the inode name
    INode existingINode = getChildINode(newChild.getLocalNameBytes());
    if (existingINode == null) {
      throw new IllegalArgumentException("No child exists to be replaced");
    } else {
      //[M] make sure that the newChild has the same parentid
      if (existingINode.getParentId() != newChild.getParentId()) {
        throw new IllegalArgumentException("Invalid parentid");
      }
      short depth = myDepth();
      long childPartitionKey  = INode.calculatePartitionId(getId(), newChild.getLocalName(), (short) (myDepth()+1));
      newChild.setPartitionId(childPartitionKey);
      EntityManager.update(newChild);
    }
  }
  
  INode getChild(String name)
      throws StorageException, TransactionContextException {
    return getChildINode(DFSUtil.string2Bytes(name));
  }

  public INode getChildINode(byte[] name)
      throws StorageException, TransactionContextException {
    short myDepth = myDepth();
    long childPartitionId = INode.calculatePartitionId(getId(), DFSUtil.bytes2String(name), (short)(myDepth+1));
    INode existingInode = EntityManager
        .find(Finder.ByNameParentIdAndPartitionId, DFSUtil.bytes2String(name),
            getId(), childPartitionId);
    if (existingInode != null && existingInode.isInTree()) {
      return existingInode;
    }
    return null;
  }

//  /**
//   * Retrieve existing INodes from a path. If existing is big enough to store
//   * all path components (existing and non-existing), then existing INodes
//   * will be stored starting from the root INode into existing[0]; if
//   * existing is not big enough to store all path components, then only the
//   * last existing and non existing INodes will be stored so that
//   * existing[existing.length-1] refers to the INode of the final component.
//   * <p/>
//   * An UnresolvedPathException is always thrown when an intermediate path
//   * component refers to a symbolic link. If the final path component refers
//   * to a symbolic link then an UnresolvedPathException is only thrown if
//   * resolveLink is true.
//   * <p/>
//   * <p/>
//   * Example: <br>
//   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
//   * following path components: ["","c1","c2","c3"],
//   * <p/>
//   * <p/>
//   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
//   * array with [c2] <br>
//   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill
//   * the
//   * array with [null]
//   * <p/>
//   * <p/>
//   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
//   * array with [c1,c2] <br>
//   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
//   * the array with [c2,null]
//   * <p/>
//   * <p/>
//   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
//   * the array with [rootINode,c1,c2,null], <br>
//   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
//   * fill the array with [rootINode,c1,c2,null]
//   *
//   * @param components
//   *     array of path component name
//   * @param resolveLink
//   *     indicates whether UnresolvedLinkException should
//   *     be thrown when the path refers to a symbolic link.
//   * @return number of existing INodes in the path
//   */
//  INodesInPath getExistingPathINodes(byte[][] components, int numOfINodes,
//      boolean resolveLink) throws UnresolvedLinkException, StorageException,
//      TransactionContextException {
//    assert
//        this.compareTo(components[0]) == 0 :
//        "Incorrect name " + getLocalName() + " expected " +
//            (components[0] == null ? null :
//                DFSUtil.bytes2String(components[0]));
//
//    INodesInPath existing = new INodesInPath(numOfINodes);
//    INode curNode = this;
//    int count = 0;
//    int index = numOfINodes - components.length;
//    if (index > 0) {
//      index = 0;
//    }
//    while (count < components.length && curNode != null) {
//      final boolean lastComp = (count == components.length - 1);
//      if (index >= 0) {
//        existing.inodes[index] = curNode;
//      }
//      if (curNode.isSymlink() && (!lastComp || (lastComp && resolveLink))) {
//        final String path = constructPath(components, 0, components.length);
//        final String preceding = constructPath(components, 0, count);
//        final String remainder =
//            constructPath(components, count + 1, components.length);
//        final String link = DFSUtil.bytes2String(components[count]);
//        final String target = ((INodeSymlink) curNode).getSymlinkString();
//        if (NameNode.stateChangeLog.isDebugEnabled()) {
//          NameNode.stateChangeLog.debug("UnresolvedPathException " +
//              " path: " + path + " preceding: " + preceding +
//              " count: " + count + " link: " + link + " target: " + target +
//              " remainder: " + remainder);
//        }
//        throw new UnresolvedPathException(path, preceding, remainder, target);
//      }
//      count++;
//      existing.count = count;
//      index++;
//      if (lastComp || !curNode.isDirectory()) {
//        break;
//      }
//      INodeDirectory parentDir = (INodeDirectory) curNode;
//      curNode = parentDir.getChildINode(components[count]);
//    }
//    return existing;
//  }
//
//  /**
//   * Retrieve the existing INodes along the given path. The first INode
//   * always exist and is this INode.
//   *
//   * @param path
//   *     the path to explore
//   * @param resolveLink
//   *     indicates whether UnresolvedLinkException should
//   *     be thrown when the path refers to a symbolic link.
//   * @return INodes array containing the existing INodes in the order they
//   * appear when following the path from the root INode to the
//   * deepest INodes. The array size will be the number of expected
//   * components in the path, and non existing components will be
//   * filled with null
//   */
//  INodesInPath getExistingPathINodes(String path, boolean resolveLink)
//      throws UnresolvedLinkException, StorageException,
//      TransactionContextException {
//    byte[][] components = getPathComponents(path);
//    return getExistingPathINodes(components, components.length, resolveLink);
//  }

  /**
   * Given a child's name, return the index of the next child
   *
   * @param name
   *     a child's name
   * @return the index of the next child
   */
  int nextChild(List<INode> children, byte[] name)
      throws StorageException, TransactionContextException {
    if (name.length == 0) { // empty name
      return 0;
    }
    int nextPos = Collections.binarySearch(children, name) + 1;
    if (nextPos >= 0) {
      return nextPos;
    }
    return -nextPos;
  }

  /**
   * Add a child inode to the directory.
   *
   * @param node
   *     INode to insert
   * @param setModTime
   *     set modification time for the parent node
   *     not needed when replaying the addition and
   *     the parent already has the proper mod time
   *  @return false if the child with this name already exists; 
   *         otherwise, return true;
   */
  boolean addChild(final INode node, final boolean setModTime) throws IOException{
    return addChild(node, setModTime, true);
  }
  
  boolean addChild(final INode node, final boolean setModTime, final boolean
      logMetadataEvent) throws IOException{
    INode existingInode = getChildINode(node.getLocalNameBytes());
    if (existingInode != null) {
      return false;
    }

    if (!node.isInTree()) {
      node.inTree();
      node.setParentNoPersistance(this);
      short childDepth = (short)(myDepth()+1);
      node.setPartitionIdNoPersistance(INode.calculatePartitionId(node.getParentId(), node.getLocalName(), childDepth));
      EntityManager.add(node);
      increaseChildrenNum();
      //add the INodeAttributes if it is Directory with Quota
//      if (this instanceof INodeDirectoryWithQuota) { // [S] I think this is not necessary now. Quota update manager will take care of this
//        ((INodeDirectoryWithQuota) this).persistAttributes();
//      }
    } else {
      //rename operation
      //do not increment if moved in same folder
      if(node.getParent().getParentId() != getId()){
        increaseChildrenNum();
      }
      node.setParent(this);
    }


    // update modification time of the parent directory
    if (setModTime) {
      setModificationTime(node.getModificationTime());
    }

    if (node.getGroupName() == null) {
      node.setGroup(getGroupName());
      node.setGroupID(getGroupID());
    }
  
    if (logMetadataEvent) {
      node.logMetadataEvent(INodeMetadataLogEntry.Operation.Add);
    }

    return true;
  }

  @Override
  QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps, byte blockStoragePolicyId, QuotaCounts counts)
      throws StorageException, TransactionContextException {
    if (isWithQuota()) {
      final DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
      if (q != null && q.isQuotaSet()) {
        return q.AddCurrentSpaceUsage(counts);
      }
    } else {
      computeDirectoryQuotaUsage(bsps, blockStoragePolicyId, counts);
    }
    return counts;
  }
  
  private QuotaCounts computeDirectoryQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, QuotaCounts counts) throws StorageException, TransactionContextException {
    if (isInTree()) {
      List<INode> children = getChildren();
      if (children != null) {
        for (INode child : children) {
          final byte childPolicyId = child.getStoragePolicyIDForQuota(blockStoragePolicyId);
          child.computeQuotaUsage(bsps, childPolicyId, counts);
        }
      }
    }
    return computeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId,
        counts);
  }
  
  /** Add quota usage for this inode excluding children. */
  public QuotaCounts computeQuotaUsage4CurrentDirectory(
      BlockStoragePolicySuite bsps, byte storagePolicyId, QuotaCounts counts) {
    counts.addNameSpace(1);
    return counts;
  }
  
  @Override
  public ContentSummaryComputationContext computeContentSummary(
      ContentSummaryComputationContext summary)
    throws StorageException, TransactionContextException {
    final DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
    if (q != null) {
      return q.computeContentSummary(this, summary);
    } else {
      return computeDirectoryContentSummary(summary);
    }
  }
  
  ContentSummaryComputationContext computeDirectoryContentSummary(ContentSummaryComputationContext summary)
      throws StorageException, TransactionContextException {
    List<INode> childrenList = getChildrenList();
    // Explicit traversing is done to enable repositioning after relinquishing
    // and reacquiring locks.
    for (int i = 0;  i < childrenList.size(); i++) {
      INode child = childrenList.get(i);
      byte[] childName = child.getLocalNameBytes();
      
      long lastYieldCount = summary.getYieldCount();
      child.computeContentSummary(summary);
      
      // Check whether the computation was paused in the subtree.
      // The counts may be off, but traversing the rest of children
      // should be made safe.
      if (lastYieldCount == summary.getYieldCount()) {
        continue;
      }
      
      // The locks were released and reacquired. Check parent first.
      if (getParent() == null) {
        // Stop further counting and return whatever we have so far.
        break;
      }
      
      // Obtain the children list again since it may have been modified.
      childrenList = getChildrenList();
      // Reposition in case the children list is changed. Decrement by 1
      // since it will be incremented when loops.
      i = nextChild(childrenList, childName) - 1;
    }
    
    // Increment the directory count for this directory.
    summary.getCounts().addContent(Content.DIRECTORY, 1);
    // Relinquish and reacquire locks if necessary.
    summary.yield();

    return summary;
  }

  /**
   * @return an empty list if the children list is null;
   * otherwise, return the children list.
   * The returned list should not be modified.
   */
  public List<INode> getChildrenList()
      throws StorageException, TransactionContextException {
    List<INode> children = getChildren();
    return children == null ? EMPTY_LIST : children;
  }

  /**
   * @return the children list which is possibly null.
   */
  private List<INode> getChildren()
      throws StorageException, TransactionContextException {
    if (!isInTree()) {
      return null;
    }

    short childrenDepth = ((short)(myDepth()+1));
    if(INode.isTreeLevelRandomPartitioned(childrenDepth)){
       return (List<INode>) EntityManager
        .findList(INode.Finder.ByParentIdFTIS, getId());
    }else{
      return (List<INode>) EntityManager
        .findList(Finder.ByParentIdAndPartitionId, getId(), getId()/*partition id for all the childred is the parent id*/);
    }
  }

  @Override
  public void destroyAndCollectBlocks(final BlockStoragePolicySuite bsps,
      BlocksMapUpdateInfo collectedBlocks, 
      final List<INode> removedINodes)
      throws StorageException, TransactionContextException {
    List<INode> children = getChildren();
    if (children != null) {
      for (INode child : children) {
        child.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
      }
    }
    parent = null;

    removedINodes.add(this);

  }

  public static long getRootDirPartitionKey(){
    return INode.calculatePartitionId(HdfsConstantsClient.GRANDFATHER_INODE_ID,ROOT_NAME,ROOT_DIR_DEPTH);
  }

  public static INodeIdentifier getRootIdentifier(){
    INodeIdentifier rootINodeIdentifier = new INodeIdentifier(INodeDirectory.ROOT_INODE_ID,
        HdfsConstantsClient.GRANDFATHER_INODE_ID, INodeDirectory.ROOT_NAME,
        INodeDirectory.getRootDirPartitionKey());
    rootINodeIdentifier.setDepth(INodeDirectory.ROOT_DIR_DEPTH);
    return rootINodeIdentifier;
  }
  
  /*
   * The following code is to dump the tree recursively for testing.
   *
   * \- foo (INodeDirectory@33dd2717)
   * \- sub1 (INodeDirectory@442172)
   * +- file1 (INodeFile@78392d4)
   * +- file2 (INodeFile@78392d5)
   * +- sub11 (INodeDirectory@8400cff)
   * \- file3 (INodeFile@78392d6)
   * \- z_file4 (INodeFile@45848712)
   */
  static final String DUMPTREE_EXCEPT_LAST_ITEM = "+-";
  static final String DUMPTREE_LAST_ITEM = "\\-";

  @VisibleForTesting
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix) throws StorageException,
      TransactionContextException {
    super.dumpTreeRecursively(out, prefix);
    if (prefix.length() >= 2) {
      prefix.setLength(prefix.length() - 2);
      prefix.append("  ");
    }
    dumpTreeRecursively(out, prefix, getChildren());
  }

  /**
   * Dump the given subtrees.
   *
   * @param prefix The prefix string that each line should print.
   * @param subs The subtrees.
   */
  @VisibleForTesting
  protected static void dumpTreeRecursively(PrintWriter out,
      StringBuilder prefix, List<? extends INode> subs) throws StorageException, TransactionContextException {
    prefix.append(DUMPTREE_EXCEPT_LAST_ITEM);
    if (subs != null && subs.size() != 0) {
      int i = 0;
      for (; i < subs.size() - 1; i++) {
        subs.get(i).dumpTreeRecursively(out, prefix);
        prefix.setLength(prefix.length() - 2);
        prefix.append(DUMPTREE_EXCEPT_LAST_ITEM);
      }

      prefix.setLength(prefix.length() - 2);
      prefix.append(DUMPTREE_LAST_ITEM);
      subs.get(i).dumpTreeRecursively(out, prefix);
    }
    prefix.setLength(prefix.length() - 2);
  }
    
  @Override
  public INode cloneInode () throws IOException{
    return new INodeDirectory(this, true);
  }

  public int getChildrenNum() {
    return childrenNum;
  }
  
  public void setChildrenNum(int childrenNum){
    this.childrenNum = childrenNum;
  }

  public void decreaseChildrenNum() throws StorageException, TransactionContextException{
    childrenNum--;
    save();
  }
  
  public void increaseChildrenNum() throws StorageException, TransactionContextException{
    childrenNum++;
    save();
  }
  
  @Override
  INodeDirectory getMetaEnabledParent()
      throws TransactionContextException, StorageException {
    if(isMetaEnabled()){
      return this;
    }
    return super.getMetaEnabledParent();
  }
}
