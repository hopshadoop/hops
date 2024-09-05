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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.metadata.hdfs.entity.XAttrMetadataLogEntry;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;

/**
 * XAttrStorage is used to read and set xattrs for an inode.
 */
@InterfaceAudience.Private
public class XAttrStorage {
  
  /**
   * Reads an existing extended attribute of inode.
   * @param inode INode to read.
   * @param attr XAttr to read.
   * @return the existing XAttr.
   */
  public static XAttr readINodeXAttr(INode inode, XAttr attr)
      throws TransactionContextException, StorageException {
    List<XAttr> attrs = readINodeXAttrs(inode, Lists.newArrayList(attr));
    if(attrs == null || attrs.isEmpty())
      return null;
    return attrs.get(0);
  }
  
  /**
   * Reads an existing extended attribute of inode.
   * @param inode INode to read.
   * @param attrs List of XAttrs to read.
   * @return the existing list of XAttrs.
   */
  public static List<XAttr> readINodeXAttrs(INode inode, List<XAttr> attrs)
      throws TransactionContextException, StorageException {
    XAttrFeature f = getXAttrFeature(inode);
    
    if(attrs == null || attrs.isEmpty()){
      return f.getXAttrs();
    }else{
      return f.getXAttr(attrs);
    }
  }
  
  /**
   * Update xattr of inode.
   * @param inode Inode to update.
   * @param xAttr the xAttr to update.
   * @param xAttrExists
   */
  public static void updateINodeXAttr(INode inode, XAttr xAttr, boolean xAttrExists)
      throws IOException {
    XAttrFeature f = getXAttrFeature(inode);
    if(!xAttrExists) {
      f.addXAttr(xAttr);
      logMetadataEvent(inode, xAttr, XAttrMetadataLogEntry.Operation.Add);
    }else{
      f.updateXAttr(xAttr);
      logMetadataEvent(inode, xAttr, XAttrMetadataLogEntry.Operation.Update);
    }
  }
  
  /**
   * Remove xattr from inode.
   * @param inode Inode to update.
   * @param xAttr the xAttr to remove.
   */
  public static void removeINodeXAttr(INode inode, XAttr xAttr)
      throws IOException {
    XAttrFeature f = getXAttrFeature(inode);
    f.removeXAttr(xAttr);
    logMetadataEvent(inode, xAttr, XAttrMetadataLogEntry.Operation.Delete);
  }
  
  private static XAttrFeature getXAttrFeature(INode inode){
    XAttrFeature f = inode.getXAttrFeature();
    if(f == null){
      f = new XAttrFeature(inode.getId());
      inode.addXAttrFeature(f);
    }
    return f;
  }
  
  public static int getMaxNumberOfUserXAttrPerInode(){
    return StoredXAttr.MAX_NUM_USER_XATTRS_PER_INODE;
  }
  
  public static int getMaxNumberOfSysXAttrPerInode(){
    return StoredXAttr.MAX_NUM_SYS_XATTRS_PER_INODE;
  }
  
  public static int getMaxXAttrSize(){
    return getMaxXAttrNameSize() + getMaxXAttrValueSize();
  }
  
  public static int getDefaultXAttrSize(){
    return getMaxXAttrNameSize() + getDefaultXAttrValueSize();
  }
  
  public static int getMaxXAttrNameSize(){
    return StoredXAttr.MAX_XATTR_NAME_SIZE;
  }
  
  public static int getDefaultXAttrValueSize(){
    return StoredXAttr.MAX_XATTR_VALUE_ROW_SIZE;
  }
  
  public static int getMaxXAttrValueSize(){
    return StoredXAttr.MAX_XATTR_VALUE_SIZE;
  }
  
  public static int getXAttrByteSize(String str){
    return StoredXAttr.getXAttrBytes(str).length;
  }
  
  private static void logMetadataEvent(INode inode, XAttr attr,
      XAttrMetadataLogEntry.Operation operation)
      throws TransactionContextException, StorageException {

    if(inode.isPathMetaEnabled()) {
      long datasetId = inode.getMetaEnabledParent().getId();
      long inodeId = inode.getId();
      int logicaltime = inode.incrementLogicalTime();
      inode.save();
      
  
      XAttrMetadataLogEntry logEntry = new XAttrMetadataLogEntry(datasetId,
          inodeId, logicaltime, inode.getPartitionId(), inode.getParentId(),
          inode.getLocalName(), attr.getValue(),
          attr.getNameSpace().getId(), attr.getName(), operation);
  
      EntityManager.add(logEntry);
    }
  }
}
