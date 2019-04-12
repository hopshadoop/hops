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

import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.metadata.hdfs.entity.XAttrMetadataLogEntry;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;

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
   * @param exists
   */
  public static void updateINodeXAttr(INode inode, XAttr xAttr,
      boolean xAttrExists)
      throws TransactionContextException, StorageException {
    XAttrFeature f = getXAttrFeature(inode);
    f.addXAttr(xAttr);
    
    if(!xAttrExists) {
      logMetadataEvent(inode, xAttr, XAttrMetadataLogEntry.Operation.Add);
    }else{
      logMetadataEvent(inode, xAttr, XAttrMetadataLogEntry.Operation.Update);
    }
  }
  
  /**
   * Remove xattr from inode.
   * @param inode Inode to update.
   * @param xAttr the xAttr to remove.
   */
  public static void removeINodeXAttr(INode inode, XAttr xAttr)
      throws TransactionContextException, StorageException {
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
  
  public static int getMaxNumberOfXAttrPerInode(){
    return StoredXAttr.MAX_NUM_XATTRS_PER_INODE;
  }
  
  public static int getMaxXAttrSize(){
    return StoredXAttr.MAX_XATTR_NAME_SIZE + StoredXAttr.MAX_XATTR_VALUE_SIZE;
  }
  
  /**
   * Verifies that the size of the name and value of an xattr is within
   * the configured limit.
   */
  public static void checkXAttrSize(final XAttr xAttr,
      final int xattrSizeLimit) {
    
    int nameSize = StoredXAttr.getXAttrBytes(xAttr.getName()).length;
    int valueSize = 0;
    if (xAttr.getValue() != null) {
      valueSize = xAttr.getValue().length;
    }
    
    if (nameSize > StoredXAttr.MAX_XATTR_NAME_SIZE) {
      throw new HadoopIllegalArgumentException(
          "The XAttr name is too big. The maximum  size of the"
              + " name is " + StoredXAttr.MAX_XATTR_NAME_SIZE
              + ", but the name size is " + nameSize);
    }
    
    if (valueSize > StoredXAttr.MAX_XATTR_VALUE_SIZE) {
      throw new HadoopIllegalArgumentException(
          "The XAttr value is too big. The maximum  size of the"
              + " value is " + StoredXAttr.MAX_XATTR_VALUE_SIZE
              + ", but the value size is " + valueSize);
    }
    
    int size = nameSize + valueSize;
    if (size > xattrSizeLimit) {
      throw new HadoopIllegalArgumentException(
          "The XAttr is too big. The maximum combined size of the"
              + " name and value is " + xattrSizeLimit
              + ", but the total size is " + size);
    }
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
          inodeId, logicaltime, attr.getNameSpace().getId(), attr.getName(),
          operation);
  
      EntityManager.add(logEntry);
    }
  }
}
