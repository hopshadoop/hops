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

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Feature for extended attributes.
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {
  
  public static final ImmutableList<XAttr> EMPTY_ENTRY_LIST =
      ImmutableList.of();
  
  private final long inodeId;
  
  public XAttrFeature(long inodeId){
    this.inodeId = inodeId;
  }
  
  public XAttrFeature(ImmutableList<XAttr> xAttrs, long inodeId)
      throws TransactionContextException, StorageException {
    this.inodeId = inodeId;
    for(XAttr attr: xAttrs){
      EntityManager.add(convertXAttrtoStored(attr));
    }
  }
  
  public List<XAttr> getXAttr(List<XAttr> attrs)
      throws StorageException, TransactionContextException {
    List<XAttr> storedAttrs = Lists.newArrayList();
    for(XAttr attr : attrs){
      StoredXAttr storedXAttr = EntityManager.find(StoredXAttr.Finder.ByPrimaryKey,
          getPrimaryKey(inodeId, attr));
      if(storedXAttr != null){
        storedAttrs.add(convertStoredtoXAttr(storedXAttr));
      }
    }
    return storedAttrs;
  }
  
  public void addXAttr(XAttr attr)
      throws TransactionContextException, StorageException {
    StoredXAttr storedXAttr = convertXAttrtoStored(attr);
    EntityManager.add(storedXAttr);
  }
  
  public void removeXAttr(XAttr attr)
      throws TransactionContextException, StorageException {
    StoredXAttr storedXAttr = convertXAttrtoStored(attr);
    EntityManager.remove(storedXAttr);
  }
  
  public ImmutableList<XAttr> getXAttrs()
      throws TransactionContextException, StorageException {
    Collection<StoredXAttr> extendedAttributes =
        EntityManager.findList(StoredXAttr.Finder.ByInodeId, inodeId);
    
    if(extendedAttributes == null)
      return EMPTY_ENTRY_LIST;
    
    List<XAttr> attrs =
        Lists.newArrayListWithExpectedSize(extendedAttributes.size());
    for(StoredXAttr attr : extendedAttributes){
      attrs.add(convertStoredtoXAttr(attr));
    }
    return ImmutableList.copyOf(attrs);
  }
  
  public void remove(final int numXAttrs) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_XATTRS_FOR_INODE){
  
      @Override
      public Object performTask() throws IOException {
        XAttrDataAccess xda = (XAttrDataAccess) HdfsStorageFactory
            .getDataAccess(XAttrDataAccess.class);
        int removed = xda.removeXAttrsByInodeId(inodeId);
        requestHandlerLOG.trace("Successfully removed " + removed + " XAttrs " +
            "out of " + numXAttrs + " for inode (" + inodeId + ")");
        return null;
      }
    }.handle();
  }
  
  private XAttr convertStoredtoXAttr(StoredXAttr attr){
    XAttr.Builder builder = new XAttr.Builder();
    builder.setName(attr.getName());
    builder.setNameSpace(XAttr.NameSpace.valueOf(attr.getNamespace()));
    builder.setValue(attr.getValue());
    return builder.build();
  }
  
  private StoredXAttr convertXAttrtoStored(XAttr attr){
    return new StoredXAttr(inodeId, attr.getNameSpace().getId(), attr.getName(),
        attr.getValue());
  }
  
  public static List<StoredXAttr.PrimaryKey> getPrimaryKeys(long inodeId,
      List<XAttr> attrs){
    List<StoredXAttr.PrimaryKey> pks =
        Lists.newArrayListWithExpectedSize(attrs.size());
    for(XAttr attr : attrs){
      pks.add(getPrimaryKey(inodeId, attr));
    }
    return pks;
  }
  
  public static StoredXAttr.PrimaryKey getPrimaryKey(long inodeId, XAttr attr){
    return new StoredXAttr.PrimaryKey(inodeId, attr.getNameSpace().getId(),
        attr.getName());
  }
 
}
