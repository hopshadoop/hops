/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.lock;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XAttrLock extends Lock{
  
  private final List<XAttr> attrs;
  private final String path;
  
  public XAttrLock(List<XAttr> attrs){
    this(attrs, null);
  }
  
  public XAttrLock(List<XAttr> attrs, String path){
    this.attrs = attrs;
    this.path = path;
  }
  
  public XAttrLock(XAttr attr){
    this(attr, null);
  }
  
  public XAttrLock(XAttr attr, String path){
    if(attr != null) {
      this.attrs = new ArrayList<>();
      this.attrs.add(attr);
    }else{
      this.attrs = null;
    }
    this.path = path;
  }
  
  public XAttrLock(){
    this.attrs = null;
    this.path = null;
  }
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    for(INode inode : inodeLock.getTargetINodes()){
      acquire(inode);
    }
    if(path!=null){
      for(INode inode: inodeLock.getChildINodes(path)){
        acquire(inode);
      }
    }
  }
  
  private void acquire(INode inode) throws TransactionContextException, StorageException{
    if(attrs == null || attrs.isEmpty()) {
        //read all xattrs
        //Skip if the inode doesn't have any xattrs
        if(!inode.hasXAttrs()){
          EntityManager.snapshotMaintenance
              (HdfsTransactionContextMaintenanceCmds.NoXAttrsAttached,
                  inode.getId());
          return;
        }
        acquireLockList(DEFAULT_LOCK_TYPE, StoredXAttr.Finder.ByInodeId, inode.getId());
      }else{
        acquireLockList(DEFAULT_LOCK_TYPE, StoredXAttr.Finder.ByPrimaryKeyBatch,
            XAttrFeature.getPrimaryKeys(inode.getId(), attrs));
      }
  }
  
  @Override
  protected Type getType() {
    return Type.XAttr;
  }
}
