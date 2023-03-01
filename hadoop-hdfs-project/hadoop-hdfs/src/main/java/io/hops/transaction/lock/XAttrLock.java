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

public class XAttrLock extends Lock {

  private static int bacthLockSize = 1000;
  private final List<XAttr> attrs;
  private final String path;

  protected static void setBacthLockSize(int bacthLockSize) {
    XAttrLock.bacthLockSize = bacthLockSize;
  }

  public XAttrLock(List<XAttr> attrs) {
    this(attrs, null);
  }

  public XAttrLock(List<XAttr> attrs, String path) {
    this.attrs = attrs;
    this.path = path;
  }

  public XAttrLock(XAttr attr) {
    this(attr, null);
  }

  public XAttrLock(XAttr attr, String path) {
    if (attr != null) {
      this.attrs = new ArrayList<>();
      this.attrs.add(attr);
    } else {
      this.attrs = null;
    }
    this.path = path;
  }

  public XAttrLock() {
    this.attrs = null;
    this.path = null;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);

    boolean canBatchLock = attrs != null && !attrs.isEmpty();

    //path inodes
    if (canBatchLock) {
      batchLockInodes(inodeLock.getTargetINodes());
    } else {
      for (INode inode : inodeLock.getTargetINodes()) {
        scanLock(inode);
      }
    }

    //lock children
    if (path != null) {
      if (canBatchLock) {
        batchLockInodes(inodeLock.getChildINodes(path));
      } else {
        for (INode inode : inodeLock.getChildINodes(path)) {
          scanLock(inode);
        }
      }
    }
  }

  private void batchLockInodes(List<INode> inodes) throws TransactionContextException, StorageException {
    List<StoredXAttr.PrimaryKey> allAttr = new ArrayList(inodes.size());
    for (INode inode : inodes) {
      if (!inode.hasXAttrs()) {
        EntityManager.snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.NoXAttrsAttached,
                inode.getId());
      } else {
        List<StoredXAttr.PrimaryKey> inodeXAttrLocks = XAttrFeature.getPrimaryKeys(inode.getId(), attrs);
        allAttr.addAll(inodeXAttrLocks);
      }
    }
    batchLockInternal(allAttr);
  }

  private void batchLockInternal(List<StoredXAttr.PrimaryKey> allAttr) throws TransactionContextException, StorageException {
    int numOfSlices;
    if (allAttr.size() <= bacthLockSize) {
      numOfSlices = 1;
    } else {
      numOfSlices = (int) Math.ceil(((double) allAttr.size()) / bacthLockSize);
    }

    for (int slice = 0; slice < numOfSlices; slice++) {
      int startIndex = slice * bacthLockSize;
      int endIndex = Math.min((slice + 1) * bacthLockSize, allAttr.size());
      List<StoredXAttr.PrimaryKey> subList = allAttr.subList(startIndex, endIndex);
      acquireLockList(DEFAULT_LOCK_TYPE, StoredXAttr.Finder.ByPrimaryKeyBatch, subList);
    }
  }

  private void scanLock(INode inode) throws TransactionContextException, StorageException {
    assert attrs == null || attrs.isEmpty();
    //read all xattrs
    //Skip if the inode doesn't have any xattrs
    if (!inode.hasXAttrs()) {
      EntityManager.snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.NoXAttrsAttached,
              inode.getId());
    } else {
      acquireLockList(DEFAULT_LOCK_TYPE, StoredXAttr.Finder.ByInodeId, inode.getId());
    }
  }

  @Override
  protected Type getType() {
    return Type.XAttr;
  }
}
