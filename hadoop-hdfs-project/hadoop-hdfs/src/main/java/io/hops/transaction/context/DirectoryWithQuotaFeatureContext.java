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
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.DirectoryWithQuotaFeatureDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.Quota;

public class DirectoryWithQuotaFeatureContext
    extends BaseEntityContext<Long, DirectoryWithQuotaFeature> {

  private final DirectoryWithQuotaFeatureDataAccess<DirectoryWithQuotaFeature> dataAccess;

  public DirectoryWithQuotaFeatureContext(
      DirectoryWithQuotaFeatureDataAccess<DirectoryWithQuotaFeature> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(DirectoryWithQuotaFeature directoryWithQuotaFeature)
      throws TransactionContextException {
    if (directoryWithQuotaFeature.getInodeId()!=null) {
      super.update(directoryWithQuotaFeature);
      if(isLogTraceEnabled()){
        log("updated-attributes", "id", directoryWithQuotaFeature.getInodeId(), "quota", directoryWithQuotaFeature.
            getQuota(), " usage", directoryWithQuotaFeature.getSpaceConsumed());
      }
    } else {
      if(isLogTraceEnabled()) {
        log("updated-attributes -- IGNORED as id is not set");
      }
    }
  }

  @Override
  public void remove(DirectoryWithQuotaFeature directoryWithQuotaFeature)
      throws TransactionContextException {
    super.remove(directoryWithQuotaFeature);
    if(isLogTraceEnabled()) {
      log("removed-attributes", "id", directoryWithQuotaFeature.getInodeId());
      for(int i = 0; i < Thread.currentThread().getStackTrace().length;i++){
       System.out.println(Thread.currentThread().getStackTrace()[i]) ;
      }
    }
  }

  @Override
  public DirectoryWithQuotaFeature find(FinderType<DirectoryWithQuotaFeature> finder, Object... params) throws
      TransactionContextException, StorageException {
    DirectoryWithQuotaFeature.Finder qfinder = (DirectoryWithQuotaFeature.Finder) finder;
    switch (qfinder) {
      case ByINodeId:
        return findByPrimaryKey(qfinder, params);
    }
    throw new UnsupportedOperationException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<DirectoryWithQuotaFeature> findList(FinderType<DirectoryWithQuotaFeature> finder, Object... params)
      throws TransactionContextException, StorageException {
    DirectoryWithQuotaFeature.Finder qfinder = (DirectoryWithQuotaFeature.Finder) finder;
    switch (qfinder) {
      case ByINodeIds:
        return findByPrimaryKeys(qfinder, params);
    }
    throw new UnsupportedOperationException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    Collection<DirectoryWithQuotaFeature> modified =
        new ArrayList<>(getModified());
    modified.addAll(getAdded());
    dataAccess.prepare(modified, getRemoved());
  }

  @Override
  Long getKey(DirectoryWithQuotaFeature directoryWithQuotaFeature) {
    return directoryWithQuotaFeature.getInodeId();
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds =
        (HdfsTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        // need to update the rows with updated inodeId or partKey
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        INodeCandidatePrimaryKey trg_param =
            (INodeCandidatePrimaryKey) params[0];
        List<INodeCandidatePrimaryKey> srcs_param =
            (List<INodeCandidatePrimaryKey>) params[1];
        List<BlockInfoContiguous> oldBlks = (List<BlockInfoContiguous>) params[2];
        updateAttributes(trg_param, srcs_param);
        break;
    }
  }

  private DirectoryWithQuotaFeature findByPrimaryKey(DirectoryWithQuotaFeature.Finder qfinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final long inodeId = (Long) params[0];
    DirectoryWithQuotaFeature result = null;
    if (contains(inodeId)) {
      result = get(inodeId);
      hit(qfinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(qfinder, params);
      result = dataAccess.findAttributesByPk(inodeId);
      gotFromDB(inodeId, result);
      miss(qfinder, result, "inodeid", inodeId, "size", size());
    }
    return result;
  }

  private Collection<DirectoryWithQuotaFeature> findByPrimaryKeys(DirectoryWithQuotaFeature.Finder qfinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final List<INodeCandidatePrimaryKey> inodePks =
        (List<INodeCandidatePrimaryKey>) params[0];
    Collection<DirectoryWithQuotaFeature> result = null;
    if (contains(inodePks)) {
      result = get(inodePks);
      hit(qfinder, result, "inodeids", inodePks);
    } else {
      aboutToAccessStorage(qfinder, inodePks);
      result = dataAccess.findAttributesByPkList(inodePks);
      gotFromDB(result);
      miss(qfinder, result, "inodeids", inodePks);
      if(result!=null){
        for(DirectoryWithQuotaFeature directoryWithQuotaFeature:result){
          log("attributes", "id", directoryWithQuotaFeature.getInodeId(), "quota", directoryWithQuotaFeature.
            getQuota(), " usage", directoryWithQuotaFeature.getSpaceConsumed());
        }

      }
    }
    return result;
  }


  private boolean contains(List<INodeCandidatePrimaryKey> iNodeCandidatePKs) {
    for (INodeCandidatePrimaryKey pk : iNodeCandidatePKs) {
      if (!contains(pk.getInodeId())) {
        return false;
      }
    }
    return true;
  }

  private Collection<DirectoryWithQuotaFeature> get(List<INodeCandidatePrimaryKey> iNodeCandidatePKs) {
    Collection<DirectoryWithQuotaFeature> directoryWithQuotaFeatures = new ArrayList<>(iNodeCandidatePKs.size());
    for (INodeCandidatePrimaryKey pk : iNodeCandidatePKs) {
      directoryWithQuotaFeatures.add(get(pk.getInodeId()));
    }
    return directoryWithQuotaFeatures;
  }

  private void updateAttributes(INodeCandidatePrimaryKey trg_param, List<INodeCandidatePrimaryKey> toBeDeletedSrcs)
      throws TransactionContextException {
    toBeDeletedSrcs.remove(trg_param);
    for (INodeCandidatePrimaryKey src : toBeDeletedSrcs) {
      if (contains(src.getInodeId())) {
        DirectoryWithQuotaFeature toBeDeleted = get(src.getInodeId());
        DirectoryWithQuotaFeature toBeAdded = clone(toBeDeleted, trg_param.getInodeId());

        remove(toBeDeleted);
        if(isLogTraceEnabled()) {
          log("snapshot-maintenance-removed-inode-attribute", "inodeId",
                  toBeDeleted.getInodeId());
        }

        add(toBeAdded);
        if(isLogTraceEnabled()) {
          log("snapshot-maintenance-added-inode-attribute", "inodeId",
                  toBeAdded.getInodeId());
        }
      }
    }
  }

  private DirectoryWithQuotaFeature clone(DirectoryWithQuotaFeature src, long inodeId) {
    return new DirectoryWithQuotaFeature.Builder(inodeId).nameSpaceQuota(src.getQuota().getNameSpace()).
        nameSpaceUsage(src.getSpaceConsumed().getNameSpace()).storageSpaceQuota(src.getQuota().getStorageSpace()).spaceUsage(
        src.getSpaceConsumed().getStorageSpace()).typeQuotas(src.getQuota().getTypeSpaces()).typeUsages(src.
        getSpaceConsumed().getTypeSpaces()).build();
  }

}
