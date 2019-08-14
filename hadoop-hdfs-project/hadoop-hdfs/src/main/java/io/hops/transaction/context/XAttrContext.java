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
package io.hops.transaction.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XAttrContext extends BaseEntityContext<StoredXAttr.PrimaryKey,
    StoredXAttr> {
  
  private final XAttrDataAccess<StoredXAttr, StoredXAttr.PrimaryKey> dataAccess;
  private final Map<Long, Collection<StoredXAttr>> xAttrsByInodeId =
      new HashMap<>();
  
  public XAttrContext(XAttrDataAccess dataAccess){
    this.dataAccess = dataAccess;
  }
  
  @Override
  StoredXAttr.PrimaryKey getKey(StoredXAttr storedXAttr) {
    return storedXAttr.getPrimaryKey();
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }
  
  @Override
  public StoredXAttr find(FinderType<StoredXAttr> finder, Object... params)
      throws TransactionContextException, StorageException {
    StoredXAttr.Finder xfinder = (StoredXAttr.Finder) finder;
    switch (xfinder){
      case ByPrimaryKey:
        return findByPrimaryKey(xfinder, params);
      case ByPrimaryKeyLocal:
        return findByPrimaryKeyLocal(xfinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  @Override
  public Collection<StoredXAttr> findList(FinderType<StoredXAttr> finder,
      Object... params) throws TransactionContextException, StorageException {
    StoredXAttr.Finder xfinder = (StoredXAttr.Finder) finder;
    switch (xfinder){
      case ByInodeId:
        return findByInodeId(xfinder, params);
      case ByInodeIdLocal:
        return findByInodeIdLocal(xfinder, params);
      case ByPrimaryKeyBatch:
        return findByPrimaryKeyBatch(xfinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  private StoredXAttr findByPrimaryKey(StoredXAttr.Finder finder,
      Object[] params) throws StorageException, StorageCallPreventedException {
    final StoredXAttr.PrimaryKey pk = (StoredXAttr.PrimaryKey) params[0];
    StoredXAttr result = null;
    if(contains(pk)){
      result = get(pk);
      hit(finder, result, "pk", pk, "results", result);
    }else{
      aboutToAccessStorage(finder, params);
      List<StoredXAttr> results =
          dataAccess.getXAttrsByPrimaryKeyBatch(Arrays.asList(pk));
      result = results.get(0);
      gotFromDB(pk, result);
      miss(finder, result, "pk", pk, "results", results);
    }
    return result;
  }
  
  private StoredXAttr findByPrimaryKeyLocal(StoredXAttr.Finder finder,
      Object[] params) throws StorageException, StorageCallPreventedException {
    final StoredXAttr.PrimaryKey pk = (StoredXAttr.PrimaryKey) params[0];
    StoredXAttr result = null;
    if(contains(pk)){
      result = get(pk);
      hit(finder, result, "pk", pk, "results", result);
    }
    return result;
  }
  
  private Collection<StoredXAttr> findByInodeId(StoredXAttr.Finder finder,
      Object[] params) throws StorageException, StorageCallPreventedException {
    final long inodeId = (Long) params[0];
    Collection<StoredXAttr> results = null;
    if(xAttrsByInodeId.containsKey(inodeId)){
      results = xAttrsByInodeId.get(inodeId);
      hit(finder, results, "inodeId", inodeId, "results", results);
    }else{
      aboutToAccessStorage(finder, params);
      results = dataAccess.getXAttrsByInodeId(inodeId);
      gotFromDB(results);
      xAttrsByInodeId.put(inodeId, results);
      miss(finder, results, "inodeId", inodeId, "results", results);
    }
    return results;
  }
  
  private Collection<StoredXAttr> findByInodeIdLocal(StoredXAttr.Finder finder,
      Object[] params) throws StorageException, StorageCallPreventedException {
    final long inodeId = (Long) params[0];
    Collection<StoredXAttr> results = null;
    if(xAttrsByInodeId.containsKey(inodeId)){
      results = xAttrsByInodeId.get(inodeId);
      hit(finder, results, "inodeId", inodeId, "results", results);
    }
    return results;
  }
  
  private Collection<StoredXAttr> findByPrimaryKeyBatch(StoredXAttr.Finder finder,
      Object[] params) throws StorageException, StorageCallPreventedException {
    final List<StoredXAttr.PrimaryKey> pks = (List<StoredXAttr.PrimaryKey>) params[0];
    List<StoredXAttr> results = null;
    if(containsAll(pks)){
      results = getAll(pks);
      hit(finder, results, "pks", pks, "results", results);
    }else{
      aboutToAccessStorage(finder, params);
      results = dataAccess.getXAttrsByPrimaryKeyBatch(pks);
      gotFromDB(pks, results);
      miss(finder, results, "pks", pks, "results", results);
    }
    return results;
  }
  
  private void gotFromDB(List<StoredXAttr.PrimaryKey> pks, List<StoredXAttr> results){
    Set<StoredXAttr.PrimaryKey> notFoundPks = Sets.newHashSet(pks);
    for(StoredXAttr attr : results){
      gotFromDB(attr);
      if(!xAttrsByInodeId.containsKey(attr.getInodeId())){
        xAttrsByInodeId.put(attr.getInodeId(), new ArrayList<StoredXAttr>());
      }
      xAttrsByInodeId.get(attr.getInodeId()).add(attr);
      notFoundPks.remove(attr.getPrimaryKey());
    }
    
    for(StoredXAttr.PrimaryKey pk : notFoundPks){
      gotFromDB(pk, null);
      if(!xAttrsByInodeId.containsKey(pk.getInodeId())){
        xAttrsByInodeId.put(pk.getInodeId(), null);
      }
    }
  }
  
  private boolean containsAll(List<StoredXAttr.PrimaryKey> pks){
    for(StoredXAttr.PrimaryKey pk : pks){
      if(!contains(pk)){
        return false;
      }
    }
    return true;
  }
  
  private List<StoredXAttr> getAll(List<StoredXAttr.PrimaryKey> pks){
    List<StoredXAttr> attrs = Lists.newArrayListWithExpectedSize(pks.size());
    for(StoredXAttr.PrimaryKey pk : pks){
      attrs.add(get(pk));
    }
    return attrs;
  }
  
  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds =
        (HdfsTransactionContextMaintenanceCmds) cmds;
    if(hopCmds == HdfsTransactionContextMaintenanceCmds.NoXAttrsAttached){
      Long inodeId = (Long) params[0];
      xAttrsByInodeId.put(inodeId, null);
    }
  }
  
  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    xAttrsByInodeId.clear();
  }
  
  @Override
  public void add(StoredXAttr attr) throws TransactionContextException {
    if (!xAttrsByInodeId.containsKey(attr.getInodeId()) || xAttrsByInodeId.get(attr.getInodeId()) == null) {
      xAttrsByInodeId.put(attr.getInodeId(), new ArrayList<StoredXAttr>());
    }
    StoredXAttr toRemove = null;
    for (StoredXAttr sattr : xAttrsByInodeId.get(attr.getInodeId())) {
      if (sattr.getPrimaryKey().equals(attr.getPrimaryKey())) {
        toRemove = sattr;
        break;
      }
    }
    if (toRemove != null) {
      xAttrsByInodeId.remove(attr);
    }
    xAttrsByInodeId.get(attr.getInodeId()).add(attr);
    super.add(attr);
  }
}
