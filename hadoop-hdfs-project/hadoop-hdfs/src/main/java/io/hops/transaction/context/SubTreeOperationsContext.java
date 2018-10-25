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

import com.google.common.base.Predicate;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.lock.TransactionLocks;
import java.util.ArrayList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SubTreeOperationsContext extends BaseEntityContext<String, SubTreeOperation> {

  private final OngoingSubTreeOpsDataAccess<SubTreeOperation> dataAccess;
  private final Map<String, Collection<SubTreeOperation>> pathToSubTreeOperation =
      new HashMap<>();

  public SubTreeOperationsContext(OngoingSubTreeOpsDataAccess<SubTreeOperation> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(SubTreeOperation hopSubTeeOperation)
      throws TransactionContextException {
    super.update(hopSubTeeOperation);
    addInternal(hopSubTeeOperation);
    if(isLogTraceEnabled()) {
      log("updated-sub-tree-op", "path", hopSubTeeOperation.getPath(), "nnid",
              hopSubTeeOperation.getNameNodeId(), "op", hopSubTeeOperation.getOpType());
    }
  }

  @Override
  public void remove(SubTreeOperation hopSubTeeOperation)
      throws TransactionContextException {
    super.forceRemove(hopSubTeeOperation);
    if(isLogTraceEnabled()) {
      log("removed-sub-tree-op", "path", hopSubTeeOperation.getPath(), "nnid",
              hopSubTeeOperation.getNameNodeId(), "op", hopSubTeeOperation.getOpType());
    }
  }

  @Override
  public SubTreeOperation find(FinderType<SubTreeOperation> finder, Object... params)
      throws TransactionContextException, StorageException {
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<SubTreeOperation> findList(FinderType<SubTreeOperation> finder,
      Object... params) throws TransactionContextException, StorageException {
    SubTreeOperation.Finder lFinder = (SubTreeOperation.Finder) finder;
    switch (lFinder) {
      case ByPathPrefix:
        return findByPathPrefix(lFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    pathToSubTreeOperation.clear();
  }

  @Override
  String getKey(SubTreeOperation hopSubTeeOperation) {
    return hopSubTeeOperation.getPath();
  }

  private Collection<SubTreeOperation> findByPathPrefix(SubTreeOperation.Finder lFinder,
      Object[] params) throws StorageCallPreventedException, StorageException {
    final String prefix = (String) params[0];
    Collection<SubTreeOperation> result = null;
    
    if(pathToSubTreeOperation.containsKey(prefix)){
      result = pathToSubTreeOperation.get(prefix);
      hit(lFinder, result, "Path Prefix", prefix, "numOfOps", result.size());
    }else{
      aboutToAccessStorage(lFinder, params);
      result = dataAccess.findByPathsByPrefix(prefix);
      gotFromDBInternal(prefix, result);
      miss(lFinder, result, "Path Prefix", prefix, "numOfOps", result.size());
    }
    return result;
  }


  @Override
  void gotFromDB(Collection<SubTreeOperation> subTreeOps) {
    super.gotFromDB(subTreeOps);
  }

  private void gotFromDBInternal(String pathPrefix, Collection<SubTreeOperation> subTreeOps) {
    gotFromDB(subTreeOps);
    addInternal(pathPrefix, subTreeOps);
    
  }

  private void addInternal(String prefix, Collection<SubTreeOperation> subTreeOps) {
    Collection<SubTreeOperation> list = pathToSubTreeOperation.get(prefix);
    if(list == null){
            list = new ArrayList();
            pathToSubTreeOperation.put(prefix, list); 
    } 
    if(subTreeOps != null) {        
        list.addAll(subTreeOps);
        pathToSubTreeOperation.put(prefix, list);
   }
    
  }
  
  private void addInternal(SubTreeOperation op) {
    Collection<SubTreeOperation> list = new ArrayList();
    list.add(op);
    addInternal(op.getPath(), list );
  }
}
