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
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.transaction.lock.TransactionLocks;
import java.util.ArrayList;
import java.util.Collection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncodingStatusContext
    extends BaseEntityContext<Integer, EncodingStatus> {

  private final EncodingStatusDataAccess<EncodingStatus> dataAccess;
  private final Map<Integer, EncodingStatus> parityInodeIdToEncodingStatus =
      new HashMap<>();

  public EncodingStatusContext(
      EncodingStatusDataAccess<EncodingStatus> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(EncodingStatus encodingStatus)
      throws TransactionContextException {
    super.update(encodingStatus);
    addInternal(encodingStatus);
  }

  @Override
  public void remove(EncodingStatus encodingStatus)
      throws TransactionContextException {
    if (!contains(encodingStatus.getInodeId())) {
      update(encodingStatus);
    }
    super.remove(encodingStatus);
    removeInternal(encodingStatus);
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    parityInodeIdToEncodingStatus.clear();
  }

  @Override
  public EncodingStatus find(FinderType<EncodingStatus> finder,
      Object... params) throws TransactionContextException, StorageException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;

    Integer inodeId = (Integer) params[0];
    if (inodeId == null) {
      return null;
    }

    switch (eFinder) {
      case ByInodeId:
        return findByINodeId(eFinder, inodeId);
      case ByParityInodeId:
        return findByParityINodeId(eFinder, inodeId);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<EncodingStatus> findList(FinderType<EncodingStatus> finder,
      Object... params) throws TransactionContextException, StorageException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;

    switch (eFinder) {
      case ByInodeIds:
        return findByINodeIds(eFinder, params);
      case ByParityInodeIds:
        return findByParityINodeIds(eFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  @Override
  public int count(CounterType<EncodingStatus> counter, Object... params)
      throws TransactionContextException, StorageException {
    EncodingStatus.Counter eCounter = (EncodingStatus.Counter) counter;
    switch (eCounter) {
      case RequestedEncodings:
        return dataAccess.countRequestedEncodings();
      case ActiveEncodings:
        return dataAccess.countActiveEncodings();
      case ActiveRepairs:
        return dataAccess.countActiveRepairs();
      case Encoded:
        return dataAccess.countEncoded();
      default:
        throw new RuntimeException(UNSUPPORTED_COUNTER);
    }
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    for (EncodingStatus status : getAdded()) {
      dataAccess.add(status);
    }

    for (EncodingStatus status : getModified()) {
      dataAccess.update(status);
    }

    for (EncodingStatus status : getRemoved()) {
      dataAccess.delete(status);
    }
  }

  @Override
  Integer getKey(EncodingStatus encodingStatus) {
    return encodingStatus.getInodeId();
  }

  private EncodingStatus findByINodeId(EncodingStatus.Finder eFinder,
      final int inodeId)
      throws StorageCallPreventedException, StorageException {
    EncodingStatus result = null;
    if (contains(inodeId)) {
      result = get(inodeId);
      hit(eFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(eFinder, inodeId);
      result = dataAccess.findByInodeId(inodeId);
      gotFromDB(inodeId, result);
      addInternal(result);
      miss(eFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private Collection<EncodingStatus> findByINodeIds(EncodingStatus.Finder eFinder, Object... params)
      throws StorageCallPreventedException, StorageException {
    Collection<Integer> inodeIds = (Collection<Integer>) params[0];
    Collection<EncodingStatus> result = new ArrayList<>(inodeIds.size());
    Collection<Integer> toGetFromDB = new ArrayList<>();
    for (int id : inodeIds) {
      if (contains(id)) {
        result.add(get(id));
        hit(eFinder, get(id), "inodeid", id);
      } else {
        toGetFromDB.add(id);
      }
    }
    if (!toGetFromDB.isEmpty()) {
      aboutToAccessStorage(eFinder, toGetFromDB);
      Collection<EncodingStatus> gotFromDB = dataAccess.findByInodeIds(toGetFromDB);
      for (EncodingStatus s : gotFromDB) {
        gotFromDB(s.getInodeId(), s);
        addInternal(s);
        miss(eFinder, s, "inodeid", s.getInodeId());
        result.add(s);
      }
      for (int id : toGetFromDB) {
        if (!contains(id)) {
          EncodingStatus s = null;
          gotFromDB(id, s);
          miss(eFinder, s, "inodeid", id);
        }
      }
    }
    return result;
  }

  private EncodingStatus findByParityINodeId(EncodingStatus.Finder eFinder,
      final int pairtyINodeId)
      throws StorageCallPreventedException, StorageException {
    EncodingStatus result = null;
    if (parityInodeIdToEncodingStatus.containsKey(pairtyINodeId)) {
      result = parityInodeIdToEncodingStatus.get(pairtyINodeId);
      hit(eFinder, result, "parityinodeid", pairtyINodeId);
    } else {
      aboutToAccessStorage(eFinder, pairtyINodeId);
      result = dataAccess.findByParityInodeId(pairtyINodeId);
      gotFromDB(result);
      addInternal(pairtyINodeId, result);
      miss(eFinder, result, "parityinodeid", pairtyINodeId);
    }
    return result;
  }

  private Collection<EncodingStatus> findByParityINodeIds(EncodingStatus.Finder eFinder, Object... params)
      throws StorageCallPreventedException, StorageException {
    Collection<Integer> inodeIds = (Collection<Integer>) params[0];
    Collection<EncodingStatus> result = new ArrayList<>(inodeIds.size());
    List<Integer> toGetFromDB = new ArrayList<>();
    for (int pairtyINodeId : inodeIds) {
      if (parityInodeIdToEncodingStatus.containsKey(pairtyINodeId)) {
        result.add(parityInodeIdToEncodingStatus.get(pairtyINodeId));
        hit(eFinder, parityInodeIdToEncodingStatus.get(pairtyINodeId), "parityinodeid", pairtyINodeId);
      } else {
        toGetFromDB.add(pairtyINodeId);
      }
    }
    if (!toGetFromDB.isEmpty()) {
      aboutToAccessStorage(eFinder, toGetFromDB);
      Collection<EncodingStatus> gotFromDB = dataAccess.findByParityInodeIds(toGetFromDB);
      if(gotFromDB!=null){
      for (EncodingStatus s : gotFromDB) {
        gotFromDB(s);
        addInternal(s.getParityInodeId(), s);
        miss(eFinder, s, "parityinodeid", s.getParityInodeId());
        result.add(s);
      }
      }
      for(int id: toGetFromDB){
        if(!parityInodeIdToEncodingStatus.containsKey(id)){
          EncodingStatus s = null;
          addInternal(id, s);
          miss(eFinder, s, "parityinodeid", id);
        }
      }
    }

    return result;
  }

  private void addInternal(EncodingStatus encodingStatus) {
    if (encodingStatus != null && encodingStatus.getParityInodeId() != null) {
      addInternal(encodingStatus.getParityInodeId(), encodingStatus);
    }
  }

  private void addInternal(int parityINodeId, EncodingStatus encodingStatus) {
    parityInodeIdToEncodingStatus.put(parityINodeId, encodingStatus);
  }

  private void removeInternal(EncodingStatus encodingStatus) {
    if (encodingStatus != null && encodingStatus.getParityInodeId() != null) {
      parityInodeIdToEncodingStatus.remove(encodingStatus.getParityInodeId());
    }
  }

}
