/*
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
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.lock.TransactionLocks;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.text.html.parser.Entity;

public class AcesContext
    extends BaseEntityContext<Ace.PrimaryKey, Ace> {
  
  private AceDataAccess<Ace> dataAccess;
  private Map<Long, List<Ace>> inodeAces = new HashMap<>();
  
  public AcesContext(AceDataAccess<Ace> aceDataAccess) {
    this.dataAccess = aceDataAccess;
  }
  
  @Override
  public Collection<Ace> findList(FinderType<Ace> finder, Object... params)
      throws TransactionContextException, StorageException {
    Ace.Finder aceFinder = (Ace.Finder) finder;
    switch (aceFinder){
      case ByInodeIdAndIndices:
        return findByPKBatched(aceFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  private Collection<Ace> findByPKBatched(Ace.Finder aceFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    long inodeId = (long) params[0];
    int[] aceIds = (int[]) params[1];
    
    List<Ace> result;
    if(inodeAces.containsKey(inodeId)){
      result = inodeAces.get(inodeId);
      hit(aceFinder, result, "inodeId", inodeId);
    } else {
      aboutToAccessStorage(aceFinder, params);
      result = dataAccess.getAcesByPKBatched(inodeId, aceIds);
      inodeAces.put(inodeId, result);
      gotFromDB(result);
      miss(aceFinder, result, "inodeId", inodeId);
    }
    
    return result;
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(),getAdded(),getModified());
  }
  
  @Override
  Ace.PrimaryKey getKey(Ace ace) {
    return new Ace.PrimaryKey(ace.getInodeId(), ace.getIndex());
  }
  
  @Override
  public void update(Ace ace) throws TransactionContextException {
    super.update(ace);
    List<Ace> aces = inodeAces.get(ace.getInodeId());
    if(aces==null){
      aces = new ArrayList<>();
      inodeAces.put(ace.getInodeId(), aces);
    }
    aces.add(ace);
  }
}