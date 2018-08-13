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
import java.io.IOException;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CacheDirectiveLock extends Lock {
  private final Long id;
  private final LockType lockType;
  private final String poolName;
  private final int maxNumResults;
  private final String path;
  
  private List<CacheDirective> directives = null;
  
  
  public CacheDirectiveLock(long id) {
    this.lockType = TransactionLockTypes.LockType.WRITE;
    this.id = id;
    this.poolName=null;
    this.maxNumResults = -1;
    this.path = null;
  }
  
  public CacheDirectiveLock(String poolName) {
    this.lockType = TransactionLockTypes.LockType.WRITE;
    this.id = null;
    this.poolName = poolName;
    this.maxNumResults = -1;
    this.path = null;
  }
  
  public CacheDirectiveLock(long id, final String path, final String pool, int maxNumResults){
    this.lockType = TransactionLockTypes.LockType.READ;
    this.id = id;
    this.poolName = pool;
    this.path = path;
    this.maxNumResults = maxNumResults;
  }
    
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(lockType);
    if (maxNumResults>0) {
      listAll();
    } else {
      if (id != null) {
        directives = new ArrayList<>(1);
        directives.add(EntityManager.find(CacheDirective.Finder.ById, id));
      }
      if (poolName != null) {
        EntityManager.findList(CacheDirective.Finder.ByPoolName, poolName);
      }
    }
    
  }
  
  private void listAll() throws TransactionContextException, StorageException{
    directives = (List<CacheDirective>) EntityManager.
        findList(CacheDirective.Finder.ByIdPoolAndPath, id, poolName, path, maxNumResults);
  }

  @Override
  protected Type getType() {
    return Type.cacheDirective;
  }
  
  public List<CacheDirective> getDirectives(){
    return directives;
  }
  
}
