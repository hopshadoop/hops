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

import io.hops.transaction.EntityManager;
import java.io.IOException;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.CachePool;

public class CachePoolLock extends Lock {
  
  private final List<String> poolNames;
  private final LockType lockType;
  
  public CachePoolLock(String poolName) {
    this.poolNames = new ArrayList<>(1);
    poolNames.add(poolName);
    this.lockType = TransactionLockTypes.LockType.WRITE;
  }
  
  public CachePoolLock(List<String> poolNames) {
    this(poolNames, TransactionLockTypes.LockType.WRITE);
  }
  
  public CachePoolLock(LockType lockType) {
    this(null, lockType);
  }
    
  public CachePoolLock(List<String> poolNames, LockType lockType) {
    this.poolNames = poolNames;
    this.lockType = lockType;
  }
 
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    setLockMode(lockType);
    if(poolNames!=null && !poolNames.isEmpty()){
      for(String poolName: poolNames){
        EntityManager.find(CachePool.Finder.ByName, poolName);
      }
    } else {
      EntityManager.findList(CachePool.Finder.All);
    }
  }

  @Override
  protected Type getType() {
    return Type.cachePool;
  }
}
