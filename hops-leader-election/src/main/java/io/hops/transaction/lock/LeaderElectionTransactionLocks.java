/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class LeaderElectionTransactionLocks implements TransactionLocks {

  private final Map<Lock.Type, Lock> locks;
  
  public LeaderElectionTransactionLocks() {
    this.locks = new EnumMap<Lock.Type, Lock>(Lock.Type.class);
  }
  
  @Override
  public TransactionLocks add(Lock lock) {
    if (locks.containsKey(lock.getType())) {
      throw new IllegalArgumentException(
          "The same lock cannot be added " + "twice!");
    }
    
    locks.put(lock.getType(), lock);
    return this;
  }

  @Override
  public TransactionLocks add(Collection<Lock> locks) {
    for (Lock lock : locks) {
      add(lock);
    }
    return this;
  }

  @Override
  public boolean containsLock(Lock.Type lock) {
    return locks.containsKey(lock);
  }
  
  @Override
  public Lock getLock(Lock.Type type) throws LockNotAddedException {
    if (!locks.containsKey(type)) {
      throw new LockNotAddedException(
          "Trying to get a lock which was not " + "added.");
    }
    return locks.get(type);
  }

  public List<Lock> getSortedLocks() {
    List<Lock> lks = new ArrayList<Lock>(locks.values());
    Collections.sort(lks);
    return lks;
  }


}
