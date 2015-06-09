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

import io.hops.metadata.hdfs.entity.SubTreeOperation;

import java.io.IOException;

public final class SubTreeOpLock extends Lock {

  private final TransactionLockTypes.LockType lockType;
  private final String pathPrefix;

  SubTreeOpLock(TransactionLockTypes.LockType lockType, String pathPrefix) {
    this.lockType = lockType;
    this.pathPrefix = pathPrefix;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    acquireLockList(lockType, SubTreeOperation.Finder.ByPathPrefix, pathPrefix);
  }

  @Override
  protected final Type getType() {
    return Type.SubTreePath;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }
}
