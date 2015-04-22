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
package io.hops.transaction.lock;

import io.hops.metadata.election.entity.LeDescriptorFactory;
import java.io.IOException;

final class LeDescriptorLock extends Lock {
  private final TransactionLockTypes.LockType lockType;
  private LeDescriptorFactory leFactory = null;

  LeDescriptorLock(LeDescriptorFactory leFactory,
      TransactionLockTypes.LockType lockType) {
    this.lockType = lockType;
    this.leFactory = leFactory;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    acquireLockList(lockType, leFactory.getAllFinder());
  }

  @Override
  protected final Type getType() {
    return Type.LeDescriptor;
  }
  
  TransactionLockTypes.LockType getLockType() {
    return lockType;
  }
}
