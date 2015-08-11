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

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.io.IOException;

final class NameNodeLeaseLock extends Lock {
  private final TransactionLockTypes.LockType lockType;
  private Lease nameNodeLease;

  NameNodeLeaseLock(TransactionLockTypes.LockType lockType) {
    this.lockType = lockType;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    nameNodeLease = acquireLock(lockType, Lease.Finder.ByHolder,
        HdfsServerConstants.NAMENODE_LEASE_HOLDER, HdfsServerConstants.NAMENODE_LEASE_HOLDER_ID);
  }

  @Override
  protected final Type getType() {
    return Type.NameNodeLease;
  }

  public Lease getNameNodeLease() {
    return nameNodeLease;
  }
}
