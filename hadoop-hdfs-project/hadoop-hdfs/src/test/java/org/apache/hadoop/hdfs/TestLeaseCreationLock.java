/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the cd Apache License, Version 2.0 (the
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
package org.apache.hadoop.hdfs;

import io.hops.transaction.lock.LeaseCreationLockComparator;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LEASE_CREATION_LOCKS_COUNT_DEFAULT;

public class TestLeaseCreationLock {

  // this lock order for the lease creation table was wrong.
  // this test produces the problem and tests that the new fix is
  // deadlock free
  @Test
  public void testLeaseOrder() throws Exception {

    int LEASE_CREATION_LOCK_ROWS = DFS_LEASE_CREATION_LOCKS_COUNT_DEFAULT;
    ArrayList<String> op0 = new ArrayList<>();
    op0.add("/Aa");       // hash  47279
    op0.add("/AaAaAaAa"); // hash 408560175

    ArrayList<String> op1 = new ArrayList<>();
    op1.add("/BB");       // hash  47279
    op1.add("/AaAaBBBB"); // hash 408560175


    // old mechanism that lead to cycles in locking
    // op0 locks /Aa then /AaAaAaAa, that is it locks rows with ids 279 and then 175
    // op1 locks /AaAaBBBB then /BB, that is it locks rows with ids 175 and then 279
    Collections.sort(op0);
    Collections.sort(op1);

    assert Math.abs(Lease.getHolderId(op0.get(0))) % LEASE_CREATION_LOCK_ROWS == Math.abs(Lease.getHolderId(op1.get(1))) % LEASE_CREATION_LOCK_ROWS;
    assert Math.abs(Lease.getHolderId(op0.get(1))) % LEASE_CREATION_LOCK_ROWS == Math.abs(Lease.getHolderId(op1.get(0))) % LEASE_CREATION_LOCK_ROWS;


    Collections.sort(op0, new LeaseCreationLockComparator(LEASE_CREATION_LOCK_ROWS));
    Collections.sort(op1, new LeaseCreationLockComparator(LEASE_CREATION_LOCK_ROWS));

    // new  mechanism that sorts the paths by lease creation row ids
    // op0 locks /AaAaAaAa then /Aa, that is it locks rows with ids 175 and then 279
    // op1 locks /AaAaBBBB then /BB, that is it locks rows with ids 175 and then 279
    assert Math.abs(Lease.getHolderId(op0.get(0))) % LEASE_CREATION_LOCK_ROWS == Math.abs(Lease.getHolderId(op1.get(0))) % LEASE_CREATION_LOCK_ROWS;
    assert Math.abs(Lease.getHolderId(op0.get(1))) % LEASE_CREATION_LOCK_ROWS == Math.abs(Lease.getHolderId(op1.get(1))) % LEASE_CREATION_LOCK_ROWS;
  }
}

