/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestLease;
import org.junit.Test;
import org.mockito.Mockito;

public class TestLeaseManager {

  Configuration conf = new HdfsConfiguration();

  @Test
  public void testRemoveLeaseWithPrefixPath() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    addLease(lm, "holder1", "/a/b");
    addLease(lm, "holder2", "/a/c");
    assertNotNull(getLeaseByPath(lm, "/a/b"));
    assertNotNull(getLeaseByPath(lm, "/a/c"));

    removeLeaseWithPrefixPath(lm, "/a");

    assertNull(getLeaseByPath(lm, "/a/b"));
    assertNull(getLeaseByPath(lm, "/a/c"));

    addLease(lm, "holder1", "/a/b");
    addLease(lm, "holder2", "/a/c");

    removeLeaseWithPrefixPath(lm, "/a/");

    assertNull(getLeaseByPath(lm, "/a/b"));
    assertNull(getLeaseByPath(lm, "/a/c"));
  }

  private void addLease(final LeaseManager lm, final String holder, final String path) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE, holder));
      }

      @Override
      public Object performTask() throws IOException {
        lm.addLease(holder, path);
        return null;
      }
    }.handle();
  }

  static Lease getLeaseByPath(final LeaseManager lm, final String path)
      throws IOException {
    return (Lease) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        locks.add(new TestLease.TestLeaseLock(TransactionLockTypes.LockType.READ, TransactionLockTypes.LockType.WRITE,
            path));
      }

      @Override
      public Object performTask() throws IOException {
        return lm.getLeaseByPath(path);
      }
    }.handle();
  }
  
  static void removeLeaseWithPrefixPath(final LeaseManager lm, final String path)
      throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
              .add(lf.getLeasePathLock(TransactionLockTypes.LockType.WRITE, path));
      }

      @Override
      public Object performTask() throws IOException {
        lm.removeLeaseWithPrefixPath(path);
        return null;
      }
    }.handle();
  }

  /** Check that even if LeaseManager.checkLease is not able to relinquish
   * leases, the Namenode does't enter an infinite loop while holding the FSN
   * write lock and thus become unresponsive
   */
  @Test (timeout=2000)
  public void testCheckLeaseNotInfiniteLoop() throws StorageException, TransactionContextException, IOException {
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    FSDirectory dir = Mockito.mock(FSDirectory.class);
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    Mockito.when(fsn.isRunning()).thenReturn(true);
    NameNode namenode = Mockito.mock(NameNode.class);
    Mockito.when(fsn.getNameNode()).thenReturn(namenode);
    Mockito.when(namenode.getId()).thenReturn(0L);
    SortedActiveNodeList sanl = Mockito.mock(SortedActiveNodeList.class);
    Mockito.when(namenode.getActiveNameNodes()).thenReturn(sanl);
    Mockito.when(sanl.getActiveNodes()).thenReturn(new ArrayList<ActiveNode>());
    Mockito.when(fsn.getFSDirectory()).thenReturn(dir);
    LeaseManager lm = new LeaseManager(fsn);

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(0,0);

    //Add some leases to the LeaseManager
    addLease(lm, "holder1", "/src1");
    addLease(lm, "holder2", "/src2");
    addLease(lm, "holder3", "/src3");
    assertEquals(lm.getNumSortedLeases(), 3);

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
  }
  
}
