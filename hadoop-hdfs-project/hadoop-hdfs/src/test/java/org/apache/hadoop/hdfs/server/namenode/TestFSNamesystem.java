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

package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFSNamesystem {

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Test that FSNamesystem#clear clears all leases.
   */
  @Test
  public void testFSNamespaceClearLeases() throws Exception {
    Configuration conf = new HdfsConfiguration();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf, null);
    LeaseManager leaseMan = fsn.getLeaseManager();
    addLease(leaseMan, "client1", "importantFile");
    assertEquals(1, leaseMan.countLease());
    fsn.clear();
    leaseMan = fsn.getLeaseManager();
    assertEquals(0, leaseMan.countLease());
  }
  
  private void addLease(final LeaseManager leaseMan, final String holder,
      final String src) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getLeaseLockAllPaths(LockType.WRITE, holder));
      }
      
      @Override
      public Object performTask() throws StorageException, IOException {
        leaseMan.addLease(holder, src);
        return null;
      }
    }.handle();
  }
  
  @Test
  /**
   * Test that isInStartupSafemode returns true only during startup safemode
   * and not also during low-resource safemode
   */
  public void testStartupSafemode() throws IOException {
    Configuration conf = new HdfsConfiguration();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    NameNode nameNode = Mockito.mock(NameNode.class);
    Mockito.doReturn(true).when(nameNode).isLeader();
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf, nameNode);

    fsn.leaveSafeMode();
    assertTrue("After leaving safemode FSNamesystem.isInStartupSafeMode still "
        + "returned true", !fsn.isInStartupSafeMode());
    assertTrue("After leaving safemode FSNamesystem.isInSafeMode still returned"
        + " true", !fsn.isInSafeMode());

    fsn.enterSafeMode(true);
    assertTrue("After entering safemode due to low resources FSNamesystem."
        + "isInStartupSafeMode still returned true", !fsn.isInStartupSafeMode());
    assertTrue("After entering safemode due to low resources FSNamesystem."
        + "isInSafeMode still returned false", fsn.isInSafeMode());
  }
  
  @Test
  public void testReplQueuesActiveAfterStartupSafemode() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    NameNode nameNode = Mockito.mock(NameNode.class);
    Mockito.doReturn(true).when(nameNode).isLeader();
    FSNamesystem fsNamesystem = FSNamesystem.loadFromDisk(conf, nameNode);
    FSNamesystem fsn = Mockito.spy(fsNamesystem);

    //Make NameNode.getNameNodeMetrics() not return null
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);

    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues were being populated during very first "
        + "safemode", !fsn.isPopulatingReplQueues());
    fsn.leaveSafeMode();
    assertTrue("FSNamesystem didn't leave safemode", !fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated even after leaving "
        + "safemode", fsn.isPopulatingReplQueues());
    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated after entering "
        + "safemode 2nd time", fsn.isPopulatingReplQueues());
  }
  
  @Test
  public void testReset() throws Exception {
    Configuration conf = new Configuration();
    DFSTestUtil.formatNameNode(conf);
    NameNode nn = mock(NameNode.class);
    when(nn.getActiveNameNodes())
        .thenReturn(new SortedActiveNodeListPBImpl(Collections.EMPTY_LIST));
    FSNamesystem fsn = new FSNamesystem(conf, nn);
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
    fsn.clear();
    assertFalse(fsn.isImageLoaded());
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
  }
}