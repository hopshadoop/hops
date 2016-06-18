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
package org.apache.hadoop.hdfs;

import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.BaseTestLock;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.hdfs.server.namenode.TestSubtreeLock;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestLease {

  public static class TestLeaseLock extends BaseTestLock {
    private final LockType leasePathLock;
    private final LockType leaseLock;
    private final String leasePath;

    public TestLeaseLock(LockType leasePathLock, LockType leaseLock,
        String leasePath) {
      this.leasePathLock = leasePathLock;
      this.leaseLock = leaseLock;
      this.leasePath = leasePath;
    }

    @Override
    protected void acquire(TransactionLocks locks) throws IOException {
      LeasePath lp =
          acquireLock(leasePathLock, LeasePath.Finder.ByPath, leasePath);
      if (lp != null) {
        acquireLock(leaseLock, Lease.Finder.ByHolderId, lp.getHolderId());
      }
    }
  }

  static boolean hasLease(final MiniDFSCluster cluster, final Path src)
      throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        locks.add(new TestLeaseLock(LockType.READ, LockType.WRITE,
            src.toString()));
      }

      @Override
      public Object performTask() throws IOException {
        return NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
            .getLeaseByPath(src.toString()) != null;
      }
    }.handle();
  }


  static int leaseCount(MiniDFSCluster cluster) throws IOException {
    return NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .countLease();
  }
  
  static final String dirString = "/test/lease";
  final Path dir = new Path(dirString);
  static final Log LOG = LogFactory.getLog(TestLease.class);
  Configuration conf = new HdfsConfiguration();

  @Test
  public void testLeaseAbort() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      cluster.waitActive();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);

      DFSClient dfs = new DFSClient(null, spyNN, conf, null);
      byte[] buf = new byte[1024];

      FSDataOutputStream c_out = createFsOut(dfs, dirString + "c");
      c_out.write(buf, 0, 1024);
      c_out.close();

      DFSInputStream c_in = dfs.open(dirString + "c");
      FSDataOutputStream d_out = createFsOut(dfs, dirString + "d");

      // stub the renew method.
      doThrow(new RemoteException(InvalidToken.class.getName(),
          "Your token is worthless")).when(spyNN).renewLease(anyString());

      // We don't need to wait the lease renewer thread to act.
      // call renewLease() manually.
      // make it look like the soft limit has been exceeded.
      LeaseRenewer originalRenewer = dfs.getLeaseRenewer();
      dfs.lastLeaseRenewal =
          Time.now() - HdfsConstants.LEASE_SOFTLIMIT_PERIOD - 1000;
      try {
        dfs.renewLease();
      } catch (IOException e) {
      }

      // Things should continue to work it passes hard limit without
      // renewing.
      try {
        d_out.write(buf, 0, 1024);
        LOG.info("Write worked beyond the soft limit as expected.");
      } catch (IOException e) {
        Assert.fail("Write failed.");
      }

      // make it look like the hard limit has been exceeded.
      dfs.lastLeaseRenewal =
          Time.now() - HdfsConstants.LEASE_HARDLIMIT_PERIOD - 1000;
      dfs.renewLease();

      // this should not work.
      try {
        d_out.write(buf, 0, 1024);
        d_out.close();
        Assert.fail(
            "Write did not fail even after the fatal lease renewal failure");
      } catch (IOException e) {
        LOG.info("Write failed as expected. ", e);
      }

      // If aborted, the renewer should be empty. (no reference to clients)
      Thread.sleep(1000);
      Assert.assertTrue(originalRenewer.isEmpty());

      // unstub
      doNothing().when(spyNN).renewLease(anyString());

      // existing input streams should work
      try {
        int num = c_in.read(buf, 0, 1);
        if (num != 1) {
          Assert.fail("Failed to read 1 byte");
        }
        c_in.close();
      } catch (IOException e) {
        LOG.error("Read failed with ", e);
        Assert.fail("Read after lease renewal failure failed");
      }

      // new file writes should work.
      try {
        c_out = createFsOut(dfs, dirString + "c");
        c_out.write(buf, 0, 1024);
        c_out.close();
      } catch (IOException e) {
        LOG.error("Write failed with ", e);
        Assert.fail("Write failed");
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testLeaseAfterRename() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      Path p = new Path("/test-file");
      Path d = new Path("/test-d");
      Path d2 = new Path("/test-d-other");

      // open a file to get a lease
      FileSystem fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(p);
      out.writeBytes("something");
      //out.hsync();
      Assert.assertTrue(hasLease(cluster, p));
      Assert.assertEquals(1, leaseCount(cluster));
      
      // just to ensure first fs doesn't have any logic to twiddle leases
      DistributedFileSystem fs2 = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      // rename the file into an existing dir
      LOG.info("DMS: rename file into dir");
      Path pRenamed = new Path(d, p.getName());
      fs2.mkdirs(d);
      fs2.rename(p, pRenamed);
      
      assertFalse("Subtree locks were not cleared properly",TestSubtreeLock.subTreeLocksExists());
      Assert.assertFalse(p + " exists", fs2.exists(p));
      Assert.assertTrue(pRenamed + " not found", fs2.exists(pRenamed));
      Assert.assertFalse("has lease for " + p, hasLease(cluster, p));
      Assert
          .assertTrue("no lease for " + pRenamed, hasLease(cluster, pRenamed));
      Assert.assertEquals(1, leaseCount(cluster));

      
      
      // rename the parent dir to a new non-existent dir
      LOG.info("DMS: rename parent dir");
      Path pRenamedAgain = new Path(d2, pRenamed.getName());
      fs2.rename(d, d2);
      assertFalse("Subtree locks were not cleared properly",TestSubtreeLock.subTreeLocksExists());
      
      // src gone
      Assert.assertFalse(d + " exists", fs2.exists(d));
      Assert.assertFalse("has lease for " + pRenamed,
          hasLease(cluster, pRenamed));
      // dst checks
      Assert.assertTrue(d2 + " not found", fs2.exists(d2));
      Assert
          .assertTrue(pRenamedAgain + " not found", fs2.exists(pRenamedAgain));
      Assert.assertTrue("no lease for " + pRenamedAgain,
          hasLease(cluster, pRenamedAgain));
      Assert.assertEquals(1, leaseCount(cluster));

      // rename the parent dir to existing dir
      // NOTE: rename w/o options moves paths into existing dir
      LOG.info("DMS: rename parent again");
      pRenamed = pRenamedAgain;
      pRenamedAgain = new Path(new Path(d, d2.getName()), p.getName());
      fs2.mkdirs(d);
      assertFalse("Subtree locks were not cleared properly",TestSubtreeLock.subTreeLocksExists());
      fs2.rename(d2, d);
      
      // src gone
      Assert.assertFalse(d2 + " exists", fs2.exists(d2));
      Assert
          .assertFalse("no lease for " + pRenamed, hasLease(cluster, pRenamed));
      // dst checks
      Assert.assertTrue(d + " not found", fs2.exists(d));
      Assert
          .assertTrue(pRenamedAgain + " not found", fs2.exists(pRenamedAgain));
      Assert.assertTrue("no lease for " + pRenamedAgain,
          hasLease(cluster, pRenamedAgain));
      Assert.assertEquals(1, leaseCount(cluster));
      
      // rename with opts to non-existent dir
      pRenamed = pRenamedAgain;
      pRenamedAgain = new Path(d2, p.getName());
      fs2.rename(pRenamed.getParent(), d2, Options.Rename.OVERWRITE);
      // src gone
      Assert.assertFalse(pRenamed.getParent() + " not found",
          fs2.exists(pRenamed.getParent()));
      Assert.assertFalse("has lease for " + pRenamed,
          hasLease(cluster, pRenamed));
      // dst checks
      Assert.assertTrue(d2 + " not found", fs2.exists(d2));
      Assert
          .assertTrue(pRenamedAgain + " not found", fs2.exists(pRenamedAgain));
      Assert.assertTrue("no lease for " + pRenamedAgain,
          hasLease(cluster, pRenamedAgain));
      Assert.assertEquals(1, leaseCount(cluster));

      // rename with opts to existing dir
      // NOTE: rename with options will not move paths into the existing dir
      pRenamed = pRenamedAgain;
      pRenamedAgain = new Path(d, p.getName());
      fs2.rename(pRenamed.getParent(), d, Options.Rename.OVERWRITE);
      // src gone
      Assert.assertFalse(pRenamed.getParent() + " not found",
          fs2.exists(pRenamed.getParent()));
      Assert.assertFalse("has lease for " + pRenamed,
          hasLease(cluster, pRenamed));
      // dst checks
      Assert.assertTrue(d + " not found", fs2.exists(d));
      Assert
          .assertTrue(pRenamedAgain + " not found", fs2.exists(pRenamedAgain));
      Assert.assertTrue("no lease for " + pRenamedAgain,
          hasLease(cluster, pRenamedAgain));
      Assert.assertEquals(1, leaseCount(cluster));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testLease() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Assert.assertTrue(fs.mkdirs(dir));
      
      Path a = new Path(dir, "a");
      Path b = new Path(dir, "b");

      DataOutputStream a_out = fs.create(a);
      a_out.writeBytes("something");

      Assert.assertTrue(hasLease(cluster, a));
      Assert.assertTrue(!hasLease(cluster, b));
      
      DataOutputStream b_out = fs.create(b);
      b_out.writeBytes("something");

      Assert.assertTrue(hasLease(cluster, a));
      Assert.assertTrue(hasLease(cluster, b));

      a_out.close();
      b_out.close();

      Assert.assertTrue(!hasLease(cluster, a));
      Assert.assertTrue(!hasLease(cluster, b));
      
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFactory() throws Exception {
    final String[] groups = new String[]{"supergroup"};
    final UserGroupInformation[] ugi = new UserGroupInformation[3];
    for (int i = 0; i < ugi.length; i++) {
      ugi[i] = UserGroupInformation.createUserForTesting("user" + i, groups);
    }

    final Configuration conf = new Configuration();
    final DFSClient c1 = createDFSClientAs(ugi[0], conf);
    FSDataOutputStream out1 = createFsOut(c1, "/out1");
    final DFSClient c2 = createDFSClientAs(ugi[0], conf);
    FSDataOutputStream out2 = createFsOut(c2, "/out2");
    Assert.assertEquals(c1.getLeaseRenewer(), c2.getLeaseRenewer());
    final DFSClient c3 = createDFSClientAs(ugi[1], conf);
    FSDataOutputStream out3 = createFsOut(c3, "/out3");
    Assert.assertTrue(c1.getLeaseRenewer() != c3.getLeaseRenewer());
    final DFSClient c4 = createDFSClientAs(ugi[1], conf);
    FSDataOutputStream out4 = createFsOut(c4, "/out4");
    Assert.assertEquals(c3.getLeaseRenewer(), c4.getLeaseRenewer());
    final DFSClient c5 = createDFSClientAs(ugi[2], conf);
    FSDataOutputStream out5 = createFsOut(c5, "/out5");
    Assert.assertTrue(c1.getLeaseRenewer() != c5.getLeaseRenewer());
    Assert.assertTrue(c3.getLeaseRenewer() != c5.getLeaseRenewer());
  }
  
  private FSDataOutputStream createFsOut(DFSClient dfs, String path)
      throws IOException {
    return new FSDataOutputStream(dfs.create(path, true), null);
  }

  static final ClientProtocol mcp = Mockito.mock(ClientProtocol.class);

  static public DFSClient createDFSClientAs(UserGroupInformation ugi,
      final Configuration conf) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
      @Override
      public DFSClient run() throws Exception {
        return new DFSClient(null, mcp, conf, null);
      }
    });
  }
}
