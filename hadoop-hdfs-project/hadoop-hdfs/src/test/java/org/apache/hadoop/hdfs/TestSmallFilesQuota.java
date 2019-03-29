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

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LastUpdatedContentSummary;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.TestSubtreeLock;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class for testing quota-related commands
 */
public class TestSmallFilesQuota {

  public static final Log LOG = LogFactory.getLog(TestSmallFilesQuota.class);

  private void runCommand(DFSAdmin admin, boolean expectError, String... args)
      throws Exception {
    runCommand(admin, args, expectError);
  }
  
  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
      throws Exception {
    int val = admin.run(args);
    if (expectEror) {
      assertEquals(val, -1);
    } else {
      assertTrue(val >= 0);
    }
  }
  
  /**
   * Tests to make sure we're getting human readable Quota exception messages
   * Test for @link{ NSQuotaExceededException, DSQuotaExceededException}
   *
   * @throws Exception
   */
  @Test
  public void testDSQuotaExceededExceptionIsHumanReadable() throws Exception {
    Integer bytes = 1024;
    try {
      throw new DSQuotaExceededException(bytes, bytes);
    } catch (DSQuotaExceededException e) {
      
      assertEquals("The DiskSpace quota is exceeded: quota = 1024 B = 1 KB" +
          " but diskspace consumed = 1024 B = 1 KB", e.getMessage());
    }
  }
  
  /**
   * Test quota related commands:
   * setQuota, clrQuota, setSpaceQuota, clrSpaceQuota, and count
   */
  @Test
  public void testQuotaCommands() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 1000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: " + fs.getUri(),
        fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem) fs;

    dfs.setStoragePolicy(new Path("/"), "DB");

    DFSAdmin admin = new DFSAdmin(conf);
    
    try {
      final int fileLen = 1024;
      final short replication = 5;
      final long spaceQuota = fileLen * replication * 15 / 8;

      // 1: create a directory /test and set its quota to be 3
      final Path parent = new Path("/test");
      assertTrue(dfs.mkdirs(parent));
      String[] args = new String[]{"-setQuota", "3", parent.toString()};
      runCommand(admin, args, false);

      //try setting space quota with a 'binary prefix'
      runCommand(admin, false, "-setSpaceQuota", "2t", parent.toString());
      assertEquals(2L << 40, DFSTestUtil.getContentSummary(dfs, parent).getSpaceQuota());
      
      // set diskspace quota to 10000 
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota),
          parent.toString());

      // 2: create directory /test/data0
      final Path childDir0 = new Path(parent, "data0");
      assertTrue(dfs.mkdirs(childDir0));

      // 3: create a file /test/datafile0
      final Path childFile0 = new Path(parent, "datafile0");
      DFSTestUtil.createFile(fs, childFile0, fileLen, replication, 0);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 4: count -q /test
      ContentSummary c = DFSTestUtil.getContentSummary(dfs, parent);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 3);
      assertEquals(c.getSpaceConsumed(), fileLen * replication);
      assertEquals(c.getSpaceQuota(), spaceQuota);
      
      // 5: count -q /test/data0
      c = DFSTestUtil.getContentSummary(dfs, childDir0);
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), -1);
      // check disk space consumed
      c = DFSTestUtil.getContentSummary(dfs, parent);
      assertEquals(c.getSpaceConsumed(), fileLen * replication);

      // 6: create a directory /test/data1
      final Path childDir1 = new Path(parent, "data1");
      boolean hasException = false;
      try {
        // HOP - Wait for quota updates to be applied
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        assertFalse(dfs.mkdirs(childDir1));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      OutputStream fout;

      // 7: create a file /test/datafile1
      final Path childFile1 = new Path(parent, "datafile1");
      hasException = false;
      try {
        fout = dfs.create(childFile1);
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // 8: clear quota /test
      runCommand(admin, new String[]{"-clrQuota", parent.toString()}, false);
      c = DFSTestUtil.getContentSummary(dfs, parent);
      assertEquals(c.getQuota(), -1);
      assertEquals(c.getSpaceQuota(), spaceQuota);

      // 9: clear quota /test/data0
      runCommand(admin, new String[]{"-clrQuota", childDir0.toString()}, false);
      c = DFSTestUtil.getContentSummary(dfs, childDir0);
      assertEquals(c.getQuota(), -1);

      // 10: create a file /test/datafile1
      fout = dfs.create(childFile1, replication);

      // 10.s: but writing fileLen bytes should result in an quota exception
      hasException = false;
      try {
        // HOP - Write in single blocks and wait to trigger exception
        fout.write(new byte[fileLen / 2]);
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        fout.write(new byte[fileLen / 2]);
        fout.close();
      } catch (QuotaExceededException e) {
        hasException = true;
        IOUtils.closeStream(fout);
      }
      assertTrue(hasException);
      
      //delete the file
      dfs.delete(childFile1, false);

      // 9.s: clear diskspace quota
      runCommand(admin, false, "-clrSpaceQuota", parent.toString());
      c = DFSTestUtil.getContentSummary(dfs,parent);
      assertEquals(c.getQuota(), -1);
      assertEquals(c.getSpaceQuota(), -1);

      // now creating childFile1 should succeed
      DFSTestUtil.createFile(dfs, childFile1, fileLen, replication, 0);


      // 11: set the quota of /test to be 1
      // HADOOP-5872 - we can set quota even if it is immediately violated 
      args = new String[]{"-setQuota", "1", parent.toString()};
      runCommand(admin, args, false);
      runCommand(admin, false, "-setSpaceQuota",  // for space quota
          Integer.toString(fileLen), args[2]);
      if (true) {
        return;
      }
      // 12: set the quota of /test/data0 to be 1
      args = new String[]{"-setQuota", "1", childDir0.toString()};
      runCommand(admin, args, false);
      
      // 13: not able create a directory under data0
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(new Path(childDir0, "in")));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      c = DFSTestUtil.getContentSummary(dfs,childDir0);
      assertEquals(c.getDirectoryCount() + c.getFileCount(), 1);
      assertEquals(c.getQuota(), 1);
      
      // 14a: set quota on a non-existent directory
      Path nonExistentPath = new Path("/test1");
      assertFalse(dfs.exists(nonExistentPath));
      args = new String[]{"-setQuota", "1", nonExistentPath.toString()};
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", "1g", // for space quota
          nonExistentPath.toString());
      
      // 14b: set quota on a file
      assertTrue(dfs.isFile(childFile0));
      args[1] = childFile0.toString();
      runCommand(admin, args, true);
      // same for space quota
      runCommand(admin, true, "-setSpaceQuota", "1t", args[1]);
      
      // 15a: clear quota on a file
      args[0] = "-clrQuota";
      runCommand(admin, args, true);
      runCommand(admin, true, "-clrSpaceQuota", args[1]);
      
      // 15b: clear quota on a non-existent directory
      args[1] = nonExistentPath.toString();
      runCommand(admin, args, true);
      runCommand(admin, true, "-clrSpaceQuota", args[1]);
      
      // 16a: set the quota of /test to be 0
      args = new String[]{"-setQuota", "0", parent.toString()};
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", "0", args[2]);
      
      // 16b: set the quota of /test to be -1
      args[1] = "-1";
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16c: set the quota of /test to be Long.MAX_VALUE+1
      args[1] = String.valueOf(Long.MAX_VALUE + 1L);
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16d: set the quota of /test to be a non integer
      args[1] = "33aa1.5";
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16e: set space quota with a value larger than Long.MAX_VALUE
      runCommand(admin, true, "-setSpaceQuota",
          (Long.MAX_VALUE / 1024 / 1024 + 1024) + "m", args[2]);
      
      // 17:  setQuota by a non-administrator
      final String username = "userxx";
      UserGroupInformation ugi = UserGroupInformation
          .createUserForTesting(username, new String[]{"groupyy"});
      
      final String[] args2 = args.clone(); // need final ref for doAs block
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          assertEquals("Not running as new user", username,
              UserGroupInformation.getCurrentUser().getShortUserName());
          DFSAdmin userAdmin = new DFSAdmin(conf);
          
          args2[1] = "100";
          runCommand(userAdmin, args2, true);
          runCommand(userAdmin, true, "-setSpaceQuota", "1g", args2[2]);
          
          // 18: clrQuota by a non-administrator
          String[] args3 = new String[]{"-clrQuota", parent.toString()};
          runCommand(userAdmin, args3, true);
          runCommand(userAdmin, true, "-clrSpaceQuota", args3[1]);
          
          return null;
        }
      });

      // 19: clrQuota on the root directory ("/") should fail
      runCommand(admin, true, "-clrQuota", "/");

      // 20: setQuota on the root directory ("/") should succeed
      runCommand(admin, false, "-setQuota", "1000000", "/");

      runCommand(admin, true, "-clrQuota", "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      runCommand(admin, new String[]{"-clrQuota", parent.toString()}, false);
      runCommand(admin, false, "-clrSpaceQuota", parent.toString());


      // 2: create directory /test/data2
      final Path childDir2 = new Path(parent, "data2");
      assertTrue(dfs.mkdirs(childDir2));


      final Path childFile2 = new Path(childDir2, "datafile2");
      final Path childFile3 = new Path(childDir2, "datafile3");
      final long fileLen2 = 512;
      final long spaceQuota2 = fileLen2 * replication;
      // set space quota to a real low value 
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2),
          childDir2.toString());
      // clear space quota
      runCommand(admin, false, "-clrSpaceQuota", childDir2.toString());
      // create a file that is greater than the size of space quota
      DFSTestUtil.createFile(fs, childFile2, fileLen2, replication, 0);

      // now set space quota again. This should succeed
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2),
          childDir2.toString());

      hasException = false;
      try {
        DFSTestUtil.createFile(fs, childFile3, fileLen2, replication, 0);
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // now test the same for root
      final Path childFile4 = new Path("/", "datafile2");
      final Path childFile5 = new Path("/", "datafile3");

      runCommand(admin, true, "-clrQuota", "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      // set space quota to a real low value 
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2),
          "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      DFSTestUtil.createFile(fs, childFile4, fileLen2, replication, 0);
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2),
          "/");

      hasException = false;
      try {
        DFSTestUtil.createFile(fs, childFile5, fileLen2, replication, 0);
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test commands that change the size of the name space:
   * mkdirs, rename, and delete
   */
  @Test
  public void testNamespaceCommands() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 1000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: " + fs.getUri(),
        fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem) fs;

    dfs.setStoragePolicy(new Path("/"), "DB");

    try {
      // 1: create directory /nqdir0/qdir1/qdir20/nqdir30
      assertTrue(dfs.mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")));

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 2: set the quota of /nqdir0/qdir1 to be 6
      final Path quotaDir1 = new Path("/nqdir0/qdir1");
      dfs.setQuota(quotaDir1, 6, HdfsConstants.QUOTA_DONT_SET);
      ContentSummary c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 3: set the quota of /nqdir0/qdir1/qdir20 to be 7
      final Path quotaDir2 = new Path("/nqdir0/qdir1/qdir20");
      dfs.setQuota(quotaDir2, 7, HdfsConstants.QUOTA_DONT_SET);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 4: Create directory /nqdir0/qdir1/qdir21 and set its quota to 2
      final Path quotaDir3 = new Path("/nqdir0/qdir1/qdir21");
      assertTrue(dfs.mkdirs(quotaDir3));
      dfs.setQuota(quotaDir3, 2, HdfsConstants.QUOTA_DONT_SET);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir3);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
      Path tempPath = new Path(quotaDir3, "nqdir32");
      assertTrue(dfs.mkdirs(tempPath));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 6: Create directory /nqdir0/qdir1/qdir21/nqdir33
      tempPath = new Path(quotaDir3, "nqdir33");
      boolean hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 7: Create directory /nqdir0/qdir1/qdir20/nqdir31
      tempPath = new Path(quotaDir2, "nqdir31");
      assertTrue(dfs.mkdirs(tempPath));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 8: Create directory /nqdir0/qdir1/qdir20/nqdir33
      tempPath = new Path(quotaDir2, "nqdir33");
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 9: Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
      tempPath = new Path(quotaDir2, "nqdir30");
      dfs.rename(new Path(quotaDir3, "nqdir32"), tempPath);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertFalse("Not all sub Tree locks cleared",TestSubtreeLock.subTreeLocksExists());
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 7);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 10: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, quotaDir3));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir30")));

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 10.a: Rename /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/nqdir32
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, new Path(quotaDir3, "nqdir32")));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir32")));

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 11: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0
      assertTrue(dfs.rename(tempPath, new Path("/nqdir0")));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 12: Create directory /nqdir0/nqdir30/nqdir33
      assertTrue(dfs.mkdirs(new Path("/nqdir0/nqdir30/nqdir33")));

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 13: Move /nqdir0/nqdir30 /nqdir0/qdir1/qdir20/qdir30
      hasException = false;
      try {
        assertFalse(dfs.rename(new Path("/nqdir0/nqdir30"), tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 14: Move /nqdir0/qdir1/qdir21 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(quotaDir3, quotaDir2));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      tempPath = new Path(quotaDir2, "qdir21");
      c = DFSTestUtil.getContentSummary(dfs,tempPath);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 15: Delete /nqdir0/qdir1/qdir20/qdir21
      dfs.delete(tempPath, true);

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 16: Move /nqdir0/qdir30 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(new Path("/nqdir0/nqdir30"), quotaDir2));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2);
      assertEquals(c.getDirectoryCount(), 5);
      assertEquals(c.getQuota(), 7);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test HDFS operations that change disk space consumed by a directory tree.
   * namely create, rename, delete, append, and setReplication.
   * <p/>
   * This is based on testNamespaceCommands() above.
   */
  @Test
  public void testSpaceCommands() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final int BLOCK_SIZE = 512;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 1000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).format(true).numDataNodes(5).build();
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: " + fs.getUri(),
        fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem) fs;

    dfs.setStoragePolicy(new Path("/"), "DB");

    try {
      int fileLen = 1024;
      short replication = 3;
      int fileSpace = fileLen * replication;
      
      // create directory /nqdir0/qdir1/qdir20/nqdir30
      assertTrue(dfs.mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")));

      // HOP - Wait for asynchronous quota updates to be applied
      // set the quota of /nqdir0/qdir1 to 4 * fileSpace
      final Path quotaDir1 = new Path("/nqdir0/qdir1");
      dfs.setQuota(quotaDir1, HdfsConstants.QUOTA_DONT_SET, 4 * fileSpace);
      ContentSummary c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getSpaceQuota(), 4 * fileSpace);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // set the quota of /nqdir0/qdir1/qdir20 to 6 * fileSpace
      final Path quotaDir20 = new Path("/nqdir0/qdir1/qdir20");
      dfs.setQuota(quotaDir20, HdfsConstants.QUOTA_DONT_SET, 6 * fileSpace);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir20);
      assertEquals(c.getSpaceQuota(), 6 * fileSpace);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // Create /nqdir0/qdir1/qdir21 and set its space quota to 2 * fileSpace
      final Path quotaDir21 = new Path("/nqdir0/qdir1/qdir21");
      assertTrue(dfs.mkdirs(quotaDir21));
      dfs.setQuota(quotaDir21, HdfsConstants.QUOTA_DONT_SET, 2 * fileSpace);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceQuota(), 2 * fileSpace);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
      Path tempPath = new Path(quotaDir21, "nqdir32");
      assertTrue(dfs.mkdirs(tempPath));

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // create a file under nqdir32/fileDir
      DFSTestUtil.createFile(dfs, new Path(tempPath, "fileDir/file1"), fileLen,
          replication, 0);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceConsumed(), fileSpace);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      FSDataOutputStream fout =
          dfs.create(new Path(quotaDir21, "nqdir33/file2"), replication);
      boolean hasException = false;
      try {
        // HOP - Write in single blocks and wait to trigger exception
        for (int i = 0; i < 2 * fileLen; i += BLOCK_SIZE) {
          fout.write(new byte[BLOCK_SIZE]);
          DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        }
        fout.close();
      } catch (QuotaExceededException e) {
        hasException = true;
        IOUtils.closeStream(fout);
      }
      assertTrue(hasException);

      // delete nqdir33
      assertTrue(dfs.delete(new Path(quotaDir21, "nqdir33"), true));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      assertEquals(c.getSpaceQuota(), 2 * fileSpace);

      // Verify space before the move:
      c = DFSTestUtil.getContentSummary(dfs,quotaDir20);
      assertEquals(c.getSpaceConsumed(), 0);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
      Path dstPath = new Path(quotaDir20, "nqdir30");
      Path srcPath = new Path(quotaDir21, "nqdir32");
      assertTrue(dfs.rename(srcPath, dstPath));
      
      // verify space after the move
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir20);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      // verify space for its parent
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      // verify space for source for the move
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      final Path file2 = new Path(dstPath, "fileDir/file2");
      int file2Len = 2 * fileLen;
      // create a larger file under /nqdir0/qdir1/qdir20/nqdir30
      DFSTestUtil.createFile(dfs, file2, file2Len, replication, 0);

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = DFSTestUtil.getContentSummary(dfs,quotaDir20);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);

      // HOP - Wait for asynchronous quota updates to be applied
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // Reverse: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/
      hasException = false;
      try {
        assertFalse(dfs.rename(dstPath, srcPath));
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      // make sure no intermediate directories left by failed rename

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertFalse(dfs.exists(srcPath));
      // directory should exist
      assertTrue(dfs.exists(dstPath));
      // verify space after the failed move
      c = DFSTestUtil.getContentSummary(dfs,quotaDir20);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);

      // Test Append :
      Thread.sleep(10000);
      // verify space quota
      c = DFSTestUtil.getContentSummary(dfs,quotaDir1);
      assertEquals(c.getSpaceQuota(), 4 * fileSpace);
      // verify space before append;
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      Thread.sleep(10000);
      OutputStream out = dfs.append(file2);
      // appending 1 fileLen should succeed
      out.write(new byte[fileLen]);
      out.close();
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      file2Len += fileLen; // after append

      // verify space after append;
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 4 * fileSpace);
      // now increase the quota for quotaDir1
      dfs.setQuota(quotaDir1, HdfsConstants.QUOTA_DONT_SET, 5 * fileSpace);
      // Now, appending more than 1 fileLen should result in an error
      out = dfs.append(file2);
      hasException = false;

      /*
       * Problem with partial append
       * How partial append test works
       * there is room for only two more blocks but the test tries to append three
       * blocks
       * Now the file size is maintained in the inode table. When ever there is 
       * a request for new block (if the quota is not exceeded) then the last 
       * block is committed and the file size is updated in the indoe table
       * 
       * here in the case the case of the third block the last block is committed
       * the size of the file is updated. However when the quota check fails and the
       * entire transaction is rolledback along with the changes of the file size
       * 
       * This file size is updted only if the file is successfully closed or a 
       * new block is added. 
       */
      try {
        // HOP - Write in single blocks and wait to trigger exception
        for (int i = 0; i < 2 ; i ++) {
          out.write(new byte[BLOCK_SIZE]);
          DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        }
        out.close();
        
        out = dfs.append(file2);
        out.write(new byte[BLOCK_SIZE]);
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        out.close();
      } catch (QuotaExceededException e) {
        hasException = true;
        IOUtils.closeStream(out);
      }
      assertTrue(hasException);
      file2Len += fileLen; // after partial append

      // verify space after partial append
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace);

      // Test set replication :
      
      // first reduce the replication
      dfs.setReplication(file2, (short) (replication - 1));
      
      // verify that space is reduced by file2Len
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace - file2Len);
      // now try to increase the replication and and expect an error.
      hasException = false;
      try {
        dfs.setReplication(file2, (short) (replication + 1));
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }

      assertTrue(hasException);
      // verify space consumed remains unchanged.
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace - file2Len);
      
      // now increase the quota for quotaDir1 and quotaDir20
      dfs.setQuota(quotaDir1, HdfsConstants.QUOTA_DONT_SET, 10 * fileSpace);
      dfs.setQuota(quotaDir20, HdfsConstants.QUOTA_DONT_SET, 10 * fileSpace);
      // then increasing replication should be ok.
      dfs.setReplication(file2, (short) (replication + 1));
      // verify increase in space
      c = DFSTestUtil.getContentSummary(dfs,dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace + file2Len);

      // Test HDFS-2053 :
      // Create directory /hdfs-2053
      final Path quotaDir2053 = new Path("/hdfs-2053");
      assertTrue(dfs.mkdirs(quotaDir2053));

      // Create subdirectories /hdfs-2053/{A,B,C}
      final Path quotaDir2053_A = new Path(quotaDir2053, "A");
      assertTrue(dfs.mkdirs(quotaDir2053_A));
      final Path quotaDir2053_B = new Path(quotaDir2053, "B");
      assertTrue(dfs.mkdirs(quotaDir2053_B));
      final Path quotaDir2053_C = new Path(quotaDir2053, "C");
      assertTrue(dfs.mkdirs(quotaDir2053_C));
      // Factors to vary the sizes of test files created in each subdir.
      // The actual factors are not really important but they allow us to create
      // identifiable file sizes per subdir, which helps during debugging.
      int sizeFactorA = 1;
      int sizeFactorB = 2;
      int sizeFactorC = 4;

      // Set space quota for subdirectory C
      dfs.setQuota(quotaDir2053_C, HdfsConstants.QUOTA_DONT_SET,
          (sizeFactorC + 1) * fileSpace);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2053_C);
      assertEquals(c.getSpaceQuota(), (sizeFactorC + 1) * fileSpace);

      // Create a file under subdirectory A
      DFSTestUtil.createFile(dfs, new Path(quotaDir2053_A, "fileA"),
          sizeFactorA * fileLen, replication, 0);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2053_A);
      assertEquals(c.getSpaceConsumed(), sizeFactorA * fileSpace);

      // Create a file under subdirectory B
      DFSTestUtil.createFile(dfs, new Path(quotaDir2053_B, "fileB"),
          sizeFactorB * fileLen, replication, 0);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2053_B);
      assertEquals(c.getSpaceConsumed(), sizeFactorB * fileSpace);
      // Create a file under subdirectory C (which has a space quota)
      DFSTestUtil.createFile(dfs, new Path(quotaDir2053_C, "fileC"),
          sizeFactorC * fileLen, replication, 0);
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2053_C);
      assertEquals(c.getSpaceConsumed(), sizeFactorC * fileSpace);

      // Check space consumed for /hdfs-2053
      c = DFSTestUtil.getContentSummary(dfs,quotaDir2053);
      assertEquals(c.getSpaceConsumed(),
          (sizeFactorA + sizeFactorB + sizeFactorC) * fileSpace);

    } finally {
      cluster.shutdown();
    }
  }

  private static void checkContentSummary(final ContentSummary expected,
      final ContentSummary computed) {
    assertEquals(expected.toString(), computed.toString());
  }

  /**
   * Violate a space quota using files of size < 1 block that don't fit in
   * the database as small file. Test that block
   * allocation conservatively assumes that for quota checking the entire
   * space of the block is used.
   */
  @Test
  public void testBlockAllocationAdjustsUsageConservatively() throws Exception {
    Configuration conf = new HdfsConfiguration();

    final int MAX_SMALL_FILE_SIZE = 6 * 1024;
    final int BLOCK_SIZE = 4 * MAX_SMALL_FILE_SIZE;

    conf.setInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    DistributedFileSystem fs = cluster.getFileSystem();

    fs.setStoragePolicy(new Path("/"), "DB");

    DFSAdmin admin = new DFSAdmin(conf);

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final String webhdfsuri = WebHdfsFileSystem.SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = new Path(webhdfsuri).getFileSystem(conf);

    try {
      Path dir = new Path(
          "/folder1/folder2/folder3/folder4/folder5/folder6/folder7/folder8/folder9/folder10");
      Path file1 = new Path(dir, "test1");
      Path file2 = new Path(dir, "test2");
      Path file3 = new Path(dir, "test3");
      Path file4 = new Path(dir, "test4");

      boolean exceededQuota = false;
      final int QUOTA_SIZE = 3 * MAX_SMALL_FILE_SIZE; // total space usage including
      // repl.
      final int FILE_SIZE = MAX_SMALL_FILE_SIZE / 2;
      ContentSummary c;
      
      // Create the directory and set the quota
      assertTrue(fs.mkdirs(dir));
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
          dir.toString());
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
          "/folder1/folder2");
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
          "/folder1/folder2/folder3/folder4/folder5");
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
          "/folder1/folder2/folder3/folder4/folder5/folder6/folder7/folder8");

      // Creating a file should use half the quota
      DFSTestUtil.createFile(fs, file1, FILE_SIZE, (short) 3, 1L);
      //DFSTestUtil.waitReplication(fs, file1, (short) 3);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = fs.getContentSummary(dir);
      checkContentSummary(c, webhdfs.getContentSummary(dir));
      assertEquals("Quota is half consumed", QUOTA_SIZE / 2,
          c.getSpaceConsumed());

      //We can create the 2nd file because small files doesn't allocate
      // blocks. so the 2nd file will use the other half of the quota.
      FSDataOutputStream out = fs.create(file2, (short) 3);
      try {
        out.write(new byte[FILE_SIZE]);
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }
      assertFalse("Quota exceeded", exceededQuota);


      //Update quota for folders
      final int QUOTA_SIZE_UPDATE = QUOTA_SIZE + BLOCK_SIZE * 3;
      final int FILE_SIZE_UPDATE = BLOCK_SIZE/2;

      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE_UPDATE),
          dir.toString());
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE_UPDATE),
          "/folder1/folder2");
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE_UPDATE),
          "/folder1/folder2/folder3/folder4/folder5");
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE_UPDATE),
          "/folder1/folder2/folder3/folder4/folder5/folder6/folder7/folder8");


      // Creating a normal file
      DFSTestUtil.createFile(fs, file3, FILE_SIZE_UPDATE, (short) 3, 1L);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      c = fs.getContentSummary(dir);
      checkContentSummary(c, webhdfs.getContentSummary(dir));
      assertEquals("Quota is partially consumed", QUOTA_SIZE +
          FILE_SIZE_UPDATE*3, c.getSpaceConsumed());

      exceededQuota = false;
      // We can not create the 4th file because even though the total space
      // used by four files (2 * 3 * MAX_SMALL_FILE_SIZE +  2 * 3 * BLOCK_SIZE)
      // would fit within the quota when a block for a file is created the
      // space used is adjusted conservatively (3 * block size, ie assumes a
      // full block is written) which will violate the quota (3 * block size)
      out = fs.create(file4, (short) 3);
      try {
        out.write(new byte[FILE_SIZE_UPDATE]);
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }
      assertTrue("Quota not exceeded", exceededQuota);

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Like the previous test but create many files. This covers bugs where
   * the quota adjustment is incorrect but it takes many files to accrue
   * a big enough accounting error to violate the quota.
   */
  @Test
  public void testMultipleFilesSmallerThanOneBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();

    final int MAX_SMALL_FILE_SIZE = 5 * 1024;
    final int BLOCK_SIZE = MAX_SMALL_FILE_SIZE + 1024;

    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 1000);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.setStoragePolicy(new Path("/"), "DB");
    DFSAdmin admin = new DFSAdmin(conf);

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final String webhdfsuri = WebHdfsFileSystem.SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = new Path(webhdfsuri).getFileSystem(conf);
    
    try {
      Path dir = new Path("/test");
      boolean exceededQuota = false;
      ContentSummary c;
      // 1kb file
      // 6kb block
      // 192kb quota
      final int FILE_SIZE = 1024;
      final int QUOTA_SIZE = 32 * (int) fs.getDefaultBlockSize(dir);
      assertEquals(6 * 1024, fs.getDefaultBlockSize(dir));
      assertEquals(192 * 1024, QUOTA_SIZE);

      // Create the dir and set the quota. We need to enable the quota before
      // writing the files as setting the quota afterwards will over-write
      // the cached disk space used for quota verification with the actual
      // amount used as calculated by INode#spaceConsumedInTree.
      assertTrue(fs.mkdirs(dir));
      runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
          dir.toString());

      // We can create at most 59 files because block allocation is
      // conservative and initially assumes a full block is used, so we
      // need to leave at least 3 * BLOCK_SIZE free space when allocating
      // the last block: (58 * 3 * 1024) (3 * 6 * 1024) = 192kb
      for (int i = 0; i < 59; i++) {
        Path file = new Path("/test/test" + i);
        DFSTestUtil.createFile(fs, file, FILE_SIZE, (short) 3, 1L);
        //DFSTestUtil.waitReplication(fs, file, (short) 3);
        // HOP - Wait for asynchronous quota updates to be applied
        Thread.sleep(1000);
      }

      // Should account for all 59 files (almost QUOTA_SIZE)
      c = fs.getContentSummary(dir);
      checkContentSummary(c, webhdfs.getContentSummary(dir));
      assertEquals("Invalid space consumed", 59 * FILE_SIZE * 3,
          c.getSpaceConsumed());
      assertEquals("Invalid space consumed", QUOTA_SIZE - (59 * FILE_SIZE * 3),
          3 * (fs.getDefaultBlockSize(dir) - FILE_SIZE));

      // Now check that trying to create another file violates the quota
      Path file = new Path("/test/test59");
      FSDataOutputStream out = fs.create(file, (short) 3);
      try {
        out.write(new byte[MAX_SMALL_FILE_SIZE+1]);
        out.close();
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }
      assertTrue("Quota not exceeded", exceededQuota);

      exceededQuota = false;
      // Now try to create another file but a small one that shouldn't
      // violate the quota since small files are not conservative
      out = fs.create(file, (short) 3);
      try {
        out.write(new byte[FILE_SIZE]);
        out.close();
        DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }

      assertFalse("Quota exceeded", exceededQuota);


    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testAsynchronousQuota() throws Exception {
    // TODO This test should not rely on timing but should call the process function of QuotaUpdateManager manually
    final Configuration conf = new HdfsConfiguration();
    final int BLOCK_SIZE = 512;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 5000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      final FileSystem fs = cluster.getFileSystem();
      final DistributedFileSystem dfs = (DistributedFileSystem) fs;
      dfs.setStoragePolicy(new Path("/"), "DB");

      Path testFolder = new Path("/test");
      dfs.mkdirs(testFolder);
      dfs.setQuota(testFolder, 2L, 2 * BLOCK_SIZE);

      // Should be fast enough to violate the quota
      Path testFile1 = new Path(testFolder, "test1");
      Path testFile2 = new Path(testFolder, "test2");
      Path testFile3 = new Path(testFolder, "test3");
      dfs.create(testFile1).close();
      dfs.create(testFile2).close();
      dfs.create(testFile3).close();

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // This time should be too late to violate the quota further
      Path testFile4 = new Path(testFolder, "test4");
      try {
        dfs.create(testFile4).close();
        fail();
      } catch (NSQuotaExceededException e) {

      }

      dfs.delete(testFile1, true);
      dfs.delete(testFile2, true);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      try {
        dfs.create(testFile4).close();
        fail();
      } catch (NSQuotaExceededException e) {

      }

      dfs.delete(testFile3, true);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      FSDataOutputStream out = dfs.create(testFile4);
      // Should be fast enough to violate the quota
      for (int i = 0; i < 3; i++) {
        out.write(new byte[BLOCK_SIZE]);
      }
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      // This time should be too late to violate the quota further
      try {
        out.write(new byte[BLOCK_SIZE]);
        out.close();
        fail();
      } catch (DSQuotaExceededException e) {

      }
      IOUtils.closeStream(out);

      dfs.delete(testFile4, true);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();

      out = dfs.create(testFile4);
      // Should be fast enough to violate the quota
      for (int i = 0; i < 2; i++) {
        out.write(new byte[BLOCK_SIZE]);
      }
      out.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetQuotaLate() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 5000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      final FileSystem fs = cluster.getFileSystem();
      final DistributedFileSystem dfs = (DistributedFileSystem) fs;

      dfs.setStoragePolicy(new Path("/"), "DB");

      Path testFolder = new Path("/test");
      dfs.mkdirs(testFolder);

      Path testFile1 = new Path(testFolder, "test1");
      dfs.create(testFile1).close();

      dfs.setQuota(testFolder, 2L, 1024);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetQuotaOnNonExistingDirectory() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 5000);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      final FileSystem fs = cluster.getFileSystem();
      final DistributedFileSystem dfs = (DistributedFileSystem) fs;

      dfs.setStoragePolicy(new Path("/"), "DB");

      Path testFolder = new Path("/test");

      dfs.setQuota(testFolder, 2L, 1024);
      fail("SetQuota on non-existing directory succeeded");
    } catch (FileNotFoundException e) {

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testDiskspaceCalculationForSmallFilesWhenMovedToBlocks() throws
      Exception{
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY, 1000);
    final int MAX_SMALL_FILE_SIZE = 64 * 1024;
    final int BLOCK_SIZE = 2 * MAX_SMALL_FILE_SIZE;

    conf.setInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, MAX_SMALL_FILE_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: " + fs.getUri(),
        fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.setStoragePolicy(new Path("/"), "DB");
    DFSAdmin admin = new DFSAdmin(conf);

    try{
      final int smallfileLen = 1024;
      final short replication = 3;
      final long spaceQuota = BLOCK_SIZE * replication * 2;

      //create a test directory
      final Path parent = new Path("/test");
      assertTrue(dfs.mkdirs(parent));

      String[] args = new String[]{"-setQuota", "5", parent.toString()};
      runCommand(admin, args, false);

      // set diskspace quota to 3 * filesize
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota),
          parent.toString());


      final Path childDir0 = new Path(parent, "data0");
      dfs.mkdirs(childDir0);

      final Path datafile0 = new Path(childDir0, "datafile0");

      //create datafile0
      DFSTestUtil.createFile(fs, datafile0, smallfileLen, replication, 0);
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();

      // count -q /test
      ContentSummary c = DFSTestUtil.getContentSummary(dfs,parent);
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 5);
      assertEquals(c.getSpaceConsumed(), smallfileLen * replication);
      assertEquals(c.getSpaceQuota(), spaceQuota);

      boolean exceededQuota = false;
      // append to the file data less than MAX_SMALL_FILE_SIZE
      OutputStream out = fs.append(datafile0);

      try {
        out.write(new byte[smallfileLen]);
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertFalse("Quota exceeded", exceededQuota);

      c = DFSTestUtil.getContentSummary(dfs,parent);
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 5);
      assertEquals(c.getSpaceConsumed(), 2 * smallfileLen * replication);
      assertEquals(c.getSpaceQuota(), spaceQuota);

      // append more than the small file max size to move to normal file with
      // blocks
      out = fs.append(datafile0);
      try {
        out.write(new byte[BLOCK_SIZE]);
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertFalse("Quota exceeded", exceededQuota);

      c = DFSTestUtil.getContentSummary(dfs,parent);
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 5);
      assertEquals(c.getSpaceConsumed(), (2 * smallfileLen  + BLOCK_SIZE)*
          replication);
      assertEquals(c.getSpaceQuota(), spaceQuota);

      //append to the limit of the quota
      out = fs.append(datafile0);
      try {
        out.write(new byte[BLOCK_SIZE-2*smallfileLen]);
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      assertFalse("Quota exceeded", exceededQuota);

      c = DFSTestUtil.getContentSummary(dfs,parent);
      assertEquals(c.getFileCount() + c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 5);
      assertEquals(c.getSpaceConsumed(), spaceQuota);
      assertEquals(c.getSpaceQuota(), spaceQuota);

      //anymore appends should fail
      out = fs.append(datafile0);
      try {
        out.write(new byte[1]);
        out.close();
      } catch (QuotaExceededException e) {
        exceededQuota = true;
        IOUtils.closeStream(out);
      }

      assertTrue("Quota exceeded", exceededQuota);

    }finally {
      cluster.shutdown();
    }
  }
}
