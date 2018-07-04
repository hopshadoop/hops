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

package org.apache.hadoop.fs;

import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.FileContextTestHelper.exists;

public class TestHDFSFileContextMainOperations
    extends FileContextMainOperationsBaseTest {
  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static HdfsConfiguration CONF = new HdfsConfiguration();
  private static int NNPORT = 5555;
  private static int QUOTA_UPDATE_INTERVAL = 1;

  @Override
  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return true;
  }

  private Path getTestRootPath(FileContext fc, String path) {
    return fileContextTestHelper.getTestRootPath(fc, path);
  }

  @BeforeClass
  public static void clusterSetupAtBegining()
      throws IOException, LoginException, URISyntaxException {
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY,
        QUOTA_UPDATE_INTERVAL);
    cluster =
        new MiniDFSCluster.Builder(CONF).numDataNodes(2).nameNodePort(NNPORT)
            .build();
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getURI(0), CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path(
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  private static void restartCluster() throws IOException, LoginException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    cluster =
        new MiniDFSCluster.Builder(CONF).numDataNodes(1).nameNodePort(NNPORT)
            .format(false).build();
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getURI(0), CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path(
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }
  
  @Override
  protected IOException unwrapException(IOException e) {
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    }
    return e;
  }
  
  @Test
  public void testOldRenameWithQuota() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);

    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    /* 
     * Test1: src does not exceed quota and dst has no quota check and hence 
     * accommodates rename
     */
    oldRename(src1, dst1, true, false);

    // HOP - Wait for asynchronous quota updates to be applied
    Thread.sleep(5000);
    /*
     * Test2: src does not exceed quota and dst has *no* quota to accommodate 
     * rename. 
     */
    // dstDir quota = 1 and dst1 already uses it
    oldRename(src2, dst2, false, true);

    /*
     * Test3: src exceeds quota and dst has *no* quota to accommodate rename
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    oldRename(dst1, src1, false, true);
  }
  
  @Test
  public void testRenameWithQuota() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "test/testRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath(fc, "test/testRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);

    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    /* 
     * Test1: src does not exceed quota and dst has no quota check and hence 
     * accommodates rename
     */
    // rename uses dstdir quota=1
    rename(src1, dst1, false, true, false, Rename.NONE);
    // rename reuses dstdir quota=1
    rename(src2, dst1, true, true, false, Rename.OVERWRITE);

    /*
     * Test2: src does not exceed quota and dst has *no* quota to accommodate 
     * rename. 
     */
    // dstDir quota = 1 and dst1 already uses it
    // HOP - Wait for asynchronous quota updates to be applied
    Thread.sleep(5000);
    createFile(src2);
    rename(src2, dst2, false, false, true, Rename.NONE);

    /*
     * Test3: src exceeds quota and dst has *no* quota to accommodate rename
     * rename to a destination that does not exist
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    rename(dst1, src1, false, false, true, Rename.NONE);
    
    /*
     * Test4: src exceeds quota and dst has *no* quota to accommodate rename
     * rename to a destination that exists and quota freed by deletion of dst
     * is same as quota needed by src.
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 100, HdfsConstants.QUOTA_DONT_SET);
    createFile(src1);
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    rename(dst1, src1, true, true, false, Rename.OVERWRITE);
  }
  
  @Test
  public void testRenameRoot() throws Exception {
    Path src = getTestRootPath(fc, "test/testRenameRoot/srcdir/src1");
    Path dst = new Path("/");
    createFile(src);
    rename(src, dst, true, false, true, Rename.OVERWRITE);
    rename(dst, src, true, false, true, Rename.OVERWRITE);
  }
  
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  @Test
  public void testEditsLogOldRename() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
    Path dst1 = getTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
    createFile(src1);
    fs.mkdirs(dst1.getParent());
    createFile(dst1);

    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    // Free up quota for a subsequent rename
    System.out.println("quota set");
    Thread.sleep(60000);
    fs.delete(dst1, true);
    System.out.println("delete");
    // HOP - Wait for asynchronous quota updates to be applied
    Thread.sleep(dst1.depth() * QUOTA_UPDATE_INTERVAL * 1000 + 2000);
    System.out.println("start rename");
    oldRename(src1, dst1, true, false);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    src1 = getTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
    dst1 = getTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
    Assert.assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    Assert.assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }
  
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  @Test
  public void testEditsLogRename() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "testEditsLogRename/srcdir/src1");
    Path dst1 = getTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
    createFile(src1);
    fs.mkdirs(dst1.getParent());
    createFile(dst1);
    
    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    System.out.println("quota set");
    Thread.sleep(60000);
    // Free up quota for a subsequent rename
    fs.delete(dst1, true);
    System.out.println("delete");
    // HOP - Wait for asynchronous quota updates to be applied
    Thread.sleep(dst1.depth() * QUOTA_UPDATE_INTERVAL + 2000);
    System.out.println("start rename");
    rename(src1, dst1, true, true, false, Rename.OVERWRITE);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    src1 = getTestRootPath(fc, "testEditsLogRename/srcdir/src1");
    dst1 = getTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
    Assert.assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    Assert.assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }
  
  private void oldRename(Path src, Path dst, boolean renameSucceeds,
      boolean exception) throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    try {
      Assert.assertEquals(renameSucceeds, fs.rename(src, dst));
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage(), exception);
    }
    Assert.assertEquals(renameSucceeds, !exists(fc, src));
    Assert.assertEquals(renameSucceeds, exists(fc, dst));
  }
  
  private void rename(Path src, Path dst, boolean dstExists,
      boolean renameSucceeds, boolean exception, Options.Rename... options)
      throws Exception {
    try {
      fc.rename(src, dst, options);
      Assert.assertTrue(renameSucceeds);
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage(), exception);
    }
    Assert.assertEquals(renameSucceeds, !exists(fc, src));
    Assert.assertEquals((dstExists || renameSucceeds), exists(fc, dst));
  }
}
