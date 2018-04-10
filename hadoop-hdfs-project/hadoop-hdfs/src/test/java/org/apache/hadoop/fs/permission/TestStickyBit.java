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
package org.apache.hadoop.fs.permission;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStickyBit {

  static UserGroupInformation user1;
  static UserGroupInformation user2;
  
  /**
   * Ensure that even if a file is in a directory with the sticky bit on,
   * another user can write to that file (assuming correct permissions).
   */
  private void confirmCanAppend(Configuration conf, FileSystem hdfs,
      Path baseDir) throws IOException, InterruptedException {
    // Create a tmp directory with wide-open permissions and sticky bit
    Path p = new Path(baseDir, "tmp");

    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));

    // Write a file to the new tmp directory as a regular user
    hdfs = DFSTestUtil.getFileSystemAs(user1, conf);
    Path file = new Path(p, "foo");
    writeFile(hdfs, file);
    hdfs.setPermission(file, new FsPermission((short) 0777));

    // Log onto cluster as another user and attempt to append to file
    hdfs = DFSTestUtil.getFileSystemAs(user2, conf);
    Path file2 = new Path(p, "foo");
    FSDataOutputStream h = hdfs.append(file2);
    h.write("Some more data".getBytes());
    h.close();
  }

  /**
   * Test that one user can't delete another user's file when the sticky bit is
   * set.
   */
  private void confirmDeletingFiles(Configuration conf, FileSystem hdfs,
      Path baseDir) throws IOException, InterruptedException {
    Path p = new Path(baseDir, "contemporary");
    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));

    // Write a file to the new temp directory as a regular user
    hdfs = DFSTestUtil.getFileSystemAs(user1, conf);
    Path file = new Path(p, "foo");
    writeFile(hdfs, file);

    // Make sure the correct user is the owner
    assertEquals(user1.getShortUserName(), hdfs.getFileStatus(file).getOwner());

    // Log onto cluster as another user and attempt to delete the file
    FileSystem hdfs2 = DFSTestUtil.getFileSystemAs(user2, conf);

    try {
      hdfs2.delete(file, false);
      fail("Shouldn't be able to delete someone else's file with SB on");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof AccessControlException);
      assertTrue(ioe.getMessage().contains("sticky bit"));
    }
  }

  /**
   * Test that if a directory is created in a directory that has the sticky bit
   * on, the new directory does not automatically get a sticky bit, as is
   * standard Unix behavior
   */
  private void confirmStickyBitDoesntPropagate(FileSystem hdfs, Path baseDir)
      throws IOException {
    Path p = new Path(baseDir, "scissorsisters");

    // Turn on its sticky bit
    hdfs.mkdirs(p, new FsPermission((short) 01666));

    // Create a subdirectory within it
    Path p2 = new Path(p, "bar");
    hdfs.mkdirs(p2);

    // Ensure new directory doesn't have its sticky bit on
    assertFalse(hdfs.getFileStatus(p2).getPermission().getStickyBit());
  }

  /**
   * Test basic ability to get and set sticky bits on files and directories.
   */
  private void confirmSettingAndGetting(FileSystem hdfs, Path baseDir)
      throws IOException {
    Path p1 = new Path(baseDir, "roguetraders");

    hdfs.mkdirs(p1);

    // Initially sticky bit should not be set
    assertFalse(hdfs.getFileStatus(p1).getPermission().getStickyBit());

    // Same permission, but with sticky bit on
    short withSB;
    withSB = (short) (hdfs.getFileStatus(p1).getPermission().toShort() | 01000);

    assertTrue((new FsPermission(withSB)).getStickyBit());

    hdfs.setPermission(p1, new FsPermission(withSB));
    assertTrue(hdfs.getFileStatus(p1).getPermission().getStickyBit());

    // Write a file to the fs, try to set its sticky bit
    Path f = new Path(baseDir, "somefile");
    writeFile(hdfs, f);
    assertFalse(hdfs.getFileStatus(f).getPermission().getStickyBit());

    withSB = (short) (hdfs.getFileStatus(f).getPermission().toShort() | 01000);

    hdfs.setPermission(f, new FsPermission(withSB));

    assertTrue(hdfs.getFileStatus(f).getPermission().getStickyBit());
  }

  @Test
  public void testGeneralSBBehavior() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      initUsers();
      
      FileSystem hdfs = cluster.getFileSystem();

      assertTrue(hdfs instanceof DistributedFileSystem);

      Path baseDir = new Path("/mcgann");
      hdfs.mkdirs(baseDir);
      confirmCanAppend(conf, hdfs, baseDir);

      baseDir = new Path("/eccleston");
      hdfs.mkdirs(baseDir);
      confirmSettingAndGetting(hdfs, baseDir);

      baseDir = new Path("/tennant");
      hdfs.mkdirs(baseDir);
      confirmDeletingFiles(conf, hdfs, baseDir);

      baseDir = new Path("/smith");
      hdfs.mkdirs(baseDir);
      confirmStickyBitDoesntPropagate(hdfs, baseDir);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testAclGeneralSBBehavior() throws Exception {
    MiniDFSCluster cluster = null;
  
    try {
      // Set up cluster for testing
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      initUsers();
    
      FileSystem hdfs = cluster.getFileSystem();
    
      assertTrue(hdfs instanceof DistributedFileSystem);
    
      Path baseDir = new Path("/mcgann");
    
      hdfs.mkdirs(baseDir);
  
      // Create a tmp directory with wide-open permissions and sticky bit
      Path p = new Path(baseDir, "tmp");
  
      hdfs.mkdirs(p);
      hdfs.setPermission(p, new FsPermission((short) 01777));
      applyAcl(p);
      confirmCanAppend(conf, hdfs,  p);
  
      baseDir = new Path("/eccleston");
      hdfs.mkdirs(baseDir);
      p = new Path(baseDir, "roguetraders");
  
      hdfs.mkdirs(p);
      applyAcl(p);
      confirmSettingAndGetting(hdfs, p);
  
      baseDir = new Path("/tennant");
      hdfs.mkdirs(baseDir);
      p = new Path(baseDir, "contemporary");
      hdfs.mkdirs(p);
      hdfs.setPermission(p, new FsPermission((short) 01777));
      applyAcl(p);
      confirmDeletingFiles(conf, hdfs, p);
  
      baseDir = new Path("/smith");
      hdfs.mkdirs(baseDir);
      p = new Path(baseDir, "scissorsisters");
  
      // Turn on its sticky bit
      hdfs.mkdirs(p, new FsPermission((short) 01666));
      applyAcl(p);
      confirmStickyBitDoesntPropagate(hdfs, p);
  
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test that one user can't rename/move another user's file when the sticky
   * bit is set.
   */
  @Test
  public void testMovingFiles() throws Exception {
    testMovingFiles(false);
  }
  
  @Test
  public void testAclMovingFiles() throws Exception {
    testMovingFiles(true);
  }
  
  private void testMovingFiles(boolean useAcl) throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;

    try {
      // Set up cluster for testing
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      initUsers();

      FileSystem hdfs = cluster.getFileSystem();

      assertTrue(hdfs instanceof DistributedFileSystem);

      // Create a tmp directory with wide-open permissions and sticky bit
      Path tmpPath = new Path("/tmp");
      Path tmpPath2 = new Path("/tmp2");
      hdfs.mkdirs(tmpPath);
      hdfs.mkdirs(tmpPath2);
      hdfs.setPermission(tmpPath, new FsPermission((short) 01777));
      if (useAcl) {
        applyAcl(tmpPath);
      }
      hdfs.setPermission(tmpPath2, new FsPermission((short) 01777));
      if (useAcl) {
        applyAcl(tmpPath2);
      }
      // Write a file to the new tmp directory as a regular user
      Path file = new Path(tmpPath, "foo");

      FileSystem hdfs2 = DFSTestUtil.getFileSystemAs(user1, conf);

      writeFile(hdfs2, file);

      // Log onto cluster as another user and attempt to move the file
      FileSystem hdfs3 = DFSTestUtil.getFileSystemAs(user2, conf);

      try {
        hdfs3.rename(file, new Path(tmpPath2, "renamed"));
        fail("Shouldn't be able to rename someone else's file with SB on");
      } catch (IOException ioe) {
        assertTrue(ioe instanceof AccessControlException);
        assertTrue(ioe.getMessage().contains("sticky bit"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Ensure that when we set a sticky bit and shut down the file system, we get
   * the sticky bit back on re-start, and that no extra sticky bits appear
   * after
   * re-start.
   */
  @Test
  public void testStickyBitPersistence() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      initUsers();

      FileSystem hdfs = cluster.getFileSystem();

      assertTrue(hdfs instanceof DistributedFileSystem);

      // A tale of three directories...
      Path sbSet = new Path("/Housemartins");
      Path sbNotSpecified = new Path("/INXS");
      Path sbSetOff = new Path("/Easyworld");

      for (Path p : new Path[]{sbSet, sbNotSpecified, sbSetOff}) {
        hdfs.mkdirs(p);
      }

      // Two directories had there sticky bits set explicitly...
      hdfs.setPermission(sbSet, new FsPermission((short) 01777));
      applyAcl(sbSet);
      hdfs.setPermission(sbSetOff, new FsPermission((short) 00777));
      applyAcl(sbSetOff);

      cluster.shutdown();

      // Start file system up again
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).format(false).build();
      hdfs = cluster.getFileSystem();

      assertTrue(hdfs.exists(sbSet));
      assertTrue(hdfs.getFileStatus(sbSet).getPermission().getStickyBit());

      assertTrue(hdfs.exists(sbNotSpecified));
      assertFalse(
          hdfs.getFileStatus(sbNotSpecified).getPermission().getStickyBit());

      assertTrue(hdfs.exists(sbSetOff));
      assertFalse(hdfs.getFileStatus(sbSetOff).getPermission().getStickyBit());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Write a quick file to the specified file system at specified path
   */
  static private void writeFile(FileSystem hdfs, Path p) throws IOException {
    FSDataOutputStream o = hdfs.create(p);
    o.write("some file contents".getBytes());
    o.close();
  }
  
  /**
   * Applies an ACL (both access and default) to the given path.
   *
   * @param p Path to set
   * @throws IOException if an ACL could not be modified
   */
  private static void applyAcl(Path p) throws IOException {
//    hdfs.modifyAclEntries(p, Arrays.asList(
//        aclEntry(ACCESS, USER, user2.getShortUserName(), ALL),
//        aclEntry(DEFAULT, USER, user2.getShortUserName(), ALL)));
  }

  private void initUsers(){
    user1 = UserGroupInformation
        .createUserForTesting("theDoctor", new String[]{"tardis"});
    user2 = UserGroupInformation
        .createUserForTesting("rose", new String[]{"powellestates"});
  }

}
