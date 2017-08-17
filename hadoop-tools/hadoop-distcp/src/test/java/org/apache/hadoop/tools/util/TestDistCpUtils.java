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

package org.apache.hadoop.tools.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Random;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistCpUtils {
  private static final Log LOG = LogFactory.getLog(TestDistCpUtils.class);

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;
  private static final FsPermission fullPerm = new FsPermission((short) 777);
  private static final FsPermission almostFullPerm = new FsPermission((short) 666);
  private static final FsPermission noPerm = new FsPermission((short) 0);
  
  @BeforeClass
  public static void create() throws IOException {
    cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(1)
        .format(true)
        .build(); 
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetRelativePathRoot() {
    Path root = new Path("/");
    Path child = new Path("/a");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/a");
  }

  @Test
  public void testGetRelativePath() {
    Path root = new Path("/tmp/abc");
    Path child = new Path("/tmp/abc/xyz/file");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/xyz/file");
  }

  @Test
  public void testPackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "");

    attributes.add(FileAttribute.REPLICATION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "R");

    attributes.add(FileAttribute.BLOCKSIZE);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RB");

    attributes.add(FileAttribute.USER);
    attributes.add(FileAttribute.CHECKSUMTYPE);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUC");

    attributes.add(FileAttribute.GROUP);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGC");

    attributes.add(FileAttribute.PERMISSION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGPC");

    attributes.add(FileAttribute.TIMES);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGPCT");
  }

  public void testUnpackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RCBUGPAXT"));

    attributes.remove(FileAttribute.REPLICATION);
    attributes.remove(FileAttribute.CHECKSUMTYPE);
    attributes.remove(FileAttribute.ACL);
    attributes.remove(FileAttribute.XATTR);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("BUGPT"));

    attributes.remove(FileAttribute.TIMES);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("BUGP"));

    attributes.remove(FileAttribute.BLOCKSIZE);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("UGP"));

    attributes.remove(FileAttribute.GROUP);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("UP"));

    attributes.remove(FileAttribute.USER);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("P"));

    attributes.remove(FileAttribute.PERMISSION);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes(""));
  }

  @Test
  public void testPreserveDefaults() throws IOException {
    FileSystem fs = FileSystem.get(config);
    
    // preserve replication, block size, user, group, permission, 
    // checksum type and timestamps    
    EnumSet<FileAttribute> attributes = 
        DistCpUtils.unpackAttributes(
            DistCpOptionSwitch.PRESERVE_STATUS_DEFAULT.substring(1));

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);
    
    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }
  
  @Test
  public void testPreserveNothingOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(dstStatus.getAccessTime() == 100);
    Assert.assertTrue(dstStatus.getModificationTime() == 100);
    Assert.assertTrue(dstStatus.getReplication() == 0);
  }

  @Test
  public void testPreservePermissionOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.PERMISSION);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveGroupOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.GROUP);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveUserOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.USER);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveReplicationOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.REPLICATION);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    // Replication shouldn't apply to dirs so this should still be 0 == 0
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveTimestampOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.TIMES);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
  }

  @Test
  public void testPreserveNothingOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreservePermissionOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.PERMISSION);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveGroupOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.GROUP);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveUserOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.USER);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveReplicationOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.REPLICATION);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveTimestampOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.TIMES);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  private static Random rand = new Random();

  public static String createTestSetup(FileSystem fs) throws IOException {
    return createTestSetup("/tmp1", fs, FsPermission.getDefault());
  }
  
  public static String createTestSetup(FileSystem fs,
                                       FsPermission perm) throws IOException {
    return createTestSetup("/tmp1", fs, perm);
  }

  public static String createTestSetup(String baseDir,
                                       FileSystem fs,
                                       FsPermission perm) throws IOException {
    String base = getBase(baseDir);
    fs.mkdirs(new Path(base + "/newTest/hello/world1"));
    fs.mkdirs(new Path(base + "/newTest/hello/world2/newworld"));
    fs.mkdirs(new Path(base + "/newTest/hello/world3/oldworld"));
    fs.setPermission(new Path(base + "/newTest"), perm);
    fs.setPermission(new Path(base + "/newTest/hello"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world1"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2/newworld"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3/oldworld"), perm);
    createFile(fs, new Path(base, "/newTest/1"));
    createFile(fs, new Path(base, "/newTest/hello/2"));
    createFile(fs, new Path(base, "/newTest/hello/world3/oldworld/3"));
    createFile(fs, new Path(base, "/newTest/hello/world2/4"));
    return base;
  }

  private static String getBase(String base) {
    String location = String.valueOf(rand.nextLong());
    return base + "/" + location;
  }

  public static void delete(FileSystem fs, String path) {
    try {
      if (fs != null) {
        if (path != null) {
          fs.delete(new Path(path), true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception encountered ", e);
    }
  }
  
  public static void createFile(FileSystem fs, String filePath) throws IOException {
    Path path = new Path(filePath);
    createFile(fs, path);
  }

  /** Creates a new, empty file at filePath and always overwrites */
  public static void createFile(FileSystem fs, Path filePath) throws IOException {
    OutputStream out = fs.create(filePath, true);
    IOUtils.closeStream(out);
  }

  /** Creates a new, empty directory at dirPath and always overwrites */
  public static void createDirectory(FileSystem fs, Path dirPath) throws IOException {
    fs.delete(dirPath, true);
    boolean created = fs.mkdirs(dirPath);
    if (!created) {
      LOG.warn("Could not create directory " + dirPath + " this might cause test failures.");
    }
  }

  public static boolean checkIfFoldersAreInSync(FileSystem fs, String targetBase, String sourceBase)
      throws IOException {
    Path base = new Path(targetBase);

     Stack<Path> stack = new Stack<Path>();
     stack.push(base);
     while (!stack.isEmpty()) {
       Path file = stack.pop();
       if (!fs.exists(file)) continue;
       FileStatus[] fStatus = fs.listStatus(file);
       if (fStatus == null || fStatus.length == 0) continue;

       for (FileStatus status : fStatus) {
         if (status.isDirectory()) {
           stack.push(status.getPath());
         }
         Assert.assertTrue(fs.exists(new Path(sourceBase + "/" +
             DistCpUtils.getRelativePath(new Path(targetBase), status.getPath()))));
       }
     }
     return true;
  }
}
