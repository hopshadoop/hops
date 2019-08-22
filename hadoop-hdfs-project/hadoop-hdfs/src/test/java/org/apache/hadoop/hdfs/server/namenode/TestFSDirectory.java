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


import com.google.common.collect.Lists;
import io.hops.TestUtil;
import io.hops.exception.StorageException;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.EnumSet;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test {@link FSDirectory}, the in-memory namespace tree.
 */
public class TestFSDirectory {
  public static final Log LOG = LogFactory.getLog(TestFSDirectory.class);

  private static final long seed = 0;
  private static final short REPLICATION = 3;

  private final Path dir = new Path("/" + getClass().getSimpleName());
  
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  private final Path file2 = new Path(sub1, "file2");

  private final Path sub11 = new Path(sub1, "sub11");
  private final Path file3 = new Path(sub11, "file3");
  private final Path file4 = new Path(sub1, "z_file4");
  private final Path file5 = new Path(sub1, "z_file5");

  private final Path sub2 = new Path(dir, "sub2");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem fsn;
  private FSDirectory fsdir;

  private DistributedFileSystem hdfs;

  private static final int numGeneratedXAttrs = 127;
  private static final ImmutableList<XAttr> generatedXAttrs =
      ImmutableList.copyOf(generateXAttrs(numGeneratedXAttrs));

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();
    
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    
    hdfs = cluster.getFileSystem();
    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, seed);

    DFSTestUtil.createFile(hdfs, file5, 1024, REPLICATION, seed);
    hdfs.mkdirs(sub2);

  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Dump the tree, make some changes, and then dump the tree again. */
  @Test
  public void testDumpTree() throws Exception {
    HopsTransactionalRequestHandler verifyFileBlocksHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.VERIFY_FILE_BLOCKS, dir.toString()) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY,  dir.toString())
                .setNameNodeID(cluster.getNameNode().getId())
                .setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
                .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
                .add(lf.getBlockLock()).add(
                lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC, LockFactory.BLK.UR, LockFactory.BLK.PE, LockFactory.BLK.IV));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        final INode root = fsdir.getINode(dir.toString());

        LOG.info("Original tree");
        final StringBuffer b1 = root.dumpTreeRecursively();
        System.out.println("b1=" + b1);

        final BufferedReader in = new BufferedReader(new StringReader(b1.toString()));

        String line = in.readLine();
        checkClassName(line);

        for (; (line = in.readLine()) != null;) {
          line = line.trim();
          assertTrue(line.startsWith(INodeDirectory.DUMPTREE_LAST_ITEM)
              || line.startsWith(INodeDirectory.DUMPTREE_EXCEPT_LAST_ITEM));
          checkClassName(line);
        }
        return b1;
      }
    };
    final StringBuffer b1 = (StringBuffer) verifyFileBlocksHandler.handle();
    LOG.info("Create a new file " + file4);
    DFSTestUtil.createFile(hdfs, file4, 1024, REPLICATION, seed);

    verifyFileBlocksHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.VERIFY_FILE_BLOCKS, dir.toString()) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                TransactionLockTypes.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, dir.toString())
                .setNameNodeID(cluster.getNameNode().getId())
                .setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getLeaseLock(TransactionLockTypes.LockType.WRITE))
                .add(lf.getLeasePathLock(TransactionLockTypes.LockType.READ_COMMITTED))
                .add(lf.getBlockLock()).add(
                lf.getBlockRelated(LockFactory.BLK.RE, LockFactory.BLK.CR, LockFactory.BLK.UC,
                        LockFactory.BLK.UR, LockFactory.BLK.PE, LockFactory.BLK.IV));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        final INode root = fsdir.getINode(dir.toString());
        final StringBuffer b2 = root.dumpTreeRecursively();
        System.out.println("b2=" + b2);

        int i = 0;
        int j = b1.length() - 1;
        for (; b1.charAt(i) == b2.charAt(i); i++);
        int k = b2.length() - 1;
        for (; b1.charAt(j) == b2.charAt(k); j--, k--);
        final String diff = b2.substring(i, k + 1);
        System.out.println("i=" + i + ", j=" + j + ", k=" + k);
        System.out.println("diff=" + diff);
        Assert.assertTrue(i > j);
        Assert.assertTrue(diff.contains(file4.getName()));
        return null;
      }
    };
    verifyFileBlocksHandler.handle();
  }
  
  static void checkClassName(String line) {
    int i = line.lastIndexOf('(');
    int j = line.lastIndexOf('@');
    final String classname = line.substring(i+1, j);
    assertTrue(classname.equals(INodeFile.class.getSimpleName())
        || classname.equals(INodeDirectory.class.getSimpleName()));
  }
  
  @Test
  public void testINodeXAttrsLimit() throws Exception {
    String testFile = file1.toString();
    
    XAttr xAttr1 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(new byte[]{0x31, 0x32, 0x33}).build();
    XAttr xAttr2 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a2").setValue(new byte[]{0x31, 0x31, 0x31}).build();
    
    addXAttr(testFile, xAttr1);
    addXAttr(testFile, xAttr2);
    
    // Adding a system namespace xAttr, isn't affected by inode xAttrs limit.
    XAttr newXAttr =
        (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.SYSTEM).
        setName("a3").setValue(new byte[]{0x33, 0x33, 0x33}).build();
    
    addXAttr(testFile, newXAttr);
    
    INode inodeFile  = TestUtil.getINode(fsn.getNameNode(), file1);
    
    Assert.assertEquals(inodeFile.getNumUserXAttrs(), 2);
    Assert.assertEquals(inodeFile.getNumSysXAttrs(), 1);
    
    // Adding a trusted namespace xAttr, is affected by inode xAttrs limit.
    XAttr newXAttr1 = (new XAttr.Builder()).setNameSpace(
        XAttr.NameSpace.TRUSTED).setName("a4").
        setValue(new byte[]{0x34, 0x34, 0x34}).build();
    try {
      addXAttr(testFile, newXAttr1);
      fail("Setting user visible xattr on inode should fail if " +
          "reaching limit.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot add additional XAttr " +
          "to inode, would exceed limit", e);
    }
  }
  

  /**
   * Verify that the first <i>num</i> generatedXAttrs are present in
   * newXAttrs.
   */
  private static void verifyXAttrsPresent(List<XAttr> newXAttrs,
      final int num) {
    assertEquals("Unexpected number of XAttrs after multiset", num,
        newXAttrs.size());
    for (int i=0; i<num; i++) {
      XAttr search = generatedXAttrs.get(i);
      assertTrue("Did not find set XAttr " + search + " + after multiset",
          newXAttrs.contains(search));
    }
  }

  private static List<XAttr> generateXAttrs(final int numXAttrs) {
    List<XAttr> generatedXAttrs = Lists.newArrayListWithCapacity(numXAttrs);
    for (int i=0; i<numXAttrs; i++) {
      XAttr xAttr = (new XAttr.Builder())
          .setNameSpace(XAttr.NameSpace.SYSTEM)
          .setName("a" + i)
          .setValue(new byte[] { (byte) i, (byte) (i + 1), (byte) (i + 2) })
          .build();
      generatedXAttrs.add(xAttr);
    }
    return generatedXAttrs;
  }

  /**
   * Test setting and removing multiple xattrs via single operations
   */
  @Test(timeout=300000)
  public void testXAttrMultiSetRemove() throws Exception {
    String testFile = file1.toString();
    
    // Keep adding a random number of xattrs and verifying until exhausted
    final Random rand = new Random(0xFEEDA);
    int numExpectedXAttrs = 0;
    while (numExpectedXAttrs < numGeneratedXAttrs) {
      LOG.info("Currently have " + numExpectedXAttrs + " xattrs");
      final int numToAdd = rand.nextInt(5)+1;

      List<XAttr> toAdd = Lists.newArrayListWithCapacity(numToAdd);
      for (int i = 0; i < numToAdd; i++) {
        if (numExpectedXAttrs >= numGeneratedXAttrs) {
          break;
        }
        toAdd.add(generatedXAttrs.get(numExpectedXAttrs));
        numExpectedXAttrs++;
      }
      LOG.info("Attempting to add " + toAdd.size() + " XAttrs");
      for (int i = 0; i < toAdd.size(); i++) {
        LOG.info("Will add XAttr " + toAdd.get(i));
      }
      List<XAttr> newXAttrs = addXAttr(testFile, toAdd,
          EnumSet.of(XAttrSetFlag.CREATE));
      verifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
    }

    // Keep removing a random number of xattrs and verifying until all gone
    while (numExpectedXAttrs > 0) {
      LOG.info("Currently have " + numExpectedXAttrs + " xattrs");
      final int numToRemove = rand.nextInt(5)+1;
      List<XAttr> toRemove = Lists.newArrayListWithCapacity(numToRemove);
      for (int i = 0; i < numToRemove; i++) {
        if (numExpectedXAttrs == 0) {
          break;
        }
        toRemove.add(generatedXAttrs.get(numExpectedXAttrs-1));
        numExpectedXAttrs--;
      }
      final int expectedNumToRemove = toRemove.size();
      LOG.info("Attempting to remove " + expectedNumToRemove + " XAttrs");
      List<XAttr> removedXAttrs = removeXAttrs(testFile, toRemove);
      List<XAttr> newXAttrs = getXAttrs(testFile);
      assertEquals("Unexpected number of removed XAttrs",
          expectedNumToRemove, removedXAttrs.size());
      verifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
    }
  }

  
  @Test(timeout=300000)
  public void testXAttrMultiAddRemoveErrors() throws Exception {
    String testFile = file1.toString();
    // Test that the same XAttr can not be multiset twice
    List<XAttr> toAdd = Lists.newArrayList();
    toAdd.add(generatedXAttrs.get(0));
    toAdd.add(generatedXAttrs.get(1));
    toAdd.add(generatedXAttrs.get(2));
    toAdd.add(generatedXAttrs.get(0));
    try {
      addXAttr(testFile, toAdd, EnumSet.of(XAttrSetFlag.CREATE));
      fail("Specified the same xattr to be set twice");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot specify the same " +
          "XAttr to be set", e);
    }

    // Test that CREATE and REPLACE flags are obeyed
    toAdd.remove(generatedXAttrs.get(0));
    addXAttr(testFile, generatedXAttrs.get(0));
    try {
      addXAttr(testFile, toAdd, EnumSet.of(XAttrSetFlag.CREATE));
      fail("Set XAttr that is already set without REPLACE flag");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("already exists", e);
    }
    try {
      addXAttr(testFile, toAdd, EnumSet.of(XAttrSetFlag.REPLACE));
      fail("Set XAttr that does not exist without the CREATE flag");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("does not exist", e);
    }

    // Sanity test for CREATE
    toAdd.remove(generatedXAttrs.get(0));
    List<XAttr> newXAttrs = addXAttr(testFile, toAdd,
        EnumSet.of(XAttrSetFlag.CREATE));
    assertEquals("Unexpected toAdd size", 2, toAdd.size());
    for (XAttr x : toAdd) {
      assertTrue("Did not find added XAttr " + x, newXAttrs.contains(x));
    }

    // Sanity test for REPLACE
    toAdd = Lists.newArrayList();
    for (int i=0; i<3; i++) {
      XAttr xAttr = (new XAttr.Builder())
          .setNameSpace(XAttr.NameSpace.SYSTEM)
          .setName("a" + i)
          .setValue(new byte[] { (byte) (i*2) })
          .build();
      toAdd.add(xAttr);
    }
    newXAttrs = addXAttr(testFile, toAdd, EnumSet.of(XAttrSetFlag.REPLACE));
    assertEquals("Unexpected number of new XAttrs", 3, newXAttrs.size());
    for (int i=0; i<3; i++) {
      assertArrayEquals("Unexpected XAttr value",
          new byte[] {(byte)(i*2)}, newXAttrs.get(i).getValue());
    }

    // Sanity test for CREATE+REPLACE
    toAdd = Lists.newArrayList();
    for (int i=0; i<4; i++) {
      toAdd.add(generatedXAttrs.get(i));
    }
    newXAttrs = addXAttr(testFile, toAdd, EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    verifyXAttrsPresent(newXAttrs, 4);
  }
  
  @Test
  public void testSysXAttrsLimit() throws Exception {
    String testFile = file1.toString();
    int maxNumXAttrs = XAttrStorage.getMaxNumberOfSysXAttrPerInode();
    List<XAttr> xAttrs =generateXAttrs(maxNumXAttrs);
    addXAttr(testFile, xAttrs,
        EnumSet.of(XAttrSetFlag.CREATE));
  
    INode inodeFile  = TestUtil.getINode(fsn.getNameNode(), file1);
    Assert.assertEquals(inodeFile.getNumUserXAttrs(), 0);
    Assert.assertEquals(inodeFile.getNumSysXAttrs(), maxNumXAttrs);
  
    // Adding a user namespace xAttr, isn't affected by system xAttrs limit.
    XAttr newXAttr1 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(new byte[]{0x31, 0x32, 0x33}).build();
  
    addXAttr(testFile, newXAttr1);
  
    inodeFile  = TestUtil.getINode(fsn.getNameNode(), file1);
    Assert.assertEquals(inodeFile.getNumUserXAttrs(), 1);
    Assert.assertEquals(inodeFile.getNumSysXAttrs(), maxNumXAttrs);
  
    XAttr newXAttr2 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.SYSTEM).
        setName("as").setValue(new byte[]{0x34, 0x35, 0x36}).build();
    try {
      addXAttr(testFile, newXAttr2);
      fail("Setting system xattr on inode should fail if " +
          "reaching limit.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot add additional " +
          "System XAttr to inode, would exceed limit", e);
    }
  }
  
  private List<XAttr> addXAttr(final String src, final XAttr xAttr) throws IOException {
    List<XAttr> newXAttrs = Lists.newArrayListWithCapacity(1);
    newXAttrs.add(xAttr);
    return addXAttr(src, newXAttrs, EnumSet.of(XAttrSetFlag.CREATE,
        XAttrSetFlag.REPLACE));
  }
  
  private List<XAttr> addXAttr(final String src, final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_XATTR) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, src);
        locks.add(il);
        locks.add(lf.getXAttrLock(xAttrs));
      }
      
      @Override
      public Object performTask() throws IOException {
        FSDirXAttrOp.unprotectedSetXAttrs(fsdir, src, xAttrs, flag);
        return null;
      }
    }.handle();
    return getXAttrs(src);
  }
  
  private List<XAttr> getXAttrs(final String src) throws IOException {
    return (List<XAttr>) new HopsTransactionalRequestHandler(HDFSOperationType.SET_XATTR) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ,
            TransactionLockTypes.INodeResolveType.PATH, src);
        locks.add(il);
        locks.add(lf.getXAttrLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        return FSDirXAttrOp.getXAttrs(fsdir.getINode(src));
      }
    }.handle();
  }
  
  private List<XAttr> removeXAttrs(final String src, final List<XAttr> xAttrs) throws IOException {
    return (List<XAttr>) new HopsTransactionalRequestHandler(HDFSOperationType.SET_XATTR) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, src);
        locks.add(il);
        locks.add(lf.getXAttrLock(xAttrs));
      }
      
      @Override
      public Object performTask() throws IOException {
        return FSDirXAttrOp.unprotectedRemoveXAttrs(fsdir, src, xAttrs);
      }
    }.handle();
  }
}
