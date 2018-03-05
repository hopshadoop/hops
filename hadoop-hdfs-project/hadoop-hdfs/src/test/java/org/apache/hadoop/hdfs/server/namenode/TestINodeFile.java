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

import io.hops.common.IDsGeneratorFactory;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Ignore;

public class TestINodeFile {

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private String userName = "Test";
  private static final PermissionStatus perm = new PermissionStatus(
      "userName", null, FsPermission.getDefault());
  private short replication;
  private long preferredBlockSize;

  @Before
  public void setup() throws IOException{
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
  }
  
  INodeFile createINodeFile(short replication, long preferredBlockSize) throws IOException {
    return new INodeFile(0, perm, null, replication, 0L, 0L, preferredBlockSize, (byte) 0);
  }
    
  private static INodeFile createINodeFile(byte storagePolicyID) throws IOException {
    return new INodeFile(0, perm, null, (short) 3, 0L, 0L, 1024L, storagePolicyID);
  }

  @Test
  public void testStoragePolicyID() throws IOException {
    for (byte i = 1; i < 16; i++) {
      final INodeFile f = createINodeFile(i);
      assertEquals(i, f.getStoragePolicyID());
    }
  }

  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication() throws IOException {
    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
        inf.getBlockReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   *
   * @throws IllegalArgumentException
   *     as the result
   */
  @Test(expected = IllegalArgumentException.class)
  public void testReplicationBelowLowerBound()
      throws IllegalArgumentException, IOException {
    replication = -1;
    preferredBlockSize = 128 * 1024 * 1024;
    createINodeFile(replication, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize() throws IOException {
    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", preferredBlockSize,
        inf.getPreferredBlockSize());
  }

  @Test
  public void testPreferredBlockSizeUpperBound() throws IOException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
        inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   *
   * @throws IllegalArgumentException
   *     as the result
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound()
      throws IllegalArgumentException, IOException {
    replication = 3;
    preferredBlockSize = -1;
    createINodeFile(replication, preferredBlockSize);
  }

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   *
   * @throws IllegalArgumentException
   *     as the result
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound()
      throws IllegalArgumentException, IOException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE + 1;
    createINodeFile(replication, preferredBlockSize);
  }

  
  @Test
  @Ignore
  public void testGetFullPathName()
      throws IOException {
    PermissionStatus perms =
        new PermissionStatus(userName, null, FsPermission.getDefault());

    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile inf =createINodeFile(replication, preferredBlockSize);
    inf.setLocalName("f");

    INodeDirectory root = new INodeDirectory(INodeDirectory.ROOT_ID ,INodeDirectory.ROOT_NAME, perms);
    INodeDirectory dir = new INodeDirectory(IDsGeneratorFactory.getInstance().getUniqueINodeID() ,"d", perms);

    assertEquals("f", inf.getFullPathName());
    assertEquals("", inf.getLocalParentDir());

    dir.addChild(inf, false);
    assertEquals("d" + Path.SEPARATOR + "f", inf.getFullPathName());
    assertEquals("d", inf.getLocalParentDir());
    
    root.addChild(dir, false);
    assertEquals(Path.SEPARATOR + "d" + Path.SEPARATOR + "f",
        inf.getFullPathName());
    assertEquals(Path.SEPARATOR + "d", dir.getFullPathName());

    assertEquals(Path.SEPARATOR, root.getFullPathName());
    assertEquals(Path.SEPARATOR, root.getLocalParentDir());
    
  }
  
  /**
   * FSDirectory#unprotectedSetQuota creates a new INodeDirectoryWithQuota to
   * replace the original INodeDirectory. Before HDFS-4243, the parent field of
   * all the children INodes of the target INodeDirectory is not changed to
   * point to the new INodeDirectoryWithQuota. This testcase tests this
   * scenario.
   */
  @Test
  public void testGetFullPathNameAfterSetQuota() throws Exception {
    long fileLen = 1024;
    replication = 3;
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
          replication).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      FSDirectory fsdir = fsn.getFSDirectory();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // Create a file for test
      final Path dir = new Path("/dir");
      final Path file = new Path(dir, "file");
      DFSTestUtil.createFile(dfs, file, fileLen, replication, 0L);

      // Check the full path name of the INode associating with the file
      checkFullPathName(fsdir, file, cluster);

      // Call FSDirectory#unprotectedSetQuota which calls
      // INodeDirectory#replaceChild
      dfs.setQuota(dir, Long.MAX_VALUE - 1, replication * fileLen * 10);
      final Path newDir = new Path("/newdir");
      final Path newFile = new Path(newDir, "file");
      // Also rename dir
      dfs.rename(dir, newDir, Options.Rename.OVERWRITE);
      // /dir/file now should be renamed to /newdir/file
      // getFullPathName can return correct result only if the parent field of
      // child node is set correctly
      checkFullPathName(fsdir, newFile, cluster);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void checkFullPathName(final FSDirectory fsdir, final Path file, final MiniDFSCluster cluster) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(cluster.getNameNode(),
            TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, file.toString()))
            .add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        INode fnode = fsdir.getINode(file.toString());
        assertEquals(file.toString(), fnode.getFullPathName());
        return null;
      }
    }.handle();
  }
  
  @Test  
  @Ignore
  public void testAppendBlocks()
      throws IOException {
    INodeFile origFile = createINodeFiles(1, "origfile")[0];
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 1L);

    INodeFile[] appendFiles = createINodeFiles(4, "appendfile");
    origFile.appendBlocks(appendFiles, getTotalBlocks(appendFiles));
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 5L);
  }

  /**
   * Gives the count of blocks for a given number of files
   *
   * @param files
   *     Array of INode files
   * @return total count of blocks
   */
  private int getTotalBlocks(INodeFile[] files)
      throws StorageException, TransactionContextException {
    int nBlocks = 0;
    for (INodeFile file : files) {
      nBlocks += file.numBlocks();
    }
    return nBlocks;
  }
  
  /**
   * Creates the required number of files with one block each
   */
  static int blkid = 0;

  private INodeFile[] createINodeFiles(int nCount, String fileNamePrefix)
      throws IOException {

    if (nCount <= 0) {
      return new INodeFile[1];
    }
    
    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile[] iNodes = new INodeFile[nCount];
    for (int i = 0; i < nCount; i++) {
      PermissionStatus perms =
          new PermissionStatus(userName, null, FsPermission.getDefault());
      iNodes[i] =
          new INodeFile(IDsGeneratorFactory.getInstance().getUniqueINodeID(), perms, null, replication, 0L, 0L, preferredBlockSize, (byte) 0);
      iNodes[i].setLocalNameNoPersistance(fileNamePrefix + Integer.toString(i));
      BlockInfo newblock = new BlockInfo();
      newblock.setBlockId(blkid++);
      newblock.setINodeId(iNodes[i].getId());
      iNodes[i].addBlock(newblock);
    }
    
    return iNodes;
  }

  /**
   * Test for the static {@link INodeFile#valueOf(INode, String)}
   * and {@link INodeFileUnderConstruction#valueOf(INode, String)} methods.
   *
   * @throws IOException
   */
  @Test
  public void testValueOf() throws IOException {
    final String path = "/testValueOf";
    final PermissionStatus perm =
        new PermissionStatus(userName, null, FsPermission.getDefault());
    final short replication = 3;

    {//cast from null
      final INode from = null;

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch (FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch (FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch (FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Directory does not exist"));
      }
    }

    {//cast from INodeFile
      final INode from =
          new INodeFile(0, perm, null, replication, 0L, 0L, preferredBlockSize, (byte) 0);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().contains("File is not under construction"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(PathIsNotDirectoryException e) {
      }
    }

    {//cast from INodeFileUnderConstruction
      final INode from =
          new INodeFileUnderConstruction(0, perm, replication, 0L, 0L, "client",
              "machine", null, (byte) 0);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should success
      final INodeFileUnderConstruction u =
          INodeFileUnderConstruction.valueOf(from, path);
      assertTrue(u == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch (PathIsNotDirectoryException e) {
      }
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(0, perm, 0L);

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch (FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch (FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeDirectory, should success
      final INodeDirectory d = INodeDirectory.valueOf(from, path);
      assertTrue(d == from);
    }
  }
  
  /**
   * Verify root always has inode id 1001 and new formated fsimage has last
   * allocated inode id 1000. Validate correct lastInodeId is persisted.
   * @throws IOException
   */
  @Test
  public void TestInodeId() throws IOException {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {    
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      int initialId = IDsGeneratorFactory.getInstance().getUniqueINodeID();

      // Create one directory and the last inode id should increase to 1002
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test1");
      assertTrue(fs.mkdirs(path));
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 2);//we can't check the id witout increasing it

      int fileLen = 1024;
      Path filePath = new Path("/test1/file");
      DFSTestUtil.createFile(fs, filePath, fileLen, (short) 1, 0);
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 4); //we can't check the id witout increasing it

      // Rename doesn't increase inode id
      Path renamedPath = new Path("/test2");
      fs.rename(path, renamedPath);
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 5);//we can't check the id witout increasing it

      cluster.restartNameNode();
      cluster.waitActive();
      // Make sure empty editlog can be handled
      cluster.restartNameNode();
      cluster.waitActive();
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 6);//we can't check the id witout increasing it

      DFSTestUtil.createFile(fs, new Path("/test2/file2"), fileLen, (short) 1,
          0);
      long id = IDsGeneratorFactory.getInstance().getUniqueINodeID();
      assertTrue(id == initialId + 8);
      fs.delete(new Path("/test2"), true);
      // create a file under construction
      FSDataOutputStream outStream = fs.create(new Path("/test3/file"));
      assertTrue(outStream != null);
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 11);

      // The lastInodeId in fsimage should remain 1006 after reboot
      cluster.restartNameNode();
      cluster.waitActive();
      assertTrue(IDsGeneratorFactory.getInstance().getUniqueINodeID() == initialId + 12);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
