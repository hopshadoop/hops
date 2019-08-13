/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.TestUtil;
import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdfs.TestFileCreation.createFile;
import static org.apache.hadoop.hdfs.TestFileCreation.writeFile;
import static org.mockito.Matchers.*;

public class TestSubtreeLock extends TestCase {
  static final Log LOG = LogFactory.getLog(TestSubtreeLock.class);

  @Test
  public void testSubtreeLocking() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());

      FSNamesystem namesystem = cluster.getNamesystem();
      namesystem.lockSubtree(path1.toUri().getPath(), SubTreeOperation.Type.NA);

      boolean exception = false;
      try {
        namesystem.lockSubtree(path1.toUri().getPath(), SubTreeOperation.Type.NA);
      } catch (RetriableException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked node",
              exception);

      exception = false;
      try {
        namesystem.lockSubtree(path2.toUri().getPath(), SubTreeOperation.Type.NA);
      } catch (RetriableException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked subtree",
              exception);

      namesystem.unlockSubtree(path1.toUri().getPath()
              , getSubTreeRootID(path1.toUri().getPath()));
      namesystem.lockSubtree(path2.toUri().getPath(), SubTreeOperation.Type.NA);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSubtreeNestedLocking() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/A/B/C/D/E"), FsPermission.getDefault());

      FSNamesystem namesystem = cluster.getNamesystem();
      namesystem.lockSubtree("/A/B/C/D/E", SubTreeOperation.Type.NA);

      boolean exception = false;
      try {
        namesystem.lockSubtree("/A/B/C", SubTreeOperation.Type.NA);
      } catch (RetriableException e) {
        exception = true;
      }
      assertTrue("Failed. Took a lock while sub tree was locked", exception);

      namesystem.unlockSubtree("/A/B/C/D/E", getSubTreeRootID("/A/B/C/D/E"));

      assertFalse("Not all subtree locsk are removed", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFileTree() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");
      Path file0 = new Path(path0.toUri().getPath(), "file0");
      Path file1 = new Path(path1.toUri().getPath(), "file1");
      Path file2 = new Path(path2.toUri().getPath(), "file2");
      Path file3 = new Path(path2.toUri().getPath(), "file3");

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());
      dfs.create(file0).close();
      dfs.create(file1).close();
      dfs.create(file2).close();
      dfs.create(file3).close();

      AbstractFileTree.FileTree fileTree = AbstractFileTree
              .createFileTreeFromPath(cluster.getNamesystem(),
                      path0.toUri().getPath());
      fileTree.buildUp(cluster.getNameNode().getNamesystem().getBlockManager().getStoragePolicySuite());
      assertEquals(path0.getName(), fileTree.getSubtreeRoot().getName());
      assertEquals(7, fileTree.getAll().size());
      assertEquals(4, fileTree.getHeight());
      assertEquals(file3.toUri().getPath(), fileTree
              .createAbsolutePath(path0.toUri().getPath(), fileTree.getInodeById(
                      TestUtil.getINodeId(cluster.getNameNode(), file3))));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCountingFileTree() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");
      Path file0 = new Path(path0.toUri().getPath(), "file0");
      Path file1 = new Path(path1.toUri().getPath(), "file1");
      Path file2 = new Path(path2.toUri().getPath(), "file2");
      Path file3 = new Path(path2.toUri().getPath(), "file3");

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());
      dfs.create(file0).close();
      final int bytes0 = 123;
      FSDataOutputStream stm = dfs.create(file1);
      writeFile(stm, bytes0);
      stm.close();
      dfs.create(file2).close();
      final int bytes1 = 253;
      stm = dfs.create(file3);
      writeFile(stm, bytes1);
      stm.close();

      AbstractFileTree.CountingFileTree fileTree = AbstractFileTree
              .createCountingFileTreeFromPath(cluster.getNamesystem(),
                      path0.toUri().getPath());
      fileTree.buildUp(cluster.getNameNode().getNamesystem().getBlockManager().getStoragePolicySuite());
      assertEquals(7, fileTree.getUsedCounts().getNameSpace());
      assertEquals(bytes0 + bytes1, fileTree.getUsedCounts().getStorageSpace());
      assertEquals(3, fileTree.getCounts().getDirectoryCount());
      assertEquals(4, fileTree.getCounts().getFileCount());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNameNodeFailureLockAcquisition()
          throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf)
              .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(2)).format(true)
              .numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");

      DistributedFileSystem dfs0 = cluster.getFileSystem(0);
      dfs0.mkdir(path0, FsPermission.getDefault());
      dfs0.mkdir(path1, FsPermission.getDefault());
      dfs0.mkdir(path2, FsPermission.getDefault());

      FSNamesystem namesystem0 = cluster.getNamesystem(0);
      FSNamesystem namesystem1 = cluster.getNamesystem(1);
      namesystem0.lockSubtree(path1.toUri().getPath(),
              SubTreeOperation.Type.NA);

      boolean exception = false;
      try {
        namesystem1.lockSubtree(path1.toUri().getPath(),
                SubTreeOperation.Type.NA);
      } catch (RetriableException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked node",
              exception);

      cluster.shutdownNameNode(0);

      long delay = conf.getLong(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY, DFSConfigKeys
              .DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT) * (conf.getInt(DFSConfigKeys
              .DFS_LEADER_MISSED_HB_THRESHOLD_KEY, DFSConfigKeys
              .DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT) + 1);
      Thread.sleep(delay);
      namesystem1.lockSubtree(path1.toUri().getPath(),
              SubTreeOperation.Type.NA);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRetry() throws IOException {
    MiniDFSCluster cluster = null;
    Thread lockKeeper = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      final Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");

      final FSNamesystem namesystem = cluster.getNamesystem();
      lockKeeper = new Thread() {
        @Override
        public void run() {
          super.run();
          try {
            Thread.sleep(10000); // 10 sec, See DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT
            namesystem.unlockSubtree(path1.toUri().getPath(),
                    getSubTreeRootID(path1.toUri().getPath()));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());

      namesystem.lockSubtree(path1.toUri().getPath(),
              SubTreeOperation.Type.NA);
      lockKeeper.start();

      assertTrue(dfs.delete(path1, true));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      try {
        if (lockKeeper != null) {
          lockKeeper.join();
        }
      } catch (InterruptedException e) {
      }
    }
  }

  @Test
  public void testDeleteRoot() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      assertFalse(cluster.getFileSystem().delete(new Path("/"), true));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteNonExisting() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      assertFalse(cluster.getFileSystem().delete(new Path("/foo/"), true));
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));

      createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.delete(new Path("/foo/bar"), false));
      assertFalse(fs.exists(new Path("/foo/bar")));

      createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.delete(new Path("/foo/bar"), true));
      assertFalse(fs.exists(new Path("/foo/bar")));

      assertTrue(fs.mkdir(new Path("/foo/bar"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo/bar/foo"), 1).close();
      assertTrue(fs.delete(new Path("/foo"), true));
      assertFalse(fs.exists(new Path("/foo/bar/foo")));
      assertFalse(fs.exists(new Path("/foo/bar/foo")));
      assertFalse(fs.exists(new Path("/foo/bar")));
      assertFalse(fs.exists(new Path("/foo")));

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDelete2() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/a"), FsPermission.getDefault()));

      assertTrue(fs.mkdir(new Path("/a/b"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/a/c"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/a/d"), FsPermission.getDefault()));

      for (int i = 0; i < 5; i++) {
        createFile(fs, new Path("/a/b/a_b_file_" + i), 1).close();
      }

      for (int i = 0; i < 5; i++) {
        createFile(fs, new Path("/a/b/a_b_file_" + i), 1).close();
      }

      for (int i = 0; i < 5; i++) {
        createFile(fs, new Path("/a/c/a_c_file_" + i), 1).close();
      }

      for (int i = 0; i < 10; i++) {
        assertTrue(fs.mkdirs(new Path("/a/b/c/a_b_c_dir_" + i)));
      }

      for (int i = 0; i < 3; i++) {
        assertTrue(fs.mkdirs(new Path("/a/b/a_b_dir_" + i)));
      }

      for (int i = 0; i < 3; i++) {
        createFile(fs, new Path("/a/b/c/b/a_b_c_d_file_" + i), 1).close();
      }
      assertTrue(fs.delete(new Path("/a")));
      assertFalse(fs.exists(new Path("/a")));

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteUnclosed() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo/bar"), 1);
      assertTrue(fs.delete(new Path("/foo/bar"), true));
      assertFalse(fs.exists(new Path("/foo/bar")));
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteSimple() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo/bar"), 1).close();

      assertTrue(fs.delete(new Path("/foo"), true));
      assertFalse(fs.exists(new Path("/foo")));
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMove() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.mkdir(new Path("/foo1"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo1/bar1"), 1).close();
      fs.rename(new Path("/foo1/bar1"), new Path("/foo/bar1"),
              Options.Rename.OVERWRITE);
      assertTrue(fs.exists(new Path("/foo/bar1")));
      assertFalse(fs.exists(new Path("/foo1/bar1")));

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

      try {
        fs.rename(new Path("/foo1/bar"), new Path("/foo/bar1"),
                Options.Rename.OVERWRITE);
        fail();
      } catch (FileNotFoundException e) {
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDepricatedRenameMoveFiles() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      createFile(fs, new Path("/foo/file1.txt"), 1).close();
      createFile(fs, new Path("/bar/file1.txt"), 1).close();

      assertTrue("Rename Failed", fs.rename(new Path("/foo/file1.txt"), new Path("/bar/file2.txt")));

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDepricatedRenameDirs() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      assertTrue(fs.mkdir(new Path("/foo1"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo2"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo1/dir1"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo2/dir2"), FsPermission.getDefault()));
      assertTrue("Rename Failed", fs.rename(new Path("/foo1/dir1"), new Path("/foo2/dir2")));

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameDirs() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      assertTrue(fs.mkdir(new Path("/foo1"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo2"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo1/dir1"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo2/dir2"), FsPermission.getDefault()));

      boolean isException = false;
      Exception exception = null;

      try {
        fs.rename(new Path("/foo1/dir1"), new Path("/foo2/dir2/dir1"), Options.Rename.NONE);
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Rename failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

      try {
        fs.rename(new Path("/foo1"), new Path("/foo2/dir2"), Options.Rename.OVERWRITE);
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertTrue("Rename failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameDirs2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      assertTrue(fs.mkdirs(new Path("/foo1/foo2"), FsPermission.getDefault()));
      assertTrue(fs.mkdirs(new Path("/bar1/bar2"), FsPermission.getDefault()));


      boolean isException = false;
      Exception exception = null;

      try {
        fs.rename(new Path("/foo1/foo2"), new Path("/bar1/bar2/"), Options.Rename.OVERWRITE);
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Rename failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameMoveFiles() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      createFile(fs, new Path("/foo/file1.txt"), 1).close();
      createFile(fs, new Path("/bar/file1.txt"), 1).close();

      boolean isException = false;
      Exception exception = null;

      try {
        fs.rename(new Path("/foo/file1.txt"), new Path("/bar/file2.txt"), Options.Rename.NONE);
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Rename failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

      //test overwrite
      try {
        fs.rename(new Path("/bar/file1.txt"), new Path("/bar/file2.txt"), Options.Rename.OVERWRITE);
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Rename failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * checks if all the sub tree locks are removed and no flags are set
   */
  public static boolean subTreeLocksExists() throws IOException {
    if (countAllSubTreeLocks() > 0) {
      return true;
    }
    return false;
  }

  public static int countAllSubTreeLocks() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST_SUBTREE_LOCK) {
              @Override
              public Object performTask() throws StorageException, IOException {
                int count = 0;
                INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);

                OngoingSubTreeOpsDataAccess oda = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory.getDataAccess(OngoingSubTreeOpsDataAccess.class);

                List<INode> inodes = ida.allINodes();
                List<SubTreeOperation> ops = (List<SubTreeOperation>) oda.allOps();

                if (ops != null && !ops.isEmpty()) {
                  for (SubTreeOperation op : ops) {
                    count++;
                    LOG.debug("On going sub tree operations table contains: \" " + op.getPath()
                            + "\" NameNode id: " + op.getNameNodeId() + " OpType: " + op.getOpType() + " " +
                            "count " + count);
                  }
                }

                if (inodes != null && !inodes.isEmpty()) {
                  for (INode inode : inodes) {
                    if (inode.isSTOLocked()) {
                      LOG.error("INode lock flag is set. Name " + inode.getLocalName()
                              + " id: " + inode.getId() + " pid: " + inode.getParentId()
                              + " locked by " + inode.getSTOLockOwner());
                    }
                  }
                }

                return count;
              }
            };
    return (Integer) subTreeLockChecker.handle();
  }

  @Test
  public void testSubtreeSetPermssion() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/A/B/C/D/E"), FsPermission.getDefault());

      boolean isException = false;
      Exception exception = null;

      try {
        dfs.setPermission(new Path("/A/B/C"), new FsPermission((short) 0777));
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Set Permission failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());


      try {
        dfs.setPermission(new Path("/A"), new FsPermission((short) 0777));
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Set Permission failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSubtreeSetOwner() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/A/B/C/D/E"), FsPermission.getDefault());

      boolean isException = false;
      Exception exception = null;

      try {
        dfs.setOwner(new Path("/A/B/C"), "test", "test");
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertFalse("Set Permission failed. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());


      isException = false;
      exception = null;

      try {
        dfs.setOwner(new Path("/A/z"), "test", "test"); //non existing path
      } catch (Exception e) {
        isException = true;
        exception = e;
      }
      assertTrue("Set Permission was supposed to fail. " + exception, isException);
      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  //taking sto lock on a file should be ignored by the lock manager 
  @Test
  public void testSubtreeIgnoreLockRequest() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/foo"));
      createFile(dfs, new Path("/foo/file1.txt"), 1).close();

      boolean isException = false;
      Exception exception = null;


      INodeIdentifier inode = cluster.getNamesystem().lockSubtree("/foo/file1.txt", SubTreeOperation.Type.NA);
      if (inode != null) {
        fail("nothing should have been locked");
      }

      inode = cluster.getNamesystem().lockSubtree("/", SubTreeOperation.Type.NA);
      if (inode != null) {
        fail("root should not have been locked");
      }


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testSubtreeMove() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/Projects/test"));
      dfs.setMetaEnabled(new Path("/Projects"), true);


      try {
        dfs.listStatus(new Path("/Projects"));
        dfs.rename(new Path("/Projects/test"), new Path("/Projects/test1"));
        dfs.rename(new Path("/Projects/test1"), new Path("/Projects/test"));
      } catch (Exception e) {
        fail("No exception should have been thrown ");
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testRecursiveDeleteAsaProxyUser() throws Exception {
    Configuration conf = new HdfsConfiguration();

    final int NumberOfFileSystems = 100;
    final String UserPrefix = "testUser";

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");

    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).format(true).build();
    cluster.waitActive();

    FileSystem superFS = cluster.getFileSystem();

    List<UserGroupInformation> ugis = new ArrayList<>();
    List<FileSystem> fss = new ArrayList<>();

    for (int u = 0; u < NumberOfFileSystems; u++) {
      UserGroupInformation ugi =
              UserGroupInformation.createProxyUserForTesting(UserPrefix + u,
                      UserGroupInformation
                              .getLoginUser(), new String[]{UserPrefix + u});

      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {

        @Override
        public FileSystem run() throws Exception {
          return cluster.getFileSystem();
        }
      });

      ugis.add(ugi);
      fss.add(fs);
    }

    try {

      superFS.mkdirs(new Path("/root"));
      superFS.setPermission(new Path("/root"), new FsPermission(FsAction.ALL,
              FsAction.ALL, FsAction.ALL));


      for (int u = 0; u < fss.size(); u++) {
        FileSystem fs = fss.get(u);
        Path root = new Path(String.format("/root/a%d", u));
        fs.mkdirs(root);
        fs.setOwner(root, UserPrefix + u, UserPrefix + u);

        fs.mkdirs(new Path(root, "b" + u));
        fs.mkdirs(new Path(root, "c" + u));

        fs.create(new Path(root, "b" + u + "/f")).close();
        fs.create(new Path(root, "c" + u + "/f")).close();

      }

      for (int u = 0; u < fss.size(); u++) {
        FileSystem fs = fss.get(u);
        Assert
                .assertTrue(fs.delete(new Path(String.format("/root/a%d", u)), true));
        FileSystem.closeAllForUGI(ugis.get(u));
      }

    } finally {
      cluster.shutdown();
    }
  }

  private long getSubTreeRootID(final String path) throws IOException {
    LinkedList<INode> nodes = new LinkedList<>();
    INodeUtil.resolvePathWithNoTransaction(path, false, nodes);
    return nodes.getLast().getId();
  }

  @Test
  public void testFailedUnsetSTOLock() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      assertTrue(fs.mkdir(new Path("/foo/bar"), FsPermission.getDefault()));
      createFile(fs, new Path("/foo/bar/foo"), 1).close();

      final FSNamesystem fsNamesystem = cluster.getNameNode().getNamesystem();
      final FSNamesystem fsNamesystemSpy = Mockito.spy(fsNamesystem);
      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      rpcServer.setFSNamesystem(fsNamesystemSpy);

      final String error = "Unable to remove STO Lock";

      Mockito.doThrow(new StorageException(error)).when(fsNamesystemSpy).unlockSubtreeInternal(anyString(),
              anyLong());

      fsNamesystem.lockSubtree("/foo", SubTreeOperation.Type.DELETE_STO);

      try {
        fsNamesystemSpy.unlockSubtree("/foo", cluster.getNameNode().getId());
      } catch (IOException e) {
        LOG.info(e, e);
        if (!e.getMessage().contains(error)) {
          throw e;
        }
      }

      // also add onother bogus STO lock
      addOngoingSTOEntry("/test/");

      int tries = 0;
      while (subTreeLocksExists()) {
        Thread.sleep(1000);
        if (++tries >= 10) {
          break;
        }
        LOG.info("Waiting to metadata cleaner to remove the lock");
      }

      assertFalse("Not All subtree locks were removed after operation ", subTreeLocksExists());
    } catch (Exception e) {
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  void addOngoingSTOEntry(final String path) throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
            HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        OngoingSubTreeOpsDataAccess<SubTreeOperation> dataAccess =
                (OngoingSubTreeOpsDataAccess) HdfsStorageFactory.getDataAccess(OngoingSubTreeOpsDataAccess.class);
        SubTreeOperation op = new SubTreeOperation(path, -1, -1, SubTreeOperation.Type.NA,
            System.currentTimeMillis(), "user", System.currentTimeMillis());
        List<SubTreeOperation> modified = new ArrayList<SubTreeOperation>();
        modified.add(op);
        dataAccess.prepare(Collections.EMPTY_LIST, Collections.EMPTY_LIST, modified);
        return null;
      }
    };
    handler.handle();
  }

}
