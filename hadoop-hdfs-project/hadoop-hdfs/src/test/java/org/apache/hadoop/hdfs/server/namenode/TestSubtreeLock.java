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
import io.hops.transaction.handler.RequestHandler;
import io.hops.transaction.lock.SubtreeLockedException;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestSubtreeLock extends TestCase {

  @Test
  public void testSubtreeLocking() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      Path path0 = new Path("/folder0");
      Path path1 = new Path(path0.toUri().getPath(), "folder1");
      Path path2 = new Path(path1.toUri().getPath(), "folder2");

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());

      FSNamesystem namesystem = cluster.getNamesystem();
      namesystem.lockSubtree(path1.toUri().getPath());

      boolean exception = false;
      try {
        namesystem.lockSubtree(path1.toUri().getPath());
      } catch (SubtreeLockedException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked node",
          exception);

      exception = false;
      try {
        namesystem.lockSubtree(path2.toUri().getPath());
      } catch (SubtreeLockedException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked subtree",
          exception);

      namesystem.unlockSubtree(path1.toUri().getPath());
      namesystem.lockSubtree(path2.toUri().getPath());
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
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
      fileTree.buildUp();
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
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
      TestFileCreation.writeFile(stm, bytes0);
      stm.close();
      dfs.create(file2).close();
      final int bytes1 = 253;
      stm = dfs.create(file3);
      TestFileCreation.writeFile(stm, bytes1);
      stm.close();

      AbstractFileTree.CountingFileTree fileTree = AbstractFileTree
          .createCountingFileTreeFromPath(cluster.getNamesystem(),
              path0.toUri().getPath());
      fileTree.buildUp();
      assertEquals(7, fileTree.getNamespaceCount());
      assertEquals(bytes0 + bytes1, fileTree.getDiskspaceCount());
      assertEquals(3, fileTree.getDirectoryCount());
      assertEquals(4, fileTree.getFileCount());
      assertEquals(fileTree.getDiskspaceCount(), fileTree.getFileSizeSummary());
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
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
      namesystem0.lockSubtree(path1.toUri().getPath());

      boolean exception = false;
      try {
        namesystem1.lockSubtree(path1.toUri().getPath());
      } catch (SubtreeLockedException e) {
        exception = true;
      }
      assertTrue("Succeeded to acquire lock on previously locked node",
          exception);

      cluster.shutdownNameNode(0);
      Thread.sleep(4000);
      namesystem1.lockSubtree(path1.toUri().getPath());
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
      final long RETRY_WAIT = RequestHandler.BASE_WAIT_TIME;
      final long RETRY_COUNT = RequestHandler.RETRY_COUNT;
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
            Thread.sleep((RETRY_COUNT - 1) * RETRY_WAIT);
            namesystem.unlockSubtree(path1.toUri().getPath());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(path0, FsPermission.getDefault());
      dfs.mkdir(path1, FsPermission.getDefault());
      dfs.mkdir(path2, FsPermission.getDefault());

      namesystem.lockSubtree(path1.toUri().getPath());
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      assertFalse(cluster.getFileSystem().delete(new Path("/foo/"), true));
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));

      TestFileCreation.createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.delete(new Path("/foo/bar"), false));
      assertFalse(fs.exists(new Path("/foo/bar")));

      TestFileCreation.createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.delete(new Path("/foo/bar"), true));
      assertFalse(fs.exists(new Path("/foo/bar")));

      assertTrue(fs.mkdir(new Path("/foo/bar"), FsPermission.getDefault()));
      TestFileCreation.createFile(fs, new Path("/foo/bar/foo"), 1).close();
      assertTrue(fs.delete(new Path("/foo"), true));
      assertFalse(fs.exists(new Path("/foo/bar/foo")));
      assertFalse(fs.exists(new Path("/foo/bar/foo")));
      assertFalse(fs.exists(new Path("/foo/bar")));
      assertFalse(fs.exists(new Path("/foo")));
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      TestFileCreation.createFile(fs, new Path("/foo/bar"), 1);
      assertTrue(fs.delete(new Path("/foo/bar"), true));
      assertFalse(fs.exists(new Path("/foo/bar")));
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
      conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY, 0);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdir(new Path("/foo"), FsPermission.getDefault()));
      TestFileCreation.createFile(fs, new Path("/foo/bar"), 1).close();
      assertTrue(fs.mkdir(new Path("/foo1"), FsPermission.getDefault()));
      TestFileCreation.createFile(fs, new Path("/foo1/bar1"), 1).close();
      fs.rename(new Path("/foo1/bar1"), new Path("/foo/bar1"),
          Options.Rename.OVERWRITE);
      assertTrue(fs.exists(new Path("/foo/bar1")));
      assertFalse(fs.exists(new Path("/foo1/bar1")));

      try {
        fs.rename(new Path("/foo1/bar1"), new Path("/foo/bar1"),
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
}
