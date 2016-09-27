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
package org.apache.hadoop.hdfs;

import io.hops.TestUtil;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class TestMetadataLog extends TestCase {
  private static final int ANY_DATASET = -1;

  private boolean checkLog(int inodeId, MetadataLogEntry.Operation operation)
      throws IOException {
    return checkLog(ANY_DATASET, inodeId, operation);
  }

  private boolean checkLog(int datasetId, int inodeId,
      MetadataLogEntry.Operation operation) throws IOException {
    Collection<MetadataLogEntry> logEntries = getMetadataLogEntries(inodeId);
    for (MetadataLogEntry logEntry : logEntries) {
      if (logEntry.getOperation().equals(operation)) {
        if (datasetId == ANY_DATASET || datasetId == logEntry.getDatasetId()) {
          return true;
        }
      }
    }
    return false;
  }

  private Collection<MetadataLogEntry> getMetadataLogEntries(final int inodeId)
      throws IOException {
    return (Collection<MetadataLogEntry>) new LightWeightRequestHandler(
        HDFSOperationType.GET_METADATA_LOG_ENTRIES) {
      @Override
      public Object performTask() throws IOException {
        MetadataLogDataAccess da = (MetadataLogDataAccess)
            HdfsStorageFactory.getDataAccess(MetadataLogDataAccess.class);
        return da.find(inodeId);
      }
    }.handle();
  }

  @Test
  public void testNonLoggingFolder() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      final Path subdir = new Path(dataset, "subdir");
      Path file = new Path(dataset, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.mkdirs(subdir);
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), subdir),
          MetadataLogEntry.Operation.ADD));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCreate() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      final Path subdir = new Path(dataset, "subdir");
      Path file = new Path(subdir, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      dfs.mkdirs(subdir);
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), subdir),
          MetadataLogEntry.Operation.ADD));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNoLogEntryBeforeClosing() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      Path file = new Path(dataset, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
      out.close();
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDelete() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      int folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      dfs.delete(folder, true);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.DELETE));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testOldRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path dataset1 = new Path(project, "dataset1");
      Path file0 = new Path(dataset0, "file");
      Path file1 = new Path(dataset1, "file");
      dfs.mkdirs(dataset0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      int dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      int dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      assertTrue(dfs.rename(file0, file1));
      assertTrue(checkLog(dataset0Id, inodeId,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path dataset1 = new Path(project, "dataset1");
      Path file0 = new Path(dataset0, "file");
      Path file1 = new Path(dataset1, "file");
      dfs.mkdirs(dataset0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      int dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      int dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      dfs.rename(file0, file1, Options.Rename.NONE);
      assertTrue(checkLog(dataset0Id, inodeId,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepOldRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path folder0 = new Path(dataset0, "folder0");
      Path dataset1 = new Path(project, "dataset1");
      Path folder1 = new Path(dataset1, "folder1");
      Path file0 = new Path(folder0, "file");

      dfs.mkdirs(folder0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();

      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      int folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      int dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      int dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);

      assertTrue(checkLog(dataset0Id, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));

      dfs.rename(folder0, folder1);

      int folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      assertTrue(checkLog(dataset0Id, folder0Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset0Id, inodeId,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset1Id, folder1Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path folder0 = new Path(dataset0, "folder0");
      Path dataset1 = new Path(project, "dataset1");
      Path folder1 = new Path(dataset1, "folder1");
      Path file0 = new Path(folder0, "file");

      dfs.mkdirs(folder0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();

      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      int folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      int dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      int dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);

      assertTrue(checkLog(dataset0Id, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));

      dfs.rename(folder0, folder1, Options.Rename.NONE);

      int folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      assertTrue(checkLog(dataset0Id, folder0Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset0Id, inodeId,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(dataset1Id, folder1Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnableLogForExistingDirectory() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_LEGACY_DELETE_ENABLE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "dataset");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      dfs.setMetaEnabled(dataset, true);
      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      int folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteFileWhileOpen() throws Exception {
    Configuration conf = new HdfsConfiguration();

    final int BYTES_PER_CHECKSUM = 1;
    final int PACKET_SIZE = BYTES_PER_CHECKSUM;
    final int BLOCK_SIZE = 1 * PACKET_SIZE;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);

      FSDataOutputStream out = dfs.create(file);
      out.writeByte(0);

      int inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      int folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertFalse(checkLog(inodeId, MetadataLogEntry.Operation.ADD));

      DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      try {
        dfs2.delete(file, false);
      }catch (Exception ex){
        fail("we shouldn't have any exception: " + ex.getMessage());
      }

      assertFalse(checkLog(inodeId, MetadataLogEntry.Operation.DELETE));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
