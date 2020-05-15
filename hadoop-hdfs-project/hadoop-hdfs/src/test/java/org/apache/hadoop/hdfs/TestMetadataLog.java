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

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import io.hops.TestUtil;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.metadata.hdfs.entity.MetaStatus;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.metadata.hdfs.entity.XAttrMetadataLogEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.XAttrStorage;
import org.apache.hadoop.hdfs.server.namenode.XAttrTestHelpers;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class TestMetadataLog extends TestCase {
  private static final int ANY_DATASET = -1;
  private static final String ANY_NAME = "-1";

  private static Comparator<MetadataLogEntry> LOGICAL_TIME_COMPARATOR = new Comparator<MetadataLogEntry>() {
    @Override
    public int compare(MetadataLogEntry o1, MetadataLogEntry o2) {
      return Integer.compare(o1.getLogicalTime(), o2.getLogicalTime());
    }
  };

  private boolean checkLog(final INode inode,
      final INodeMetadataLogEntry.Operation operation)
      throws IOException {
    return checkLog(null, inode, operation);
  }
  
  private boolean checkLog(final INode dataset, final INode inode,
      final INodeMetadataLogEntry.Operation operation) throws IOException {
    final long datasetId = dataset == null ? ANY_DATASET : dataset.getId();
    Collection<MetadataLogEntry> logEntries = getMetadataLogEntries(inode.getId());
    for (MetadataLogEntry logEntry : logEntries) {
      if (logEntry.getOperationId() == operation.getId()) {
        return (datasetId == ANY_DATASET || datasetId == logEntry.getDatasetId())
            && inode.getPartitionId() == logEntry.getInodePartitionId()
            && inode.getParentId() == logEntry.getInodeParentId()
            && inode.getLocalName().equals(logEntry.getInodeName());
      }
    }
    return false;
  }
  
  private boolean checkXAttrLogAddAll(final INode dataset, final INode inode) throws IOException {
    return checkXAttrLog(dataset, inode, ANY_NAME, (byte)-1, (short)-1,
        XAttrMetadataLogEntry.Operation.AddAll);
  }
  
  private boolean checkXAttrLog(final INode dataset, final INode inode,
      final String xAttrName,
      final XAttrMetadataLogEntry.Operation operation) throws IOException {
    return checkXAttrLog(dataset, inode, xAttrName, (byte)0, (short)1,
        operation);
  }
  
  private boolean checkXAttrLog(final INode dataset, final INode inode,
      final String xAttrName, byte namesapce, short numParts,
      final XAttrMetadataLogEntry.Operation operation) throws IOException {
    final long datasetId = dataset == null ? ANY_DATASET : dataset.getId();
    Collection<MetadataLogEntry> logEntries = getMetadataLogEntries(inode.getId());
    for (MetadataLogEntry logEntry : logEntries) {
      if (logEntry.getOperationId() == operation.getId()) {
        XAttrMetadataLogEntry xAttrLogEntry = (XAttrMetadataLogEntry)logEntry;
        return (datasetId == ANY_DATASET || datasetId == logEntry.getDatasetId())
            && (xAttrName.equals(ANY_NAME) || xAttrName.equals(xAttrLogEntry.getName()))
            && namesapce == xAttrLogEntry.getNamespace()
            && numParts == xAttrLogEntry.getNumParts()
            && inode.getPartitionId() == logEntry.getInodePartitionId()
            && inode.getParentId() == logEntry.getInodeParentId()
            && inode.getLocalName().equals(logEntry.getInodeName());
      }
    }
    return false;
  }
  
  private Collection<MetadataLogEntry> getMetadataLogEntries(final INode inode)
      throws IOException {
    return getMetadataLogEntries(inode.getId());
  }
  
  private Collection<MetadataLogEntry> getMetadataLogEntries(final long inodeId)
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
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), dataset),
          INodeMetadataLogEntry.Operation.Add));
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), subdir),
          INodeMetadataLogEntry.Operation.Add));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), file),
          INodeMetadataLogEntry.Operation.Add));
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
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      dfs.mkdirs(subdir);
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), dataset),
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), subdir),
          INodeMetadataLogEntry.Operation.Add));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), file),
          INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          subdir, file}, new int[]{1, 1, 1});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testAppend() throws Exception {
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
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      dfs.mkdirs(subdir);
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), dataset),
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), subdir),
          INodeMetadataLogEntry.Operation.Add));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      INode inode = TestUtil.getINode(cluster.getNameNode(), file);
      assertTrue(checkLog(inode, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          subdir, file}, new int[]{1, 1, 1});
      
      dfs.append(file).close();
      dfs.append(file).close();

      List<MetadataLogEntry> inodeLogEntries = new
          ArrayList<>(getMetadataLogEntries(inode));
      Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);

      assertTrue(inodeLogEntries.size() == 3);
      for(int i=0; i<3;i++){
        assertEquals(i+1, inodeLogEntries.get(i).getLogicalTime());
        assertTrue(((INodeMetadataLogEntry)inodeLogEntries.get(i)).getOperation() ==
            INodeMetadataLogEntry.Operation.Add);
      }
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          subdir, file}, new int[]{1, 1, 3});
      
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
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), dataset),
          INodeMetadataLogEntry.Operation.Add));
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), file),
          INodeMetadataLogEntry.Operation.Add));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
              file}, new int[]{1, 0});
      out.close();
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), file),
          INodeMetadataLogEntry.Operation.Add));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
              file}, new int[]{1, 1});
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
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folderId = TestUtil.getINode(cluster.getNameNode(), folder);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
      assertTrue(checkLog(datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
              folder, file}, new int[]{1,1,1});
      dfs.delete(folder, true);
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Delete));

      checkLogicalTimeDeleteAfterAdd(new INode[]{folderId, inodeId});
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset},
          new int[]{1});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testOldRename() throws Exception {
    testRename(true);
  }
  
  @Test
  public void testRename() throws Exception {
    testRename(false);
  }
  
  public void testRename(boolean oldRename) throws Exception {
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
      dfs.setMetaStatus(dataset0, MetaStatus.META_ENABLED);
      dfs.setMetaStatus(dataset1, MetaStatus.META_ENABLED);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
      INode file0Id = TestUtil.getINode(cluster.getNameNode(), file0);
      INode dataset0Id = TestUtil.getINode(cluster.getNameNode(), dataset0);
      INode dataset1Id = TestUtil.getINode(cluster.getNameNode(), dataset1);
  
      assertTrue(checkLog(dataset0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(dataset1Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(file0Id, INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset0,
              dataset1, file0}, new int[]{1, 1, 1});
      
      if(oldRename){
        assertTrue(dfs.rename(file0, file1));
      }else {
        dfs.rename(file0, file1, Options.Rename.NONE);
      }
      
      INode file1Id = TestUtil.getINode(cluster.getNameNode(), file1);
      
      assertEquals(file0Id.getId(), file1Id.getId());
      assertFalse(checkLog(dataset1Id, file0Id,
          INodeMetadataLogEntry.Operation.Rename));
      assertTrue(checkLog(dataset1Id, file1Id,
          INodeMetadataLogEntry.Operation.Rename));
      assertEquals(2, getMetadataLogEntries(file0Id).size());
  
      checkLogicalTimeAddRename(file0Id);
      checkLogicalTimeForINodes(cluster.getNameNode(),
          new Path[]{dataset0, dataset1, file1}, new int[]{1, 1, 2});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepOldRename() throws Exception {
    testDeepRename(true);
  }

  @Test
  public void testDeepRename() throws Exception{
    testDeepRename(false);
  }
  
  private void testDeepRename(boolean oldRename) throws Exception {
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

      dfs.setMetaStatus(dataset0, MetaStatus.META_ENABLED);
      dfs.setMetaStatus(dataset1, MetaStatus.META_ENABLED);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
  
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file0);
      INode folder0Id = TestUtil.getINode(cluster.getNameNode(), folder0);
      INode dataset0Id = TestUtil.getINode(cluster.getNameNode(), dataset0);
      INode dataset1Id = TestUtil.getINode(cluster.getNameNode(), dataset1);
  
      assertTrue(checkLog(dataset0Id, dataset0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(dataset0Id, folder0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(dataset0Id, inodeId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(dataset1Id, dataset1Id, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset0,
          dataset1, folder0, file0}, new int[]{1, 1, 1, 1});
      
      if(oldRename){
        dfs.rename(folder0, folder1);
      }else {
        dfs.rename(folder0, folder1, Options.Rename.NONE);
      }

      INode folder1Id = TestUtil.getINode(cluster.getNameNode(), folder1);
      assertEquals(folder0Id.getId(), folder1Id.getId());
      assertFalse(checkLog(dataset1Id, folder0Id,
          INodeMetadataLogEntry.Operation.Rename));
      assertTrue(checkLog(dataset1Id, folder1Id,
          INodeMetadataLogEntry.Operation.Rename));
      assertTrue(checkLog(dataset1Id, inodeId,
          INodeMetadataLogEntry.Operation.ChangeDataset));
  
      checkLogicalTimeAddRename(folder0Id);
      checkLogicalTimeAddRChangeDataset(inodeId);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset0,
          dataset1, folder1, new Path(folder1, file0.getName())},
          new int[]{1, 1, 2, 2});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnableLogForExistingDirectory() throws Exception {
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
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{projects,
          project, dataset, folder, file}, new int[]{0, 0, 0, 0, 0});
      
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folderId = TestUtil.getINode(cluster.getNameNode(), folder);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
  
      assertTrue(checkLog(datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{projects,
          project, dataset, folder, file}, new int[]{0, 0, 1, 1, 1});
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
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);

      FSDataOutputStream out = dfs.create(file);
      out.writeByte(0);
  
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folderId = TestUtil.getINode(cluster.getNameNode(), folder);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
  
      assertTrue(checkLog(datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Add));
      assertFalse(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{1, 1, 0});
      
      DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      try {
        dfs2.delete(file, false);
      }catch (Exception ex){
        fail("we shouldn't have any exception: " + ex.getMessage());
      }

      assertFalse(checkLog(inodeId,
          INodeMetadataLogEntry.Operation.Delete));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder},
          new int[]{1});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepOldRenameInTheSameDataset() throws Exception {
   testDeepRenameInTheSameDataset(true);
  }

  @Test
  public void testDeepRenameInTheSameDataset() throws Exception {
    testDeepRenameInTheSameDataset(false);
  }

  @Test
  public void testOldDeepRenameToNonMetaEnabledDir() throws Exception {
    testDeepRenameToNonMetaEnabledDir(true);
  }

  @Test
  public void testDeepRenameToNonMetaEnabledDir() throws Exception {
    testDeepRenameToNonMetaEnabledDir(false);
  }

  @Test
  public void testDeleteDatasetAfterOldRename() throws Exception {
    testDeleteDatasetAfterRename(true);
  }

  @Test
  public void testDeleteDatasetAfterRename() throws Exception {
    testDeleteDatasetAfterRename(false);
  }

  private void testDeepRenameInTheSameDataset(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder0 = new Path(dataset, "folder0");
      Path folder1 = new Path(folder0, "folder1");
      Path file = new Path(folder1, "file");

      Path newFolder = new Path(dataset, "newFolder");

      dfs.mkdirs(folder1, FsPermission.getDefault());

      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
  
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folder0Id = TestUtil.getINode(cluster.getNameNode(), folder0);
      INode folder1Id = TestUtil.getINode(cluster.getNameNode(), folder1);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
  
      assertTrue(checkLog(datasetId, datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder1Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder0, folder1, file}, new int[]{1, 1, 1, 1});
      
      if(oldRename){
        dfs.rename(folder0, newFolder);
      }else{
        dfs.rename(folder0, newFolder, Options.Rename.NONE);
      }

      INode newFolderId = TestUtil.getINode(cluster.getNameNode(), newFolder);
      
      assertEquals(folder0Id.getId(), newFolderId.getId());
      
      assertFalse(checkLog(datasetId, folder0Id,
          INodeMetadataLogEntry.Operation.Rename));
      assertTrue(checkLog(datasetId, newFolderId,
          INodeMetadataLogEntry.Operation.Rename));
      
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(folder1Id).size());
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(inodeId).size());

      checkLogicalTimeAddRename(folder0Id);
  
      Path newFolder1 = new Path(newFolder, folder1.getName());
      Path newFile = new Path(newFolder1, file.getName());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          newFolder, newFolder1, newFile}, new int[]{1, 2, 1, 1});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testDeepRenameToNonMetaEnabledDir(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder0 = new Path(dataset, "folder0");
      Path folder1 = new Path(folder0, "folder1");
      Path file = new Path(folder1, "file");

      Path newFolder = new Path(project, "newFolder");

      dfs.mkdirs(folder1, FsPermission.getDefault());

      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();

      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folder0Id = TestUtil.getINode(cluster.getNameNode(), folder0);
      INode folder1Id = TestUtil.getINode(cluster.getNameNode(), folder1);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
  
      assertTrue(checkLog(datasetId, datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder1Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder0, folder1, file}, new int[]{1, 1, 1, 1});
      
      if(oldRename){
        dfs.rename(folder0, newFolder);
      }else{
        dfs.rename(folder0, newFolder, Options.Rename.NONE);
      }
  
      INode newFolderId = TestUtil.getINode(cluster.getNameNode(), newFolder);
      assertTrue(checkLog(datasetId, folder0Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, folder1Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, inodeId,
          INodeMetadataLogEntry.Operation.Delete));
      assertFalse(checkLog(datasetId, newFolderId,
          INodeMetadataLogEntry.Operation.Delete));
      assertEquals("Subfolders and files shouldn't be logged for addition " +
          "during a move to a non MetaEnabled directoy", 2,
          getMetadataLogEntries(folder1Id).size());
      assertEquals("Subfolders and files shouldn't be logged for addition " +
              "during a move to a non MetaEnabled directoy", 2,
          getMetadataLogEntries(inodeId).size());

      //Check logical times
      checkLogicalTimeDeleteAfterAdd(new INode[]{folder0Id, folder1Id,
          inodeId});
  
      Path newFolder1 = new Path(newFolder, folder1.getName());
      Path newFile = new Path(newFolder1, file.getName());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          newFolder, newFolder1, newFile}, new int[]{1, 2, 2, 2});
      
      //Move the directory back to the dataset
      if(oldRename){
        dfs.rename(newFolder, folder0);
      }else{
        dfs.rename(newFolder, folder0, Options.Rename.NONE);
      }
  
      assertTrue(checkLog(datasetId, folder0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder1Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeAddDeleteAdd(folder0Id);
      checkLogicalTimeAddDeleteAdd(folder1Id);
      checkLogicalTimeAddDeleteAdd(inodeId);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder0, folder1, file}, new int[]{1, 3, 3, 3});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testDeleteDatasetAfterRename(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dataset = new Path("/dataset");
      Path folder1 = new Path(dataset, "folder1");
      Path folder2 = new Path(folder1, "folder2");
      Path folder3 = new Path(folder2, "folder3");

      dfs.mkdirs(folder3, FsPermission.getDefault());
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
  
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
      INode folder1Id = TestUtil.getINode(cluster.getNameNode(), folder1);
      INode folder2Id = TestUtil.getINode(cluster.getNameNode(), folder2);
      INode folder3Id = TestUtil.getINode(cluster.getNameNode(), folder3);
  
      assertTrue(checkLog(datasetId, datasetId,
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder1Id,
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder2Id,
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder3Id,
          INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(),
          new Path[]{dataset, folder1, folder2, folder3}, new int[]{1, 1, 1,
              1});
      
      Path file1 = new Path(folder3, "file1");
      TestFileCreation.create(dfs, file1, 1).close();

      Path file2 = new Path(folder3, "file2");
      TestFileCreation.create(dfs, file2, 1).close();
  
      INode file1Id = TestUtil.getINode(cluster.getNameNode(), file1);
      INode file2Id = TestUtil.getINode(cluster.getNameNode(), file2);

      assertTrue(checkLog(datasetId, file1Id,
          INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, file2Id,
          INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder1, folder2, folder3, file1, file2}, new int[]{1, 1, 1, 1, 1,
          1});
      
      Path newDataset = new Path("/newDataset");

      if(oldRename){
        dfs.rename(dataset, newDataset);
      }else{
        dfs.rename(dataset, newDataset, Options.Rename.NONE);
      }
  
      INode newDatasetId = TestUtil.getINode(cluster.getNameNode(), newDataset);
      assertEquals(newDatasetId.getId(), datasetId.getId());
  
      assertFalse(checkLog(datasetId, datasetId,
          INodeMetadataLogEntry.Operation.Rename));
      
      assertTrue(checkLog(datasetId, newDatasetId,
          INodeMetadataLogEntry.Operation.Rename));
      
      assertFalse(checkLog(datasetId, folder1Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertFalse(checkLog(datasetId, folder2Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertFalse(checkLog(datasetId, folder3Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertFalse(checkLog(datasetId, file1Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertFalse(checkLog(datasetId, file2Id,
          INodeMetadataLogEntry.Operation.Delete));
  
      Path newFolder1 = new Path(newDataset, folder1.getName());
      Path newFolder2 = new Path(newFolder1, folder2.getName());
      Path newFolder3 = new Path(newFolder2, folder3.getName());
      Path newFile1 = new Path(newFolder3, file1.getName());
      Path newFile2 = new Path(newFolder3, file2.getName());
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{newDataset,
          newFolder1, newFolder2, newFolder3, newFile1, newFile2},
          new int[]{2, 1, 1, 1, 1, 1});
      
      assertTrue(dfs.delete(newDataset, true));
  
      assertTrue(checkLog(datasetId, newDatasetId,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, folder1Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, folder2Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, folder3Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, file1Id,
          INodeMetadataLogEntry.Operation.Delete));
      assertTrue(checkLog(datasetId, file2Id,
          INodeMetadataLogEntry.Operation.Delete));

      checkLogicalTimeDeleteAfterAdd(new INode[]{folder1Id, folder2Id,
          folder3Id, file1Id, file2Id});
  
      checkLogicalTimeAddRenameDelete(datasetId);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  
  @Test
  public void testSettingAndUnsettingMetaEnabled() throws Exception{
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
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folderId = TestUtil.getINode(cluster.getNameNode(), folder);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
  
      assertTrue(checkLog(datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{1, 1, 1});

      dfs.setMetaStatus(dataset, MetaStatus.DISABLED);
      
      assertEquals(1, getMetadataLogEntries(inodeId).size());
      assertEquals(1, getMetadataLogEntries(folderId).size());
      assertEquals(1, getMetadataLogEntries(datasetId).size());
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{1, 1, 1});
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
  
      assertEquals(2, getMetadataLogEntries(inodeId).size());
      assertEquals(2, getMetadataLogEntries(folderId).size());
      assertEquals(2, getMetadataLogEntries(datasetId).size());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{2, 2, 2});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void checkLogicalTimeDeleteAfterAdd(INode[] inodes) throws
      IOException {
    for(INode inode : inodes){
      List<MetadataLogEntry> inodeLogEntries = new
          ArrayList<>(getMetadataLogEntries(inode));
      Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
      assertTrue(inodeLogEntries.size() == 2);
      assertTrue(inodeLogEntries.get(0).getOperationId() ==
          INodeMetadataLogEntry.Operation.Add.getId());
      assertTrue(inodeLogEntries.get(1).getOperationId() ==
          INodeMetadataLogEntry.Operation.Delete.getId());
    }
  }
  
  private void checkLogicalTimeAddDeleteAdd(final INode inode) throws IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 3);
    assertTrue(inodeLogEntries.get(0).getOperationId() ==
        INodeMetadataLogEntry.Operation.Add.getId());
    assertTrue(inodeLogEntries.get(1).getOperationId() ==
        INodeMetadataLogEntry.Operation.Delete.getId());
    assertTrue(inodeLogEntries.get(2).getOperationId() ==
        INodeMetadataLogEntry.Operation.Add.getId());
  }
  
  private void checkLogicalTimeAddRenameDelete(final INode inode) throws IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 3);
    assertTrue(inodeLogEntries.get(0).getOperationId() ==
        INodeMetadataLogEntry.Operation.Add.getId());
    assertTrue(inodeLogEntries.get(1).getOperationId() ==
        INodeMetadataLogEntry.Operation.Rename.getId());
    assertTrue(inodeLogEntries.get(2).getOperationId() ==
        INodeMetadataLogEntry.Operation.Delete.getId());
  }
  
  private void checkLogicalTimeAddRename(final INode inode) throws IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 2);
    assertTrue(inodeLogEntries.get(0).getOperationId() ==
        INodeMetadataLogEntry.Operation.Add.getId());
    assertTrue(inodeLogEntries.get(1).getOperationId() ==
        INodeMetadataLogEntry.Operation.Rename.getId());
  }
  
  private void checkLogicalTimeAddRChangeDataset(final INode inode) throws
      IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 2);
    assertTrue(inodeLogEntries.get(0).getOperationId() ==
        INodeMetadataLogEntry.Operation.Add.getId());
    assertTrue(inodeLogEntries.get(1).getOperationId() ==
        INodeMetadataLogEntry.Operation.ChangeDataset.getId());
  }
  
  private void checkLogicalTimeForINodes(NameNode nameNode, Path[] inodesPaths,
      int[] logicalTimes) throws IOException{
    int i=0;
    for(Path path : inodesPaths){
      assertEquals(logicalTimes[i], TestUtil.getINode(nameNode, path)
          .getLogicalTime());
      i++;
    }
  }
  
  @Test
  public void testXAttrNonLogginFolder() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    
    final String name1 = "user.metadata";
    final byte[] value1 = "this file metadata".getBytes(Charsets.UTF_8);
    
    try{
      
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      Path file = new Path(dir, "file");
      DFSTestUtil.createFile(dfs, file, 0, (short)1, 0);
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dir);
  
      assertTrue(getMetadataLogEntries(inodeId).isEmpty());
      
      dfs.setXAttr(file, name1, value1);
  
      assertTrue(getMetadataLogEntries(inodeId).isEmpty());
  
      dfs.setMetaStatus(dir, MetaStatus.META_ENABLED);
  
      assertEquals(2, getMetadataLogEntries(inodeId).size());
  
      assertTrue(checkXAttrLogAddAll(datasetId, inodeId));
      
      assertTrue(checkLog(datasetId, inodeId,
          INodeMetadataLogEntry.Operation.Add));
  
      dfs.delete(file, true);
  
      assertTrue(checkLog(datasetId, inodeId,
          INodeMetadataLogEntry.Operation.Delete));
  
      checkIfNoXAttrsForINode(inodeId);
      
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
  
  
  @Test
  public void testXAttrOnMetaEnabledDir() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    
    final String name1 = "user.metadata";
    final byte[] value1 = "this file metadata".getBytes(Charsets.UTF_8);
  
    final String name2 = "user.path";
    final byte[] value2 = "/this/is/my/test/path/".getBytes(Charsets.UTF_8);
    final byte[] value3 = "/replace/the/old/path".getBytes(Charsets.UTF_8);
  
    try{
      
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      dfs.setMetaStatus(dir, MetaStatus.META_ENABLED);
      
      Path file = new Path(dir, "file");
      DFSTestUtil.createFile(dfs, file, 0, (short)1, 0);
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dir);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{1, 1});
      
      //set xatrr name1=value1
      dfs.setXAttr(file, name1, value1);
  
      assertTrue(Arrays.equals(value1, dfs.getXAttr(file, name1)));
      
      assertTrue(checkXAttrLog(datasetId, inodeId,
          XAttrHelper.buildXAttr(name1).getName(),
          XAttrMetadataLogEntry.Operation.Add));
      
      assertEquals(2, getMetadataLogEntries(inodeId).size());
  
      //set xatrr name2=value2
      dfs.setXAttr(file, name2, value2);
  
      assertTrue(Arrays.equals(value2, dfs.getXAttr(file, name2)));
  
      assertTrue(checkXAttrLog(datasetId, inodeId,
          XAttrHelper.buildXAttr(name2).getName(),
          XAttrMetadataLogEntry.Operation.Add));
  
      assertEquals(3, getMetadataLogEntries(inodeId).size());
      
      assertEquals(3, TestUtil.getINode(cluster
      .getNameNode(), file).getLogicalTime());
      
      //remove xatrr name1
      dfs.removeXAttr(file, name1);
  
      checkXAttrLogicalTimeAddDelete(inodeId,
          XAttrHelper.buildXAttr(name1).getName());
  
      assertEquals(4, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
      //replace xattr name2=value3
      dfs.setXAttr(file, name2, value3);
  
      assertTrue(Arrays.equals(value3, dfs.getXAttr(file, name2)));
      
      assertEquals(5, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
      //remove xattr name2
      dfs.removeXAttr(file, name2);
  
      checkXAttrLogicalTimeAddUpdateDelete(inodeId,
          XAttrHelper.buildXAttr(name2).getName());
      assertEquals(6, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
      //set xattr on dir
      dfs.setXAttr(dir, name1, value1);
      assertTrue(Arrays.equals(value1, dfs.getXAttr(dir, name1)));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{6, 2});
  
      //replace xattr on dir
      dfs.setXAttr(dir, name1, value2);
      assertTrue(Arrays.equals(value2, dfs.getXAttr(dir, name1)));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{6, 3});
      
      //set xattr on dir name2=value3
      dfs.setXAttr(dir, name2, value3);
      assertTrue(Arrays.equals(value3, dfs.getXAttr(dir, name2)));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{6, 4});
      
      //remove xattr on dir
      dfs.removeXAttr(dir, name1);
  
      checkXAttrLogicalTimeAddUpdateDelete(datasetId,
          XAttrHelper.buildXAttr(name1).getName());
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{6, 5});
      
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
  
  private void checkXAttrLogicalTimeAddDelete(final INode inode,
      final String name) throws IOException {
    List<MetadataLogEntry> allEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
  
    Collection<MetadataLogEntry> filtered =
        Collections2.filter(allEntries,
        new Predicate<MetadataLogEntry>() {
      @Override
      public boolean apply(
          @Nullable
              MetadataLogEntry logEntry) {
        if(logEntry instanceof XAttrMetadataLogEntry){
          return ((XAttrMetadataLogEntry)logEntry).getName().equals(name);
        }
        return false;
      }
    });
    
    List<MetadataLogEntry> xAttrLogEntries = new ArrayList<>(filtered);
    
    Collections.sort(xAttrLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(xAttrLogEntries.size() == 2);
    assertTrue(xAttrLogEntries.get(0).getOperationId() ==
        XAttrMetadataLogEntry.Operation.Add.getId());
    assertTrue(xAttrLogEntries.get(1).getOperationId() ==
        XAttrMetadataLogEntry.Operation.Delete.getId());
  }
  
  private void checkXAttrLogicalTimeAddUpdateDelete(final INode inode,
      final String name) throws IOException {
    List<MetadataLogEntry> allEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Collection<MetadataLogEntry> filtered =
        Collections2.filter(allEntries,
            new Predicate<MetadataLogEntry>() {
              @Override
              public boolean apply(
                  @Nullable
                      MetadataLogEntry logEntry) {
                if(logEntry instanceof XAttrMetadataLogEntry){
                  return ((XAttrMetadataLogEntry)logEntry).getName().equals(name);
                }
                return false;
              }
            });
    
    List<MetadataLogEntry> xAttrLogEntries = new ArrayList<>(filtered);
    
    Collections.sort(xAttrLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(xAttrLogEntries.size() == 3);
    assertTrue(xAttrLogEntries.get(0).getOperationId() ==
        XAttrMetadataLogEntry.Operation.Add.getId());
    assertTrue(xAttrLogEntries.get(1).getOperationId() ==
        XAttrMetadataLogEntry.Operation.Update.getId());
    assertTrue(xAttrLogEntries.get(2).getOperationId() ==
        XAttrMetadataLogEntry.Operation.Delete.getId());
  }
  
  private boolean checkIfNoXAttrsForINode(final INode inode)
      throws IOException {
    return (Boolean) new LightWeightRequestHandler(HDFSOperationType.TEST){
  
      @Override
      public Object performTask() throws IOException {
        XAttrDataAccess da = (XAttrDataAccess)
            HdfsStorageFactory.getDataAccess(XAttrDataAccess.class);
        return da.getXAttrsByInodeId(inode.getId()) == null;
      }
    }.handle();
  }
  
  @Test
  public void testRenameDataset() throws Exception{
    testRenameDataset(false);
  }
  
  @Test
  public void testOldRenameDataset() throws Exception{
    testRenameDataset(true);
  }
  
  private void testRenameDataset(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder0 = new Path(dataset, "folder0");
      Path folder1 = new Path(folder0, "folder1");
      Path file = new Path(folder1, "file");
      
      Path newDataset = new Path(project, "newDataset");
      
      dfs.mkdirs(folder1, FsPermission.getDefault());
      
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
  
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folder0Id = TestUtil.getINode(cluster.getNameNode(), folder0);
      INode folder1Id = TestUtil.getINode(cluster.getNameNode(), folder1);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
      
      assertTrue(checkLog(datasetId, folder0Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, folder1Id, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{projects,
          project, dataset, folder0, folder1, file}, new int[]{0, 0, 1, 1, 1,
          1});
      
      if(oldRename){
        dfs.rename(dataset, newDataset);
      }else{
        dfs.rename(dataset, newDataset, Options.Rename.NONE);
      }
      
      INode newDatasetId = TestUtil.getINode(cluster.getNameNode(),
      newDataset);
      
      assertEquals(datasetId.getId(), newDatasetId.getId());
  
      assertFalse(checkLog(datasetId, datasetId,
          INodeMetadataLogEntry.Operation.Rename));
      assertTrue(checkLog(datasetId, newDatasetId,
          INodeMetadataLogEntry.Operation.Rename));
      
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(folder1Id).size());
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(folder0Id).size());
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(inodeId).size());
  
      Path newFolder0 = new Path(newDataset, folder0.getName());
      Path newFolder1 = new Path(newFolder0, folder1.getName());
      Path newFile = new Path(newFolder1, file.getName());
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{newDataset,
          newFolder0, newFolder1, newFile}, new int[]{2, 1, 1, 1});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testSetMetaEnabledOnNonExistingDirectory() throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path root = new Path("/");
      Path dir = new Path(root,"dir");
      
      try {
        dfs.setMetaStatus(dir, MetaStatus.META_ENABLED);
        fail("should fail to set metaEnabled on non existing directory");
      }catch (FileNotFoundException ex){
      }
      
      dfs.mkdirs(dir, FsPermission.getDefault());
  
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), dir),
          INodeMetadataLogEntry.Operation.Add));
      
      dfs.setMetaStatus(dir, MetaStatus.META_ENABLED);
  
      assertTrue(checkLog(TestUtil.getINode(cluster.getNameNode(), dir),
          INodeMetadataLogEntry.Operation.Add));
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testSetMetaEnabledOnRoot() throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path root = new Path("/");
      Path dir = new Path(root,"dir");
      dfs.mkdirs(dir, FsPermission.getDefault());
      
      try {
        dfs.setMetaStatus(root, MetaStatus.META_ENABLED);
        fail("should fail to set metaEnabled on the root since subtree locks " +
            "are disabled for the root");
      }catch (FileNotFoundException ex){
      }
  
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), dir),
          INodeMetadataLogEntry.Operation.Add));
      assertFalse(checkLog(TestUtil.getINode(cluster.getNameNode(), root),
          INodeMetadataLogEntry.Operation.Add));
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testLargeXAttrMetaEnabled_27KB() throws Exception {
    testLargeXAttrMetaEnabled(2, 5);
  }
  
  @Test
  public void testLargeXAttrMetaEnabled_135KB() throws Exception {
    testLargeXAttrMetaEnabled(10, 5);
  }
  
  @Test
  public void testLargeXAttrMetaEnabled_1_3MB() throws Exception {
    testLargeXAttrMetaEnabled(100, 5);
  }
  
  @Test
  public void testLargeXAttrMetaEnabled_3_44MB() throws Exception {
    testLargeXAttrMetaEnabled(255, 5);
  }
  
  private void testLargeXAttrMetaEnabled(int rows, int numXAttrs) throws Exception {
    int MAX_VALUE_SIZE = rows * XAttrStorage.getDefaultXAttrValueSize();
    int MAX_SIZE = XAttrStorage.getMaxXAttrNameSize() + MAX_VALUE_SIZE;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, numXAttrs);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, MAX_SIZE);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try{
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      dfs.setMetaStatus(dir, MetaStatus.META_ENABLED);
  
      Path file = new Path(dir, "file");
      DFSTestUtil.createFile(dfs, file, 0, (short)1, 0);
      
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dir);
  
      assertTrue(checkLog(datasetId, datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(datasetId, inodeId, INodeMetadataLogEntry.Operation.Add));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file, dir},
          new int[]{1, 1});
      
      final String nameBase = "user.test";
      
      Map<String, byte[]> testXAttrs = new HashMap<>();
      
      // setXAttrs
      for(int i=0; i < numXAttrs; i++){
        String name = nameBase+ i;
        byte[] value =
            XAttrTestHelpers.generateRandomByteArrayWithRandomSize(MAX_VALUE_SIZE);
        testXAttrs.put(name, value);
        dfs.setXAttr(file, name, value);
        byte[] returnedValue = dfs.getXAttr(file, name);
        assertArrayEquals(value, returnedValue);
      }
      
      // check metadata log entries
      checkAllXAttrLogs(datasetId, inodeId, testXAttrs,
          XAttrMetadataLogEntry.Operation.Add);
      
      assertEquals(numXAttrs + 1, getMetadataLogEntries(inodeId).size());
      assertEquals(numXAttrs + 1, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
      //get all xattrs attached to file
      Map<String, byte[]> returnedXAttrs =dfs.getXAttrs(file);
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
      
      // get xattrs by their name
      returnedXAttrs = dfs.getXAttrs(file,
          new ArrayList<String>(testXAttrs.keySet()));
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
      
      // replace all xattrs
      for(int i=0; i < numXAttrs; i++){
        String name = nameBase+ i;
        byte[] value =
            XAttrTestHelpers.generateRandomByteArrayWithRandomSize(MAX_VALUE_SIZE);
        testXAttrs.put(name, value);
        dfs.setXAttr(file, name, value, EnumSet.of(XAttrSetFlag.REPLACE));
        byte[] returnedValue = dfs.getXAttr(file, name);
        assertArrayEquals(value, returnedValue);
      }
  
      // check metadata log entries
      checkAllXAttrLogs(datasetId, inodeId, testXAttrs,
          XAttrMetadataLogEntry.Operation.Update);
  
      assertEquals( (2 * numXAttrs) + 1, getMetadataLogEntries(inodeId).size());
      assertEquals((2 * numXAttrs) + 1, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
      //get all xattrs attached to file
      returnedXAttrs =dfs.getXAttrs(file);
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
      
      // remove xattrs one by one
      for(int i=0; i< numXAttrs; i++){
        String name = nameBase + i;
        dfs.removeXAttr(file, name);
        returnedXAttrs = dfs.getXAttrs(file);
        assertEquals(numXAttrs - (i+1), returnedXAttrs.size());
        for(Map.Entry<String, byte[]> entry : returnedXAttrs.entrySet()){
          assertArrayEquals(entry.getValue(), testXAttrs.get(entry.getKey()));
        }
      }
      
      //check metadata logs
      for(int i=0; i< numXAttrs; i++){
        XAttr xAttr = XAttrHelper.buildXAttr(nameBase + i);
        checkXAttrLogicalTimeAddUpdateDelete(inodeId, xAttr.getName());
      }
      
      assertEquals( (3 * numXAttrs) + 1, getMetadataLogEntries(inodeId).size());
      assertEquals((3 * numXAttrs) + 1, TestUtil.getINode(cluster
          .getNameNode(), file).getLogicalTime());
      
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
  
  
  private void checkAllXAttrLogs(final INode dataset, final INode inode,
      final Map<String, byte[]> xAttrs, final XAttrMetadataLogEntry.Operation operation) throws IOException {
    List<MetadataLogEntry> allEntries = new
        ArrayList<>(getMetadataLogEntries(inode));
    
    Map<String, XAttrMetadataLogEntry> logEntriesByName = new HashMap<>();
    for(MetadataLogEntry logEntry : allEntries){
      if(logEntry instanceof XAttrMetadataLogEntry){
        XAttrMetadataLogEntry xAttrMetadataLogEntry =
            (XAttrMetadataLogEntry) logEntry;
        if(xAttrMetadataLogEntry.getOperation() == operation){
          logEntriesByName.put(xAttrMetadataLogEntry.getName(),
              xAttrMetadataLogEntry);
        }
      }
    }
    
    int totalNumOfParts = 0;
    assertEquals(xAttrs.size(), logEntriesByName.size());
    for(Map.Entry<String, byte[]> xattrEntry : xAttrs.entrySet()){
      XAttr xAttr = XAttrHelper.buildXAttr(xattrEntry.getKey());
      XAttrMetadataLogEntry logEntry = logEntriesByName.get(xAttr.getName());
      assertNotNull(logEntry);
      
      assertEquals(dataset.getId(), logEntry.getDatasetId());
      assertEquals(inode.getId(), logEntry.getInodeId());
      assertEquals(inode.getLocalName(), logEntry.getInodeName());
      assertEquals(inode.getParentId(), logEntry.getInodeParentId());
      assertTrue(inode.getPartitionId() == logEntry.getInodePartitionId());
      assertEquals(xAttr.getName(), logEntry.getName());
      assertEquals(xAttr.getNameSpace().getId(), logEntry.getNamespace());
      assertEquals(StoredXAttr.getNumParts(xattrEntry.getValue()),
          logEntry.getNumParts());
      totalNumOfParts += logEntry.getNumParts();
    }
    
    assertEquals(totalNumOfParts, XAttrTestHelpers.getXAttrTableRowCount());
  }
  
  @Test
  public void testSettingAndUnsettingMetaEnabledOnDirWithXAttrs() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, 100);
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
      TestFileCreation.create(dfs, file, 1).close();
      
      String name1 = "user.test1";
      byte[] value1 = "this is my test value".getBytes();
  
      String name2 = "user.test2";
      byte[] value2 = "this is my test value2".getBytes();
      
      dfs.setXAttr(dataset, name1, value1);
      dfs.setXAttr(dataset, name2, value2);
  
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
      INode inodeId = TestUtil.getINode(cluster.getNameNode(), file);
      INode folderId = TestUtil.getINode(cluster.getNameNode(), folder);
      INode datasetId = TestUtil.getINode(cluster.getNameNode(), dataset);
      
      assertTrue(checkLog(datasetId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(folderId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkLog(inodeId, INodeMetadataLogEntry.Operation.Add));
      assertTrue(checkXAttrLogAddAll(datasetId, datasetId));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{2, 1, 1});
      
      dfs.setMetaStatus(dataset, MetaStatus.DISABLED);
      
      assertEquals(1, getMetadataLogEntries(inodeId).size());
      assertEquals(1, getMetadataLogEntries(folderId).size());
      assertEquals(2, getMetadataLogEntries(datasetId).size());
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{2, 1, 1});
  
      dfs.setXAttr(folder, name1, value1);
  
      dfs.setMetaStatus(dataset, MetaStatus.META_ENABLED);
  
      assertTrue(checkXAttrLogAddAll(datasetId, folderId));
      
      assertEquals(2, getMetadataLogEntries(inodeId).size());
      assertEquals(3, getMetadataLogEntries(folderId).size());
      assertEquals(4, getMetadataLogEntries(datasetId).size());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{dataset,
          folder, file}, new int[]{4, 3, 2});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
