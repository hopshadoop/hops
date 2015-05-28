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
import io.hops.metadata.hdfs.dal.AccessTimeLogDataAccess;
import io.hops.metadata.hdfs.entity.AccessTimeLogEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class TestAccessTimeLog extends TestCase {

  private int getLogEntryCount(final int inodeId) throws IOException {
    return (Integer) new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        AccessTimeLogDataAccess<AccessTimeLogEntry> da =
            (AccessTimeLogDataAccess) HdfsStorageFactory.getDataAccess(
                AccessTimeLogDataAccess.class);
        Collection<AccessTimeLogEntry> logEntries = da.find(inodeId);
        return logEntries.size();
      }
    }.handle();
  }

  @Test
  public void testAccessTime() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);
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
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      FSDataInputStream in = dfs.open(file);
      in.close();
      int fileId = TestUtil.getINodeId(cluster.getNameNode(), file);
      assertEquals(2, getLogEntryCount(fileId));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNonLoggingDir() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);
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
      dfs.mkdirs(subdir);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      FSDataInputStream in = dfs.open(file);
      in.close();
      int fileId = TestUtil.getINodeId(cluster.getNameNode(), file);
      assertEquals(0, getLogEntryCount(fileId));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
