/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.tools.HDFSConcat;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;

/**
 * This class makes sure that acl checking is properly done on different namespace operations.
 * HDFS Tests assume that acls are available in memory.
 */
public class TestAclLocksAcquired extends junit.framework.TestCase{
  
  
  private MiniDFSCluster cluster;
  private static final Configuration conf;
  private DistributedFileSystem dfs;
  private static String dirOwner = "superuser";
  private static String dirGroup = "supergroup";
  
  
  static {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
  }
  
  private UserGroupInformation robin;
  private UserGroupInformation superuser;
  private UserGroupInformation dir1admin;
  
  @Before
  public void setUp() throws IOException, InterruptedException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);
  
    superuser = UserGroupInformation.createUserForTesting(dirOwner, new String[]{dirGroup});
    robin = UserGroupInformation.createUserForTesting("robin", new String[]{"robin", "dir1"});
    dir1admin = UserGroupInformation.createUserForTesting("dir1", new String[]{"dir1", "supergroup"});
   
    Path dir1 = new Path("/dir1");
    cluster.getFileSystem().mkdir(dir1, new FsPermission(ALL, ALL, NONE));
    cluster.getFileSystem().setOwner(dir1, "dir1", "dir1" );
    
    List<AclEntry> acl = new ArrayList<>();
    acl.add(new AclEntry.Builder().setScope(AclEntryScope.DEFAULT).setType(AclEntryType.USER).setName("robin")
        .setPermission(NONE).build());
    setAclAs("/dir1", superuser, acl);
   
    
    mkdirAs("/dir1/dir2", dir1admin, new FsPermission(ALL, ALL, NONE));
    
  }
  
  private void mkdirAs(final String path, UserGroupInformation ugi, final FsPermission permission)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        cluster.getFileSystem().mkdir(new Path(path), permission);
        cluster.getFileSystem().close();
        return null;
      }
    });
  }
  
  private void setAclAs(final String path, UserGroupInformation ugi, final List<AclEntry> acl)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        cluster.getFileSystem().setAcl(new Path(path), acl);
        cluster.getFileSystem().close();
        return null;
      }
    });
  }
  
  private void createFileAs(final String path, UserGroupInformation ugi) throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        FSDataOutputStream fsDataOutputStream = cluster.getFileSystem().create(new Path(path), false);
        fsDataOutputStream.writeInt(0);
        fsDataOutputStream.close();
        return null;
      }
    });
  }
  
  private FileStatus getFileInfoAs(final String path, UserGroupInformation ugi) throws IOException,
      InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<FileStatus>() {
      @Override
      public FileStatus run() throws Exception {
        return cluster.getFileSystem().getFileStatus(new Path(path));
      }
    });
  }
  
  @After
  public void tearDown() throws IOException {
    if (dfs != null) {
      dfs.close();
    }
    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }
  
  @Test
  public void testCreateFile() throws IOException, InterruptedException {
    try{
      createFileAs("/dir1/dir2/file1", robin);
      fail("Should throw exception");
    } catch (IOException e){
      if (!(e instanceof AccessControlException)){
        fail("Should throw access control exception.");
      }
    }
  }
  
  @Test
  public void testGetFileInfo() throws IOException, InterruptedException {
    createFileAs("/dir1/dir2/file1", dir1admin);
    try{
      FileStatus status = getFileInfoAs("/dir1/dir2/file1", robin);
      fail("Should throw exception.");
    } catch (IOException e){
      if (!(e instanceof AccessControlException)){
        fail("Should throw access control exception.");
      }
    }
    
  }
  
}
