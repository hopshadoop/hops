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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Unit tests covering FSPermissionChecker.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
public class TestFSPermissionChecker {
  private static final long PREFERRED_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final short REPLICATION = 3;
  private static final String SUPERGROUP = "supergroup";
  private static final String SUPERUSER = "superuser";
  private static final UserGroupInformation BRUCE =
      UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
      UserGroupInformation.createUserForTesting("diana", new String[] { "sales" });
  private static final UserGroupInformation CLARK =
      UserGroupInformation.createUserForTesting("clark", new String[] { "execs" });
     
  private Configuration conf;
  private MiniDFSCluster cluster;
  
  @Before
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    SimulatedFSDataset.setFactory(conf);
    //assume supergroup & superuser match
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }
  
  @After
  public void tearDown() throws IOException {
    cluster.getFileSystem().close();
    cluster.shutdown();
  }
  
  @Test
  public void testAclOwner() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile( file1, "bruce", "execs", (short)0640);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ);
    assertPermissionGranted(BRUCE, "/file1", WRITE);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionDenied(BRUCE, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
  }
  
  @Test
  public void testAclNamedUser() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0640);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, "diana", READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }
  
  @Test
  public void testAclNamedUserDeny() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1,"bruce", "execs", (short)0644);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, "diana", NONE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
  }
  
  @Test
  public void testAclNamedUserTraverseDeny() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce","execs", (short)0755);
    Path file1 = new Path(dir1,"file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "diana", NONE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, MASK, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }
  
  @Test
  public void testAclNamedUserMask() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0620);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, "diana", READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, WRITE),
        aclEntry(ACCESS, OTHER, NONE));
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }
  
  @Test
  public void testAclGroup() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0640);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }
  
  @Test
  public void testAclGroupDeny() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "sales", (short)0604);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, NONE),
        aclEntry(ACCESS, MASK, NONE),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }
  
  @Test
  public void testAclGroupTraverseDeny() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce","execs", (short)0755);
    Path file1 = new Path(dir1,"file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, NONE),
        aclEntry(ACCESS, MASK, NONE),
        aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }
  
  @Test
  public void testAclGroupTraverseDenyOnlyDefaultEntries() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce","execs", (short)0755);
    Path file1 = new Path(dir1,"file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, NONE),
        aclEntry(ACCESS, OTHER, READ_EXECUTE),
        //aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, "sales", NONE),
        aclEntry(DEFAULT, GROUP, NONE));
        //aclEntry(DEFAULT, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }
  
  @Test
  public void testAclGroupMask() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_WRITE),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }
  
  @Test
  public void testAclNamedGroup() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0640);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, GROUP, "sales", READ),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }
  
  @Test
  public void testAclNamedGroupDeny() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "sales", (short)0644);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, GROUP, "execs", NONE),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }
  
  @Test
  public void testAclNamedGroupTraverseDeny() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce","execs", (short)0755);
    Path file1 = new Path(dir1, "file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", NONE),
        aclEntry(ACCESS, MASK, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }
  
  @Test
  public void testAclNamedGroupMask() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "execs", (short)0644);
    addAcl(file1,
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, GROUP, "sales", READ_WRITE),
        aclEntry(ACCESS, MASK, READ),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }
  
  @Test
  public void testAclOther() throws IOException {
    Path file1 = new Path("/file1");
    createINodeFile(file1, "bruce", "sales", (short)0774);
    addAcl(file1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "diana", ALL),
        aclEntry(ACCESS, GROUP, READ_WRITE),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", ALL);
    assertPermissionGranted(DIANA, "/file1", ALL);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }
  
  @Test
  public void testInheritDefault() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce", "sales", (short)0770);
    Path file1 = new Path(dir1, "file1");
    createINodeFile(file1, "bruce", "sales", (short)0770);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "clark", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "execs", ALL),
        aclEntry(ACCESS, GROUP, ALL),
        aclEntry(ACCESS, OTHER, ALL),
        aclEntry(DEFAULT, GROUP, "execs", READ),
        aclEntry(DEFAULT, USER, "diana", NONE));
    
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
  }
  
  @Test
  public void testInheritDefaultDeep() throws IOException {
    Path dir1 = new Path("/dir1");
    createINodeDirectory(dir1, "bruce", "sales", (short)0770);
    Path dir2 = new Path(dir1, "dir2");
    createINodeDirectory(dir2, "bruce", "sales", (short)0770);
    Path file1 = new Path(dir2, "file1");
    createINodeFile(file1, "bruce", "sales", (short)0770);
    addAcl(dir1,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "clark", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "execs", ALL),
        aclEntry(ACCESS, GROUP, ALL),
        aclEntry(ACCESS, OTHER, ALL),
        aclEntry(DEFAULT, GROUP, "execs", READ),
        aclEntry(DEFAULT, USER, "diana", NONE));
    
    addAcl(dir2,
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "clark", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "execs", ALL),
        aclEntry(ACCESS, GROUP, ALL),
        aclEntry(ACCESS, OTHER, ALL),
        aclEntry(DEFAULT, GROUP, "execs", NONE),
        aclEntry(DEFAULT, USER, "diana", READ));
    
    assertPermissionDenied(CLARK, "/dir1/dir2/file1", READ);
    assertPermissionGranted(DIANA, "/dir1/dir2/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/dir2/file1", WRITE);
  }
  
  @Test
  public void testInheritedBlockedByIntermediateDefault() throws IOException {
  
  }
  
  private void addAcl(final Path src, final AclEntry... acl)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_ACL){
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE, TransactionLockTypes.INodeResolveType.PATH, src.toString())
                .setNameNodeID(cluster.getNameNode().getId()).setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getAcesLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        INode inode = cluster.getNamesystem().getINode(src.toString());
        AclStorage.updateINodeAcl(inode, Arrays.asList(acl));
        return null;
      }
    }.handle();
  }
  
  private void assertPermissionGranted(final UserGroupInformation user, final String path,
      final FsAction access) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_ACCESS){
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ, TransactionLockTypes.INodeResolveType.PATH, path)
                .setNameNodeID(cluster.getNameNode().getId()).setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getAcesLock());
      }
      
      @Override
      public Object performTask() throws IOException {
        FSDirectory dir = cluster.getNamesystem().getFSDirectory();
        INodesInPath iip = dir.getINodesInPath(path, true);
        new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(iip,
            false, null, null, access, null, false);
        return null;
      }
    }.handle();
  }
  
  private void assertPermissionDenied(final UserGroupInformation user, final String path,
      final FsAction access) throws IOException {
    try {
      new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_ACCESS){
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ, TransactionLockTypes.INodeResolveType.PATH, path)
                  .setNameNodeID(cluster.getNameNode().getId()).setActiveNameNodes(cluster.getNameNode().getActiveNameNodes().getActiveNodes());
          locks.add(il);
          locks.add(lf.getAcesLock());
        }
        
        @Override
        public Object performTask() throws IOException {
          FSDirectory dir = cluster.getNamesystem().getFSDirectory();
          INodesInPath iip = dir.getINodesInPath(path, true);
          new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(iip,
              false, null, null, access, null, false);
          return null;
        }
      }.handle();
      fail("expected AccessControlException for user + " + user + ", path = " +
          path + ", access = " + access);
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
              e.getMessage().contains(user.getUserName().toString()));
    }
  }
  
  private void createINodeDirectory(
      Path src, String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
        FsPermission.createImmutable(perm));
    cluster.getNamesystem().mkdirs(src.toString(), permStatus, false);
    cluster.getNamesystem().setOwner(src.toString(), owner, group);
    cluster.getNamesystem().setPermission(src.toString(), FsPermission.createImmutable(perm));
  }
  
  private void createINodeFile(Path src,
      String owner, String group, short perm) throws IOException {
    DFSTestUtil.createFile(cluster.getFileSystem(), src, 0,(short)1, 0L);
    cluster.getNamesystem().setOwner(src.toString(), owner, group);
    cluster.getNamesystem().setPermission(src.toString(), FsPermission.createImmutable(perm));
  }
}