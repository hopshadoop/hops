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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

public class TestSubtreeLockACL extends TestCase{
  
  private static Log log = LogFactory.getLog(TestSubtreeLockACL.class);
 
  Configuration conf = new HdfsConfiguration();
  MiniDFSCluster cluster;

  Path subtrees = new Path("/subtrees");
  Path subtree1 = new Path(subtrees, "subtree1");
  Path level1folder1 = new Path(subtree1, "level1folder1");
  Path level1folder2 = new Path(subtree1, "level1folder2");
  Path level2folder1 = new Path(level1folder1, "level2folder1");
  Path level2file1 = new Path(level1folder1, "level2file1");
  Path level3file1 = new Path(level2folder1, "level2file1");
  
  Path subtree2 = new Path(subtrees, "subtree2");
  
  String sharedGroup = "sharedgroup";
  UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[]{"user1",
      sharedGroup});
  UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", new String[]{"user2",
      sharedGroup});
  
  
  {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
  }
  
  public void setup() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).build();
    cluster.waitActive();
    createFileTree();
  }
  
  public void teardown(){
    if(cluster!=null)
    cluster.shutdown();
  }



  //This test automatically tests destination ancestor since the implementation treats an existing parent as ancestor.
  @Test
  public void testRenameBlockedByDestinationParentAccessAcl() throws IOException, InterruptedException {
    try {
      setup();

      setReadOnlyUserAccessAcl(user2.getShortUserName(), subtree2);

      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });

      try {
        user2fs.rename(level1folder1, new Path(subtree2, "newname"));
        fail("Owner permission should block rename");
      } catch (AccessControlException expected) {
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("inode=\"/subtrees/subtree2\""));
        //Operation should fail.
      }
    } finally {
      teardown();
    }
  }

  @Test
  public void testSubtreeMoveBlockedSourceParentAccessAcl() throws IOException, InterruptedException {
   
    try {
      setup();
      
      //Make src readonly via access acl
      setReadOnlyUserAccessAcl(user2.getShortUserName(), subtree1);
     
      //Try to move subtree1 under subtree2. Should fail because of access acl.
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });
  
      try {
        user2fs.rename(level1folder1, new Path(subtree2, "newname"));
        fail("Acl should block move");
      } catch (AccessControlException expected){
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("inode=\"/subtrees/subtree1\""));
        //Operation should fail.
      }
  
    } finally {
      teardown();
    }
  }
  
  @Test
  public void testSubtreeMoveBlockedByInheritedDefaultAcl() throws IOException, InterruptedException {
    try {
      setup();
      
      //Deny access via default acl down subtree1
      setReadOnlyUserDefaultAcl(user2.getShortUserName(), subtree1);
      
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });
  
      try {
        //Try to move level2folder1 under subtree2. Should fail because of inherited acl in level1folder1.
        user2fs.rename(level2folder1, new Path(subtree2, "newname"));
        fail("Acl should block move");
      } catch (AccessControlException expected){
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("inode=\"/subtrees/subtree1/level1folder1\""));
        //Operation should fail.
      }
  
    } finally {
      teardown();
    }
  }
@Test
  public void testSubtreeMoveNotBlockedByDeepAcl() throws IOException, InterruptedException {
    try {
      setup();

      //Deny access via default acl down level1folder1
      setDenyUserAccessAcl(user2.getShortUserName(), level2folder1);

      //Try to delete subtree1. Should fail because of access acl down the tree.
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });

      try {
        user2fs.rename(subtree1, new Path(subtree2, "newname"));
      } catch (AccessControlException expected){
        fail("Operation should complete without errors");
      }

    } finally {
      teardown();
    }
  }

  @Test
  public void testSubtreeDeleteBlockedByAccessAcl() throws IOException, InterruptedException {
    try {
      setup();

      //Deny access via default acl down subtree1
      setDenyUserAccessAcl(user2.getShortUserName(), level1folder1);

      //Try to delete subtree1. Should fail because of access acl down the tree.
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });

      try {
        user2fs.delete(subtree1, true);
        fail("Acl should block delete");
      } catch (AccessControlException expected){
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("projectedInode=\"level1folder1\""));
        //Operation should fail.
      }

    } finally {
      teardown();
    }
  }

  @Test
  public void testSubtreeDeleteBlockedByInheritedDefaultAcl() throws IOException, InterruptedException {
    try {
      setup();

      //Deny access via default acl down subtree1
      setDenyUserDefaultAcl(user2.getShortUserName(), subtree1);

      //Try to delete subtree1. Should fail because of access acl down the tree.
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });

      try {
        user2fs.delete(subtree1, true);
        fail("Acl should block delete");
      } catch (AccessControlException expected){
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("projectedInode=\"level1folder1\""));
        //Operation should fail.
      }

    } finally {
      teardown();
    }
  }

  @Test
  public void testSubtreeDeleteBlockedByInheritedDefaultDeepAcl() throws IOException, InterruptedException {
    try {
      setup();

      //Deny access via default acl down level1folder1
      setDenyUserDefaultAcl(user2.getShortUserName(), level1folder1);

      //Try to delete subtree1. Should fail because of access acl down the tree.
      FileSystem user2fs = user2.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });

      try {
        user2fs.delete(subtree1, true);
        fail("Acl should block delete");
      } catch (AccessControlException expected){
        assertTrue("Wrong inode triggered access control exception.", expected.getMessage().contains("projectedInode=\"level2folder1\""));
        //Operation should fail.
      }

    } finally {
      teardown();
    }
  }


  
  
  
  private void createFileTree() throws IOException {
    FileSystem superFs = cluster.getFileSystem();

    FsPermission defaultPerm = FsPermission.createImmutable((short) 0775);

    //subtrees folder
    superFs.mkdirs(subtrees);
    superFs.setPermission(subtrees, defaultPerm);
    superFs.setOwner(subtrees, user1.getShortUserName(), sharedGroup);
    //subtree1
    superFs.mkdirs(subtree1);
    superFs.setPermission(subtree1, defaultPerm);
    superFs.mkdirs(level1folder1);
    superFs.setPermission(level1folder1, defaultPerm);
    superFs.mkdirs(level1folder2);
    superFs.setPermission(level1folder2, defaultPerm);
    superFs.mkdirs(level2folder1);
    superFs.setPermission(level2folder1, defaultPerm);
    DFSTestUtil.createFile(superFs, level2file1, 1000, (short) 1, 0);
    superFs.setPermission(level2file1, defaultPerm);
    DFSTestUtil.createFile(superFs, level3file1, 1000, (short) 1, 0);
    superFs.setPermission(level3file1, defaultPerm);

    superFs.setOwner(subtree1, user1.getShortUserName(), sharedGroup);
    superFs.setOwner(level1folder1, user1.getShortUserName(), sharedGroup);
    superFs.setOwner(level1folder2, user1.getShortUserName(), sharedGroup);
    superFs.setOwner(level2folder1, user1.getShortUserName(), sharedGroup);
    superFs.setOwner(level2file1, user1.getShortUserName(), sharedGroup);
    superFs.setOwner(level3file1, user1.getShortUserName(), sharedGroup);
    
    //subtree2
    superFs.mkdirs(subtree2);
    superFs.setPermission(subtree2, defaultPerm);
    superFs.setOwner(subtree2, user1.getShortUserName(), sharedGroup);
  }

  private List<AclEntry> createUserEntry(String username, boolean isDefaultScope, FsAction permission) {
    ArrayList<AclEntry> newEntry = new ArrayList<>();
    newEntry.add(new AclEntry.Builder()
            .setScope(isDefaultScope? AclEntryScope.DEFAULT : AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .setName(username)
            .setPermission(permission).build());
    return newEntry;
  }

  private void setReadOnlyUserAccessAcl(String username, Path path) throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();

    List<AclEntry> readOnlyUserAcl = createUserEntry(username, false, FsAction.READ_EXECUTE);

    fileSystem.modifyAclEntries(path, readOnlyUserAcl);

    AclStatus aclStatus = fileSystem.getAclStatus(path);
    boolean found = false;
    for (AclEntry aclEntry : aclStatus.getEntries()) {
      if (aclEntry.getScope().equals(AclEntryScope.ACCESS) && aclEntry.getType().equals(AclEntryType.USER) &&
              aclEntry.getName().equals(username) && aclEntry.getPermission().equals(FsAction.READ_EXECUTE)) {
        found = true;
        break;
      }
    }
    assertTrue("Did not manage to update acl for path " + path.toString(), found);
  }

  private void setReadOnlyUserDefaultAcl(String username, Path path) throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();

    List<AclEntry> userEntry = createUserEntry(username, true, FsAction.READ_EXECUTE);

    fileSystem.modifyAclEntries(path, userEntry);

    AclStatus aclStatus = fileSystem.getAclStatus(path);
    boolean found = false;
    for (AclEntry aclEntry : aclStatus.getEntries()) {
      if (aclEntry.getScope().equals(AclEntryScope.DEFAULT) && aclEntry.getType().equals(AclEntryType.USER) &&
              aclEntry.getName().equals(username) && aclEntry.getPermission().equals(FsAction.READ_EXECUTE)) {
        found = true;
        break;
      }
    }
    assertTrue("Did not manage to update acl for path " + path.toString(), found);

  }

  private void setDenyUserAccessAcl(String username, Path path) throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();
    
    List<AclEntry> denyUserAcl = createUserEntry(username, false, FsAction.NONE);

    fileSystem.modifyAclEntries(path, denyUserAcl);
  
    AclStatus aclStatus = fileSystem.getAclStatus(path);
    boolean found = false;
    for (AclEntry aclEntry : aclStatus.getEntries()) {
      if (aclEntry.getScope().equals(AclEntryScope.ACCESS) && aclEntry.getType().equals(AclEntryType.USER) &&
          aclEntry.getName().equals(username) && aclEntry.getPermission().equals(FsAction.NONE)){
        found = true;
        break;
      }
    }
    assertTrue("Did not manage to update acl for path " + path.toString(), found);
  }
  
  private void setDenyUserDefaultAcl(String username, Path path) throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();
   
    List<AclEntry> denyUserDefaultAcl = createUserEntry(username, true, FsAction.NONE);

    fileSystem.modifyAclEntries(path, denyUserDefaultAcl);
  
    AclStatus aclStatus = fileSystem.getAclStatus(path);
    boolean found = false;
    for (AclEntry aclEntry : aclStatus.getEntries()) {
      if (aclEntry.getScope().equals(AclEntryScope.DEFAULT) && aclEntry.getType().equals(AclEntryType.USER) &&
          aclEntry.getName().equals(username) && aclEntry.getPermission().equals(FsAction.NONE)){
        found = true;
        break;
      }
    }
    
    assertTrue("Did not manage to update acl for path " + path.toString(), found);
  }
}
