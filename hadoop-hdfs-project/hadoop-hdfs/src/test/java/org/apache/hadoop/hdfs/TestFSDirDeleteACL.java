/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.Assert;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;

public class TestFSDirDeleteACL {
  
  @Test
  public void testDeleteFileinACLEnabledDir() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    
    String testUserName = "testUser";
    
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();
    final DistributedFileSystem fs = cluster.getFileSystem();
    
    try{
      Path dir = new Path("/dir");
      Path file = new Path(dir, "file");
      
      fs.mkdirs(dir);
      
      DFSTestUtil.createFile(fs, file, 1, (short) 1,0);
      
      List<AclEntry> aclSpec = Lists.newArrayList(
          aclEntry(ACCESS, USER, ALL),
          aclEntry(ACCESS, USER, testUserName, ALL),
          aclEntry(ACCESS, GROUP, READ_EXECUTE),
          aclEntry(ACCESS, OTHER, NONE));
      
      fs.setAcl(dir, aclSpec);
      
      UserGroupInformation ugi =
          UserGroupInformation.createProxyUserForTesting(testUserName,
              UserGroupInformation
                  .getLoginUser(), new String[]{testUserName});
  
      FileSystem testUserFS =
          ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return cluster.getFileSystem();
        }
      });
  
      try {
        testUserFS.delete(file, true);
      }catch (Exception ex){
        Assert.fail("Should not throw any exception");
      }
      
    }finally {
      cluster.shutdown();
    }
  }
}
