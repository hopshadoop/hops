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
package io.hops.security;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestUsersGroups {

  private static final Log LOG = LogFactory.getLog(TestUsersGroups.class);

  @After
  public void afterTest() {
    UsersGroups.stop();
  }

  @Test
  public void testGroupsCacheIsEnabled() {
    Configuration conf = new HdfsConfiguration();
    assertFalse(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
            .isCacheEnabled());

    conf.set(DFSConfigKeys.HADOOP_SECURITY_GROUP_MAPPING, "org.apache.hadoop" +
            ".security.JniBasedUnixGroupsMappingWithFallback");

    assertTrue(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
            .isCacheEnabled());

    conf.set(DFSConfigKeys.HADOOP_SECURITY_GROUP_MAPPING, "io.hops.security" +
            ".HopsGroupsWithFallBack");

    assertFalse(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
            .isCacheEnabled());
  }

  @Test
  public void testUsersGroupsCache0() throws Exception {
    testUsersGroupsCache(0, 0);
  }

  @Test
  public void testUsersGroupsCache1() throws Exception {
    testUsersGroupsCache(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testUsersGroupsCache(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();

    String currentUser = "user0";

    String[] groups = new String[3];
    for (int i = 0; i < 3; i++) {
      groups[i] = "group" + i;
    }

    try {
      UsersGroups.getUserID(currentUser);
    } catch (UserNotFoundException e) {
    }

    for (String group : groups) {
      try {
        UsersGroups.getGroupID(group);
      } catch (GroupNotFoundException e) {
      }
    }

    UsersGroups.addUser(currentUser);

    for (String group : groups) {
      UsersGroups.addGroup(group);
    }

    for (String group : groups) {
      try {
        UsersGroups.addGroup(group);
      } catch (GroupAlreadyExistsException e) {
      }
    }

    try {
      UsersGroups.addUser(currentUser);
    } catch (UserAlreadyExistsException e) {
    }

    //user0 -> {group0, group1}
    UsersGroups.addUserToGroups(currentUser, Arrays.copyOfRange(groups, 0, 2));

    try {
      UsersGroups.addUserToGroup(currentUser, groups[0]);
    } catch (UserAlreadyInGroupException e) {
    }

    int userId = UsersGroups.getUserID(currentUser);
    assertNotEquals(userId, 0);
    assertEquals(currentUser, UsersGroups.getUser(userId));
    List<String> cachedGroups = UsersGroups.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[0], groups[1])));

    for (int i = 0; i < 2; i++) {
      int groupId = UsersGroups.getGroupID(groups[i]);
      assertNotEquals(groupId, 0);
      assertEquals(groups[i], UsersGroups.getGroup(groupId));
    }

    assertNotEquals(UsersGroups.getGroupID(groups[2]), 0);

    //remove group group0
    UsersGroups.removeGroup(groups[0]);

    try {
      UsersGroups.getGroupID(groups[0]);
    } catch (GroupNotFoundException e) {
    }

    cachedGroups = UsersGroups.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[1])));

    //remover user from group1
    UsersGroups.removeUserFromGroup(currentUser, groups[1]);

    UsersGroups.getGroupID(groups[1]);
    UsersGroups.getUserID(currentUser);
    try {
      UsersGroups.getGroups(currentUser);
    } catch (GroupsNotFoundForUserException e) {
    }

    //remove group1
    UsersGroups.removeGroup(groups[1]);

    try {
      UsersGroups.getGroupID(groups[1]);
    } catch (GroupNotFoundException e) {
    }

    try {
      UsersGroups.getGroups(currentUser);
    } catch (GroupsNotFoundForUserException e) {
    }

    // append group to user: user0 -> {group2}
    UsersGroups.addUserToGroup(currentUser, groups[2]);

    cachedGroups = UsersGroups.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[2])));

    // remove user and group
    UsersGroups.removeUser(currentUser);
    UsersGroups.removeGroup(groups[2]);
    try {
      UsersGroups.getUserID(currentUser);
    } catch (UserNotFoundException e) {
    }
    try {
      UsersGroups.getGroupID(groups[2]);
    } catch (GroupNotFoundException e) {
    }
    try {
      UsersGroups.getGroups(currentUser);
    } catch (UserNotFoundException e) {
    }
  }

  @Test
  public void testAddRemoveUsersGroups0() throws Exception {
    testAddRemoveUsersGroups(0, 0);
  }

  @Test
  public void testAddRemoveUsersGroups1() throws Exception {
    testAddRemoveUsersGroups(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testAddRemoveUsersGroups(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();

    String user = "user";

    //add User
    UsersGroups.addUser(user);

    int userId = UsersGroups.getUserID(user);
    assertNotSame(0, userId);
    assertEquals(user, UsersGroups.getUser(userId));

    //add Group
    String group = "group";
    UsersGroups.addGroup(group);

    int groupId = UsersGroups.getGroupID(group);
    assertNotSame(0, groupId);
    assertEquals(group, UsersGroups.getGroup(groupId));

    try {
      UsersGroups.getGroups(user);
    } catch (GroupsNotFoundForUserException e) {
    }

    // add user-group
    UsersGroups.addUserToGroup(user, group);

    List<String> groups = UsersGroups.getGroups(user);
    assertNotNull(groups);
    assertEquals(Arrays.asList(group), groups);

    UsersGroups.removeUserFromGroup(user, group);

    try {
      UsersGroups.getGroups(user);
    } catch (GroupsNotFoundForUserException e) {
    }

    //remove group
    UsersGroups.removeGroup(group);
    try {
      UsersGroups.getGroupID(group);
    } catch (GroupNotFoundException e) {
    }

    //remove user
    UsersGroups.removeUser(user);
    try {
      UsersGroups.getUserID(user);
    } catch (UserNotFoundException e) {
    }

    try {
      UsersGroups.getGroups(user);
    } catch (UserNotFoundException e) {
    }
  }

  @Test
  public void testUsersGroupsNotConfigurad() throws IOException {
    UsersGroups.addUserToGroup("user", "group1");
    assertEquals(UsersGroups.getGroupID("group1"), 0);
    assertEquals(UsersGroups.getUserID("user"), 0);
    assertNull(UsersGroups.getGroups("user"));
  }

  /**
   * Test groups of a user that exists in the database but its groups not.
   * In that case it should fall back to Unix groups
   */
  @Test
  public void testGetUnixUserGroups() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    JniBasedUnixGroupsMappingWithFallback jniGroupsMapping = new JniBasedUnixGroupsMappingWithFallback();
    // Directly get the groups the user belongs to
    List<String> loginUserGroups = jniGroupsMapping.getGroups(ugi.getUserName());
    assertFalse(loginUserGroups.isEmpty());

    UsersGroups.addUserToGroups(ugi.getUserName(), new String[]{});
    List<String> ugiGroups = ugi.getGroups();
    System.out.println("Count: " + ugiGroups.size() + " Groups: " + Arrays.toString(ugiGroups.toArray()));
    assertFalse(ugiGroups.isEmpty());
    assertThat(ugiGroups, Matchers.equalTo(loginUserGroups));
  }

  @Test
  public void testAddUsers0() throws Exception {
    testAddUsers(0, 0);
  }

  @Test
  public void testAddUsers1() throws Exception {
    testAddUsers(5, CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testAddUsers(int cacheTime, int cacheSize) throws IOException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();

    String currentUser = "user";
    final int NUM_GROUPS = 5;
    String[] groups = new String[NUM_GROUPS];
    for (int i = 0; i < NUM_GROUPS; i++) {
      groups[i] = "group" + i;
      UsersGroups.addGroup(groups[i]);
    }

    UsersGroups.addUser(currentUser);

    //user -> group0,1,2
    UsersGroups.addUserToGroups(currentUser, Arrays.copyOfRange(groups, 0, 3));

    int userId = UsersGroups.getUserID(currentUser);
    assertNotSame(0, userId);
    assertEquals(UsersGroups.getUser(userId), currentUser);

    for (String group : groups) {
      int groupId = UsersGroups.getGroupID(group);
      assertNotSame(0, groupId);
      assertEquals(UsersGroups.getGroup(groupId), group);
    }

    assertThat(UsersGroups.getGroups(currentUser),
            Matchers.containsInAnyOrder(groups[0], groups[1], groups[2]));

    //remove the user from the database
    UsersGroups.getCache().removeUserFromDB(UsersGroups.getUserID(currentUser));

    if (cacheSize != 0 && cacheTime != 0) {
      userId = UsersGroups.getUserID(currentUser);
      assertNotSame(0, userId);
    } else {
      try {
        UsersGroups.getUserID(currentUser);
      } catch (UserNotFoundException e) {
      }
    }

    //Wait for the group updater to kick in
    try {
      Thread.sleep(cacheTime * 1000 + 500);
    } catch (InterruptedException e) {
    }


    try {
      UsersGroups.getUserID(currentUser);
    } catch (UserNotFoundException e) {
    }

    UsersGroups.addUser(currentUser);

    // all the groups should be there
    for (String group : groups) {
      assertNotEquals(UsersGroups.getGroupID(group), 0);
    }

    UsersGroups.addUserToGroups(currentUser, groups);

    List<String> cachedGroups = UsersGroups.getGroups(currentUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups), cachedGroups);

    //remove last group
    UsersGroups.getCache().removeGroupFromDB(UsersGroups.getGroupID(groups[NUM_GROUPS - 1]));

    cachedGroups = UsersGroups.getGroups(currentUser);
    Collections.sort(cachedGroups);

    if (cacheSize != 0 && cacheTime != 0) {
      assertEquals(Arrays.asList(groups), cachedGroups);
    } else {
      assertEquals(Arrays.asList(Arrays.copyOfRange(groups, 0, NUM_GROUPS - 1)), cachedGroups);
    }

    try {
      Thread.sleep(cacheTime * 1000 + 500);
    } catch (InterruptedException e) {
    }

    cachedGroups = UsersGroups.getGroups(currentUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(Arrays.copyOfRange(groups, 0, NUM_GROUPS - 1)), cachedGroups);
  }

  @Test
  public void testGroupMappingsRefresh() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
            .build();
    cluster.waitActive();

    cluster.getNameNode().getRpcServer().refreshUserToGroupsMappings();

    UsersGroups.addUser("user");
    UsersGroups.addGroup("group1");
    UsersGroups.addGroup("group2");
    UsersGroups.addGroup("group3");

    UsersGroups.addUserToGroups("user", new String[]{"group1", "group2"});

    int userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    assertEquals(UsersGroups.getUser(userId), "user");

    int groupId = UsersGroups.getGroupID("group1");
    assertNotSame(0, groupId);
    assertEquals(UsersGroups.getGroup(groupId), "group1");

    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
            ("group1", "group2"));

    removeUser(userId);

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);

    cluster.getNameNode().getRpcServer().refreshUserToGroupsMappings();

    try {
      userId = UsersGroups.getUserID("user");
    } catch (UserNotFoundException e){}

    UsersGroups.addUser("user");

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);

    UsersGroups.addUserToGroups("user", new String[]{"group1", "group2"});

    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
            ("group1", "group2"));

    removeUser(userId);

    try {
      UsersGroups.addUserToGroups("user", new String[]{"group3"});
    } catch (UserNotFoundException e){

    }

    UsersGroups.addUser("user");
    UsersGroups.addUserToGroups("user", new String[]{"group1", "group2"});
    UsersGroups.addUserToGroups("user", new String[]{"group3"});

    int newUserId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);

    //Auto incremented Ids
    assertTrue(newUserId > userId);

    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
            ("group1", "group2", "group3"));

    cluster.shutdown();
  }



  @Test
  public void testFKConstraintException() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();


    UsersGroups.addUser("u1");
    int uid = UsersGroups.getUserID("u1");
    UsersGroups.addGroup("g1");
    int gid = UsersGroups.getGroupID("g1");

    UsersGroups.addUserToGroup("u1", "g1");
    try {
      UsersGroups.addUserToGroup("u1", "g1");
    } catch (UserAlreadyInGroupException e){
    }


    try {
      UsersGroups.getCache().addUserToGroupDB(0, gid);
    } catch (UserNotFoundException e){

    }

    try {
      UsersGroups.getCache().addUserToGroupDB(uid, 0);
    } catch (GroupNotFoundException e){

    }

    //MySQL throws ERROR 1062 (23000): Duplicate entry 'xxx-xxx' for key 'PRIMARY'
    //But clusterj does not.
    //IF THIS TEST FAILS THEN FIX THE CODE IN addUserToGroupDB
    UsersGroups.getCache().addUserToGroupDB(uid,gid);
    UsersGroups.getCache().addUserToGroupDB(uid,gid);

  }

  @Test
  public void setOwnerMultipleTimes0() throws Exception {
    setOwnerMultipleTimes(0, 0);
  }

  @Test
  public void setOwnerMultipleTimes1() throws Exception {
    setOwnerMultipleTimes(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void setOwnerMultipleTimes(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new HdfsConfiguration();

    boolean cacheEnabled = (cacheTime != 0 && cacheSize != 0);

    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");

    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1)
                    .build();
    cluster.waitActive();

    DistributedFileSystem dfs = cluster.getFileSystem();

    Path base = new Path("/projects/project1");
    dfs.mkdirs(base);
    Path child = new Path(base, "dataset");
    dfs.mkdirs(child);

    dfs.setOwner(base, "testUser", "testGroup");

    removeGroup(UsersGroups.getGroupID("testGroup"));

    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.createProxyUserForTesting("testUser",
              UserGroupInformation.getLoginUser(), new String[]{"testGroup"});
    } catch (Exception e) {
      e.getCause().printStackTrace();
      if (e.getCause() instanceof GroupNotFoundException) {
      } else {
        throw e;
      }
    }

    if (cacheEnabled) {
      UsersGroups.clearCache();
    }

    if (cacheEnabled) {
      //previous call to createProxyUserForTesting also creates the missing users
      //and groups. If cache is enable, then users are not created and the users/groups
      //will be returned from the cache. if the cache is disabled then the users/groups
      //will be created
      UsersGroups.addGroup("testGroup");
    }

    ugi = UserGroupInformation.createProxyUserForTesting("testUser",
            UserGroupInformation.getLoginUser(), new String[]{"testGroup"});


    FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {

      @Override
      public FileSystem run() throws Exception {
        return cluster.getFileSystem();
      }
    });


    fs.mkdirs(new Path(base, "testdir"));

    dfs.setOwner(base, "testUser", "testGroup");

    cluster.shutdown();
  }

  private void removeUser(final int userId) throws IOException {
    new LightWeightRequestHandler(
            HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        UserDataAccess da = (UserDataAccess) HdfsStorageFactory.getDataAccess(UserDataAccess
                .class);
        da.removeUser(userId);
        return null;
      }
    }.handle();
  }

  private void removeGroup(final int groupId) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        GroupDataAccess da = (GroupDataAccess) HdfsStorageFactory.getDataAccess
                (GroupDataAccess.class);
        da.removeGroup(groupId);
        return null;
      }
    }.handle();
  }

  @Test
  public void testUserNameAndGroupNameCaseSensitivity0() throws Exception {
    testUserNameAndGroupNameCaseSensitivity(0, 0);
  }

  @Test
  public void testUserNameAndGroupNameCaseSensitivity1() throws Exception {
    testUserNameAndGroupNameCaseSensitivity(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testUserNameAndGroupNameCaseSensitivity(int cacheTime, int cacheSize) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    DistributedFileSystem dfs = cluster.getFileSystem();

    Path base = new Path("/projects/project1");
    dfs.mkdirs(base);
    Path child = new Path(base, "dataset");
    dfs.mkdirs(child);

    dfs.setOwner(base, "testUser", "testGroup");

    FileStatus fileStatus = dfs.getFileStatus(base);
    assertTrue(fileStatus.getOwner().equals("testUser"));
    assertTrue(fileStatus.getGroup().equals("testGroup"));

    dfs.setOwner(base, "testuser", "testgroup");
    fileStatus = dfs.getFileStatus(base);
    assertTrue(fileStatus.getOwner().equals("testuser"));
    assertTrue(fileStatus.getGroup().equals("testgroup"));

    cluster.getNameNode().getRpcServer().refreshUserToGroupsMappings();

    assertTrue(UsersGroups.getUserID("testUser") != 0);
    assertTrue(UsersGroups.getUserID("testuser") != 0);

    assertNotEquals(UsersGroups.getUserID("testUser"), UsersGroups.getUserID
            ("testuser"));

    assertTrue(UsersGroups.getGroupID("testGroup") != 0);
    assertTrue(UsersGroups.getGroupID("testgroup") != 0);

    assertNotEquals(UsersGroups.getGroupID("testGroup"), UsersGroups.getGroupID
            ("testgroup"));

    cluster.shutdown();
  }

  private class AddUser implements Callable {
    private final String userName;
    private final String groupName;

    AddUser(String userName, String groupName) {
      this.userName = userName;
      this.groupName = groupName;
    }

    @Override
    public Object call() throws Exception {
      try {
        UsersGroups.addUser(userName);
      } catch (UserAlreadyExistsException e) {
        LOG.info(e.getMessage());
      }
      try {
        UsersGroups.addGroup(groupName);
      } catch (GroupAlreadyExistsException e) {
        LOG.info(e.getMessage());
      }
      try {
        UsersGroups.addUserToGroup(userName, groupName);

      } catch (UserAlreadyInGroupException e) {
        LOG.info(e.getMessage());
      }

      return null;
    }
  }

  @Test
  public void testConcurrentAddUser0() throws Exception {
    testConcurrentAddUser(0, 0);
  }

  @Test
  public void testConcurrentAddUser1() throws Exception {
    testConcurrentAddUser(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testConcurrentAddUser(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    UsersGroups.createSyncRow();

    final String userName = "user1";
    final String groupNmae = "group1";
    final int CONCURRENT_USERS = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);

    List<Callable<Integer>> callables = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_USERS; i++) {
      callables.add(new AddUser(userName, groupNmae));
    }

    List<Future<Integer>> futures = executorService.invokeAll(callables);
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);

    UsersGroups.clearCache();

    for (Future<Integer> f : futures) {
      try {
        f.get();
      } catch (ExecutionException ex) {
        ex.printStackTrace();
        fail();
      }
    }
  }

  private class SetOwner implements Callable<Boolean> {
    private final DistributedFileSystem dfs;
    private final Path path;
    private final String userName;
    private final String groupName;

    SetOwner(DistributedFileSystem dfs, Path path, String userName, String
            groupName) {
      this.dfs = dfs;
      this.path = path;
      this.userName = userName;
      this.groupName = groupName;
    }

    @Override
    public Boolean call() throws Exception {
      for (int i = 0; i < 10; i++) {
        dfs.setOwner(path, userName, groupName);
        FileStatus fileStatus = dfs.getFileStatus(path);
        if (userName != null) {
          assertEquals(userName, fileStatus.getOwner());
        }
        if (groupName != null) {
          assertEquals(groupName, fileStatus.getGroup());
        }
      }
      return true;
    }
  }

  @Test
  public void testConcurrentSetSameOwner0() throws Exception {
    testConcurrentSetSameOwner(0, 0);
  }

  @Test
  public void testConcurrentSetSameOwner1() throws Exception {
    testConcurrentSetSameOwner(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testConcurrentSetSameOwner(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
            .build();
    cluster.waitActive();

    DistributedFileSystem dfs = cluster.getFileSystem();
    Path base = new Path("/base");
    dfs.mkdirs(base);

    final String userName = "user";
    final String groupName = "group";
    final int CONCURRENT_USERS = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);
    List<Callable<Boolean>> callables = new ArrayList<>();

    for (int i = 0; i < CONCURRENT_USERS; i++) {
      Path file = new Path(base, "file" + i);
      dfs.create(file).close();
      callables.add(new SetOwner(dfs, file, userName,
              groupName));
    }

    List<Future<Boolean>> futures = executorService.invokeAll(callables);
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    for (Future<Boolean> f : futures) {
      assertTrue(f.get());
    }
    cluster.shutdown();
  }


  @Test
  public void testSetOwnerOnOutdatedCache0() throws Exception {
    testSetOwnerOnOutdatedCache(0, 0);
  }

  @Test
  public void testSetOwnerOnOutdatedCache1() throws Exception {
    testSetOwnerOnOutdatedCache(5, CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  // With the new changes introduced in HOPS-676, the user addition and removal
  // happen in the DistributedFileSystem which should take care of informing
  // other namenodes caches about the addition/removal.
  public void testSetOwnerOnOutdatedCache(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1)
            .build();
    try {
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      Path base = new Path("/base");
      dfs.mkdirs(base);

      final String username = "user";
      final String groupname = "group";
      final String newgroupname = "newgroup";

      dfs.setOwner(base, username, groupname);

      FileStatus fileStatus = dfs.getFileStatus(base);
      assertEquals(username, fileStatus.getOwner());
      assertEquals(groupname, fileStatus.getGroup());

      int userId = UsersGroups.getUserID(username);
      int groupId = UsersGroups.getGroupID(groupname);

      UsersGroups.getCache().removeUserFromDB(userId);
      UsersGroups.getCache().removeGroupFromDB(groupId);

      if (cacheSize == 0 && cacheTime == 0) { // No Cache
        try {
          UsersGroups.getUserID(username);
        } catch (UserNotFoundException e) {
        }
        try {
          UsersGroups.getGroupID(groupname);
        } catch (GroupNotFoundException e) {
        }
        fileStatus = dfs.getFileStatus(base);
        assertTrue(fileStatus.getGroup().isEmpty());
        assertTrue(fileStatus.getOwner().isEmpty());
      } else {
        assertNotEquals(0, UsersGroups.getUserID(username));
        assertNotEquals(0, UsersGroups.getGroupID(groupname));
        fileStatus = dfs.getFileStatus(base);
        assertEquals(fileStatus.getGroup(), groupname);
        assertEquals(fileStatus.getOwner(), username);
      }

      Thread.sleep(cacheTime * 1000);

      fileStatus = dfs.getFileStatus(base);
      assertTrue(fileStatus.getGroup().isEmpty());
      assertTrue(fileStatus.getOwner().isEmpty());

      try {
        UsersGroups.getUserID(username);
      } catch (UserNotFoundException e) {
      }
      try {
        UsersGroups.getGroupID(groupname);
      } catch (GroupNotFoundException e) {
      }
      fileStatus = dfs.getFileStatus(base);
      assertTrue(fileStatus.getGroup().isEmpty());
      assertTrue(fileStatus.getOwner().isEmpty());

      dfs.setOwner(base, username, newgroupname);

      fileStatus = dfs.getFileStatus(base);
      assertEquals(username, fileStatus.getOwner());
      assertEquals(newgroupname, fileStatus.getGroup());

      int newUserId = UsersGroups.getUserID(username);

      assertTrue(newUserId != userId);
      assertNotEquals(userId, newUserId);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSuperUserCheck() throws Exception {
    Configuration conf = new HdfsConfiguration();

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
            .build();
    cluster.waitActive();

    String user = "testUser";

    DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.addUser(user);

    UserGroupInformation ugi =
            UserGroupInformation.createProxyUserForTesting(user,
                    UserGroupInformation
                            .getLoginUser(), new String[]{});

    DistributedFileSystem dfsTestUser =
            (DistributedFileSystem)
                    ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                      @Override
                      public FileSystem run() throws Exception {
                        return cluster.getFileSystem();
                      }
                    });

    try {
      dfsTestUser.addUser("user");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }

    try {
      dfsTestUser.addGroup("group");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }

    try {
      dfsTestUser.addUserToGroup("user", "group");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }

    try {
      dfsTestUser.removeUser("user");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }

    try {
      dfsTestUser.removeGroup("group");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }

    try {
      dfsTestUser.removeUserFromGroup("user", "group");
      fail();
    } catch (AccessControlException ex) {
      //only super use should be able to call
    }
  }


  @Test
  public void testSetOwnerDontAddUserToGroup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    DistributedFileSystem dfs = cluster.getFileSystem();
    Path base = new Path("/base");
    dfs.mkdirs(base);

    final String userName = "user";
    final String groupName = "group";

    dfs.setOwner(base, userName, groupName);

    try {
      UsersGroups.getGroups(userName);
      fail();
    } catch (GroupsNotFoundForUserException e) {

    }
    assertNotSame(0, UsersGroups.getUserID(userName));
    assertNotSame(0, UsersGroups.getGroupID(groupName));

    cluster.shutdown();
  }

  static AtomicBoolean fail = new AtomicBoolean(false);
  Random rand = new Random(System.currentTimeMillis());

  @Test
  public void testMultiUserMultiGrp0() throws Exception {
    testMultiUserMultiGrp(0, 0);
  }

  @Test
  public void testMultiUserMultiGrp1() throws Exception {
    testMultiUserMultiGrp(CommonConfigurationKeys.HOPS_UG_CACHE_SECS_DEFAULT,
            CommonConfigurationKeys.HOPS_UG_CACHE_SIZE_DEFUALT);
  }

  public void testMultiUserMultiGrp(int cacheTime, int cacheSize) throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int NUM_NAMENODES = 2;

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, Integer.toString(cacheTime));
    conf.set(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, Integer.toString(cacheSize));

    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).nnTopology(MiniDFSNNTopology.
                    simpleHOPSTopology(NUM_NAMENODES)).numDataNodes(1).format(true).build();
    cluster.waitActive();

    try {

      DistributedFileSystem superFS = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      int numUsers = 10; //>=1
      DistributedFileSystem[] fss = new DistributedFileSystem[numUsers];
      Path[] files = new Path[numUsers];

      for (int i = 0; i < numUsers; i++) {
        superFS.addUser("user" + i);
        LOG.info("Adding User "+i);
        superFS.addGroup("group" + i);
        LOG.info("Adding Group "+i);
      }

      //add all users to all groups
      for (int i = 0; i < numUsers; i++) {
        for (int j = 0; j < numUsers; j++) {
          superFS.addUserToGroup("user" + i, "group" + j);
          LOG.info("Adding User "+i+" to Group "+j);
        }
      }

      // create file system objects
      for (int i = 0; i < numUsers; i++) {
        UserGroupInformation ugi =
                UserGroupInformation.createRemoteUser("user" + i);

        DistributedFileSystem fs = (DistributedFileSystem) ugi
                .doAs(new PrivilegedExceptionAction<FileSystem>() {
                  @Override
                  public FileSystem run() throws Exception {
                    return cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));
                  }
                });
        fss[i] = fs;
      }

      Path path = new Path("/Projects");
      superFS.mkdirs(path);
      superFS.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      path = new Path("/Projects/dataset");
      fss[0].mkdirs(path);
      fss[0].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
      fss[0].setOwner(path, "user0", "group0");


      for (int i = 0; i < numUsers; i++) {
        path = new Path("/Projects/dataset/user" + i);
        fss[i].mkdirs(path);
        fss[i].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
        fss[i].setOwner(path, "user" + i, "group" + i);
        path = new Path("/Projects/dataset/user" + i + "/file" + i);
        fss[i].create(path).close();
        fss[i].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
        fss[i].setOwner(path, "user" + i, "group" + i);
        files[i] = path;
      }


      Thread[] threads = new Thread[numUsers];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(new Worker(fss[i], files));
        threads[i].start();
      }

      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }


      if (fail.get()) {
        fail("Test failed no exception should have occurred during the test");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      cluster.shutdown();
    }
  }


  class Worker implements Runnable {

    DistributedFileSystem fs;
    Path[] paths;

    Worker(DistributedFileSystem fs, Path[] paths) {
      this.fs = fs;
      this.paths = paths;
    }

    public void run() {
      try {
        for (int i = 0; i < 1000; i++) {
          if (fail.get()) { //some other thread has failed
            return;
          }

          FileStatus status = fs.getFileStatus(paths[rand.nextInt(paths.length)]);
          Thread.sleep(0);
          if (status == null) {
            fail.set(true);
            return;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail.set(true);
        return;
      }
    }
  }
}
