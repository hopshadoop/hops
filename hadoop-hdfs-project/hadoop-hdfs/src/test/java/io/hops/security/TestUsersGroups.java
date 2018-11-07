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

import io.hops.exception.UniqueKeyConstraintViolationException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUsersGroups {
  
  static UsersGroupsCache newUsersGroupsMapping(Configuration conf){
    return new UsersGroupsCache((UserDataAccess)
        HdfsStorageFactory.getDataAccess(UserDataAccess.class),
        (UserGroupDataAccess) HdfsStorageFactory.getDataAccess(
            UserGroupDataAccess.class), (GroupDataAccess)HdfsStorageFactory.getDataAccess(
        GroupDataAccess.class),conf.getInt(CommonConfigurationKeys
        .HOPS_UG_CACHE_SECS, CommonConfigurationKeys
        .HOPS_UG_CACHE_SECS_DEFAULT), conf.getInt(CommonConfigurationKeys
        .HOPS_UG_CACHE_SIZE, CommonConfigurationKeys
        .HOPS_UG_CACHE_SIZE_DEFUALT));
  }
  
  @After
  public void afterTest() {
    UsersGroups.stop();
  }
  
  @Test
  public void testGroupsCacheIsEnabled(){
    Configuration conf = new HdfsConfiguration();
    assertFalse(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
        .isCacheEnabled());
    
    conf.set(DFSConfigKeys.HADOOP_SECURITY_GROUP_MAPPING,"org.apache.hadoop" +
        ".security.JniBasedUnixGroupsMappingWithFallback");
    
    assertTrue(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
        .isCacheEnabled());
  
    conf.set(DFSConfigKeys.HADOOP_SECURITY_GROUP_MAPPING,"io.hops.security" +
        ".HopsGroupsWithFallBack");
  
    assertFalse(Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf)
        .isCacheEnabled());
  }
  
  @Test
  public void testUsersGroupsCache() throws Exception{
    Configuration conf = new Configuration();
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    
    UsersGroupsCache cache = newUsersGroupsMapping(conf);
    
    String currentUser = "user0";
    
    String[] groups = new String[3];
    for(int i=0; i< 3; i++){
      groups[i] = "group"+i;
    }

    assertEquals(cache.getUserId(currentUser), 0);
    
    //user0 -> {group0, group1}
    cache.addUserGroups(currentUser,
        Arrays.copyOfRange(groups, 0, 2));

    int userId = cache.getUserId(currentUser);
    assertNotEquals(userId, 0);
    assertEquals(currentUser, cache.getUserName(userId));
    
    for(int i=0; i<2; i++){
      int groupId = cache.getGroupId(groups[i]);
      assertNotEquals(groupId, 0);
      assertEquals(groups[i], cache.getGroupName(groupId));
    }

    assertEquals(cache.getGroupId(groups[2]), 0);
    
    List<String> cachedGroups = cache.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[0], groups[1])));

    //remove group by name [group0]
    cache.removeGroup(groups[0]);

    int groupId = cache.getGroupId(groups[0]);
    assertEquals(groupId, 0);
    
    cachedGroups = cache.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[1])));

    //remove group1
    groupId = cache.getGroupId(groups[1]);
    cache.removeGroup(groups[1]);

    String groupName = cache.getGroupName(groupId);
    assertNull(groupName);

    cachedGroups = cache.getGroups(currentUser);
    assertNull(cachedGroups);
    
    //add group2
    cache.addGroup(groups[2]);
    groupId = cache.getGroupId(groups[2]);
    assertNotEquals(groupId, 0);

    cachedGroups = cache.getGroups(currentUser);
    assertNull(cachedGroups);

    // append group to user: user0 -> {group2}
    cache.addUserToGroup(currentUser, groups[2]);
    
    cachedGroups = cache.getGroups(currentUser);
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups[2])));
  }

  @Test
  public void testCacheEviction() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
  
    final int CACHE_SIZE = 5;
    final int CACHE_SECS = 5;
  
    conf.setInt(CommonConfigurationKeys.HOPS_UG_CACHE_SIZE, CACHE_SIZE);
    conf.setInt(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, CACHE_SECS);
    UsersGroupsCache cache = newUsersGroupsMapping(conf);
  
    String[] users = new String[CACHE_SIZE];
    int[] usersIds = new int[CACHE_SIZE];
    String[] groups = new String[CACHE_SIZE];
  
    for (int i = 0; i < CACHE_SIZE; i++) {
      users[i] = "user" + i;
      groups[i] = "group" + i;
    }
  
    for (int i = 0; i < CACHE_SIZE; i++) {
      cache.addUserGroups(users[i],
          Arrays.copyOfRange(groups, i, (i == CACHE_SIZE - 1 ?
              i + 1 : i + 2)));
      usersIds[i] = cache.getUserId(users[i]);
    }
  
    for (int i = 0; i < CACHE_SIZE; i++) {
      List<String> cachedGroups = cache.getGroups(users[i]);
      assertNotNull(cachedGroups);
      Collections.sort(cachedGroups);
      assertEquals(Arrays.asList(Arrays.copyOfRange(groups, i,
          (i == CACHE_SIZE - 1 ? i + 1 : i + 2))), cachedGroups);
    }
  
    // add a new user to check eviction
    String newUser = "newUser";
    cache.addUser(newUser);
  
    int newUserId = cache.getUserId(newUser);
    assertNotEquals(newUserId, 0);
    assertEquals(cache.getUserName(newUserId), newUser);
  
    //user 0 was evicted from cache
    assertNull(cache.getUserIdFromCache(users[0]));
    //however, the associated groups should be still in the cache
    assertNotNull(cache.getGroupsFromCache(users[0]));
  
    //associate the new user with group0, group1, group2
    cache.addUserGroups(newUser, Arrays.copyOfRange(groups, 0, 3));
  
    //user 1 groups should be evicted, since it is the last recently used in
    // this case
    assertNull(cache.getGroupsFromCache(users[1]));
    assertNotNull(cache.getGroupsFromCache(newUser));
  
    List<String> cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(Arrays.copyOfRange(groups, 0, 3)), cachedGroups);
  
    //repopulate the cache with user0
    assertEquals(cache.getUserId(users[0]), usersIds[0]);
  
  
    //add a new group
    String newGroup = "newgroup";
  
    cache.addGroup(newGroup);
    int newGroupId = cache.getGroupId(newGroup);
    assertNotEquals(newGroupId, 0);
    assertEquals(cache.getGroupName(newGroupId), newGroup);
  
    //group 0 should be evicted from cache
    assertNull(cache.getGroupIdFromCache(groups[0]));
  
    //add the new user to the new group
    cache.addUserToGroup(newUser, newGroup);
    cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[0], groups[1], groups[2],
        newGroup), cachedGroups);
  
    //remove the new group from cache
    cache.removeGroupFromCache(newGroup);
    assertNull(cache.getGroupIdFromCache(newGroup));
  
    cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[0], groups[1], groups[2]), cachedGroups);
  
    //wait for some time for time based eviction to happen
    Thread.sleep(CACHE_SECS * 1000);
  
    //now the newUser groups should be updated to include the newGroup
    cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[0], groups[1], groups[2],
        newGroup), cachedGroups);
  
    //retry to associate the new user to the new group
    cache.addUserToGroup(newUser, newGroup);
    cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[0], groups[1], groups[2],
        newGroup), cachedGroups);
  
    assertNotNull(cache.getGroupIdFromCache(newGroup));
    assertEquals(newGroupId, (int) cache.getGroupIdFromCache(newGroup));
  
    //remove group0
    cache.removeGroup(groups[0]);
  
    assertNull(cache.getGroupIdFromCache(groups[0]));
    assertEquals(0, cache.getGroupId(groups[0]));
  
    //get groups for user0 and newUser, it shouldn't include group0
    //user0 shouldn't have groups in the cache due to the previous timeout
    assertNull(cache.getGroupsFromCache(users[0]));
    assertEquals(Arrays.asList(groups[1]), cache.getGroups(users[0]));
    assertEquals(Arrays.asList(groups[1]), cache.getGroupsFromCache(users[0]));
  
    cachedGroups = cache.getGroupsFromCache(newUser);
    assertNotNull(cachedGroups);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[1], groups[2],
        newGroup), cachedGroups);
  
    cachedGroups = cache.getGroups(newUser);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[1], groups[2],
        newGroup), cachedGroups);
  
    //remove group1
    cache.removeGroup(groups[1]);
  
    assertNull(cache.getGroupIdFromCache(groups[1]));
    assertEquals(0, cache.getGroupId(groups[1]));
  
    assertNull(cache.getGroupsFromCache(users[0]));
    assertNull(cache.getGroups(users[0]));
  
    cachedGroups = cache.getGroupsFromCache(newUser);
    assertNotNull(cachedGroups);
    Collections.sort(cachedGroups);
    assertEquals(Arrays.asList(groups[2],
        newGroup), cachedGroups);
  
    //remove newUser
    cache.removeUser(newUser);
  
    assertNull(cache.getUserIdFromCache(newUser));
    assertEquals(0, cache.getUserId(newUser));
    assertNull(cache.getGroupsFromCache(newUser));
    assertNull(cache.getGroups(newUser));
  }


  @Test
  public void testAddRemoveUsersGroups() throws IOException {
    Configuration conf = new Configuration();
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    UsersGroupsCache cache = newUsersGroupsMapping(conf);
    
    String user = "user";
    
    //add User
    cache.addUserGroupTx(user, null, false);
    
    int userId = cache.getUserId(user);
    assertNotSame(0, userId);
    assertEquals(user, cache.getUserName(userId));
  
    //add Group
    String group = "group";
    cache.addUserGroupTx(null, group, false);
    
    int groupId = cache.getGroupId(group);
    assertNotSame(0, groupId);
    assertEquals(group, cache.getGroupName(groupId));
    
    List<String> groups = cache.getGroups(user);
    assertNull(groups);
    
    // add user-group
    cache.addUserGroupTx(user, group, false);
  
    groups = cache.getGroups(user);
    assertNotNull(groups);
    assertEquals(Arrays.asList(group), groups);
  
    //Update cache only
  
    cache.addUserGroupTx(user, group, true);
    groups = cache.getGroupsFromCache(user);
    assertNotNull(groups);
    assertEquals(Arrays.asList(group), groups);
    
    
    //removeFromCache only
    
    //remove user-group
    
    cache.removeUserGroupTx(user, group, true);
    assertNull(cache.getGroupsFromCache(user));
    assertNotNull(cache.getGroups(user));
    assertEquals(Arrays.asList(group), cache.getGroups(user));
  
    //remove group
    cache.removeUserGroupTx(null, group, true);
    assertNull(cache.getGroupIdFromCache(group));
    groupId = cache.getGroupId(group);
    assertNotSame(0, groupId);
    
    //remove user
    cache.removeUserGroupTx(user, null, true);
    assertNull(cache.getUserIdFromCache(user));
    userId = cache.getUserId(user);
    assertNotSame(0, userId);
  
    //removeFromCache and DB
  
    //remove user - group
    
    cache.removeUserGroupTx(user, group, false);
    assertNull(cache.getGroupsFromCache(user));
    assertNull(cache.getGroups(user));
    
    //remove group
    cache.removeUserGroupTx(null, group, false);
    groupId = cache.getGroupId(group);
    assertNull(cache.getGroupIdFromCache(group));
    assertEquals(0, groupId);
  
    //remove user
    cache.removeUserGroupTx(user, null, false);
    userId = cache.getUserId(user);
    assertNull(cache.getUserIdFromCache(user));
    assertEquals(0, userId);
    
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
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
  
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    JniBasedUnixGroupsMappingWithFallback jniGroupsMapping = new JniBasedUnixGroupsMappingWithFallback();
    // Directly get the groups the user belongs to
    List<String> loginUserGroups = jniGroupsMapping.getGroups(ugi.getUserName());
    assertFalse(loginUserGroups.isEmpty());
    
    UsersGroups.addUserGroupsTx(ugi.getUserName(), new String[]{});
    List<String> ugiGroups = ugi.getGroups();
    assertFalse(ugiGroups.isEmpty());
    assertThat(ugiGroups, Matchers.equalTo(loginUserGroups));
  }
  
  @Test
  public void testAddUsers() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, 10);
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    UsersGroups.addUserGroupsTx("user", new String[]{"group1", "group2"});

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

    //Wait for the group updater to kick in
    try {
      Thread.sleep(10500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    userId = UsersGroups.getUserID("user");
    assertEquals(0, userId);
    assertNull(UsersGroups.getGroups("user"));

    UsersGroups.addUserGroupsTx("user", new String[]{"group1",
        "group2"});

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2"));
    
    removeUser(userId);

    UsersGroups.addUserGroupsTx("user", new String[]{"group3"});

    int newUserId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    
    assertNotSame(userId, newUserId);

    UsersGroups.addUserGroupsTx("user", new String[]{"group1",
        "group2"});
    
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2", "group3"));
  }

  @Test
  public void testGroupMappingsRefresh() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    cluster.waitActive();

    cluster.getNameNode().getRpcServer().refreshUserToGroupsMappings();

    UsersGroups.addUserGroupsTx("user", new String[]{"group1", "group2"});

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

    userId = UsersGroups.getUserID("user");
    assertEquals(0, userId);
    assertNull(UsersGroups.getGroups("user"));

    UsersGroups.addUserGroupsTx("user", new String[]{"group1",
        "group2"});

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
  
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2"));

    removeUser(userId);

    UsersGroups.addUserGroupsTx("user", new String[]{"group3"});

    int newUserId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    
    //Auto incremented Ids
    assertTrue(newUserId > userId);

    UsersGroups.addUserGroupsTx("user", new String[]{"group1",
        "group2"});
    
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2", "group3"));

    cluster.shutdown();
  }


  @Test
  public void setOwnerMultipleTimes() throws Exception {
    Configuration conf = new HdfsConfiguration();
    
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
  
    UserGroupInformation ugi =
        UserGroupInformation.createProxyUserForTesting("testUser",
            UserGroupInformation
                .getLoginUser(), new String[]{"testGroup"});
  
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
    new LightWeightRequestHandler(
        HDFSOperationType.TEST) {
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
  public void testUserNameAndGroupNameCaseSensitivity() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
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

  private class AddUser implements Callable<Integer>{
    private final boolean useTransaction;
    private final String userName;
    AddUser(boolean useTransaction, String userName){
      this.useTransaction = useTransaction;
      this.userName = userName;
    }
    @Override
    public Integer call() throws Exception {
      if(useTransaction) {
        UsersGroups.addUserGroupsTx(userName, null);
      }else {
        UsersGroups.addUser(userName);
      }
      return UsersGroups.getUserID(userName);
    }
  }

  @Test
  public void testConcurrentAddUser() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    final String userName = "user1";
    final int CONCURRENT_USERS = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);

    List<Callable<Integer>> callables = new ArrayList<>();
    for(int i=0; i< CONCURRENT_USERS; i++){
      callables.add(new AddUser(false, userName));
    }

    List<Future<Integer>> futures = executorService.invokeAll(callables);
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    UsersGroups.clearCache();
    Integer userId = UsersGroups.getUserID(userName);

    int success = 0;
    int failure = 0;
    for(Future<Integer> f : futures){
      try {
        Integer otherUserId = f.get();
        assertNotEquals(otherUserId, Integer.valueOf(0));
        assertEquals(otherUserId, userId);
        success++;
      }catch (ExecutionException ex){
        if(ex.getCause() instanceof UniqueKeyConstraintViolationException){
          failure++;
        }else{
          fail();
        }
      }
    }

    assertTrue(success >= 1);
    assertTrue(failure == (CONCURRENT_USERS - success));
  }

  @Test
  public void testConcurrentAddUserTx() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    final String userName = "user1";
    final int CONCURRENT_USERS = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);

    List<Callable<Integer>> callables = new ArrayList<>();
    for(int i=0; i< CONCURRENT_USERS; i++){
      callables.add(new AddUser(true, userName));
    }

    List<Future<Integer>> futures = executorService.invokeAll(callables);
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    UsersGroups.clearCache();
    Integer userId = UsersGroups.getUserID(userName);
    for(Future<Integer> f : futures){
      Integer otherUserId = f.get();
      assertNotEquals(otherUserId, Integer.valueOf(0));
      assertEquals(otherUserId, userId);
    }

  }


  private class SetOwner implements Callable<Boolean>{
    private final DistributedFileSystem dfs;
    private final Path path;
    private final String userName;
    private final String groupName;
    SetOwner(DistributedFileSystem dfs, Path path, String userName, String
        groupName){
      this.dfs = dfs;
      this.path = path;
      this.userName = userName;
      this.groupName = groupName;
    }
    @Override
    public Boolean call() throws Exception {
      dfs.setOwner(path, userName, groupName);
      FileStatus fileStatus = dfs.getFileStatus(path);
      if(userName != null) {
        assertEquals(userName, fileStatus.getOwner());
      }
      if(groupName != null) {
        assertEquals(groupName, fileStatus.getGroup());
      }
      return true;
    }
  }

  @Test
  public void testConcurrentSetSameOwner() throws Exception{
    Configuration conf = new HdfsConfiguration();
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

    for(int i=0; i<CONCURRENT_USERS; i++){
      Path file = new Path(base, "file"+i);
      dfs.create(file).close();
      callables.add(new SetOwner(dfs, file,userName,
          groupName));
    }

    List<Future<Boolean>> futures = executorService.invokeAll(callables);
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    for(Future<Boolean> f : futures){
      assertTrue(f.get());
    }
    cluster.shutdown();
  }
  

  // With the new changes introduced in HOPS-676, the user addition and removal
  // happen in the DistributedFileSystem which should take care of informing
  // other namenodes caches about the addition/removal.
  @Test
  public void testSetOwnerOnOutdatedCache() throws Exception{
    final int TIME_TO_EVICT = 5;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(CommonConfigurationKeys.HOPS_UG_CACHE_SECS, TIME_TO_EVICT);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
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

      removeUser(userId);
      
      Thread.sleep(TIME_TO_EVICT * 1000);
      
      dfs.setOwner(base, username, newgroupname);

      fileStatus = dfs.getFileStatus(base);
      assertEquals(username, fileStatus.getOwner());
      assertEquals(newgroupname, fileStatus.getGroup());

      int newUserId = UsersGroups.getUserID(username);

      assertTrue(newUserId > userId);
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
  
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1)
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
    
    try{
      dfsTestUser.addUser("user");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  
    try{
      dfsTestUser.addGroup("group");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  
    try{
      dfsTestUser.addUserToGroup("user","group");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  
    try{
      dfsTestUser.removeUser("user");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  
    try{
      dfsTestUser.removeGroup("group");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  
    try{
      dfsTestUser.removeUserFromGroup("user","group");
      fail();
    }catch (AccessControlException ex){
      //only super use should be able to call
    }
  }
  
  @Test
  public void testSetOwnerDontAddUserToGroup() throws Exception{
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    cluster.waitActive();
    
    DistributedFileSystem dfs = cluster.getFileSystem();
    Path base = new Path("/base");
    dfs.mkdirs(base);
    
    final String userName = "user";
    final String groupName = "group";
    
    dfs.setOwner(base, userName, groupName);
    
    assertNull(UsersGroups.getGroups(userName));
    assertNotSame(0, UsersGroups.getUserID(userName));
    assertNotSame(0, UsersGroups.getGroupID(groupName));
    
    cluster.shutdown();
  }

}
