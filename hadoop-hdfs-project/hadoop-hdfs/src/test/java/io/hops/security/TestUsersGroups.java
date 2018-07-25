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

import com.google.common.collect.Lists;
import io.hops.exception.UniqueKeyConstraintViolationException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.hdfs.entity.User;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

public class TestUsersGroups {
  
  @After
  public void afterTest() {
    UsersGroups.stop();
  }
  
  @Test
  public void testUsersGroupsCache(){
    UsersGroupsCache cache = new UsersGroupsCache(10);

    User currentUser = new User(1, "user0");
    List<Group> groups = Lists.newArrayList();
    for(int i=1; i< 10; i++){
      groups.add(new Group(i, "group"+i));
    }

    //user1 -> {group1, group2}
    cache.addUserGroups(currentUser, groups.subList(0, 2));

    Integer userId = cache.getUserId(currentUser.getName());

    assertNotNull(userId);
    assertEquals((int)userId, currentUser.getId());

    for(int i=0; i<2; i++){
      Integer groupId = cache.getGroupId(groups.get(i).getName());
      assertNotNull(groupId);
      assertEquals((int)groupId, groups.get(i).getId());
    }

    assertNull(cache.getGroupId(groups.get(2).getName()));

    List<String> cachedGroups = cache.getGroups(currentUser.getName());
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups.get(0).getName(),
        groups.get(1).getName())));

    //remove group by name [group1]
    cache.removeGroup(groups.get(0).getName());

    Integer groupId = cache.getGroupId(groups.get(0).getName());
    assertNull(groupId);

    cachedGroups = cache.getGroups(currentUser.getName());
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups.get(1).getName())));

    //remove group by id [2]
    cache.removeGroup(groups.get(1).getId());

    String groupName = cache.getGroupName(groups.get(1).getId());
    assertNull(groupName);

    cachedGroups = cache.getGroups(currentUser.getName());
    assertNull(cachedGroups);

    //add group [3]

    cache.addGroup(groups.get(2));
    groupId = cache.getGroupId(groups.get(2).getName());
    assertNotNull(groupId);
    assertEquals((int)groupId, groups.get(2).getId());

    cachedGroups = cache.getGroups(currentUser.getName());
    assertNull(cachedGroups);

    // append group to user: user1 -> {group3}
    cache.appendUserGroups(currentUser.getName(), Arrays.asList(groups.get(2)
        .getName()));

    cachedGroups = cache.getGroups(currentUser.getName());
    assertNotNull(cachedGroups);
    assertTrue(cachedGroups.equals(Arrays.asList(groups.get(2).getName())));
  }

  @Test
  public void testCacheEviction(){
    final int CACHE_SIZE = 5;
    UsersGroupsCache cache = new UsersGroupsCache(CACHE_SIZE);

    List<User> users = Lists.newArrayList();
    List<Group> groups = Lists.newArrayList();
    List<String> groupNames = Lists.newArrayList();

    for(int i=1; i<=CACHE_SIZE; i++){
      users.add(new User(i, "user"+ i));
      String groupName = "group" + i;
      groups.add(new Group(i, groupName));
      groupNames.add(groupName);
    }

    for(int i=0; i<CACHE_SIZE; i++){
      cache.addUserGroups(users.get(i), groups.subList(i, (i==CACHE_SIZE - 1 ?
      i + 1 : i + 2)));
    }

    for(int i=0; i<CACHE_SIZE; i++){
      List<String> cachedGroups = cache.getGroups(users.get(i).getName());
      assertNotNull(cachedGroups);
      assertTrue(cachedGroups.equals(groupNames.subList(i, (i==CACHE_SIZE - 1 ?
          i + 1 : i + 2))));
    }

    // add a new user to check eviction
    User newUser = new User(CACHE_SIZE + 1, "newUser");
    cache.addUser(newUser);

    Integer userId = cache.getUserId(newUser.getName());
    assertNotNull(userId);
    assertEquals((int)userId, newUser.getId());

    userId = cache.getUserId(users.get(0).getName());
    assertNull(userId);
    List<String> cachedGroups = cache.getGroups(users.get(0).getName());
    assertNull(cachedGroups);

    // group1 should be removed since it is associated with only user1
    Integer groupId = cache.getGroupId(groups.get(0).getName());
    assertNull("Cache eviction should remove " + groups.get(0)
        .getName(), groupId);

    // group2 shouldn't be removed since it is associated with user1 and user2
    groupId = cache.getGroupId(groups.get(1).getName());
    assertNotNull("Cache eviction shouldn't remove " + groups.get(1)
        .getName(), groupId);


    //add 2 new group
    Group newGroup1 = new Group(CACHE_SIZE+1, "newgroup1");
    Group newGroup2 = new Group(CACHE_SIZE+2, "newgroup2");

    cache.addGroup(newGroup1);
    groupId = cache.getGroupId(newGroup1.getName());
    assertNotNull(groupId);
    assertEquals((int) groupId, newGroup1.getId());

    cache.addGroup(newGroup2);
    groupId = cache.getGroupId(newGroup2.getName());
    assertNotNull(groupId);
    assertEquals((int) groupId, newGroup2.getId());

    // group2 should be removed
    groupId = cache.getGroupId(groups.get(1).getName());
    assertNull("Cache eviction should remove " + groups.get(1)
        .getName(), groupId);

    //user2 associated with group2 should be removed
    userId = cache.getUserId(users.get(1).getName());
    assertNull("Cache eviction should remove all users associated with " +
        "a removed group ", userId);

    // group3 shouldn't be removed since it is associated with user2 and user3
    groupId = cache.getGroupId(groups.get(2).getName());
    assertNotNull("Cache eviction shouldn't remove " + groups.get(2)
        .getName(), groupId);

    //user3 shouldn't be removed
    userId = cache.getUserId(users.get(2).getName());
    assertNotNull("Cache eviction shouldn't remove " + users.get(2).getName()
        +" since it wasn't associated with " + groups.get(1).getName(),
        userId);

    cachedGroups = cache.getGroups(users.get(2).getName());
    assertNotNull(cachedGroups);
    assertEquals(cachedGroups, Arrays.asList(groups.get(2).getName(), groups
        .get(3).getName()));

    //add another group
    Group newGroup3 = new Group(CACHE_SIZE+3, "newgroup3");

    cache.addGroup(newGroup3);
    groupId = cache.getGroupId(newGroup1.getName());
    assertNotNull(groupId);
    assertEquals((int) groupId, newGroup1.getId());

    //group3 should be removed
    groupId = cache.getGroupId(groups.get(2).getName());
    assertNull("Cache eviction should remove " + groups.get(2)
        .getName(), groupId);

    //user3 should be removed
    userId = cache.getUserId(users.get(2).getName());
    assertNull("Cache eviction should remove all users associated with " +
        "a removed group ", userId);
  }


  @Test
  public void testUsersGroupsNotConfigurad() throws IOException {
    UsersGroups.addUserToGroups("user", new String[]{"group1", "group2"});
    assertEquals(UsersGroups.getGroupID("group1"), 0);
    assertEquals(UsersGroups.getUserID("user"), 0);
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
    
    UsersGroups.addUserToGroupsTx(ugi.getUserName(), new String[]{});
    List<String> ugiGroups = ugi.getGroups();
    assertFalse(ugiGroups.isEmpty());
    assertThat(ugiGroups, Matchers.equalTo(loginUserGroups));
  }
  
  @Test
  public void testAddUsers() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HOPS_GROUPS_UPDATER_ROUND, 10);
    HdfsStorageFactory.resetDALInitialized();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1", "group2"});

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

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1",
        "group2"});

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2"));
    
    removeUser(userId);

    UsersGroups.addUserToGroupsTx("user", new String[]{"group3"});

    int newUserId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);

    //Auto incremented Ids
    assertTrue(newUserId > userId);

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1",
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

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1", "group2"});

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

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1",
        "group2"});

    userId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
  
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2"));

    removeUser(userId);

    UsersGroups.addUserToGroupsTx("user", new String[]{"group3"});

    int newUserId = UsersGroups.getUserID("user");
    assertNotSame(0, userId);
    
    //Auto incremented Ids
    assertTrue(newUserId > userId);

    UsersGroups.addUserToGroupsTx("user", new String[]{"group1",
        "group2"});
    
    assertThat(UsersGroups.getGroups("user"), Matchers.containsInAnyOrder
        ("group1", "group2", "group3"));

    cluster.shutdown();
  }


  @Test
  public void setOwnerMultipleTimes() throws IOException {
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

    removeGroup(UsersGroups.getGroupID("testGroup"));
    dfs.flushCacheGroup("testGroup");

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
        UsersGroups.addUserToGroupsTx(userName, null);
      }else {
        UsersGroups.addUserToGroups(userName, null);
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


  @Test
  public void testSetOwnerOnOutdatedCache() throws Exception{
    Configuration conf = new HdfsConfiguration();
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

}
