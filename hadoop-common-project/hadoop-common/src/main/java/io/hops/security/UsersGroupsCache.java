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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import io.hops.exception.ForeignKeyConstraintViolationException;
import io.hops.exception.StorageException;
import io.hops.exception.UniqueKeyConstraintViolationException;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.hdfs.entity.User;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@VisibleForTesting
@InterfaceAudience.Private
class UsersGroupsCache {
  
  private final Log LOG = LogFactory.getLog(UsersGroupsCache.class);
  
  private enum UsersOperationsType implements RequestHandler.OperationType {
    ADD_USER,
    REMOVE_USER,
    ADD_GROUP,
    REMOVE_GROUP,
    GET_USER_GROUPS,
    GET_USER,
    GET_GROUP,
    ADD_USER_GROUPS,
    REMOVE_USER_GROUPS
  }
  
  private class UserNotFoundException extends Exception{
    public UserNotFoundException(String message) {
      super(message);
    }
  }
  
  private class GroupNotFoundException extends Exception{
    public GroupNotFoundException(String message) {
      super(message);
    }
  }
  
  private class GroupsNotFoundForUserException extends Exception{
    public GroupsNotFoundForUserException(String message) {
      super(message);
    }
  }
  
  private LoadingCache<String, List<String>> userToGroupsCache;

  private LoadingCache<Integer, String> idToUserCache;
  private LoadingCache<String, Integer> userToIdCache;
  
  private LoadingCache<Integer, String> idToGroupCache;
  private LoadingCache<String, Integer> groupToIdCache;
  
  private CacheLoader<String, List<String>> userToGroupsLoader =
      new CacheLoader<String, List<String>>() {

    @Override
    public List<String> load(String userName) throws Exception {
      LOG.debug("Get groups from DB for user=" + userName);
      List<Group> groups = getGroupsFromDB(userName, getUserId(userName));
      if(groups == null || groups.isEmpty()){
        throw new GroupsNotFoundForUserException("No groups found for user (" + userName + ")");
      }
      
      List<String> groupNames = Lists.newArrayListWithExpectedSize(groups.size());

      for(Group group : groups){
        groupNames.add(group.getName());
        addGroupToCache(group.getId(), group.getName());
      }
      return groupNames;
    }
  };
  
  private RemovalListener<String, List<String>> userToGroupsRemoval =
      new RemovalListener<String, List<String>>() {
    @Override
    public void onRemoval(RemovalNotification<String, List<String>> rn) {
      LOG.debug("User's groups removal notification for " + rn.toString() +
          "(" + rn.getCause() + ")");
    }
  };
  
  private CacheLoader<Integer, String> idToUserLoader = new CacheLoader<Integer, String>() {
    @Override
    public String load(Integer userId) throws Exception {
      LOG.debug("Get user from DB by id=" + userId);
      User user = getUserFromDB(null, userId);
      if(user != null){
        userToIdCache.put(user.getName(), userId);
        return user.getName();
      }
      throw new UserNotFoundException("User (" + userId + ") was not found.");
    }
  };
  
  private RemovalListener<Integer, String> idToUserRemoval = new RemovalListener<Integer, String>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, String> rn) {
      LOG.debug("User removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };
  
  private CacheLoader<String, Integer> userToIdLoader = new CacheLoader<String, Integer>() {
    @Override
    public Integer load(String userName) throws Exception {
      LOG.debug("Get user from DB by name=" + userName);
      User user = getUserFromDB(userName, null);
      if(user != null){
        idToUserCache.put(user.getId(), userName);
        return user.getId();
      }
      throw new UserNotFoundException("User (" + userName + ") was not found.");
    }
  };
  
  private RemovalListener<String, Integer> userToIdsNameRemoval =
      new RemovalListener<String, Integer>() {
    @Override
    public void onRemoval(RemovalNotification<String, Integer> rn) {
      LOG.debug("User removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };
  
  private CacheLoader<Integer, String> idToGroupLoader = new CacheLoader<Integer, String>() {
    @Override
    public String load(Integer groupId) throws Exception {
      LOG.debug("Get group from DB by id=" + groupId);
      Group group = getGroupFromDB(null, groupId);
      if(group != null){
        groupToIdCache.put(group.getName(), groupId);
        return group.getName();
      }
      throw new GroupNotFoundException("Group (" + groupId + ") was not found.");
    }
  };
  
  private RemovalListener<Integer, String> idToGroupsRemoval =
      new RemovalListener<Integer, String>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, String> rn) {
      LOG.debug("Group removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };
  
  private CacheLoader<String, Integer> groupToIdsLoader =
      new CacheLoader<String, Integer>() {
    @Override
    public Integer load(String groupName) throws Exception {
      LOG.debug("Get group from DB by name=" + groupName);
      Group group = getGroupFromDB(groupName, null);
      if(group != null){
        idToGroupCache.put(group.getId(), groupName);
        return group.getId();
      }
      throw new GroupNotFoundException("Group (" + groupName + ") was not found.");
    }
  };
  
  private RemovalListener<String, Integer> groupToIdsRemoval =
      new RemovalListener<String, Integer>() {
        @Override
        public void onRemoval(RemovalNotification<String, Integer> rn) {
          LOG.debug("Group removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
        }
  };
  
  private final UserGroupDataAccess userGroupDataAccess;
  private final GroupDataAccess<Group> groupDataAccess;
  private final UserDataAccess<User> userDataAccess;
  private final boolean isConfigured;
  
  public UsersGroupsCache(UserDataAccess uda, UserGroupDataAccess ugda,
      GroupDataAccess gda, int evectionTime, int lrumax){
    
    this.userDataAccess = uda;
    this.userGroupDataAccess = ugda;
    this.groupDataAccess = gda;
    
    userToGroupsCache = CacheBuilder.newBuilder()
        .maximumSize(lrumax)
        .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
        .removalListener(userToGroupsRemoval)
        .build(userToGroupsLoader);
    
    idToUserCache = CacheBuilder.newBuilder()
        .maximumSize(lrumax)
        .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
        .removalListener(idToUserRemoval)
        .build(idToUserLoader);

    userToIdCache = CacheBuilder.newBuilder()
        .maximumSize(lrumax)
        .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
        .removalListener(userToIdsNameRemoval)
        .build(userToIdLoader);

    idToGroupCache = CacheBuilder.newBuilder()
        .maximumSize(lrumax)
        .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
        .removalListener(idToGroupsRemoval)
        .build(idToGroupLoader);
    
    groupToIdCache = CacheBuilder.newBuilder()
        .maximumSize(lrumax)
        .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
        .removalListener(groupToIdsRemoval)
        .build(groupToIdsLoader);
    
    isConfigured = userDataAccess != null
        && userGroupDataAccess != null && groupDataAccess != null;
  }
  
  Integer addUserIfNotInCache(String userName) throws IOException{
    if(!isConfigured)
      return null;
    
    if(userName == null){
      return null;
    }

    Integer userId = userToIdCache.getIfPresent(userName);
    if(userId != null){
      LOG.debug("User " + userName + " is already in cache with id=" + userId);
      return userId;
    }
    addUser(userName);
    return getUserIdFromCache(userName);
  }
  
  void addUser(String userName) throws IOException {
    if(!isConfigured)
      return;
    
    if(userName == null)
      return;
    LOG.debug("Add user to DB name=" + userName);
    User user = addUserToDB(userName);
    LOG.debug("User Added " + user);
    addUserToCache(user.getId(), user.getName());
  }
  
  private void addUserToCache(Integer userId, String userName){
    idToUserCache.put(userId, userName);
    userToIdCache.put(userName, userId);
  }
  
  void removeUser(String userName) throws IOException {
    if(!isConfigured)
      return;
    
    if(userName == null)
      return;
  
    LOG.debug("Remove user from DB name=" + userName);
    Integer userId = getUserId(userName);
    removeUserFromDB(userId);
    removeUserFromCache(userId, userName);
  }
  
  void removeUserFromCache(String userName){
    if(userName != null) {
      removeUserFromCache(userToIdCache.getIfPresent(userName), userName);
    }
  }
  
  private void removeUserFromCache(Integer userId, String userName){
    if(userId != null) {
      idToUserCache.invalidate(userId);
    }
    if(userName != null) {
      userToIdCache.invalidate(userName);
      userToGroupsCache.invalidate(userName);
    }
  }
  
  int getUserId(String userName) throws IOException {
    if(!isConfigured)
      return 0;
    
    if(userName == null)
      return 0;
    try {
      return userToIdCache.get(userName);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof UserNotFoundException){
        return 0;
      }
      throw new IOException(e);
    }
  }
  
  Integer getUserIdFromCache(String userName){
    if(userName == null)
      return 0;
    
    return userToIdCache.getIfPresent(userName);
  }
  
  String getUserName(Integer userId) throws IOException {
    if(!isConfigured)
      return null;
    
    if(userId == null || userId <= 0)
      return null;
    try {
      return idToUserCache.get(userId);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof UserNotFoundException){
        return null;
      }
      throw new IOException(e);
    }
  }
  
  
  Integer addGroupIfNotInCache(String groupName) throws IOException{
    if(!isConfigured)
      return null;
    
    if(groupName == null){
      return null;
    }

    Integer groupId = groupToIdCache.getIfPresent(groupName);
    if(groupId != null){
      LOG.debug("Group " + groupName + " is already in cache with id=" + groupId);
      return groupId;
    }
    addGroup(groupName);
    return getGroupIdFromCache(groupName);
  }
  
  void addGroup(String groupName) throws IOException {
    if(!isConfigured)
      return;
    
    if(groupName == null)
      return;
  
    LOG.debug("Add group to DB name=" + groupName);
    Group group = addGroupToDB(groupName);
    LOG.debug("Group Added " + group);
    addGroupToCache(group.getId(), group.getName());
  }
  
  private void addGroupToCache(Integer groupId, String groupName){
    idToGroupCache.put(groupId, groupName);
    groupToIdCache.put(groupName, groupId);
  }
  
  void removeGroup(String groupName) throws IOException {
    if(!isConfigured)
      return;
    
    if(groupName == null)
      return;
    
    LOG.debug("Remove group from DB name=" + groupName);
    Integer groupId = getGroupId(groupName);
    removeGroupFromDB(groupId);
    removeGroupFromCache(groupId, groupName);
  }
  
  void removeGroupFromCache(String groupName){
    if(groupName != null) {
      removeGroupFromCache(groupToIdCache.getIfPresent(groupName), groupName);
    }
  }
  
  private void removeGroupFromCache(Integer groupId, String groupName){
    if(groupId != null) {
      idToGroupCache.invalidate(groupId);
    }
    if(groupName != null) {
      groupToIdCache.invalidate(groupName);

      //get all the users in cache that are part of this grp
      Map<String, List<String>> u2gMap = userToGroupsCache.asMap();
      for (String user : u2gMap.keySet()) {
        if(u2gMap.get(user).contains(groupName)){
          List<String> userGroups = userToGroupsCache.getIfPresent(user);
          userGroups.remove(groupName);
          if(userGroups.isEmpty()){
            userToGroupsCache.invalidate(user);
          }
        }
      }
    }
  }
  
  int getGroupId(String groupName) throws IOException {
    if(!isConfigured)
      return 0;
    
    if(groupName == null)
      return 0;
    try {
      return groupToIdCache.get(groupName);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof GroupNotFoundException){
        return 0;
      }
      throw new IOException(e);
    }
  }
  
  Integer getGroupIdFromCache(String groupName){
    if(groupName == null)
      return 0;
    return groupToIdCache.getIfPresent(groupName);
  }
  
  String getGroupName(Integer groupId) throws IOException {
    if(!isConfigured)
      return null;
    
    if(groupId == null || groupId <= 0)
      return null;
    try {
      return idToGroupCache.get(groupId);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof GroupNotFoundException){
        return null;
      }
      throw new IOException(e);
    }
  }
  
  
  void removeUserFromGroup(String userName, String groupName) throws IOException {
    if(!isConfigured)
      return;
    
    if(userName == null || groupName == null)
      return;
    
    LOG.debug("Remove user-group from DB user=" + userName + ", group=" + groupName);
    Integer userId = getUserId(userName);
    Integer groupId = getGroupId(groupName);
  
    removeUserFromGroupFromDB(userId, groupId);
    removeUserFromGroupInCache(userName, groupName);
  }
  
  List<String> getGroups(String user) throws IOException {
    if(!isConfigured)
      return null;
    
    if(user == null)
      return null;
    try {
      return userToGroupsCache.get(user);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof GroupsNotFoundForUserException){
        return null;
      }
      throw new IOException(e);
    }
  }
  
  List<String> getGroupsFromCache(String user) throws IOException {
    if (user == null)
      return null;
    return userToGroupsCache.getIfPresent(user);
  }
  
  
  void removeUserGroupTx(String user, String group, boolean cacheOnly) throws IOException{
    if(cacheOnly){
      if(user != null && group == null){
        removeUserFromCache(user);
      }else if(user == null && group != null){
        removeGroupFromCache(group);
      }else if(user != null && group != null){
        removeUserFromGroupInCache(user, group);
      }
    }else{
      if(user != null && group == null){
        removeUser(user);
      }else if(user == null && group != null){
        removeGroup(group);
      }else if(user != null && group != null){
        removeUserFromGroup(user, group);
      }
    }
  }
  
  void addUserGroupTx(String user, String group, boolean cacheOnly) throws IOException{
    if(cacheOnly){
      if(user != null && group != null){
        addUserToGroupsInCache(user, Arrays.asList(group));
      }
    }else{
      addUserGroupTx(user, group);
    }
  }
  
  void addUserGroupTx(String user, String group) throws IOException {
    addUserGroupsTx(user, new String[]{group});
  }
  
  void addUserToGroup(String user, String group) throws IOException {
    addUserGroups(user, new String[]{group});
  }
  
  void addUserGroupsTx(String user, String[] groups) throws IOException {
    if(!isConfigured)
      return;
    
    try {
      addUserGroupsInternalTx(user, groups);
    } catch (ForeignKeyConstraintViolationException ex) {
      removeUserFromCache(user);
      for (String group : groups) {
        removeGroupFromCache(group);
      }
      addUserGroupsInternalTx(user, groups);
    } catch (UniqueKeyConstraintViolationException ex){
      LOG.debug("User/Group was already added: " + ex);
    }
  }
  
  private void addUserGroupsInternalTx(final String user, final
  String[] grps) throws IOException {
    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.ADD_USER_GROUPS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        addUserGroups(user, grps);
        return null;
      }
    }.handle();
  }
  
  /**
   * Adds the user, and the groups if they don't exist in the cache to the
   * database. Also, add the user to these groups.
   * @param user
   * @param grps
   * @throws IOException
   */
  void addUserGroups(String user,
      String[] grps) throws IOException {
    
    if(!isConfigured)
      return;
    
    LOG.debug("Add user (" + user + ") to groups " + grps);
    
    Collection<String> groups = null;
    
    if (grps != null) {
      groups = Collections2.filter(Arrays.asList(grps), Predicates
          .<String>notNull());
    }
  
    Integer userId = null;
    if(user != null){
      List<String> availableGroups = userToGroupsCache.getIfPresent(user);
      if (availableGroups != null && groups != null && !groups.isEmpty()) {
        if (availableGroups.containsAll(groups)) {
          LOG.debug("Groups (" + grps + ") already available in the cache for" +
              " user (" + user + ")");
          return;
        }
      }
  
      userId = addUserIfNotInCache(user);
    }
    
    if (groups != null && !groups.isEmpty()) {
      List<Integer> groupIds = Lists.newArrayList();
      
      for (String group : groups) {
        Integer groupId = addGroupIfNotInCache(group);
        groupIds.add(groupId);
      }
  
      if (userId != null) {
        userGroupDataAccess.addUserToGroups(userId, groupIds);
        addUserToGroupsInCache(user, groups);
      }
    }
  }
  
  
  void addUserToGroupsInCache(String user, Collection<String> groups){
    List<String> currentGroups = userToGroupsCache.getIfPresent(user);
    if(currentGroups == null){
      currentGroups = new ArrayList<>();
      userToGroupsCache.put(user, currentGroups);
    }
    
    Set<String> newGroups = new HashSet<>();
    newGroups.addAll(groups);
    newGroups.removeAll(currentGroups);
    currentGroups.addAll(newGroups);
  }
  
  
  void removeUserFromGroupInCache(String user, String group){
    List<String> currentGroups = userToGroupsCache.getIfPresent(user);
    if(currentGroups == null){
      return;
    }
    currentGroups.remove(group);
    if(currentGroups.isEmpty()){
      userToGroupsCache.invalidate(user);
    }
  }
  
  void clear(){
    userToGroupsCache.invalidateAll();
    idToUserCache.invalidateAll();
    userToIdCache.invalidateAll();
    idToGroupCache.invalidateAll();
    groupToIdCache.invalidateAll();
  }
  
  
  private User getUserFromDB(final String userName, final Integer userId)
      throws IOException {
    if(!isConfigured)
      return null;
    
    return (User) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.GET_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return userName == null ? userDataAccess.getUser(userId) :
            userDataAccess.getUser(userName);
      }
    }.handle();
  }
  
  private User addUserToDB(final String userName)
      throws IOException {
    if(!isConfigured)
      return null;
    
    return (User) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.ADD_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return userDataAccess.addUser(userName);
      }
    }.handle();
  }
  
  private void removeUserFromDB(final Integer userId)
      throws IOException {
    if(!isConfigured)
      return;
    
    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        userDataAccess.removeUser(userId);
        return null;
      }
    }.handle();
  }
  
  private Group getGroupFromDB(final String groupName, final Integer
      groupId)
      throws IOException {
    if(!isConfigured)
      return null;
    
    return (Group) new LightWeightRequestHandler(
        UsersGroupsCache.UsersOperationsType.GET_GROUP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return groupName == null ? groupDataAccess.getGroup(groupId) :
            groupDataAccess.getGroup(groupName);
      }
    }.handle();
  }
  
  private Group addGroupToDB(final String groupName)
      throws IOException {
    if(!isConfigured)
      return null;
    
    return (Group) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.ADD_GROUP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return groupDataAccess.addGroup(groupName);
      }
    }.handle();
  }
  
  private void removeGroupFromDB(final Integer groupId)
      throws IOException {
    if(!isConfigured)
      return;
    
    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_GROUP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        groupDataAccess.removeGroup(groupId);
        return null;
      }
    }.handle();
  }
  
  private void removeUserFromGroupFromDB(final Integer userId,
      final Integer groupId) throws IOException{
    if(!isConfigured)
      return;
  
    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_USER_GROUPS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        userGroupDataAccess.removeUserFromGroup(userId, groupId);
        return null;
      }
    }.handle();
  }
  
  private List<Group> getGroupsFromDB(final String userName,
      final Integer userId)
      throws IOException {
    if(!isConfigured)
      return null;
    
    return (List<Group>) new LightWeightRequestHandler
        (UsersGroupsCache.UsersOperationsType.GET_USER_GROUPS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        List<Group> result = null;
        boolean transactionActive = connector.isTransactionActive();
        
        if (!transactionActive) {
          connector.beginTransaction();
        }
        
        User user = userId == null ? userDataAccess.getUser(userName) :
            userDataAccess.getUser(userId);
        
        if (user != null) {
          List<Group> groups = userGroupDataAccess.getGroupsForUser(user.getId());
          result =groups;
        }
        if (!transactionActive) {
          connector.commit();
        }
        
        return result;
      }
    }.handle();
    
  }
  
}
