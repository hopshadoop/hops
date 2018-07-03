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

import com.google.common.base.Predicates;
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
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Public
public class UsersGroups {

  private static final Log LOG = LogFactory.getLog(UsersGroups.class);

  private enum UsersOperationsType implements RequestHandler.OperationType {
    ADD_USER,
    GET_USER_GROUPS,
    GET_USER,
    GET_GROUP
  }

  private static class GroupsUpdater implements Runnable {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        Set<String> knownUsers = cache.knownUsers();
        try {
          for (String user : knownUsers) {
            try {
              List<String> groups = getGroupsFromDBAndUpdateCached(user);
              LOG.debug("Update Groups for [" + user + "] groups=" + groups);
            } catch (IOException e) {
              LOG.error(e);
            }
          }
          Thread.sleep(groupUpdateTime * 1000);
        } catch (InterruptedException e) {
          LOG.error(e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static UserGroupDataAccess userGroupDataAccess;
  private static GroupDataAccess<Group> groupDataAccess;
  private static UserDataAccess<User> userDataAccess;
  private static int groupUpdateTime;

  private static UsersGroupsCache cache;

  private static Thread th = null;
  private static boolean isConfigured = false;

  public static void init(UserDataAccess uda, UserGroupDataAccess ugda,
      GroupDataAccess gda, int gutime, int lrumax) {
    userDataAccess = uda;
    userGroupDataAccess = ugda;
    groupDataAccess = gda;
    groupUpdateTime = gutime;
    cache = new UsersGroupsCache(lrumax);
    if (userGroupDataAccess != null &&
        userDataAccess != null && groupDataAccess != null && th ==
        null) {
      th = new Thread(new GroupsUpdater(), "GroupsUpdater");
      th.setDaemon(true);
      th.start();
      isConfigured = true;
    }
  }

  public static void stop() {
    if (th != null) {
      th.interrupt();
      th = null;
    }
    isConfigured = false;
  }

  public static List<String> getGroups(final String user)
      throws IOException {
    if (!isConfigured) {
      return null;
    }

    List<String> groups = cache.getGroups(user);
    if (groups == null) {
      LOG.debug("Fetching groups for (" + user + ") from db");
      groups = getGroupsFromDBAndUpdateCached(user);
    }else{
      LOG.debug("Returning fetched groups from cache for (" + user + ")" );
    }
    return groups;
  }

  public static int getUserID(final String userName) throws IOException {
    if (!isConfigured) {
      return 0;
    }

    if (userName == null) {
      return 0;
    }

    Integer userId = cache.getUserId(userName);
    if (userId != null) {
      LOG.debug("Returning user id from cache (" + userName + ")");
      return userId;
    }

    User user = getUserFromDB(userName, null);
    if (user == null) {
      LOG.debug("Removing user (" + userName + ")");
      cache.removeUser(userName);
      return 0;
    }

    LOG.debug("Adding user (" + userName +  "," + user.getId()  + ") to " +
        "cache");
    cache.addUser(user);
    return user.getId();
  }

  public static int getGroupID(final String groupName) throws IOException {
    if (!isConfigured) {
      return 0;
    }

    if (groupName == null) {
      return 0;
    }

    Integer groupId = cache.getGroupId(groupName);
    if (groupId != null) {
      LOG.debug("Returning group id from cache (" + groupName + ")");
      return groupId;
    }

    Group group = getGroupFromDB(groupName, null);
    if (group == null) {
      LOG.debug("Removing group (" + groupName + ")");
      cache.removeGroup(groupName);
      return 0;
    }

    LOG.debug("Adding group (" + groupName +  "," + group.getId()  + ") to " +
        "cache");
    cache.addGroup(group);
    return group.getId();
  }

  public static String getUser(final int userId) throws IOException {
    if (!isConfigured) {
      return null;
    }

    if (userId <= 0) {
      return null;
    }
    String userName = cache.getUserName(userId);
    if (userName != null) {
      LOG.debug("Returning user from cache (" + userId + ")");
      return userName;
    }

    User user = getUserFromDB(null, userId);
    if (user == null) {
      LOG.debug("Removing user (" + userId + ")");
      cache.removeUser(userId);
      return null;
    }

    LOG.debug("Adding user (" + user.getName() +  "," + userId  + ") to " +
        "cache");
    cache.addUser(user);
    return user.getName();
  }

  public static String getGroup(final int groupId) throws IOException {
    if (!isConfigured) {
      return null;
    }

    if (groupId <= 0) {
      return null;
    }
    String groupName = cache.getGroupName(groupId);
    if (groupName != null) {
      LOG.debug("Returning group from cache (" + groupId + ")");
      return groupName;
    }

    Group group = getGroupFromDB(null, groupId);
    if (group == null) {
      LOG.debug("Removing group (" + groupId + ")");
      cache.removeGroup(groupId);
      return null;
    }

    LOG.debug("Adding group (" + group.getName() +  "," + groupId  + ") to " +
        "cache");
    cache.addGroup(group);
    return group.getName();
  }

  private static List<String> getGroupsFromDBAndUpdateCached(String user)
      throws IOException {
    Pair<User, List<Group>> userGroups = getGroupsFromDB(user);

    if (userGroups == null) {
      cache.removeUser(user);
      return null;
    }

    if (userGroups.getValue() == null) {
      cache.removeUser(userGroups.getKey());
      return null;
    }

    return cache.addUserGroups(userGroups.getKey(), userGroups.getValue());
  }

  private static Pair<User, List<Group>> getGroupsFromDB(final String userName)
      throws IOException {
    
    final LightWeightRequestHandler getGroups = new LightWeightRequestHandler
        (UsersOperationsType.GET_USER_GROUPS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        Pair<User, List<Group>> result = null;
        boolean transactionActive = connector.isTransactionActive();
        
        if (!transactionActive) {
          connector.beginTransaction();
        }

        Integer userId = cache.getUserId(userName);
        User user = userId == null ? userDataAccess.getUser(userName) :
            userDataAccess.getUser(userId);

        if (user != null) {
          List<Group> groups = userGroupDataAccess.getGroupsForUser(user.getId());
          result = new Pair<User, List<Group>>(user, groups);
        }
        if (!transactionActive) {
          connector.commit();
        }

        return result;
      }
    };

    return (Pair<User, List<Group>>) getGroups.handle();
  }

  private static User getUserFromDB(final String userName, final Integer userId)
      throws IOException {

    return (User) new LightWeightRequestHandler(UsersOperationsType.GET_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return userName == null ? userDataAccess.getUser(userId) :
            userDataAccess.getUser(userName);
      }
    }.handle();
  }

  private static Group getGroupFromDB(final String groupName, final Integer
      groupId)
      throws IOException {

    return (Group) new LightWeightRequestHandler(
        UsersOperationsType.GET_GROUP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        return groupName == null ? groupDataAccess.getGroup(groupId) :
            groupDataAccess.getGroup(groupName);
      }
    }.handle();
  }

  public static void addUserToGroupsTx(final String user, final String[]
      groups) throws IOException {

    if(!isConfigured)
      return;

    try {
      addUserToGroupsInternalTx(user, groups);
    } catch (ForeignKeyConstraintViolationException ex) {
      flushUser(user);
      for (String group : groups) {
        flushGroup(group);
      }
      addUserToGroupsInternalTx(user, groups);
    } catch (UniqueKeyConstraintViolationException ex){
      LOG.debug("User/Group was already added: " + ex);
    }
  }

  public static void addUserToGroups(final String user, final String[]
      groups) throws IOException {

    if(!isConfigured)
      return;

    addUserToGroupsInternal(user, groups);
  }

  private static void addUserToGroupsInternalTx(final String user, final
  String[] grps) throws IOException {
      new LightWeightRequestHandler(UsersOperationsType.ADD_USER) {
        @Override
        public Object performTask() throws StorageException, IOException {
          addUserToGroupsInternal(user, grps);
          return null;
        }
      }.handle();
  }

  private static void addUserToGroupsInternal(final String user, final
      String[] grps) throws StorageException {

    Collection<String> groups = null;

    if(grps != null) {
      groups = Collections2.filter(Arrays.asList(grps), Predicates
          .<String>notNull());
    }

    List<String> availableGroups = cache.getGroups(user);
    if(availableGroups != null && groups != null){
      if(availableGroups.containsAll(groups)){
        return;
      }
    }

    Integer userId = cache.getUserId(user);

    if(userId == null && user != null) {
      User addedUser = userDataAccess.addUser(user);
      cache.addUser(addedUser);
      userId = addedUser.getId();
    }

    if(groups != null){
      for(String group : groups){
        List<Integer> groupIds = Lists.newArrayList();
        Integer groupId = cache.getGroupId(group);
        if(groupId == null){
          Group addedGroup = groupDataAccess.addGroup(group);
          cache.addGroup(addedGroup);
          groupId = addedGroup.getId();
        }
        groupIds.add(groupId);

        if(userId != null) {
          userGroupDataAccess.addUserToGroups(userId, groupIds);
        }
      }

      if(user != null){
        cache.appendUserGroups(user, groups);
      }
    }
  }

  static void flushUser(String userName){
    LOG.debug("remove user (" + userName + ") from the cache");
    cache.removeUser(userName);
  }

  static void flushGroup(String groupName){
    LOG.debug("remove user (" + groupName + ") from the cache");
    cache.removeGroup(groupName);
  }

  static void clearCache(){
    LOG.debug("clear usersgroups cache");
    cache.clear();
  }
}
