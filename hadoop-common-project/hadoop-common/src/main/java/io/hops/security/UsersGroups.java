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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
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
      while (true) {
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
        }
      }
    }
  }

  private static StorageConnector storageConnector;
  private static UserGroupDataAccess userGroupDataAccess;
  private static GroupDataAccess<Group> groupDataAccess;
  private static UserDataAccess<User> userDataAccess;
  private static int groupUpdateTime;

  private static UsersGroupsCache cache;

  private static Thread th = null;
  private static boolean isConfigured = false;

  public static void init(StorageConnector connector, UserDataAccess uda,
      UserGroupDataAccess ugda, GroupDataAccess gda, int gutime, int lrumax) {
    storageConnector = connector;
    userDataAccess = uda;
    userGroupDataAccess = ugda;
    groupDataAccess = gda;
    groupUpdateTime = gutime;
    cache = new UsersGroupsCache(lrumax);
    if (storageConnector != null && userGroupDataAccess != null &&
        userDataAccess != null && groupDataAccess != null && th ==
        null) {
      th = new Thread(new GroupsUpdater(), "GroupsUpdater");
      th.setDaemon(true);
      th.start();
      isConfigured = true;
    }
  }

  public static List<String> getGroups(final String user)
      throws IOException {
    if (!isConfigured) {
      return null;
    }

    List<String> groups = cache.getGroups(user);
    if (groups == null) {
      groups = getGroupsFromDBAndUpdateCached(user);
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
      return userId;
    }

    User user = getUserFromDB(userName, null);
    if (user == null) {
      cache.removeUser(userName);
      return 0;
    }

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
      return groupId;
    }

    Group group = getGroupFromDB(groupName, null);
    if (group == null) {
      cache.removeGroup(groupName);
      return 0;
    }

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
      return userName;
    }

    User user = getUserFromDB(null, userId);
    if (user == null) {
      cache.removeUser(userId);
      return null;
    }

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
      return groupName;
    }

    Group group = getGroupFromDB(null, groupId);
    if (group == null) {
      cache.removeGroup(groupId);
      return null;
    }

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
        boolean transactionActive = connector.isTransactionActive();

        if (!transactionActive) {
          connector.beginTransaction();
        }

        User user = userDataAccess.getUser(userName);
        if (user == null) {
          return null;
        }

        List<Group> groups = userGroupDataAccess.getGroupsForUser(user.getId());

        if (!transactionActive) {
          storageConnector.commit();
        }

        return new Pair<User, List<Group>>(user, groups);
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

  public static void addUserToGroups(final String user, final String[] groups)
      throws IOException {
    if (!isConfigured) {
      return;
    }

    new LightWeightRequestHandler(UsersOperationsType.ADD_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {

        User addedUser = null;
        if(user != null){
          addedUser = userDataAccess.addUser(user);
          cache.addUser(addedUser);
        }

        if (groups != null) {
          for (String group : groups) {
            if (group != null) {
              Group addedGroup = groupDataAccess.addGroup(group);
              if(addedUser != null && addedGroup != null) {
                userGroupDataAccess.addUserToGroup(addedUser.getId(),
                    addedGroup.getId());
              }
              cache.addGroup(addedGroup);
            }
          }
        }
        return null;
      }
    }.handle();
  }
}
