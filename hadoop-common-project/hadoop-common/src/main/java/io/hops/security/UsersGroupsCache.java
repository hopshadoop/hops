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
import com.google.common.cache.*;
import com.google.common.collect.Lists;
import io.hops.StorageConnector;
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
    ADD_USER_TO_GROUPS,
    REMOVE_USER_FROM_GROUPS,
    CREATE_LOCK_ROWS
  }

  private final UserGroupDataAccess userGroupDataAccess;
  private final GroupDataAccess<Group> groupDataAccess;
  private final UserDataAccess<User> userDataAccess;

  private LoadingCache<String, List<String>> userToGroupsCache;

  private LoadingCache<Integer, String> idToUserCache;
  private LoadingCache<String, Integer> userToIdCache;
  private LoadingCache<Integer, Integer> deletedUsersCache;

  private LoadingCache<Integer, String> idToGroupCache;
  private LoadingCache<String, Integer> groupToIdCache;
  private LoadingCache<Integer, Integer> deletedGroupsCache;

  private static final String lockRowName = "#HopsSyncUser#";
  private static User lockUser;

  private CacheLoader<String, List<String>> userToGroupsLoader
          = new CacheLoader<String, List<String>>() {
    @Override
    public List<String> load(String userName) throws Exception {
      LOG.debug("Get groups from DB for user: " + userName);
      List<Group> groups = getUserGroupsFromDB(userName, getUserId(userName));
      if (groups == null || groups.isEmpty()) {
        throw new GroupsNotFoundForUserException("No groups found for user (" + userName + ")");
      }

      List<String> groupNames = Lists.newArrayListWithExpectedSize(groups.size());

      for (Group group : groups) {
        groupNames.add(group.getName());
        updateGroupCache(group.getId(), group.getName());
      }
      return groupNames;
    }
  };

  private RemovalListener<String, List<String>> userToGroupsRemoval
          = new RemovalListener<String, List<String>>() {
    @Override
    public void onRemoval(RemovalNotification<String, List<String>> rn) {
      LOG.debug("User's groups removal notification for " + rn.toString()
              + "(" + rn.getCause() + ")");
    }
  };

  private CacheLoader<Integer, String> idToUserLoader = new CacheLoader<Integer, String>() {
    @Override
    public String load(Integer userId) throws Exception {
      LOG.debug("Get user from DB by ID. UserID: " + userId);
      User user = getUserFromDB(null, userId);
      if (user != null) {
        userToIdCache.put(user.getName(), userId);
        return user.getName();
      }
      deletedUsersCache.put(userId, userId);
      throw new UserNotFoundException("User ID: " + userId + " not found.");
    }
  };

  private RemovalListener<Integer, String> idToUserRemoval
          = new RemovalListener<Integer, String>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, String> rn) {
      LOG.debug("User removal notification for " + rn.toString()
              + "(" + rn.getCause() + ")");
    }
  };

  private CacheLoader<String, Integer> userToIdLoader = new CacheLoader<String, Integer>() {
    @Override
    public Integer load(String userName) throws Exception {
      LOG.debug("Get user from DB by name: " + userName);
      User user = getUserFromDB(userName, null);
      if (user != null) {
        idToUserCache.put(user.getId(), userName);
        return user.getId();
      }
      throw new UserNotFoundException("User name: " + userName + " not found.");
    }
  };

  private RemovalListener<String, Integer> userToIdsNameRemoval
          = new RemovalListener<String, Integer>() {
    @Override
    public void onRemoval(RemovalNotification<String, Integer> rn) {
      LOG.debug("User removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };

  private CacheLoader<Integer, String> idToGroupLoader = new CacheLoader<Integer, String>() {
    @Override
    public String load(Integer groupId) throws Exception {
      LOG.debug("Get group from DB by id: " + groupId);
      Group group = getGroupFromDB(null, groupId);
      if (group != null) {
        groupToIdCache.put(group.getName(), groupId);
        return group.getName();
      }
      deletedGroupsCache.put(groupId, groupId);
      throw new GroupNotFoundException("Group ID: " + groupId + " not found.");
    }
  };

  private RemovalListener<Integer, String> idToGroupsRemoval
          = new RemovalListener<Integer, String>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, String> rn) {
      LOG.debug("Group removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };

  private CacheLoader<String, Integer> groupToIdsLoader = new CacheLoader<String, Integer>() {
    @Override
    public Integer load(String groupName) throws Exception {
      LOG.debug("Get group from DB by name: " + groupName);
      Group group = getGroupFromDB(groupName, null);
      if (group != null) {
        idToGroupCache.put(group.getId(), groupName);
        return group.getId();
      }
      throw new GroupNotFoundException("Group name: " + groupName + " not found.");

    }
  };

  private RemovalListener<String, Integer> groupToIdsRemoval
          = new RemovalListener<String, Integer>() {
    @Override
    public void onRemoval(RemovalNotification<String, Integer> rn) {
      LOG.debug("Group removal notification for " + rn.toString() + "(" + rn.getCause() + ")");
    }
  };

  private RemovalListener<Integer, Integer> deletedUsersRemoval
          = new RemovalListener<Integer, Integer>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, Integer> rn) {
    }
  };

  private CacheLoader<Integer, Integer> deletedUsersLoader = new CacheLoader<Integer, Integer>() {
    @Override
    public Integer load(Integer groupId) throws Exception {
      throw new UnsupportedOperationException("Deleted user can not be loaded from DB.");
    }
  };

  private RemovalListener<Integer, Integer> deletedGroupRemoval
          = new RemovalListener<Integer, Integer>() {
    @Override
    public void onRemoval(RemovalNotification<Integer, Integer> rn) {
    }
  };

  private CacheLoader<Integer, Integer> deletedGroupLoader = new CacheLoader<Integer, Integer>() {
    @Override
    public Integer load(Integer groupId) throws Exception {
      throw new UnsupportedOperationException("Deleted group can not be loaded from DB.");
    }
  };
  public UsersGroupsCache(UserDataAccess uda, UserGroupDataAccess ugda,
                          GroupDataAccess gda, int evectionTime, int lrumax) throws IOException {

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

    deletedUsersCache = CacheBuilder.newBuilder()
            .maximumSize(lrumax)
            .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
            .removalListener(deletedUsersRemoval)
            .build(deletedUsersLoader);

    deletedGroupsCache = CacheBuilder.newBuilder()
            .maximumSize(lrumax)
            .expireAfterWrite(evectionTime, TimeUnit.SECONDS)
            .removalListener(deletedGroupRemoval)
            .build(deletedGroupLoader);
  }


  public void createSyncRow() throws IOException {
    new LightWeightRequestHandler(UsersOperationsType.CREATE_LOCK_ROWS) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Creating UsersGroups Lock Row");
        boolean fail = false;
        User user;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }
        try {

          user = userDataAccess.getUser(lockRowName);
          if(user == null){
            user = userDataAccess.addUser(lockRowName);
          }

          if (localTx) {
            connector.commit();
          }
        } catch (UniqueKeyConstraintViolationException ue) {  // can happen if multi NNs are
          // started at the same time
          user = userDataAccess.getUser(lockRowName);
          if (localTx) {
            connector.commit();
          }
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
        lockUser = user;
        return null;
      }
    }.handle();
  }

  private void ugExclusiveLock(StorageConnector connector) throws IOException {
    connector.writeLock();
    User user = null;
    if (lockUser != null) {
      user = userDataAccess.getUser(lockUser.getId());
    }
    connector.readCommitted();

    if (user == null) {
      throw new RuntimeException("Hard Error. Users/Groups synchronization row is missing.");
    }
  }

  // Taking shared lock has significant performance impact when the cache is disabled
  // This is because we are using only one database row to synchronize the
  // users/groups operations.
  private void ugSharedLock(StorageConnector connector) throws StorageException {
//    connector.readLock();
//    User user = null;
//    if (lockUser != null) {
//      user = userDataAccess.getUser(lockUser.getId());
//    }
//    connector.readCommitted();
//
//    if (user == null) {
//      throw new RuntimeException("Hard Error. Users/Groups synchronization row is missing.");
//    }
  }

  public void clear() {
    userToGroupsCache.invalidateAll();
    idToUserCache.invalidateAll();
    userToIdCache.invalidateAll();
    idToGroupCache.invalidateAll();
    groupToIdCache.invalidateAll();
    deletedGroupsCache.invalidateAll();
    deletedUsersCache.invalidateAll();
  }

  private User getUserFromDB(final String userName, final Integer userId)
          throws IOException {
    return (User) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.GET_USER) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Get User: " + userName + " from DB.");
        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugSharedLock(connector);
          User user = userName == null ? userDataAccess.getUser(userId) :
                  userDataAccess.getUser(userName);

          if (localTx) {
            connector.commit();
          }

          return user;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  private User addUserToDB(final String userName) throws IOException {

    return (User) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.ADD_USER) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Add User: " + userName + " to DB.");
        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          User user = userDataAccess.addUser(userName);

          if (localTx) {
            connector.commit();
          }
          return user;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  @VisibleForTesting
  protected void removeUserFromDB(final Integer userId) throws IOException {

    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_USER) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Remove UserID: " + userId + " from DB.");
        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          userDataAccess.removeUser(userId);

          if (localTx) {
            connector.commit();
          }

          return null;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  private Group getGroupFromDB(final String groupName, final Integer groupId) throws IOException {

    return (Group) new LightWeightRequestHandler(
            UsersGroupsCache.UsersOperationsType.GET_GROUP) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Get GroupName: " + groupName + " GroupID: " + groupId + " from DB.");
        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugSharedLock(connector);
          Group group = groupName == null ? groupDataAccess.getGroup(groupId) :
                  groupDataAccess.getGroup(groupName);

          if (localTx) {
            connector.commit();
          }

          return group;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  private Group addGroupToDB(final String groupName) throws IOException {

    return (Group) new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.ADD_GROUP) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Add Group: " + groupName + " to DB.");

        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          Group group = groupDataAccess.addGroup(groupName);

          if (localTx) {
            connector.commit();
          }

          return group;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  @VisibleForTesting
  protected void removeGroupFromDB(final Integer groupId) throws IOException {

    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_GROUP) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Remove GroupID: " + groupId + " from DB.");

        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          groupDataAccess.removeGroup(groupId);

          if (localTx) {
            connector.commit();
          }

          return null;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  private void removeUserFromGroupDB(final Integer userId, final Integer groupId) throws
          IOException {

    new LightWeightRequestHandler(UsersGroupsCache.UsersOperationsType.REMOVE_USER_FROM_GROUPS) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Removing user from group. UserID: " + userId + " GropuID: " + groupId + ".");

        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          userGroupDataAccess.removeUserFromGroup(userId, groupId);

          if (localTx) {
            connector.commit();
          }

          return null;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  @VisibleForTesting
  public void addUserToGroupDB(final int userId, final int groupId) throws IOException {

    new LightWeightRequestHandler(UsersOperationsType.ADD_USER_TO_GROUPS) {
      @Override
      public Object performTask() throws IOException {
        LOG.debug("Add user to group. UserID: " + userId + " GroupID: " + groupId + ".");

        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugExclusiveLock(connector);
          userGroupDataAccess.addUserToGroup(userId, groupId);

          if (localTx) {
            connector.commit();
          }

          return null;
        } catch (IOException e) {
          fail = true;

          if (e instanceof ForeignKeyConstraintViolationException) {
            //find out which of the IDs are missing.
            User user = getUserFromDB(null, userId);  // Throws UserNotFoundException
            if (user == null) {
              invCachesUserRemoved(userId);
              throw new UserNotFoundException("Unable to add UserGroup mapping because user with " +
                      "ID: " + userId + " does not exist.");
            }

            Group group = getGroupFromDB(null, groupId); // Throws GroupNotFoundException
            if (group == null) {
              invCachesGroupRemoved(groupId);
              throw new GroupNotFoundException("Unable to add UserGroup mapping because group " +
                      "with  ID: " + groupId + " does not exist.");
            }
          }
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  private List<Group> getUserGroupsFromDB(final String userName, final Integer userId) throws
          IOException {

    return (List<Group>) new LightWeightRequestHandler
            (UsersGroupsCache.UsersOperationsType.GET_USER_GROUPS) {
      @Override
      public Object performTask() throws IOException {
        List<Group> result = null;
        boolean fail = false;
        boolean localTx = !connector.isTransactionActive();
        if (localTx) {
          connector.beginTransaction();
        }

        try {
          ugSharedLock(connector);
          User user = userId == null ? userDataAccess.getUser(userName) :
                  userDataAccess.getUser(userId);
          if (user != null) {
            result = userGroupDataAccess.getGroupsForUser(user.getId());
          }

          if (localTx) {
            connector.commit();
          }

          return result;
        } catch (IOException e) {
          fail = true;
          throw e;
        } finally {
          if (fail && localTx) {
            connector.rollback();
          }
        }
      }
    }.handle();
  }

  public String getUserName(int userId) throws IOException {
    if(userId == 0){
      return null;
    }

    try {

      // case of deleted users
      if(deletedUsersCache.getIfPresent(userId) != null){
        throw new UserNotFoundException("User ID: " + userId + " not found.");
      }

      // get from DB
      return idToUserCache.get(userId);

    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public int getUserId(String userName) throws IOException {
    if(userName == null){
      return 0;
    }

    try {
      return userToIdCache.get(userName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public List<String> getGroups(String user) throws IOException {
    assert user != null;

    try {
      return userToGroupsCache.get(user);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public int getGroupId(String groupName) throws IOException {
    if(groupName == null){
      return 0;
    }

    try {
      return groupToIdCache.get(groupName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public String getGroupName(int groupId) throws IOException {
    if(groupId == 0){
      return null;
    }

    try {

      // case of deleted groups
      if(deletedGroupsCache.getIfPresent(groupId) != null){
        throw new GroupNotFoundException("Group ID: " + groupId + " not found.");
      }

      return idToGroupCache.get(groupId);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public Integer addUser(String userName) throws IOException {

    if (userName == null) {
      return null;
    }

    try {
      int id = userToIdCache.get(userName);
      throw new UserAlreadyExistsException("User: " + userName + " already exists with ID: " + id);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserNotFoundException) {
        try {
          User user = addUserToDB(userName);
          updateUserCache(user.getId(), user.getName());
        } catch (UniqueKeyConstraintViolationException ue) {
          throw new UserAlreadyExistsException("User: " + userName + " already exists.");
        }
      } else {
        throw new IOException(e);
      }
    }

    return getUserId(userName);
  }


  public Integer addGroup(String groupName) throws IOException {

    if (groupName == null) {
      return null;
    }
    try {
      groupToIdCache.get(groupName);
      throw new GroupAlreadyExistsException("Group: " + groupName + " already exists");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupNotFoundException) {
        try {
          Group group = addGroupToDB(groupName);
          updateGroupCache(group.getId(), group.getName());
        } catch (UniqueKeyConstraintViolationException ue) {
          throw new GroupAlreadyExistsException("Group: " + groupName + " already exists");
        }
      } else {
        throw new IOException(e);
      }
    }
    return getGroupId(groupName);
  }

  public void removeUser(String userName) throws IOException {

    if (userName == null)
      return;

    try {
      int userID = userToIdCache.get(userName);
      LOG.debug("Remove user from DB name: " + userName);
      removeUserFromDB(userID);
      invCacheUserRemoved(userID, userName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserNotFoundException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public void removeGroup(String group) throws IOException {
    assert group != null;

    LOG.debug("Remove group from DB name: " + group);
    try {
      int groupID = groupToIdCache.get(group);
      removeGroupFromDB(groupID);
      invCachesGroupRemoved(groupID, group);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupNotFoundException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public void addUserToGroups(String user, String[] groups) throws IOException {
    assert user != null;
    assert groups != null;

    for (String group : groups) {
      addUserToGroup(user, group);
    }
  }

  public void addUserToGroup(String user, String group) throws IOException {
    assert user != null && group != null;

    LOG.debug("Adding user: " + user + " to Group: " + group);

    List<String> availableGroups = Collections.EMPTY_LIST;
    try {
      availableGroups = userToGroupsCache.get(user);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupsNotFoundForUserException) {
        // ok, continue
      } else {
        throw new IOException(e);
      }
    }

    if (availableGroups.contains(group)) {
      throw new UserAlreadyInGroupException("User: " + user + " is already part of Group: " + group + ".");
    }

    try {

      int userID = userToIdCache.get(user);
      int groupID = groupToIdCache.get(group);

      addUserToGroupDB(userID, groupID);

      invCacheUserAddedToGroup(user, group);

    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserNotFoundException ||
              e.getCause() instanceof GroupNotFoundException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  public void removeUserFromGroup(String user, String group) throws IOException {

    if (user == null || group == null)
      return;

    LOG.debug("Remove user-group from DB user: " + user + ", group: " + group);
    try {

      int userId = userToIdCache.get(user);
      int groupId = groupToIdCache.get(group);

      removeUserFromGroupDB(userId, groupId);

      invCachesUserRemovedFromGroup(user, group);

    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserNotFoundException ||
              e.getCause() instanceof GroupNotFoundException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
  }

  //-----------------------Cache Invlaidation Functions-------------------------
  private void invCachesGroupRemoved(int groupID) {
    String groupName = null;
    try {
      groupName = idToGroupCache.get(groupID);
    } catch (ExecutionException e) {
    }

    invCachesGroupRemoved(groupID, groupName);
  }

  protected void invCachesGroupRemoved(String group) throws IOException {
    // get the ID
    int groupID = 0;

    try {
      groupID = getGroupId(group);
    } catch (GroupNotFoundException e) {
    }

    invCachesGroupRemoved(groupID, group);
  }

  private void invCachesGroupRemoved(int groupId, String groupName) {
    if (groupId != 0) {
      idToGroupCache.invalidate(groupId);
    }

    if (groupName != null) {
      groupToIdCache.invalidate(groupName);

      //get all the users in cache that are part of this grp
      Map<String, List<String>> u2gMap = userToGroupsCache.asMap();
      for (String user : u2gMap.keySet()) {
        if (u2gMap.get(user).contains(groupName)) {
          userToGroupsCache.invalidate(user);
        }
      }
    }
  }

  private void invCachesUserRemoved(int userID) {
    String userName = null;
    try {
      userName = idToUserCache.get(userID);
    } catch (ExecutionException e) {
    }

    invCacheUserRemoved(userID, userName);
  }

  protected void invCacheUserRemoved(String user) throws IOException {
    assert user != null;
    // get the ID
    int userID = 0;

    try {
      userID = getUserId(user);
    } catch (UserNotFoundException e) {
    }

    invCacheUserRemoved(userID, user);
  }

  private void invCacheUserRemoved(int userId, String userName) {
    if (userId != 0) {
      idToUserCache.invalidate(userId);
    }

    if (userName != null) {
      userToIdCache.invalidate(userName);
      userToGroupsCache.invalidate(userName);
    }
  }

  protected void invCachesUserRemovedFromGroup(String user, String group) {
    userToGroupsCache.invalidate(user);
  }

  protected void invCacheUserAddedToGroup(String user, String group) {
    userToGroupsCache.invalidate(user);
  }

  //---------------------Explict Cache Updates---------------------------------
  private void updateGroupCache(Integer groupID, String groupName) {
    idToGroupCache.put(groupID, groupName);
    groupToIdCache.put(groupName, groupID);
  }

  private void updateUserCache(Integer userID, String userName) {
    idToUserCache.put(userID, userName);
    userToIdCache.put(userName, userID);
  }
}
