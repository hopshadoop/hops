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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.hdfs.entity.User;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Public
public class UsersGroups {

  private static final Log LOG = LogFactory.getLog(UsersGroups.class);

  private enum UsersOperationsType implements RequestHandler.OperationType{
    ADD_USER,
    GET_USER_GROUPS
  }

  private static class GroupsUpdater implements Runnable {
    @Override
    public void run() {
      while (true) {
        Set<String> knownUsers = cached.keySet();
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
  private static GroupDataAccess groupDataAccess;
  private static UserDataAccess userDataAccess;
  private static int groupUpdateTime;

  private static ConcurrentLinkedHashMap<String, List<String>> cached;

  private static Thread th = null;
  public static void init(StorageConnector connector, UserDataAccess uda,
      UserGroupDataAccess ugda, GroupDataAccess gda, int gutime, int lrumax){
    storageConnector = connector;
    userDataAccess = uda;
    userGroupDataAccess = ugda;
    groupDataAccess = gda;
    groupUpdateTime = gutime;
    cached = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    if(storageConnector != null && userGroupDataAccess != null && th == null){
      th = new Thread(new GroupsUpdater(), "GroupsUpdater");
      th.setDaemon(true);
      th.start();
    }
  }

  public static List<String> getGroups(final String user)
      throws IOException {
    if(cached == null){
      return null;
    }

    List<String> groups = cached.get(user);
    if(groups == null){
      groups = getGroupsFromDBAndUpdateCached(user);
    }
    return groups;
  }

  private static List<String> getGroupsFromDBAndUpdateCached(String user)
      throws IOException {
    List<String> groups = getGroupsFromDB(user);
    cached.remove(user);
    if(groups != null) {
      cached.put(user, groups);
    }
    return groups;
  }

  private static List<String> getGroupsFromDB(final String user)
      throws IOException {
    if(storageConnector == null || userGroupDataAccess == null){
      return null;
    }

    final LightWeightRequestHandler getGroups = new LightWeightRequestHandler
        (UsersOperationsType.GET_USER_GROUPS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        boolean transactionActive = connector.isTransactionActive();

        if(!transactionActive)
          connector.beginTransaction();

        List<Group> groups = userGroupDataAccess.getGroupsForUser(getUserID
            (user));

        if(!transactionActive)
          storageConnector.commit();

        return groups;
      }
    };

    List<Group> groups = (List<Group>) getGroups.handle();
    List<String> groupNames = null;
    //User should be associated with at least one group
    if(groups != null && !groups.isEmpty()){
      Collection<String> grp = Collections2.transform(groups, new
          Function<Group, String>() {
        @Override
        public String apply(Group input) {
          return input.getName();
        }
      });
      groupNames = Lists.newArrayList(grp);
    }

    return groupNames;
  }

  public static byte[] getUserID(String user){
    if(user == null)
      return null;
    return DigestUtils.md5(user);
  }

  public static byte[] getGroupID(String group){
    if(group == null)
      return null;
    return DigestUtils.md5(group);
  }

  public static void addUserToGroups(final String user, final String[] groups)
      throws IOException {
    if(userDataAccess == null || groupDataAccess == null ||
        userGroupDataAccess == null){
      return;
    }
    new LightWeightRequestHandler(UsersOperationsType.ADD_USER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        byte[] userId = getUserID(user);
        userDataAccess.addUser(new User(userId, user));
        if(groups != null) {
          for(String  group : groups){
            if(group != null) {
              byte[] groupId = UsersGroups.getGroupID(group);
              groupDataAccess.addGroup(new Group(groupId, group));
              userGroupDataAccess.addUserToGroup(userId, groupId);
            }
          }
        }
        return null;
      }
    }.handle();
  }
}
