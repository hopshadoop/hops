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
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Private
public final class UsersGroups {
  
  private static final Log LOG = LogFactory.getLog(UsersGroups.class);
  private static UsersGroupsCache usersGroupsMapping;

  public static synchronized void init(UserDataAccess uda, UserGroupDataAccess ugda,
      GroupDataAccess gda, int evcttime, int lrumax) throws IOException {
    if(usersGroupsMapping == null) {
      LOG.info("UsersGroups Initialized.");
      usersGroupsMapping =
          new UsersGroupsCache(uda, ugda, gda, evcttime, lrumax);

      usersGroupsMapping.createSyncRow();
    }
  }

  public static void createSyncRow()
          throws IOException {
    if(usersGroupsMapping == null){
      throw new RuntimeException("UserGroups Cache is not initialized");
    }
    usersGroupsMapping.createSyncRow();
  }

  public static List<String> getGroups(String user)
      throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return null;
    }
    return usersGroupsMapping.getGroups(user);
  }

  public static int getGroupID(String groupName) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return 0;
    }
   return usersGroupsMapping.getGroupId(groupName);
  }
  
  public static int getUserID(String userName) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return 0;
    }
    return usersGroupsMapping.getUserId(userName);
  }
  
  public static String getUser(int userId) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return null;
    }
    return usersGroupsMapping.getUserName(userId);
  }

  public static String getGroup(int groupId) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return null;
    }
    return usersGroupsMapping.getGroupName(groupId);
  }
  
  public static void removeUser(String user) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.removeUser(user);
  }

  public static void removeGroup(String group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.removeGroup(group);
  }

  public static void removeUserFromGroup(String user, String group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.removeUserFromGroup(user, group);
  }

  public static void addUser(String user) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.addUser(user);
  }
  
  public static void addGroup(String group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.addGroup(group);
  }
  
  public static void addUserToGroup(String user, String group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.addUserToGroup(user, group);
  }

  public static void addUserToGroups(String user, String[] group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.addUserToGroups(user, group);
  }

  public static void clearCache(){
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.clear();
  }
  
  public static void  invCacheUserRemoved(String user) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.invCacheUserRemoved(user);
  }

  public static void  invCacheGroupRemoved(String group) throws IOException {
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.invCachesGroupRemoved(group);
  }

  public static void  invCacheUserRemovedFromGroup(String user, String group){
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.invCachesUserRemovedFromGroup(user, group);
  }

  public static void  invCacheUserAddedToGroup(String user, String group){
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
      return;
    }
    usersGroupsMapping.invCacheUserAddedToGroup(user, group);
  }

  @VisibleForTesting
  public static void stop(){
    if(usersGroupsMapping != null){
      usersGroupsMapping.clear();
      usersGroupsMapping = null;
    }
  }

  @VisibleForTesting
  protected static UsersGroupsCache getCache(){
    if(usersGroupsMapping == null){
      LOG.warn("UsersGroups was not initialized.");
    }
    return usersGroupsMapping;
  }
}
