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
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.hdfs.entity.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UsersGroupsCache {

  private ConcurrentLinkedHashMap<String, List<String>> usersToGroups;
  private ConcurrentLinkedHashMap<String, List<String>> groupsToUsers;
  private ConcurrentLinkedHashMap<Integer, String> idsToUsers;
  private ConcurrentLinkedHashMap<String, Integer> usersToIds;
  private ConcurrentLinkedHashMap<Integer, String> idsToGroups;
  private ConcurrentLinkedHashMap<String, Integer> groupsToIds;

  public UsersGroupsCache(int lrumax){
    usersToGroups = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    groupsToUsers = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    idsToUsers = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    usersToIds = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    idsToGroups = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
    groupsToIds = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).build();
  }


  List<String> getGroups(String user){
    return usersToGroups.get(user);
  }

  Set<String> knownUsers(){
    return usersToGroups.keySet();
  }

  void removeUser(User user){
    removeUser(user.getId());
    removeUser(user.getName());
  }

  void removeUser(String userName){
    if(userName == null)
      return;

    Integer userId = usersToIds.remove(userName);
    if(userId != null){
      idsToUsers.remove(userId);
    }
    cleanCacheOnUserRemoval(userName);
  }

  void removeUser(Integer userId){
    if(userId == null)
      return;
    String userName = idsToUsers.remove(userId);
    if(userName != null) {
      usersToIds.remove(userName);
      cleanCacheOnUserRemoval(userName);
    }
  }

  void removeGroup(String groupName){
    if(groupName == null)
      return;

    Integer groupId = groupsToIds.remove(groupName);
    if(groupId != null){
      idsToGroups.remove(groupId);
    }
    cleanCacheOnGroupRemoval(groupName);
  }

  void removeGroup(Integer groupId){
    if(groupId == null)
      return;

    String groupName = idsToGroups.remove(groupId);
    if(groupName != null){
      groupsToIds.remove(groupName);
      cleanCacheOnGroupRemoval(groupName);
    }
  }

  private void cleanCacheOnUserRemoval(String userName){
    List<String> groups = usersToGroups.remove(userName);
    if(groups == null)
      return;

    for(String group : groups){
      List<String> groupUsers = groupsToUsers.get(group);
      if(groupUsers != null){
        groupUsers.remove(userName);
        if(groupUsers.isEmpty()){
          removeGroup(group);
        }
      }
    }
  }

  private void cleanCacheOnGroupRemoval(String groupName){
    List<String> users = groupsToUsers.remove(groupName);
    if(users == null)
      return;

    for(String user : users){
      List<String> userGroups = usersToGroups.get(user);
      if(userGroups != null){
        userGroups.remove(groupName);
        if(userGroups.isEmpty()){
          removeUser(user);
        }
      }
    }
  }

  List<String> addUserGroups(User user, List<Group> groups){
    List<String> groupNames = Lists.newArrayListWithExpectedSize(groups.size());
    for(Group group : groups){
      groupNames.add(group.getName());
      addGroup(group);
      groupsToUsers.get(group.getName()).add(user.getName());
    }
    addUser(user);
    usersToGroups.putIfAbsent(user.getName(), groupNames);
    return groupNames;
  }

  void addUser(User user){
    if(user == null)
      return;
    idsToUsers.putIfAbsent(user.getId(), user.getName());
    usersToIds.putIfAbsent(user.getName(), user.getId());
  }

  void addGroup(Group group){
    if (group == null)
      return;
    idsToGroups.putIfAbsent(group.getId(), group.getName());
    groupsToIds.putIfAbsent(group.getName(), group.getId());
    groupsToUsers.putIfAbsent(group.getName(), new ArrayList<String>());
  }

  Integer getUserId(String userName){ return usersToIds.get(userName); }

  Integer getGroupId(String groupName){
    return groupsToIds.get(groupName);
  }

  String getUserName(Integer userId) { return idsToUsers.get(userId); }

  String getGroupName(Integer groupId) { return idsToGroups.get(groupId); }

}
