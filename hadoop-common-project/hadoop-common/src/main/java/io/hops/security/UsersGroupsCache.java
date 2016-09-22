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
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.hdfs.entity.User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UsersGroupsCache {

  private ConcurrentLinkedHashMap<String, List<String>> usersToGroups;
  private ConcurrentLinkedHashMap<String, List<String>> groupsToUsers;
  private ConcurrentLinkedHashMap<Integer, String> idsToUsers;
  private ConcurrentLinkedHashMap<String, Integer> usersToIds;
  private ConcurrentLinkedHashMap<Integer, String> idsToGroups;
  private ConcurrentLinkedHashMap<String, Integer> groupsToIds;

  private EvictionListener usersToGroupsEvictionListener = new EvictionListener() {
    @Override
    public void onEviction(Object key, Object value) {
      String user = (String) key;
      List<String> groups = (List<String>) value;
      removeUser(user);
      cleanCacheOnUserRemoval(user, groups);
    }
  };

  private EvictionListener groupsToUsersEvictionListener = new EvictionListener() {
    @Override
    public void onEviction(Object key, Object value) {
      String group = (String) key;
      List<String> users = (List<String>) value;
      removeGroup(group);
      for(String user: users){
        removeUser(user);
      }
    }
  };

  private EvictionListener idsToUsersEvictionListener = new EvictionListener() {
    @Override
    public void onEviction(Object key, Object value) {
      removeUser(new User((Integer)key, (String)value));
    }
  };

  private EvictionListener usersToIdsEvictionListener = new EvictionListener() {
    @Override
    public void onEviction(Object key, Object value) {
      removeUser(new User((Integer)value, (String)key));
    }
  };

  private EvictionListener idsToGroupsEvictionListener = new EvictionListener
      () {
    @Override
    public void onEviction(Object key, Object value) {
      removeGroup(new Group((Integer)key, (String) value), true);
    }
  };

  private EvictionListener groupsToIdsEvictionListener = new EvictionListener
      () {
    @Override
    public void onEviction(Object key, Object value) {
      removeGroup(new Group((Integer)value, (String) key), true);
    }
  };


  public UsersGroupsCache(int lrumax){
    usersToGroups = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(usersToGroupsEvictionListener).build();
    groupsToUsers = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(groupsToUsersEvictionListener).build();
    idsToUsers = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(idsToUsersEvictionListener).build();
    usersToIds = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(usersToIdsEvictionListener).build();
    idsToGroups = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(idsToGroupsEvictionListener).build();
    groupsToIds = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity
        (lrumax).listener(groupsToIdsEvictionListener).build();
  }


  List<String> getGroups(String user){
    if(user == null)
      return null;
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

  void removeGroup(Group group){
    removeGroup(group, false);
  }

  void removeGroup(String groupName){
    removeGroup(groupName, false);
  }

  void removeGroup(Integer groupId){
    removeGroup(groupId, false);
  }

  private void removeGroup(Group group, boolean removeUsers){
    removeGroup(group.getId(), removeUsers);
    removeGroup(group.getName(), removeUsers);
  }

  private void removeGroup(String groupName, boolean removeUsers){
    if(groupName == null)
      return;

    Integer groupId = groupsToIds.remove(groupName);
    if(groupId != null){
      idsToGroups.remove(groupId);
    }
    cleanCacheOnGroupRemoval(groupName, removeUsers);
  }

  private void removeGroup(Integer groupId, boolean removeUsers){
    if(groupId == null)
      return;

    String groupName = idsToGroups.remove(groupId);
    if(groupName != null){
      groupsToIds.remove(groupName);
      cleanCacheOnGroupRemoval(groupName, removeUsers);
    }
  }

  private void cleanCacheOnUserRemoval(String userName){
    cleanCacheOnUserRemoval(userName, usersToGroups.remove(userName));
  }

  private void cleanCacheOnUserRemoval(String userName, List<String> groups){
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

  private void cleanCacheOnGroupRemoval(String groupName, boolean removeUsers){
    List<String> users = groupsToUsers.remove(groupName);
    if(users == null)
      return;

    for(String user : users){
      if(removeUsers){
        removeUser(user);
      }else {
        List<String> userGroups = usersToGroups.get(user);
        if(userGroups != null){
          userGroups.remove(groupName);
          if(userGroups.isEmpty()){
            removeUser(user);
          }
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
    usersToGroups.put(user.getName(), groupNames);
    return groupNames;
  }

  void appendUserGroups(String user, Collection<String> groups){
    List<String> currentGroups = usersToGroups.get(user);
    if(currentGroups == null){
      currentGroups = new ArrayList<String>();
    }

    Set<String> newGroups = new HashSet<String>();

    newGroups.addAll(groups);
    newGroups.removeAll(currentGroups);


    for(String group : newGroups){
      groupsToUsers.putIfAbsent(group, new ArrayList<String>());
      groupsToUsers.get(group).add(user);
    }

    currentGroups.addAll(newGroups);
    usersToGroups.put(user, currentGroups);
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

  Integer getUserId(String userName){
    if(userName == null)
      return null;
    return usersToIds.get(userName);
  }

  Integer getGroupId(String groupName){
    if(groupName == null)
      return null;
    return groupsToIds.get(groupName);
  }

  String getUserName(Integer userId) {
    if(userId == null)
      return null;
    return idsToUsers.get(userId);
  }

  String getGroupName(Integer groupId) {
    if(groupId == null)
      return null;
    return idsToGroups.get(groupId);
  }

  void clear(){
    usersToGroups.clear();
    groupsToUsers.clear();
    idsToUsers.clear();
    usersToIds.clear();
    idsToGroups.clear();
    groupsToIds.clear();
  }

}
