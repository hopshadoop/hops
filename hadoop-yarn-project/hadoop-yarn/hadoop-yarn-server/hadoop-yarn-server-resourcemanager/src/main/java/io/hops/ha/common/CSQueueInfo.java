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
package io.hops.ha.common;

import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueueUserInfoDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSQueueDataAccess;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;

public class CSQueueInfo {

  public class CSLeafQueueUserInfo {

    private int consumedMemory;
    private int consumedVCores;
    private int pendingApplications;
    private int activeApplications;

    public CSLeafQueueUserInfo() {

    }

    public void setConsumedMemory(int consumedMemory) {
      this.consumedMemory = consumedMemory;
    }

    public int getConsumedMemory() {
      return consumedMemory;
    }

    public int getConsumedVCores() {
      return consumedVCores;
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }

    public void setConsumedVCores(int consumedVcores) {
      this.consumedVCores = consumedVcores;
    }

    public void setPendingApplications(int pendingApplications) {
      this.pendingApplications = pendingApplications;
    }

    public void setActiveApplications(int activeApplications) {
      this.activeApplications = activeApplications;
    }

  }

  private static final Log LOG = LogFactory.getLog(
          SchedulerApplicationInfo.class);
  private Map<String, io.hops.metadata.yarn.entity.capacity.CSQueue> csQueueToAdd =
          new HashMap<String, io.hops.metadata.yarn.entity.capacity.CSQueue>();
  private Map<String, CSLeafQueueUserInfo> csLeafQueueUserInfoToAdd =
          new HashMap<String, CSLeafQueueUserInfo>();
  private Set<String> usersToRemove = new HashSet<String>();

  public void persist(CSQueueDataAccess csQDA,
          CSLeafQueueUserInfoDataAccess csLeafQUI) throws StorageException {
    persistCSQueueInfoToAdd(csQDA);
    persistCSLeafQueueInfoToAdd(csLeafQUI);
    persistCSLeafQueueUsersToRemove();
  }

  private void persistCSQueueInfoToAdd(CSQueueDataAccess QMDA) throws
          StorageException {
    if (csQueueToAdd != null) {
      QMDA.addAll(csQueueToAdd.values());
    }
  }

  private void persistCSLeafQueueInfoToAdd(CSLeafQueueUserInfoDataAccess QMDA)
          throws StorageException {
    if (csLeafQueueUserInfoToAdd != null) {
      List<io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo> toAddQueues
              = new ArrayList<io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo>();
      for (String username : csLeafQueueUserInfoToAdd.keySet()) {

        CSLeafQueueUserInfo csLeafQueueUser = csLeafQueueUserInfoToAdd.get(
                username);
        io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo hopCSLeafQueueUserInfo
                = new io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo(
                        username,
                        csLeafQueueUser.getConsumedMemory(),
                        csLeafQueueUser.getConsumedVCores(),
                        csLeafQueueUser.getPendingApplications(),
                        csLeafQueueUser.getActiveApplications()
                );

        toAddQueues.add(hopCSLeafQueueUserInfo);
    }
      QMDA.addAll(toAddQueues);

  }
  }

  public void persistCSLeafQueueUsersToRemove() throws StorageException {
    if (usersToRemove != null && !usersToRemove.isEmpty()) {
      CSLeafQueueUserInfoDataAccess csLQueueDA
              = (CSLeafQueueUserInfoDataAccess) RMStorageFactory.getDataAccess(
                      CSLeafQueueUserInfoDataAccess.class);
      List<io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo> usersToRemoveList
              = new ArrayList<io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo>();
      for (String userName : usersToRemove) {
        usersToRemoveList.add(
                new io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo(
                        userName, 0, 0, 0, 0));
      }

      csLQueueDA.removeAll(usersToRemoveList);
    }
  }

  public void addCSQueue(String csQueuePath, CSQueue csQueueAdd) {
    boolean isParent;
        isParent = csQueueAdd.getChildQueues() != null;

        int numContainers = -1;
        if (!isParent) {
          numContainers = ((LeafQueue) csQueueAdd).getNumContainers();
        }

        io.hops.metadata.yarn.entity.capacity.CSQueue hopCSQueue
                = new io.hops.metadata.yarn.entity.capacity.CSQueue(
                        csQueueAdd.getQueueName(),
                        csQueueAdd.getQueuePath(),
                        csQueueAdd.getUsedCapacity(),
                        csQueueAdd.getUsedResources().getMemory(),
                        csQueueAdd.getUsedResources().getVirtualCores(),
                        csQueueAdd.getAbsoluteUsedCapacity(),
                        isParent,
                        numContainers
                );
    csQueueToAdd.put(csQueuePath, hopCSQueue);

  }

  public void addCSLeafUsers(String userName) {
    csLeafQueueUserInfoToAdd.put(userName, new CSLeafQueueUserInfo());
    usersToRemove.remove(userName);
  }

  public void addCSLeafQueueUsersToRemove(String userName) {
    if(csLeafQueueUserInfoToAdd.remove(userName)==null){
      usersToRemove.add(userName);
    }
  }

  public CSLeafQueueUserInfo getCSLeafUserInfo(String userName) {
    if (csLeafQueueUserInfoToAdd.containsKey(userName)) {
      return csLeafQueueUserInfoToAdd.get(userName);
    }

    CSLeafQueueUserInfo userInfo = new CSLeafQueueUserInfo();
    csLeafQueueUserInfoToAdd.put(userName, userInfo);
    usersToRemove.remove(userName);
    return userInfo;
  }

}
