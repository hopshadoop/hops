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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueuesPendingAppsDataAccess;
import io.hops.metadata.yarn.entity.capacity.LeafQueuePendingApp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

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
  private Map<String, CSLeafQueueUserInfo> csLeafQueueUserInfoToAdd =
          new HashMap<String, CSLeafQueueUserInfo>();
  private Set<String> usersToRemove = new HashSet<String>();
  private Map<String, LeafQueuePendingApp> csLeafQueuePendingAppToAdd
          = new HashMap<String, LeafQueuePendingApp>();
  private Map<String, LeafQueuePendingApp> csLeafQueuePendingAppToRemove
          = new HashMap<String, LeafQueuePendingApp>();

  public void persist(
          StorageConnector connector) throws StorageException {
    persistCSLeafQueuesPendingAppToAdd();
    connector.flush();
    persistCSLeafQueuesPendingAppToRemove();
    connector.flush();
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

  public void addCSLeafPendingApp(FiCaSchedulerApp application, String queuePath) {
    csLeafQueuePendingAppToAdd.put(application.getApplicationAttemptId().
            toString(),
            new LeafQueuePendingApp(application.getApplicationAttemptId().
                    toString(),
                    queuePath));
  }

  public void removeCSLeafPendingApp(FiCaSchedulerApp application) {
    if (csLeafQueuePendingAppToAdd.remove(application.getApplicationAttemptId().
            toString())
            == null) {
      csLeafQueuePendingAppToRemove.put(application.getApplicationAttemptId().
              toString(),
              new LeafQueuePendingApp(application.getApplicationAttemptId().
                      toString()));
    }
  }

  private void persistCSLeafQueuesPendingAppToAdd() throws StorageException {
    if (!csLeafQueuePendingAppToAdd.isEmpty()) {
      CSLeafQueuesPendingAppsDataAccess DA
              = (CSLeafQueuesPendingAppsDataAccess) RMStorageFactory.
              getDataAccess(
                      CSLeafQueuesPendingAppsDataAccess.class);
      DA.addAll(csLeafQueuePendingAppToAdd.values());
    }
  }

  private void persistCSLeafQueuesPendingAppToRemove() throws StorageException {
    if (!csLeafQueuePendingAppToRemove.isEmpty()) {
      CSLeafQueuesPendingAppsDataAccess DA
              = (CSLeafQueuesPendingAppsDataAccess) RMStorageFactory.
              getDataAccess(
                      CSLeafQueuesPendingAppsDataAccess.class);
      DA.removeAll(csLeafQueuePendingAppToRemove.values());
    }
  }
}
