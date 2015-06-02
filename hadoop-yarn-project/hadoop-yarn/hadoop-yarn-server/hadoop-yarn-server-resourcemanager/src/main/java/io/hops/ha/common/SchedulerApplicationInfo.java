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
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.entity.QueueMetrics;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerApp;

/**
 * Contains scheduler specific information about Applications.
 */
public class SchedulerApplicationInfo {

  private static final Log LOG =
      LogFactory.getLog(SchedulerApplicationInfo.class);
  private Map<ApplicationId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication>
      schedulerApplicationsToAdd =
      new HashMap<ApplicationId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication>();
  private List<ApplicationId> applicationsIdToRemove =
      new ArrayList<ApplicationId>();
  private Map<String, FiCaSchedulerAppInfo> fiCaSchedulerAppInfo =
      new HashMap<String, FiCaSchedulerAppInfo>();
  
  public void persist(QueueMetricsDataAccess QMDA) throws StorageException {
    //TODO: The same QueueMetrics (DEFAULT_QUEUE) is persisted with every app. Its extra overhead. We can persist it just once
    persistApplicationIdToAdd(QMDA);
    persistApplicationIdToRemove();
    persistFiCaSchedulerAppInfo();
  }

  private void persistApplicationIdToAdd(QueueMetricsDataAccess QMDA)
      throws StorageException {
    if (!schedulerApplicationsToAdd.isEmpty()) {
      SchedulerApplicationDataAccess sappDA =
          (SchedulerApplicationDataAccess) RMStorageFactory
              .getDataAccess(SchedulerApplicationDataAccess.class);
      List<QueueMetrics> toAddQueueMetricses = new ArrayList<QueueMetrics>();
      List<SchedulerApplication> toAddSchedulerApp =
          new ArrayList<SchedulerApplication>();
      for (ApplicationId appId : schedulerApplicationsToAdd.keySet()) {
        if (!applicationsIdToRemove.remove(appId)) {
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
              schedulerApplicationToAdd = schedulerApplicationsToAdd.get(appId);
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics
              toAddQM = schedulerApplicationToAdd.getQueue().getMetrics();
          QueueMetrics toAdHopQueueMetrics =
              new QueueMetrics(toAddQM.getQueueName(),
                  toAddQM.getAppsSubmitted(), toAddQM.getAppsRunning(),
                  toAddQM.getAppsPending(), toAddQM.getAppsCompleted(),
                  toAddQM.getAppsKilled(), toAddQM.getAppsFailed(),
                  toAddQM.getAllocatedMB(), toAddQM.getAllocatedVirtualCores(),
                  toAddQM.getAllocatedContainers(),
                  toAddQM.getAggregateContainersAllocated(),
                  toAddQM.getaggregateContainersReleased(),
                  toAddQM.getAvailableMB(), toAddQM.getAvailableVirtualCores(),
                  toAddQM.getPendingMB(), toAddQM.getPendingVirtualCores(),
                  toAddQM.getPendingContainers(), toAddQM.getReservedMB(),
                  toAddQM.getReservedVirtualCores(),
                  toAddQM.getReservedContainers(), toAddQM.getActiveUsers(),
                  toAddQM.getActiveApps(), 0);

          toAddQueueMetricses.add(toAdHopQueueMetrics);


          //Persist SchedulerApplication - Value of applications Map
          LOG.debug("adding scheduler app " + appId.toString());
          SchedulerApplication toAddSchedulerApplication =
              new SchedulerApplication(appId.toString(),
                  schedulerApplicationToAdd.getUser(),
                  schedulerApplicationToAdd.getQueue().getQueueName());
          toAddSchedulerApp.add(toAddSchedulerApplication);
        }
      }
      QMDA.addAll(toAddQueueMetricses);
      sappDA.addAll(toAddSchedulerApp);
    }
  }

  private void persistApplicationIdToRemove() throws StorageException {
    if (!applicationsIdToRemove.isEmpty()) {
      SchedulerApplicationDataAccess sappDA =
          (SchedulerApplicationDataAccess) RMStorageFactory
              .getDataAccess(SchedulerApplicationDataAccess.class);
      List<SchedulerApplication> applicationsToRemove =
          new ArrayList<SchedulerApplication>();
      for (ApplicationId appId : applicationsIdToRemove) {
        LOG.debug("remove scheduler app " + appId.toString());
        applicationsToRemove
            .add(new SchedulerApplication(appId.toString(), null, null));
      }
      sappDA.removeAll(applicationsToRemove);
      //TORECOVER OPT clean the table that depend on this one
    }
  }

  public void setSchedulerApplicationtoAdd(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication schedulerApplicationToAdd,
      ApplicationId applicationIdToAdd) {
    this.schedulerApplicationsToAdd
        .put(applicationIdToAdd, schedulerApplicationToAdd);
  }

  public void setApplicationIdtoRemove(ApplicationId applicationIdToRemove) {
    this.applicationsIdToRemove.add(applicationIdToRemove);
  }

  public FiCaSchedulerAppInfo getFiCaSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    if (fiCaSchedulerAppInfo.get(appAttemptId.toString()) == null) {
      fiCaSchedulerAppInfo
          .put(appAttemptId.toString(), new FiCaSchedulerAppInfo(appAttemptId));
    }
    return fiCaSchedulerAppInfo.get(appAttemptId.toString());
  }

  private void persistFiCaSchedulerAppInfo() throws StorageException {
    for (FiCaSchedulerAppInfo appInfo : fiCaSchedulerAppInfo.values()) {
      appInfo.persist();
    }
  }

  public void setFiCaSchedulerAppInfo(
      SchedulerApplicationAttempt schedulerApp) {
    FiCaSchedulerAppInfo ficaInfo = new FiCaSchedulerAppInfo(schedulerApp);
    fiCaSchedulerAppInfo
        .put(schedulerApp.getApplicationAttemptId().toString(), ficaInfo);
  }

}
