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
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.SchedulerApplicationInfoToAdd;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Contains scheduler specific information about Applications.
 */
public class SchedulerApplicationInfo {

  private static final Log LOG =
      LogFactory.getLog(SchedulerApplicationInfo.class);
  
  private final TransactionStateImpl transactionState;
  private Map<ApplicationId, SchedulerApplicationInfoToAdd>
      schedulerApplicationsToAdd =
      new HashMap<ApplicationId, SchedulerApplicationInfoToAdd>();
  private Set<ApplicationId> applicationsIdToRemove =
      new HashSet<ApplicationId>();
  Lock fiCaSchedulerAppInfoLock = new ReentrantLock();
  private Map<String, Map<String, FiCaSchedulerAppInfo>> fiCaSchedulerAppInfo =
      new HashMap<String, Map<String, FiCaSchedulerAppInfo>>();
  
  public SchedulerApplicationInfo(TransactionStateImpl transactionState){
    this.transactionState = transactionState;
  }
  
  public void persist(QueueMetricsDataAccess QMDA, StorageConnector connector) throws StorageException {
    //TODO: The same QueueMetrics (DEFAULT_QUEUE) is persisted with every app. Its extra overhead. We can persist it just once
    persistApplicationIdToAdd(QMDA);
    persistFiCaSchedulerAppInfo(connector);
    persistApplicationIdToRemove();
  }

  private void persistApplicationIdToAdd(QueueMetricsDataAccess QMDA)
      throws StorageException {
    if (!schedulerApplicationsToAdd.isEmpty()) {
      SchedulerApplicationDataAccess sappDA =
          (SchedulerApplicationDataAccess) RMStorageFactory
              .getDataAccess(SchedulerApplicationDataAccess.class);
      List<SchedulerApplication> toAddSchedulerApp =
          new ArrayList<SchedulerApplication>();
      for (SchedulerApplicationInfoToAdd appInfo : schedulerApplicationsToAdd.values()) {
        

          LOG.debug("adding scheduler app " + appInfo.getSchedulerApplication().getAppid());


          toAddSchedulerApp.add(appInfo.getSchedulerApplication());
        
      }
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
        applicationsToRemove
            .add(new SchedulerApplication(appId.toString(), null, null));
      }
      sappDA.removeAll(applicationsToRemove);
    }
  }

  public void setSchedulerApplicationtoAdd(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication schedulerApplicationToAdd,
          ApplicationId applicationIdToAdd) {

    //Persist SchedulerApplication - Value of applications Map
    SchedulerApplication toAddSchedulerApplication = new SchedulerApplication(
            applicationIdToAdd.toString(),
            schedulerApplicationToAdd.getUser(),
            schedulerApplicationToAdd.getQueue().getQueueName());

    SchedulerApplicationInfoToAdd appInfo = new SchedulerApplicationInfoToAdd(
            toAddSchedulerApplication);

    this.schedulerApplicationsToAdd
            .put(applicationIdToAdd, appInfo);
    applicationsIdToRemove.remove(applicationIdToAdd);
  }

  public void setApplicationIdtoRemove(ApplicationId applicationIdToRemove) {
    if(schedulerApplicationsToAdd.remove(applicationIdToRemove)==null){
      this.applicationsIdToRemove.add(applicationIdToRemove);
    }
  }

  public FiCaSchedulerAppInfo getFiCaSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    fiCaSchedulerAppInfoLock.lock();
    try{
    ApplicationId appId = appAttemptId.getApplicationId();
    if(fiCaSchedulerAppInfo.get(appId.toString())==null){
      fiCaSchedulerAppInfo.put(appId.toString(), new HashMap<String, FiCaSchedulerAppInfo>());
    }
    if (fiCaSchedulerAppInfo.get(appId.toString()).get(appAttemptId.toString()) == null) {
      Map<String, FiCaSchedulerAppInfo> map = fiCaSchedulerAppInfo.get(appId.toString());
      String appAttemptIdString = appAttemptId.toString();
      FiCaSchedulerAppInfo appInfo = new FiCaSchedulerAppInfo(appAttemptId, transactionState);
      map.put(appAttemptIdString, appInfo);
    }
    Map<String, FiCaSchedulerAppInfo> map = fiCaSchedulerAppInfo.get(appId.toString());
    String appAttemptIdString = appAttemptId.toString();
    return map.get(appAttemptIdString);
    }finally{
      fiCaSchedulerAppInfoLock.unlock();
    }
  }

  private void persistFiCaSchedulerAppInfo(StorageConnector connector) throws StorageException {
    if(!fiCaSchedulerAppInfo.isEmpty()){
    AgregatedAppInfo agregatedAppInfo = new AgregatedAppInfo();
    for (Map<String,FiCaSchedulerAppInfo> map : fiCaSchedulerAppInfo.values()) {
      for(FiCaSchedulerAppInfo appInfo: map.values()){
        appInfo.agregate(agregatedAppInfo);
      }
    }
    agregatedAppInfo.persist();
    }
  }
  
}
