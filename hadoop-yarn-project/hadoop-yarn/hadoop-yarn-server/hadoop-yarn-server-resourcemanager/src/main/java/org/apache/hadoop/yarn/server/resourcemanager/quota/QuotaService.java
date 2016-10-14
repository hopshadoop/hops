/*
 * Copyright 2015 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.quota.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.dal.quota.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.ContainerCheckPoint;
import io.hops.metadata.yarn.entity.quota.ContainerLog;
import io.hops.metadata.yarn.entity.quota.ProjectDailyCost;
import io.hops.metadata.yarn.entity.quota.ProjectDailyId;
import io.hops.metadata.yarn.entity.quota.ProjectQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.HopsWorksHelper;
import io.hops.util.RMStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class QuotaService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(QuotaService.class);

  private Thread quotaSchedulingThread;
  private volatile boolean stopped = false;
  private long minNumberOfTicks = 1;
  private long batchTime;
  private int batchSize;
  private int minVcores;
  private int minMemory;
  private float basePrice;
  
  ApplicationStateDataAccess appStatDS
          = (ApplicationStateDataAccess) RMStorageFactory.
          getDataAccess(ApplicationStateDataAccess.class);
  Map<String, String> applicationOwnerCache = new HashMap<>();
  Map<String, ContainerCheckPoint> containersCheckPoints;
  Set<String> recovered = new HashSet<>();

  BlockingQueue<ContainerLog> eventContainersLogs
          = new LinkedBlockingQueue<>();

  public QuotaService() {
    super("quota scheduler service");
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new quota schedular service");
    recover();
    quotaSchedulingThread = new Thread(new WorkingThread());
    quotaSchedulingThread.setName("Quota scheduling service");
    quotaSchedulingThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (quotaSchedulingThread != null) {
      quotaSchedulingThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopped the quota schedular service.");
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    minNumberOfTicks = conf.getInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE,
            YarnConfiguration.DEFAULT_QUOTA_MIN_TICKS_CHARGE);
    batchTime = conf.getLong(YarnConfiguration.QUOTA_BATCH_TIME,
            YarnConfiguration.DEFAULT_QUOTA_BATCH_TIME);
    batchSize = conf.getInt(YarnConfiguration.QUOTA_BATCH_SIZE,
            YarnConfiguration.DEFAULT_QUOTA_BATCH_SIZE);
    minVcores= conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    minMemory= conf.getInt(YarnConfiguration.QUOTA_MINIMUM_CHARGED_MB, YarnConfiguration.DEFAULT_QUOTA_MINIMUM_CHARGED_MB);
    basePrice= conf.getFloat(YarnConfiguration.QUOTA_BASE_PRICE, YarnConfiguration.DEFAULT_QUOTA_BASE_PRICE);
  }

  public void insertEvents(Collection<ContainerLog> containersLogs) {
    for (ContainerLog cl : containersLogs) {
      eventContainersLogs.add(cl);
    }
  }

  private class WorkingThread implements Runnable {

    @Override
    public void run() {
      LOG.info("Quota Scheduler started");

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try{
        final List<ContainerLog> containersLogs = new ArrayList<>();
        Long start = System.currentTimeMillis();
        long duration = 0;
        //batch logs to reduce the number of roundtrips to the database
        //can probably be removed once we have the ndb asynchronous library 
        do {
          ContainerLog log = eventContainersLogs.poll(Math.max(1, batchTime
                  - duration), TimeUnit.MILLISECONDS);
          if (log != null) {
            containersLogs.add(log);
          }
          duration = System.currentTimeMillis() - start;
        } while (duration < batchTime && containersLogs.size() < batchSize);

        computeAndApplyCharge(containersLogs, false);
        }catch(InterruptedException | IOException ex){
          LOG.error(ex,ex);
        }
      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void computeAndApplyCharge(
          final Collection<ContainerLog> ContainersLogs,
          final boolean isRecover) throws IOException {
    LightWeightRequestHandler quotaSchedulerHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        computeAndApplyChargeInt(ContainersLogs, isRecover);
        connector.commit();
        return null;
      }

    };
    quotaSchedulerHandler.handle();
  }

  private void computeAndApplyChargeInt(
          final Collection<ContainerLog> ContainersLogs,
          final boolean isRecover) throws StorageException {
    //Get Data  ** ProjectQuota **
    ProjectQuotaDataAccess pqDA
            = (ProjectQuotaDataAccess) RMStorageFactory.getDataAccess(
                    ProjectQuotaDataAccess.class);
    Map<String, ProjectQuota> projectsQuotaMap = pqDA.getAll();
    final long curentDay = TimeUnit.DAYS.convert(System.currentTimeMillis(),
            TimeUnit.MILLISECONDS);
    Map<String, ProjectQuota> chargedProjects = new HashMap<>();
    Map<ProjectDailyId, ProjectDailyCost> chargedProjectsDailyCost
            = new HashMap<>();

    List<ContainerLog> toBeRemovedContainersLogs
            = new ArrayList<>();
    List<ContainerCheckPoint> toBePercistedContainerCheckPoint
            = new ArrayList<>();
    List<ContainerCheckPoint> toBeRemovedContainerCheckPoint
            = new ArrayList<>();

    // Calculate the quota
    for (ContainerLog containerLog : ContainersLogs) {
      if (!isRecover && recovered.remove(containerLog.getContainerid())) {
        //we have already charged this project when recovering we should
        //not charge it two times
        continue;
      }
      if (isRecover) {
        recovered.add(containerLog.getContainerid());
      }
      // Get ApplicationId from ContainerId
      ContainerId containerId = 
              ConverterUtils.toContainerId(containerLog.getContainerid());
      ApplicationId appId = containerId.getApplicationAttemptId().
              getApplicationId();

      //Get ProjectId from ApplicationId in ** ApplicationState Table ** 
      String appOwner = applicationOwnerCache.get(appId.toString());
      if (appOwner == null) {
        ApplicationState appState = (ApplicationState) appStatDS.
                findByApplicationId(appId.toString());
        if (appState == null) {
          LOG.error("Application not found: " + appId.toString()
                  + " for container " + containerLog.getContainerid());
          continue;
        } else {
          if (applicationOwnerCache.size() > 100000) {
            //if the cahs is too big empty it and it will be refilled with
            //the active applications
            //TODO make a proper chash
            applicationOwnerCache = new HashMap<>();
          }
          appOwner = appState.getUser();
          applicationOwnerCache.put(appId.toString(), appOwner);
        }
      }

      String projectName = HopsWorksHelper.getProjectName(appOwner);
      String user = HopsWorksHelper.getUserName(appOwner);

      //comput used ticks
      Long checkpoint = containerLog.getStart();
      float currentMultiplicator = containerLog.getMultiplicator();
      ContainerCheckPoint lastCheckPoint = containersCheckPoints.get(
              containerLog.getContainerid());
      if (lastCheckPoint != null) {
        checkpoint = lastCheckPoint.getCheckPoint();
        currentMultiplicator = lastCheckPoint.getMultiplicator();
      }
      long nbRunningTicks = containerLog.getStop() - checkpoint;

      // Decide what to do with the ticks
      if (nbRunningTicks > 0) {
        if (containerLog.getExitstatus()
                == ContainerExitStatus.CONTAINER_RUNNING_STATE) {
          //The container as been running for more than one checkpoint duration
          ContainerCheckPoint newCheckpoint = new ContainerCheckPoint(
                  containerLog.getContainerid(), containerLog.getStop(),
                  currentMultiplicator);
          containersCheckPoints.
                  put(containerLog.getContainerid(), newCheckpoint);
          toBePercistedContainerCheckPoint.add(newCheckpoint);

          LOG.debug("charging project still running " + projectName
                  + " for container " + containerLog.getContainerid()
                  + " current ticks "
                  + nbRunningTicks + "(" + containerLog.getStart() + ", "
                  + containerLog.getStop() + ", " + checkpoint
                  + ") current multiplicator " + currentMultiplicator);

          float charge = computeCharge(nbRunningTicks, currentMultiplicator,
                  containerLog.getNbVcores(), containerLog.getMemoryUsed());
          chargeProjectQuota(chargedProjects, projectsQuotaMap,
                  projectName, user, containerLog.getContainerid(), charge);
          //** ProjectDailyCost charging**
          chargeProjectDailyCost(chargedProjectsDailyCost, projectName,
                  user, curentDay, charge);

        } else {
          //The container has finished running
          toBeRemovedContainersLogs.add((ContainerLog) containerLog);
          if (checkpoint != containerLog.getStart()) {
            toBeRemovedContainerCheckPoint.add(new ContainerCheckPoint(
                    containerLog.getContainerid()));
            containersCheckPoints.remove(containerLog.getContainerid());
          }
          //** ProjectQuota charging**
          LOG.debug("charging project finished " + projectName
                  + " for container " + containerLog.getContainerid()
                  + " current ticks " + nbRunningTicks + " current multiplicator "
                  + currentMultiplicator);
          float charge = computeCharge(nbRunningTicks, currentMultiplicator,
                  containerLog.getNbVcores(), containerLog.getMemoryUsed());
          chargeProjectQuota(chargedProjects, projectsQuotaMap,
                  projectName, user, containerLog.getContainerid(), charge);

          //** ProjectDailyCost charging**
          chargeProjectDailyCost(chargedProjectsDailyCost, projectName,
                  user, curentDay, charge);
        }
      } else if (checkpoint == containerLog.getStart() && containerLog.
              getExitstatus() == ContainerExitStatus.CONTAINER_RUNNING_STATE) {
        //create a checkPoint at start to store multiplicator.
        ContainerCheckPoint newCheckpoint = new ContainerCheckPoint(
                containerLog.getContainerid(), containerLog.getStart(),
                currentMultiplicator);
        containersCheckPoints.put(containerLog.getContainerid(), newCheckpoint);
        toBePercistedContainerCheckPoint.add(newCheckpoint);
      }
    }
    // Delet the finished ContainersLogs
    ContainersLogsDataAccess csDA = (ContainersLogsDataAccess) RMStorageFactory.
            getDataAccess(ContainersLogsDataAccess.class);
    csDA.removeAll(toBeRemovedContainersLogs);

    //Add and remove Containers checkpoints
    ContainersCheckPointsDataAccess ccpDA
            = (ContainersCheckPointsDataAccess) RMStorageFactory.getDataAccess(
                    ContainersCheckPointsDataAccess.class);
    ccpDA.addAll(toBePercistedContainerCheckPoint);
    ccpDA.removeAll(toBeRemovedContainerCheckPoint);

    if (LOG.isDebugEnabled()) {
      // Show all charged project
      for (ProjectQuota _cpq : chargedProjects.values()) {
        LOG.debug("RIZ:: Charged projects: " + _cpq.toString()
                + " charge amount:" + _cpq.getTotalUsedQuota());
      }
    }

    // Add all the changed project quota to NDB
    pqDA.addAll(chargedProjects.values());
    ProjectsDailyCostDataAccess pdcDA
            = (ProjectsDailyCostDataAccess) RMStorageFactory.getDataAccess(
                    ProjectsDailyCostDataAccess.class);
    pdcDA.addAll(chargedProjectsDailyCost.values());
  }

  Map<ProjectDailyId, ProjectDailyCost> projectsDailyCostCache;
  long cashDay = -1;

  private void chargeProjectQuota(
          Map<String, ProjectQuota> chargedProjectsQuota,
          Map<String, ProjectQuota> projectsQuotaMap,
          String projectid, String user, String containerId, float charge) {

    LOG.info("Quota: project " + projectid + " user " + user
            + " has been charged " + charge + " for container: " + containerId);

    ProjectQuota projectQuota
            = (ProjectQuota) projectsQuotaMap.get(projectid);
    if (projectQuota != null) {
      projectQuota.decrementQuota(charge);

      chargedProjectsQuota.put(projectid, projectQuota);
    } else {
      LOG.error("Project not found: " + projectid);
    }
  }

  private void chargeProjectDailyCost(
          Map<ProjectDailyId, ProjectDailyCost> chargedProjectsDailyCost,
          String projectid, String user, long day, float charge) {

    LOG.debug("Quota: project " + projectid + " user " + user + " has used "
            + charge + " credits, on day: " + day);
    if (cashDay != day) {
      projectsDailyCostCache = new HashMap<>();
      cashDay = day;
    }

    ProjectDailyId key = new ProjectDailyId(projectid, user, day);
    ProjectDailyCost projectDailyCost = projectsDailyCostCache.get(key);

    if (projectDailyCost == null) {
      projectDailyCost = new ProjectDailyCost(projectid, user, day, 0);
      projectsDailyCostCache.put(key, projectDailyCost);
    }

    projectDailyCost.incrementCharge(charge);

    chargedProjectsDailyCost.put(key, projectDailyCost);

  }

  private float computeCharge(long ticks, float multiplicator, int nbVcores,
          int memoryUsed) {
    if (ticks < minNumberOfTicks) {
      ticks = minNumberOfTicks;
    }
    //the pricePerTick is set for a minimum sized container, the price to pay is
    //proportional to the container size on the most used resource
    float vcoresUsage = (float) nbVcores / minVcores;
    float memoryUsage = (float) memoryUsed / minMemory;
    float credit = (float) ticks * Math.max(vcoresUsage, memoryUsage)
            * multiplicator * basePrice;
    return credit;
  }

  public void recover() throws IOException {

    final long day = TimeUnit.DAYS.convert(System.currentTimeMillis(),
            TimeUnit.MILLISECONDS);
    LightWeightRequestHandler recoveryHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        ProjectsDailyCostDataAccess pdcDA
                = (ProjectsDailyCostDataAccess) RMStorageFactory.
                getDataAccess(ProjectsDailyCostDataAccess.class);
        projectsDailyCostCache = pdcDA.getByDay(day);

        ContainersCheckPointsDataAccess ccpDA
                = (ContainersCheckPointsDataAccess) RMStorageFactory.
                getDataAccess(ContainersCheckPointsDataAccess.class);
        containersCheckPoints = ccpDA.getAll();

        //Get Data  ** ContainersLogs **
        ContainersLogsDataAccess csDA
                = (ContainersLogsDataAccess) RMStorageFactory.getDataAccess(
                        ContainersLogsDataAccess.class);
        Map<String, ContainerLog> hopContainersLogs = csDA.getAll();
        connector.commit();
        return hopContainersLogs;
      }
    };
    final Map<String, ContainerLog> hopContainersLogs = (Map<String, ContainerLog>) recoveryHandler.handle();

    //run logic on all
    computeAndApplyCharge(hopContainersLogs.values(), true);

  }

}
