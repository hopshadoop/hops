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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerCheckPoint;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyId;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.HopsWorksHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
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

  private Thread quotaSchedulingThread;
  private volatile boolean stopped = false;
  private long ticksPerCredit = 1;
  private long minNumberOfTicks = 1;
  private static final Log LOG = LogFactory.getLog(QuotaService.class);

  ApplicationStateDataAccess appStatDS
          = (ApplicationStateDataAccess) RMStorageFactory.
          getDataAccess(ApplicationStateDataAccess.class);
  Map<String, String> applicationStateCache = new HashMap<String, String>();
  Map<String, Long> containersCheckPoints;
  Set<String> recovered = new HashSet<String>();

  BlockingQueue<ContainersLogs> eventContainersLogs
          = new LinkedBlockingQueue<ContainersLogs>();

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
    LOG.info("Stopping the quota schedular service.");
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    ticksPerCredit=conf.getInt(YarnConfiguration.QUOTAS_TICKS_PER_CREDIT, 
            YarnConfiguration.DEFAULT_QUOTAS_TICKS_PER_CREDIT);
    minNumberOfTicks=conf.getInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 
            YarnConfiguration.DEFAULT_QUOTAS_MIN_TICKS_CHARGE);
  }

  public void insertEvents(Collection<ContainersLogs> containersLogs) {
    for (ContainersLogs cl : containersLogs) {
      eventContainersLogs.add(cl);
    }
  }

  private class WorkingThread implements Runnable {

    @Override
    public void run() {
      LOG.info("Quota Scheduler started");

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {

          final List<ContainersLogs> hopContainersLogs
                  = new ArrayList<ContainersLogs>();
          Long start = System.currentTimeMillis();
          long duration = 0;
          ContainersLogs log = null;
          do {
            try {
              log = eventContainersLogs.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
              LOG.error(ex);
            }
            if (log != null) {
              hopContainersLogs.add(log);
            }
            duration = System.currentTimeMillis() - start;
            //TODO put 1000 and 100 in the config file.
          } while (duration < 1000 && hopContainersLogs.size() < 100);

          start = System.currentTimeMillis();
          computeAndApplyCharge(hopContainersLogs, false);
          duration = System.currentTimeMillis() - start;
          LOG.debug("RIZ: " + duration);
        } catch (IOException ex) {
          LOG.error(ex, ex);
        }
      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void computeAndApplyCharge(
          final Collection<ContainersLogs> hopContainersLogs,
          final boolean isRecover)
          throws IOException {
    LightWeightRequestHandler quotaSchedulerHandler
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
                      @Override
                      public Object performTask() throws
                      IOException {
                        connector.beginTransaction();
                        connector.writeLock();

                        //Get Data  ** YarnProjectsQuota **
                        YarnProjectsQuotaDataAccess _pqDA
                        = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                        getDataAccess(
                                YarnProjectsQuotaDataAccess.class);
                        Map<String, YarnProjectsQuota> hopYarnProjectsQuotaMap
                        = _pqDA.getAll();
                        long _miliSec = System.currentTimeMillis();
                        final long _day = TimeUnit.DAYS.convert(
                                _miliSec,
                                TimeUnit.MILLISECONDS);
                        Map<String, YarnProjectsQuota> chargedYarnProjectsQuota
                        = new HashMap<String, YarnProjectsQuota>();
                        Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost
                        = new HashMap<YarnProjectsDailyId, YarnProjectsDailyCost>();

                        List<ContainersLogs> toBeRemovedContainersLogs
                        = new ArrayList<ContainersLogs>();
                        List<ContainerCheckPoint> toBeAddedContainerCheckPoint
                        = new ArrayList<ContainerCheckPoint>();
                        List<ContainerCheckPoint> toBeRemovedContainerCheckPoint
                        = new ArrayList<ContainerCheckPoint>();

                        // Calculate the quota 
                        LOG.debug("RIZ:: ContainersLogs count : "
                                + hopContainersLogs.size());
                        for (ContainersLogs _ycl : hopContainersLogs) {
                          if (!isRecover && recovered.remove(_ycl.
                                  getContainerid())) {
                            continue;
                          }
                          // Get ApplicationId from ContainerId
                          LOG.debug(
                                  "RIZ:: ContainersLogs entry : "
                                  + _ycl.toString());
                          ContainerId _cId = ConverterUtils.toContainerId(_ycl.
                                  getContainerid());
                          ApplicationId _appId = _cId.getApplicationAttemptId().
                          getApplicationId();

                          //Get ProjectId from ApplicationId in ** ApplicationState Table ** 
                          String _appUser = applicationStateCache.get(_appId.
                                  toString());
                          if (_appUser == null) {
                            ApplicationState _appStat
                            = (ApplicationState) appStatDS.findByApplicationId(
                                    _appId.toString());
                            if (_appStat == null) {
                              LOG.error("Application not found: " + _appId.
                                      toString() + " for container " + _ycl.
                                      getContainerid());
                              continue;
                            } else {
                              if (applicationStateCache.size()
                              > 100000) {
                                applicationStateCache
                                = new HashMap<String, String>();
                              }
                              _appUser = _appStat.getUser();
                              applicationStateCache.put(_appId.
                                      toString(),
                                      _appUser);
                            }
                          }

                          String _projectName = HopsWorksHelper.
                          getProjectName(_appUser);
                          String _user = HopsWorksHelper.
                          getUserName(
                                  _appUser);
                          LOG.debug("RIZ:: App : " + _appId.
                                  toString()
                                  + " User : " + _appUser);

                          // Calculate the charge
                          long totalTicks = _ycl.getStop() - _ycl.getStart();

                          Long checkpoint = containersCheckPoints.get(_ycl.
                                  getContainerid());
                          if (checkpoint == null) {
                            checkpoint =  _ycl.getStart();
                          }
                          long currentTicks = _ycl.getStop() - checkpoint;
                          
                          
                          // Decide what to do with the charge
                          if (currentTicks > 0) {
                            if (_ycl.getExitstatus()
                            == ContainerExitStatus.CONTAINER_RUNNING_STATE) {
                              //>> Edit log entry + Increase Quota
                              containersCheckPoints.put(_ycl.
                                      getContainerid(), _ycl.
                                      getStop());
                              toBeAddedContainerCheckPoint.add(
                                      new ContainerCheckPoint(
                                              _ycl.
                                              getContainerid(),
                                              _ycl.
                                              getStop()));

                              LOG.debug(
                                      "charging project still running "
                                      + _projectName
                                      + " for container "
                                      + _ycl.getContainerid()
                                      + " current ticks "
                                      + currentTicks + "(" + _ycl.
                                      getStart()
                                      + ", " + _ycl.getStop()
                                      + ", "
                                      + checkpoint + ")");
                              
                              chargeYarnProjectsQuota(
                                      chargedYarnProjectsQuota,
                                      hopYarnProjectsQuotaMap,
                                      _projectName, _user,
                                      currentTicks, _ycl.getContainerid(),
                                      _ycl.getExitstatus());

                              //** YarnProjectsDailyCost charging**
                              chargeYarnProjectsDailyCost(
                                      chargedYarnProjectsDailyCost,
                                      _projectName, _user, _day,
                                      currentTicks);

                            } else {
                              //>> Delete log entry + Increase Quota                                   
                              toBeRemovedContainersLogs.add(
                                      (ContainersLogs) _ycl);
                              if (isRecover) {
                                recovered.add(_ycl.getContainerid());
                              }
                              if (checkpoint != _ycl.getStart()) {
                                toBeRemovedContainerCheckPoint.add(
                                        new ContainerCheckPoint(_ycl.
                                                getContainerid(), checkpoint));
                                containersCheckPoints.remove(_ycl.
                                        getContainerid());
                              }
                              //** YarnProjectsQuota charging**
                              LOG.debug(
                                      "charging project finished "
                                      + _projectName
                                      + " for container "
                                      + _ycl.getContainerid()
                                      + " charge "
                                      + currentTicks);
                              chargeYarnProjectsQuota(
                                      chargedYarnProjectsQuota,
                                      hopYarnProjectsQuotaMap,
                                      _projectName, _user,
                                      currentTicks, _ycl.getContainerid(),
                                      _ycl.getExitstatus());

                              //** YarnProjectsDailyCost charging**
                              chargeYarnProjectsDailyCost(
                                      chargedYarnProjectsDailyCost,
                                      _projectName, _user, _day,
                                      currentTicks);

                            }
                          }
                        }
                        // Delet the finished ContainersLogs
                        ContainersLogsDataAccess _csDA
                        = (ContainersLogsDataAccess) RMStorageFactory.
                        getDataAccess(
                                ContainersLogsDataAccess.class);
                        _csDA.removeAll(toBeRemovedContainersLogs);

                        //Add and remove Containers checkpoints 
                        ContainersCheckPointsDataAccess ccpDA
                        = (ContainersCheckPointsDataAccess) RMStorageFactory.
                        getDataAccess(
                                ContainersCheckPointsDataAccess.class);
                        ccpDA.addAll(toBeAddedContainerCheckPoint);
                        ccpDA.removeAll(
                                toBeRemovedContainerCheckPoint);

                        // Show all charged project
                        if (LOG.isDebugEnabled()) {
                          for (YarnProjectsQuota _cpq
                          : chargedYarnProjectsQuota.values()) {
                            LOG.debug("RIZ:: Charged projects: "
                                    + _cpq.
                                    toString() + " charge amount:"
                                    + _cpq.
                                    getTotalUsedQuota());
                          }
                        }

                        // Add all the changed project quotato NDB
                        _pqDA.addAll(chargedYarnProjectsQuota.
                                values());
                        YarnProjectsDailyCostDataAccess _pdcDA
                        = (YarnProjectsDailyCostDataAccess) RMStorageFactory.
                        getDataAccess(
                                YarnProjectsDailyCostDataAccess.class);
                        _pdcDA.addAll(
                                chargedYarnProjectsDailyCost.
                                values());
                        connector.commit();
                        return null;
                      }

                    };
            quotaSchedulerHandler.handle();
  }

  Map<YarnProjectsDailyId, YarnProjectsDailyCost> projectsDailyCostCache;
  long cashDay = -1;

  private void chargeYarnProjectsDailyCost(
          Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost,
          String _projectid, String _user, long _day, long ticks) {
    long charge = computeCharge(ticks);
    LOG.debug("Quota: project " + _projectid + " user " + _user + " has used "
            + charge + " credits");
    if (cashDay != _day) {
      projectsDailyCostCache
              = new HashMap<YarnProjectsDailyId, YarnProjectsDailyCost>();
      cashDay = _day;
    }

    YarnProjectsDailyId _key = new YarnProjectsDailyId(_projectid, _user, _day);
    YarnProjectsDailyCost _tempPdc
            = projectsDailyCostCache.get(_key);

    if (_tempPdc == null) {
      _tempPdc = new YarnProjectsDailyCost(_projectid, _user, _day, 0);
      projectsDailyCostCache.put(_key, _tempPdc);
    }

    YarnProjectsDailyCost _incrementedPdc = new YarnProjectsDailyCost(
            _projectid, _user, _day, _tempPdc.getCreditsUsed() + (int) charge);
    chargedYarnProjectsDailyCost.put(_key, _incrementedPdc);
    projectsDailyCostCache.put(_key, _incrementedPdc);
  }

  private void chargeYarnProjectsQuota(
          Map<String, YarnProjectsQuota> chargedYarnProjectsQuota,
          Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList,
          String _projectid,  String _user, long ticks, String containerId, 
          int exitStatus) {
    long charge = computeCharge(ticks);
    LOG.info("Quota: project " + _projectid + " user " + _user + " has used "
            + charge
            + " credits (container: " + containerId + ", exit status: "
            + exitStatus + ")");
    YarnProjectsQuota _tempPq = (YarnProjectsQuota) hopYarnProjectsQuotaList.
            get(_projectid);
    if (_tempPq != null) {
      YarnProjectsQuota _modifiedPq = new YarnProjectsQuota(_projectid,
              _tempPq.getRemainingQuota() - (int) charge,
              _tempPq.getTotalUsedQuota() + (int) charge);

      chargedYarnProjectsQuota.put(_projectid, _modifiedPq);
      hopYarnProjectsQuotaList.put(_projectid, _modifiedPq);
    } else {
      LOG.error("Project not found: " + _projectid);
    }

  }
  
  private long computeCharge(long ticks){
    if(ticks<minNumberOfTicks){
      ticks=minNumberOfTicks;
    }
    long charge = ticks/ticksPerCredit;
    if(ticks%ticksPerCredit!=0){
      charge++;
    }
    return charge;
  }
  
  public void recover() {

    long _miliSec = System.currentTimeMillis();
    final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);
    try {
      LightWeightRequestHandler recoveryHandler
              = new LightWeightRequestHandler(
                      YARNOperationType.TEST) {
                        @Override
                        public Object performTask() throws IOException {
                          connector.beginTransaction();
                          connector.writeLock();
                          YarnProjectsDailyCostDataAccess _pdcDA
                          = (YarnProjectsDailyCostDataAccess) RMStorageFactory.
                          getDataAccess(YarnProjectsDailyCostDataAccess.class);
                          projectsDailyCostCache = _pdcDA.getByDay(_day);

                          ContainersCheckPointsDataAccess ccpDA
                          = (ContainersCheckPointsDataAccess) RMStorageFactory.
                          getDataAccess(ContainersCheckPointsDataAccess.class);
                          containersCheckPoints = ccpDA.getAll();
                          connector.commit();
                          return null;
                        }
                      };
              recoveryHandler.handle();

              //getAll
              LightWeightRequestHandler logsHandler
                      = new LightWeightRequestHandler(
                              YARNOperationType.TEST) {
                                @Override
                                public Object performTask() throws IOException {
                                  connector.beginTransaction();
                                  connector.readLock();

                                  //Get Data  ** ContainersLogs **
                                  ContainersLogsDataAccess _csDA
                                  = (ContainersLogsDataAccess) RMStorageFactory.
                                  getDataAccess(ContainersLogsDataAccess.class);
                                  Map<String, ContainersLogs> hopContainersLogs
                                  = _csDA.getAll();
                                  connector.commit();
                                  return hopContainersLogs;
                                }

                              };
                      final Map<String, ContainersLogs> hopContainersLogs
                              = (Map<String, ContainersLogs>) logsHandler.
                              handle();
                      //run logic on all
                      computeAndApplyCharge(hopContainersLogs.values(), true);

    } catch (StorageException ex) {
      LOG.error(ex, ex);
    } catch (IOException ex) {
      LOG.error(ex, ex);
    }
  }

}
