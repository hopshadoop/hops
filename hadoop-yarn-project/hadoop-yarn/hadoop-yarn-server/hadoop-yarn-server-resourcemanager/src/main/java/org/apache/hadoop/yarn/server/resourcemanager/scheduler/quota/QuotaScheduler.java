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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyId;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.HopsWorksHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import io.hops.metadata.yarn.entity.ContainerCheckPoint;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rizvi
 */
public class QuotaScheduler implements Runnable {

  private static final Log LOG = LogFactory.getLog(QuotaScheduler.class);
  private StorageConnector connector = null;
  ApplicationStateDataAccess appStatDS = null;
  Map<String, String> applicationStateCache = null;
  Map<String, Long> containersCheckPoints;
  boolean stopped = false;
  long frequency;

  public void init(Configuration conf) {
    frequency = conf.getLong(YarnConfiguration.QUOTAS_MONITOR_INTERVAL,
            YarnConfiguration.DEFAULT_QUOTAS_MONITOR_INTERVAL);
  }

  @Override
  public void run() {
    LOG.info("Quota Scheduler started");

    //Get DataAccess and Map for ** ApplicationState **
    this.appStatDS = (ApplicationStateDataAccess) RMStorageFactory.
            getDataAccess(ApplicationStateDataAccess.class);
    this.applicationStateCache = new HashMap<String, String>();

    while (!stopped && !Thread.currentThread().isInterrupted()) {
      try {

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
                        = (Map<String, ContainersLogs>) logsHandler.handle();

                LightWeightRequestHandler quotaSchedulerHandler
                        = new LightWeightRequestHandler(
                                YARNOperationType.TEST) {
                                  @Override
                                  public Object performTask() throws IOException {
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
                                    for (ContainersLogs _ycl
                                    : hopContainersLogs.values()) {

                                      // Get ApplicationId from ContainerId
                                      LOG.debug("RIZ:: ContainersLogs entry : "
                                              + _ycl.toString());
                                      ContainerId _cId = ConverterUtils.
                                      toContainerId(
                                              _ycl.getContainerid());
                                      ApplicationId _appId = _cId.
                                      getApplicationAttemptId().
                                      getApplicationId();

                                      //Get ProjectId from ApplicationId in ** ApplicationState Table ** 
                                      String _appUser = applicationStateCache.
                                      get(
                                              _appId.toString());
                                      if (_appUser == null) {
                                        ApplicationState _appStat
                                        = (ApplicationState) appStatDS.
                                        findByApplicationId(_appId.toString());
                                        if (_appStat == null) {
                                          LOG.error("Application not found: "
                                                  + _appId.
                                                  toString() + " for container "
                                                  + _ycl.
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
                                              _projectName);
                                      LOG.debug("RIZ:: App : " + _appId.
                                              toString()
                                              + " User : " + _appUser);

                                      // Calculate the charge
                                      long _charge = _ycl.getStop() - _ycl.
                                      getStart();

                                      Long checkpoint = containersCheckPoints.
                                      get(
                                              _ycl.getContainerid());
                                      if (checkpoint != null) {
                                        _charge = _ycl.getStop() - checkpoint;
                                      }

                                      // Decide what to do with the charge
                                      if (_charge > 0) {
                                        if (_ycl.getExitstatus()
                                        == ContainerExitStatus.CONTAINER_RUNNING_STATE) {
                                          //>> Edit log entry + Increase Quota
                                          containersCheckPoints.put(_ycl.
                                                  getContainerid(), _ycl.
                                                  getStop());
                                          toBeAddedContainerCheckPoint.add(
                                                  new ContainerCheckPoint(_ycl.
                                                          getContainerid(),
                                                          _ycl.
                                                          getStop()));

                                          LOG.debug(
                                                  "charging project still running "
                                                  + _projectName
                                                  + " for container "
                                                  + _ycl.getContainerid()
                                                  + " charge "
                                                  + _charge + "(" + _ycl.
                                                  getStart()
                                                  + ", " + _ycl.getStop() + ", "
                                                  + checkpoint + ")");
                                          chargeYarnProjectsQuota(
                                                  chargedYarnProjectsQuota,
                                                  hopYarnProjectsQuotaMap,
                                                  _projectName,
                                                  _charge);

                                          //** YarnProjectsDailyCost charging**
                                          chargeYarnProjectsDailyCost(
                                                  chargedYarnProjectsDailyCost,
                                                  _projectName, _user, _day,
                                                  _charge);

                                        } else if (_ycl.getExitstatus()
                                        == ContainerExitStatus.ABORTED || _ycl.
                                        getExitstatus()
                                        == ContainerExitStatus.DISKS_FAILED
                                        || _ycl.getExitstatus()
                                        == ContainerExitStatus.PREEMPTED) {
                                          LOG.warn(
                                                  "Quota: container finished unexpectidely: "
                                                  + _ycl.getContainerid()
                                                  + " exit status " + _ycl.
                                                  getExitstatus()
                                                  + " should have costed "
                                                  + _charge);
                                          //>> Delete log entry
                                          toBeRemovedContainersLogs.add(
                                                  (ContainersLogs) _ycl);
                                          if (checkpoint != null) {
                                            toBeRemovedContainerCheckPoint.add(
                                                    new ContainerCheckPoint(
                                                            _ycl.
                                                            getContainerid(),
                                                            checkpoint));
                                            containersCheckPoints.remove(_ycl.
                                                    getContainerid());
                                          }
                                        } else {
                                          //>> Delete log entry + Increase Quota                                   
                                          toBeRemovedContainersLogs.add(
                                                  (ContainersLogs) _ycl);
                                          if (checkpoint != null) {
                                            toBeRemovedContainerCheckPoint.add(
                                                    new ContainerCheckPoint(
                                                            _ycl.
                                                            getContainerid(),
                                                            checkpoint));
                                            containersCheckPoints.remove(_ycl.
                                                    getContainerid());
                                          }
                                          //** YarnProjectsQuota charging**
                                          LOG.debug("charging project finished "
                                                  + _projectName
                                                  + " for container "
                                                  + _ycl.getContainerid()
                                                  + " charge "
                                                  + _charge);
                                          chargeYarnProjectsQuota(
                                                  chargedYarnProjectsQuota,
                                                  hopYarnProjectsQuotaMap,
                                                  _projectName,
                                                  _charge);

                                          //** YarnProjectsDailyCost charging**
                                          chargeYarnProjectsDailyCost(
                                                  chargedYarnProjectsDailyCost,
                                                  _projectName, _user, _day,
                                                  _charge);

                                        }
                                      }
                                    }
                                    // Delet the finished ContainersLogs
                                    ContainersLogsDataAccess _csDA
                                    = (ContainersLogsDataAccess) RMStorageFactory.
                                    getDataAccess(ContainersLogsDataAccess.class);
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
                                    _pdcDA.addAll(chargedYarnProjectsDailyCost.
                                            values());
                                    connector.commit();
                                    return null;
                                  }

                                };
                        long start = System.currentTimeMillis();
                        quotaSchedulerHandler.handle();
                        long duration = System.currentTimeMillis() - start;
                        LOG.debug("RIZ: " + duration);
                        Thread.sleep(Math.max(0, frequency - duration));
      } catch (InterruptedException ex) {
        //Logger.getLogger(QuotaScheduler.class.getName()).log(Level.SEVERE, null, ex);
        LOG.
                error("Schedular thread is exiting with exception" + ex.
                        getMessage());
      } catch (StorageInitializtionException ex) {
        LOG.error(ex, ex);
      } catch (StorageException ex) {
        LOG.error(ex, ex);
      } catch (IOException ex) {
        LOG.error(ex, ex);
      }
    }
    LOG.info("Quota scheduler thread is exiting gracefully");
  }

  Map<YarnProjectsDailyId, YarnProjectsDailyCost> projectsDailyCostCache;
  long cashDay = -1;

  private void chargeYarnProjectsDailyCost(
          Map<YarnProjectsDailyId, YarnProjectsDailyCost> chargedYarnProjectsDailyCost,
          String _projectid, String _user, long _day, long _charge) {
    LOG.info("Quota: project " + _projectid + " user " + _user + " has used "
            + _charge);
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
            _projectid, _user, _day, _tempPdc.getCreditsUsed() + (int) _charge);
    chargedYarnProjectsDailyCost.put(_key, _incrementedPdc);
    projectsDailyCostCache.put(_key, _incrementedPdc);
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
    } catch (StorageException ex) {
      LOG.error(ex, ex);
    } catch (IOException ex) {
      LOG.error(ex, ex);
    }
  }

  private void chargeYarnProjectsQuota(
          Map<String, YarnProjectsQuota> chargedYarnProjectsQuota,
          Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList,
          String _projectid, long _charge) {
    LOG.info("Quota: project " + _projectid + " has used " + _charge);
    YarnProjectsQuota _tempPq = (YarnProjectsQuota) hopYarnProjectsQuotaList.
            get(_projectid);
    if (_tempPq != null) {
      YarnProjectsQuota _modifiedPq = new YarnProjectsQuota(_projectid,
              _tempPq.getRemainingQuota() - (int) _charge,
              _tempPq.getTotalUsedQuota() + (int) _charge);

      chargedYarnProjectsQuota.put(_projectid, _modifiedPq);
      hopYarnProjectsQuotaList.put(_projectid, _modifiedPq);
    } else {
      LOG.error("Project not found: " + _projectid);
    }

  }

  public void stop() {
    stopped = true;
  }
}
