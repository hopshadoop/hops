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
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author rizvi
 */
public class TestQuotaSchedulerService {

  private static final Log LOG = LogFactory.getLog(
          TestQuotaSchedulerService.class);
  private StorageConnector connector = null;

  @Before
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE);
    LOG.info("DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
    LOG.info("DFS_STORAGE_DRIVER_CLASS : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS);
    LOG.info("DFS_STORAGE_DRIVER_CLASS_DEFAULT : "
            + YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT);

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }


  public void PrepareScenario() throws StorageException, IOException {
    LOG.info("--- START: TestContainerUsage ---");
    LOG.info("--- Checking ContainerStatus ---");

    try {

      final List<RMNode> hopRMNode = new ArrayList<RMNode>();
      hopRMNode.add(new RMNode("Andromeda3:51028"));


      final List<ApplicationState> hopApplicationState
              = new ArrayList<ApplicationState>();
      hopApplicationState.add(new ApplicationState(
              "application_1450009406746_0001", new byte[0], "Project07__rizvi",
              "DistributedShell", "FINISHING"));


      final List<ContainersLogs> hopContainersLogs
              = new ArrayList<ContainersLogs>();
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_01_000001",
              10, 11, ContainerExitStatus.SUCCESS));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_02_000001",
              10, 11, ContainerExitStatus.ABORTED));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_03_000001",
              10, 11, ContainerExitStatus.CONTAINER_RUNNING_STATE));

      final List<YarnProjectsQuota> hopYarnProjectsQuota
              = new ArrayList<YarnProjectsQuota>();
      hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", 50, 0));

      long _miliSec = System.currentTimeMillis();
      final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);
      final List<YarnProjectsDailyCost> hopYarnProjectsDailyCost
              = new ArrayList<YarnProjectsDailyCost>();
      hopYarnProjectsDailyCost.add(new YarnProjectsDailyCost("Project07",
              "rizvi", _day, 0));

      LightWeightRequestHandler bomb;
      bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          RMNodeDataAccess _rmDA = (RMNodeDataAccess) RMStorageFactory.
                  getDataAccess(RMNodeDataAccess.class);
          _rmDA.addAll(hopRMNode);


          ApplicationStateDataAccess<ApplicationState> _appState
                  = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                          ApplicationStateDataAccess.class);
          _appState.addAll(hopApplicationState);


          ContainersLogsDataAccess<ContainersLogs> _clDA
                  = (ContainersLogsDataAccess) RMStorageFactory.
                  getDataAccess(ContainersLogsDataAccess.class);
          _clDA.addAll(hopContainersLogs);

          YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                  = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                  getDataAccess(YarnProjectsQuotaDataAccess.class);
          _pqDA.addAll(hopYarnProjectsQuota);

          YarnProjectsDailyCostDataAccess<YarnProjectsDailyCost> _pdcDA
                  = (YarnProjectsDailyCostDataAccess) RMStorageFactory.
                  getDataAccess(YarnProjectsDailyCostDataAccess.class);
          _pdcDA.addAll(hopYarnProjectsDailyCost);

          connector.commit();
          return null;
        }
      };
      bomb.handle();

    } catch (StorageInitializtionException ex) {
      //LOG.error(ex);
    } catch (StorageException ex) {
      //LOG.error(ex);
    }
  }

  public boolean CheckProject() {

    Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList;

    try {

      LightWeightRequestHandler bomb;
      bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                  = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                  getDataAccess(YarnProjectsQuotaDataAccess.class);
          Map<String, YarnProjectsQuota> _hopYarnProjectsQuotaList = _pqDA.
                  getAll();

          connector.commit();
          return _hopYarnProjectsQuotaList;
        }
      };
      hopYarnProjectsQuotaList = (Map<String, YarnProjectsQuota>) bomb.handle();

      for (Map.Entry<String, YarnProjectsQuota> _ycl : hopYarnProjectsQuotaList.
              entrySet()) {
        if (_ycl.getValue().getProjectid().equalsIgnoreCase("Project07")
                && _ycl.getValue().getRemainingQuota() == 48 && _ycl.getValue().
                getTotalUsedQuota() == 2) {
          //LOG.info("Projects : " + _ycl.getValue().toString());
          return true;
        }

      }

    } catch (Exception e) {
      LOG.error(e, e);
    }
    return false;
  }

  public boolean CheckProjectDailyCost() {

    Map<String, YarnProjectsDailyCost> hopYarnProjectsDailyCostList;

    try {

      LightWeightRequestHandler bomb;
      bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          YarnProjectsDailyCostDataAccess _pdcDA
                  = (YarnProjectsDailyCostDataAccess) RMStorageFactory.
                  getDataAccess(YarnProjectsDailyCostDataAccess.class);
          Map<String, YarnProjectsDailyCost> hopYarnProjectsDailyCostList
                  = _pdcDA.getAll();

          connector.commit();
          return hopYarnProjectsDailyCostList;
        }
      };

      hopYarnProjectsDailyCostList = (Map<String, YarnProjectsDailyCost>) bomb.
              handle();
      long _miliSec = System.currentTimeMillis();
      final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);

      for (Map.Entry<String, YarnProjectsDailyCost> _ypdc
              : hopYarnProjectsDailyCostList.entrySet()) {
        if (_ypdc.getValue().getProjectName().equalsIgnoreCase("Project07")
                && _ypdc.getValue().getProjectUser().equalsIgnoreCase("rizvi")
                && _ypdc.getValue().getDay() == _day && _ypdc.getValue().
                getCreditsUsed() == 2) {
          //LOG.info("Projects : " + _ycl.getValue().toString());
          return true;
        }

      }

    } catch (Exception e) {
    }
    return false;
  }

  @Test
  public void TestPrimaryOperation() {

    try {
      // Prepare the scenario
      PrepareScenario();

      // Run the schedulat
      QuotaSchedulerService qs = new QuotaSchedulerService();
      qs.serviceStart();
      Thread.currentThread().sleep(9000);
      qs.serviceStop();

      Assert.assertTrue(
              "Schedulers primary operation failed. Inconsistent data in YarnProjectsQuota",
              CheckProject());
      Assert.assertTrue(
              "Schedulars primary operation failed. Inconsistent data in YarnProjectsDailyCost",
              CheckProjectDailyCost());

    } catch (Exception ex) {
      Logger.getLogger(TestQuotaSchedulerService.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

}
