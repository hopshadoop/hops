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
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

public class TestQuotaService {

  private static final Log LOG = LogFactory.getLog(TestQuotaService.class);
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
              10, 11, ContainerExitStatus.SUCCESS, (float) 0.1));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_02_000001",
              10, 11, ContainerExitStatus.ABORTED, (float) 0.1));
      hopContainersLogs.add(new ContainersLogs(
              "container_1450009406746_0001_03_000001",
              10, 110, ContainerExitStatus.CONTAINER_RUNNING_STATE, (float) 0.1));

      final List<YarnProjectsQuota> hopYarnProjectsQuota
              = new ArrayList<YarnProjectsQuota>();
      hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", 50, 0));

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

  public void CheckProject(float credits, float used) throws IOException {

    Map<String, YarnProjectsQuota> hopYarnProjectsQuotaList;

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
      Assert.assertTrue(_ycl.getValue().getProjectid().equalsIgnoreCase(
              "Project07"));
      Assert.assertEquals(credits, _ycl.getValue().getRemainingQuota());
      Assert.assertEquals(used, _ycl.getValue().getTotalUsedQuota());

    }

  }

  public void CheckProjectDailyCost(float used) throws IOException {

    Map<String, YarnProjectsDailyCost> hopYarnProjectsDailyCostList;

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
      Assert.assertTrue(_ypdc.getValue().getProjectName().equalsIgnoreCase(
              "Project07"));
      Assert.assertTrue(_ypdc.getValue().getProjectUser().equalsIgnoreCase(
              "rizvi"));
      Assert.assertEquals(_day, _ypdc.getValue().getDay());
      Assert.assertEquals(used, _ypdc.getValue().getCreditsUsed());

    }
  }

  @Test(timeout = 6000)
  public void TestRecover() throws IOException, Exception {

    // Prepare the scenario
    PrepareScenario();

    // Run the schedulat
    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 100);
    qs.init(conf);
    qs.serviceStart();
    Thread.currentThread().sleep(1000);
    qs.serviceStop();

    CheckProject(20,30);
    CheckProjectDailyCost(30);

  }

  @Test
//        (timeout = 6000)
  public void TestStream() throws Exception {
    int initialCredits = 50;
    int totalCost = 0;
    //prepare database
    final List<ApplicationState> hopApplicationState
            = new ArrayList<ApplicationState>();
    hopApplicationState.add(new ApplicationState(
            "application_1450009406746_0001", new byte[0], "Project07__rizvi",
            "DistributedShell", "FINISHING"));
    final List<YarnProjectsQuota> hopYarnProjectsQuota
            = new ArrayList<YarnProjectsQuota>();
    hopYarnProjectsQuota.add(new YarnProjectsQuota("Project07", initialCredits,
            0));

    LightWeightRequestHandler prepareHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        ApplicationStateDataAccess<ApplicationState> _appState
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
        _appState.addAll(hopApplicationState);

        YarnProjectsQuotaDataAccess<YarnProjectsQuota> _pqDA
                = (YarnProjectsQuotaDataAccess) RMStorageFactory.
                getDataAccess(YarnProjectsQuotaDataAccess.class);
        _pqDA.addAll(hopYarnProjectsQuota);

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();

    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    qs.init(conf);
    qs.serviceStart();
    //add containers
    for (int i = 0; i < 10; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float)0.1));
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some containers
    for (int i = 0; i < 3; i++) {
    List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 5, ContainerExitStatus.SUCCESS,(float) 0.1));
        totalCost+=1;
      }
    qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //checkpoint remaining containers
    for (int i = 3; i < 10; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 10,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some checkpointed containers
    for (int i = 3; i < 6; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 15, ContainerExitStatus.SUCCESS,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //preempt some containers
    for (int i = 6; i < 9; i++) {
      List<ContainersLogs> logs = new ArrayList<ContainersLogs>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainersLogs("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 16, ContainerExitStatus.PREEMPTED,(float) 0.1));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(2000);
    CheckProject(initialCredits - totalCost, totalCost);
    CheckProjectDailyCost(totalCost);
  }

}
