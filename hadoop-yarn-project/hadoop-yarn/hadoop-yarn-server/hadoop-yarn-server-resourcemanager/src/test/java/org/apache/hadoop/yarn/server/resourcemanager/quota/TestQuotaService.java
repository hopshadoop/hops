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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.quota.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.quota.ContainerLog;
import io.hops.metadata.yarn.entity.quota.ProjectDailyCost;
import io.hops.metadata.yarn.entity.quota.ProjectQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestQuotaService {

  private static final Log LOG = LogFactory.getLog(TestQuotaService.class);
  private StorageConnector connector = null;

  @Before
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  public void PrepareScenario() throws StorageException, IOException {
    LOG.info("--- START: TestContainerUsage ---");
    LOG.info("--- Checking ContainerStatus ---");

    try {

      final List<RMNode> hopRMNode = new ArrayList<RMNode>();
      hopRMNode.add(new RMNode("Andromeda3:51028"));

      final ApplicationState hopApplicationState = new ApplicationState(
              "application_1450009406746_0001", new byte[0], "Project07__rizvi",
              "DistributedShell", "FINISHING");

      final List<ContainerLog> hopContainerLog
              = new ArrayList<>();
      hopContainerLog.add(new ContainerLog(
              "container_1450009406746_0001_01_000001",
              10, 11, ContainerExitStatus.SUCCESS, (float) 0.1, 1 ,1024));
      hopContainerLog.add(new ContainerLog(
              "container_1450009406746_0001_02_000001",
              10, 11, ContainerExitStatus.ABORTED, (float) 0.1,1,1024));
      hopContainerLog.add(new ContainerLog(
              "container_1450009406746_0001_03_000001",
              10, 110, ContainerExitStatus.CONTAINER_RUNNING_STATE, (float) 0.1,1,1024));

      final List<ProjectQuota> hopProjectQuota
              = new ArrayList<ProjectQuota>();
      hopProjectQuota.add(new ProjectQuota("Project07", 50, 0));

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
          _appState.add(hopApplicationState);

          ContainersLogsDataAccess<ContainerLog> _clDA
                  = (ContainersLogsDataAccess) RMStorageFactory.
                  getDataAccess(ContainersLogsDataAccess.class);
          _clDA.addAll(hopContainerLog);

          ProjectQuotaDataAccess<ProjectQuota> _pqDA
                  = (ProjectQuotaDataAccess) RMStorageFactory.
                  getDataAccess(ProjectQuotaDataAccess.class);
          _pqDA.addAll(hopProjectQuota);

          connector.commit();
          return null;
        }
      };
      bomb.handle();

    } catch (StorageInitializtionException | StorageException ex) {
      //LOG.error(ex);
    }
    
  }

  public void CheckProject(float credits, float used) throws IOException {

    Map<String, ProjectQuota> hopProjectQuotaList;

    LightWeightRequestHandler bomb;
    bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        ProjectQuotaDataAccess<ProjectQuota> _pqDA
                = (ProjectQuotaDataAccess) RMStorageFactory.
                getDataAccess(ProjectQuotaDataAccess.class);
        Map<String, ProjectQuota> _hopProjectQuotaList = _pqDA.
                getAll();

        connector.commit();
        return _hopProjectQuotaList;
      }
    };
    hopProjectQuotaList = (Map<String, ProjectQuota>) bomb.handle();

    for (Map.Entry<String, ProjectQuota> _ycl : hopProjectQuotaList.
            entrySet()) {
      Assert.assertTrue(_ycl.getValue().getProjectid().equalsIgnoreCase(
              "Project07"));
      Assert.assertEquals(credits, _ycl.getValue().getRemainingQuota(),0);
      Assert.assertEquals(used, _ycl.getValue().getTotalUsedQuota(),0);

    }

  }

  public void CheckProjectDailyCost(float used) throws IOException {

    Map<String, ProjectDailyCost> hopYarnProjectsDailyCostList;

    LightWeightRequestHandler bomb;
    bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        ProjectsDailyCostDataAccess _pdcDA
                = (ProjectsDailyCostDataAccess) RMStorageFactory.
                getDataAccess(ProjectsDailyCostDataAccess.class);
        Map<String, ProjectDailyCost> hopYarnProjectsDailyCostList
                = _pdcDA.getAll();

        connector.commit();
        return hopYarnProjectsDailyCostList;
      }
    };

    hopYarnProjectsDailyCostList = (Map<String, ProjectDailyCost>) bomb.
            handle();
    long _miliSec = System.currentTimeMillis();
    final long _day = TimeUnit.DAYS.convert(_miliSec, TimeUnit.MILLISECONDS);

    for (Map.Entry<String, ProjectDailyCost> _ypdc
            : hopYarnProjectsDailyCostList.entrySet()) {
      Assert.assertTrue(_ypdc.getValue().getProjectName().equalsIgnoreCase(
              "Project07"));
      Assert.assertTrue(_ypdc.getValue().getProjectUser().equalsIgnoreCase(
              "rizvi"));
      Assert.assertEquals(_day, _ypdc.getValue().getDay());
      Assert.assertEquals(used, _ypdc.getValue().getCreditsUsed(),0);

    }
  }

  @Test(timeout = 6000)
  public void TestRecover() throws IOException, Exception {

    // Prepare the scenario
    PrepareScenario();

    // Run the schedulat
    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE, 100);
    qs.init(conf);
    qs.serviceStart();
    Thread.currentThread().sleep(1000);
    qs.serviceStop();

    CheckProject(20,30);
    CheckProjectDailyCost(30);

  }

  @Test (timeout = 120000)
  public void TestStream() throws Exception {
    int initialCredits = 50;
    int totalCost = 0;
    //prepare database
    final ApplicationState hopApplicationState = new ApplicationState(
            "application_1450009406746_0001", new byte[0], "Project07__rizvi",
            "DistributedShell", "FINISHING");
    final List<ProjectQuota> hopProjectQuota
            = new ArrayList<ProjectQuota>();
    hopProjectQuota.add(new ProjectQuota("Project07", initialCredits,
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
        _appState.add(hopApplicationState);

        ProjectQuotaDataAccess<ProjectQuota> _pqDA
                = (ProjectQuotaDataAccess) RMStorageFactory.
                getDataAccess(ProjectQuotaDataAccess.class);
        _pqDA.addAll(hopProjectQuota);

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();

    QuotaService qs = new QuotaService();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE, 10);
    qs.init(conf);
    qs.serviceStart();
    //add containers
    for (int i = 0; i < 10; i++) {
      List<ContainerLog> logs = new ArrayList<>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainerLog("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float)0.1, 1 , 1024));
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some containers
    for (int i = 0; i < 3; i++) {
    List<ContainerLog> logs = new ArrayList<ContainerLog>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainerLog("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 5, ContainerExitStatus.SUCCESS,(float) 0.1,1,1024));
        totalCost+=1;
      }
    qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //checkpoint remaining containers
    for (int i = 3; i < 10; i++) {
      List<ContainerLog> logs = new ArrayList<ContainerLog>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainerLog("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 10,
                ContainerExitStatus.CONTAINER_RUNNING_STATE,(float) 0.1,1,1024));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //finish some checkpointed containers
    for (int i = 3; i < 6; i++) {
      List<ContainerLog> logs = new ArrayList<ContainerLog>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainerLog("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 15, ContainerExitStatus.SUCCESS,(float) 0.1,1,1024));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(1000);
    //preempt some containers
    for (int i = 6; i < 9; i++) {
      List<ContainerLog> logs = new ArrayList<ContainerLog>();

      for (int j = 0; j < i; j++) {
        logs.add(new ContainerLog("container_1450009406746_0001_0" + i
                + "_00000" + j, i, i + 16, ContainerExitStatus.PREEMPTED,(float) 0.1,1,1024));
        totalCost+=1;
      }
      qs.insertEvents(logs);
    }
    Thread.sleep(2000);
    CheckProject(initialCredits - totalCost, totalCost);
    CheckProjectDailyCost(totalCost);
  }

}
