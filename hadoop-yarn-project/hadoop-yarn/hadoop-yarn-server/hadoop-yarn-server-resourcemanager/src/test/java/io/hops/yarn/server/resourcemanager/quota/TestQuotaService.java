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
package io.hops.yarn.server.resourcemanager.quota;

import com.google.common.collect.ImmutableMap;
import io.hops.StorageConnector;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.metadata.yarn.entity.quota.ProjectDailyCost;
import io.hops.metadata.yarn.entity.quota.ProjectQuota;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestQuotaService {

  private static final Log LOG = LogFactory.getLog(TestQuotaService.class);
  private StorageConnector connector = null;
  Configuration conf;
  
  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_QUOTA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.QUOTA_VARIABLE_PRICE_ENABLED, true);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  public void CheckProject(float credits, float used) throws IOException {

    Map<String, ProjectQuota> hopProjectQuotaList;

    LightWeightRequestHandler bomb;
    bomb = new LightWeightRequestHandler(YARNOperationType.OTHER) {
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
    bomb = new LightWeightRequestHandler(YARNOperationType.OTHER) {
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

  @Test (timeout = 120000)
  public void TestQuotaService() throws Exception {
    int initialCredits = 50;
    int totalCost = 0;
    //prepare database
    String user = "Project07__rizvi";
    ApplicationId appId = ApplicationId.fromString("application_1450009406746_0001");
    
    final List<ProjectQuota> hopProjectQuota
            = new ArrayList<ProjectQuota>();
    hopProjectQuota.add(new ProjectQuota("Project07", initialCredits,
            0));

    LightWeightRequestHandler prepareHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        ProjectQuotaDataAccess<ProjectQuota> _pqDA
                = (ProjectQuotaDataAccess) RMStorageFactory.
                getDataAccess(ProjectQuotaDataAccess.class);
        _pqDA.addAll(hopProjectQuota);

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();
    RMContext rmContext = Mockito.mock(RMContext.class);
    ConcurrentMap<ApplicationId, RMApp> rmApps = Mockito.mock(ConcurrentMap.class);
    RMApp rmApp = Mockito.mock(RMApp.class);
    PriceMultiplicatorService pms = Mockito.mock(PriceMultiplicatorService.class);
    
    Mockito.when(rmContext.getRMApps()).thenReturn(rmApps);
    Mockito.when(rmContext.getPriceMultiplicatorService()).thenReturn(pms);
    
    Mockito.when(rmApps.get(Mockito.any(ApplicationId.class))).thenReturn(rmApp);
    Mockito.when(rmApp.getUser()).thenReturn(user);
    
    Mockito.when(pms.getMultiplicator(Mockito.any(PriceMultiplicator.MultiplicatorType.class))).thenReturn(new Float(1));
    
    QuotaService qs = new QuotaService(rmContext);
    
    //make sure that no quota will be automatically computed during the time of the test to avoid race condition
    conf.setInt(YarnConfiguration.QUOTA_SCHEDULING_PERIOD, 1200000); 
    qs.init(conf);
    qs.serviceStart();
    //add containers
    for (int i = 0; i < 10; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        RMContainer container = Mockito.mock(RMContainer.class);
        Mockito.when(container.getContainerId()).thenReturn(containerId);
        Mockito.when(container.getAllocatedResource()).thenReturn(TestUtils.createResource(1024, 1, ImmutableMap.<String, Integer>builder().build()));
        Mockito.when(container.getAllocatedNode()).thenReturn(null);
        Mockito.when(container.getAllocatedPriority()).thenReturn(Priority.UNDEFINED);
        Mockito.when(container.getCreationTime()).thenReturn(0L);
        qs.containerStarted(container);
      }
    }
    Thread.sleep(1000);
    //finish some containers
    for (int i = 0; i < 3; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        RMContainer container = Mockito.mock(RMContainer.class);
        Mockito.when(container.getContainerId()).thenReturn(containerId);
        Mockito.when(container.getDiagnosticsInfo()).thenReturn(null);
        Mockito.when(container.getContainerExitStatus()).thenReturn(0);
        Mockito.when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
        Mockito.when(container.getFinishTime()).thenReturn(1000L);
        qs.containerFinished(container);
        totalCost+=10;
      }
    }
    Thread.sleep(1000);
    
    //trigger quota computation on the remaining containers
    for (int i = 3; i < 10; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        totalCost+=10;
        qs.computeQuota(containerId, 10000L);
      }
    }
    Thread.sleep(1000);
    //finish some more containers
    for (int i = 3; i < 6; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        RMContainer container = Mockito.mock(RMContainer.class);
        Mockito.when(container.getContainerId()).thenReturn(containerId);
        Mockito.when(container.getDiagnosticsInfo()).thenReturn(null);
        Mockito.when(container.getContainerExitStatus()).thenReturn(0);
        Mockito.when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
        Mockito.when(container.getFinishTime()).thenReturn(19000L);
        qs.containerFinished(container);
        totalCost+=9;
      }
    }
    Thread.sleep(1000);
    //update the remaining containers
    for (int i = 6; i < 10; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        RMContainer container = Mockito.mock(RMContainer.class);
        Mockito.when(container.getContainerId()).thenReturn(containerId);
        Mockito.when(container.getAllocatedResource()).thenReturn(TestUtils.createResource(2048, 1, ImmutableMap.<String, Integer>builder().build()));
        Mockito.when(container.getAllocatedNode()).thenReturn(null);
        Mockito.when(container.getAllocatedPriority()).thenReturn(Priority.UNDEFINED);
        Mockito.when(container.getCreationTime()).thenReturn(0L);
        qs.containerUpdated(container, 20000L);
        totalCost+=10;
      }
    }
    Thread.sleep(1000);
    //finish the updated containers
    for (int i = 6; i < 10; i++) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      for (int j = 0; j < i; j++) {
        ContainerId containerId = ContainerId.newContainerId(appAttemptId, j);
        RMContainer container = Mockito.mock(RMContainer.class);
        Mockito.when(container.getContainerId()).thenReturn(containerId);
        Mockito.when(container.getDiagnosticsInfo()).thenReturn(null);
        Mockito.when(container.getContainerExitStatus()).thenReturn(0);
        Mockito.when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
        Mockito.when(container.getFinishTime()).thenReturn(30000L);
        qs.containerFinished(container);
        totalCost+=20;
      }
    }
    Thread.sleep(1000);
    CheckProject(initialCredits - totalCost, totalCost);
    CheckProjectDailyCost(totalCost);
  }

}
