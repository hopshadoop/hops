/*
 * Copyright 2016 Apache Software Foundation.
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
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPriceMultiplicationFactorService {

  private static final Log LOG = LogFactory.getLog(
          TestPriceMultiplicationFactorService.class);
  private StorageConnector connector = null;
  private Configuration conf = null;
  private final static int WAIT_SLEEP_MS = 100;
  private final int GB = 1024;
  private volatile boolean stopped = false;

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_QUOTA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.QUOTA_VARIABLE_PRICE_ENABLED, true);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  @Test
  public void TestMultiplicatorEvaluation() throws Exception {

    conf.setFloat(YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD_GENERAL, 0.2f);
    conf.setFloat(YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD_GPU, 0.2f);
    conf.setFloat(YarnConfiguration.QUOTA_INCREMENT_FACTOR_GENERAL, 10f);
    conf.setFloat(YarnConfiguration.QUOTA_INCREMENT_FACTOR_GPU, 20f);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, DominantResourceCalculator.class.getName());
    conf.set(YarnConfiguration.NM_RESOURCE_PLUGINS, ResourceInformation.GPU_URI);
    conf.set(YarnConfiguration.RESOURCE_TYPES, ResourceInformation.GPU_URI);

    MockRM rm = new MockRM(conf);
    rm.start();

    ConsumeSomeResources(rm);
    Thread.sleep(1000);
    //GENERAL: half of the resources are allocated: 1AM and 6 Containers with 1 vcore and 1MB each.
    //so cluster utilisation is 0.5, overpricing is 0.5-0.2(threshold)=0,3
    //multiplicator is 1+0,3*10(increment factor) = 4
    //GPU: all the GPU resources are allocated.
    //so cluster utilisation is 1, overpricing is 1-0.2(threshold)=0,8
    //multiplicator is 1+0,8*20(increment factor) = 17
    CheckCurrentMultiplicator(4, 17);

  }

  private void ConsumeSomeResources(MockRM rm) throws Exception {
    // Start the nodes
    MockNM nm1 = rm.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", TestUtils
        .createResource(10 * GB, 10, ImmutableMap.<String, Integer>builder()
            .put(ResourceInformation.GPU_URI, 1).build()));

    RMApp app = rm.submitApp(1 * GB);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    //request for containers
    int request = 3;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());

    //kick the scheduler
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    while (contReceived < request) { 
      nm1.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(request, conts.size());

    Thread.sleep(5000);

    //request for containers
    request = 2;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());
    //send node2 heartbeat
    conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    contReceived = conts.size();
    while (contReceived < request) { 
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(request, conts.size());
    
    am.allocate("h1", 1 * GB, 1, new ArrayList<ContainerId>(), 1);
    conts = am.allocate(new ArrayList<ResourceRequest>(),new ArrayList<ContainerId>()).getAllocatedContainers();
    contReceived = conts.size();
    while (contReceived < 1) { //only 4 containers can be allocated on node1
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(1, conts.size());
    
    Thread.sleep(1000);
  }
    
  private void CheckCurrentMultiplicator(final float generalMultiplicator, final float gpuMultiplicator) throws
      Exception {
    LightWeightRequestHandler currentMultiplicatorHandler
              = new LightWeightRequestHandler(
                      YARNOperationType.OTHER) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          PriceMultiplicatorDataAccess pmDA
                  = (PriceMultiplicatorDataAccess) RMStorageFactory.getDataAccess(
                          PriceMultiplicatorDataAccess.class);
          Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> priceList = pmDA.getAll();

          connector.commit();
          return priceList;
        }
      };
      int nbTry=0;
      Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentMultiplicators = new HashMap<>();
      while (nbTry < 10) {
        currentMultiplicators
            = (Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator>) currentMultiplicatorHandler.handle();
        if (currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.GENERAL).getValue() == generalMultiplicator
            && currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.GPU).getValue() == gpuMultiplicator) {
          break;
        }
       Thread.sleep(500);
       nbTry++;
      }
    Assert.assertEquals(generalMultiplicator, currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.GENERAL).
        getValue(), 0);
    Assert.
        assertEquals(gpuMultiplicator, currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.GPU).getValue(), 0);
  }
}
