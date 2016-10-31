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
package org.apache.hadoop.yarn.server.resourcemanager.quota;

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
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
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
    conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
            "target/test-classes/RT_EventAPIConfig.ini");
    conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
            "target/test-classes/RM_EventAPIConfig.ini");

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  @Test
  public void TestMultiplicatorEvaluation() throws Exception {

    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL, 2000);
    conf.setFloat(YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD, 0.2f);
    conf.setFloat(YarnConfiguration.QUOTA_INCREMENT_FACTOR, 10f);

    MockRM rm = new MockRM(conf);
    rm.start();

    ConsumeSomeResources(rm);
    //all the resources are allocated: 1AM and 14 Containers 
    //so cluster utilisation is 1, overpricing is 1-0.2(threshold)=0,8
    //multiplicator is 1+0,8*10(increment factor) = 9
    CheckCurrentMultiplicator(9);

  }

    private void ConsumeSomeResources(MockRM rm) throws Exception {
    // Start the nodes
    MockNM nm1 = rm.registerNode("h1:1234", 5 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 10 * GB);

    RMApp app = rm.submitApp(1 * GB);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    //request for containers
    int request = 4;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());

    //kick the scheduler
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    while (contReceived < 4) { //only 4 containers can be allocated on node1
      nm1.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(4, conts.size());

    Thread.sleep(5000);

    //request for containers
    request = 10;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());
    //send node2 heartbeat
    conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    contReceived = conts.size();
    while (contReceived < 10) { //Now rest of the (request-4=10) containers can be allocated on node1
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(10, conts.size());
    Thread.sleep(1000);
  }
    
  private void CheckCurrentMultiplicator(float value) throws Exception {
      LightWeightRequestHandler currentMultiplicatorHandler
              = new LightWeightRequestHandler(
                      YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          PriceMultiplicatorDataAccess pmDA
                  = (PriceMultiplicatorDataAccess) RMStorageFactory.getDataAccess(
                          PriceMultiplicatorDataAccess.class);
          Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> priceList = pmDA.getAll();

          connector.commit();
          return priceList.get(PriceMultiplicator.MultiplicatorType.VARIABLE).getValue();
        }
      };
      float currentMultiplicator = (Float) currentMultiplicatorHandler.handle();
      Assert.assertEquals(value, currentMultiplicator,0);
  }
}
