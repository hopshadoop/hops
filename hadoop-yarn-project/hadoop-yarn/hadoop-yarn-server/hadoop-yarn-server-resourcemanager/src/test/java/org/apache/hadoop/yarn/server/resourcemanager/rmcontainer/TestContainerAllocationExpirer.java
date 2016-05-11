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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestContainerAllocationExpirer {

   private static final Log LOG = LogFactory.
      getLog(TestContainerAllocationExpirer.class);

  private Configuration conf;
    
  private final int GB = 1024;
  
    @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,10000);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
              FifoScheduler.class, ResourceScheduler.class);
  }
  
  @Test
  public void testContainerExpiration() throws Exception {
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      // Register node1
      MockNM nm = rm.registerNode("127.0.0.1:1234", 4 * GB, 4);
      

      nm.nodeHeartbeat(true);
      //HOP :: Sleep to allow previous events to be processed
      Thread.sleep(
          conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
              YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
              2);
      // wait..
      int waitCount = 20;
      int size = rm.getRMContext().getActiveRMNodes().size();
      while ((size = rm.getRMContext().getActiveRMNodes().size()) != 2 &&
          waitCount-- > 0) {
        LOG.info("Waiting for node managers to register : " + size);
        Thread.sleep(100);
      }
      Assert.assertEquals(1, rm.getRMContext().getActiveRMNodes().size());
      // Submit an application
      RMApp app1 = rm.submitApp(128);

      // kick the scheduling
      nm.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId(), nm);
      am1.registerAppAttempt();

      LOG.info("sending container requests ");
      am1.addRequests(new String[]{"*"}, 1 * GB, 1, 1);
      AllocateResponse alloc1Response = am1.schedule(); // send the request

      // kick the scheduler
      nm.nodeHeartbeat(true);
      int waitCounter = 20;
      LOG.info("heartbeating nm1");
      while (alloc1Response.getAllocatedContainers().size() < 1 &&
          waitCounter-- > 0) {
        LOG.info("Waiting for containers to be created for app 1...");
        Thread.sleep(500);
        nm.nodeHeartbeat(true);
        alloc1Response = am1.schedule();
      }
      LOG.info("received container : " +
          alloc1Response.getAllocatedContainers().size());
      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);
      ContainerId allocatedContainer = alloc1Response.getAllocatedContainers().get(0).getId();
      // kick the scheduler
      nm.nodeHeartbeat(attempt1.getAppAttemptId(), allocatedContainer.getId(), ContainerState.RUNNING);
      waitCounter = 20;
      while(rm.getResourceScheduler().getRMContainer(allocatedContainer).getState() != RMContainerState.RUNNING&&
          waitCounter-- > 0){
         Thread.sleep(500);
        nm.nodeHeartbeat(true);
      }
      LOG.info("container state : " + rm.getResourceScheduler().getRMContainer(allocatedContainer).getState());
      Assert.assertTrue(rm.getResourceScheduler().getRMContainer(allocatedContainer).getState() == RMContainerState.RUNNING);
      //expire container
      Thread.sleep(30000);
      Assert.assertTrue(rm.getResourceScheduler().getRMContainer(allocatedContainer) == null);
    } finally {
      rm.stop();
    }

  }
}
