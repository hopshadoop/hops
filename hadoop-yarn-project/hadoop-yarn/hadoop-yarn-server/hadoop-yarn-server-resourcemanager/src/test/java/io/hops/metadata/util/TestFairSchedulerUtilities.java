/*
 * Copyright (C) 2015 hops.io.
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

package io.hops.metadata.util;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TestFairSchedulerUtilities {
  private static final Log LOG =
      LogFactory.getLog(TestFairSchedulerUtilities.class);
  private Configuration conf;
  private final int GB = 1024;
  private RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private String appType = "MockApp";

  @Before
  public void setup() throws IOException {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMStorageFactory.getConnector().formatStorage();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
          ResourceScheduler.class);
      // All tests assume only one assignment per node update
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (StorageException ex) {
      LOG.error(ex);
    }
  }

  @Test
  public void TestSimpleFairShareCalculation() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    //submit an application of 2GB memory
    RMApp app1 = rm.submitApp(3 * GB, "", "user1", null, "queue1");
    RMApp app2 = rm.submitApp(1 * GB, "", "user2", null, "queue2");

    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();

    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    nm1.nodeHeartbeat(true);


    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();

    Thread.sleep(3000);


    //get the Scheduler
    FairScheduler fairScheduler = (FairScheduler) rm.getResourceScheduler();

    Collection<FSLeafQueue> queues =
        fairScheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    rm.stop();

  }
}
