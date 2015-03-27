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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;

import java.io.IOException;

public class TestDistributedRTRM {

  private static final Log LOG = LogFactory.getLog(TestDistributedRTRM.class);
  private Configuration conf;

  @Before
  public void setup() {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);

    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }

  // ********** ENABLE THIS TEST TO EVALUATE DISTRIBUTED_RT ******* //

  /**
   * Start ResourceManager.
   * <p/>
   *
   * @throws java.lang.InterruptedException
   * @throws java.io.IOException
   */
  //@Test
  public void startRM() throws InterruptedException, IOException {
    RMUtilities.InitializeDB();
    ResourceManager rm = new ResourceManager();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED, true);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD, 500);
    rm.init(conf);
    rm.start();

    while (true) {
      //rmMap.get(0).waitForServiceToStop(100000);
      Thread.sleep(100);
    }
  }
}
