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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import java.io.IOException;

public class DistributedRTRMEvaluation {

  private static final Log LOG =
      LogFactory.getLog(DistributedRTRMEvaluation.class);

  private Configuration conf;

  public DistributedRTRMEvaluation() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED, true);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
  }

  public DistributedRTRMEvaluation(int pendingPeriod)
      throws IOException, InterruptedException {
    this();
    System.out.println(
        "setting HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD to " + pendingPeriod);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
        pendingPeriod);
  }

  public static void main(String[] args)
      throws IOException, YarnException, InterruptedException {
    System.out.println("version 1.0");
    if (args.length == 0) {
      //TODO display help
      return;
    }

    if (args[0].equals("run")) {
      int pendingPeriod = Integer.parseInt(args[1]);
      DistributedRTRMEvaluation drt =
          new DistributedRTRMEvaluation(pendingPeriod);
      drt.start();
    } else if (args[0].equals("format")) {
      DistributedRTRMEvaluation drt = new DistributedRTRMEvaluation();
      drt.format();
    }

  }

  public void start() throws InterruptedException {
    ResourceManager rm = new ResourceManager();
    rm.init(conf);
    rm.start();

    while (true) {
      Thread.sleep(100);
    }
  }

  public void format() throws IOException {
    RMUtilities.InitializeDB();
  }
}
