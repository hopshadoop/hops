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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

/**
 *
 * @author rizvi
 */
public class QuotaSchedulerService extends AbstractService {

  private Thread quotaSchedulingThread;
  QuotaScheduler quotaScheduler;
  private volatile boolean stopped;
  private static final Log LOG = LogFactory.getLog(QuotaSchedulerService.class);

  public QuotaSchedulerService() {
    super("quota scheduler service");
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new quota schedular service");
    quotaScheduler.recover();
    quotaSchedulingThread = new Thread(quotaScheduler);
    quotaSchedulingThread.setName("Quota scheduling service");
    quotaSchedulingThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if(quotaScheduler!=null){
      quotaScheduler.stop();
    }
    if (quotaSchedulingThread != null) {
      quotaSchedulingThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopping the quota schedular service.");
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    quotaScheduler = new QuotaScheduler();
    quotaScheduler.init(conf);
  }
}
