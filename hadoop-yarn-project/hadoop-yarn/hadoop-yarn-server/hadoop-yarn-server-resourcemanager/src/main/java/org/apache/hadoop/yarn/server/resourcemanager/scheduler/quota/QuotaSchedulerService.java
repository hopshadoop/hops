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

import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

/**
 *
 * @author rizvi
 */
public class QuotaSchedulerService extends AbstractService{

    private Thread quotaSchedulingThread;  
    private volatile boolean stopped;
    private static final Log LOG = LogFactory.getLog(QuotaSchedulerService.class);

    
    public QuotaSchedulerService(String name) {
        super(name);
    }
    
  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new quota schedular service");
    quotaSchedulingThread = new Thread(new QuotaScheduler());
    quotaSchedulingThread.setName("Quota scheduling service");
    quotaSchedulingThread.start();
    super.serviceStart();
  }
  
  
  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (quotaSchedulingThread != null) {
      quotaSchedulingThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopping the quota schedular service.");
  }

  /*
  private class QuotaSchedular  implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (this) {
          Iterator<Map.Entry<O, Long>> iterator = running.entrySet().iterator();

          //avoid calculating current time everytime in loop
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<O, Long> entry = iterator.next();
            if (currentTime > entry.getValue() + expireInterval) {
              iterator.remove();
              expire(entry.getKey());
              LOG.info("Expired:" + entry.getKey().toString() +
                  " Timed out after " + expireInterval / 1000 + " secs");
            }
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }*/    
    
}
