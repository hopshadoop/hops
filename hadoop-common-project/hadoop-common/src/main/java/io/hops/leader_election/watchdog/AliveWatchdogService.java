/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.leader_election.watchdog;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Assume an external oracle machine which decides whether a cluster or data center
 * should be alive or not. This oracle machine could be a simple http server returning
 * 200 for one url and 404 for another.
 *
 * This service will block the start-up process of critical Active-Active components such
 * as the NameNode or ResourceManager that are stretched in multiple Data Centers to avoid
 * metadata inconsistencies.
 *
 * If the oracle machines replies that the current region/DC is active, it will allow the
 * start-up and periodically monitor for changes. If the current region/DC is not active
 * then the watchdog will kill the service, either during start-up or at runtime.
 */
public class AliveWatchdogService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(AliveWatchdogService.class);

  private final TimeUnit intervalTimeunit = TimeUnit.MILLISECONDS;
  private long interval;
  private AliveWatchdogPoller poller;
  private ScheduledExecutorService executorService;

  public AliveWatchdogService() {
    super("Active watchdog service");
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    interval = conf.getTimeDuration(
        CommonConfigurationKeys.ALIVE_WATCHDOG_POLL_INTERVAL,
        CommonConfigurationKeys.ALIVE_WATCHDOG_POLL_INTERVAL_DEFAULT,
        intervalTimeunit);
    if (interval < 20) {
      LOG.warn("Interval is set too low. Setting it to 20ms");
      interval = 20;
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting Alive Watchdog service");
    LOG.info("Polling interval is " + getConfig().get(CommonConfigurationKeys.ALIVE_WATCHDOG_POLL_INTERVAL));
    // That's to allow tests set their own poller
    if (poller == null) {
      poller = loadPoller();
    }
    poller.init();
    LOG.debug("Loaded poller class");
    BackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(5)
        .setMaximumIntervalMillis(interval)
        .build();

    if (!shouldIBeAliveWithRetry(backOff)) {
      LOG.info("We should not be alive. Bye");
      exit();
    } else {
      // This branch is to facilitate testing where we mock the exit() method

      // If oracle has decided we should be alive wait for at least as much as
      // watchdog interval before we proceed to give enough time for the services
      // on the other DC to kill themselves
      intervalTimeunit.sleep(interval + 1000L);
      PollerDaemon watchdog = new PollerDaemon(backOff);
      LOG.debug("Starting watchdog");
      executorService = HadoopExecutors.newScheduledThreadPool(1);
      executorService.scheduleAtFixedRate(watchdog, 0L, interval, intervalTimeunit);
    }
    super.serviceStart();
  }

  @VisibleForTesting
  ExecutorService getExecutorService() {
    return executorService;
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("Stopping Alive Watchdog service");
    stopExecutorService();
    if (poller != null) {
      poller.destroy();
    }
  }

  private void stopExecutorService() {
    if (executorService != null) {
      try {
        executorService.shutdown();
        if (!executorService.awaitTermination(100L, TimeUnit.MILLISECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException ex) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private AliveWatchdogPoller loadPoller() throws ClassNotFoundException {
    String pollerClass = getConfig().get(CommonConfigurationKeys.ALIVE_WATCHDOG_POLLER_CLASS);
    if (pollerClass == null) {
      throw new IllegalArgumentException("Alive watchdog service is enabled but poller class is not set");
    }
    LOG.info("Loading poller class " + pollerClass);
    Class<?> clazz = getConfig().getClassByName(pollerClass);
    return (AliveWatchdogPoller) ReflectionUtils.newInstance(clazz, getConfig());
  }

  @VisibleForTesting
  void setPoller(AliveWatchdogPoller poller) {
    this.poller = poller;
  }

  void exit() {
    stopExecutorService();
    System.exit(1);
  }

  private boolean shouldIBeAliveWithRetry(BackOff backOff) throws Exception {
    backOff.reset();
    while (true) {
      try {
        return poller.shouldIBeAlive();
      } catch (Exception ex) {
        long backOffTimeout = backOff.getBackOffInMillis();
        if (backOffTimeout != -1) {
          LOG.debug("Error while polling to check whether we should be alive or not. Retrying in "
              + backOffTimeout + "ms");
          try {
            TimeUnit.MILLISECONDS.sleep(backOffTimeout);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
          }
        } else {
          throw ex;
        }
      }
    }
  }

  private class PollerDaemon implements Runnable {
    private final BackOff backOff;

    private PollerDaemon(BackOff backOff) {
      this.backOff = backOff;
    }

    @Override
    public void run() {
      LOG.debug("Checking whether we should be alive or not");
      try {
        if (!shouldIBeAliveWithRetry(backOff)) {
          LOG.info("Watchdog decided we should not be alive. Bye...");
          exit();
        }
        LOG.debug("We should be alive");
      } catch (Exception ex) {
        LOG.fatal("Watchdog could not contact oracle service. Shutting down for safety");
        exit();
      }
    }
  }
}
