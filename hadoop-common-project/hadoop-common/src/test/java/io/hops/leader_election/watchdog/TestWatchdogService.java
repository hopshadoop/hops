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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestWatchdogService {

  private static class AlwaysAlivePoller implements AliveWatchdogPoller {

    int calledCounter = 0;
    @Override
    public Boolean shouldIBeAlive() throws Exception {
      calledCounter++;
      return true;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void destroy() throws Exception {}

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }

  private static class AlwaysDeadPoller implements AliveWatchdogPoller {

    @Override
    public Boolean shouldIBeAlive() throws Exception {
      return false;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() throws Exception {}

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }

  private static class SwitchPoller implements AliveWatchdogPoller {

    private final int tippingPoint;
    private int calls = 0;
    private CountDownLatch tipped;

    private SwitchPoller(int tippingPoint, CountDownLatch tipped) {
      this.tippingPoint = tippingPoint;
      this.tipped = tipped;
    }

    @Override
    public Boolean shouldIBeAlive() throws Exception {
      if (calls > tippingPoint) {
        tipped.countDown();
      }
      return calls++ < tippingPoint;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() throws Exception {}

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }

  private static class RetryPoller implements AliveWatchdogPoller {

    int calls = 0;
    boolean startJitter = false;
    CountDownLatch wentThrough;
    final int tippingPoint;

    private RetryPoller(CountDownLatch wentThrough) {
      this(wentThrough, 3);
    }

    private RetryPoller(CountDownLatch wentThrough, int tippingPoint) {
      this.wentThrough = wentThrough;
      this.tippingPoint = tippingPoint;
    }

    @Override
    public Boolean shouldIBeAlive() throws Exception {
      if (startJitter && calls++ < tippingPoint) {
        throw new IOException("oops");
      }
      // First time it's called is from the serviceStart
      // Start jitter afterwards
      if (!startJitter) {
        startJitter = true;
      } else {
        wentThrough.countDown();
      }
      return true;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() throws Exception {}

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }

  @Test
  public void testAliveAtStartUp() throws Exception {
    Configuration conf = new Configuration(true);
    AliveWatchdogService realWatchdog = new AliveWatchdogService();
    AliveWatchdogService watchdogService = Mockito.spy(realWatchdog);
    Mockito.doNothing().when(watchdogService).exit();
    try {
      watchdogService.serviceInit(conf);
      AlwaysAlivePoller poller = new AlwaysAlivePoller();
      watchdogService.setPoller(poller);
      watchdogService.serviceStart();
      Assert.assertTrue(poller.calledCounter > 0);
      Mockito.verify(watchdogService, Mockito.never()).exit();
    } finally {
      watchdogService.serviceStop();
    }
  }

  @Test
  public void testNotAliveAtStartUp() throws Exception {
    Configuration conf = new Configuration(true);
    AliveWatchdogService realWatchdog = new AliveWatchdogService();
    AliveWatchdogService watchdogService = Mockito.spy(realWatchdog);
    Mockito.doNothing().when(watchdogService).exit();
    try {
      watchdogService.serviceInit(conf);
      AlwaysDeadPoller poller = new AlwaysDeadPoller();
      watchdogService.setPoller(poller);
      watchdogService.serviceStart();
      Mockito.verify(watchdogService, Mockito.times(1)).exit();
      Assert.assertNull(watchdogService.getExecutorService());
    } finally {
      watchdogService.serviceStop();
    }
  }

  @Test
  public void testWatchdogSwitch() throws Exception {
    Configuration conf = new Configuration(true);
    conf.set(CommonConfigurationKeys.ALIVE_WATCHDOG_POLL_INTERVAL, "20ms");
    AliveWatchdogService watchdogService = Mockito.spy(new AliveWatchdogService());
    Mockito.doNothing().when(watchdogService).exit();
    CountDownLatch tipped = new CountDownLatch(1);
    try {
      watchdogService.serviceInit(conf);
      SwitchPoller poller = new SwitchPoller(5, tipped);
      watchdogService.setPoller(poller);
      watchdogService.serviceStart();
      tipped.await(5, TimeUnit.SECONDS);
      Mockito.verify(watchdogService, Mockito.atLeastOnce()).exit();
    } finally {
      watchdogService.serviceStop();
    }
    Assert.assertTrue(watchdogService.getExecutorService().isTerminated());
    Assert.assertTrue(watchdogService.getExecutorService().isShutdown());
  }

  @Test
  public void testPollerRetry() throws Exception {
    Configuration conf = new Configuration(true);
    AliveWatchdogService watchdogService = Mockito.spy(new AliveWatchdogService());
    Mockito.doNothing().when(watchdogService).exit();
    CountDownLatch wentThrough = new CountDownLatch(1);
    try {
      watchdogService.serviceInit(conf);
      RetryPoller poller = new RetryPoller(wentThrough);
      watchdogService.setPoller(poller);
      watchdogService.serviceStart();
      wentThrough.await(5, TimeUnit.SECONDS);
      Mockito.verify(watchdogService, Mockito.never()).exit();
    } finally {
      watchdogService.serviceStop();
    }
  }

  @Test
  public void testPollerRetryGiveUp() throws Exception {
    Configuration conf = new Configuration(true);
    // Backoff is maxing at this interval. Make it short enough so the test does not take long
    conf.set(CommonConfigurationKeys.ALIVE_WATCHDOG_POLL_INTERVAL, "50ms");
    AliveWatchdogService watchdogService = Mockito.spy(new AliveWatchdogService());
    Mockito.doNothing().when(watchdogService).exit();
    try {
      watchdogService.serviceInit(conf);
      // Always throw exception
      RetryPoller poller = new RetryPoller(null, Integer.MAX_VALUE);
      watchdogService.setPoller(poller);
      watchdogService.serviceStart();
      // Backoff is time based so the best and easiest would be to sleep well enough
      // until all retries are exhausted
      TimeUnit.SECONDS.sleep(3);
      Mockito.verify(watchdogService, Mockito.atLeastOnce()).exit();
    } finally {
      watchdogService.serviceStop();
    }
  }
}
