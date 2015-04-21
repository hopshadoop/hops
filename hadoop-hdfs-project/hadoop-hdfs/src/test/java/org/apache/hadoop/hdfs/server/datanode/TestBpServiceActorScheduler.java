/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static java.lang.Math.abs;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import org.apache.hadoop.hdfs.server.datanode.BPServiceActor.Scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


/**
 * Verify the block report and heartbeat scheduling logic of BPServiceActor
 * using a few different values .
 */
public class TestBpServiceActorScheduler {
  protected static final Log LOG = LogFactory.getLog(TestBpServiceActorScheduler.class);

  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final long HEARTBEAT_INTERVAL_MS = 5000;      // 5 seconds
  private final Random random = new Random(System.nanoTime());

  @Test
  public void testInit() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.isHeartbeatDue(now));
    }
  }

  @Test
  public void testScheduleHeartbeat() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleNextHeartbeat();
      assertFalse(scheduler.isHeartbeatDue(now));
      scheduler.scheduleHeartbeat();
      assertTrue(scheduler.isHeartbeatDue(now));
    }
  }

  private Scheduler makeMockScheduler(long now) {
    LOG.info("Using now = " + now);
    Scheduler mockScheduler = spy(new Scheduler(HEARTBEAT_INTERVAL_MS));
    doReturn(now).when(mockScheduler).monotonicNow();
    mockScheduler.nextHeartbeatTime = now;
    return mockScheduler;
  }

  List<Long> getTimestamps() {
    return Arrays.asList(
        0L, Long.MIN_VALUE, Long.MAX_VALUE, // test boundaries
        Long.MAX_VALUE - 1,                 // test integer overflow
        abs(random.nextLong()),             // positive random
        -abs(random.nextLong()));           // negative random
  }
}
