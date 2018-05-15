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
package org.apache.hadoop.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestExponentialBackOff {
  
  @Test
  public void testDefaultValues() {
    ExponentialBackOff backOff = new ExponentialBackOff();
    assertEquals(ExponentialBackOff.INITIAL_INTERVAL_MILLIS, backOff.getInitialIntervalMillis());
    assertEquals(ExponentialBackOff.MAXIMUM_INTERVAL_MILLIS, backOff.getMaximumIntervalMillis());
    assertEquals(ExponentialBackOff.RANDOMIZATION_FACTOR, backOff.getRandomizationFactor(), 0.01);
    assertEquals(ExponentialBackOff.MULTIPLIER, backOff.getMultiplier(), 0.01);
    assertEquals(ExponentialBackOff.MAXIMUM_RETRIES, backOff.getMaximumRetries());
  }
  
  @Test
  public void testBuilder() {
    long initialInterval = 150;
    long maximumInterval = 400;
    double randomizationFactor = 0.3;
    double multiplier = 1.2;
    int maximumRetries = 3;
    
    ExponentialBackOff backoff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(initialInterval)
        .setMaximumIntervalMillis(maximumInterval)
        .setRandomizationFactor(randomizationFactor)
        .setMultiplier(multiplier)
        .setMaximumRetries(maximumRetries)
        .build();
    assertEquals(initialInterval, backoff.getInitialIntervalMillis());
    assertEquals(maximumInterval, backoff.getMaximumIntervalMillis());
    assertEquals(randomizationFactor, backoff.getRandomizationFactor(), 0.01);
    assertEquals(multiplier, backoff.getMultiplier(), 0.01);
    assertEquals(maximumRetries, backoff.getMaximumRetries());
  }
  
  @Test
  public void testMaximumRetries() {
    int maxRetries = 3;
    BackOff backOff = new ExponentialBackOff.Builder()
        .setMaximumRetries(maxRetries).build();
    int retries = 0;
    while (backOff.getBackOffInMillis() != -1) {
      retries++;
    }
    assertEquals(maxRetries, retries);
  }
  
  @Test
  public void testExponentialBackOff() {
    BackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(100)
        .setMaximumIntervalMillis(2700)
        .setMultiplier(3)
        .setRandomizationFactor(0)
        .setMaximumRetries(6)
        .build();
    long[] expectedTimeouts = new long[] {100, 300, 900, 2700, 2700, 2700, -1};
    for (long timeout : expectedTimeouts) {
      assertEquals(timeout, backOff.getBackOffInMillis());
    }
  }
  
  @Test
  public void testReset() {
    ExponentialBackOff backOff = new ExponentialBackOff.Builder()
        .setRandomizationFactor(0)
        .setMultiplier(1)
        .build();
    backOff.getBackOffInMillis();
    backOff.getBackOffInMillis();
    backOff.reset();
    assertEquals(0, backOff.getNumberOfRetries());
    assertEquals(backOff.getInitialIntervalMillis(), backOff.getBackOffInMillis());
  }
}
