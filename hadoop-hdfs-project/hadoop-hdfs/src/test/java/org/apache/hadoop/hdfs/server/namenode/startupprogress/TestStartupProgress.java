/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressTestHelper.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Status.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestStartupProgress {

  private StartupProgress startupProgress;

  @Before
  public void setUp() {
    startupProgress = new StartupProgress();
  }




  @Test(timeout=10000)
  public void testInitialState() {
    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(0L, view.getElapsedTime());
    assertEquals(0.0f, view.getPercentComplete(), 0.001f);
    List<Phase> phases = new ArrayList<Phase>();

    for (Phase phase: view.getPhases()) {
      phases.add(phase);
      assertEquals(0L, view.getElapsedTime(phase));
      assertNull(view.getFile(phase));
      assertEquals(0.0f, view.getPercentComplete(phase), 0.001f);
      assertEquals(Long.MIN_VALUE, view.getSize(phase));
      assertEquals(PENDING, view.getStatus(phase));
      assertEquals(0L, view.getTotal(phase));

      for (Step step: view.getSteps(phase)) {
        fail(String.format("unexpected step %s in phase %s at initial state",
          step, phase));
      }
    }

    assertArrayEquals(EnumSet.allOf(Phase.class).toArray(), phases.toArray());
  }
}
