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
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType.*;

/**
 * Utility methods that help with writing tests covering startup progress.
 */
public class StartupProgressTestHelper {

  /**
   * Increments a counter a certain number of times.
   * 
   * @param prog StartupProgress to increment
   * @param phase Phase to increment
   * @param step Step to increment
   * @param delta long number of times to increment
   */
  public static void incrementCounter(StartupProgress prog, Phase phase,
      Step step, long delta) {
    StartupProgress.Counter counter = prog.getCounter(phase, step);
    for (long i = 0; i < delta; ++i) {
      counter.increment();
    }
  }

  /**
   * Sets up StartupProgress to final state after startup sequence has completed.
   * 
   * @param prog StartupProgress to set
   */
  public static void setStartupProgressForFinalState(StartupProgress prog) {
    prog.beginPhase(SAFEMODE);
    Step awaitingBlocks = new Step(AWAITING_REPORTED_BLOCKS);
    prog.beginStep(SAFEMODE, awaitingBlocks);
    prog.setTotal(SAFEMODE, awaitingBlocks, 400L);
    incrementCounter(prog, SAFEMODE, awaitingBlocks, 400L);
    prog.endStep(SAFEMODE, awaitingBlocks);
    prog.endPhase(SAFEMODE);
  }
}
