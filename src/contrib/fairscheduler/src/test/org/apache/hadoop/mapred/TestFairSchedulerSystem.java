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
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * System tests for the fair scheduler. These run slower than the
 * mock-based tests in TestFairScheduler but have a better chance
 * of catching synchronization bugs with the real JT.
 */
public class TestFairSchedulerSystem {
  static final int NUM_THREADS=5;

  private void runSleepJob(JobConf conf) throws Exception {
    String[] args = { "-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
    ToolRunner.run(conf, new SleepJob(), args);
  }

  /**
   * Bump up the frequency of preemption updates to test against
   * deadlocks, etc.
   */
  @Test
  public void testPreemptionUpdates() throws Exception {
    MiniMRCluster mr = null;
    try {
      final int taskTrackers = 1;

      JobConf conf = new JobConf();
      conf.set("mapred.jobtracker.taskScheduler", FairScheduler.class.getCanonicalName());
      conf.set("mapred.fairscheduler.update.interval", "0");
      conf.set("mapred.fairscheduler.preemption.interval", "0");
      conf.set("mapred.fairscheduler.preemption", "true");
      conf.set("mapred.fairscheduler.eventlog.enabled", "true");
      conf.set(JTConfig.JT_PERSIST_JOBSTATUS, "false");

      mr = new MiniMRCluster(taskTrackers, "file:///", 1, null, null, conf);
      final MiniMRCluster finalMR = mr;
      ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(NUM_THREADS);
      for (int i = 0; i < NUM_THREADS; i++) {
        futures.add(exec.submit(new Callable<Void>() {
          public Void call() throws Exception {
            JobConf jobConf = finalMR.createJobConf();
            runSleepJob(jobConf);
            return null;
          }
        }));
      }

      for (Future<Void> future : futures) {
        future.get();
      }
    } finally {
      if (mr != null) { mr.shutdown();
      }
    }
  
  }
}
