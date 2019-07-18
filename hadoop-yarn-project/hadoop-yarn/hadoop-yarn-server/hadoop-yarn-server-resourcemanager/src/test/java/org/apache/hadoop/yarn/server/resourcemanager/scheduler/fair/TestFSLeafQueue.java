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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSLeafQueue extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFSLeafQueue.class.getName() + ".xml").getAbsolutePath();
  private Resource maxResource = Resources.createResource(1024 * 8);

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  @Test
  public void testUpdateDemand() {
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    String queueName = "root.queue1";
    FSLeafQueue schedulable = new FSLeafQueue(queueName, scheduler, null);
    schedulable.setMaxShare(new ConfigurableResource(maxResource));
    assertEquals(schedulable.getMetrics().getMaxApps(), Integer.MAX_VALUE);
    assertEquals(schedulable.getMetrics().getSchedulingPolicy(),
        SchedulingPolicy.DEFAULT_POLICY.getName());

    FSAppAttempt app = mock(FSAppAttempt.class);
    Mockito.when(app.getDemand()).thenReturn(maxResource);
    Mockito.when(app.getResourceUsage()).thenReturn(Resources.none());

    schedulable.addApp(app, true);
    schedulable.addApp(app, true);

    schedulable.updateDemand();

    assertTrue("Demand is greater than max allowed ",
        Resources.equals(schedulable.getDemand(), maxResource));
  }

  @Test (timeout = 5000)
  public void test() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\"></queue>");
    out.println("<queue name=\"queueB\"></queue>");
    out.println("</allocations>");
    out.close();

    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    for(FSQueue queue: scheduler.getQueueManager().getQueues()) {
      assertEquals(queue.getMetrics().getMaxApps(), Integer.MAX_VALUE);
      assertEquals(queue.getMetrics().getSchedulingPolicy(),
          SchedulingPolicy.DEFAULT_POLICY.getName());
    }

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    scheduler.update();

    // Queue A wants 3 * 1024. Node update gives this all to A
    createSchedulingRequest(3 * 1024, "queueA", "user1");
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    // Queue B arrives and wants 1 * 1024
    createSchedulingRequest(1 * 1024, "queueB", "user1");
    scheduler.update();
    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());
  }

  @Test
  public void testConcurrentAccess() {
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    String queueName = "root.queue1";
    final FSLeafQueue schedulable = scheduler.getQueueManager().
      getLeafQueue(queueName, true);
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    final FSAppAttempt app =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1",
            schedulable, null, rmContext);

    // this needs to be in sync with the number of runnables declared below
    int testThreads = 2;
    List<Runnable> runnables = new ArrayList<Runnable>();

    // add applications to modify the list
    runnables.add(new Runnable() {
      @Override
      public void run() {
        for (int i=0; i < 500; i++) {
          schedulable.addApp(app, true);
        }
      }
    });

    // iterate over the list a couple of times in a different thread
    runnables.add(new Runnable() {
      @Override
      public void run() {
        for (int i=0; i < 500; i++) {
          schedulable.getResourceUsage();
        }
      }
    });

    final List<Throwable> exceptions = Collections.synchronizedList(
        new ArrayList<Throwable>());
    final ExecutorService threadPool = HadoopExecutors.newFixedThreadPool(
        testThreads);

    try {
      final CountDownLatch allExecutorThreadsReady =
          new CountDownLatch(testThreads);
      final CountDownLatch startBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(testThreads);
      for (final Runnable submittedTestRunnable : runnables) {
        threadPool.submit(new Runnable() {
          public void run() {
            allExecutorThreadsReady.countDown();
            try {
              startBlocker.await();
              submittedTestRunnable.run();
            } catch (final Throwable e) {
              exceptions.add(e);
            } finally {
              allDone.countDown();
            }
          }
        });
      }
      // wait until all threads are ready
      allExecutorThreadsReady.await();
      // start all test runners
      startBlocker.countDown();
      int testTimeout = 2;
      assertTrue("Timeout waiting for more than " + testTimeout + " seconds",
          allDone.await(testTimeout, TimeUnit.SECONDS));
    } catch (InterruptedException ie) {
      exceptions.add(ie);
    } finally {
      threadPool.shutdownNow();
    }
    assertTrue("Test failed with exception(s)" + exceptions,
        exceptions.isEmpty());
  }
}
