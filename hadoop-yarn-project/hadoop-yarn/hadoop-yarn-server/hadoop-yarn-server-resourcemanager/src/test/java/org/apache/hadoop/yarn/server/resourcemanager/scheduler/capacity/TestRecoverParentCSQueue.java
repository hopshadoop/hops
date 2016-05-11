/*
 * Copyright (C) 2015 hops.io.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestRecoverParentCSQueue {

  private static final Log LOG = LogFactory.getLog(
          TestRecoverParentCSQueue.class);

  RMContext rmContext;
  YarnConfiguration conf;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;

  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";
  final static float DELTA = 0.0001f;

  private final ResourceCalculator resourceComparator
          = new DefaultResourceCalculator();

  @Before
  public void setUp() throws Exception {
    rmContext = TestUtils.getMockRMContext();
    conf = new YarnConfiguration();
    csConf = new CapacitySchedulerConfiguration();

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMStorageFactory.getConnector().formatStorage();

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
            Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
            Resources.createResource(16 * GB, 32));
    when(csContext.getClusterResources()).
            thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
            thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
            thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).
            thenReturn(resourceComparator);
  }

  private static final String A = "hop_parent1";
  private static final String B = "hop_parent2";

  private void setupSingleLevelQueues(CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{A, B});

    final String Q_A = CapacitySchedulerConfiguration.ROOT + "." + A;
    conf.setCapacity(Q_A, 30);

    final String Q_B = CapacitySchedulerConfiguration.ROOT + "." + B;
    conf.setCapacity(Q_B, 70);

    LOG.info("Setup top-level queues a and b");
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    doReturn(user).when(application).getUser();
    doReturn(Resources.createResource(0, 0)).when(application).getHeadroom();
    return application;
  }

  private void stubQueueAllocation(final CSQueue queue,
          final Resource clusterResource, final FiCaSchedulerNode node,
          final int allocation) {
    stubQueueAllocation(queue, clusterResource, node, allocation,
            NodeType.NODE_LOCAL);
  }

  private void stubQueueAllocation(final CSQueue queue,
          final Resource clusterResource, final FiCaSchedulerNode node,
          final int allocation, final NodeType type) {

    // Simulate the queue allocation
    doAnswer(new Answer<CSAssignment>() {
      @Override
      public CSAssignment answer(InvocationOnMock invocation) throws Throwable {
        try {
          throw new Exception();
        } catch (Exception e) {
          LOG.info("FOOBAR q.assignContainers q=" + queue.getQueueName()
                  + " alloc=" + allocation + " node=" + node.getNodeName());
        }
        final Resource allocatedResource = Resources.createResource(allocation);
        if (queue instanceof ParentQueue) {
          ((ParentQueue) queue).allocateResource(clusterResource,
                  allocatedResource);
        } else {
          FiCaSchedulerApp app1 = getMockApplication(0, "");
          ((LeafQueue) queue).allocateResource(clusterResource, app1,
                  allocatedResource);
        }

        // Next call - nothing
        if (allocation > 0) {
          doReturn(new CSAssignment(Resources.none(), type)).
                  when(queue).assignContainers(eq(clusterResource), eq(node),
                          any(TransactionState.class));

          // Mock the node's resource availability
          Resource available = node.getAvailableResource();
          doReturn(Resources.subtractFrom(available, allocatedResource)).
                  when(node).getAvailableResource();
        }

        return new CSAssignment(allocatedResource, type);
      }
    }).
            when(queue).assignContainers(eq(clusterResource), eq(node), any(
                            TransactionState.class));
  }

  private float computeQueueAbsoluteUsedCapacity(CSQueue queue,
          int expectedMemory, Resource clusterResource) {
    return (((float) expectedMemory / (float) clusterResource.getMemory()));
  }

  private float computeQueueUsedCapacity(CSQueue queue,
          int expectedMemory, Resource clusterResource) {
    return (expectedMemory
            / (clusterResource.getMemory() * queue.getAbsoluteCapacity()));
  }

  private void verifyQueueMetrics(CSQueue queue,
          int expectedMemory, Resource clusterResource) {
    assertEquals(
            computeQueueAbsoluteUsedCapacity(queue, expectedMemory,
                    clusterResource),
            queue.getAbsoluteUsedCapacity(),
            DELTA);
    assertEquals(
            computeQueueUsedCapacity(queue, expectedMemory, clusterResource),
            queue.getUsedCapacity(),
            DELTA);

  }

}
