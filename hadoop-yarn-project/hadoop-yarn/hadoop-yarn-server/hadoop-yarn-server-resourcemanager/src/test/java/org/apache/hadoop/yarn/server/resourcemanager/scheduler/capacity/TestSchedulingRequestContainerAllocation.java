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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.TargetApplicationsNamespace;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTagWithNamespace;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.and;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.cardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;

public class TestSchedulingRequestContainerAllocation {
  private static final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  public void testIntraAppAntiAffinity() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 5 containers allocated (1 AM + 1 node each).
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(5, schedulerApp.getLiveContainers().size());

    // Similarly, app1 asks 10 anti-affinity containers at different priority,
    // it should be satisfied as well.
    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("reducer"), "reducer");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 9 containers allocated (1 AM + 8 containers).
    Assert.assertEquals(9, schedulerApp.getLiveContainers().size());

    // Test anti-affinity to both of "mapper/reducer", we should only get no
    // container allocated
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("reducer2"), "mapper");
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 10 containers allocated (1 AM + 9 containers).
    Assert.assertEquals(9, schedulerApp.getLiveContainers().size());

    rm1.close();
  }

  @Test
  public void testIntraAppAntiAffinityWithMultipleTags() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
        "tag_1_1", "tag_1_2");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 3 containers allocated (1 AM + 2 task).
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(3, schedulerApp.getLiveContainers().size());

    // app1 asks for 1 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2. With allocation_tag = tag_2_1/tag_2_2
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("tag_2_1", "tag_2_2"),
        "tag_1_1", "tag_1_2");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 4 containers allocated (1 AM + 2 task (first request) +
    // 1 task (2nd request).
    Assert.assertEquals(4, schedulerApp.getLiveContainers().size());

    // app1 asks for 10 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2/tag_2_1/tag_2_2. With allocation_tag = tag_3
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("tag_3"),
        "tag_1_1", "tag_1_2", "tag_2_1", "tag_2_2");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 1 more containers allocated
    // 1 AM + 2 task (first request) + 1 task (2nd request) +
    // 1 task (3rd request)
    Assert.assertEquals(5, schedulerApp.getLiveContainers().size());

    rm1.close();
  }

  /**
   * This UT covers some basic end-to-end inter-app anti-affinity
   * constraint tests. For comprehensive tests over different namespace
   * types, see more in TestPlacementConstraintsUtil.
   * @throws Exception
   */
  @Test
  public void testInterAppAntiAffinity() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 3 anti-affinity containers for the same app. It should
    // only get 3 containers allocated to 3 different nodes..
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    System.out.println("Mappers on HOST0: "
        + rmNodes[0].getAllocationTagsWithCount().get("mapper"));
    System.out.println("Mappers on HOST1: "
        + rmNodes[1].getAllocationTagsWithCount().get("mapper"));
    System.out.println("Mappers on HOST2: "
        + rmNodes[2].getAllocationTagsWithCount().get("mapper"));

    // App1 should get 4 containers allocated (1 AM + 3 mappers).
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(4, schedulerApp.getLiveContainers().size());

    // app2 -> c
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nms[0]);

    // App2 asks for 3 containers that anti-affinity with any mapper,
    // since 3 out of 4 nodes already have mapper containers, all 3
    // containers will be allocated on the other node.
    TargetApplicationsNamespace.All allNs =
        new TargetApplicationsNamespace.All();
    am2.allocateAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, allNs.toString(),
        ImmutableSet.of("foo"), "mapper");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // App2 should get 4 containers allocated (1 AM + 3 container).
    Assert.assertEquals(4, schedulerApp2.getLiveContainers().size());

    // The allocated node should not have mapper tag.
    Assert.assertTrue(schedulerApp2.getLiveContainers()
        .stream().allMatch(rmContainer -> {
          // except the nm host
          if (!rmContainer.getContainer().getNodeId().equals(rmNodes[0])) {
            return !rmContainer.getAllocationTags().contains("mapper");
          }
          return true;
        }));

    // app3 -> c
    RMApp app3 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nms[0]);

    // App3 asks for 3 containers that anti-affinity with any mapper.
    // Unlike the former case, since app3 source tags are also mapper,
    // it will anti-affinity with itself too. So there will be only 1
    // container be allocated.
    am3.allocateAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, allNs.toString(),
        ImmutableSet.of("mapper"), "mapper");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    FiCaSchedulerApp schedulerApp3 = cs.getApplicationAttempt(
        am3.getApplicationAttemptId());

    // App3 should get 2 containers allocated (1 AM + 1 container).
    Assert.assertEquals(2, schedulerApp3.getLiveContainers().size());

    rm1.close();
  }

  @Test
  public void testSchedulingRequestDisabledByDefault() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    boolean caughtException = false;
    try {
      // Since feature is disabled by default, we should expect exception.
      am1.allocateIntraAppAntiAffinity(
          ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
          Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
          "tag_1_1", "tag_1_2");
    } catch (Exception e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    rm1.close();
  }

  @Test
  public void testSchedulingRequestWithNullConstraint() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    PlacementConstraint constraint = targetNotIn("node", allocationTag("t1"))
        .build();
    SchedulingRequest sc = SchedulingRequest
        .newInstance(0, Priority.newInstance(1),
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
            ImmutableSet.of("t1"),
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
            constraint);
    AllocateRequest request = AllocateRequest.newBuilder()
        .schedulingRequests(ImmutableList.of(sc)).build();
    am1.allocate(request);

    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    FiCaSchedulerApp schedApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(2, schedApp.getLiveContainers().size());


    // Send another request with null placement constraint,
    // ensure there is no NPE while handling this request.
    sc = SchedulingRequest
        .newInstance(1, Priority.newInstance(1),
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
            ImmutableSet.of("t2"),
            ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
            null);
    AllocateRequest request1 = AllocateRequest.newBuilder()
        .schedulingRequests(ImmutableList.of(sc)).build();
    am1.allocate(request1);

    for (int i = 0; i < 4; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNodes[i]));
    }

    Assert.assertEquals(4, schedApp.getLiveContainers().size());

    rm1.close();
  }

  private void doNodeHeartbeat(MockNM... nms) throws Exception {
    for (MockNM nm : nms) {
      nm.nodeHeartbeat(true);
    }
  }

  private List<Container> waitForAllocation(int allocNum, int timeout,
      MockAM am, MockNM... nms) throws Exception {
    final List<Container> result = new ArrayList<>();
    GenericTestUtils.waitFor(() -> {
      try {
        AllocateResponse response = am.schedule();
        List<Container> allocated = response.getAllocatedContainers();
        System.out.println("Expecting allocation: " + allocNum
            + ", actual allocation: " + allocated.size());
        for (Container c : allocated) {
          System.out.println("Container " + c.getId().toString()
              + " is allocated on node: " + c.getNodeId().toString()
              + ", allocation tags: "
              + String.join(",", c.getAllocationTags()));
        }
        result.addAll(allocated);
        if (result.size() == allocNum) {
          return true;
        }
        doNodeHeartbeat(nms);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return false;
    }, 500, timeout);
    return result;
  }

  private static SchedulingRequest schedulingRequest(int requestId,
      int containers, int cores, int mem, PlacementConstraint constraint,
      String... tags) {
    return schedulingRequest(1, requestId, containers, cores, mem,
        ExecutionType.GUARANTEED, constraint, tags);
  }

  private static SchedulingRequest schedulingRequest(
      int priority, long allocReqId, int containers, int cores, int mem,
      ExecutionType execType, PlacementConstraint constraint, String... tags) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .allocationTags(new HashSet<>(Arrays.asList(tags)))
        .executionType(ExecutionTypeRequest.newInstance(execType, true))
        .resourceSizing(
            ResourceSizing.newInstance(containers,
                Resource.newInstance(mem, cores)))
        .placementConstraintExpression(constraint)
        .build();
  }

  private int getContainerNodesNum(List<Container> containers) {
    Set<NodeId> nodes = new HashSet<>();
    if (containers != null) {
      containers.forEach(c -> nodes.add(c.getNodeId()));
    }
    return nodes.size();
  }

  @Test(timeout = 30000L)
  public void testInterAppCompositeConstraints() throws Exception {
    // This test both intra and inter app constraints.
    // Including simple affinity, anti-affinity, cardinality constraints,
    // and simple AND composite constraints.
    YarnConfiguration config = new YarnConfiguration();
    config.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(config);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234", 100*GB, 100);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 100*GB, 100);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 100*GB, 100);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 100*GB, 100);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 100*GB, 100);

      RMApp app1 = rm.submitApp(1*GB, ImmutableSet.of("hbase"));
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // App1 (hbase)
      // h1: hbase-master(1)
      // h2: hbase-master(1)
      // h3:
      // h4:
      // h5:
      PlacementConstraint pc = targetNotIn("node",
          allocationTag("hbase-master")).build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 2048, pc, "hbase-master")));
      List<Container> allocated = waitForAllocation(2, 3000, am1, nm1, nm2);

      // 2 containers allocated
      Assert.assertEquals(2, allocated.size());
      // containers should be distributed on 2 different nodes
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      // App1 (hbase)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1)
      // h4: hbase-rs(1)
      // h5:
      pc = targetNotIn("node", allocationTag("hbase-rs")).build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(2, 4, 1, 1024, pc, "hbase-rs")));
      allocated = waitForAllocation(4, 3000, am1, nm1, nm2, nm3, nm4, nm5);

      Assert.assertEquals(4, allocated.size());
      Assert.assertEquals(4, getContainerNodesNum(allocated));

      // App2 (web-server)
      // Web server instance has 2 instance and non of them can be co-allocated
      // with hbase-master.
      RMApp app2 = rm.submitApp(1*GB, ImmutableSet.of("web-server"));
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      // App2 (web-server)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1), ws-inst(1)
      // h4: hbase-rs(1), ws-inst(1)
      // h5:
      pc = and(
          targetIn("node", allocationTagWithNamespace(
              new TargetApplicationsNamespace.All().toString(),
              "hbase-master")),
          targetNotIn("node", allocationTag("ws-inst"))).build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 2048, pc, "ws-inst")));
      allocated = waitForAllocation(2, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(2, allocated.size());
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("If ws-inst is allocated to a node,"
                + " this node should have inherited the ws-inst tag ",
            rmNode.getAllocationTagsWithCount().get("ws-inst") == 1);
        Assert.assertTrue("ws-inst should be co-allocated to "
                + "hbase-master nodes",
            rmNode.getAllocationTagsWithCount().get("hbase-master") == 1);
      }

      // App3 (ws-servant)
      // App3 has multiple instances that must be co-allocated
      // with app2 server instance, and each node cannot have more than
      // 3 instances.
      RMApp app3 = rm.submitApp(1*GB, ImmutableSet.of("ws-servants"));
      MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm3);


      // App3 (ws-servant)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1), ws-inst(1), ws-servant(3)
      // h4: hbase-rs(1), ws-inst(1), ws-servant(3)
      // h5:
      pc = and(
          targetIn("node", allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppTag("web-server").toString(),
              "ws-inst")),
          cardinality("node", 0, 2, "ws-servant")).build();
      am3.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 10, 1, 512, pc, "ws-servant")));
      // total 6 containers can be allocated due to cardinality constraint
      // each round, 2 containers can be allocated
      allocated = waitForAllocation(6, 10000, am3, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(6, allocated.size());
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("Node has ws-servant allocated must have 3 instances",
            rmNode.getAllocationTagsWithCount().get("ws-servant") == 3);
        Assert.assertTrue("Every ws-servant container should be co-allocated"
                + " with ws-inst",
            rmNode.getAllocationTagsWithCount().get("ws-inst") == 1);
      }
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 30000L)
  public void testMultiAllocationTagsConstraints() throws Exception {
    // This test simulates to use PC to avoid port conflicts
    YarnConfiguration config = new YarnConfiguration();
    config.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(config);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234", 10*GB, 10);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 10*GB, 10);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 10*GB, 10);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 10*GB, 10);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 10*GB, 10);

      RMApp app1 = rm.submitApp(1*GB, ImmutableSet.of("server1"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm1);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      // App1 uses ports: 7000, 8000 and 9000
      String[] server1Ports =
          new String[] {"port_6000", "port_7000", "port_8000"};
      PlacementConstraint pc = targetNotIn("node",
          allocationTagWithNamespace(AllocationTagNamespaceType.ALL.toString(),
              server1Ports))
          .build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 1024, pc, server1Ports)));
      List<Container> allocated = waitForAllocation(2, 3000,
          am1, nm1, nm2, nm3, nm4, nm5);

      // 2 containers allocated
      Assert.assertEquals(2, allocated.size());
      // containers should be distributed on 2 different nodes
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      // App1 uses ports: 6000
      String[] server2Ports = new String[] {"port_6000"};
      RMApp app2 = rm.submitApp(1*GB, ImmutableSet.of("server2"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm2);
      RMAppAttempt app2attempt1 = app2.getCurrentAppAttempt();
      MockAM am2 = rm.sendAMLaunched(app2attempt1.getAppAttemptId());
      am2.registerAppAttempt();

      pc = targetNotIn("node",
          allocationTagWithNamespace(AllocationTagNamespaceType.ALL.toString(),
              server2Ports))
          .build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 3, 1, 1024, pc, server2Ports)));
      allocated = waitForAllocation(3, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(3, allocated.size());
      Assert.assertEquals(3, getContainerNodesNum(allocated));

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("server2 should not co-allocate to server1 as"
                + " they both need to use port 6000",
            rmNode.getAllocationTagsWithCount().get("port_6000") == 1);
        Assert.assertFalse(rmNode.getAllocationTagsWithCount()
            .containsKey("port_7000"));
        Assert.assertFalse(rmNode.getAllocationTagsWithCount()
            .containsKey("port_8000"));
      }
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 30000L)
  public void testInterAppConstraintsWithNamespaces() throws Exception {
    // This test verifies inter-app constraints with namespaces
    // not-self/app-id/app-tag
    YarnConfiguration config = new YarnConfiguration();
    config.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(config);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234:", 100*GB, 100);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 100*GB, 100);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 100*GB, 100);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 100*GB, 100);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 100*GB, 100);

      ApplicationId app5Id = null;
      Map<ApplicationId, List<Container>> allocMap = new HashMap<>();
      // 10 apps and all containers are attached with foo tag
      for (int i = 0; i<10; i++) {
        // App1 ~ app5 tag "former5"
        // App6 ~ app10 tag "latter5"
        String applicationTag = i<5 ? "former5" : "latter5";
        RMApp app = rm.submitApp(1*GB, ImmutableSet.of(applicationTag));
        // Allocate AM container on nm1
        doNodeHeartbeat(nm1, nm2, nm3, nm4, nm5);
        RMAppAttempt attempt = app.getCurrentAppAttempt();
        MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
        am.registerAppAttempt();

        PlacementConstraint pc = targetNotIn("node", allocationTag("foo"))
            .build();
        am.addSchedulingRequest(
            ImmutableList.of(
                schedulingRequest(1, 3, 1, 1024, pc, "foo")));
        List<Container> allocated = waitForAllocation(3, 3000,
            am, nm1, nm2, nm3, nm4, nm5);
        // Memorize containers that has app5 foo
        if (i == 5) {
          app5Id = am.getApplicationAttemptId().getApplicationId();
        }
        allocMap.put(am.getApplicationAttemptId().getApplicationId(),
            allocated);
      }

      Assert.assertNotNull(app5Id);
      Assert.assertEquals(3, getContainerNodesNum(allocMap.get(app5Id)));

      // *** app-id
      // Submit another app, use app-id constraint against app5
      RMApp app1 = rm.submitApp(1*GB, ImmutableSet.of("xyz"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm1);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      PlacementConstraint pc = targetIn("node",
          allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppID(app5Id).toString(),
              "foo"))
          .build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 3, 1, 1024, pc, "foo")));
      List<Container> allocated = waitForAllocation(3, 3000,
          am1, nm1, nm2, nm3, nm4, nm5);

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      List<Container> app5Alloc = allocMap.get(app5Id);
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("This app is affinity with app-id/app5/foo "
                + "containers",
            app5Alloc.stream().anyMatch(
                c5 -> c5.getNodeId() == c.getNodeId()));
      }

      // *** app-tag
      RMApp app2 = rm.submitApp(1*GB);
      // Allocate AM container on nm1
      doNodeHeartbeat(nm2);
      RMAppAttempt app2attempt1 = app2.getCurrentAppAttempt();
      MockAM am2 = rm.sendAMLaunched(app2attempt1.getAppAttemptId());
      am2.registerAppAttempt();

      pc = targetNotIn("node",
          allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppTag("xyz").toString(),
              "foo"))
          .build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 1024, pc, "foo")));
      allocated = waitForAllocation(2, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(2, allocated.size());

      // none of them can be allocated to nodes that has app5 foo containers
      for (Container c : app5Alloc) {
        Assert.assertNotEquals(c.getNodeId(),
            allocated.iterator().next().getNodeId());
      }

      // *** not-self
      RMApp app3 = rm.submitApp(1*GB);
      // Allocate AM container on nm1
      doNodeHeartbeat(nm3);
      RMAppAttempt app3attempt1 = app3.getCurrentAppAttempt();
      MockAM am3 = rm.sendAMLaunched(app3attempt1.getAppAttemptId());
      am3.registerAppAttempt();

      pc = cardinality("node",
          new TargetApplicationsNamespace.NotSelf().toString(),
          1, 1, "foo").build();
      am3.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 1, 1, 1024, pc, "foo")));
      allocated = waitForAllocation(1, 3000, am3, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(1, allocated.size());
      // All 5 containers should be allocated
      Assert.assertTrue(rmNodes.get(allocated.iterator().next().getNodeId())
          .getAllocationTagsWithCount().get("foo") == 2);
    } finally {
      rm.stop();
    }
  }
}
