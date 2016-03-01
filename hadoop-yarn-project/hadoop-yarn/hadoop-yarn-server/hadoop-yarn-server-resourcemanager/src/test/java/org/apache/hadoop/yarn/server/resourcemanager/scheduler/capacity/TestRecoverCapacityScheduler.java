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
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;

import static org.junit.Assert.*;

public class TestRecoverCapacityScheduler {

  private static final Log LOG = LogFactory.getLog(TestCapacityScheduler.class);
  private final int GB = 1024;
  private final RecordFactory recordFactory
          = RecordFactoryProvider.getRecordFactory(null);

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String C1 = "c1";
  private static final String D = "d";
  private static final String E = "e";
  private YarnConfiguration conf;

  @Before
  public void setUp() throws Exception {

    CapacitySchedulerConfiguration csConf
            = new CapacitySchedulerConfiguration();
    csConf.setBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
    setupQueueConfigurationReservation(csConf);
    conf = new YarnConfiguration(csConf);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();

    conf.setClass(YarnConfiguration.RM_SCHEDULER,
            CapacityScheduler.class, ResourceScheduler.class);
    conf.setClass(YarnConfiguration.RM_STORE,NDBRMStateStore.class , RMStateStore.class);
  }

  private void setupQueueConfigurationReservation(
          CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{A, B, C, D,
      E});

    // root.a setup 
    final String Q_A = CapacitySchedulerConfiguration.ROOT + "." + A;
    conf.setCapacity(Q_A, 8.5f);
    conf.setMaximumCapacity(Q_A, 20);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");

    //root.b setup
    final String Q_B = CapacitySchedulerConfiguration.ROOT + "." + B;
    conf.setCapacity(Q_B, 80);
    conf.setMaximumCapacity(Q_B, 99);
    conf.setAcl(Q_B, QueueACL.SUBMIT_APPLICATIONS, "*");

    //root.c setup
    final String Q_C = CapacitySchedulerConfiguration.ROOT + "." + C;
    conf.setCapacity(Q_C, 1.5f);
    conf.setMaximumCapacity(Q_C, 10);
    conf.setAcl(Q_C, QueueACL.SUBMIT_APPLICATIONS, " ");

    //root.c.c1 setup , here c is the parent to c1
    final String Q_C1 = CapacitySchedulerConfiguration.ROOT + "." + C + "." + C1;
    conf.setQueues(Q_C, new String[]{C1});
    conf.setCapacity(Q_C1, 100);

    //root.d setup
    final String Q_D = CapacitySchedulerConfiguration.ROOT + "." + D;
    conf.setCapacity(Q_D, 9);
    conf.setMaximumCapacity(Q_D, 11);
    conf.setAcl(Q_D, QueueACL.SUBMIT_APPLICATIONS, "user_d");

    //root.e setup
    final String Q_E = CapacitySchedulerConfiguration.ROOT + "." + E;
    conf.setCapacity(Q_E, 1);
    conf.setMaximumCapacity(Q_E, 1);
    conf.setAcl(Q_E, QueueACL.SUBMIT_APPLICATIONS, "user_e");
  }

  @Test(timeout = 900000)
  public void testContainerReservation() throws Exception {

    Map<FiCaSchedulerApp, String> appRMContainers
            = new HashMap<FiCaSchedulerApp, String>();

    RMStorageFactory.getConnector().formatStorage();
    MockRM mockResMan = new MockRM(conf);
    mockResMan.start();

    CapacityScheduler capScheduler = (CapacityScheduler) mockResMan.
            getResourceScheduler();

    // Manipulate queue 'a'
    LeafQueue a = (LeafQueue) capScheduler.getQueue(A);
    //unset maxCapacity
    a.setMaxCapacity(1.0f, null);

    /*
     * This fakecluster resource will be used to manually manipulate the
     * following fields
     * maxActiveApplications
     * ,maxActiveAppsUsingAbsCap,maxActiveApplicationsPerUser
     * if we don't setup these fields, we can not add the application from here
     * which means
     * we it won't submit in to the queue because maxactivapplication etc fields
     * are small.
     * This is manily depends on the nodes we are creating.
     */
    Resource fakeClusterResource = Resources.createResource(100 * 16 * GB, 100
            * 32);

    // Users
    final String user_0 = "sri";
    final String user_1 = "Daniel";

    /*
     * Registering the node with following configuaration, vcore are calculating
     * by using this method - YarnConfiguration.DEFAULT_NM_VCORES =8 ,
     * YarnConfiguration.DEFAULT_NM_PMEM_MB=
     * 8*1024
     * Math.max(1, (memory * YarnConfiguration.DEFAULT_NM_VCORES) /
     * YarnConfiguration.DEFAULT_NM_PMEM_MB),
     */
    MockNM node_1 = mockResMan.registerNode("127.0.0.1:1234", 4 * GB);

    //Check the db
    Thread.sleep(1000);
    Map<String, io.hops.metadata.yarn.entity.FiCaSchedulerNode> dbRmNodes
            = RMUtilities.getAllFiCaSchedulerNodesFullTransaction();
    assertEquals(1, dbRmNodes.size());

    ApplicationId appId_1 = getApplicationId(101);
    TransactionState transaction = getTransactionState(101);

    AppAddedSchedulerEvent appAddedSchedulerEvent_1
            = new AppAddedSchedulerEvent(
                    appId_1, a.getQueueName(), user_0, transaction);
    capScheduler.handle(appAddedSchedulerEvent_1);

    a.initializeApplicationLimits(fakeClusterResource);

    ApplicationId appId_2 = getApplicationId(102);
    AppAddedSchedulerEvent appAddedSchedulerEvent_2
            = new AppAddedSchedulerEvent(
                    appId_2, a.getQueueName(), user_1, transaction);
    capScheduler.handle(appAddedSchedulerEvent_2);

    ApplicationAttemptId attemptId_1 = createAppAttemptId(appId_1.
            getClusterTimestamp(), appId_1.getId(), 1);

    AppAttemptAddedSchedulerEvent appAttempEvent_1
            = new AppAttemptAddedSchedulerEvent(attemptId_1, false, transaction);
    capScheduler.handle(appAttempEvent_1);

    ApplicationAttemptId attemptId_2 = createAppAttemptId(appId_2.
            getClusterTimestamp(), appId_2.getId(), 2);

    AppAttemptAddedSchedulerEvent appAttempEvent_2
            = new AppAttemptAddedSchedulerEvent(attemptId_2, false, transaction);

    capScheduler.handle(appAttempEvent_2);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (4 * GB),
            numNodes * 16);

    //allocate resource to application 1 submitted by user sri
    Priority priority = TestUtils.createMockPriority(1);
    FiCaSchedulerApp app_1 = capScheduler.getApplicationAttempt(attemptId_1);
    app_1.updateResourceRequests(Collections.singletonList(
            TestUtils.
            createResourceRequest(ResourceRequest.ANY, 1 * GB, 2, true,
                    priority, recordFactory)), transaction);

    FiCaSchedulerApp app_2 = capScheduler.getApplicationAttempt(attemptId_2);
    app_2.updateResourceRequests(Collections.singletonList(
            TestUtils.
            createResourceRequest(ResourceRequest.ANY, 4 * GB, 1, true,
                    priority, recordFactory)), transaction);

        // Start testing...
    //TransactionState tsAssignContainer_1 = getTransactionState(107);
    // Only 1 container
    a.
            assignContainers(clusterResource, capScheduler.getNode(node_1.
                            getNodeId()), transaction);
    assertEquals(1 * GB, a.getUsedResources().getMemory());
    assertEquals(1 * GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(1 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(0 * GB, a.getMetrics().getAvailableMB());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.
            assignContainers(clusterResource, capScheduler.getNode(node_1.
                            getNodeId()), transaction);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());

    // Now , Daniel's application should go to reserve state
    a.
            assignContainers(clusterResource, capScheduler.getNode(node_1.
                            getNodeId()), transaction);
    assertEquals(6 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(4 * GB, app_2.getCurrentReservation().getMemory());
    assertEquals(2 * GB, capScheduler.getNode(node_1.getNodeId()).
            getUsedResource().getMemory());
    assertEquals(4 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());

    // Save the container id for the assertions later on
    appRMContainers.put(app_1, app_1.getLiveContainersMap().keySet().iterator().
            next().toString());

    // Now free 1 container from app_0 i.e. 1G
    a.completedContainer(clusterResource, app_1, capScheduler.getNode(node_1.
            getNodeId()), app_1.getLiveContainers().iterator().next(),
            null, RMContainerEventType.KILL, null, transaction);
    a.
            assignContainers(clusterResource, capScheduler.getNode(node_1.
                            getNodeId()), transaction);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(1 * GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(4 * GB, app_2.getCurrentReservation().getMemory());
    assertEquals(1 * GB, capScheduler.getNode(node_1.getNodeId()).
            getUsedResource().getMemory());
    assertEquals(4 * GB, a.getMetrics().getReservedMB());
    assertEquals(1 * GB, a.getMetrics().getAllocatedMB());

    // Now finish another container from app_0 and fulfill the reservation
    a.completedContainer(clusterResource, app_1, capScheduler.getNode(node_1.
            getNodeId()),
            app_1.getLiveContainers().iterator().next(), null,
            RMContainerEventType.KILL, null, transaction);

    a.
            assignContainers(clusterResource, capScheduler.getNode(node_1.
                            getNodeId()), transaction);

    // Save the container id for the assertions later on
    appRMContainers.put(app_2, app_2.getLiveContainersMap().keySet().iterator().
            next().toString());

    // Commit
    transaction.decCounter(TransactionState.TransactionType.RM);

    assertEquals(4 * GB, a.getUsedResources().getMemory());
    assertEquals(0 * GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4 * GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, app_2.getCurrentReservation().getMemory());
    assertEquals(4 * GB, capScheduler.getNode(node_1.getNodeId()).
            getUsedResource().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(4 * GB, a.getMetrics().getAllocatedMB());

    // Retrieve RMContainer data from the db and make assertions
    // App 1
    Thread.sleep(1000);
    Map<String, io.hops.metadata.yarn.entity.RMContainer> containers
            = RMUtilities.getAllRMContainersFulTransaction();
    io.hops.metadata.yarn.entity.RMContainer rmContainer = containers.get(
            appRMContainers.get(app_1));
    assertEquals(RMContainerState.KILLED.toString(), rmContainer.getState());

    // App 2
    //List<RMContainer> list = (List<RMContainer>) app_2.getLiveContainers();
    rmContainer = containers.get(appRMContainers.get(app_2));
    assertEquals(RMContainerState.ALLOCATED.toString(), rmContainer.getState());

    
    //TORECOVER queues and queue metrics are not persisted correctly.
    // Retrieve QueueMetrics data from the db and make assertions
//    List<io.hops.metadata.yarn.entity.QueueMetrics> queueMetricsList
//            = RMUtilities.getAllQueueMetrics();
//    assertEquals(1, queueMetricsList.size());
//
//    io.hops.metadata.yarn.entity.QueueMetrics queueMetrics = queueMetricsList.
//            get(0);
//    assertEquals(4 * GB, queueMetrics.getAllocatedmb());
//    assertEquals(1, queueMetrics.getAllocatedvcores());
//    assertEquals(1, queueMetrics.getAllocatedcontainers());
//    assertEquals(3L, queueMetrics.getAggregatecontainersallocated());

    // Retrieve CSQueue data from the db and make assertions
//    List<io.hops.metadata.yarn.entity.capacity.CSQueue> csQueues = RMUtilities.
//            getAllCSQueues();
//    io.hops.metadata.yarn.entity.capacity.CSQueue rootA = null;
//
//    for (io.hops.metadata.yarn.entity.capacity.CSQueue q : csQueues) {
//      if (q.getPath().equals("root.a")) {
//        rootA = q;
//        break;
//      }
//    }
//
//    assertNotNull(rootA);
//    assertEquals(4 * GB, rootA.getUsedResourceMemory());
//    assertEquals(1, rootA.getUsedResourceVCores());
//    assertEquals(1, rootA.getNumContainers());

    // Retrieve CSLeafQueueUserInfo from the db and make assertions
    Collection<CSLeafQueueUserInfo> leafQueueUserInfoList = RMUtilities.
            getAllCSLeafQueueUserInfoFullTransaction().values();

    for (CSLeafQueueUserInfo leafQueueUserInfo : leafQueueUserInfoList) {
      // sri
      if (leafQueueUserInfo.getUserName().equals(user_0)) {
        assertEquals(0 * GB, leafQueueUserInfo.getConsumedMemory());
        assertEquals(0, leafQueueUserInfo.getConsumedVCores());
      }

      // Daniel
      if (leafQueueUserInfo.getUserName().equals(user_1)) {
        assertEquals(4 * GB, leafQueueUserInfo.getConsumedMemory());
        assertEquals(1, leafQueueUserInfo.getConsumedVCores());
      }
    }

    // Test CapacityScheduler node map
    List<io.hops.metadata.yarn.entity.FiCaSchedulerNode> nodeList
            = new ArrayList<io.hops.metadata.yarn.entity.FiCaSchedulerNode>(
                    RMUtilities.getAllFiCaSchedulerNodesFullTransaction().values());
    io.hops.metadata.yarn.entity.FiCaSchedulerNode node = nodeList.get(0);

    assertEquals(1, nodeList.size());
    assertEquals(1, node.getNumOfContainers());

    // Test application list
    Map<String, io.hops.metadata.yarn.entity.SchedulerApplication> appMap
            = RMUtilities.getSchedulerApplicationsFullTransaction();
    assertEquals(2, appMap.size());

    for (io.hops.metadata.yarn.entity.SchedulerApplication schedulerApp
            : appMap.values()) {
      assertEquals(A, schedulerApp.getQueuename());
    }

    mockResMan.stop();
  }

  @Test(timeout = 90000)
  public void testAMStatusWithRMRestart() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();

    MockNM nm = rm1.registerNode("host0:1234", 10 * GB);
    MockNM nm1 = rm1.registerNode("host1:1234", 10 * GB);

    RMApp application = rm1.submitApp(2 * GB, "", "user1", null, "a");

    nm.nodeHeartbeat(true);

    Thread.sleep(1000);
    RMAppAttempt attempt = application.getCurrentAppAttempt();

    MockAM appManager = rm1.sendAMLaunched(attempt.getAppAttemptId());
    appManager.registerAppAttempt();

    appManager.allocate("host0", 1 * GB, 1, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    List<Container> conts = appManager.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    int waitCount = 0;
    while (contReceived < 1 && waitCount++ < 200) {
      LOG.info(
              "Got " + contReceived + " containers. Waiting to get 1");
      Thread.sleep(100);
      conts = appManager.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      contReceived += conts.size();
      nm.nodeHeartbeat(true);
    }
    Thread.sleep(2000);

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm1.
            getResourceScheduler();
    SchedulerNodeReport nmReport = capacityScheduler.getNodeReport(
            nm.getNodeId());

    // lets check the application resource usage
    assertEquals("2 GB AM and 1 GB another container", 3 * GB, nmReport.getUsedResource()
            .getMemory());
    assertEquals("1 Container for the AM and another one requested", 2,
            nmReport.getNumContainers());

    rm1.stop();

    // Start second RM
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    NDBRMStateStore stateStore = new NDBRMStateStore();
    stateStore.init(conf);
    MockRM rm2 = new MockRM(conf, stateStore);
    rm2.start();
    CapacityScheduler scheduler = (CapacityScheduler) rm2.getResourceScheduler();

    Map<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> apps = scheduler.
            getSchedulerApplications();
    assertTrue("app " + application.getApplicationId() + " was not recovered",
            apps.containsKey(application.getApplicationId()));

    SchedulerApplication<FiCaSchedulerApp> attempt1 = apps.get(application.getApplicationId());

    assertTrue("AM container should be marked", scheduler.getRMContainer(
            attempt.getMasterContainer().getId()).isAMContainer());

    Collection<RMContainer> attempt1Containers = attempt1.getCurrentAppAttempt().getLiveContainers();

    ContainerId nonAMContainer = null;
    for (RMContainer c : attempt1Containers) {
      if (!c.getContainerId().equals(attempt.getMasterContainer().getId())) {
        nonAMContainer = c.getContainerId();
        break;
      }
    }
    assertFalse("Normal container should not be marked as AM", scheduler.getRMContainer(
            nonAMContainer).isAMContainer());

    rm2.stop();
  }

  @Test(timeout = 90000)
  public void testCapacitySchedulerRecovery() throws Exception {

    LOG.info("--- START: testCapacityScheduler ---");
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            ProportionalCapacityPreemptionPolicy.class,
            SchedulingEditPolicy.class);

    MockRM mockResMan = new MockRM(conf);
    mockResMan.start();

    //Vcores are calculating by using thsi in Mock 
    // YarnConfiguration.DEFAULT_NM_VCORES) / YarnConfiguration.DEFAULT_NM_PMEM_MB),
    // so if memory is greater than 1 then memory is equal to vcore
    MockNM nodeManager1 = mockResMan.registerNode("host_0:8888", 10 * GB);
    MockNM nodeManager2 = mockResMan.registerNode("host_1:9999", 15 * GB);

    //submit an application of 2GB memory to queue a , please see the capacity allocation
    // we need to test what would would happen if we submit an application over allocation capacity 
    // to that queue a    
    RMApp application_0 = mockResMan.submitApp(2 * GB, "", "user1", null, "a");
    RMApp application_1 = mockResMan.submitApp(3 * GB, "", "user2", null, "b");
    // RMApp application_2 = mockResMan.submitApp(1 * GB, "", "user3", null, "b1");

    nodeManager1.nodeHeartbeat(true);
    nodeManager2.nodeHeartbeat(true);
    Thread.sleep(1000);
    RMAppAttempt attempt_1 = application_0.getCurrentAppAttempt();
    RMAppAttempt attempt_2 = application_1.getCurrentAppAttempt();
        //RMAppAttempt attempt_3 = application_2.getCurrentAppAttempt();

    // Lets start the application manager to run the submitted application  aftet this call, ha_appschedulinginfo
    //ha+rmcontainer table will be updated 
    MockAM appManager_1 = mockResMan.sendAMLaunched(attempt_1.getAppAttemptId());
    appManager_1.registerAppAttempt();

    MockAM appManager_2 = mockResMan.sendAMLaunched(attempt_2.getAppAttemptId());
    appManager_2.registerAppAttempt();

    // MockAM appManager_3 = mockResMan.sendAMLaunched(attempt_3.getAppAttemptId());
    // appManager_3.registerAppAttempt();
    CapacityScheduler capacityScheduler = (CapacityScheduler) mockResMan.
            getResourceScheduler();

    // Add container to the preemption list and persist it
    RMContainer toPreemptContainer = capacityScheduler.getRMContainer(
            attempt_1.getMasterContainer().getId());
    ProportionalCapacityPreemptionPolicy policy = getPreemptionPolicy(mockResMan);
    Assert.assertNotNull("ProportionalPreemptionPolicy is null", policy);
    if (policy != null) {
      policy.addPreemptedContainer(toPreemptContainer);
      TransactionState transactionState =
              new TransactionStateImpl(TransactionState.TransactionType.RM);
      policy.persistPreempted(transactionState);
      transactionState.decCounter(TransactionState.TransactionType.RM);
    }

    SchedulerNodeReport report_nm1 = capacityScheduler.getNodeReport(
            nodeManager1.getNodeId());

    // lets check the application resource usage
    assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());

    LOG.info("--- END: testCapacityScheduler ---");

    LOG.info("--- END: Starting the second resource manager ---");

    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    NDBRMStateStore stateStore2 = new NDBRMStateStore();
    stateStore2.init(conf);
    MockRM rm2 = new MockRM(conf, stateStore2);
    rm2.start();
    CapacityScheduler scheduler = (CapacityScheduler) rm2.getResourceScheduler();

    Map<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> apps = scheduler.
            getSchedulerApplications();
    assertTrue("app " + application_0.getApplicationId() + " was not recovered",
            apps.containsKey(application_0.getApplicationId()));
    assertTrue("app " + application_1.getApplicationId() + " was not recovered",
            apps.containsKey(application_1.getApplicationId()));

    //check that nodes are recovered properly
    Map<NodeId, FiCaSchedulerNode> afterRecoveryNodes = scheduler.getAllNodes();
    assertEquals(2, afterRecoveryNodes.size());

    // Check that CS preempted containers are recovered
    policy = getPreemptionPolicy(rm2);
    Assert.assertNotNull("ProportionalPreemptionPolicy is null", policy);
    if (policy != null) {
      Map<RMContainer, Long> preemptedContainers = policy.getPreemptedContainers();
      Assert.assertEquals("One preempted container should be there", 1,
              preemptedContainers.size());
      for (Map.Entry<RMContainer, Long> entry : preemptedContainers.entrySet()) {
        Assert.assertEquals("Container: " + toPreemptContainer.getContainerId().toString()
                + " should be there", toPreemptContainer.getContainerId().toString(),
                entry.getKey().getContainerId().toString());
      }
    }

    //TODO, check that the queuemetrics are good
    QueueMetrics queueMetrics = scheduler.getRootQueueMetrics();
    assertTrue("wrong queueMetrics value", queueMetrics.getActiveApps()
            == mockResMan.getResourceScheduler().getRootQueueMetrics().
            getActiveApps());

    mockResMan.stop();
    //Note : 
    //Following tables are updating currently , 
    //ha_csqueue
    //ha_appschedulinginfo
    //ha_csleafqueueuserinfo
    //ha_node
    //ha_rmnode
    //ha_ficascheduler_node
    //ha_appschedulinginfo
    //ha_rmcontainer
    //ha_resourcerequest
    // we have to implement the recover function reload the capacity scheduler
  }


  private ProportionalCapacityPreemptionPolicy getPreemptionPolicy(MockRM rm) {
    List<SchedulingMonitor> monitors = rm.getRMActiveService().getSchedulingMonitors();
    SchedulingEditPolicy policy = null;

    for (SchedulingMonitor mon : monitors) {
      policy = mon.getSchedulingEditPolicy();

      if (policy instanceof ProportionalCapacityPreemptionPolicy) {
        return (ProportionalCapacityPreemptionPolicy) policy;
      }
    }
    return null;
  }

  private TransactionState getTransactionState(int id) {
    TransactionState ts = new TransactionStateImpl(
            TransactionState.TransactionType.RM);
    byte[] allNMRequestData = new byte[10];
    try {
      RMUtilities.persistAppMasterRPC(id, RPC.Type.SubmitApplication,
              allNMRequestData);
    } catch (IOException ex) {
      LOG.error(ex);
    }
    return ts;
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }

  private ApplicationAttemptId createAppAttemptId(long timestamp, int appId,
          int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId attId
            = ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
  }

}
