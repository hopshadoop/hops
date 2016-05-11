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
package io.hops.metadata.util;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHopYarnAPIUtilities {


  private static final Log LOG = LogFactory.
      getLog(TestHopYarnAPIUtilities.class);
  private YarnConfiguration conf;
  private final int GB = 1024;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final String appType = "MockApp";

  @Before
  public void setup()
      throws StorageInitializtionException, StorageException, IOException {
    LOG.info("Setting up Factories");
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class, ResourceScheduler.class);
  }

  @Test(timeout = 30000)
  public void testRPCPersistence() throws IOException {
    int rpcID = HopYarnAPIUtilities.getRPCID();
    RPC.Type type = RPC.Type.RegisterApplicationMaster;
    byte[] array = type.toString().getBytes();
    RMUtilities.persistAppMasterRPC(rpcID, type, array);

    rpcID = HopYarnAPIUtilities.getRPCID();
    type = RPC.Type.FinishApplicationMaster;
    array = type.toString().getBytes();
    RMUtilities.persistAppMasterRPC(rpcID, type, array);

    rpcID = HopYarnAPIUtilities.getRPCID();
    type = RPC.Type.Allocate;
    array = type.toString().getBytes();
    RMUtilities.persistAppMasterRPC(rpcID, type, array);

    //TODO verify that the rpc are in the db
  }

  @Test(timeout = 30000)
  public void random() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    //register one NodeManager
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    //submit an application of 2GB memory
    RMApp app1 = rm.submitApp(3 * GB, "", "user1", null, "queue1");
    RMApp app2 = rm.submitApp(3 * GB, "", "user2", null, "queue2");

    /**
     * ************************
     * THIS TEST PASSES AS IT IS IF WE INCREASE THE MEMORY OF ONE OF THE 2 APPS,
     * THEN IT WILL FAIL AS THE
     * ENTIRE CLUSTER CAPACITY IS 6 GB
     * APPATTEMPT STATE WILL NOT BE ALLOCATED AND AN EXCEPTION WILL BE THROWN
     *************************
     */
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();

    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(2000);

  }

  //TODO most of this test is useless since we do not persist the resousrce anymore
  // clean the test
  @Test(timeout = 60000)
  public void test() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    //register two NodeManagers
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("127.0.0.2:5678", 4 * GB);
    Thread.sleep(1000);
    //verify that they are persisted in the db
    Map<String,FiCaSchedulerNode> dbNodes = RMUtilities.
            getAllFiCaSchedulerNodesFullTransaction();
    assertEquals(2, dbNodes.size());

    //submit an application of 2GB memory
    RMApp app1 = rm.submitApp(2048);
    Thread.sleep(2000);
    //at this point tables :
    //ha_fifoscheduler_apps,
    //ha_schedulerapplication
    //should container one row for the specific app. You can verify that looking at the db.

    //also the tables :
    //ha_appschedulinginfo,
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();

    //get the scheduler
    FifoScheduler fifoScheduler = (FifoScheduler) rm.getResourceScheduler();

    //get applications map
    Map<ApplicationId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication>
        apps = fifoScheduler.
        getSchedulerApplications();
    //get nodes map
    Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>
        nodes = fifoScheduler.getNodes();

    //get current application
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        sA = apps.get(app1.getApplicationId());
    //get currentApplicationAttemptId
    SchedulerApplicationAttempt saAttempt = sA.getCurrentAppAttempt();

    List<org.apache.hadoop.yarn.api.records.ResourceRequest> reqs =
        saAttempt.getAppSchedulingInfo().
            getAllResourceRequests();
    //retrieve requests from the database
    Map<String, List<ResourceRequest>> requests =
        RMUtilities.getAllResourceRequestsFullTransaction();
    List<ResourceRequest> dbReqs = requests.get(attempt1.
        getAppAttemptId().toString());

    //compare
    assertEquals(reqs.size(), dbReqs.size());

    //its time to kick the scheduling, 2GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    //Thread.sleep(1000);

    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    SchedulerNodeReport report_nm1 =
        fifoScheduler.getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());

    //get newlyAllocatedContainers
    List<RMContainer> newlyAllocatedContainers = saAttempt.
        getNewlyAllocatedContainers();
    //get launchedContainers
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
        ficaNode = nodes.get(nm1.getNodeId());
    List<RMContainer> launchedContainers = ficaNode.getRunningContainers();

    //get liveContainers
    Map<ContainerId, RMContainer> liveContainers = saAttempt.
        getLiveContainersMap();


    //submit a second application of 2GB memory
    RMApp app2 = rm.submitApp(2048);
    // kick the scheduling, 2GB given to AM, remaining 2 GB on nm2
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    SchedulerNodeReport report_nm2 =
        fifoScheduler.getNodeReport(nm2.getNodeId());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());


    // add request for containers
    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // add request for containers
    am2.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 3 * GB, 0, 1);
    AllocateResponse alloc2Response = am2.schedule(); // send the request

    // kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0 GB to nm1
    nm1.nodeHeartbeat(true);
    //Thread.sleep(1000);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      alloc1Response = am1.schedule();
    }
    while (alloc2Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(1000);
      alloc2Response = am2.schedule();
    }
    // kick the scheduler, nothing given remaining 2 GB to nm2
    nm2.nodeHeartbeat(true);
    Thread.sleep(1000);

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    List<Container> allocated2 = alloc2Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated2.size());
    Assert.assertEquals(3 * GB, allocated2.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated2.get(0).getNodeId());

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    report_nm2 = rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getAvailableResource().getMemory());

    Assert.assertEquals(6 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());


    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(2000);
  }

  @Test(timeout = 60000)
  public void testAppSubmissionAndNodeUpdate() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    ClientRMService rmService = rm.getClientRMService();

    GetApplicationsRequest getRequest = GetApplicationsRequest
        .newInstance(EnumSet.of(YarnApplicationState.KILLED));

    ApplicationId appId1 = getApplicationId(100);
    ApplicationId appId2 = getApplicationId(101);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    SubmitApplicationRequest submitRequest1 =
        mockSubmitAppRequest(appId1, null, null);

    SubmitApplicationRequest submitRequest2 =
        mockSubmitAppRequest(appId2, null, null);

    try {
      rmService.submitApplication(submitRequest1);
      rmService.submitApplication(submitRequest2);

    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }

    assertEquals("Incorrect number of apps in the RM", 0,
        rmService.getApplications(getRequest).getApplicationList().size());
    Thread.sleep(1000);

    //test persistance of schedulerapplication
    Map<String, SchedulerApplication> schedulerApplications = RMUtilities.
        getSchedulerApplicationsFullTransaction();
    assertEquals("db does not contain good number of schedulerApplications", 2,
        schedulerApplications.size());

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat2.
        getNextHeartBeatInterval());

    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(2000);
  }

  @Test(timeout = 30000)
  public void testForceKillApplication() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    ClientRMService rmService = rm.getClientRMService();
    GetApplicationsRequest getRequest = GetApplicationsRequest
        .newInstance(EnumSet.of(YarnApplicationState.KILLED));

    ApplicationId appId1 = getApplicationId(100);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    SubmitApplicationRequest submitRequest1 =
        mockSubmitAppRequest(appId1, null, null);

    try {
      rmService.submitApplication(submitRequest1);

    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }

    assertEquals("Incorrect number of apps in the RM", 0,
        rmService.getApplications(getRequest).getApplicationList().size());
    Thread.sleep(1000);
    //TODO: check what have to be present in the db
    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(2000);
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue) {
    return mockSubmitAppRequest(appId, name, queue, null);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue, Set<String> tags) {
    return mockSubmitAppRequest(appId, name, queue, tags, false);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue, Set<String> tags, boolean unmanaged) {

    //    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);
    ContainerLaunchContext amContainerSpec = new ContainerLaunchContextPBImpl();
    org.apache.hadoop.yarn.api.records.Resource resource = Resources
        .createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext submissionContext = recordFactory.
        newRecordInstance(ApplicationSubmissionContext.class);
    submissionContext.setAMContainerSpec(amContainerSpec);
    submissionContext.setApplicationName(name);
    submissionContext.setQueue(queue);
    submissionContext.setApplicationId(appId);
    submissionContext.setResource(resource);
    submissionContext.setApplicationType(appType);
    submissionContext.setApplicationTags(tags);
    submissionContext.setUnmanagedAM(unmanaged);

    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }

}
