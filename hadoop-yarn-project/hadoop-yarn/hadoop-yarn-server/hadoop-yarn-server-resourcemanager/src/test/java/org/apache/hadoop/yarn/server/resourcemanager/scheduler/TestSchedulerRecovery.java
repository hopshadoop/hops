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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.TestHopYarnAPIUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSchedulerRecovery {

  private static final Log LOG = LogFactory.
      getLog(TestHopYarnAPIUtilities.class);
  private YarnConfiguration conf;
  private final int GB = 1024;
  private RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private String appType = "MockApp";

  @Before
  public void setup() {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMUtilities.InitializeDB();
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (StorageException ex) {
      LOG.error(ex);
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }

  @Test(timeout = 60000)
  public void testRecovery() throws Exception {
    conf.setClass(YarnConfiguration.RM_STORE, NDBRMStateStore.class,
        RMStateStore.class);
    NDBRMStateStore stateStore = new NDBRMStateStore();
    stateStore.init(conf);
    MockRM rm = new MockRM(conf, stateStore);
    LOG.debug("HOP :: rm.start");
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

    assertEquals("Incorrect number of apps in the RM", 0, rmService.
        getApplications(getRequest).getApplicationList().size());

    Thread.sleep(1000);

    MockNM nm1 = rm.registerNode("host1:1235", 5120);
    MockNM nm2 = rm.registerNode("host2:5679", 10240);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    org.junit.Assert.
        assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    org.junit.Assert.assertEquals(4000, nodeHeartbeat2.
        getNextHeartBeatInterval());

    //    FifoScheduler fifoScheduler = (FifoScheduler) rm.getResourceScheduler();
    //    Map<NodeId, FiCaSchedulerNode> beforeRecoveryNodes = fifoScheduler.getNodes();
    //    assertEquals(2, beforeRecoveryNodes.size());
    //rm.close();
    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);

    LOG.debug("HOP :: rm.stop");
    int rmActiveApps = rm.getResourceScheduler().getRootQueueMetrics().
        getActiveApps();
    rm.stop();
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);

    NDBRMStateStore stateStore2 = new NDBRMStateStore();
    stateStore2.init(conf);
    MockRM rm2 = new MockRM(conf, stateStore2);
    LOG.debug("HOP :: rm2.start");
    rm2.start();
    Thread.sleep(1000);
    FifoScheduler scheduler = (FifoScheduler) rm2.getResourceScheduler();
    //        scheduler.reinitialize(conf, null);
    //        scheduler.recover(null);

    Map<ApplicationId, SchedulerApplication> apps = scheduler.
        getSchedulerApplications();
    Assert.assertTrue("app " + appId1.getId() + " was not recoverd",
        apps.containsKey(appId1));
    Assert.assertTrue("app " + appId2.getId() + " was not recoverd",
        apps.containsKey(appId2));

    //check that nodes are recovered properly
    Map<NodeId, FiCaSchedulerNode> afterRecoveryNodes = scheduler.getNodes();
    for (NodeId id : afterRecoveryNodes.keySet()) {
      LOG.info("node after recovery " + id);
    }
    assertEquals(2, afterRecoveryNodes.size());

    //TODO, check that the queuemetrics are good
    int rm2ActiveApps = scheduler.getRootQueueMetrics().getActiveApps();
    LOG.debug("HOP :: rm2.stop");
    rm2.stop();
    Thread.sleep(2000);
    //    Assert.assertTrue("wrong queueMetrics value", rm2ActiveApps
    //            == rmActiveApps);

  }

  @Test(timeout = 60000)
  /**
   * Test RMNode and RMContextImpl recovery.
   */ public void testRMNodeAndRMContextRecovery() throws Exception {
    NDBRMStateStore stateStore = new NDBRMStateStore();
    stateStore.init(conf);

    //Start first ResourceManager
    MockRM rm = new MockRM(conf, stateStore);
    LOG.debug("HOP :: rm.start");
    rm.start();
    ApplicationId appId1 = getApplicationId(100);
    ApplicationAttemptId a1 =
        createAppAttemptId(appId1.getClusterTimestamp(), appId1.getId(), 1);

    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);

    //Register Node Manager
    MockNM nm1 = rm.registerNode(nodeId.toString(), 5120);
    //Sleep to allow pending retrieval to process event
    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);

    NodeHeartbeatResponse resp1 =
        nm1.nodeHeartbeat(a1, 1, ContainerState.RUNNING);

    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);

    //Start second RM with same NodeID and recover to check if RM state 
    //is properly recovered
    InlineDispatcher rmDispatcher = new InlineDispatcher();
    RMContainerTokenSecretManager containerTokenSecretManager =
        mock(RMContainerTokenSecretManager.class);
    NMTokenSecretManagerInRM nmTokenSecretManager =
        mock(NMTokenSecretManagerInRM.class);
    RMContext rmContext = new RMContextImpl(rmDispatcher, null, null, null,
        mock(DelegationTokenRenewer.class), null, containerTokenSecretManager,
        nmTokenSecretManager, null, null, conf);
    RMNodeImpl node =
        new RMNodeImpl(nodeId, rmContext, nodeId.getHost(), 0, 0, null, null,
            null);

    RMStateStore.RMState rmState = stateStore.loadState(rmContext);
    node.recover(rmState);

    rmContext.recover(rmState);
    //Assert RMNode state
    assertEquals(1, node.getContainersToCleanUp().size());
    assertEquals(0, node.getAppsToCleanup().size());
    assertEquals(0, node.getQueueSize());
    assertEquals(1, ((RMNodeImpl) node).getJustLaunchedContainers().size());
    //Assert RMContextImpl state
    assertEquals(1, rmContext.getActiveRMNodes().size());
    assertEquals(0, rmContext.getInactiveRMNodes().size());

    //Send delayed heartbeat to allow RM to deactivate RMNode
    nm1.nodeHeartbeat(new HashMap<ApplicationId, List<ContainerStatus>>(), true,
        resp1.getResponseId() - 2);
    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);
    rmContext = new RMContextImpl(rmDispatcher, null, null, null,
        mock(DelegationTokenRenewer.class), null, containerTokenSecretManager,
        nmTokenSecretManager, null, null, conf);
    rmContext.recover(rmState);

    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);
    assertEquals(1, RMUtilities
        .getRMContextInactiveNodes(rmContext, new RMStateStore.RMState(), conf)
        .size());
    LOG.debug("HOP :: rm.stop");
    rm.stop();
    Thread.sleep(
        conf.getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
            YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD) *
            2);
  }

  private ApplicationAttemptId createAppAttemptId(long timestamp, int appId,
      int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
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
    Resource resource = Resources.createResource(
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

    //    SubmitApplicationRequest submitRequest =
    //            new SubmitApplicationRequestPBImpl();
    //    submitRequest.setApplicationSubmissionContext(submissionContext);
    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }
}
