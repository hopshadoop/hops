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
package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.rmstatestore.*;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceBlacklistRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TestCascadeRemoval {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
    }

    @Test
    public void testRemoveAllocateResponse() throws Exception {
        TransactionState transactionState =
                new TransactionStateImpl(TransactionState.TransactionType.RM);
        ApplicationId appId0 =
                ApplicationId.newInstance(System.currentTimeMillis(), 0);
        ApplicationAttemptId appAttId0 =
                ApplicationAttemptId.newInstance(appId0, 0);

        List<Container> allocatedContainers0 = new ArrayList<Container>();
        allocatedContainers0.add(createContainer(appAttId0, 0));
        allocatedContainers0.add(createContainer(appAttId0, 1));

        List<ContainerStatus> complCont0 = new ArrayList<ContainerStatus>();
        complCont0.add(createContainerStatus(appAttId0, 20));
        complCont0.add(createContainerStatus(appAttId0, 21));

        ((TransactionStateImpl) transactionState)
                .addAllocateResponse(appAttId0, new ApplicationMasterService
                        .AllocateResponseLock(createAllocateResponse(1,
                        allocatedContainers0, complCont0)));

        ApplicationId appId1 =
                ApplicationId.newInstance(System.currentTimeMillis(), 1);
        ApplicationAttemptId appAttId1 =
                ApplicationAttemptId.newInstance(appId1, 1);

        List<Container> allocatedContainers1 = new ArrayList<Container>();
        allocatedContainers1.add(createContainer(appAttId1, 2));
        allocatedContainers1.add(createContainer(appAttId1, 3));

        List<ContainerStatus> complCont1 = new ArrayList<ContainerStatus>();
        complCont1.add(createContainerStatus(appAttId1, 22));
        complCont1.add(createContainerStatus(appAttId1, 23));

        ((TransactionStateImpl) transactionState)
                .addAllocateResponse(appAttId1, new ApplicationMasterService
                        .AllocateResponseLock(createAllocateResponse(2,
                        allocatedContainers1, complCont1)));

        ApplicationId appId2 =
                ApplicationId.newInstance(System.currentTimeMillis(), 2);
        ApplicationAttemptId appAttId2 =
                ApplicationAttemptId.newInstance(appId2, 2);

        List<Container> allocatedContainers2 = new ArrayList<Container>();
        allocatedContainers2.add(createContainer(appAttId2, 4));
        allocatedContainers2.add(createContainer(appAttId2, 5));

        List<ContainerStatus> complCont2 = new ArrayList<ContainerStatus>();
        complCont2.add(createContainerStatus(appAttId2, 24));
        complCont2.add(createContainerStatus(appAttId2, 25));

        ((TransactionStateImpl) transactionState)
                .addAllocateResponse(appAttId2, new ApplicationMasterService
                        .AllocateResponseLock(createAllocateResponse(3,
                        allocatedContainers2, complCont2)));

        transactionState.decCounter(TransactionState.TransactionType.RM);

        Thread.sleep(2000);

        // Verify that are persisted
        AllocateResponseDataAccess allocRespDAO =
                (AllocateResponseDataAccess) RMStorageFactory
                        .getDataAccess(AllocateResponseDataAccess.class);
        Map<String, io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse> allocRespRes =
                allocRespDAO.getAll();
        Assert.assertEquals("There should be three application attempt IDs for allocated response", 3,
                allocRespRes.size());

        AllocatedContainersDataAccess allocContDAO =
                (AllocatedContainersDataAccess) RMStorageFactory
                .getDataAccess(AllocatedContainersDataAccess.class);
        Map<String, List<String>> allocContRes = allocContDAO.getAll();
        Assert.assertEquals("There should be three application attempt ID for allocated containers", 3,
                allocContRes.size());

        CompletedContainersStatusDataAccess complContDAO =
                (CompletedContainersStatusDataAccess) RMStorageFactory
                .getDataAccess(CompletedContainersStatusDataAccess.class);
        Map<String, byte[]> complContRes = complContDAO.getAll();

        Assert.assertEquals("There should be three application attempt IDs for completed containers", 3,
                complContRes.size());

        // Removing AllocateResponse should cascade the removal of allocated containers
        transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        ((TransactionStateImpl) transactionState)
                .removeAllocateResponse(appAttId0, 1,
                        stringifyContainers(allocatedContainers0),
                        stringifyContainerStatuses(complCont0));
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(2000);

        allocRespRes = allocRespDAO.getAll();
        Assert.assertEquals("There should be two application attempt IDs for allocated response", 2,
                allocRespRes.size());

        allocContRes = allocContDAO.getAll();
        Assert.assertEquals("There should be two application attempt IDs for allocated containers", 2,
                allocContRes.size());
        Assert.assertTrue("Application attempt ID " + appAttId1.toString() + " should be there",
                allocContRes.containsKey(appAttId1.toString()));
        Assert.assertTrue("Application attempt ID " + appAttId2.toString() + " should be there",
                allocContRes.containsKey(appAttId2.toString()));

        complContRes = complContDAO.getAll();
        Assert.assertEquals("There should be two application attempt IDs for completed containers", 2,
                complContRes.size());
        Assert.assertTrue("Application attempt ID " + appAttId1.toString() + " should be there",
                complContRes.containsKey(appAttId1.toString()));
        Assert.assertTrue("Application attempt ID " + appAttId2.toString() + " should be there",
                complContRes.containsKey(appAttId2.toString()));

        // Remove the other two
        transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        ((TransactionStateImpl) transactionState)
                .removeAllocateResponse(appAttId1, 2,
                        stringifyContainers(allocatedContainers1),
                        stringifyContainerStatuses(complCont1));
        ((TransactionStateImpl) transactionState)
                .removeAllocateResponse(appAttId2, 3,
                        stringifyContainers(allocatedContainers2),
                        stringifyContainerStatuses(complCont2));
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(2000);

        allocContRes = allocContDAO.getAll();
        Assert.assertTrue("There should be no application attempt IDs for allocated containers",
                allocContRes.isEmpty());
        allocRespRes = allocRespDAO.getAll();
        Assert.assertTrue("There should be no application attempt IDs for allocated response",
                allocRespRes.isEmpty());
        complContRes = complContDAO.getAll();
        Assert.assertTrue("There should be no application attempt IDs for completed containers",
                complContRes.isEmpty());
    }

    @Test
    public void testRemoveFiCaSchedulerNode() throws Exception {
        Configuration conf = new YarnConfiguration();
        MockRM rm = new MockRM(conf);
        rm.start();

        TransactionState transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);

        RMNode rmNode0 = new RMNodeImpl(
                NodeId.newInstance("127.0.0.1", 1234),
                rm.getRMContext(),
                "host0",
                9090,
                8080,
                new NodeBase("/:somepath0"),
                ResourceOption.newInstance(
                        Resource.newInstance(6 * 1024, 6),
                        10),
                "1.0");

        RMNode rmNode1 = new RMNodeImpl(
                NodeId.newInstance("127.0.0.1", 1235),
                rm.getRMContext(),
                "host1",
                9090,
                8080,
                new NodeBase("/:somepath1"),
                ResourceOption.newInstance(
                        Resource.newInstance(6 * 1024, 6),
                        10),
                "1.0");

        FiCaSchedulerNode ficaNode0 = new FiCaSchedulerNode(rmNode0, false, rm.getRMContext());
        ApplicationId appId0 =
                ApplicationId.newInstance(System.currentTimeMillis(), 0);
        ApplicationAttemptId appAttId0 =
                ApplicationAttemptId.newInstance(appId0, 0);
        RMContainer rmContainer00 = new RMContainerImpl(createContainer(appAttId0, 1),
                appAttId0, NodeId.newInstance("127.0.0.1", 1234), "user",
                rm.getRMContext(), transactionState);
        ficaNode0.allocateContainer(appId0, rmContainer00, transactionState);
        RMContainer rmContainer01 = new RMContainerImpl(createContainer(appAttId0, 2),
                appAttId0, NodeId.newInstance("127.0.0.1", 1234), "user",
                rm.getRMContext(), transactionState);
        ficaNode0.allocateContainer(appId0, rmContainer01, transactionState);


        FiCaSchedulerNode ficaNode1 = new FiCaSchedulerNode(rmNode1, false, rm.getRMContext());
        ApplicationId appId1 =
                ApplicationId.newInstance(System.currentTimeMillis(), 1);
        ApplicationAttemptId appAttId1 =
                ApplicationAttemptId.newInstance(appId1, 1);
        RMContainer rmContainer10 = new RMContainerImpl(createContainer(appAttId1, 10),
                appAttId1, NodeId.newInstance("127.0.0.1", 1235), "user",
                rm.getRMContext(), transactionState);
        ficaNode1.allocateContainer(appId1, rmContainer10, transactionState);
        RMContainer rmContainer11 = new RMContainerImpl(createContainer(appAttId1, 11),
                appAttId1, NodeId.newInstance("127.0.0.1", 1235), "user",
                rm.getRMContext(), transactionState);
        ficaNode1.allocateContainer(appId1, rmContainer11, transactionState);


        ((TransactionStateImpl) transactionState)
                .addFicaSchedulerNodeInfoToAdd(ficaNode0.getNodeID().toString(), ficaNode0);
        ((TransactionStateImpl) transactionState)
                .addFicaSchedulerNodeInfoToAdd(ficaNode1.getNodeID().toString(), ficaNode1);
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(2000);

        // Verify they are persisted
        FiCaSchedulerNodeDataAccess ficaNodeDAO =
                (FiCaSchedulerNodeDataAccess) RMStorageFactory
                .getDataAccess(FiCaSchedulerNodeDataAccess.class);
        Map<String, io.hops.metadata.yarn.entity.FiCaSchedulerNode> ficaNodeRes =
                ficaNodeDAO.getAll();
        Assert.assertEquals("There should be two RMNode IDs", 2,
                ficaNodeRes.size());
        Assert.assertTrue("RMNode " + rmNode0.getNodeID().toString() + " should be there",
                ficaNodeRes.containsKey(rmNode0.getNodeID().toString()));
        Assert.assertTrue("RMNode " + rmNode1.getNodeID().toString() + " should be there",
                ficaNodeRes.containsKey(rmNode1.getNodeID().toString()));

        LaunchedContainersDataAccess launchedDAO =
                (LaunchedContainersDataAccess) RMStorageFactory
                .getDataAccess(LaunchedContainersDataAccess.class);
        Map<String, List<LaunchedContainers>> launchedRes =
                launchedDAO.getAll();
        Assert.assertEquals("There should be two FiCa Nodes", 2,
                launchedRes.size());
        Assert.assertEquals("FiCa node " + ficaNode0.getNodeID().toString() + " should have two launched containers",
                2, launchedRes.get(ficaNode0.getNodeID().toString()).size());
        Assert.assertEquals("FiCa node " + ficaNode1.getNodeID().toString() + " should have two launched containers",
                2, launchedRes.get(ficaNode1.getNodeID().toString()).size());

        // Remove first node
        transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        ((TransactionStateImpl) transactionState)
                .addFicaSchedulerNodeInfoToRemove(ficaNode0.getNodeID().toString(), ficaNode0);
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(2000);

        ficaNodeRes = ficaNodeDAO.getAll();
        Assert.assertEquals("By now there should be only one RMNode", 1,
                ficaNodeRes.size());
        Assert.assertTrue("RMNode " + rmNode1.getNodeID().toString() + " should still be there",
                ficaNodeRes.containsKey(rmNode1.getNodeID().toString()));

        launchedRes = launchedDAO.getAll();
        Assert.assertEquals("By now there should be only one FiCa node", 1,
                launchedRes.size());
        Assert.assertEquals("FiCa node " + ficaNode1.getNodeID().toString() + " should have two launched container",
                2, launchedRes.get(ficaNode1.getNodeID().toString()).size());

        // Remove the second one
        transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        ((TransactionStateImpl) transactionState)
                .addFicaSchedulerNodeInfoToRemove(ficaNode1.getNodeID().toString(), ficaNode1);
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(2000);

        ficaNodeRes = ficaNodeDAO.getAll();
        Assert.assertTrue("No RMNodes should exist",
                ficaNodeRes.isEmpty());

        launchedRes = launchedDAO.getAll();
        Assert.assertTrue("No FiCa node launched containers should exist",
                launchedRes.isEmpty());

        rm.stop();
    }

    @Test
    public void testRemoveApplicationState() throws Exception {
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 0);
        MockRM rm = new MockRM(conf);
        rm.start();

        MockNM nm0 = rm.registerNode("host0:1234", 6 * 1024, 6);

        RMApp app0 = rm.submitApp(1024, "name", "user", null, "queue1");
        RMApp app1 = rm.submitApp(1024, "name", "user", null, "queue1");

        nm0.nodeHeartbeat(true);

        RMAppAttempt appAtt0 = app0.getCurrentAppAttempt();

        Thread.sleep(2000);
        MockAM am0 = rm.sendAMLaunched(appAtt0.getAppAttemptId());
        am0.registerAppAttempt();

        // Verify RMApp is in the correct state
        Assert.assertEquals("RMApp " + app1.getApplicationId().toString() + " should be in state" +
                        "ACCEPTED", RMAppState.ACCEPTED,
                app1.getState());

        // Send attempt failed event in order to create a new one
        TransactionState ts = new TransactionStateImpl(TransactionState.TransactionType.APP);
        RMAppEvent appAttFailed = new RMAppFailedAttemptEvent(app1.getApplicationId(),
                RMAppEventType.ATTEMPT_FAILED, "Failed", true, ts);
        app1.handle(appAttFailed);
        ts.decCounter(TransactionState.TransactionType.APP);

        Thread.sleep(2000);

        // Verify app state and app attempt are persisted
        ApplicationStateDataAccess appStateDAO =
                (ApplicationStateDataAccess) RMStorageFactory
                .getDataAccess(ApplicationStateDataAccess.class);
        List<ApplicationState> appStateRes = appStateDAO.getAll();
        Assert.assertEquals("There should be two applications persisted", 2,
                appStateRes.size());

        ApplicationAttemptStateDataAccess appAttDAO =
                (ApplicationAttemptStateDataAccess) RMStorageFactory
                .getDataAccess(ApplicationAttemptStateDataAccess.class);
        Map<String, List<ApplicationAttemptState>> appAttRes =
                appAttDAO.getAll();

        Assert.assertEquals("There should be two applications sets for app attempt persisted", 2,
                appAttRes.size());
        Assert.assertEquals("App " + app1.getApplicationId() + " should have two app attempts", 2,
                appAttRes.get(app1.getApplicationId().toString()).size());

        // Remove app1
        am0.unregisterAppAttempt();
        ts = new TransactionStateImpl(TransactionState.TransactionType.APP);

        rm.getRMAppManager().handle(new RMAppManagerEvent(
                app1.getApplicationId(),
                RMAppManagerEventType.APP_COMPLETED,
                ts));
        Thread.sleep(2000);
        ts.decCounter(TransactionState.TransactionType.APP);
        Thread.sleep(3000);

        appStateRes = appStateDAO.getAll();
        Assert.assertEquals("There should be one application by now", 1,
                appStateRes.size());

        appAttRes = appAttDAO.getAll();
        Assert.assertEquals("There should be one application set for app attempts by now", 1,
                appAttRes.size());
        Assert.assertTrue("Application " + app0.getApplicationId() + " should have app attempts set",
                appAttRes.containsKey(app0.getApplicationId().toString()));
        Assert.assertEquals("Application " + app0.getApplicationId() + " has one app attempt", 1,
                appAttRes.get(app0.getApplicationId().toString()).size());

        rm.stop();
    }

    @Test
    public void testRemoveSchedulerApplication() throws Exception {
        // It should also succeed with Capacity scheduler
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);

        MockRM rm = new MockRM(conf);
        rm.start();

        MockNM nm0 = rm.registerNode("host0:1234", 6 * 1024, 6);

        RMApp app0 = rm.submitApp(1024, "name", "user", null, "default");
        RMApp app1 = rm.submitApp(1024, "name", "user", null, "default");
        nm0.nodeHeartbeat(true);

        RMAppAttempt appAtt0 = app0.getCurrentAppAttempt();
        RMAppAttempt appAtt1 = app1.getCurrentAppAttempt();

        MockAM am0 = rm.sendAMLaunched(appAtt0.getAppAttemptId());
        am0.registerAppAttempt();

        MockAM am1 = rm.sendAMLaunched(appAtt1.getAppAttemptId());
        am1.registerAppAttempt();

        Thread.sleep(2000);

        // Verify everything is persisted
        SchedulerApplicationDataAccess schAppDAO =
                (SchedulerApplicationDataAccess) RMStorageFactory
                .getDataAccess(SchedulerApplicationDataAccess.class);
        Map<String, SchedulerApplication> schAppRes =
                schAppDAO.getAll();
        Assert.assertEquals("There should be two application IDs", 2,
                schAppRes.size());

        AppSchedulingInfoDataAccess appSchInfoDAO =
                (AppSchedulingInfoDataAccess) RMStorageFactory
                .getDataAccess(AppSchedulingInfoDataAccess.class);
        List<AppSchedulingInfo> appSchInfoRes =
                appSchInfoDAO.findAll();
        Assert.assertEquals("There should be two app scheduling infos", 2,
                appSchInfoRes.size());

        // Kill an app
        rm.killApp(app0.getApplicationId());

        Thread.sleep(2000);

        schAppRes = schAppDAO.getAll();
        Assert.assertEquals("By now there should be only one app", 1,
                schAppRes.size());
        Assert.assertTrue("Only app " + app1.getApplicationId().toString() + " should exist",
                schAppRes.containsKey(app1.getApplicationId().toString()));

        appSchInfoRes = appSchInfoDAO.findAll();
        Assert.assertEquals("There should be only one app scheduling info", 1,
                appSchInfoRes.size());
        Assert.assertEquals("And that should be for app " + app1.getApplicationId().toString(),
                app1.getApplicationId().toString(),
                appSchInfoRes.get(0).getAppId());

        rm.stop();
    }

    @Test
    public void testRemoveAppSchedulingInfo() throws Exception {
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);

        MockRM rm = new MockRM(conf);
        rm.start();

        MockNM nm = rm.registerNode("host0:1234", 8 * 1024, 8);

        RMApp app0 = rm.submitApp(1024, "name", "user", null, "default");
        RMApp app1 = rm.submitApp(1024, "name", "user", null, "default");
        nm.nodeHeartbeat(true);

        RMAppAttempt appAtt0 = app0.getCurrentAppAttempt();
        RMAppAttempt appAtt1 = app1.getCurrentAppAttempt();

        MockAM am0 = rm.sendAMLaunched(appAtt0.getAppAttemptId());
        am0.registerAppAttempt();

        MockAM am1 = rm.sendAMLaunched(appAtt1.getAppAttemptId());
        am1.registerAppAttempt();

        am0.allocate("host0", 1024, 3, new ArrayList<ContainerId>());
        nm.nodeHeartbeat(true);

        AllocateRequest allocReq0 = new AllocateRequestPBImpl();
        ResourceBlacklistRequest blackReq0 = new ResourceBlacklistRequestPBImpl();
        List<String> blackListed0 = new ArrayList<String>();
        blackListed0.add("bl0_0");
        blackListed0.add("bl0_1");
        blackReq0.setBlacklistAdditions(blackListed0);
        allocReq0.setResourceBlacklistRequest(blackReq0);
        am0.allocate(allocReq0);

        AllocateRequest allocReq1 = new AllocateRequestPBImpl();
        ResourceBlacklistRequest blackReq1 = new ResourceBlacklistRequestPBImpl();
        List<String> blackListed1 = new ArrayList<String>();
        blackListed1.add("bl1_0");
        blackListed1.add("bl1_1");
        blackListed1.add("bl1_2");
        blackReq1.setBlacklistAdditions(blackListed1);
        allocReq1.setResourceBlacklistRequest(blackReq1);
        am1.allocate(allocReq1);

        nm.nodeHeartbeat(true);

        Thread.sleep(2000);

        // Verify everything is persisted
        AppSchedulingInfoDataAccess appSchDAO =
                (AppSchedulingInfoDataAccess) RMStorageFactory
                .getDataAccess(AppSchedulingInfoDataAccess.class);
        AppSchedulingInfoBlacklistDataAccess appSchBlDAO =
                (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory
                .getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);

        List<AppSchedulingInfo> appSchRes = appSchDAO.findAll();
        Assert.assertEquals("There should be two application attempts", 2,
                appSchRes.size());

        Map<String, List<AppSchedulingInfoBlacklist>> appSchBlRes =
                appSchBlDAO.getAll();
        Assert.assertEquals("There should be two sets of blacklisted resources", 2,
                appSchBlRes.size());
        Assert.assertEquals("Attempt " + appAtt0.getAppAttemptId().toString() + " should have two" +
                "blacklisted resources", 2,
                appSchBlRes.get(appAtt0.getAppAttemptId().toString()).size());
        Assert.assertEquals("Attempt " + appAtt1.getAppAttemptId().toString() + " should have three" +
                        "blacklisted resources", 3,
                appSchBlRes.get(appAtt1.getAppAttemptId().toString()).size());

        rm.killApp(app0.getApplicationId());

        Thread.sleep(2000);

        appSchRes = appSchDAO.findAll();
        Assert.assertEquals("There should be one application attempt by now", 1,
                appSchRes.size());
        Assert.assertEquals("Application attempt " + appAtt1.getAppAttemptId().toString()
                + " should be there", appAtt1.getAppAttemptId().toString(),
                appSchRes.get(0).getSchedulerAppId());

        appSchBlRes = appSchBlDAO.getAll();
        Assert.assertEquals("There should be one set of blacklisted resources", 1,
                appSchBlRes.size());
        Assert.assertEquals("Attempt " + appAtt1.getAppAttemptId().toString() + " should have three" +
                        "blacklisted resources", 3,
                appSchBlRes.get(appAtt1.getAppAttemptId().toString()).size());

        rm.stop();
    }

    private List<String> stringifyContainers(List<Container> in) {
        List<String> ret = new ArrayList<String>();

        for (Container id : in) {
            ret.add(id.getId().toString());
        }

        return ret;
    }

    private List<String> stringifyContainerStatuses(List<ContainerStatus> in) {
        List<String> ret = new ArrayList<String>();

        for (ContainerStatus id : in) {
            ret.add(id.getContainerId().toString());
        }

        return ret;
    }

    private AllocateResponse createAllocateResponse(int responseId,
                                                    List<Container> allocatedContainers,
                                                    List<ContainerStatus> completedContainers) {

        return AllocateResponse.newInstance(responseId, completedContainers,
                allocatedContainers, new ArrayList<NodeReport>(),
                Resource.newInstance(1024 * 5, 5), AMCommand.AM_RESYNC,
                2, null, new ArrayList<NMToken>());
    }

    private ContainerStatus createContainerStatus(ApplicationAttemptId appAttId,
                                                  int containerId) {
        return ContainerStatus.newInstance(
                ContainerId.newInstance(appAttId, containerId),
                ContainerState.COMPLETE,
                "GOOD",
                ContainerExitStatus.SUCCESS);
    }

    private Container createContainer(ApplicationAttemptId appAttId, int containerId) {
        Container container = Container.newInstance(ContainerId.newInstance(appAttId, containerId),
                NodeId.newInstance("host0", 0), "host0",
                Resource.newInstance(1024 * 4, 4),
                Priority.newInstance(1),
                Token.newInstance("identifier".getBytes(), "someKind",
                        "password".getBytes(), "someService"));
        return container;
    }
}
