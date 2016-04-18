/*
 * Copyright 2016 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.YarnRunningPriceDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.YarnRunningPrice;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.GroupMembershipService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NdbRtStreamingProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.Before;
import org.junit.Test;

public class TestPriceEstimationService {

  private static final Log LOG = LogFactory.getLog(
          TestPriceEstimationService.class);
  private StorageConnector connector = null;
  private Configuration conf = null;
  private final static int WAIT_SLEEP_MS = 100;
  private final int GB = 1024;
  private volatile boolean stopped = false;

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
            "target/test-classes/RT_EventAPIConfig.ini");
    conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
            "target/test-classes/RM_EventAPIConfig.ini");

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }

  @Test
  public void TestPriceEstimation() throws Exception {

    // Starting price estination service
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, 2000);
    conf.setFloat(YarnConfiguration.OVERPRICING_THRESHOLD_MB, 0.2f);
    conf.setFloat(YarnConfiguration.OVERPRICING_THRESHOLD_VC, 0.2f);

    conf.setFloat(YarnConfiguration.BASE_PRICE_PER_TICK_FOR_MEMORY, 50f);
    conf.setFloat(YarnConfiguration.BASE_PRICE_PER_TICK_FOR_VIRTUAL_CORE, 50f);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);

    MockRM rm = new MockRM(conf);
    rm.start();

    //TestStreamingService();
    PriceFixerService ps = new PriceFixerService(rm.getRMContext());
    ps.init(conf);
    ps.start();

    ConsumeSomeResources(rm);
    CheckCurrentRunningPrice(116.00f);

  }

  private void CheckCurrentRunningPrice(float value) throws Exception {
    try {
      LightWeightRequestHandler currentPriceHandler
              = new LightWeightRequestHandler(
                      YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          //Insert Pending Event
          List<PendingEvent> pendingEventsToAdd = new ArrayList<PendingEvent>();
          pendingEventsToAdd.add(new PendingEvent("nodeid", -1, 0, pendingId++));
          //PendingEventDataAccess pendingEventDA =(PendingEventDataAccess) RMStorageFactory.getDataAccess(PendingEventDataAccess.class );
          YarnRunningPriceDataAccess runningPriceDA
                  = (YarnRunningPriceDataAccess) RMStorageFactory.getDataAccess(
                          YarnRunningPriceDataAccess.class);
          Map<Integer, YarnRunningPrice> priceList = runningPriceDA.getAll();

          connector.commit();
          return priceList.get(1).getPrice();
        }
      };
      float currentRunningPrice = (Float) currentPriceHandler.handle();
      Assert.assertEquals(currentRunningPrice, value);
    } catch (IOException ex) {
      LOG.warn("Unable to update container statuses table", ex);
    }
  }

  private void ConsumeSomeResources(MockRM rm) throws Exception {
    // Start the nodes
    MockNM nm1 = rm.registerNode("h1:1234", 5 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 10 * GB);

    RMApp app = rm.submitApp(1 * GB);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    //request for containers
    int request = 4;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());

    //kick the scheduler
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    while (contReceived < 4) { //only 4 containers can be allocated on node1
      nm1.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(4, conts.size());

    Thread.sleep(5000);

    //request for containers
    request = 10;
    am.allocate("h1", 1 * GB, request, new ArrayList<ContainerId>());
    //send node2 heartbeat
    conts = am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
    contReceived = conts.size();
    while (contReceived < 10) { //Now rest of the (request-4=10) containers can be allocated on node1
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
      Thread.sleep(WAIT_SLEEP_MS);
    }
    Assert.assertEquals(10, conts.size());
    Thread.sleep(1000);
  }

  @Test
//(timeout=60000)
  public void TestStreamingService() throws Exception {
//public void TestStreamingService(MockRM rmRT) throws Exception {
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.
            setBoolean(
                    YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED,
                    true);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    conf.
            setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
                    1);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(YarnConfiguration.QUOTAS_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);

    RMContextImpl rmContext = new RMContextImpl();
    rmContext.setHAEnabled(true);
    rmContext.setDistributedEnabled(true);
    GroupMembershipService gps = new GroupMembershipService(null, rmContext);
    rmContext.setRMGroupMembershipService(gps);
    gps.init(conf);
    gps.start();
    Thread.sleep(1000);

    org.junit.Assert.
            assertTrue(rmContext.getGroupMembershipService().isLeader());

    MockRM rmRT = new MockRM(conf);
    rmRT.start();

    org.junit.Assert.assertFalse(
            rmRT.getRMContext().getGroupMembershipService().isLeader());
    NdbRtStreamingProcessor rtStreamingProcessor = new NdbRtStreamingProcessor(
            rmRT.getRMContext());
    RMStorageFactory.kickTheNdbEventStreamingAPI(false, conf);
    new Thread(rtStreamingProcessor).start();
    Thread.sleep(1000);
    int t = 0;
    org.junit.Assert.assertNotNull(rmRT.getRMContext().
            getContainersLogsService());
    if (rmRT.getRMContext().getContainersLogsService() == null) {
      LOG.info("TEST :: Container log service not started");
    } else {
      // Change the global running price
      //** rmRT.getRMContext().getContainersLogsService().insertPriceEvent(100f, 12345678); **

      LightWeightRequestHandler bomb;
      bomb = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();

          YarnRunningPriceDataAccess<YarnRunningPrice> runningPriceDA
                  = (YarnRunningPriceDataAccess) RMStorageFactory.getDataAccess(
                          YarnRunningPriceDataAccess.class);

          if (runningPriceDA != null) {
            runningPriceDA.add(new YarnRunningPrice(
                    YarnRunningPrice.PriceType.VARIABLE, 12345678, 100f));
          } else {
            LOG.info("DataAccess failed!");
          }

          connector.commit();
          return null;
        }
      };
      bomb.handle();

      while (t < 10) {
        List<RMNode> rmNodes = generateRMNodesToAdd(1);
        List<RMContainer> rmContainers = new ArrayList<RMContainer>();
        List<ContainerStatus> containerStatuses
                = new ArrayList<ContainerStatus>();
        generateRMContainersToAdd(1, t, rmNodes, rmContainers, containerStatuses);
        populateDB(rmNodes, rmContainers, containerStatuses);
        Thread.sleep(monitorInterval);
        t++;
      }
    }

    // Insert first batch of 10 dummy containers
    List<RMNode> rmNodes1 = generateRMNodesToAdd(10);
    List<RMContainer> rmContainers1 = new ArrayList<RMContainer>();
    List<ContainerStatus> containerStatuses1 = new ArrayList<ContainerStatus>();
    generateRMContainersToAdd(10, t, rmNodes1, rmContainers1, containerStatuses1);
    populateDB(rmNodes1, rmContainers1, containerStatuses1);
    t = t + 10;

    Thread.sleep(monitorInterval * 2);
    //simulating heartbeats forcing the streaming api to deliver events

    Thread.sleep(monitorInterval + (monitorInterval / 2));
    int u = 0;
    while (u < 10) {
      List<RMNode> rmNodes = generateRMNodesToAdd(1);
      List<RMContainer> rmContainers = new ArrayList<RMContainer>();
      List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
      generateRMContainersToAdd(1, u + t, rmNodes, rmContainers,
              containerStatuses);
      populateDB(rmNodes, rmContainers, containerStatuses);
      Thread.sleep(monitorInterval);
      u++;
    }

    // Check if container logs have correct values
    Map<String, ContainersLogs> cl = getContainersLogs();
    for (int i = 0; i < 10; i++) {
      ContainersLogs entry = cl.get(containerStatuses1.get(i).getContainerid());
      LOG.info(entry.toString());
      LOG.info(entry.getPrice());
      org.junit.Assert.assertNotNull(entry);
      float _price = entry.getPrice();
      org.junit.Assert.assertEquals((Float) 100f, (Float) _price);
    }
    rmRT.stop();

  }

  /**
   * Read all containers logs table entries
   *
   * @return
   */
  private Map<String, ContainersLogs> getContainersLogs() {
    Map<String, ContainersLogs> containersLogs
            = new HashMap<String, ContainersLogs>();

    try {
      LightWeightRequestHandler allContainersHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          ContainersLogsDataAccess containersLogsDA
                  = (ContainersLogsDataAccess) RMStorageFactory.getDataAccess(
                          ContainersLogsDataAccess.class);
          connector.beginTransaction();
          connector.readCommitted();

          Map<String, ContainersLogs> allContainersLogs = containersLogsDA.
                  getAll();

          connector.commit();

          return allContainersLogs;
        }
      };
      containersLogs = (Map<String, ContainersLogs>) allContainersHandler.
              handle();
    } catch (IOException ex) {
      LOG.warn("Unable to retrieve containers logs", ex);
    }

    return containersLogs;
  }

//Populates DB with fake RM, Containers and status entries
  private void populateDB(
          final List<RMNode> rmNodesToAdd,
          final List<RMContainer> rmContainersToAdd,
          final List<ContainerStatus> containerStatusToAdd
  ) {
    try {
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          connector.beginTransaction();
          connector.writeLock();
          //Insert Pending Event
          List<PendingEvent> pendingEventsToAdd = new ArrayList<PendingEvent>();
          for (RMNode node : rmNodesToAdd) {
            pendingEventsToAdd.add(new PendingEvent(node.getNodeId(), 0, 0,
                    node.getPendingEventId()));
          }
          PendingEventDataAccess pendingEventDA
                  = (PendingEventDataAccess) RMStorageFactory.getDataAccess(
                          PendingEventDataAccess.class);
          pendingEventDA.addAll(pendingEventsToAdd);

          // Insert RM nodes
          RMNodeDataAccess rmNodesDA = (RMNodeDataAccess) RMStorageFactory.
                  getDataAccess(RMNodeDataAccess.class);
          rmNodesDA.addAll(rmNodesToAdd);

          // Insert RM Containers
          RMContainerDataAccess rmcontainerDA
                  = (RMContainerDataAccess) RMStorageFactory.getDataAccess(
                          RMContainerDataAccess.class);
          rmcontainerDA.addAll(rmContainersToAdd);

          // Insert container statuses
          ContainerStatusDataAccess containerStatusDA
                  = (ContainerStatusDataAccess) RMStorageFactory.getDataAccess(
                          ContainerStatusDataAccess.class);
          containerStatusDA.addAll(containerStatusToAdd);

          connector.commit();
          return null;
        }
      };
      LOG.info("populating DB");
      bomb.handle();
      LOG.info("populated DB");
    } catch (IOException ex) {
      LOG.warn("Unable to populate DB", ex);
    }
  }

  Random random = new Random();

  private void generateRMContainersToAdd(int nbContainers, int startNo,
          List<RMNode> rmNodesList, List<RMContainer> rmContainers,
          List<ContainerStatus> containersStatus) {
    for (int i = startNo; i < (startNo + nbContainers); i++) {
      int randRMNode = random.nextInt(rmNodesList.size());
      RMNode randomRMNode = rmNodesList.get(randRMNode);
      RMContainer container = new RMContainer(
              "container_1450009406746_0001_01_00000" + i, "appAttemptId",
              randomRMNode.getNodeId(), "user", "reservedNodeId", i, i, i, i, i,
              "state", "finishedStatusState", i);
      rmContainers.add(container);

      ContainerStatus status = new ContainerStatus(container.getContainerId(),
              ContainerState.RUNNING.toString(), null,
              ContainerExitStatus.SUCCESS, randomRMNode.getNodeId(),
              randomRMNode.getPendingEventId(),
              ContainerStatus.Type.JUST_LAUNCHED);
      containersStatus.add(status);
    }
  }

  int pendingId = 0;

  private List<RMNode> generateRMNodesToAdd(int nbNodes) {
    List<RMNode> toAdd = new ArrayList<RMNode>();
    for (int i = 0; i < nbNodes; i++) {
      RMNode rmNode = new RMNode("nodeid_" + i + ":" + 9999, "hostName", 1, 1,
              "nodeAddress", "httpAddress", "", 1, "RUNNING", "version", 1, 1,
              pendingId++);
      toAdd.add(rmNode);
    }
    return toAdd;
  }

}
