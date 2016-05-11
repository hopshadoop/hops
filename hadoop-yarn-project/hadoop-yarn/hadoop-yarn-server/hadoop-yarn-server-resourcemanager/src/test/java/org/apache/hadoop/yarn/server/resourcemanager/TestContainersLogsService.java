/*
 * Copyright 2015 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.YarnVariables;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;

public class TestContainersLogsService {

  private static final Log LOG = LogFactory.getLog(ContainersLogsService.class);

  private Configuration conf;
  Random random = new Random();

  @Before
  public void setup() throws IOException {
    try {
      conf = new YarnConfiguration();
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
              "target/test-classes/RT_EventAPIConfig.ini");
      conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
              "target/test-classes/RM_EventAPIConfig.ini");
      LOG.info("initialize db");
      RMUtilities.InitializeDB();
    } catch (StorageInitializtionException ex) {
      LOG.error(null, ex);
    } catch (IOException ex) {
      LOG.error(null, ex);
    }
  }

  /**
   * Basic tick counter test, check if initialized in variables table
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testTickCounterInitialization() throws Exception {
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, 1000);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, false);

    YarnVariables tc = getTickCounter();

    // Check if tick counter is initialized with YARN variables
    Assert.assertNotNull(tc);
  }

  /**
   * Test if checkpoint is created with correct container status values
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testCheckpoints() throws Exception {
    int checkpointTicks = 10;
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, true);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, checkpointTicks);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            1);
    RMContext rmContext = new RMContextImpl();
    ContainersLogsService logService = new ContainersLogsService(rmContext);
    logService.init(conf);

    try {
      // Insert dummy data into necessary tables
      List<RMNode> rmNodes = generateRMNodesToAdd(10);
      List<RMContainer> rmContainers = new ArrayList<RMContainer>();
      List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
      generateRMContainersToAdd(10, 0, rmNodes,rmContainers,containerStatuses);
      populateDB(rmNodes, rmContainers, containerStatuses);

      logService.start();

      int sleepTillCheckpoint = monitorInterval * (checkpointTicks + 1);
      Thread.sleep(sleepTillCheckpoint);

      // Check if checkpoint has containers and correct values
      Map<String, ContainersLogs> cl = getContainersLogs();
      for (ContainerStatus cs : containerStatuses) {
        ContainersLogs entry = cl.get(cs.getContainerid());
        Assert.assertNotNull(entry);
        Assert.assertEquals(checkpointTicks, entry.getStop());
        Assert.assertEquals(ContainersLogs.CONTAINER_RUNNING_STATE, entry.
                getExitstatus());
      }

    } finally {
      logService.stop();
    }
  }

  /**
   * Test if container statuses that were not captures by ContainersLogs
   * service are correctly recorded into DB. This is an unlikely edge-case.
   *
   * @throws Exception
   */
  //TODO do we need it with the streaming library? (For ex when the node faile?)
  //@Test(timeout=60000)
  public void testUnknownExitstatusUseCase() throws Exception {
    int monitorInterval = 1000;
    int timeout = 2;
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, false);

    MockRM rm = new MockRM(conf);

    try {
      // Insert dummy data into necessary tables
      List<RMNode> rmNodes = generateRMNodesToAdd(1);
      List<RMContainer> rmContainers = new ArrayList<RMContainer>() ;
      List<ContainerStatus> containerStatuses= new ArrayList<ContainerStatus>();
      generateRMContainersToAdd(10, 0,rmNodes,rmContainers,containerStatuses);
      populateDB(rmNodes, rmContainers, containerStatuses);

      rm.start();
      
      Thread.sleep(monitorInterval * timeout);

      // Delete RM node, which should delete container statuses
      removeRMNodes(rmNodes);

      Thread.sleep(monitorInterval * timeout);

      // Check if all containers are marked with exitstatus UNKNOWN
      Map<String, ContainersLogs> cl = getContainersLogs();
      for (ContainerStatus cs : containerStatuses) {
        ContainersLogs entry = cl.get(cs.getContainerid());
        Assert.assertNotNull(entry);
        Assert.assertEquals(0, entry.getStart());
        Assert.assertTrue("entry stop should be " + timeout + " or +1 but it is "
                + entry.getStop(), (entry.getStop() == timeout) || (entry.
                getStop() == timeout + 1));
        Assert.assertEquals(ContainersLogs.UNKNOWN_CONTAINER_EXIT, entry.
                getExitstatus());
      }
    } finally {
      rm.stop();
    }
  }

  /**
   * Tests if tick counter and container statuses are recorded correctly after
   * RM is stopped and new RM takes over.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testFailover() throws Exception {
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, false);

    // Insert dummy data into necessary tables
    List<RMNode> rmNodes = generateRMNodesToAdd(10);
    List<RMContainer> rmContainers = new ArrayList<RMContainer>();
    List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
    generateRMContainersToAdd(10, 0,rmNodes,rmContainers,containerStatuses);
    populateDB(rmNodes, rmContainers, containerStatuses);

    // Start RM1
    RMContext rmContext = new RMContextImpl();
    ContainersLogsService logService1 = new ContainersLogsService(rmContext);
    logService1.init(conf);
    logService1.start();
    Thread.sleep(monitorInterval * 5);
    logService1.stop();

    // Change Container Statuses to COMPLETED
    List<ContainerStatus> updatedStatuses = changeContainerStatuses(
            containerStatuses,
            ContainerState.COMPLETE.toString(),
            ContainerExitStatus.SUCCESS
    );
    updateContainerStatuses(updatedStatuses);

    // Start RM2
    ContainersLogsService logService2 = new ContainersLogsService(rmContext);
    logService2.init(conf);
    logService2.start();
    Thread.sleep(monitorInterval * 2);
    logService2.stop();

    // Check if tick counter is correct
    YarnVariables tc = getTickCounter();
    Assert.assertTrue(tc.getValue()>=5);

    // Check if container logs have correct values
    Map<String, ContainersLogs> cl = getContainersLogs();
    for (ContainerStatus cs : containerStatuses) {
      ContainersLogs entry = cl.get(cs.getContainerid());
      Assert.assertNotNull(entry);
      Assert.assertEquals(0, entry.getStart());
      Assert.assertTrue(entry.getStop() == 4 || entry.getStop() == 5);
      Assert.assertEquals(ContainerExitStatus.SUCCESS, entry.getExitstatus());
    }
  }

  /**
   * Test general use case where Container Statuses are added and changes over
   * time.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testFullUseCase() throws Exception {
    int monitorInterval = 2000;
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, true);
    conf.setInt(YarnConfiguration.QUOTAS_MIN_TICKS_CHARGE, 10);
    conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            1);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(
            YarnConfiguration.QUOTAS_ENABLED, true);
    MockRM rm = new MockRM(conf);

    // Insert first batch of dummy containers
    List<RMNode> rmNodes1 = generateRMNodesToAdd(10);
    List<RMContainer> rmContainers1 = new ArrayList<RMContainer>();
    List<ContainerStatus> containerStatuses1 = new ArrayList<ContainerStatus>();
    generateRMContainersToAdd(10, 0, rmNodes1, rmContainers1, containerStatuses1);
    
    populateDB(rmNodes1, rmContainers1, containerStatuses1);

    rm.start();
    
    Thread.sleep(monitorInterval*5);

    // Insert second batch of dummy containers
    List<RMNode> rmNodes2 = new ArrayList<RMNode>();
    for (RMNode node : rmNodes1){
      rmNodes2.add(new RMNode(node.getNodeId(), node.getHostName(), node.
              getCommandPort(), node.getHttpPort(), node.getNodeAddress(), node.
              getHttpAddress(), node.getHealthReport(),
              node.getLastHealthReportTime(), node.getCurrentState(), node.
              getNodemanagerVersion(), -1, //overcomitTimeOut is never set and getting it return an error
              pendingId++));
    }
    List<RMContainer> rmContainers2 = new ArrayList<RMContainer>();
    List<ContainerStatus> containerStatuses2 = new ArrayList<ContainerStatus>();
    generateRMContainersToAdd(10, 10, rmNodes2,rmContainers2,containerStatuses2);
    populateDB(rmNodes2, rmContainers2, containerStatuses2);
    
    //simulating heartbeats forcing the streaming api to deliver events
    Thread.sleep(monitorInterval+monitorInterval/2);
    int tick1 = rm.getRMContext().getContainersLogsService().getCurrentTick();
    int t = 0;
    while (t < 5) {
      commitDummyPendingEvent();
      Thread.sleep(monitorInterval);
      t++;
    }
    
    // Complete half of first and second batch of dummy containers
    List<ContainerStatus> csUpdate1 = new ArrayList<ContainerStatus>();
    for (int i = 0; i < 5; i++) {
      ContainerStatus cs = containerStatuses1.get(i);
      ContainerStatus csNewStatus = new ContainerStatus(
              cs.getContainerid(),
              ContainerState.COMPLETE.toString(),
              null,
              ContainerExitStatus.SUCCESS,
              cs.getRMNodeId(),
              pendingId++, ContainerStatus.Type.UCI);
      csUpdate1.add(csNewStatus);

      ContainerStatus cs2 = containerStatuses2.get(i);
      ContainerStatus cs2NewStatus = new ContainerStatus(
              cs2.getContainerid(),
              ContainerState.COMPLETE.toString(),
              null,
              ContainerExitStatus.ABORTED,
              cs2.getRMNodeId(),
              pendingId, ContainerStatus.Type.UCI);
      csUpdate1.add(cs2NewStatus);
    }
    updateContainerStatuses(csUpdate1);

    //simulating heartbeats forcing the streaming api to deliver events
    Thread.sleep(monitorInterval+monitorInterval/2);
    int tick2 = rm.getRMContext().getContainersLogsService().getCurrentTick();
    t = 0;
    while (t < 5) {
      commitDummyPendingEvent();
      Thread.sleep(monitorInterval);
      t++;
    }

    // Complete second half of second batch of dummy containers
    List<ContainerStatus> csUpdate2 = new ArrayList<ContainerStatus>();
    for (int i = 5; i < 10; i++) {
      ContainerStatus cs = containerStatuses2.get(i);
      ContainerStatus csNewStatus = new ContainerStatus(
              cs.getContainerid(),
              ContainerState.COMPLETE.toString(),
              null,
              ContainerExitStatus.SUCCESS,
              cs.getRMNodeId(),
              pendingId++, ContainerStatus.Type.UCI);
      csUpdate2.add(csNewStatus);
    }
    updateContainerStatuses(csUpdate2);

    //simulating heartbeats forcing the streaming api to deliver events
    Thread.sleep(monitorInterval+monitorInterval/2);
    int tick3 = rm.getRMContext().getContainersLogsService().getCurrentTick();
    commitDummyPendingEvent();
    t = 0;
    while (t < 5) {
      commitDummyPendingEvent();
      Thread.sleep(monitorInterval);
      t++;
    }

    rm.stop();

 
    // Check if container logs have correct values
    Map<String, ContainersLogs> cl = getContainersLogs();
    for (int i = 0; i < 5; i++) {
      ContainersLogs entry = cl.get(containerStatuses1.get(i).getContainerid());
      Assert.assertNotNull(entry);
      Assert.assertEquals( 0, entry.getStart());
      Assert.assertEquals( tick2, entry.getStop());
      Assert.assertEquals(ContainerExitStatus.SUCCESS, entry.getExitstatus());

      ContainersLogs entry2 = cl.get(containerStatuses2.get(i).getContainerid());
      Assert.assertNotNull(entry2);
      Assert.assertEquals(tick1, entry2.getStart());
      Assert.assertEquals(tick2, entry2.getStop());
      Assert.assertEquals(ContainerExitStatus.ABORTED, entry2.getExitstatus());

      ContainersLogs entry3 = cl.get(containerStatuses2.get(5 + i).
              getContainerid());
      Assert.assertNotNull(entry3);
      Assert.assertEquals(tick1, entry3.getStart());
      Assert.assertEquals(tick3, entry3.getStop());
      Assert.assertEquals(ContainerExitStatus.SUCCESS, entry.getExitstatus());

      ContainersLogs entry4 = cl.get(containerStatuses1.get(5 + i).
              getContainerid());
      Assert.assertNotNull(entry4);
      Assert.assertEquals(0, entry4.getStart());
      Assert.assertEquals(20, entry4.getStop());
      Assert.assertEquals(ContainersLogs.CONTAINER_RUNNING_STATE, entry4.
              getExitstatus());
    }
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
                  List<PendingEvent> pendingEventsToAdd = 
                          new ArrayList<PendingEvent>();
                  for(RMNode node: rmNodesToAdd){
                    pendingEventsToAdd.add(new PendingEvent(node.getNodeId(), 0,
                            0, node.getPendingEventId()));
                  }
                  PendingEventDataAccess pendingEventDA =
                          (PendingEventDataAccess) RMStorageFactory.
                          getDataAccess(PendingEventDataAccess.class);
                  pendingEventDA.addAll(pendingEventsToAdd);
                  
                  // Insert RM nodes
                  RMNodeDataAccess rmNodesDA
                  = (RMNodeDataAccess) RMStorageFactory
                  .getDataAccess(RMNodeDataAccess.class);
                  rmNodesDA.addAll(rmNodesToAdd);

                  // Insert RM Containers
                  RMContainerDataAccess rmcontainerDA
                  = (RMContainerDataAccess) RMStorageFactory
                  .getDataAccess(RMContainerDataAccess.class);
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

  private void updateContainerStatuses(
          final List<ContainerStatus> containerStatusToAdd) {
    try {
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();

                  //Insert Pending Event
                  List<PendingEvent> pendingEventsToAdd = 
                          new ArrayList<PendingEvent>();
                  for(ContainerStatus containerStatus: containerStatusToAdd){
                    pendingEventsToAdd.add(
                            new PendingEvent(containerStatus.getRMNodeId(), -1,
                            0, containerStatus.getPendingEventId()));
                  }
                  PendingEventDataAccess pendingEventDA =
                          (PendingEventDataAccess) RMStorageFactory.
                          getDataAccess(PendingEventDataAccess.class);
                  pendingEventDA.addAll(pendingEventsToAdd);
                  
                  // Update container statuses
                  ContainerStatusDataAccess containerStatusDA
                  = (ContainerStatusDataAccess) RMStorageFactory.getDataAccess(
                          ContainerStatusDataAccess.class);
                  containerStatusDA.addAll(containerStatusToAdd);

                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
    } catch (IOException ex) {
      LOG.warn("Unable to update container statuses table", ex);
    }
  }

  private void commitDummyPendingEvent() {
    try {
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();

                  //Insert Pending Event
                  List<PendingEvent> pendingEventsToAdd = 
                          new ArrayList<PendingEvent>();
                  
                    pendingEventsToAdd.add(
                            new PendingEvent("nodeid", -1,
                            0, pendingId++));
                  
                  PendingEventDataAccess pendingEventDA =
                          (PendingEventDataAccess) RMStorageFactory.
                          getDataAccess(PendingEventDataAccess.class);
                  pendingEventDA.addAll(pendingEventsToAdd);
                  
                 

                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
    } catch (IOException ex) {
      LOG.warn("Unable to update container statuses table", ex);
    }
  }
  /**
   * Read tick counter from YARN variables table
   *
   * @return
   */
  private YarnVariables getTickCounter() {
    YarnVariables tickCounter = null;

    try {
      LightWeightRequestHandler tickHandler = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  YarnVariablesDataAccess yarnVariablesDA
                  = (YarnVariablesDataAccess) RMStorageFactory
                  .getDataAccess(YarnVariablesDataAccess.class);

                  connector.beginTransaction();
                  connector.readCommitted();

                  YarnVariables tc
                  = (YarnVariables) yarnVariablesDA
                  .findById(HopYarnAPIUtilities.CONTAINERSTICKCOUNTER);

                  connector.commit();
                  return tc;
                }
              };
      tickCounter = (YarnVariables) tickHandler.handle();
    } catch (IOException ex) {
      LOG.warn("Unable to retrieve tick counter from YARN variables", ex);
    }

    return tickCounter;
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
                  = (ContainersLogsDataAccess) RMStorageFactory
                  .getDataAccess(ContainersLogsDataAccess.class);
                  connector.beginTransaction();
                  connector.readCommitted();

                  Map<String, ContainersLogs> allContainersLogs
                  = containersLogsDA.getAll();

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

  private void removeRMNodes(final Collection<RMNode> RMNodes) {
    try {
      LightWeightRequestHandler RMNodesHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws StorageException {
                  RMNodeDataAccess rmNodesDA
                  = (RMNodeDataAccess) RMStorageFactory
                  .getDataAccess(RMNodeDataAccess.class);

                  connector.beginTransaction();
                  connector.writeLock();

                  rmNodesDA.removeAll(RMNodes);

                  connector.commit();

                  return null;
                }
              };
      RMNodesHandler.handle();
    } catch (IOException ex) {
      LOG.warn("Unable to remove RM nodes from table", ex);
    }
  }

  private void generateRMContainersToAdd(int nbContainers,
          int startNo, List<RMNode> rmNodesList, List<RMContainer> rmContainers,
          List<ContainerStatus> containersStatus) {
    for (int i = startNo; i < (startNo + nbContainers); i++) {
      int randRMNode = random.nextInt(rmNodesList.size());
      RMNode randomRMNode = rmNodesList.get(randRMNode);
      RMContainer container = new RMContainer(
              "container_1450009406746_0001_01_00000" + i, "appAttemptId",
              randomRMNode.getNodeId(), "user", "reservedNodeId", i, i, i, i, i,
              "state", "finishedStatusState", i);
      rmContainers.add(container);
      
      ContainerStatus status = new ContainerStatus(
              container.getContainerId(),
              ContainerState.RUNNING.toString(),
              null,
              ContainerExitStatus.SUCCESS,
              randomRMNode.getNodeId(),
              randomRMNode.getPendingEventId(),ContainerStatus.Type.UCI);
      containersStatus.add(status);
    }
  }

  int pendingId=0;
  private List<RMNode> generateRMNodesToAdd(int nbNodes) {
    List<RMNode> toAdd = new ArrayList<RMNode>();
    for (int i = 0; i < nbNodes; i++) {
      RMNode rmNode = new RMNode("nodeid_" + i + ":" + 9999, "hostName", 1,
              1, "nodeAddress", "httpAddress", "", 1, "RUNNING",
              "version", 1, pendingId++);
      toAdd.add(rmNode);
    }
    return toAdd;
  }



  private List<ContainerStatus> changeContainerStatuses(
          List<ContainerStatus> containerStatuses,
          String newStatus,
          int exitStatus) {
    List<ContainerStatus> toAdd = new ArrayList<ContainerStatus>();

    for (ContainerStatus entry : containerStatuses) {
      ContainerStatus status = new ContainerStatus(
              entry.getContainerid(),
              newStatus,
              null,
              exitStatus,
              entry.getRMNodeId(),
              0, ContainerStatus.Type.UCI);
      toAdd.add(status);
    }
    return toAdd;
  }
}
