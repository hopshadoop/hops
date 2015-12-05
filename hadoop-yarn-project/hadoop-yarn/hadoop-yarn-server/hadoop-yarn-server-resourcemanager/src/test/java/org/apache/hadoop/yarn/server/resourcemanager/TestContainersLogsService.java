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
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.ContainersLogs;
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
            RMUtilities.InitializeDB();
            RMStorageFactory.getConnector().formatStorage();
        } catch (StorageInitializtionException ex) {
            LOG.error(null, ex);
        } catch (IOException ex) {
            LOG.error(null, ex);
        }
    }

//    @Test
    public void testStreaming() throws Exception {
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, 1000);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, false);

        MockRM rm;
        
        List<RMNode> rmNodes = generateRMNodesToAdd(10);
        List<RMContainer> rmContainers = generateRMContainersToAdd(10, 0);
        List<ContainerStatus> containerStatuses
                = generateContainersStatusToAdd(rmNodes, rmContainers);
        populateDB(rmNodes, rmContainers, containerStatuses);

        rm = new MockRM(conf);
        
//        List<RMNode> rmNodes = generateRMNodesToAdd(10);
//        List<RMContainer> rmContainers = generateRMContainersToAdd(10, 0);
//        List<ContainerStatus> containerStatuses
//                = generateContainersStatusToAdd(rmNodes, rmContainers);
//        populateDB(rmNodes, rmContainers, containerStatuses);
        
        
        rm.start();

        Thread.sleep(50000000);

        rm.stop();
    }

    
    

    /**
     * Basic tick counter test, check if initialized in variables table
     *
     * @throws Exception
     */
//    @Test
    public void testTickCounterInitialization() throws Exception {
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, 1000);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, false);

        YarnVariables tc = getTickCounter();

        // Check if tick counter is initialized with YARN variables
        Assert.assertNotNull(tc);
    }

    /**
     * Test if checkpoint is created with correct container status values
     *
     * @throws Exception
     */
//    @Test
    public void testCheckpoints() throws Exception {
        int checkpointTicks = 500;
        int monitorInterval = 1000;
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, monitorInterval);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, true);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_TICKS, checkpointTicks);

        MockRM rm = new MockRM(conf);

        try {
            // Insert dummy data into necessary tables
            List<RMNode> rmNodes = generateRMNodesToAdd(10);
            List<RMContainer> rmContainers = generateRMContainersToAdd(10, 0);
            List<ContainerStatus> containerStatuses
                    = generateContainersStatusToAdd(rmNodes, rmContainers);
            populateDB(rmNodes, rmContainers, containerStatuses);

            rm.start();

            RMStorageFactory.kickTheNdbEventStreamingAPI(false);

            int sleepTillCheckpoint = monitorInterval * (checkpointTicks + 1);
            Thread.sleep(sleepTillCheckpoint);

            // Check if checkpoint has containers and correct values
            Map<String, ContainersLogs> cl = getContainersLogs();
            for (ContainerStatus cs : containerStatuses) {
                ContainersLogs entry = cl.get(cs.getContainerid());
                Assert.assertNotNull(entry);
                Assert.assertEquals(0, entry.getStart());
                Assert.assertEquals(checkpointTicks, entry.getStop());
                Assert.assertEquals(ContainersLogs.CONTAINER_RUNNING_STATE, entry.getExitstatus());
            }

        } finally {
            rm.stop();
        }
    }

    /**
     * Test if container statuses that were not captures by ContainersLogs
     * service are correctly recorded into DB. This is an unlikely edge-case.
     *
     * @throws Exception
     */
//    @Test
    public void testUnknownExitstatusUseCase() throws Exception {
        int monitorInterval = 1000;
        int timeout = 2;
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, monitorInterval);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, false);

        MockRM rm = new MockRM(conf);

        try {
            // Insert dummy data into necessary tables
            List<RMNode> rmNodes = generateRMNodesToAdd(1);
            List<RMContainer> rmContainers = generateRMContainersToAdd(10, 0);
            List<ContainerStatus> containerStatuses
                    = generateContainersStatusToAdd(rmNodes, rmContainers);
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
                Assert.assertTrue((entry.getStop() == timeout) || (entry.getStop() == timeout + 1));
                Assert.assertEquals(ContainersLogs.UNKNOWN_CONTAINER_EXIT, entry.getExitstatus());
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
//    @Test
    public void testFailover() throws Exception {
        int monitorInterval = 1000;
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, monitorInterval);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, false);

        // Insert dummy data into necessary tables
        List<RMNode> rmNodes = generateRMNodesToAdd(10);
        List<RMContainer> rmContainers = generateRMContainersToAdd(10, 0);
        List<ContainerStatus> containerStatuses
                = generateContainersStatusToAdd(rmNodes, rmContainers);
        populateDB(rmNodes, rmContainers, containerStatuses);

        // Start RM1
        MockRM rm1 = new MockRM(conf);
        rm1.start();
        Thread.sleep(monitorInterval * 5);
        rm1.stop();

        // Change Container Statuses to COMPLETED
        List<ContainerStatus> updatedStatuses = changeContainerStatuses(
                containerStatuses,
                ContainerState.COMPLETE.toString(),
                ContainerExitStatus.SUCCESS
        );
        updateContainerStatuses(updatedStatuses);

        // Start RM2
        MockRM rm2 = new MockRM(conf);
        rm2.start();
        Thread.sleep(monitorInterval * 2);
        rm2.stop();

        // Check if tick counter is correct
        YarnVariables tc = getTickCounter();
        Assert.assertEquals(7, tc.getValue());

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
//    @Test
    public void testFullUseCase() throws Exception {
        int monitorInterval = 1000;
        int checkpointTicks = 10;
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_MONITOR_INTERVAL, monitorInterval);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_TICK_INCREMENT, 1);
        conf.setBoolean(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS, true);
        conf.setInt(YarnConfiguration.QUOTAS_CONTAINERS_LOGS_CHECKPOINTS_TICKS, checkpointTicks);

        MockRM rm = new MockRM(conf);

        // Insert first batch of dummy containers
        List<RMNode> rmNodes1 = generateRMNodesToAdd(10);
        List<RMContainer> rmContainers1 = generateRMContainersToAdd(10, 0);
        List<ContainerStatus> containerStatuses1
                = generateContainersStatusToAdd(rmNodes1, rmContainers1);
        populateDB(rmNodes1, rmContainers1, containerStatuses1);

        rm.start();

        Thread.sleep(monitorInterval * 5);

        // Insert second batch of dummy containers
        List<RMNode> rmNodes2 = new ArrayList<RMNode>();
        List<RMContainer> rmContainers2 = generateRMContainersToAdd(10, 10);
        List<ContainerStatus> containerStatuses2
                = generateContainersStatusToAdd(rmNodes1, rmContainers2);
        populateDB(rmNodes2, rmContainers2, containerStatuses2);

        Thread.sleep(monitorInterval * 5);

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
                    0);
            csUpdate1.add(csNewStatus);

            ContainerStatus cs2 = containerStatuses2.get(i);
            ContainerStatus cs2NewStatus = new ContainerStatus(
                    cs2.getContainerid(),
                    ContainerState.COMPLETE.toString(),
                    null,
                    ContainerExitStatus.ABORTED,
                    cs2.getRMNodeId(),
                    0);
            csUpdate1.add(cs2NewStatus);
        }
        updateContainerStatuses(csUpdate1);

        Thread.sleep(monitorInterval * 5);

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
                    0);
            csUpdate2.add(csNewStatus);
        }
        updateContainerStatuses(csUpdate2);

        Thread.sleep(monitorInterval * 5);

        rm.stop();

        // Check if tick counter is correct
        YarnVariables tc = getTickCounter();
        Assert.assertEquals(20, tc.getValue());

        // Check if container logs have correct values
        Map<String, ContainersLogs> cl = getContainersLogs();
        for (int i = 0; i < 5; i++) {
            ContainersLogs entry = cl.get(containerStatuses1.get(i).getContainerid());
            Assert.assertNotNull(entry);
            Assert.assertEquals(0, entry.getStart());
            Assert.assertEquals(11, entry.getStop());
            Assert.assertEquals(ContainerExitStatus.SUCCESS, entry.getExitstatus());

            ContainersLogs entry2 = cl.get(containerStatuses2.get(i).getContainerid());
            Assert.assertNotNull(entry2);
            Assert.assertEquals(6, entry2.getStart());
            Assert.assertEquals(11, entry2.getStop());
            Assert.assertEquals(ContainerExitStatus.ABORTED, entry2.getExitstatus());

            ContainersLogs entry3 = cl.get(containerStatuses2.get(5 + i).getContainerid());
            Assert.assertNotNull(entry3);
            Assert.assertEquals(6, entry3.getStart());
            Assert.assertEquals(16, entry3.getStop());
            Assert.assertEquals(ContainerExitStatus.SUCCESS, entry.getExitstatus());

            ContainersLogs entry4 = cl.get(containerStatuses1.get(5 + i).getContainerid());
            Assert.assertNotNull(entry4);
            Assert.assertEquals(0, entry4.getStart());
            Assert.assertEquals(20, entry4.getStop());
            Assert.assertEquals(ContainersLogs.CONTAINER_RUNNING_STATE, entry4.getExitstatus());
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

                    // Insert RM nodes
                    RMNodeDataAccess rmNodesDA = (RMNodeDataAccess) RMStorageFactory
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
            bomb.handle();
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
        Map<String, ContainersLogs> containersLogs = new HashMap<String, ContainersLogs>();

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
            containersLogs = (Map<String, ContainersLogs>) allContainersHandler.handle();
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

    private List<RMContainer> generateRMContainersToAdd(int nbContainers, int startNo) {
        List<RMContainer> toAdd = new ArrayList<RMContainer>();
        for (int i = startNo; i < (startNo + nbContainers); i++) {
            RMContainer container = new RMContainer("containerid" + i + "_" + random.
                    nextInt(10), "appAttemptId",
                    "nodeId", "user", "reservedNodeId", i, i, i, i, i,
                    "state", "finishedStatusState", i);
            toAdd.add(container);
        }
        return toAdd;
    }

    private List<RMNode> generateRMNodesToAdd(int nbNodes) {
        List<RMNode> toAdd = new ArrayList<RMNode>();
        for (int i = 0; i < nbNodes; i++) {
            RMNode rmNode = new RMNode("nodeid_" + i, "hostName", 1,
                    1, "nodeAddress", "httpAddress", "", 1, "currentState",
                    "version", 1, 1, 0);
            toAdd.add(rmNode);
        }
        return toAdd;
    }

    private List<ContainerStatus> generateContainersStatusToAdd(
            List<RMNode> rmNodesList,
            List<RMContainer> rmContainersList) {
        List<ContainerStatus> toAdd = new ArrayList<ContainerStatus>();
        for (RMContainer rmContainer : rmContainersList) {
            // Get random RM nodes for FK
            int randRMNode = random.nextInt(rmNodesList.size());
            RMNode randomRMNode = rmNodesList.get(randRMNode);

            ContainerStatus status = new ContainerStatus(
                    rmContainer.getContainerIdID(),
                    ContainerState.RUNNING.toString(),
                    null,
                    ContainerExitStatus.SUCCESS,
                    randomRMNode.getNodeId(),
                    0);
            toAdd.add(status);
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
                    0);
            toAdd.add(status);
        }
        return toAdd;
    }
}
