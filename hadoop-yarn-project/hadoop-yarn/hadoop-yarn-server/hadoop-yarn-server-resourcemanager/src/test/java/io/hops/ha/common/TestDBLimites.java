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
package io.hops.ha.common;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;

public class TestDBLimites {

  private static final Log LOG = LogFactory.getLog(TestDBLimites.class);
  Random random = new Random();

  @Before
  public void setup() throws IOException {
    try {
      LOG.info("Setting up Factories");
      Configuration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMStorageFactory.getConnector().formatStorage();
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (StorageException ex) {
      LOG.error(ex);
    }
  }

  private List<RMContainer> generateRMContainersToAdd(int nbContainers) {
    List<RMContainer> toAdd = new ArrayList<RMContainer>();
    for (int i = 0; i < nbContainers; i++) {
      RMContainer container = new RMContainer("containerid" + i + "_" + random.
              nextInt(10), "appAttemptId",
              "nodeId", "user", "reservedNodeId", i, i, i, i, i,
              "state", "finishedStatusState", i);
      toAdd.add(container);
    }
    return toAdd;
  }

//  @Test
  public void rmContainerClusterJBombing() throws IOException {
    final List<RMContainer> toAdd = generateRMContainersToAdd(2000);

    for (int i = 0; i < 100; i++) {
      long start = System.currentTimeMillis();
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  RMContainerDataAccess rmcontainerDA
                  = (RMContainerDataAccess) RMStorageFactory
                  .getDataAccess(RMContainerDataAccess.class);
                  rmcontainerDA.addAll(toAdd);
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
      long duration = System.currentTimeMillis() - start;
      LOG.info(i + ") duration: " + duration);
    }
  }

  private List<Container> generateContainersToAdd(int nbContainers) {
    List<Container> toAdd = new ArrayList<Container>();

    for (int i = 0; i < nbContainers; i++) {
      byte[] container = new byte[77];
      random.nextBytes(container);
      Container c = new Container("containerId" + i + "_" + random.nextInt(10),
              container);
    }
    return toAdd;
  }

//  @Test
  public void ContainerClusterJBombing() throws IOException {
    final List<Container> toAdd = generateContainersToAdd(2000);
    LOG.info("start commit containers");
    for (int i = 0; i < 50; i++) {
      long start = System.currentTimeMillis();
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  ContainerDataAccess containerDA
                  = (ContainerDataAccess) RMStorageFactory
                  .getDataAccess(ContainerDataAccess.class);
                  containerDA.addAll(toAdd);
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
      long duration = System.currentTimeMillis() - start;
      LOG.info(i + ") duration: " + duration);
    }
  }

  private List<ContainerStatus> generateContainersStatusToAdd(
          List<RMContainer> rmContainersList) {
    List<ContainerStatus> toAdd = new ArrayList<ContainerStatus>();
    for (RMContainer rmContainer : rmContainersList) {
      ContainerStatus status = new ContainerStatus(rmContainer.getContainerId(),
              "running", "", 0,
              "nodeid_" + random.nextInt(10),0,ContainerStatus.Type.UCI);
      toAdd.add(status);
    }
    return toAdd;
  }

  private List<NextHeartbeat> generateNextHeartbeatToAdd(int nbHB) {
    List<NextHeartbeat> toAdd = new ArrayList<NextHeartbeat>();
    for (int i = 0; i < nbHB; i++) {
      NextHeartbeat nhb
              = new NextHeartbeat("nodeid_" + random.nextInt(10), true,0);
      toAdd.add(nhb);
    }
    return toAdd;
  }

  private List<NodeHBResponse> generateNodeHBResponseToAdd(int nbResponse) {
    List<NodeHBResponse> toAdd = new ArrayList<NodeHBResponse>();
    for (int i = 0; i < nbResponse; i++) {
      byte[] response = new byte[7];
      random.nextBytes(response);
      NodeHBResponse nr = new NodeHBResponse("nodeid_" + random.nextInt(10),
              response);
      toAdd.add(nr);
    }
    return toAdd;
  }

  private List<RMNode> generateRMNodeToAdd(int nbRMNodes) {
    List<RMNode> toAdd = new ArrayList<RMNode>();
    for (int i = 0; i < nbRMNodes; i++) {
      RMNode rmNode
              = new RMNode("nodeid_" + random.nextInt(4000), "hostName", 1,
                      1, "nodeAddress", "httpAddress", "", 1, "currentState",
                      "version", 1, 0);
      toAdd.add(rmNode);
    }
    return toAdd;
  }

  private List<ResourceRequest> generateResourceRequests(int nbRequests) {
    List<ResourceRequest> toAdd = new ArrayList<ResourceRequest>();
    for (int i = 0; i < nbRequests; i++) {
      byte[] state = new byte[10];
      random.nextBytes(state);
      ResourceRequest r = new ResourceRequest("appid_"+ random.nextInt(30), 1, "",
              state);
      toAdd.add(r);
    }
    return toAdd;
  }

//  @Test
  public void longRunTest() throws IOException {
    LOG.info("start Test");
    LightWeightRequestHandler createRMNode = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();
                RMNodeDataAccess da = (RMNodeDataAccess) RMStorageFactory.
                getDataAccess(RMNodeDataAccess.class);
                List<RMNode> toAdd = new ArrayList<RMNode>();
                for (int i = 0; i < 4000; i++) {
                  RMNode rmNode = new RMNode("nodeid_" + i, "hostName", 1,
                          1, "nodeAddress", "httpAddress", "", 1, "currentState",
                          "version", 1,0);
                  toAdd.add(rmNode);
                }

                da.addAll(toAdd);
                
                SchedulerApplicationDataAccess sada = (SchedulerApplicationDataAccess)
                        RMStorageFactory.getDataAccess(SchedulerApplicationDataAccess.class);
                List<SchedulerApplication> appToAdd = new ArrayList<SchedulerApplication>();
                for(int i=0;i<30;i++){
                  SchedulerApplication sa = new SchedulerApplication("appid_" + i, "user",
                          "queue");
                  appToAdd.add(sa);
                }
                sada.addAll(appToAdd);
                
                AppSchedulingInfoDataAccess asda = (AppSchedulingInfoDataAccess) 
                        RMStorageFactory.getDataAccess(AppSchedulingInfoDataAccess.class);
                List<AppSchedulingInfo> astoAdd = new ArrayList<AppSchedulingInfo>();
                for(int i=0; i<30; i++){
                  AppSchedulingInfo as = new AppSchedulingInfo("appid_" + i, "appid_" + i, "queue",
                          "user", 1, true, true);
                  astoAdd.add(as);
                }
                asda.addAll(astoAdd);
                connector.commit();
                return null;
              }
            };
    createRMNode.handle();

    for (int i = 0; i < 10000; i++) {
      final List<RMContainer> toAddRMContainers
              = generateRMContainersToAdd(2000);
      final List<Container> toAddContainers = generateContainersToAdd(2000);
      final List<ContainerStatus> toAddContainerStatus
              = generateContainersStatusToAdd(toAddRMContainers);
      final List<NextHeartbeat> toAddNextHeartbeats
              = generateNextHeartbeatToAdd(1000);
      final List<NodeHBResponse> toAddNodeHBResponse
              = generateNodeHBResponseToAdd(1000);
      final List<RMNode> toAddRMNodes = generateRMNodeToAdd(1000);
      final List<ResourceRequest> toAddResourceRequests
              = generateResourceRequests(2500);
      long start = System.currentTimeMillis();
      LightWeightRequestHandler bomb = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();
                  if(random.nextBoolean()){
                  RMNodeDataAccess da = (RMNodeDataAccess) RMStorageFactory.
                  getDataAccess(RMNodeDataAccess.class);
                  da.addAll(toAddRMNodes);
                  }
                  if(random.nextBoolean()){
                  RMContainerDataAccess rmcontainerDA
                  = (RMContainerDataAccess) RMStorageFactory
                  .getDataAccess(RMContainerDataAccess.class);
                  rmcontainerDA.addAll(toAddRMContainers);
                  }
                  if(random.nextBoolean()){
                  ContainerDataAccess containerDA
                  = (ContainerDataAccess) RMStorageFactory
                  .getDataAccess(ContainerDataAccess.class);
                  containerDA.addAll(toAddContainers);
                  }
                  if(random.nextBoolean()){
                  ContainerStatusDataAccess containerStatusDA
                  = (ContainerStatusDataAccess) RMStorageFactory.getDataAccess(
                          ContainerStatusDataAccess.class);
                  containerStatusDA.addAll(toAddContainerStatus);
                  }
                  NextHeartbeatDataAccess nhbda
                  = (NextHeartbeatDataAccess) RMStorageFactory.getDataAccess(
                          NextHeartbeatDataAccess.class);
                  nhbda.updateAll(toAddNextHeartbeats);

                  NodeHBResponseDataAccess nhbrda
                  = (NodeHBResponseDataAccess) RMStorageFactory.getDataAccess(
                          NodeHBResponseDataAccess.class);
                  nhbrda.addAll(toAddNodeHBResponse);
                  
                  if(random.nextBoolean()){
                  ResourceRequestDataAccess rrda
                  = (ResourceRequestDataAccess) RMStorageFactory.getDataAccess(
                          ResourceRequestDataAccess.class);
                  rrda.addAll(toAddResourceRequests);
                  }
                  connector.commit();
                  return null;
                }
              };
      bomb.handle();
      long duration = System.currentTimeMillis() - start;
//      Assert.assertTrue("commit too long " + duration, duration<1000);
      LOG.info(i + ") duration: " + duration);

    }
  }
}
