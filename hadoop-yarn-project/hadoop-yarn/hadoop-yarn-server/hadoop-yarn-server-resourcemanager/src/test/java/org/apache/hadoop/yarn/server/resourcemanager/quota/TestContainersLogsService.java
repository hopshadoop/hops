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
package org.apache.hadoop.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.dal.quota.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.ContainerLog;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import org.apache.hadoop.conf.Configuration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
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
      LOG.info("initialize db");
      DBUtility.InitializeDB();
    } catch (StorageInitializtionException ex) {
      LOG.error(null, ex);
    } catch (IOException ex) {
      LOG.error(null, ex);
    }
  }

  private final int GB = 1024;
  
  /**
   * Test if checkpoint is created with correct container status values
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testCheckpoints() throws Exception {
    int checkpointTicks = 10;
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setBoolean(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, true);
    conf.setInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE, checkpointTicks);
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            1);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 10 * GB);

    RMApp app = rm.submitApp(1 * GB);
    
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
      Thread.sleep(100);
    }
    Assert.assertEquals(4, conts.size());
    
    List<org.apache.hadoop.yarn.api.records.ContainerStatus> containersStatus = new ArrayList<>();
    for(Container c: conts){
      
      containersStatus.add(org.apache.hadoop.yarn.api.records.ContainerStatus.
              newInstance(c.getId(), ContainerState.RUNNING, "",0));
    }
    Map<ApplicationId, List<org.apache.hadoop.yarn.api.records.ContainerStatus>> status = new HashMap<>();
    status.put(am.getApplicationAttemptId().getApplicationId(), containersStatus);
    long initialCheckPointTick = rm.getRMContext().getContainersLogsService().getCurrentTick();
    nm1.nodeHeartbeat(status, true);

    int sleepTillCheckpoint = monitorInterval * (checkpointTicks + 1);
    Thread.sleep(sleepTillCheckpoint);
    // Check if checkpoint has containers and correct values
    Map<String, ContainerLog> cl = getContainersLogs();
    
    for (Container c: conts) {
      ContainerLog entry = cl.get(c.getId().toString());
      Assert.assertNotNull(entry);
      Assert.assertEquals(checkpointTicks+initialCheckPointTick, entry.getStop());
      Assert.assertEquals(ContainerExitStatus.CONTAINER_RUNNING_STATE, entry.
              getExitstatus());
    }
  }


  /**
   * Read all containers logs table entries
   *
   * @return
   */
  private Map<String, ContainerLog> getContainersLogs() throws IOException {
    Map<String, ContainerLog> containersLogs;

    LightWeightRequestHandler allContainersHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        ContainersLogsDataAccess containersLogsDA
                = (ContainersLogsDataAccess) RMStorageFactory
                .getDataAccess(ContainersLogsDataAccess.class);
        connector.beginTransaction();
        connector.readCommitted();

        Map<String, ContainerLog> allContainersLogs = containersLogsDA.getAll();

        connector.commit();

        return allContainersLogs;
      }
    };
    containersLogs = (Map<String, ContainerLog>) allContainersHandler.handle();

    return containersLogs;
  }
  
  
   /**
   * Test if logs are removed when nm shutdown
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testNMShutdown() throws Exception {
    int checkpointTicks = 10;
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setBoolean(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, true);
    conf.setInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE, checkpointTicks);
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            1);
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 4 * GB);

    RMApp app = rm.submitApp(1 * GB);
    
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

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
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(100);
    }
    Assert.assertEquals(4, conts.size());
    
    List<org.apache.hadoop.yarn.api.records.ContainerStatus> containersStatusNM1 = new ArrayList<>();
    List<org.apache.hadoop.yarn.api.records.ContainerStatus> containersStatusNM2 = new ArrayList<>();
    for(Container c: conts){
      if(c.getNodeId().equals(nm1.getNodeId())){
        containersStatusNM1.add(org.apache.hadoop.yarn.api.records.ContainerStatus.
              newInstance(c.getId(), ContainerState.RUNNING, "",0));
      }else{
        containersStatusNM2.add(org.apache.hadoop.yarn.api.records.ContainerStatus.
              newInstance(c.getId(), ContainerState.RUNNING, "",0));
      }
    }
    Map<ApplicationId, List<org.apache.hadoop.yarn.api.records.ContainerStatus>> status = new HashMap<>();
    status.put(am.getApplicationAttemptId().getApplicationId(), containersStatusNM1);
    nm1.nodeHeartbeat(status, true);
    
    status = new HashMap<>();
    status.put(am.getApplicationAttemptId().getApplicationId(), containersStatusNM2);
    nm2.nodeHeartbeat(status, true);
    
    Thread.sleep(monitorInterval+500);
    // Check if the containers are in containersLog
    Map<String, ContainerLog> cl = getContainersLogs();
    
    for (Container c: conts) {
      ContainerLog entry = cl.get(c.getId().toString());
      Assert.assertNotNull(entry);
    }
    
    nm2.unregisterNode();
    
    Thread.sleep(monitorInterval*2);
    
    // Check if the containers are in containersLog
    cl = getContainersLogs();
    
    for (Container c: conts) {
      ContainerLog entry = cl.get(c.getId().toString());
      if(c.getNodeId().equals(nm1.getNodeId())){
        Assert.assertNotNull(entry);
        Assert.assertEquals(ContainerExitStatus.CONTAINER_RUNNING_STATE, entry.
              getExitstatus());
      }else{
        if(entry!=null){
          Assert.assertNotEquals(ContainerExitStatus.CONTAINER_RUNNING_STATE, entry.getExitStatus());
        }
      }
    }
  }
  
  /**
   * Test if logs are removed when nm die
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testNMDie() throws Exception {
    int checkpointTicks = 10;
    int monitorInterval = 1000;
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL,
            monitorInterval);
    conf.setBoolean(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_ENABLED, true);
    conf.setInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE, checkpointTicks);
    conf.setInt(YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            1);
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 4 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 4 * GB);

    RMApp app = rm.submitApp(1 * GB);
    
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

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
      nm2.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      //LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(100);
    }
    Assert.assertEquals(4, conts.size());
    
    List<org.apache.hadoop.yarn.api.records.ContainerStatus> containersStatusNM1 = new ArrayList<>();
    List<org.apache.hadoop.yarn.api.records.ContainerStatus> containersStatusNM2 = new ArrayList<>();
    for(Container c: conts){
      if(c.getNodeId().equals(nm1.getNodeId())){
        containersStatusNM1.add(org.apache.hadoop.yarn.api.records.ContainerStatus.
              newInstance(c.getId(), ContainerState.RUNNING, "",0));
      }else{
        containersStatusNM2.add(org.apache.hadoop.yarn.api.records.ContainerStatus.
              newInstance(c.getId(), ContainerState.RUNNING, "",0));
      }
    }
    Map<ApplicationId, List<org.apache.hadoop.yarn.api.records.ContainerStatus>> status = new HashMap<>();
    status.put(am.getApplicationAttemptId().getApplicationId(), containersStatusNM1);
    nm1.nodeHeartbeat(status, true);
    
    status = new HashMap<>();
    status.put(am.getApplicationAttemptId().getApplicationId(), containersStatusNM2);
    nm2.nodeHeartbeat(status, true);
    
    Thread.sleep(monitorInterval+500);
    // Check if the containers are in containersLog
    Map<String, ContainerLog> cl = getContainersLogs();
    
    for (Container c: conts) {
      ContainerLog entry = cl.get(c.getId().toString());
      Assert.assertNotNull(entry);
    }
    
    rm.expireNM(nm2.getNodeId());
    
    Thread.sleep(monitorInterval*2);
    
    // Check if the containers are in containersLog
    cl = getContainersLogs();
    
    for (Container c: conts) {
      ContainerLog entry = cl.get(c.getId().toString());
      if(c.getNodeId().equals(nm1.getNodeId())){
        Assert.assertNotNull(entry);
        Assert.assertEquals(ContainerExitStatus.CONTAINER_RUNNING_STATE, entry.
              getExitstatus());
      }else{
        if(entry!=null){
          Assert.assertNotEquals(ContainerExitStatus.CONTAINER_RUNNING_STATE, entry.getExitStatus());
        }
      }
    }
  }

}
