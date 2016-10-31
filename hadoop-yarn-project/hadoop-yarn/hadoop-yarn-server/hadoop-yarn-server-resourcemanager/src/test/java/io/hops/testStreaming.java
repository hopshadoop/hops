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
package io.hops;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.streaming.ContainerIdToCleanEvent;
import io.hops.streaming.ContainerStatusEvent;
import io.hops.streaming.DBEvent;
import io.hops.streaming.FinishedApplicationsEvent;
import io.hops.streaming.NextHeartBeatEvent;
import io.hops.streaming.PendingEventEvent;
import io.hops.streaming.RMNodeEvent;
import io.hops.streaming.ResourceEvent;
import io.hops.streaming.UpdatedContainerInfoEvent;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class testStreaming {

  private static Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new YarnConfiguration();
    // Set configuration options
    conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
            "target/test-classes/RT_EventAPIConfig.ini");
    conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
            "target/test-classes/RM_EventAPIConfig.ini");
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  @Test
  public void test() throws StorageInitializtionException, IOException,
          InterruptedException {
    RMStorageFactory.kickTheNdbEventStreamingAPI(true, conf);
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        RMNodeDataAccess rmnDA
                = (RMNodeDataAccess) RMStorageFactory
                .getDataAccess(RMNodeDataAccess.class);
        rmnDA.add(new RMNode("nodeid", "node name", 42, 43,
                "tout vat bien", 1, "tiptop",
                "version", 2));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    DBEvent event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof RMNodeEvent);

    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        PendingEventDataAccess DA
                = (PendingEventDataAccess) RMStorageFactory
                .getDataAccess(PendingEventDataAccess.class);
        DA.add(new PendingEvent("nodeId", PendingEvent.Type.NODE_ADDED,
                PendingEvent.Status.NEW, 0, 1));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof PendingEventEvent);
    
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        ResourceDataAccess DA
                = (ResourceDataAccess) RMStorageFactory
                .getDataAccess(ResourceDataAccess.class);
        DA.add(new Resource("resource", 1, 2, 3));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof ResourceEvent);
    
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        UpdatedContainerInfoDataAccess DA
                = (UpdatedContainerInfoDataAccess) RMStorageFactory
                .getDataAccess(UpdatedContainerInfoDataAccess.class);
        List<UpdatedContainerInfo> toAdd = new ArrayList<>();
        toAdd.add(new UpdatedContainerInfo("rmnodeid", "containerid", 1,
                2));
        DA.addAll(toAdd);

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof UpdatedContainerInfoEvent);
    
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        ContainerStatusDataAccess DA
                = (ContainerStatusDataAccess) RMStorageFactory
                .getDataAccess(ContainerStatusDataAccess.class);
        List<ContainerStatus> toAdd = new ArrayList<>();
        toAdd.add(new ContainerStatus("containerid", "state", "diagnostics",
                0, "rmnodeid", 1, 2));
        DA.addAll(toAdd);

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof ContainerStatusEvent);
    
    RMStorageFactory.stopTheNdbEventStreamingAPI();
    RMStorageFactory.kickTheNdbEventStreamingAPI(false, conf);
    Thread.sleep(1000);
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        ContainerIdToCleanDataAccess DA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        DA.add(new ContainerId("rmnodeId", "containerId"));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof ContainerIdToCleanEvent);
    
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        NextHeartbeatDataAccess DA
                = (NextHeartbeatDataAccess) RMStorageFactory
                .getDataAccess(NextHeartbeatDataAccess.class);
        DA.update(new NextHeartbeat("nodeId", true));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof NextHeartBeatEvent);
    
    handler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();

        FinishedApplicationsDataAccess DA
                = (FinishedApplicationsDataAccess) RMStorageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
        DA.add(new FinishedApplications("rmnodeId", "applicationId"));

        connector.commit();
        return null;
      }
    };
    handler.handle();
    Thread.sleep(1000);
    Assert.assertEquals(DBEvent.receivedEvents.size(), 1);
    event = DBEvent.receivedEvents.take();
    Assert.assertTrue(event instanceof FinishedApplicationsEvent);
  }

}
