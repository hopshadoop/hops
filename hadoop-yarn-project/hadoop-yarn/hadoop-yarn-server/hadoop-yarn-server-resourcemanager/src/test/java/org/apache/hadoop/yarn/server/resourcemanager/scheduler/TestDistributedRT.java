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
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;

/**
 * Unit testing for the HOPS Distributed ResourceTrackerService.
 */
public class TestDistributedRT {

  private static final Log LOG = LogFactory.getLog(TestDistributedRT.class);
  private Configuration conf;

  @Before
  public void setup() {
    try {
      LOG.info("Setting up Factories");
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.EVENT_RT_CONFIG_PATH,
              "target/test-classes/RT_EventAPIConfig.ini");
      conf.set(YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
              "target/test-classes/RM_EventAPIConfig.ini");
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMUtilities.InitializeDB();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
    } catch (StorageInitializtionException ex) {
      LOG.error(ex);
    } catch (IOException ex) {
      LOG.error(ex);
    }
  }

  
  /**
   * Test to evaluate performance of RMNode batch.
   *
   * @throws Exception
   */
  @Test(timeout = 50000)
  public void testGetRMNodePerformance() throws Exception {
    MockRM rm = new MockRM(conf);
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, false);
    rm.start();
    String nodeId = "host1:1234";
    int numOfRetrievals = 500;
    MockNM nm1 = rm.registerNode(nodeId, 5120);

    nm1.nodeHeartbeat(BuilderUtils.
            newApplicationAttemptId(BuilderUtils.newApplicationId(0, 0), 0), 1,
        ContainerState.RUNNING);

    Thread.sleep(2000);
    rm.stop();
    Thread.sleep(5000);
    float nonBatchTime = 0;
    for (int i = 0; i < numOfRetrievals; i++) {
      long start = System.currentTimeMillis();
      RMUtilities.getRMNode(nodeId, null, conf);
      nonBatchTime += System.currentTimeMillis() - start;
    }
    nonBatchTime = nonBatchTime / numOfRetrievals;
    LOG.debug("HOP :: nonBatchTime=" + nonBatchTime + " ms");
    Thread.sleep(2000);
    float batchTime = 0;
    for (int i = 0; i < numOfRetrievals; i++) {
      long start = System.currentTimeMillis();
      RMUtilities.getRMNodeBatch(nodeId);
      batchTime += System.currentTimeMillis() - start;
    }
    batchTime = batchTime / numOfRetrievals;
    LOG.debug("HOP :: nonBatchTime=" + nonBatchTime + " ms");
    LOG.debug("HOP :: BatchTime=" + batchTime + " ms");
    Thread.sleep(2000);
  }

  /**
   * Tests that HopPendingEvents are persisted and retrieved properly.
   *
   * @throws StorageException
   * @throws IOException
   */
  @Test
  public void testGetPendingEvents() throws StorageException, IOException {
    //Create PendingEvents
    List<PendingEvent> newPendingEvents = new ArrayList<PendingEvent>();
    PendingEvent event1 = new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_ADDED,
        TablesDef.PendingEventTableDef.NEW, 3);
    PendingEvent event2 = new PendingEvent("c", TablesDef.PendingEventTableDef.NODE_ADDED,
        TablesDef.PendingEventTableDef.NEW, 1);
    PendingEvent event3 =
        new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_UPDATED,
            TablesDef.PendingEventTableDef.NEW, 0);
    PendingEvent event4 =
        new PendingEvent("a", TablesDef.PendingEventTableDef.NODE_UPDATED,
            TablesDef.PendingEventTableDef.NEW, 2);
    newPendingEvents.add(event1);
    newPendingEvents.add(event2);
    newPendingEvents.add(event3);
    newPendingEvents.add(event4);
    //Persist events to database
    RMUtilities.setPendingEvents(newPendingEvents);
    //Retrieve events from database ordered by id 
    Map<String, ConcurrentSkipListSet<PendingEvent>> events =
        RMUtilities.getPendingEvents(0, TablesDef.PendingEventTableDef.NEW);
    assertEquals(2, events.size());
  }

  /**
   * Tests that a NM is successfully registered when the request is sent
   * to the Leader RM and to the ResourceTracker.
   *
   * @throws InterruptedException
   * @throws Exception
   */
  @Test
  public void testNMRegistration() throws InterruptedException, Exception {
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD, 500);
    MockRM rm1 = new MockRM(conf);
    MockRM rm2 = new MockRM(conf);
    MockRM rmL;
    MockRM rmRT;
    rm1.start();
    rm2.start();
    try {
      //Wait for leader election to start
      Thread.sleep(3000);
      if (rm1.getRMContext().isLeader()) {
        rmL = rm1;
        rmRT = rm2;
      } else {
        rmL = rm2;
        rmRT = rm1;
      }
      FifoScheduler scheduler = (FifoScheduler) rmL.getResourceScheduler();

      //1. Register NM to the leader RM
      rmL.registerNode("host1:1234", 5012);
      int retries = 0;
      while (rmRT.getRMContext().getActiveRMNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      //Assert that registration was successful
      Assert.assertEquals(1, rmL.getRMContext().getActiveRMNodes().size());
      //Wait for scheduler to process added node
      retries = 0;
      while (scheduler.getNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, scheduler.getNodes().size());

      //2. Register NM to the ResourceTracker
      MockNM nm2 = rmRT.registerNode("host2:5678", 1024);
      //Wait for RT to process added node
      retries = 0;
      while (rmRT.getRMContext().getActiveRMNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, rmRT.getRMContext().getActiveRMNodes().size());
      //Wait for scheduler to process added node
      retries = 0;
      while (scheduler.getNodes().size() == 1 && retries++ < 20) {
        Thread.sleep(1000);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(2, scheduler.getNodes().size());

      //NM2 is lost, scheduler should be updated
      rmRT.sendNodeLost(nm2);
      retries = 0;
      while (scheduler.getNodes().size() == 2 && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, scheduler.getNodes().size());
    } finally {
      rm1.stop();
      rm2.stop();
    }
  }
  
  /**
   * Tests that heartbeats are processed by the Leader and ResourceTracker.
   *
   * @throws InterruptedException
   * @throws Exception
   */
  @Test
  public void testNMHeartbeat() throws InterruptedException, Exception {
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD, 500);
    MockRM rm1 = new MockRM(conf);
    MockRM rm2 = new MockRM(conf);
    MockRM rmL, rmRT;
    rm1.start();
    rm2.start();
    try {
      MockNM nm1, nm2;
      //Wait for leader election to start
      Thread.sleep(3000);
      if (rm1.getRMContext().isLeader()) {
        rmL = rm1;
        rmRT = rm2;
      } else {
        rmL = rm2;
        rmRT = rm1;
      }
      FifoScheduler scheduler = (FifoScheduler) rmL.getResourceScheduler();

      //1. Register NM to the leader RM
      nm1 = rmL.registerNode("host1:1234", 5012);
      int retries = 0;
      while (rmRT.getRMContext().getActiveRMNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      //Assert that registration was successful
      Assert.assertEquals(1, rmL.getRMContext().getActiveRMNodes().size());
      //Wait for scheduler to process added node
      retries = 0;
      while (scheduler.getNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, scheduler.getNodes().size());

      //2. Register NM to the ResourceTracker
      nm2 = rmRT.registerNode("host2:5678", 1024);
      //Wait for RT to process added node
      retries = 0;
      while (rmRT.getRMContext().getActiveRMNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, rmRT.getRMContext().getActiveRMNodes().size());
      //Wait for scheduler to process added node
      retries = 0;
      while (scheduler.getNodes().size() == 1 && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(2, scheduler.getNodes().size());

      //3. NM1 sends a heartbeat to the leader
      nm1.nodeHeartbeat(true);

      //4. NM2 sends a heartbeat to the ResourceTracker
      nm2.nodeHeartbeat(true);
      //Wait for heartbeat to be processed
      Thread.sleep(
              conf.
              getInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD,
                      YarnConfiguration.DEFAULT_HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD)
              * 2);
      //5. NM2 sends a heartbeat to the ResourceTracker to report unhealthy
      nm2.nodeHeartbeat(false);
      retries = 0;
      while (scheduler.getNodes().size() == 2 && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, scheduler.getNodes().size());

      //6. NM1 sends a heartbeat to the Leader to report unhealthy
      nm1.nodeHeartbeat(false);
      retries = 0;
      while (scheduler.getNodes().size() == 1 && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(0, scheduler.getNodes().size());
    } finally {
      rm1.stop();
      rm2.stop();
    }
  }
  
  /**
   * Tests that the RT and Scheduler will successfully handle a NM
   * that sends two registration requests.
   *
   * @throws InterruptedException
   * @throws Exception
   */
  @Test
  public void testConcurrentRegistration()
      throws InterruptedException, Exception {
    conf.setBoolean(YarnConfiguration.DISTRIBUTED_RM, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setInt(YarnConfiguration.HOPS_PENDING_EVENTS_RETRIEVAL_PERIOD, 500);
    MockRM rm1 = new MockRM(conf);
    MockRM rm2 = new MockRM(conf);
    MockRM rmL, rmRT;
    rm1.start();
    rm2.start();
    try {
      //Wait for leader election to start
      Thread.sleep(3000);
      if (rm1.getRMContext().isLeader()) {
        rmL = rm1;
        rmRT = rm2;
      } else {
        rmL = rm2;
        rmRT = rm1;
      }
      FifoScheduler scheduler = (FifoScheduler) rmL.getResourceScheduler();
      rmRT.registerNode("host:1234", 1024);
      rmRT.registerNode("host:1234", 1024);
      //Assert that registration was successful and node was registered once
      Assert.assertEquals(1, rmRT.getRMContext().getActiveRMNodes().size());
      //Wait for scheduler to process added node
      int retries = 0;
      while (scheduler.getNodes().isEmpty() && retries++ < 20) {
        Thread.sleep(100);
        commitDummyPendingEvent();
      }
      Assert.assertEquals(1, scheduler.getNodes().size());
    } finally {
      rm1.stop();
      rm2.stop();
    }
  }
  
  int pendingId=1000;
  
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
  
}
