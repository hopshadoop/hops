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
package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRecoverRMContainerImpl {

  public DrainDispatcher drainDispatcher;

  @Before
  public void setUp() throws StorageInitializtionException, StorageException,
          IOException {
    InlineDispatcher rmDispatcher = new InlineDispatcher();

    YarnConfiguration conf = new YarnConfiguration();
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMStorageFactory.getConnector().formatStorage();

    EventHandler<RMAppAttemptEvent> appAttemptEventHandler = mock(
            EventHandler.class);
    EventHandler generic = mock(EventHandler.class);

    drainDispatcher = new DrainDispatcher();
    drainDispatcher.register(RMAppAttemptEventType.class,
            appAttemptEventHandler);
    drainDispatcher.register(RMNodeEventType.class, generic);
    drainDispatcher.init(conf);
    drainDispatcher.start();
  }

    // we need this function to insert the container and conatiner state (ha_container table) . Since we are only testing HopRMContainer, when we 
  // are retriving data from ha_rmcontainer, container_id is actually fetching from ha_container table. so just insert it here.
  public static void setContainerIdAndState(final String containerId,
          final RMContainer rmContainer) throws IOException {
    LightWeightRequestHandler setApplicationAttemptIdHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();
                List<io.hops.metadata.yarn.entity.Container> toAddContainers
                = new ArrayList<io.hops.metadata.yarn.entity.Container>();
                ContainerDataAccess cDA
                = (ContainerDataAccess) RMStorageFactory.getDataAccess(
                        ContainerDataAccess.class);
                io.hops.metadata.yarn.entity.Container hopContainer
                = new io.hops.metadata.yarn.entity.Container(rmContainer.
                        getContainerId().toString(),
                        ((ContainerPBImpl) rmContainer.getContainer()).
                        getProto().toByteArray());
                toAddContainers.add(hopContainer);
                cDA.addAll(toAddContainers);
                connector.commit();
                return null;
              }
            };
    setApplicationAttemptIdHandler.handle();
  }

  @Test
  public void testPersistReserveResource() throws IOException, InterruptedException {

    NodeId nodeId = BuilderUtils.newNodeId("host", 3425);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
            appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    ContainerAllocationExpirer expirer = mock(ContainerAllocationExpirer.class);

    Resource resource = BuilderUtils.newResource(512, 1);
    Priority priority = BuilderUtils.newPriority(5);

    // The reserved fields that are being tested
    Resource reservedResource = BuilderUtils.newResource(512, 1);
    NodeId reservedNodeId = BuilderUtils.newNodeId("host-reserved", 3426);
    Priority reservedPriority = BuilderUtils.newPriority(4);

    Container container = BuilderUtils.newContainer(containerId, nodeId,
            "host:3465", resource, priority, null);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    RMContainer rmContainer = new RMContainerImpl(container, appAttemptId,
            nodeId, "user", rmContext, null);

    assertEquals(RMContainerState.NEW, rmContainer.getState());
    assertEquals(resource, rmContainer.getAllocatedResource());
    assertEquals(nodeId, rmContainer.getAllocatedNode());
    assertEquals(priority, rmContainer.getAllocatedPriority());
    verify(writer).containerStarted(any(RMContainer.class), any(
            TransactionState.class));

    int rpcID = HopYarnAPIUtilities.getRPCID();
    byte[] submitAppData = new byte[1];

    RMUtilities.persistAppMasterRPC(rpcID, RPC.Type.SubmitApplication,
            submitAppData);

    TransactionStateImpl reservedTransaction
            = new TransactionStateImpl(
                    TransactionState.TransactionType.RM);

    rmContainer.handle(new RMContainerReservedEvent(containerId,
            reservedResource, reservedNodeId, reservedPriority,
            reservedTransaction));
    drainDispatcher.await();
    assertEquals(RMContainerState.RESERVED, rmContainer.getState());

    reservedTransaction.decCounter(TransactionState.TransactionType.RM);
    //wait for the transaction to be commited.
    Thread.sleep(1000);
    setContainerIdAndState(rmContainer.getContainerId().toString(), rmContainer);

    // Here we are calling databse fetching part and comparing with one we have inserted
    Map<String, io.hops.metadata.yarn.entity.RMContainer> containers
            = RMUtilities.getAllRMContainers();
    io.hops.metadata.yarn.entity.RMContainer recoverRmCont = containers.get(
            rmContainer.getContainerId().toString());

    // Testing whether the reserved fields are indeed restored
    assertEquals(recoverRmCont.getReservedNodeIdID(), reservedNodeId.toString());
    assertEquals(recoverRmCont.getReservedPriorityID(), reservedPriority.
            getPriority());
    assertEquals(recoverRmCont.getReservedMemory(), reservedResource.getMemory());
    assertEquals(recoverRmCont.getReservedVCores(), reservedResource.
            getVirtualCores());
  }
}
