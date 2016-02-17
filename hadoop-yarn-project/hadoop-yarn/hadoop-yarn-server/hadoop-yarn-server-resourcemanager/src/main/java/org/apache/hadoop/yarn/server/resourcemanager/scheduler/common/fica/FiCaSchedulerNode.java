/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

import java.io.IOException;
import java.util.List;


public class FiCaSchedulerNode extends SchedulerNode implements Recoverable{

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerNode.class);

  private final RMContext rmContext;
  
  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName, 
          RMContext rmContext) {
    super(node, usePortForNodeName);

    this.rmContext = rmContext;
  }

  public void recover(RMStateStore.RMState state) throws Exception{
    io.hops.metadata.yarn.entity.FiCaSchedulerNode hopNode = 
            state.getAllFiCaSchedulerNodes().get(getRMNode().getNodeID().toString());
    numContainers = hopNode.getNumOfContainers();
    recoverResources(state);
    recoverLaunchedContainers(hopNode, state);
    if(hopNode.getReservedContainerId()!=null){
      setReservedContainer(state.getRMContainer(hopNode.getReservedContainerId(),
            rmContext), null);
    }
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, Priority priority,
      RMContainer container, TransactionState transactionState) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // Sanity check
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException(
            "Trying to reserve" + " container " + container +
                " on node " + container.getReservedNode() +
                " when currently" + " reserved resource " +
                reservedContainer + " on node " +
                reservedContainer.getReservedNode());
      }

      // Cannot reserve more than one application attempt on a given node!
      // Reservation is still against attempt.
      if (!reservedContainer.getContainer().getId()
          .getApplicationAttemptId().equals(
              container.getContainer().getId()
                  .getApplicationAttemptId())) {
        throw new IllegalStateException(
            "Trying to reserve" + " container " + container +
                " for application " + application.getApplicationAttemptId() +
                " when currently" + " reserved container " +
                reservedContainer + " on node " + this);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated reserved container " +
                container.getContainer().getId() + " on node " + this +
                " for application attempt " + application.getApplicationAttemptId());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reserved container " + container.getContainer().getId() +
                        " on node " + this +
                " for application attempt" + application.getApplicationAttemptId());
      }
    }

    setReservedContainer(container, transactionState);

  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application,
      TransactionState transactionState) {

    // adding NP checks as this can now be called for preemption
    if (getReservedContainer() != null
            && getReservedContainer().getContainer() != null
            && getReservedContainer().getContainer().getId() != null
            && getReservedContainer().getContainer().getId()
                .getApplicationAttemptId() != null) {

      // Cannot unreserve for wrong application...
      ApplicationAttemptId reservedApplication =
          getReservedContainer().getContainer().getId().getApplicationAttemptId();
      if (!reservedApplication.equals(application.getApplicationAttemptId())) {
        throw new IllegalStateException(
            "Trying to unreserve " + " for application " +
                application.getApplicationAttemptId() +
                " when currently reserved " + " for application " +
                reservedApplication.getApplicationId() + " on node " + this);
      }
    }

    setReservedContainer(null, transactionState);

  }

  private void recoverLaunchedContainers(
      io.hops.metadata.yarn.entity.FiCaSchedulerNode hopNode,
      RMStateStore.RMState state) throws IOException {
    //Map<ContainerId, RMContainer> launchedContainers
    List<LaunchedContainers> hopLaunchedContainersList =
        state.getLaunchedContainers(hopNode.getRmnodeId());
    for (LaunchedContainers lc : hopLaunchedContainersList) {

      RMContainer rMContainer =
          state.getRMContainer(lc.getRmContainerID(), rmContext);
      launchedContainers.put(rMContainer.getContainerId(), rMContainer);
    }

  }

  private void recoverResources(RMStateStore.RMState state) throws IOException {
    
    //TORECOVER recover from allocated containers.
    Resource hoptotalCapability = state.getResource(
            getRMNode().getNodeID().toString(), Resource.TOTAL_CAPABILITY,
            Resource.FICASCHEDULERNODE);
    Resource hopavailable = state.getResource(getRMNode().getNodeID().toString(),
            Resource.AVAILABLE, Resource.FICASCHEDULERNODE);
    Resource hopused = state.getResource(getRMNode().getNodeID().toString(),
            Resource.USED, Resource.FICASCHEDULERNODE);

    if (hoptotalCapability != null) {
      totalResourceCapability = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hoptotalCapability.getMemory(),
              hoptotalCapability.getVirtualCores());
    }
    if (hopavailable != null) {
      availableResource = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hopavailable.getMemory(),
              hopavailable.getVirtualCores());
    }
    if (hopused != null) {
      usedResource = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hopused.getMemory(), hopused.getVirtualCores());
    }
  }
}
