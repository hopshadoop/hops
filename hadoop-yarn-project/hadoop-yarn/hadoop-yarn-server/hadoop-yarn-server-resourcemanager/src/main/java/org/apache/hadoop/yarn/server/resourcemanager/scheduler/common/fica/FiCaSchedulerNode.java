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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

public class FiCaSchedulerNode extends SchedulerNode implements Recoverable{

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerNode.class);
      //recovered
  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);//recovered
  private org.apache.hadoop.yarn.api.records.Resource availableResource =
      recordFactory
          .newRecordInstance(org.apache.hadoop.yarn.api.records.Resource.class);
      //recovered
  private org.apache.hadoop.yarn.api.records.Resource usedResource =
      recordFactory
          .newRecordInstance(org.apache.hadoop.yarn.api.records.Resource.class);
      //recovered
  private org.apache.hadoop.yarn.api.records.Resource totalResourceCapability;
      //recovered
  private volatile int numContainers;//recovered
  private RMContainer reservedContainer;//recovered
  /* set of containers that are allocated containers */
  private final Map<ContainerId, RMContainer> launchedContainers =
      new HashMap<ContainerId, RMContainer>();//recovered
  private final RMNode rmNode;//recovered
  private final String nodeName;//recovered
  private final RMContext rmContext;
  
  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName, 
          RMContext rmContext) {
    this.rmNode = node;
    this.rmContext = rmContext;
    this.availableResource.setMemory(node.getTotalCapability().getMemory());
    this.availableResource
        .setVirtualCores(node.getTotalCapability().getVirtualCores());
    totalResourceCapability = org.apache.hadoop.yarn.api.records.Resource
        .newInstance(node.getTotalCapability().getMemory(),
            node.getTotalCapability().getVirtualCores());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
  }

  public void recover(RMStateStore.RMState state) throws Exception{
    io.hops.metadata.yarn.entity.FiCaSchedulerNode hopNode = 
            state.getAllFiCaSchedulerNodes().get(rmNode.getNodeID().toString());
    numContainers = hopNode.getNumOfContainers();
    recoverResources(state);
    recoverLaunchedContainers(hopNode, state);
    if(hopNode.getReservedContainerId()!=null){
      reservedContainer = state.getRMContainer(hopNode.getReservedContainerId(), 
            rmContext);
    }
  }


  public RMNode getRMNode() {
    return this.rmNode;
  }

  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  @Override
  public String getNodeName() {
    return nodeName;
  }

  @Override
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   *
   * @param applicationId
   *     application
   * @param rmContainer
   *     allocated container
   */
  public synchronized void allocateContainer(ApplicationId applicationId,
      RMContainer rmContainer, TransactionState transactionState) {
    Container container = rmContainer.getContainer();
    deductAvailableResource(container.getResource(), transactionState);
    ++numContainers;

    launchedContainers.put(container.getId(), rmContainer);
    if (transactionState != null) {
      //HOP :: Update numContainers
      ((TransactionStateImpl) transactionState)
          .getFicaSchedulerNodeInfoToUpdate(this.getNodeID().toString())
          .infoToUpdate(this);
      //HOP :: Update reservedContainer
      ((TransactionStateImpl) transactionState)
          .getFicaSchedulerNodeInfoToUpdate(rmNode.getNodeID().toString())
          .toAddLaunchedContainers(container.getId().toString(),
              rmContainer.getContainerId().toString());
    }
    LOG.info("Assigned container " + container.getId() + " of capacity " +
        container.getResource() + " on host " + rmNode.getNodeAddress() +
        ", which currently has " + numContainers + " containers, " +
        getUsedResource() + " used and " + getAvailableResource() +
        " available");
  }

  @Override
  public synchronized org.apache.hadoop.yarn.api.records.Resource getAvailableResource() {
    return this.availableResource;
  }

  @Override
  public synchronized org.apache.hadoop.yarn.api.records.Resource getUsedResource() {
    return this.usedResource;
  }

  @Override
  public org.apache.hadoop.yarn.api.records.Resource getTotalResource() {
    return this.totalResourceCapability;
  }

  private synchronized boolean isValidContainer(Container c) {
    if (launchedContainers.containsKey(c.getId())) {
      return true;
    }
    return false;
  }

  private synchronized void updateResource(Container container,
      TransactionState transactionState) {
    addAvailableResource(container.getResource(), transactionState);

    --numContainers;
    //HOP :: Update numContainers
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .getFicaSchedulerNodeInfoToUpdate(this.getNodeID().toString())
          .infoToUpdate(this);
    }
  }

  /**
   * Release an allocated container on this node.
   *
   * @param container
   *     container to be released
   */
  public synchronized void releaseContainer(Container container,
      TransactionState transactionState) {
    if (!isValidContainer(container)) {
      LOG.error("Invalid container released " + container);
      return;
    }

        /* remove the containers from the nodemanger */
    if (null != launchedContainers.remove(container.getId())) {
      //HOP :: Update reservedContainer
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState)
            .getFicaSchedulerNodeInfoToUpdate(rmNode.getNodeID().toString())
            .toRemoveLaunchedContainers(container.getId().toString());
      }
      updateResource(container, transactionState);
    }

    LOG.info("Released container " + container.getId() + " of capacity " +
        container.getResource() + " on host " + rmNode.getNodeAddress() +
        ", which currently has " + numContainers + " containers, " +
        getUsedResource() + " used and " + getAvailableResource() +
        " available" + ", release resources=" + true);
  }

  private synchronized void addAvailableResource(
      org.apache.hadoop.yarn.api.records.Resource resource,
      TransactionState transactionState) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for " +
          rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
    //HOP :: Update resources

  }

  private synchronized void deductAvailableResource(
      org.apache.hadoop.yarn.api.records.Resource resource,
      TransactionState transactionState) {
    if (resource == null) {
      LOG.error(
          "Invalid deduction of null resource for " + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
    //HOP :: Update resources
  }

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers=" +
        getNumContainers() + " available=" +
        getAvailableResource().getMemory() + " used=" +
        getUsedResource().getMemory();
  }

  @Override
  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<RMContainer> getRunningContainers() {
    return new ArrayList<RMContainer>(launchedContainers.values());
  }

  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, Priority priority,
      RMContainer reservedContainer, TransactionState transactionState) {
    // Check if it's already reserved
    if (this.reservedContainer != null) {
      // Sanity check
      if (!reservedContainer.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException(
            "Trying to reserve" + " container " + reservedContainer +
                " on node " + reservedContainer.getReservedNode() +
                " when currently" + " reserved resource " +
                this.reservedContainer + " on node " +
                this.reservedContainer.getReservedNode());
      }

      // Cannot reserve more than one application attempt on a given node!
      // Reservation is still against attempt.
      if (!this.reservedContainer.getContainer().getId()
          .getApplicationAttemptId().equals(
              reservedContainer.getContainer().getId()
                  .getApplicationAttemptId())) {
        throw new IllegalStateException(
            "Trying to reserve" + " container " + reservedContainer +
                " for application " + application.getApplicationAttemptId() +
                " when currently" + " reserved container " +
                this.reservedContainer + " on node " + this);
      }

      LOG.info("Updated reserved container " +
          reservedContainer.getContainer().getId() + " on node " + this +
          " for application " + application);
    } else {
      LOG.info(
          "Reserved container " + reservedContainer.getContainer().getId() +
              " on node " + this + " for application " + application);
    }
    this.reservedContainer = reservedContainer;
    //HOP :: Update reservedContainer
    ((TransactionStateImpl) transactionState)
        .getFicaSchedulerNodeInfoToUpdate(this.getNodeID().toString())
        .updateReservedContainer(this);

  }

  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application,
      TransactionState transactionState) {

    // adding NP checks as this can now be called for preemption
    if (reservedContainer != null && reservedContainer.getContainer() != null &&
        reservedContainer.getContainer().getId() != null &&
        reservedContainer.getContainer().getId().getApplicationAttemptId() !=
            null) {

      // Cannot unreserve for wrong application...
      ApplicationAttemptId reservedApplication =
          reservedContainer.getContainer().getId().getApplicationAttemptId();
      if (!reservedApplication.equals(application.getApplicationAttemptId())) {
        throw new IllegalStateException(
            "Trying to unreserve " + " for application " +
                application.getApplicationAttemptId() +
                " when currently reserved " + " for application " +
                reservedApplication.getApplicationId() + " on node " + this);
      }
    }
    reservedContainer = null;
    ((TransactionStateImpl) transactionState).
            getFicaSchedulerNodeInfoToUpdate(this.getNodeID().toString())
        .infoToUpdate(this);

  }

  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  @Override
  public synchronized void applyDeltaOnAvailableResource(
      org.apache.hadoop.yarn.api.records.Resource deltaResource, 
          TransactionState ts) {
    // we can only adjust available resource if total resource is changed.
    Resources.addTo(this.availableResource, deltaResource);
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
            rmNode.getNodeID().toString(), Resource.TOTAL_CAPABILITY, 
            Resource.FICASCHEDULERNODE);
    Resource hopavailable = state.getResource(rmNode.getNodeID().toString(),
            Resource.AVAILABLE, Resource.FICASCHEDULERNODE);
    Resource hopused = state.getResource(rmNode.getNodeID().toString(),
            Resource.USED, Resource.FICASCHEDULERNODE);

    if (hoptotalCapability != null) {
      this.totalResourceCapability = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hoptotalCapability.getMemory(),
              hoptotalCapability.getVirtualCores());
    }
    if (hopavailable != null) {
      this.availableResource = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hopavailable.getMemory(),
              hopavailable.getVirtualCores());
    }
    if (hopused != null) {
      this.usedResource = org.apache.hadoop.yarn.api.records.Resource
          .newInstance(hopused.getMemory(), hopused.getVirtualCores());
    }
  }
}
