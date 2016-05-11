/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * Represents an application attempt from the viewpoint of the FIFO or Capacity
 * scheduler.
 */
@Private
@Unstable
public class FiCaSchedulerApp extends SchedulerApplicationAttempt
        implements Recoverable {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerApp.class);

  private final Set<ContainerId> containersToPreempt =
      new HashSet<ContainerId>();
      //TORECOVER CAPACITY IF/When preemption is implemented

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, int maxAllocatedContainersPerRequest) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext, 
            maxAllocatedContainersPerRequest);
  }

  @Override
  public void recover(RMStateStore.RMState state) throws IOException {
    super.recover(state);
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      TransactionState transactionState) {

    // Remove from the list of containers
    if (null == liveContainers.remove(rmContainer.getContainerId())) {
      return false;
    }
    ((TransactionStateImpl)transactionState).addRMContainerToRemove(rmContainer);
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Inform the container
    rmContainer.handle(
        new RMContainerFinishedEvent(containerId, containerStatus, event,
            transactionState));
    LOG.info("Completed container: " + rmContainer.getContainerId() +
        " in state: " + rmContainer.getState() + " event:" + event);

    containersToPreempt.remove(rmContainer.getContainerId());

    RMAuditLogger
        .logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER, "SchedulerApp",
            getApplicationId(), containerId);
    
    // Update usage metrics 
    Resource containerResource =
        rmContainer.getContainer().getResource();
    queue.getMetrics()
        .releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);
    return true;
  }

  synchronized public RMContainer allocate(NodeType type,
      FiCaSchedulerNode node, Priority priority, ResourceRequest request,
      Container container, TransactionState transactionState) {
    LOG.debug("HOP :: FiCaSchedulerApp.allocate -START");
    if (isStopped) {
      return null;
    }
    
    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // Create RMContainer
    RMContainer rmContainer =
        new RMContainerImpl(container, this.getApplicationAttemptId(),
            node.getNodeID(), appSchedulingInfo.getUser(), this.rmContext,
            transactionState);

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    
    LOG.debug("HOP :: AttemptId is " +
        this.appSchedulingInfo.getApplicationAttemptId().toString());
    LOG.debug("HOP :: Size is " + newlyAllocatedContainers.size());
    
    liveContainers.put(container.getId(), rmContainer);
    

    // Update consumption and track allocations
    appSchedulingInfo
        .allocate(type, node, priority, request, container, transactionState);
    Resources.addTo(currentConsumption, container.getResource());
    //HOP : Update Resources
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).addRMContainerToAdd(
              (RMContainerImpl) rmContainer);
      ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
              this.appSchedulingInfo.getApplicationId()).
              getFiCaSchedulerAppInfo(
              this.appSchedulingInfo.getApplicationAttemptId()).
          updateAppInfo(this);
    }
    // Inform the container
    LOG.debug("HOP :: RMContainerEventType.START");
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START,
            transactionState));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId=" +
          container.getId().getApplicationAttemptId() + " container=" +
          container.getId() + " host=" + container.getNodeId().getHost() +
          " type=" + type);
    }
    RMAuditLogger
        .logSuccess(getUser(), AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
            getApplicationId(), container.getId());
    
    return rmContainer;
  }

  public synchronized boolean unreserve(FiCaSchedulerNode node,
      Priority priority, TransactionState transactionState) {
    Map<NodeId, RMContainer> reservedContainers =
        this.reservedContainers.get(priority);

    if (reservedContainers != null) {
      RMContainer reservedContainer =
          reservedContainers.remove(node.getNodeID());
      ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
              this.appSchedulingInfo.getApplicationId()).
              getFiCaSchedulerAppInfo(getApplicationAttemptId()).
              removeReservedContainer(reservedContainer, transactionState);
      // unreserve is now triggered in new scenarios (preemption)
      // as a consequence reservedcontainer might be null, adding NP-checks
      if (reservedContainer.getContainer() != null &&
          reservedContainer.getContainer().getResource() != null) {

        if (reservedContainers.isEmpty()) {
          this.reservedContainers.remove(priority);
        }
        // Reset the re-reservation count
        resetReReservations(priority, transactionState);

        Resource resource =
            reservedContainer.getContainer().getResource();
        Resources.subtractFrom(currentReservation, resource);

        //HOP : Update Resources
        ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
                this.appSchedulingInfo.getApplicationId())
                .getFiCaSchedulerAppInfo(
                        this.appSchedulingInfo.getApplicationAttemptId())
                .toUpdateResource(
                        io.hops.metadata.yarn.entity.Resource.CURRENTRESERVATION,
                        currentReservation);


        LOG.info(
            "Application " + getApplicationId() + " unreserved " + " on node " +
                node + ", currently has " + reservedContainers.size() +
                " at priority " + priority + "; currentReservation " +
                currentReservation);
        return true;
      }
    }
    return false;
  }

  public synchronized float getLocalityWaitFactor(Priority priority,
      int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources =
        Math.max(this.getResourceRequests(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float) requiredResources / clusterNodes), 1.0f);
  }

  public synchronized Resource getTotalPendingRequests() {
    Resource ret =
        Resource.newInstance(0, 0);
    for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
      // to avoid double counting we count only "ANY" resource requests
      if (ResourceRequest.isAnyLocation(rr.getResourceName())) {
        Resources.addTo(ret,
            Resources.multiply(rr.getCapability(), rr.getNumContainers()));
      }
    }
    return ret;
  }

  public synchronized void addPreemptContainer(ContainerId cont,
      TransactionState transactionState) {
    // ignore already completed containers
    if (liveContainers.containsKey(cont)) {
      containersToPreempt.add(cont);
    }
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @param rc
   * @param clusterResource
   * @param minimumAllocation
   * @return an allocation
   */
  public synchronized Allocation getAllocation(ResourceCalculator rc,
    Resource clusterResource, Resource minimumAllocation,
    TransactionState transactionState) {

    Set<ContainerId> currentContPreemption = Collections
        .unmodifiableSet(new HashSet<ContainerId>(containersToPreempt));
    containersToPreempt.clear();
    Resource tot =
        Resource.newInstance(0, 0);
    for (ContainerId c : currentContPreemption) {
      Resources.addTo(tot, liveContainers.get(c).getContainer().getResource());
    }
    int numCont = (int) Math
        .ceil(Resources.divide(rc, clusterResource, tot, minimumAllocation));
    ResourceRequest rr = ResourceRequest
        .newInstance(Priority.UNDEFINED, ResourceRequest.ANY, minimumAllocation,
            numCont);
    ContainersAndNMTokensAllocation allocation =
        pullNewlyAllocatedContainersAndNMTokens(transactionState);
    return new Allocation(allocation.getContainerList(), getHeadroom(), null,
        currentContPreemption, Collections.singletonList(rr),
        allocation.getNMTokenList());
  }

}
