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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import io.hops.ha.common.TransactionState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

@Private
@Unstable
public class AppSchedulable extends Schedulable {
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  
  private FairScheduler scheduler;
  private FSSchedulerApp app;
  private Resource demand = Resources.createResource(0);
  private long startTime;
  private static RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private static final Log LOG = LogFactory.getLog(AppSchedulable.class);
  private FSLeafQueue queue;
  private RMContainerTokenSecretManager containerTokenSecretManager;

  private RMContainerComparator comparator = new RMContainerComparator();

  public AppSchedulable(FairScheduler scheduler, FSSchedulerApp app,
      FSLeafQueue queue) {
    this.scheduler = scheduler;
    this.app = app;
    this.startTime = scheduler.getClock().getTime();
    this.queue = queue;
    this.containerTokenSecretManager = scheduler.
        getContainerTokenSecretManager();
  }

  @Override
  public String getName() {
    return app.getApplicationId().toString();
  }

  public FSSchedulerApp getApp() {
    return app;
  }

  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    // Demand is current consumption plus outstanding requests
    Resources.addTo(demand, app.getCurrentConsumption());

    // Add up outstanding resource requests
    synchronized (app) {
      for (Priority p : app.getPriorities()) {
        for (ResourceRequest r : app.getResourceRequests(p).values()) {
          Resource total =
              Resources.multiply(r.getCapability(), r.getNumContainers());
          Resources.addTo(demand, total);
        }
      }
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Resource getResourceUsage() {
    // Here the getPreemptedResources() always return zero, except in
    // a preemption round
    return Resources.subtract(app.getCurrentConsumption(),
            app.getPreemptedResources());
  }


  @Override
  public Resource getMinShare() {
    return Resources.none();
  }
  
  @Override
  public Resource getMaxShare() {
    return Resources.unbounded();
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  @Override
  public ResourceWeights getWeights() {
    return scheduler.getAppWeight(this);
  }

  @Override
  public Priority getPriority() {
    // Right now per-app priorities are not passed to scheduler,
    // so everyone has the same priority.
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }

  /**
   * Create and return a container object reflecting an allocation for the
   * given appliction on the given node with the given capability and
   * priority.
   */
  public Container createContainer(FSSchedulerApp application,
      FSSchedulerNode node, Resource capability, Priority priority,
      TransactionState ts) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils
        .newContainerId(application.getApplicationAttemptId(),
            application.getNewContainerId(ts));

    // Create the container
    Container container = BuilderUtils
        .newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(),
            capability, priority, null);

    return container;
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in the {@link FSSchedulerNode} and {@link SchedulerApp} classes.
   */
  private void reserve(Priority priority, FSSchedulerNode node,
      Container container, boolean alreadyReserved,
      TransactionState transactionState) {
    LOG.info("Making reservation: node=" + node.getNodeName() +
            " app_id=" + app.getApplicationId());
    if (!alreadyReserved) {
      getMetrics().reserveResource(app.getUser(), container.getResource());
      RMContainer rmContainer =
          app.reserve(node, priority, null, container, transactionState);
      node.reserveResource(app, priority, rmContainer);
    } else {
      RMContainer rmContainer = node.getReservedContainer();
      app.reserve(node, priority, rmContainer, container, transactionState);
      node.reserveResource(app, priority, rmContainer);
    }
  }

  /**
   * Remove the reservation on {@code node} at the given
   * {@link Priority}. This dispatches to the SchedulerApp and SchedulerNode
   * handlers for an unreservation.
   */
  public void unreserve(Priority priority, FSSchedulerNode node,
          TransactionState transactionState) {
    RMContainer rmContainer = node.getReservedContainer();
    app.unreserve(node, priority, transactionState);
    node.unreserveResource(app);
    getMetrics().unreserveResource(app.getUser(),
        rmContainer.getContainer().getResource());
  }


  /**
   * Assign a container to this node to facilitate {@code request}. If node
   * does
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   * @param node
   * The node to try placing the container on.
   * @param priority
   * The requested priority for the container.
   * @param request
   * The ResourceRequest we're trying to satisfy.
   * @param type
   * The locality of the assignment.
   * @param reserved
   * Whether there's already a container reserved for this app on the node.
   * @return
   * If an assignment was made, returns the resources allocated to the
   * container.  If a reservation was made, returns
   * FairScheduler.CONTAINER_RESERVED.  If no assignment or reservation was
   * made, returns an empty resource.
   */

  private Resource assignContainer(FSSchedulerNode node, ResourceRequest request,
                                   NodeType type, boolean reserved,
                                   TransactionState transactionState) {

    // How much does this request need?
    Resource capability = request.getCapability();

    // How much does the node have?
    Resource available = node.getAvailableResource();

    Container container = null;
    if (reserved) {
      container = node.getReservedContainer().getContainer();
    } else {
      container =
          createContainer(app, node, capability, request.getPriority(), transactionState);
    }

    // Can we allocate a container on this node?
    if (Resources.fitsIn(capability, available)) {
      // Inform the application of the new container for this request
      RMContainer allocatedContainer =
          app.allocate(type, node, request.getPriority(), request, container,
              transactionState);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        if (reserved) {
          unreserve(request.getPriority(), node, transactionState);
        }
        return Resources.none();
      }

      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(request.getPriority(), node, transactionState);
      }

      // Inform the node
      node.allocateContainer(app.getApplicationId(), allocatedContainer);

      return container.getResource();
    } else {
      // The desired container won't fit here, so reserve
      reserve(request.getPriority(), node, container, reserved, transactionState);

      return FairScheduler.CONTAINER_RESERVED;
    }
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved,
      TransactionState transactionState) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    Collection<Priority> prioritiesToTry = (reserved) ?
        Arrays.asList(node.getReservedContainer().getReservedPriority()) :
        app.getPriorities();
    
    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    synchronized (app) {
      for (Priority priority : prioritiesToTry) {
        if (app.getTotalRequiredResources(priority) <= 0 ||
            !hasContainerForNode(priority, node)) {
          continue;
        }
        
        app.addSchedulingOpportunity(priority, transactionState);

        ResourceRequest rackLocalRequest =
            app.getResourceRequest(priority, node.getRackName());
        ResourceRequest localRequest =
            app.getResourceRequest(priority, node.getNodeName());
        
        if (localRequest != null && !localRequest.getRelaxLocality()) {
          LOG.warn("Relax locality off is not supported on local request: " +
              localRequest);
        }
        
        NodeType allowedLocality;
        if (scheduler.isContinuousSchedulingEnabled()) {
          allowedLocality = app.getAllowedLocalityLevelByTime(priority,
              scheduler.getNodeLocalityDelayMs(),
              scheduler.getRackLocalityDelayMs(),
              scheduler.getClock().getTime(), transactionState);
        } else {
          allowedLocality = app.getAllowedLocalityLevel(priority,
              scheduler.getNumClusterNodes(),
              scheduler.getNodeLocalityThreshold(),
              scheduler.getRackLocalityThreshold(), transactionState);
        }

        if (rackLocalRequest != null &&
            rackLocalRequest.getNumContainers() != 0 && localRequest != null &&
            localRequest.getNumContainers() != 0) {
          return assignContainer(node, localRequest,
              NodeType.NODE_LOCAL, reserved, transactionState);
        }
        
        if (rackLocalRequest != null && !rackLocalRequest.getRelaxLocality()) {
          continue;
        }

        if (rackLocalRequest != null &&
            rackLocalRequest.getNumContainers() != 0 &&
            (allowedLocality.equals(NodeType.RACK_LOCAL) ||
                allowedLocality.equals(NodeType.OFF_SWITCH))) {
          return assignContainer(node, rackLocalRequest,
              NodeType.RACK_LOCAL, reserved, transactionState);
        }

        ResourceRequest offSwitchRequest =
            app.getResourceRequest(priority, ResourceRequest.ANY);
        if (offSwitchRequest != null && !offSwitchRequest.getRelaxLocality()) {
          continue;
        }
        
        if (offSwitchRequest != null &&
            offSwitchRequest.getNumContainers() != 0 &&
            allowedLocality.equals(NodeType.OFF_SWITCH)) {
          return assignContainer(node, offSwitchRequest,
              NodeType.OFF_SWITCH, reserved, transactionState);
        }
      }
    }
    return Resources.none();
  }

  /**
   * Called when this application already has an existing reservation on the
   * given node.  Sees whether we can turn the reservation into an allocation.
   * Also checks whether the application needs the reservation anymore, and
   * releases it if not.
   *
   * @param node
   * Node that the application has an existing reservation on
   */
    public Resource assignReservedContainer(FSSchedulerNode node,
                                            TransactionState transactionState) {
        RMContainer rmContainer = node.getReservedContainer();
        Priority priority = rmContainer.getReservedPriority();

        // Make sure the application still needs requests at this priority
        if (app.getTotalRequiredResources(priority) == 0) {
            unreserve(priority, node, transactionState);
            return Resources.none();
        }

        // Fail early if the reserved container won't fit.
        // Note that we have an assumption here that there's only one container size
        // per priority.
        if (!Resources.fitsIn(node.getReservedContainer().getReservedResource(),
                node.getAvailableResource())) {
            return Resources.none();
        }

        return assignContainer(node, true, transactionState);
    }



  @Override
  public Resource assignContainer(FSSchedulerNode node,
      TransactionState transactionState) {
    return assignContainer(node, false, transactionState);
  }

  /**
   * Preempt a running container according to the priority
   */
  @Override
  public RMContainer preemptContainer() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("App " + getName() + " is going to preempt a running " +
          "container");
    }

    RMContainer toBePreempted = null;
    for (RMContainer container : app.getLiveContainers()) {
      if (!app.getPreemptionContainers().contains(container) &&
              (toBePreempted == null ||
                comparator.compare(toBePreempted, container)> 0 )) {
        toBePreempted = container;
      }
    }

    return toBePreempted;
  }

  /**
   * Whether this app has containers requests that could be satisfied on the
   * given node, if the node had full space.
   */
  public boolean hasContainerForNode(Priority prio, FSSchedulerNode node) {
    ResourceRequest anyRequest =
        app.getResourceRequest(prio, ResourceRequest.ANY);
    ResourceRequest rackRequest =
        app.getResourceRequest(prio, node.getRackName());
    ResourceRequest nodeRequest =
        app.getResourceRequest(prio, node.getNodeName());

    return
        // There must be outstanding requests at the given priority:
        anyRequest != null && anyRequest.getNumContainers() > 0 &&
            // If locality relaxation is turned off at *-level, there must be a
            // non-zero request for the node's rack:
            (anyRequest.getRelaxLocality() ||
                (rackRequest != null && rackRequest.getNumContainers() > 0)) &&
            // If locality relaxation is turned off at rack-level, there must be a
            // non-zero request at the node:
            (rackRequest == null || rackRequest.getRelaxLocality() ||
                (nodeRequest != null && nodeRequest.getNumContainers() > 0)) &&
            // The requested container must be able to fit on the node:
            Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
                anyRequest.getCapability(),
                node.getRMNode().getTotalCapability());
  }

  static class RMContainerComparator implements Comparator<RMContainer>,
          Serializable {
    @Override
    public int compare(RMContainer c1, RMContainer c2) {
      int ret = c1.getContainer().getPriority().compareTo(
              c2.getContainer().getPriority());
      if (ret == 0) {
        return c2.getContainerId().compareTo(c1.getContainerId());
      }
      return ret;
    }
  }
}
