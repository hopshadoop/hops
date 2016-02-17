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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.ResourceRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class keeps track of all the consumption of an application. This also
 * keeps track of current running/completed containers for the application.
 */
@Private
@Unstable
public class AppSchedulingInfo {

  private static final Log LOG = LogFactory.getLog(AppSchedulingInfo.class);
  private final ApplicationAttemptId applicationAttemptId;//recovered
  final ApplicationId applicationId;//recovered
  private final String queueName;//recovered
  Queue queue;//recovered
  final String user;//recovered
  private final AtomicInteger containerIdCounter = new AtomicInteger(0);
      //recovered

  final Set<Priority> priorities = new TreeSet<Priority>(
      new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
      //recovered
  final Map<Priority, Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest>>
      requests =
      new HashMap<Priority, Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest>>();
      //recovered
  private Set<String> blacklist = new HashSet<String>();//recovered

  //private final ApplicationStore store;
  private ActiveUsersManager activeUsersManager;//TORECOVER

  /* Allocated by scheduler */
  boolean pending = true;// for app metrics //recovered

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId, String user,
      Queue queue, ActiveUsersManager activeUsersManager) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.queueName = queue.getQueueName();
    this.user = user;
    this.activeUsersManager = activeUsersManager;

  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getUser() {
    return user;
  }

  public synchronized boolean isPending() {
    return pending;
  }

  /**
   * Clear any pending requests from this application.
   */
  private synchronized void clearRequests(TransactionState transactionState) {

    for (Priority priority : priorities) {
      for (org.apache.hadoop.yarn.api.records.ResourceRequest request : requests
          .get(priority).values()) {
        if (transactionState != null) {
          ((TransactionStateImpl) transactionState)
              .getSchedulerApplicationInfos(this.applicationId).
              getFiCaSchedulerAppInfo(this.applicationAttemptId).
              setRequestsToRemove(request);
        }
      }
    }

    priorities.clear();

    requests.clear();

    LOG.info("Application " + applicationId + " requests cleared");
  }

  public int getNewContainerId() {
    return this.containerIdCounter
        .incrementAndGet();//pushed to db in SchedulerApplicationAttempt
  }

  public int getLastContainerId() {
    return this.containerIdCounter.get();
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources
   * acquired by the application.
   *
   * @param requests
   *     resources to be acquired
   * * @param recoverPreemptedRequest recover Resource Request on preemption
   */
  synchronized public void updateResourceRequests(
      List<org.apache.hadoop.yarn.api.records.ResourceRequest> requests,
      boolean recoverPreemptedRequest, TransactionState ts) {
    QueueMetrics metrics = queue.getMetrics();

    // Update resource requests
    for (org.apache.hadoop.yarn.api.records.ResourceRequest request : requests) {
      Priority priority = request.getPriority();
      String resourceName = request.getResourceName();
      boolean updatePendingResources = false;
      org.apache.hadoop.yarn.api.records.ResourceRequest lastRequest = null;

      if (resourceName
          .equals(org.apache.hadoop.yarn.api.records.ResourceRequest.ANY)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("update:" + " application=" + applicationId + " request=" +
              request);
        }
        updatePendingResources = true;

        // Premature optimization?
        // Assumes that we won't see more than one priority request updated
        // in one call, reasonable assumption... however, it's totally safe
        // to activate same application more than once.
        // Thus we don't need another loop ala the one in decrementOutstanding()
        // which is needed during deactivate.
        if (request.getNumContainers() > 0) {
          activeUsersManager.activateApplication(user, applicationId);
        }
      }

      Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest> asks =
          this.requests.get(priority);

      if (asks == null) {
        asks =
            new HashMap<String, org.apache.hadoop.yarn.api.records.ResourceRequest>();
        this.requests.put(priority, asks);
        this.priorities.add(priority);

      } 
      lastRequest = asks.get(resourceName);
 
      if (recoverPreemptedRequest && lastRequest != null) {
        // Increment the number of containers to 1, as it is recovering a
        // single container.
        request.setNumContainers(lastRequest.getNumContainers() + 1);
      }

      asks.put(resourceName, request);
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
          .getFiCaSchedulerAppInfo(this.applicationAttemptId)
          .setRequestsToAdd(request);

      if (updatePendingResources) {

        // Similarly, deactivate application?
        if (request.getNumContainers() <= 0) {
          LOG.info("checking for deactivate... ");
          checkForDeactivation();
        }

        int lastRequestContainers =
            lastRequest != null ? lastRequest.getNumContainers() : 0;
        Resource lastRequestCapability =
            lastRequest != null ? lastRequest.getCapability() :
                Resources.none();
        metrics.incrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        metrics.decrPendingResources(user, lastRequestContainers,
            lastRequestCapability);
      }
    }
  }

  /**
   * The ApplicationMaster is updating the blacklist
   *
   * @param blacklistAdditions
   *     resources to be added to the blacklist
   * @param blacklistRemovals
   *     resources to be removed from the blacklist
   */
  synchronized public void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals, TransactionState ts) {
    // Add to blacklist
    if (blacklistAdditions != null) {
      blacklist.addAll(blacklistAdditions);
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId).
            getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setBlacklistToAdd(blacklistAdditions);
      }
    }

    // Remove from blacklist
    if (blacklistRemovals != null) {
      blacklist.removeAll(blacklistRemovals);
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId)
            .setBlacklistToRemove(blacklistRemovals);
      }
    }
  }

  synchronized public Collection<Priority> getPriorities() {
    return priorities;
  }

  synchronized public Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest> getResourceRequests(
      Priority priority) {
    return requests.get(priority);
  }

  synchronized public List<org.apache.hadoop.yarn.api.records.ResourceRequest> getAllResourceRequests() {
    List<org.apache.hadoop.yarn.api.records.ResourceRequest> ret =
        new ArrayList<org.apache.hadoop.yarn.api.records.ResourceRequest>();
    for (Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest> r : requests
        .values()) {
      ret.addAll(r.values());
    }
    return ret;
  }

  synchronized public org.apache.hadoop.yarn.api.records.ResourceRequest getResourceRequest(
      Priority priority, String resourceName) {
    Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest>
        nodeRequests = requests.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
  }

  public synchronized Resource getResource(Priority priority) {
    org.apache.hadoop.yarn.api.records.ResourceRequest request =
        getResourceRequest(priority,
            org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
    return request.getCapability();
  }

  public synchronized boolean isBlacklisted(String resourceName) {
    return blacklist.contains(resourceName);
  }

  /**
   * Resources have been allocated to this application by the resource
   * scheduler. Track them.
   *
   * @param type
   *     the type of the node
   * @param node
   *     the nodeinfo of the node
   * @param priority
   *     the priority of the request.
   * @param request
   *     the request
   * @param container
   *     the containers allocated.
   * @param ts
   */
  synchronized public List<org.apache.hadoop.yarn.api.records.ResourceRequest>
         allocate(NodeType type, SchedulerNode node, Priority priority,
      org.apache.hadoop.yarn.api.records.ResourceRequest request,
      Container container, TransactionState ts) {
    List<org.apache.hadoop.yarn.api.records.ResourceRequest> resourceRequests =
            new ArrayList<org.apache.hadoop.yarn.api.records.ResourceRequest>();
    if (type == NodeType.NODE_LOCAL) {
      allocateNodeLocal(node, priority, request, container, resourceRequests, 
              ts);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, container, resourceRequests, 
              ts);
    } else {
      allocateOffSwitch(request, resourceRequests);
    }
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // once an allocation is done we assume the application is
      // running from scheduler's POV.
      pending =
          false; //push in FiCaSchedulerApp, TORECOVER FAIR check if done in FFSchedulerApp (fair)
      metrics.runAppAttempt(applicationId, user);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationId=" + applicationId + " container=" +
          container.getId() + " host=" + container.getNodeId().toString() +
          " user=" + user + " resource=" + request.getCapability());
    }
    metrics.allocateResources(user, 1, request.getCapability(), true);
    return resourceRequests;
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   *
   * @param allocatedContainers
   *     resources allocated to the application
   */
  synchronized private void allocateNodeLocal(SchedulerNode node,
      Priority priority,
      org.apache.hadoop.yarn.api.records.ResourceRequest nodeLocalRequest,
      Container container,
      List<org.apache.hadoop.yarn.api.records.ResourceRequest> resourceRequests,
          TransactionState ts) {
    // Update future requirements
    nodeLocalRequest.setNumContainers(nodeLocalRequest.getNumContainers() - 1);
    if (nodeLocalRequest.getNumContainers() == 0) {
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToRemove(
                this.requests.get(priority).get(node.getNodeName()));
      }
      this.requests.get(priority).remove(node.getNodeName());
    } else {
      //update the request in db
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToAdd(nodeLocalRequest);
      }
    }

    org.apache.hadoop.yarn.api.records.ResourceRequest rackLocalRequest =
        requests.get(priority).get(node.getRackName());
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToRemove(
                this.requests.get(priority).get(node.getRackName()));
      }
      this.requests.get(priority).remove(node.getRackName());
    } else {
      //update the request in db
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToAdd(rackLocalRequest);
      }
    }

    org.apache.hadoop.yarn.api.records.ResourceRequest offRackRequest =
        requests.get(priority)
            .get(org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    if (ts != null) {
      //update the request in db
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId).
          getFiCaSchedulerAppInfo(this.applicationAttemptId).
          setRequestsToAdd(offRackRequest);
    }
    
    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   *
   * @param allocatedContainers
   *     resources allocated to the application
   */
  synchronized private void allocateRackLocal(SchedulerNode node,
      Priority priority,
      org.apache.hadoop.yarn.api.records.ResourceRequest rackLocalRequest,
      Container container,
      List<org.apache.hadoop.yarn.api.records.ResourceRequest> resourceRequests,
      TransactionState ts) {
    // Update future requirements
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId)
            .getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToRemove(
                this.requests.get(priority).get(node.getRackName()));
      }
      this.requests.get(priority).remove(node.getRackName());
    } else {
      //update request in db
      if (ts != null) {
        ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId).
            getFiCaSchedulerAppInfo(this.applicationAttemptId).
            setRequestsToAdd(rackLocalRequest);
      }
    }

    org.apache.hadoop.yarn.api.records.ResourceRequest offRackRequest =
        requests.get(priority)
            .get(org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
    if (ts != null) {
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(this.applicationId).
          getFiCaSchedulerAppInfo(this.applicationAttemptId).
          setRequestsToAdd(offRackRequest);
    }
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   *
   * @param allocatedContainers
   *     resources allocated to the application
   */
  synchronized private void allocateOffSwitch(
      org.apache.hadoop.yarn.api.records.ResourceRequest offSwitchRequest,
      List<org.apache.hadoop.yarn.api.records.ResourceRequest> resourceRequests)
  {
    // Update future requirements
    decrementOutstanding(offSwitchRequest);
    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
  }

  synchronized private void decrementOutstanding(
      org.apache.hadoop.yarn.api.records.ResourceRequest offSwitchRequest) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;

    // Do not remove ANY
    offSwitchRequest.setNumContainers(numOffSwitchContainers);

    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      checkForDeactivation();
    }
  }
  
  public org.apache.hadoop.yarn.api.records.ResourceRequest cloneResourceRequest(
                 org.apache.hadoop.yarn.api.records.ResourceRequest request) {
    org.apache.hadoop.yarn.api.records.ResourceRequest newRequest = 
            org.apache.hadoop.yarn.api.records.ResourceRequest.newInstance(
        request.getPriority(), request.getResourceName(),
        request.getCapability(), 1, request.getRelaxLocality());
    return newRequest;
  }

  synchronized private void checkForDeactivation() {
    boolean deactivate = true;
    for (Priority priority : getPriorities()) {
      org.apache.hadoop.yarn.api.records.ResourceRequest request =
          getResourceRequest(priority,
              org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
      if (request.getNumContainers() > 0) {
        deactivate = false;
        break;
      }
    }
    if (deactivate) {
      activeUsersManager.deactivateApplication(user, applicationId);
    }
  }

  //TODORECOVER ?
  synchronized public void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    for (Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest> asks : requests
        .values()) {
      org.apache.hadoop.yarn.api.records.ResourceRequest request =
          asks.get(org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
      if (request != null) {
        oldMetrics.decrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        newMetrics.incrPendingResources(user, request.getNumContainers(),
            request.getCapability());
      }
    }
    oldMetrics.moveAppFrom(this);
    newMetrics.moveAppTo(this);
    activeUsersManager.deactivateApplication(user, applicationId);
    activeUsersManager = newQueue.getActiveUsersManager();
    activeUsersManager.activateApplication(user, applicationId);
    this.queue = newQueue;
  }

  synchronized public void stop(RMAppAttemptState rmAppAttemptFinalState,
      TransactionState transactionState) {
    // clear pending resources metrics for the application
    QueueMetrics metrics = queue.getMetrics();
    for (Map<String, org.apache.hadoop.yarn.api.records.ResourceRequest> asks : requests
        .values()) {
      org.apache.hadoop.yarn.api.records.ResourceRequest request =
          asks.get(org.apache.hadoop.yarn.api.records.ResourceRequest.ANY);
      if (request != null) {
        metrics.decrPendingResources(user, request.getNumContainers(), request
            .getCapability()); //TORECOVER MS: this updates to queuemetrics are not pushed
      }
    }
    metrics.finishAppAttempt(applicationId, pending,
        user);//TORECOVER OPT: this updates to queuemetrics are not pushed

    // Clear requests themselves
    clearRequests(transactionState);
  }

  public synchronized void setQueue(Queue queue) {
    this.queue = queue;
  }

  public synchronized Set<String> getBlackList() {
    return this.blacklist;
  }

  public synchronized void transferStateFromPreviousAppSchedulingInfo(
      AppSchedulingInfo appInfo) {
    this.blacklist = appInfo.getBlackList();
  }

  public void recover(io.hops.metadata.yarn.entity.AppSchedulingInfo hopInfo,
      RMStateStore.RMState state) {
    this.pending = hopInfo.isPending();
    this.containerIdCounter.addAndGet(hopInfo.getContaineridcounter());
    try {
      //construct priorities Set and requests Map
      List<ResourceRequest> resourceRequestlist =
          state.getResourceRequests(this.applicationAttemptId.toString());
      if (resourceRequestlist != null && !resourceRequestlist.isEmpty()) {
        for (ResourceRequest hop : resourceRequestlist) {
          //construct Priority
          Priority priority = Priority.newInstance(hop.getPriority());
          //construct ResourceRequest
          ResourceRequestPBImpl resourceRequest = new ResourceRequestPBImpl(
              ResourceRequestProto.parseFrom(hop.getResourcerequeststate()));
          this.priorities.add(priority);
          if (this.requests.get(priority) == null) {
            this.requests.put(priority,
                new HashMap<String, org.apache.hadoop.yarn.api.records.ResourceRequest>());
          }
          this.requests.get(priority)
              .put(resourceRequest.getResourceName(), resourceRequest);
        }
      }

      //construct blackList set
      List<AppSchedulingInfoBlacklist> blackList =
          state.getBlackList(this.applicationAttemptId.toString());
      if (blackList != null && !blackList.isEmpty()) {
        for (AppSchedulingInfoBlacklist hop : blackList) {
          blacklist.add(hop.getBlacklisted());
        }
      }

    } catch (IOException ex) {
      Logger.getLogger(AppSchedulingInfo.class.getName())
          .log(Level.SEVERE, null, ex);
    }
  }
}
