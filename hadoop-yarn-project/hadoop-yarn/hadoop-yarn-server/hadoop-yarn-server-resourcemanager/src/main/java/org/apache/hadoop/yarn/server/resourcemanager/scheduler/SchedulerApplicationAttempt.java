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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.hops.ha.common.FiCaSchedulerAppInfo;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppContainer;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.entity.SchedulerAppReservations;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerApp;

/**
 * Represents an application attempt from the viewpoint of the scheduler. Each
 * running app attempt in the RM corresponds to one instance of this class.
 */
@Private
@Unstable
public class SchedulerApplicationAttempt implements Recoverable{

  private static final Log LOG =
      LogFactory.getLog(SchedulerApplicationAttempt.class);

  protected final org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo
      appSchedulingInfo;//recovered
  protected Map<ContainerId, RMContainer> liveContainers =
      new HashMap<ContainerId, RMContainer>();//recovered
  protected final Map<Priority, Map<NodeId, RMContainer>> reservedContainers =
      new HashMap<Priority, Map<NodeId, RMContainer>>();
      //recovered
  private final Multiset<Priority> reReservations = HashMultiset.create();
      //recovered
  protected final org.apache.hadoop.yarn.api.records.Resource
      currentReservation =
      org.apache.hadoop.yarn.api.records.Resource.newInstance(0, 0);//recovered
  private org.apache.hadoop.yarn.api.records.Resource resourceLimit =
      org.apache.hadoop.yarn.api.records.Resource.newInstance(0, 0);//recovered
  protected org.apache.hadoop.yarn.api.records.Resource currentConsumption =
      org.apache.hadoop.yarn.api.records.Resource.newInstance(0, 0);//recovered

  private org.apache.hadoop.yarn.api.records.Resource amResource;
  private boolean unmanagedAM = true;

  protected List<RMContainer> newlyAllocatedContainers =
      new ArrayList<RMContainer>();//recovered
  /**
   * Count how many times the application has been given an opportunity to
   * schedule a task at each priority. Each time the scheduler asks the
   * application for a task at this priority, it is incremented, and each time
   * the application successfully schedules a task, it is reset to 0.
   */
  Multiset<Priority> schedulingOpportunities = HashMultiset.create();// recovered in fs
  // Time of the last container scheduled at the current allowed level
  protected Map<Priority, Long> lastScheduledContainer
          = new HashMap<Priority, Long>();//recovered
  protected Queue queue;//recovered
  protected boolean isStopped = false;//recovered
  protected final RMContext rmContext;//recovered
  protected final int maxAllocatedContainersPerRequest;
  
  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, int maxAllocatedContainersPerRequest) {
    this.rmContext = rmContext;
    this.appSchedulingInfo =
        new org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo(
            applicationAttemptId, user, queue, activeUsersManager);
    this.queue = queue;
    this.maxAllocatedContainersPerRequest = maxAllocatedContainersPerRequest;

    if (rmContext != null && rmContext.getRMApps() != null &&
            rmContext.getRMApps()
            .containsKey(applicationAttemptId.getApplicationId())) {
      ApplicationSubmissionContext appSubmissionContext =
              rmContext.getRMApps().get(applicationAttemptId.getApplicationId())
              .getApplicationSubmissionContext();
      if (appSubmissionContext != null) {
        amResource = appSubmissionContext.getResource();
        unmanagedAM = appSubmissionContext.getUnmanagedAM();
      }
    }
  }

  /**
   * Get the live containers of the application.
   *
   * @return live containers of the application
   */
  public synchronized Collection<RMContainer> getLiveContainers() {
    return new ArrayList<RMContainer>(liveContainers.values());
  }

  /**
   * Is this application pending?
   *
   * @return true if it is else false.
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }

  /**
   * Get {@link ApplicationAttemptId} of the application master.
   *
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }

  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }

  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return appSchedulingInfo.getResourceRequests(priority);
  }

  public int getNewContainerId(TransactionState ts) {
    if (ts != null) {
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
              this.appSchedulingInfo.applicationId).
              getFiCaSchedulerAppInfo(this.getApplicationAttemptId()).
              updateAppInfo(this);
    }
    return appSchedulingInfo.getNewContainerId();
  }

  public int getLastContainerId() {
    return appSchedulingInfo.getLastContainerId();
  }

  public Collection<Priority> getPriorities() {
    return appSchedulingInfo.getPriorities();
  }

  public synchronized ResourceRequest getResourceRequest(Priority priority,
      String resourceName) {
    return this.appSchedulingInfo.getResourceRequest(priority, resourceName);
  }

  public synchronized int getTotalRequiredResources(Priority priority) {
    return getResourceRequest(priority, ResourceRequest.ANY).getNumContainers();
  }

  public synchronized org.apache.hadoop.yarn.api.records.Resource getResource(
      Priority priority) {
    return appSchedulingInfo.getResource(priority);
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }

  public org.apache.hadoop.yarn.api.records.Resource getAMResource() {
    return amResource;
  }

  public boolean getUnmanagedAM() {
    return unmanagedAM;
  }

  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  protected synchronized void resetReReservations(Priority priority,
          TransactionState ts) {
    reReservations.setCount(priority, 0);
   ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
           this.appSchedulingInfo.applicationId).getFiCaSchedulerAppInfo(getApplicationAttemptId()).resetReReservations(priority);
  }

  protected synchronized void addReReservation(Priority priority, 
          TransactionState ts) {
    reReservations.add(priority);
    ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
            this.appSchedulingInfo.applicationId).getFiCaSchedulerAppInfo(
                    getApplicationAttemptId()).addReReservation(priority);
  }

  public synchronized int getReReservations(Priority priority) {
    return reReservations.count(priority);
  }

  /**
   * Get total current reservations. Used only by unit tests
   *
   * @return total current reservations
   */
  @Stable
  @Private
  public synchronized org.apache.hadoop.yarn.api.records.Resource getCurrentReservation() {
    return currentReservation;
  }

  public Queue getQueue() {
    return queue;
  }

  public synchronized void updateResourceRequests(
      List<ResourceRequest> requests, TransactionState ts) {
    if (!isStopped) {
      appSchedulingInfo.updateResourceRequests(requests, false, ts);
    }
  }

  public synchronized void recoverResourceRequests(
      List<ResourceRequest> requests, TransactionState ts) {
    if (!isStopped) {
      appSchedulingInfo.updateResourceRequests(requests, true, ts);
    }
  }

  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState,
      TransactionState ts) {
    // Cleanup all scheduling information
    isStopped = true;
    if (ts != null) {
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
              this.appSchedulingInfo.applicationId).
              getFiCaSchedulerAppInfo(this.getApplicationAttemptId()).
              updateAppInfo(this);
    }
    appSchedulingInfo.stop(rmAppAttemptFinalState, ts);
  }

  public synchronized boolean isStopped() {
    return isStopped;
  }


  public synchronized org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo getAppSchedulingInfo() {
    return appSchedulingInfo;
  }


  /**
   * Get the list of reserved containers
   *
   * @return All of the reserved containers.
   */
  public synchronized List<RMContainer> getReservedContainers() {
    List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
    for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : this.reservedContainers
        .entrySet()) {
      reservedContainers.addAll(e.getValue().values());
    }
    return reservedContainers;
  }

  public synchronized RMContainer reserve(SchedulerNode node, Priority priority,
      RMContainer rmContainer, Container container,
      TransactionState transactionState) {
    // Create RMContainer if necessary
    if (rmContainer == null) {
      rmContainer = new RMContainerImpl(container, getApplicationAttemptId(),
          node.getNodeID(), appSchedulingInfo.getUser(), rmContext,
          transactionState);

      Resources.addTo(currentReservation, container.getResource());
      //HOP : Update Resources
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
                this.appSchedulingInfo.applicationId)
            .getFiCaSchedulerAppInfo(
                this.appSchedulingInfo.getApplicationAttemptId())
            .toUpdateResource(Resource.CURRENTRESERVATION, currentReservation);
      }
      // Reset the re-reservation count
      resetReReservations(priority, transactionState);
    } else {
      ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
              this.appSchedulingInfo.applicationId).setFiCaSchedulerAppInfo(this);
      // Note down the re-reservation
      addReReservation(priority, transactionState);
    }
    rmContainer.handle(
        new RMContainerReservedEvent(container.getId(), container.getResource(),
            node.getNodeID(), priority, transactionState));

    Map<NodeId, RMContainer> reservedContainers =
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);
    //HOP : Update ReservedContainers
    LOG.debug("SchedulerApplicationAttempt: Persist reservedContainer "
            + getApplicationAttemptId().toString() + " containerid "
            + rmContainer.getContainerId().toString());
    ((TransactionStateImpl) transactionState).getSchedulerApplicationInfos(
            this.appSchedulingInfo.applicationId).
            getFiCaSchedulerAppInfo(getApplicationAttemptId()).
            addReservedContainer(rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Application attempt " + getApplicationAttemptId()
              + " reserved container " + rmContainer + " on node " + node
              + ". This attempt currently has " + reservedContainers.size()
              + " reserved containers at priority " + priority
              + "; currentReservation " + currentReservation.getMemory());
    }

    return rmContainer;
  }

  /**
   * Has the application reserved the given <code>node</code> at the given
   * <code>priority</code>?
   *
   * @param node
   *     node to be checked
   * @param priority
   *     priority of reserved container
   * @return true is reserved, false if not
   */
  public synchronized boolean isReserved(SchedulerNode node,
      Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
        this.reservedContainers.get(priority);
    if (reservedContainers != null) {
      return reservedContainers.containsKey(node.getNodeID());
    }
    return false;
  }

  public synchronized void setHeadroom(
      org.apache.hadoop.yarn.api.records.Resource globalLimit,
      TransactionState transactionState) {
    this.resourceLimit = globalLimit;
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   *
   * @return available resource headroom
   */
  public synchronized org.apache.hadoop.yarn.api.records.Resource getHeadroom() {
    // Corner case to deal with applications being slightly over-limit
    if (resourceLimit.getMemory() < 0) {
      resourceLimit.setMemory(0);
    }

    return resourceLimit;
  }

  public synchronized int getNumReservedContainers(Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
        this.reservedContainers.get(priority);
    return (reservedContainers == null) ? 0 : reservedContainers.size();
  }

  @SuppressWarnings("unchecked")
  public synchronized void containerLaunchedOnNode(ContainerId containerId,
      NodeId nodeId, TransactionState transactionState) {
    // Inform the container
    RMContainer rmContainer = getRMContainer(containerId);
    if (rmContainer == null) {
      // Some unknown container sneaked into the system. Kill it.
      rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(nodeId, containerId, transactionState));
      return;
    }

    rmContainer.handle(
        new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED,
            transactionState));
  }

  public synchronized void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId() +
              " headRoom=" + getHeadroom() + " currentConsumption=" +
              currentConsumption.getMemory());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId() +
                " request=" + request);
          }
        }
      }
    }
  }

  public org.apache.hadoop.yarn.api.records.Resource getCurrentConsumption() {
    return currentConsumption;
  }

  public void recover(RMStateStore.RMState state) throws IOException{
    io.hops.metadata.yarn.entity.AppSchedulingInfo hopInfo =
            state.getAppSchedulingInfo(
                    appSchedulingInfo.applicationId.toString());
    this.appSchedulingInfo.recover(hopInfo, state);
    ApplicationAttemptId applicationAttemptId =
        this.appSchedulingInfo.getApplicationAttemptId();
    this.isStopped = hopInfo.isStoped();
    try {
      Resource recoveringCurrentReservation = state
          .getResource(applicationAttemptId.toString(),
              Resource.CURRENTRESERVATION,
              Resource.SCHEDULERAPPLICATIONATTEMPT);
      if (recoveringCurrentReservation != null) {
        currentReservation.setMemory(recoveringCurrentReservation.getMemory());
        currentReservation
            .setVirtualCores(recoveringCurrentReservation.getVirtualCores());
      }
      Resource recoveringResourceLimit = state
          .getResource(applicationAttemptId.toString(), Resource.RESOURCELIMIT,
              Resource.SCHEDULERAPPLICATIONATTEMPT);
      if (recoveringResourceLimit != null) {
        resourceLimit.setMemory(recoveringResourceLimit.getMemory());
        resourceLimit
            .setVirtualCores(recoveringResourceLimit.getVirtualCores());
      }
      recoverNewlyAllocatedContainers(applicationAttemptId, state);
      recoverLiveContainers(applicationAttemptId, state);
      recoverReservations(applicationAttemptId, state);
      recoverReservedContainers(applicationAttemptId, state);
      recoverSchedulingOpportunities(applicationAttemptId, state);
      recoverLastScheduledContainer(applicationAttemptId, state);
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName())
          .log(Level.SEVERE, null, ex);
    }
  }

  private void recoverNewlyAllocatedContainers(
      ApplicationAttemptId applicationAttemptId, RMStateStore.RMState state) {
    try {
      List<String> list =
          state.getNewlyAllocatedContainers(applicationAttemptId.toString());
      if (list != null && !list.isEmpty()) {
        for (String rmContainerId : list) {
          newlyAllocatedContainers
              .add(state.getRMContainer(rmContainerId, rmContext));
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName())
          .log(Level.SEVERE, null, ex);
    }
  }

  //Nikos: maybe the recovery of liveContainers should take place in the above method.
  private void recoverLiveContainers(ApplicationAttemptId applicationAttemptId,
      RMStateStore.RMState state) {
    try {
      List<String> list =
          state.getLiveContainers(applicationAttemptId.toString());
      if (list != null) {
        for (String rmContainerId : list) {
          RMContainer rMContainer =
              state.getRMContainer(rmContainerId, rmContext);
          liveContainers.put(rMContainer.getContainerId(), rMContainer);
          Resources.addTo(currentConsumption, rMContainer.getContainer().getResource());
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName())
          .log(Level.SEVERE, null, ex);
    }
  }

  private void recoverReservations(ApplicationAttemptId applicationAttemptId,
          RMStateStore.RMState state) throws IOException {
    List<SchedulerAppReservations> list = state.getRereservations(
            applicationAttemptId.toString());
    if (list != null && !list.isEmpty()) {
      for (SchedulerAppReservations hop : list) {
        //construct Priority
        Priority priority = Priority.newInstance(hop.getPriority_id());
        this.reReservations.setCount(priority, hop.getCounter());
      }
    }
  }

  private void recoverReservedContainers(
          ApplicationAttemptId applicationAttemptId, RMStateStore.RMState state) {
    try {
      List<FiCaSchedulerAppReservedContainers> list = state.
              getReservedContainers(applicationAttemptId.toString());
      if (list != null && list.isEmpty()) {
        for (FiCaSchedulerAppReservedContainers hop : list) {
          //construct RMContainer
          RMContainer rmContainer = state.
                  getRMContainer(hop.getRmcontainer_id(), rmContext);
          //construct Priority
          Priority priority = Priority.newInstance(hop.getPriority_id());
          //construct NodeId
          NodeId nodeId = ConverterUtils.toNodeId(hop.getNodeid());

          Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.
                  get(priority);
          if (reservedContainers == null) {
            reservedContainers = new HashMap<NodeId, RMContainer>();
            this.reservedContainers.put(priority, reservedContainers);
          }
          reservedContainers.put(nodeId, rmContainer);
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

  private void recoverSchedulingOpportunities(
          ApplicationAttemptId applicationAttemptId, RMStateStore.RMState state) {
    try {
      List<FiCaSchedulerAppSchedulingOpportunities> list = state.
              getSchedulingOpportunities(applicationAttemptId.toString());
      if (list != null && !list.isEmpty()) {
        for (FiCaSchedulerAppSchedulingOpportunities hop : list) {
          Priority priority = Priority.newInstance(hop.getPriority_id());
          schedulingOpportunities.setCount(priority, hop.getCounter());
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

  private void recoverLastScheduledContainer(
          ApplicationAttemptId applicationAttemptId, RMStateStore.RMState state) {
    try {
      List<FiCaSchedulerAppLastScheduledContainer> list = state.
              getLastScheduledContainers(applicationAttemptId.toString());
      if (list != null && !list.isEmpty()) {
        for (FiCaSchedulerAppLastScheduledContainer hop : list) {
          Priority priority = Priority.newInstance(hop.getPriority_id());
          lastScheduledContainer.put(priority, hop.getTime());
        }
      }
    } catch (IOException ex) {
      Logger.getLogger(SchedulerApplicationAttempt.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

  public static class ContainersAndNMTokensAllocation {

    List<Container> containerList;
    List<NMToken> nmTokenList;

    public ContainersAndNMTokensAllocation(List<Container> containerList,
        List<NMToken> nmTokenList) {
      this.containerList = containerList;
      this.nmTokenList = nmTokenList;
    }

    public List<Container> getContainerList() {
      return containerList;
    }

    public List<NMToken> getNMTokenList() {
      return nmTokenList;
    }
  }

  // Create container token and NMToken altogether, if either of them fails for
  // some reason like DNS unavailable, do not return this container and keep it
  // in the newlyAllocatedContainers waiting to be refetched.
  public synchronized ContainersAndNMTokensAllocation pullNewlyAllocatedContainersAndNMTokens(
      TransactionState transactionState) {
    List<Container> returnContainerList =
        new ArrayList<Container>(newlyAllocatedContainers.size());
    List<NMToken> nmTokens = new ArrayList<NMToken>();
    int count = 0;
    for (Iterator<RMContainer> i = newlyAllocatedContainers.iterator();
         i.hasNext(); ) {
      RMContainer rmContainer = i.next();
      Container container = rmContainer.getContainer();
      try {
        // create container token and NMToken altogether.
        container.setContainerToken(rmContext.getContainerTokenSecretManager()
            .createContainerToken(container.getId(), container.getNodeId(),
                getUser(), container.getResource()));
        NMToken nmToken = rmContext.getNMTokenSecretManager()
            .createAndGetNMToken(getUser(), getApplicationAttemptId(),
                container);
        if (nmToken != null) {
          nmTokens.add(nmToken);
        }
      } catch (IllegalArgumentException e) {
        // DNS might be down, skip returning this container.
        LOG.error("Error trying to assign container token and NM token to" +
            " an allocated container " + container.getId(), e);
        continue;
      }
      returnContainerList.add(container);
      i.remove();
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
          RMContainerEventType.ACQUIRED, transactionState));
      count++;
      if (maxAllocatedContainersPerRequest > 0 && count
              > maxAllocatedContainersPerRequest) {
        LOG.info("Blocking the allocation of more than "
                + maxAllocatedContainersPerRequest + " containers");
        break;
      }
    }
    return new ContainersAndNMTokensAllocation(returnContainerList, nmTokens);
  }

  public synchronized void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals, TransactionState ts) {
    if (!isStopped) {
      this.appSchedulingInfo
          .updateBlacklist(blacklistAdditions, blacklistRemovals, ts);
    }
  }

  public boolean isBlacklisted(String resourceName) {
    return this.appSchedulingInfo.isBlacklisted(resourceName);
  }

  public synchronized void addSchedulingOpportunity(Priority priority,
          TransactionState ts) {
    schedulingOpportunities.setCount(priority,
            schedulingOpportunities.count(priority) + 1);

    //HOP : Update SchedulingOpportunities
    ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
            this.appSchedulingInfo.applicationId).
            getFiCaSchedulerAppInfo(getApplicationAttemptId()).
            addSchedulingOppurtunity(priority, schedulingOpportunities.count(
                            priority) - 1);

  }

  public synchronized void subtractSchedulingOpportunity(Priority priority,
          TransactionState ts) {
    int count = schedulingOpportunities.count(priority) - 1;
    this.schedulingOpportunities.setCount(priority, Math.max(count, 0));

    ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
            this.appSchedulingInfo.applicationId).
            getFiCaSchedulerAppInfo(getApplicationAttemptId()).
            addSchedulingOppurtunity(priority, Math.max(count, 0));

  }

  /**
   * Return the number of times the application has been given an opportunity
   * to
   * schedule a task at the given priority since the last time it successfully
   * did so.
   */
  public synchronized int getSchedulingOpportunities(Priority priority) {
    return schedulingOpportunities.count(priority);
  }

  /**
   * Should be called when an application has successfully scheduled a
   * container, or when the scheduling locality threshold is relaxed. Reset
   * various internal counters which affect delay scheduling
   *
   * @param priority
   *     The priority of the container scheduled.
   */
  public synchronized void resetSchedulingOpportunities(Priority priority,
          TransactionState ts) {
    resetSchedulingOpportunities(priority, System.currentTimeMillis(), ts);
  }
  // used for continuous scheduling

  public synchronized void resetSchedulingOpportunities(Priority priority,
    long currentTimeMs, TransactionState ts) {
    lastScheduledContainer.put(priority, currentTimeMs);
    schedulingOpportunities.setCount(priority, 0);

    //HOP : Update lastScheduledContainers, schedulingOpportunities
    FiCaSchedulerAppInfo fica = ((TransactionStateImpl) ts).
            getSchedulerApplicationInfos(this.appSchedulingInfo.applicationId).
            getFiCaSchedulerAppInfo(
                    getApplicationAttemptId());
    fica.addLastScheduledContainer(priority, currentTimeMs);
    fica.addSchedulingOppurtunity(priority, 0);
  }

  public synchronized ApplicationResourceUsageReport getResourceUsageReport() {
    return ApplicationResourceUsageReport
        .newInstance(liveContainers.size(), reservedContainers.size(),
            Resources.clone(currentConsumption),
            Resources.clone(currentReservation),
            Resources.add(currentConsumption, currentReservation));
  }

  public synchronized Map<ContainerId, RMContainer> getLiveContainersMap() {
    return this.liveContainers;
  }

  public synchronized org.apache.hadoop.yarn.api.records.Resource getResourceLimit() {
    return this.resourceLimit;
  }

  //For testing
  public synchronized List<RMContainer> getNewlyAllocatedContainers() {
    return this.newlyAllocatedContainers;
  }

  public synchronized Map<Priority, Long> getLastScheduledContainer() {
    return this.lastScheduledContainer;
  }

  public synchronized void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    LOG.debug("transfering state for appattempt " +
        this.appSchedulingInfo.getApplicationAttemptId());
    this.liveContainers = appAttempt.getLiveContainersMap();
    this.currentConsumption = appAttempt.getCurrentConsumption();
    this.resourceLimit = appAttempt.getResourceLimit();
    this.lastScheduledContainer = appAttempt.getLastScheduledContainer();
    this.appSchedulingInfo.transferStateFromPreviousAppSchedulingInfo(
        appAttempt.appSchedulingInfo);
    //TORECOVER??
  }

  //TORECOVER FAIR
  public synchronized void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    String user = getUser();
    for (RMContainer liveContainer : liveContainers.values()) {
      org.apache.hadoop.yarn.api.records.Resource resource =
          liveContainer.getContainer().getResource();
      oldMetrics.releaseResources(user, 1, resource);
      newMetrics.allocateResources(user, 1, resource, false);
    }
    for (Map<NodeId, RMContainer> map : reservedContainers.values()) {
      for (RMContainer reservedContainer : map.values()) {
        org.apache.hadoop.yarn.api.records.Resource resource =
            reservedContainer.getReservedResource();
        oldMetrics.unreserveResource(user, resource);
        newMetrics.reserveResource(user, resource);
      }
    }

    appSchedulingInfo.move(newQueue);
    this.queue = newQueue;
  }
}
