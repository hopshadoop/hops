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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import com.google.common.annotations.VisibleForTesting;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.ha.common.transactionStateWrapper;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.QueueMetrics;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FifoScheduler extends AbstractYarnScheduler
    implements Configurable {

  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);
      //recovered
  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);//recovered
  Configuration conf;//recovered
  protected Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>
      nodes =
      new ConcurrentHashMap<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>();
      //recovered
  private boolean initialized;//recovered
  private org.apache.hadoop.yarn.api.records.Resource minimumAllocation;
      //recovered
  private org.apache.hadoop.yarn.api.records.Resource maximumAllocation;
      //recovered
  private boolean usePortForNodeName;//recovered
  private ActiveUsersManager activeUsersManager;//recovered
  private static final String DEFAULT_QUEUE_NAME = "default";//recovered
  private org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics
      metrics;//recovered
  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();//recovered
  
  private int maxAllocatedContainersPerRequest = -1;
  
  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo(boolean includeChildQueues,
        boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(1.0f);
      if (clusterResource.getMemory() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        queueInfo.setCurrentCapacity(
            (float) usedResource.getMemory() / clusterResource.getMemory());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
          new HashMap<QueueACL, AccessControlList>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
        UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo =
          recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      return getQueueAcls().get(acl).isUserAllowed(user);
    }

    @Override
    public ActiveUsersManager getActiveUsersManager() {
      return activeUsersManager;
    }
  }; // recovered

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem =
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem =
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException(
          "Invalid resource scheduler memory" + " allocation configuration" +
              ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB +
              "=" + minMem + ", " +
              YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + "=" +
              maxMem + ", min and max should be greater than 0" +
              ", max should be no smaller than min.");
    }
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public org.apache.hadoop.yarn.api.records.Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }

  @Override
  public org.apache.hadoop.yarn.api.records.Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  @Override
  public synchronized void reinitialize(Configuration conf, RMContext rmContext,
          TransactionState transactionState)
      throws IOException {
    setConf(conf);
    if (!this.initialized) {
      validateConf(conf);
      this.rmContext = rmContext;
      //Use ConcurrentSkipListMap because applications need to be ordered
      this.applications = new ConcurrentSkipListMap<ApplicationId,
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication>();
      this.minimumAllocation = Resources.createResource(
          conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
      this.maximumAllocation = Resources.createResource(
          conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
      this.usePortForNodeName = conf.getBoolean(
          YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
      this.metrics =
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics
              .forQueue(DEFAULT_QUEUE_NAME, null, false, conf);
      this.activeUsersManager = new ActiveUsersManager(metrics);
      this.initialized = true;
      this.maxAllocatedContainersPerRequest = this.conf.getInt(
              YarnConfiguration.MAX_ALLOCATED_CONTAINERS_PER_REQUEST,
              YarnConfiguration.DEFAULT_MAX_ALLOCATED_CONTAINERS_PER_REQUEST);
    }
  }

  @Override
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      TransactionState transactionState) {
    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.error(
          "Calling allocate on removed " + "or non existant application " +
              applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, resourceCalculator, clusterResource,
        minimumAllocation, maximumAllocation);

    // Release containers
    for (ContainerId releasedContainer : release) {
      RMContainer rmContainer = getRMContainer(releasedContainer);
      if (rmContainer == null) {
        RMAuditLogger
            .logFailure(application.getUser(), AuditConstants.RELEASE_CONTAINER,
                "Unauthorized access or invalid container", "FifoScheduler",
                "Trying to release container not owned by app or with invalid id",
                application.getApplicationId(), releasedContainer);
      }
      containerCompleted(rmContainer, SchedulerUtils
              .createAbnormalContainerStatus(releasedContainer,
                  SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED, transactionState);
    }

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " + "application " +
            applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {
        LOG.debug(
            "allocate: pre-update" + " applicationId=" + applicationAttemptId +
                " application=" + application);
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask, transactionState);

        LOG.debug(
            "allocate: post-update" + " applicationId=" + applicationAttemptId +
                " application=" + application);
        application.showRequests();

        LOG.debug(
            "allocate:" + " applicationId=" + applicationAttemptId + " #ask=" +
                ask.size());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals,
          transactionState);
      ContainersAndNMTokensAllocation allocation =
          application.pullNewlyAllocatedContainersAndNMTokens(transactionState);
      return new Allocation(allocation.getContainerList(),
          application.getHeadroom(), null, null, null,
          allocation.getNMTokenList());
    }
  }

  @VisibleForTesting
  FiCaSchedulerApp getApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        app = applications.get(applicationAttemptId.getApplicationId());
    if (app != null) {
      return (FiCaSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId applicationAttemptId) {
    FiCaSchedulerApp app = getApplicationAttempt(applicationAttemptId);
    return app == null ? null : new SchedulerAppReport(app);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId applicationAttemptId) {
    FiCaSchedulerApp app = getApplicationAttempt(applicationAttemptId);
    return app == null ? null : app.getResourceUsageReport();
  }

  private org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode getNode(
      NodeId nodeId) {
    return nodes.get(nodeId);
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String queue, String user, TransactionState transactionState) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        application =
        new org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication(
            DEFAULT_QUEUE, user);
    applications.put(applicationId, application);
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).
              getSchedulerApplicationInfos(applicationId)
          .setSchedulerApplicationtoAdd(application, applicationId);
    }
    metrics.submitApp(user);
    LOG.info("Accepted application " + applicationId + " from user: " + user +
        ", currently num of applications: " + applications.size());
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED,
            transactionState));
  }

  private synchronized void addApplicationAttempt(
      ApplicationAttemptId appAttemptId,
      boolean transferStateFromPreviousAttempt,
      TransactionState transactionState) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        application = applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    // TODO: Fix store
    FiCaSchedulerApp schedulerApp =
        new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE,
            activeUsersManager, this.rmContext, maxAllocatedContainersPerRequest);
    //Nikos: At this point AppSchedulingInfo is created in SchedulerApplicationAttempt constructor
    if (transferStateFromPreviousAttempt) {
      schedulerApp
          .transferStateFromPreviousAttempt(application.getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(schedulerApp, transactionState);

    metrics.submitAppAttempt(user);
    LOG.info("Added Application Attempt " + appAttemptId +
        " to scheduler from user " + application.getUser());
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.ATTEMPT_ADDED,
            transactionState));
  }

  private synchronized void doneApplication(ApplicationId applicationId,
      RMAppState finalState, TransactionState transactionState) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        application = applications.get(applicationId);
    if (application == null) {
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }

    // Inform the activeUsersManager
    activeUsersManager
        .deactivateApplication(application.getUser(), applicationId);
    application.stop(finalState);
    applications.remove(applicationId);

    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).
              getSchedulerApplicationInfos(applicationId)
          .setApplicationIdtoRemove(applicationId);
    }
  }

  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers,
      TransactionState transactionState) throws IOException {
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        application = applications.get(applicationAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException(
          "Unknown application " + applicationAttemptId + " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers &&
          container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + container.getContainerId());
        continue;
      }
      containerCompleted(container, SchedulerUtils
              .createAbnormalContainerStatus(container.getContainerId(),
                  SchedulerUtils.COMPLETED_APPLICATION),
          RMContainerEventType.KILL, transactionState);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState, transactionState);
  }

  /**
   * Heart of the scheduler...
   *
   * @param node
   *     node on which resources are available to be allocated
   */
  private void assignContainers(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      TransactionState transactionState) {
    LOG.debug(
        "assignContainers:" + " node=" + node.getRMNode().getNodeAddress() +
            " #applications=" + applications.size());

    // Try to assign containers to applications in fifo order
    for (Map.Entry<ApplicationId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication> e : applications
        .entrySet()) {
      FiCaSchedulerApp application =
          (FiCaSchedulerApp) e.getValue().getCurrentAppAttempt();
      if (application == null) {
        continue;
      }
      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }

        for (Priority priority : application.getPriorities()) {
          int maxContainers =
              getMaxAllocatableContainers(application, priority, node,
                  NodeType.OFF_SWITCH);
          // Ensure the application needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers =
                assignContainersOnNode(node, application, priority,
                    transactionState);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        }
      }

      LOG.debug("post-assignContainers");
      application.showRequests();

      // Done
      if (Resources.lessThan(resourceCalculator, clusterResource,
          node.getAvailableResource(), minimumAllocation)) {
        break;
      }
    }

    // Update the applications' headroom to correctly take into
    // account the containers assigned in this update.
    for (org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication application : applications
        .values()) {
      FiCaSchedulerApp attempt =
          (FiCaSchedulerApp) application.getCurrentAppAttempt();
      if (attempt == null) {
        continue;
      }
      attempt.setHeadroom(Resources.subtract(clusterResource, usedResource),
          transactionState);
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState)
            .updateClusterResource(clusterResource);
      }
    }
  }

  private int getMaxAllocatableContainers(FiCaSchedulerApp application,
      Priority priority,
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      NodeType type) {
    int maxContainers = 0;

    ResourceRequest offSwitchRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchRequest != null) {
      maxContainers = offSwitchRequest.getNumContainers();
    }

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = application
          .getResourceRequest(priority, node.getRMNode().getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers =
          Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest = application
          .getResourceRequest(priority, node.getRMNode().getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers =
            Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
    }

    return maxContainers;
  }

  private int assignContainersOnNode(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      TransactionState transactionState) {
    // Data-local
    int nodeLocalContainers =
        assignNodeLocalContainers(node, application, priority,
            transactionState);

    // Rack-local
    int rackLocalContainers =
        assignRackLocalContainers(node, application, priority,
            transactionState);

    // Off-switch
    int offSwitchContainers =
        assignOffSwitchContainers(node, application, priority,
            transactionState);

    LOG.debug("assignContainersOnNode:" + " node=" +
        node.getRMNode().getNodeAddress() + " application=" +
        application.getApplicationId().getId() + " priority=" +
        priority.getPriority() + " #assigned=" +
        (nodeLocalContainers + rackLocalContainers + offSwitchContainers));

    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeLocalContainers(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      TransactionState transactionState) {
    int assignedContainers = 0;
    ResourceRequest request =
        application.getResourceRequest(priority, node.getNodeName());
    if (request != null) {
      // Don't allocate on this node if we don't need containers on this rack
      ResourceRequest rackRequest = application
          .getResourceRequest(priority, node.getRMNode().getRackName());
      if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
        return 0;
      }

      int assignableContainers = Math.min(
          getMaxAllocatableContainers(application, priority, node,
              NodeType.NODE_LOCAL), request.getNumContainers());
      assignedContainers =
          assignContainer(node, application, priority, assignableContainers,
              request, NodeType.NODE_LOCAL, transactionState);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      TransactionState transactionState) {
    int assignedContainers = 0;
    ResourceRequest request = application
        .getResourceRequest(priority, node.getRMNode().getRackName());
    if (request != null) {
      // Don't allocate on this rack if the application doens't need containers
      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, ResourceRequest.ANY);
      if (offSwitchRequest.getNumContainers() <= 0) {
        return 0;
      }

      int assignableContainers = Math.min(
          getMaxAllocatableContainers(application, priority, node,
              NodeType.RACK_LOCAL), request.getNumContainers());
      assignedContainers =
          assignContainer(node, application, priority, assignableContainers,
              request, NodeType.RACK_LOCAL, transactionState);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      TransactionState transactionState) {
    int assignedContainers = 0;
    ResourceRequest request =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (request != null) {
      assignedContainers = assignContainer(node, application, priority,
          request.getNumContainers(), request, NodeType.OFF_SWITCH,
          transactionState);
    }
    return assignedContainers;
  }

  private int assignContainer(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority, int assignableContainers,
      ResourceRequest request, NodeType type,
      TransactionState transactionState) {
    LOG.debug(
        "assignContainers:" + " node=" + node.getRMNode().getNodeAddress() +
            " application=" + application.getApplicationId().getId() +
            " priority=" + priority.getPriority() + " assignableContainers=" +
            assignableContainers + " request=" + request + " type=" + type);
    org.apache.hadoop.yarn.api.records.Resource capability =
        request.getCapability();

    int availableContainers = node.getAvailableResource().getMemory() /
        capability.getMemory(); // TODO: A buggy
    // application
    // with this
    // zero would
    // crash the
    // scheduler.
    int assignedContainers =
        Math.min(assignableContainers, availableContainers);

    if (assignedContainers > 0) {
      for (int i = 0; i < assignedContainers; ++i) {

        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils
            .newContainerId(application.getApplicationAttemptId(),
                application.getNewContainerId(transactionState));

        // Create the container
        Container container = BuilderUtils.newContainer(containerId, nodeId,
            node.getRMNode().getHttpAddress(), capability, priority, null);

        // Allocate!
        // Inform the application
        RMContainer rmContainer = application
            .allocate(type, node, priority, request, container,
                transactionState);

        // Inform the node
        node.allocateContainer(application.getApplicationId(), rmContainer,
            transactionState);

        // Update usage for this container
        Resources.addTo(usedResource, capability);
        if (transactionState != null) {
          ((TransactionStateImpl) transactionState)
              .updateUsedResource(clusterResource);
        }
      }

    }

    return assignedContainers;
  }

  private synchronized void nodeUpdate(RMNode rmNode,
      TransactionState transactionState) {
    LOG.debug("HOP :: nodeUpdate, rmNode:" + rmNode.getNodeID().toString());
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
        node = getNode(rmNode.getNodeID());

    // Update resource if any change
    SchedulerUtils.updateResourceIfChanged(node, rmNode, clusterResource, LOG,
            transactionState);
    
    List<UpdatedContainerInfo> containerInfoList =
        rmNode.pullContainerUpdates(transactionState);
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers =
        new ArrayList<ContainerStatus>();
    for (UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers
          .addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node,
          transactionState);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      containerCompleted(getRMContainer(containerId), completedContainer,
          RMContainerEventType.FINISHED, transactionState);
    }

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
        node.getAvailableResource(), minimumAllocation)) {
      LOG.debug(
          "Node heartbeat " + rmNode.getNodeID() + " available resource = " +
              node.getAvailableResource());

      assignContainers(node, transactionState);

      LOG.debug("Node after allocation " + rmNode.getNodeID() + " resource = " +
          node.getAvailableResource());
    }
    LOG.debug("HOP :: metrics.setAvailableResourcesToQueue, clusterResource:" +
        clusterResource +
        ", usedResource:" +
        usedResource);
    metrics.setAvailableResourcesToQueue(
        Resources.subtract(clusterResource, usedResource));
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .updateClusterResource(clusterResource);
    }
  }

  @Override
  public void handle(SchedulerEvent event) {
    LOG.debug("HOP :: FifoScheduler received event of type:" + event.getType());
    switch (event.getType()) {
      case NODE_ADDED: {
        NodeAddedSchedulerEvent nodeAddedEvent =
            (NodeAddedSchedulerEvent) event;
        org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.
                fica.FiCaSchedulerNode
            ficaNode =
            new org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.
                    fica.FiCaSchedulerNode(
                nodeAddedEvent.getAddedRMNode(), usePortForNodeName, rmContext);
        addNode(nodeAddedEvent.getAddedRMNode(), ficaNode,
            event.getTransactionState());
      }
      break;
      case NODE_REMOVED: {
        NodeRemovedSchedulerEvent nodeRemovedEvent =
            (NodeRemovedSchedulerEvent) event;
        removeNode(nodeRemovedEvent.getRemovedRMNode(),
            event.getTransactionState());
      }
      break;
      case NODE_UPDATE: {
        NodeUpdateSchedulerEvent nodeUpdatedEvent =
            (NodeUpdateSchedulerEvent) event;
        nodeUpdate(nodeUpdatedEvent.getRMNode(), event.getTransactionState());
      }
      break;
      case APP_ADDED: {
        AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
        addApplication(appAddedEvent.getApplicationId(),
            appAddedEvent.getQueue(), appAddedEvent.getUser(),
            event.getTransactionState());
      }
      break;
      case APP_REMOVED: {
        AppRemovedSchedulerEvent appRemovedEvent =
            (AppRemovedSchedulerEvent) event;
        doneApplication(appRemovedEvent.getApplicationID(),
            appRemovedEvent.getFinalState(), event.getTransactionState());
      }
      break;
      case APP_ATTEMPT_ADDED: {
        AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
            (AppAttemptAddedSchedulerEvent) event;
        addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
            appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
            event.getTransactionState());
      }
      break;
      case APP_ATTEMPT_REMOVED: {
        AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
            (AppAttemptRemovedSchedulerEvent) event;
        try {
          doneApplicationAttempt(
              appAttemptRemovedEvent.getApplicationAttemptID(),
              appAttemptRemovedEvent.getFinalAttemptState(),
              appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts(),
              event.getTransactionState());
        } catch (IOException ie) {
          LOG.error("Unable to remove application " +
              appAttemptRemovedEvent.getApplicationAttemptID(), ie);
        }
      }
      break;
      case CONTAINER_EXPIRED: {
        ContainerExpiredSchedulerEvent containerExpiredEvent =
            (ContainerExpiredSchedulerEvent) event;
        ContainerId containerid = containerExpiredEvent.getContainerId();
        containerCompleted(getRMContainer(containerid), SchedulerUtils
                .createAbnormalContainerStatus(containerid,
                    SchedulerUtils.EXPIRED_CONTAINER),
            RMContainerEventType.EXPIRE, event.getTransactionState());
      }
      break;
      default:
        LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private void containerLaunchedOnNode(ContainerId containerId,
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      TransactionState transactionState) {
    // Get the application for the finished container
    FiCaSchedulerApp application = getCurrentAttemptForContainer(containerId);
    if (application == null) {
      LOG.info("Unknown application " +
          containerId.getApplicationAttemptId().getApplicationId() +
          " launched container " + containerId + " on node: " + node);
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeCleanContainerEvent(node.getNodeID(), containerId,
              transactionState));

      return;
    }

    application.containerLaunchedOnNode(containerId, node.getNodeID(),
        transactionState);
  }

  @Lock(FifoScheduler.class)
  private synchronized void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      TransactionState transactionState) {
    if (rmContainer == null) {
      LOG.info("Null container completed..." + containerStatus.getContainerId());
      return;
    }

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();

    // Get the node on which the container was allocated
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
        node = getNode(container.getNodeId());

    if (application == null) {
      LOG.info("Unknown application: " + appId + " released container " +
          container.getId() + " on node: " + node + " with event: " + event);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, event,
        transactionState);

    // Inform the node
    node.releaseContainer(container, transactionState);

    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());
    LOG.info("Application attempt " + application.getApplicationAttemptId() +
        " released container " + container.getId() + " on node: " + node +
        " with event: " + event);

  }

  private org.apache.hadoop.yarn.api.records.Resource clusterResource =
      recordFactory
          .newRecordInstance(org.apache.hadoop.yarn.api.records.Resource.class);
      //recovered
  private org.apache.hadoop.yarn.api.records.Resource usedResource =
      recordFactory
          .newRecordInstance(org.apache.hadoop.yarn.api.records.Resource.class);
      //recovered

  private synchronized void removeNode(RMNode nodeInfo,
      TransactionState transactionState) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
        node = getNode(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }

    // Kill running containers
    for (RMContainer container : node.getRunningContainers()) {
      containerCompleted(container, SchedulerUtils
              .createAbnormalContainerStatus(container.getContainerId(),
                  SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL,
          transactionState);
    }

    //Remove the node
    this.nodes.remove(nodeInfo.getNodeID());
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .addFicaSchedulerNodeInfoToRemove(nodeInfo.getNodeID().toString(),
              node);
    }

    // Update cluster metrics
    Resources
        .subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .updateClusterResource(clusterResource);
    }
  }

  @Override
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null);
  }

  private synchronized void addNode(RMNode nodeManager,
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node,
      TransactionState ts) {
    this.nodes.put(nodeManager.getNodeID(), node);
    LOG.debug("HOP :: FifoScheduler - node was added");
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
    if (ts != null) {
      ((TransactionStateImpl) ts)
          .addFicaSchedulerNodeInfoToAdd(nodeManager.getNodeID().toString(),
              node);
      ((TransactionStateImpl) ts).updateClusterResource(clusterResource);
    }
  }

  @Override
  public void recover(RMState state) {
    try {
      // recover queuemetrics
      List<QueueMetrics> recoverQueueMetrics = state.getAllQueueMetrics();
      if (recoverQueueMetrics != null) {
        if (recoverQueueMetrics.size() > 1) {
          throw new UnsupportedOperationException(
              "there should be only one queueMetrics in the fifoScheduler");
        }
        if (!recoverQueueMetrics.isEmpty()) {
          metrics =
              org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics
                  .forQueue(recoverQueueMetrics.get(0).getQueuename(), null,
                      false, conf);
          metrics.recover(recoverQueueMetrics.get(0));
        }
      }
      //recover applications map
      Map<String, SchedulerApplication> appsList =
          state.getSchedulerApplications();
      for (SchedulerApplication fsapp : appsList.values()) {
        //construct appliactionId - key of applications map
        ApplicationId appId = ConverterUtils.toApplicationId(fsapp.getAppid());

        //retrieve HopSchedulerApplication
        SchedulerApplication hopSchedulerApplication =
            state.getSchedulerApplication(fsapp.getAppid());
        //construct SchedulerAppliaction
        org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
            app =
            new org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication(
                DEFAULT_QUEUE, hopSchedulerApplication.getUser());

        //retrieve HopApplicationAttemptId for this specific appId
        AppSchedulingInfo hopFiCaSchedulerApp =
            state.getAppSchedulingInfo(fsapp.getAppid());
        if (hopFiCaSchedulerApp != null) {
          //construct ApplicationAttemptId
          ApplicationAttemptId appAttemptId = ConverterUtils
              .toApplicationAttemptId(hopFiCaSchedulerApp.getSchedulerAppId());


          FiCaSchedulerApp appAttempt =
            new FiCaSchedulerApp(appAttemptId,
                  hopFiCaSchedulerApp.getUser(),
                  DEFAULT_QUEUE, activeUsersManager, this.rmContext,
                  maxAllocatedContainersPerRequest);
          appAttempt.recover(state);
          app.setCurrentAppAttempt(appAttempt, null);
        }
        applications.put(appId, app);

      }
      //recover nodes map
      Collection<FiCaSchedulerNode> nodesList = 
              state.getAllFiCaSchedulerNodes().values();
      if (nodesList != null && !nodesList.isEmpty()) {
        for (FiCaSchedulerNode fsnode : nodesList) {

          //retrieve nodeId - key of nodes map
          NodeId nodeId = ConverterUtils.toNodeId(fsnode.getRmnodeId());
          //retrieve HopFiCaSchedulerNode

          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
              ficaNode =
              new org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode(
                  this.rmContext.
                      getActiveRMNodes().get(nodeId), usePortForNodeName, 
                      rmContext);
          ficaNode.recover(state);

          nodes.put(nodeId, ficaNode);
        }
      }
      //TORECOVER recover from nodes
      Resource recovered =
          state.getResource("cluster", Resource.CLUSTER, Resource.AVAILABLE);
      if (recovered != null) {
        clusterResource.setMemory(recovered.getMemory());
        clusterResource.setVirtualCores(recovered.getVirtualCores());
      }
      recovered = state.getResource("cluster", Resource.CLUSTER, Resource.USED);
      if (recovered != null) {
        usedResource.setMemory(recovered.getMemory());
        usedResource.setVirtualCores(recovered.getVirtualCores());
      }
    } catch (Exception ex) {
      LOG.error(ex, ex);
    }
  }

  //For testing
  @VisibleForTesting
  public Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode> getNodes() {
    return nodes;
  }

  @Override
  public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
        node = getNode(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  private FiCaSchedulerApp getCurrentAttemptForContainer(
      ContainerId containerId) {
    org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication
        app = applications
        .get(containerId.getApplicationAttemptId().getApplicationId());
    if (app != null) {
      return (FiCaSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }

  @Override
  public org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
  }

  @Override
  public synchronized List<ApplicationAttemptId> getAppsInQueue(
      String queueName) {
    if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
      List<ApplicationAttemptId> attempts =
          new ArrayList<ApplicationAttemptId>(applications.size());
      for (org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication app : applications
          .values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }
}
