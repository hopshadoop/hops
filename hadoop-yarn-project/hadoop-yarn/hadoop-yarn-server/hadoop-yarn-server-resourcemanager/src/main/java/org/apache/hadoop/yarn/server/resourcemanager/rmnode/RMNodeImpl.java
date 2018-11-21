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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.JWTSecurityHandler;
import org.apache.hadoop.yarn.server.resourcemanager.security.X509SecurityHandler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public abstract class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  protected final WriteLock writeLock;

  protected final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
  protected volatile boolean nextHeartBeat = true;

  protected final NodeId nodeId;
  protected final RMContext context;
  protected final String hostName;
  protected final int commandPort;
  protected int httpPort;
  private final String nodeAddress; // The containerManager address
  protected String httpAddress;
  /* Snapshot of total resources before receiving decommissioning command */
  private volatile Resource originalTotalCapability;
  protected volatile Resource totalCapability;
  private final Node node;

  private String healthReport;
  private long lastHealthReportTime;
  protected String nodeManagerVersion;

  private long timeStamp;
  /* Aggregated resource utilization for the containers. */
  private ResourceUtilization containersUtilization;
  /* Resource utilization for the node. */
  private ResourceUtilization nodeUtilization;

  protected final ContainerAllocationExpirer containerAllocationExpirer;
  /* set of containers that have just launched */
  protected final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();

  /* set of containers that need to be cleaned */
  protected final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* set of containers that need to be signaled */
  protected final List<SignalContainerRequest> containersToSignal =
      new ArrayList<SignalContainerRequest>();

  /*
   * set of containers to notify NM to remove them from its context. Currently,
   * this includes containers that were notified to AM about their completion
   */
  protected final Set<ContainerId> containersToBeRemovedFromNM =
      new HashSet<ContainerId>();

  /* the list of applications that have finished and need to be purged */
  protected final List<ApplicationId> finishedApplications =
      new ArrayList<ApplicationId>();

  /* the list of applications that are running on this node */
  protected final List<ApplicationId> runningApplications =
      new ArrayList<ApplicationId>();
  
  protected final Map<ContainerId, Container> toBeDecreasedContainers =
      new HashMap<>();
  
  private final Map<ContainerId, Container> nmReportedIncreasedContainers =
      new HashMap<>();
  
  // Map of renewed application certificates that should be propagated to NM
  private final Map<ApplicationId, UpdatedCryptoForApp> appX509ToUpdate =
      new ConcurrentHashMap<>();
  
  // Map of renewed application JWT that should be propagated to NM
  private final Map<ApplicationId, UpdatedCryptoForApp> appJWTToUpdate =
      new ConcurrentHashMap<>();

  protected NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)

      //Transitions from NEW state
      .addTransition(NodeState.NEW, NodeState.RUNNING,
          RMNodeEventType.STARTED, new AddNodeTransition())
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.NEW, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))

      //Transitions from RUNNING state
      .addTransition(NodeState.RUNNING,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.RUNNING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.RUNNING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.RUNNING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.RUNNING, EnumSet.of(NodeState.RUNNING),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.DECREASE_CONTAINER,
          new DecreaseContainersTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.UPDATE_CRYPTO_MATERIAL, new UpdateCryptoMaterialForAppTransition())

      //Transitions from REBOOTED state
      .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())

      //Transitions from DECOMMISSIONED state
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

       //Transitions from DECOMMISSIONING state
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.RUNNING,
          RMNodeEventType.RECOMMISSION,
          new RecommissionNodeTransition(NodeState.RUNNING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.DECOMMISSIONING,
          EnumSet.of(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.DECOMMISSIONING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))

      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.DECOMMISSIONING, EnumSet.of(
          NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenRunningTransition())

      //Transitions from LOST state
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      //Transitions from UNHEALTHY state
      .addTransition(NodeState.UNHEALTHY,
          EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenUnHealthyTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.UNHEALTHY,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.UNHEALTHY, EnumSet.of(NodeState.UNHEALTHY),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      //Transitions from SHUTDOWN state
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      // create the topology tables
      .installTopology();

  protected final StateMachine<NodeState, RMNodeEventType,
                             RMNodeEvent> stateMachine;

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability, String nodeManagerVersion) {
    this.nodeId = nodeId;
    this.context = context;
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    this.node = node;
    this.healthReport = "Healthy";
    this.lastHealthReportTime = System.currentTimeMillis();
    this.nodeManagerVersion = nodeManagerVersion;
    this.timeStamp = 0;

    this.latestNodeHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();

    this.containerAllocationExpirer = context.getContainerAllocationExpirer();
  }

  @Override
  public String toString() {
    return this.nodeId.toString();
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public int getCommandPort() {
    return commandPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  // Test only
  public void setHttpPort(int port) {
    this.httpPort = port;
  }

  @Override
  public NodeId getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getNodeAddress() {
    return this.nodeAddress;
  }

  @Override
  public String getHttpAddress() {
    return this.httpAddress;
  }

  @Override
  public Resource getTotalCapability() {
    return this.totalCapability;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }
  
  @Override
  public Node getNode() {
    return this.node;
  }
  
  @Override
  public String getHealthReport() {
    this.readLock.lock();

    try {
      return this.healthReport;
    } finally {
      this.readLock.unlock();
    }
  }
  
  public void setHealthReport(String healthReport) {
    this.writeLock.lock();

    try {
      this.healthReport = healthReport;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void setLastHealthReportTime(long lastHealthReportTime) {
    this.writeLock.lock();

    try {
      this.lastHealthReportTime = lastHealthReportTime;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public long getLastHealthReportTime() {
    this.readLock.lock();

    try {
      return this.lastHealthReportTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getNodeManagerVersion() {
    return nodeManagerVersion;
  }

  @Override
  public ResourceUtilization getAggregatedContainersUtilization() {
    this.readLock.lock();

    try {
      return this.containersUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.writeLock.lock();

    try {
      this.containersUtilization = containersUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ResourceUtilization getNodeUtilization() {
    this.readLock.lock();

    try {
      return this.nodeUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.writeLock.lock();

    try {
      this.nodeUtilization = nodeUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public NodeState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ApplicationId> getAppsToCleanup() {
    this.readLock.lock();

    try {
      return new ArrayList<ApplicationId>(this.finishedApplications);
    } finally {
      this.readLock.unlock();
    }

  }
  
  @Override
  public List<ApplicationId> getRunningApps() {
    this.readLock.lock();
    try {
      return new ArrayList<ApplicationId>(this.runningApplications);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {

    this.readLock.lock();

    try {
      return new ArrayList<ContainerId>(this.containersToClean);
    } finally {
      this.readLock.unlock();
    }
  };

  @VisibleForTesting
  public Collection<Container> getToBeDecreasedContainers() {
    return toBeDecreasedContainers.values(); 
  }

  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {

    this.readLock.lock();

    try {
      return this.latestNodeHeartBeatResponse;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void resetLastNodeHeartBeatResponse() {
    this.writeLock.lock();
    try {
      latestNodeHeartBeatResponse.setResponseId(0);
    } finally {
      this.writeLock.unlock();
    }
  }

  protected void updateMetricsForRejoinedNode(NodeState previousNodeState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    metrics.incrNumActiveNodes();

    switch (previousNodeState) {
    case LOST:
      metrics.decrNumLostNMs();
      break;
    case REBOOTED:
      metrics.decrNumRebootedNMs();
      break;
    case DECOMMISSIONED:
      metrics.decrDecommisionedNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.decrNumShutdownNMs();
      break;
    default:
      LOG.debug("Unexpected previous node state");
    }
  }

  // Update metrics when moving to Decommissioning state
  private void updateMetricsForGracefulDecommission(NodeState initialState,
      NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    switch (initialState) {
    case UNHEALTHY :
      metrics.decrNumUnhealthyNMs();
      break;
    case RUNNING :
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING :
      metrics.decrDecommissioningNMs();
      break;
    default :
      LOG.warn("Unexpcted initial state");
    }

    switch (finalState) {
    case DECOMMISSIONING :
      metrics.incrDecommissioningNMs();
      break;
    case RUNNING :
      metrics.incrNumActiveNodes();
      break;
    default :
      LOG.warn("Unexpected final state");
    }
  }

  protected void updateMetricsForDeactivatedNode(NodeState initialState,
                                               NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();

    switch (initialState) {
    case RUNNING:
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING:
      metrics.decrDecommissioningNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case NEW:
      break;
    default:
      LOG.warn("Unexpected initial state");
    }

    switch (finalState) {
    case DECOMMISSIONED:
      metrics.incrDecommisionedNMs();
      break;
    case LOST:
      metrics.incrNumLostNMs();
      break;
    case REBOOTED:
      metrics.incrNumRebootedNMs();
      break;
    case UNHEALTHY:
      metrics.incrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.incrNumShutdownNMs();
      break;
    default:
      LOG.warn("Unexpected final state");
    }
  }
  
  abstract protected void handleRunningAppOnNode(RMNodeImpl rmNode,
          RMContext context, ApplicationId appId, NodeId nodeId);
  
  private static void updateNodeResourceFromEvent(RMNodeImpl rmNode, 
     RMNodeResourceUpdateEvent event){
      ResourceOption resourceOption = event.getResourceOption();
      // Set resource on RMNode
      rmNode.totalCapability = resourceOption.getResource();
  }

  public static class AddNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {
  
    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.addNodeTransitionInternal(rmNode,event);
    }
  }

  protected abstract void addNodeTransitionInternal(RMNodeImpl rmNode, RMNodeEvent event);

  protected static List<NMContainerStatus> updateNewNodeMetricsAndContainers(
          RMNodeImpl rmNode, RMNodeStartedEvent startEvent) {
    List<NMContainerStatus> containers;
    ClusterMetrics.getMetrics().incrNumActiveNodes();
    containers = startEvent.getNMContainerStatuses();
    if (containers != null && !containers.isEmpty()) {
      for (NMContainerStatus container : containers) {
        if (container.getContainerState() == ContainerState.RUNNING) {
          rmNode.launchedContainers.add(container.getContainerId());
        }
      }
    }
    return containers;
  }
  
  protected abstract NodeState reconnectNodeTransitionInternal(RMNodeImpl rmNode, RMNodeEvent event);
  
  protected void handleNMContainerStatus(
          List<NMContainerStatus> nmContainerStatuses, RMNodeImpl rmnode) {
    if (nmContainerStatuses != null) {
      List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
      for (NMContainerStatus nmContainerStatus : nmContainerStatuses) {
        containerStatuses.add(createContainerStatus(nmContainerStatus));
      }
      rmnode.handleContainerStatus(containerStatuses);
    }
  }

  private ContainerStatus createContainerStatus(
          NMContainerStatus remoteContainer) {
    ContainerStatus cStatus = ContainerStatus.newInstance(remoteContainer.
            getContainerId(),
            remoteContainer.getContainerState(),
            remoteContainer.getDiagnostics(),
            remoteContainer.getContainerExitStatus());
    return cStatus;
  }

  public static class ReconnectNodeTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      return rmNode.reconnectNodeTransitionInternal(rmNode, event);
    }
  }
  
  public static class UpdateNodeResourceWhenRunningTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeResourceUpdateEvent updateEvent = (RMNodeResourceUpdateEvent)event;
      updateNodeResourceFromEvent(rmNode, updateEvent);
      // Notify new resourceOption to scheduler
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeResourceUpdateSchedulerEvent(rmNode, updateEvent.getResourceOption()));
    }
  }
  
  public static class UpdateNodeResourceWhenUnusableTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // The node is not usable, only log a warn message
      LOG.warn("Try to update resource on a "+ rmNode.getState().toString() +
          " node: "+rmNode.toString());
      updateNodeResourceFromEvent(rmNode, (RMNodeResourceUpdateEvent)event);
      // No need to notify scheduler as schedulerNode is not function now
      // and can sync later from RMnode.
    }
  }
  
  public static class UpdateCryptoMaterialForAppTransition implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
  
    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent rmNodeEvent) {
      RMNodeUpdateCryptoMaterialForAppEvent updateEvent = (RMNodeUpdateCryptoMaterialForAppEvent) rmNodeEvent;
      LOG.info("Node " + rmNode.toString() + " received UPDATE_CRYPTO_MATERIAL event for app " + updateEvent
          .getSecurityMaterial().getApplicationId());
      UpdatedCryptoForApp updatedCrypto = recordFactory.newRecordInstance(UpdatedCryptoForApp.class);
      if (updateEvent.getSecurityMaterial() instanceof X509SecurityHandler.X509SecurityManagerMaterial) {
        X509SecurityHandler.X509SecurityManagerMaterial updatedMaterial =
            (X509SecurityHandler.X509SecurityManagerMaterial) updateEvent.getSecurityMaterial();
        ByteBuffer keyStore = ByteBuffer.wrap(updatedMaterial.getKeyStore());
        ByteBuffer trustStore = ByteBuffer.wrap(updatedMaterial.getTrustStore());
        updatedCrypto.setKeyStore(keyStore);
        updatedCrypto.setKeyStorePassword(updatedMaterial.getKeyStorePassword());
        updatedCrypto.setTrustStore(trustStore);
        updatedCrypto.setTrustStorePassword(updatedMaterial.getTrustStorePassword());
        updatedCrypto.setVersion(updatedMaterial.getCryptoMaterialVersion());
        rmNode.appX509ToUpdate.put(updateEvent.getSecurityMaterial().getApplicationId(), updatedCrypto);
      }
      
      if (updateEvent.getSecurityMaterial() instanceof JWTSecurityHandler.JWTSecurityManagerMaterial) {
        JWTSecurityHandler.JWTSecurityManagerMaterial updatedMaterial =
            (JWTSecurityHandler.JWTSecurityManagerMaterial) updateEvent.getSecurityMaterial();
        updatedCrypto.setJWT(updatedMaterial.getToken());
        updatedCrypto.setJWTExpiration(updatedMaterial.getExpirationDate().toEpochMilli());
        rmNode.appJWTToUpdate.put(updateEvent.getSecurityMaterial().getApplicationId(), updatedCrypto);
      }
    }
  }
  
  public static class CleanUpAppTransition
          implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.cleanUpAppTransitionInternal(rmNode, event);
    }
  }

  abstract protected void cleanUpAppTransitionInternal(RMNodeImpl rmNode,
          RMNodeEvent event);

  public static class CleanUpContainerTransition implements
          SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.cleanUpContainerTransitionInternal(rmNode, event);
    }
  }

  abstract protected void cleanUpContainerTransitionInternal(RMNodeImpl rmNode,
          RMNodeEvent event);
    
  public static class AddContainersToBeRemovedFromNMTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToBeRemovedFromNM.addAll(((
          RMNodeFinishedContainersPulledByAMEvent) event).getContainers());
    }
  }
  
  public static class DecreaseContainersTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
 
    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeDecreaseContainerEvent de = (RMNodeDecreaseContainerEvent) event;

      rmNode.decreaseContainersInt(rmNode, de);
    }
  }

  protected abstract void decreaseContainersInt(RMNodeImpl rmNode, RMNodeDecreaseContainerEvent de);
  
  public static class DeactivateNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.deactivateNode(rmNode, finalState);
    }
  }

  abstract protected void deactivateNode(RMNodeImpl rmNode, final NodeState finalState);
  


  /**
   * The transition to put node in decommissioning state.
   */
  public static class DecommissioningNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

  private final NodeState initState;
  private final NodeState finalState;

  public DecommissioningNodeTransition(NodeState initState,
      NodeState finalState) {
    this.initState = initState;
    this.finalState = finalState;
  }

  @Override
  public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
    LOG.info("Put Node " + rmNode.nodeId + " in DECOMMISSIONING.");
    // Update NM metrics during graceful decommissioning.
    rmNode.updateMetricsForGracefulDecommission(initState, finalState);
    if (rmNode.originalTotalCapability == null) {
      rmNode.originalTotalCapability = Resources.clone(rmNode.totalCapability);
      LOG.info("Preserve original total capability: "
          + rmNode.originalTotalCapability);
    }
  }
}

public static class RecommissionNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

  private final NodeState finalState;

  public RecommissionNodeTransition(NodeState finalState) {
    this.finalState = finalState;
  }

  @Override
  public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
    // Restore the original total capability
    if (rmNode.originalTotalCapability != null) {
      rmNode.totalCapability = rmNode.originalTotalCapability;
      rmNode.originalTotalCapability = null;
    }
    LOG.info("Node " + rmNode.nodeId + " in DECOMMISSIONING is " + "recommissioned back to RUNNING.");
    rmNode
        .updateMetricsForGracefulDecommission(rmNode.getState(), finalState);
    //update the scheduler with the restored original total capability
    rmNode.context
        .getDispatcher()
        .getEventHandler()
        .handle(
            new NodeResourceUpdateSchedulerEvent(rmNode, ResourceOption
                .newInstance(rmNode.totalCapability, 0)));
  }
}

  /**
   * Status update transition when node is healthy.
   */
  public static class StatusUpdateWhenHealthyTransition implements
          MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      return rmNode.statusUpdateWhenHealthyTransitionInternal(rmNode, event);
    }
  }

  abstract protected NodeState statusUpdateWhenHealthyTransitionInternal(
          RMNodeImpl rmNode, RMNodeEvent event);

  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      return rmNode.statusUpdateWhenUnHealthyTransitionInternal(rmNode, event);
    }
  }

  protected abstract NodeState statusUpdateWhenUnHealthyTransitionInternal(RMNodeImpl rmNode, RMNodeEvent event);

  public static class SignalContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.signalContainerInt(rmNode, event);
    }
  }

  protected abstract void signalContainerInt(RMNodeImpl rmNode, RMNodeEvent event);
  
  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    return this.pullContainerUpdatesInternal();
  }
  
  protected abstract List<UpdatedContainerInfo> pullContainerUpdatesInternal();

  @VisibleForTesting
  public void setNextHeartBeat(boolean nextHeartBeat) {
    this.nextHeartBeat = nextHeartBeat;
  }
  
  @VisibleForTesting
  public int getQueueSize() {
    return nodeUpdateQueue.size();
  }

  // For test only.
  @VisibleForTesting
  public Set<ContainerId> getLaunchedContainers() {
    return this.launchedContainers;
  }

  @Override
  public Set<String> getNodeLabels() {
    RMNodeLabelsManager nlm = context.getNodeLabelManager();
    if (nlm == null || nlm.getLabelsOnNode(nodeId) == null) {
      return CommonNodeLabelsManager.EMPTY_STRING_SET;
    }
    return nlm.getLabelsOnNode(nodeId);
  }
  
  protected void handleReportedIncreasedContainers(
      List<Container> reportedIncreasedContainers) {
    for (Container container : reportedIncreasedContainers) {
      ContainerId containerId = container.getId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      }
      
      this.nmReportedIncreasedContainers.put(containerId, container);
    }
  }

  abstract protected void handleContainerStatus(List<ContainerStatus> containerStatuses);

  protected List<ContainerStatus> findLostContainers(int numRemoteRunning,
      List<ContainerStatus> containerStatuses) {
    if (numRemoteRunning >= launchedContainers.size()) {
      return Collections.emptyList();
    }
    Set<ContainerId> nodeContainers =
        new HashSet<ContainerId>(numRemoteRunning);
    List<ContainerStatus> lostContainers = new ArrayList<ContainerStatus>(
        launchedContainers.size() - numRemoteRunning);
    for (ContainerStatus remoteContainer : containerStatuses) {
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        nodeContainers.add(remoteContainer.getContainerId());
      }
    }
    Iterator<ContainerId> iter = launchedContainers.iterator();
    while (iter.hasNext()) {
      ContainerId containerId = iter.next();
      if (!nodeContainers.contains(containerId)) {
        String diag = "Container " + containerId
            + " was running but not reported from " + nodeId;
        LOG.warn(diag);
        lostContainers.add(SchedulerUtils.createAbnormalContainerStatus(
            containerId, diag));
        iter.remove();
      }
    }
    return lostContainers;
  }

  protected void handleLogAggregationStatus(
      List<LogAggregationReport> logAggregationReportsForApps) {
    for (LogAggregationReport report : logAggregationReportsForApps) {
      RMApp rmApp = this.context.getRMApps().get(report.getApplicationId());
      if (rmApp != null) {
        ((RMAppImpl)rmApp).aggregateLogReport(this.nodeId, report);
      }
    }
  }

  @Override
  public List<Container> pullNewlyIncreasedContainers() {
    try {
      writeLock.lock();

      if (nmReportedIncreasedContainers.isEmpty()) {
        return Collections.EMPTY_LIST;
      } else {
        List<Container> container =
            new ArrayList<Container>(nmReportedIncreasedContainers.values());
        nmReportedIncreasedContainers.clear();
        return container;
      }
      
    } finally {
      writeLock.unlock();
    }
   }

  public Resource getOriginalTotalCapability() {
    return this.originalTotalCapability;
  }

  @Override
  public long getUntrackedTimeStamp() {
    return this.timeStamp;
  }

  @Override
  public void setUntrackedTimeStamp(long ts) {
    this.timeStamp = ts;
  }
  
  public boolean getNextHeartbeat(){
    return nextHeartBeat;
  }
  
  @Override
  public Map<ApplicationId, UpdatedCryptoForApp> getAppX509ToUpdate() {
    return appX509ToUpdate;
  }
  
  @Override
  public Map<ApplicationId, UpdatedCryptoForApp> getAppJWTToUpdate() {
    return appJWTToUpdate;
  }
}
