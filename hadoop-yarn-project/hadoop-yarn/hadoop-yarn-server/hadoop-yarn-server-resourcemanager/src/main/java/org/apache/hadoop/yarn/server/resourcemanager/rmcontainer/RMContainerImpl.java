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
package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.RMContainer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.hadoop.yarn.event.AsyncDispatcher;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMContainerImpl implements
    org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer {

  private static final Log LOG = LogFactory.getLog(RMContainerImpl.class);

  private static final StateMachineFactory<RMContainerImpl, RMContainerState, RMContainerEventType, RMContainerEvent>
      stateMachineFactory =
      new StateMachineFactory<RMContainerImpl, RMContainerState, RMContainerEventType, RMContainerEvent>(
          RMContainerState.NEW)
          // Transitions from NEW state
          .addTransition(RMContainerState.NEW, RMContainerState.ALLOCATED,
              RMContainerEventType.START, new ContainerStartedTransition())
          .addTransition(RMContainerState.NEW, RMContainerState.KILLED,
              RMContainerEventType.KILL).addTransition(RMContainerState.NEW,
          RMContainerState.RESERVED, RMContainerEventType.RESERVED,
          new ContainerReservedTransition())
          // Transitions from RESERVED state
          .addTransition(RMContainerState.RESERVED, RMContainerState.RESERVED,
              RMContainerEventType.RESERVED, new ContainerReservedTransition())
          .addTransition(RMContainerState.RESERVED, RMContainerState.ALLOCATED,
              RMContainerEventType.START,
              new ContainerStartedTransition()).addTransition(
          RMContainerState.RESERVED, RMContainerState.KILLED,
          RMContainerEventType.KILL) // nothing to do
          .addTransition(RMContainerState.RESERVED, RMContainerState.RELEASED,
              RMContainerEventType.RELEASED) // nothing to do

              // Transitions from ALLOCATED state
          .addTransition(RMContainerState.ALLOCATED, RMContainerState.ACQUIRED,
              RMContainerEventType.ACQUIRED, new AcquiredTransition())
          .addTransition(RMContainerState.ALLOCATED, RMContainerState.EXPIRED,
              RMContainerEventType.EXPIRE,
              new FinishedTransition()).addTransition(
          RMContainerState.ALLOCATED, RMContainerState.KILLED,
          RMContainerEventType.KILL, new FinishedTransition())
          // Transitions from ACQUIRED state
          .addTransition(RMContainerState.ACQUIRED, RMContainerState.RUNNING,
              RMContainerEventType.LAUNCHED, new LaunchedTransition())
          .addTransition(RMContainerState.ACQUIRED, RMContainerState.COMPLETED,
              RMContainerEventType.FINISHED,
              new ContainerFinishedAtAcquiredState())
          .addTransition(RMContainerState.ACQUIRED, RMContainerState.RELEASED,
              RMContainerEventType.RELEASED, new KillTransition())
          .addTransition(RMContainerState.ACQUIRED, RMContainerState.EXPIRED,
              RMContainerEventType.EXPIRE, new KillTransition()).addTransition(
          RMContainerState.ACQUIRED, RMContainerState.KILLED,
          RMContainerEventType.KILL, new KillTransition())
          // Transitions from RUNNING state
          .addTransition(RMContainerState.RUNNING, RMContainerState.COMPLETED,
              RMContainerEventType.FINISHED, new FinishedTransition())
          .addTransition(RMContainerState.RUNNING, RMContainerState.KILLED,
              RMContainerEventType.KILL, new KillTransition())
          .addTransition(RMContainerState.RUNNING, RMContainerState.RELEASED,
              RMContainerEventType.RELEASED,
              new KillTransition()).addTransition(RMContainerState.RUNNING,
          RMContainerState.RUNNING, RMContainerEventType.EXPIRE)
          // Transitions from COMPLETED state
          .addTransition(RMContainerState.COMPLETED, RMContainerState.COMPLETED,
              EnumSet.of(RMContainerEventType.EXPIRE,
                  RMContainerEventType.RELEASED, RMContainerEventType.KILL))
              // Transitions from EXPIRED state
          .addTransition(RMContainerState.EXPIRED, RMContainerState.EXPIRED,
              EnumSet
                  .of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))
              // Transitions from RELEASED state
          .addTransition(RMContainerState.RELEASED, RMContainerState.RELEASED,
              EnumSet.of(RMContainerEventType.EXPIRE,
                  RMContainerEventType.RELEASED, RMContainerEventType.KILL,
                  RMContainerEventType.FINISHED))
              // Transitions from KILLED state
          .addTransition(RMContainerState.KILLED, RMContainerState.KILLED,
              EnumSet.of(RMContainerEventType.EXPIRE,
                  RMContainerEventType.RELEASED, RMContainerEventType.KILL,
                  RMContainerEventType.FINISHED))
              // create the topology tables
          .installTopology();

  private final StateMachine<RMContainerState, RMContainerEventType, RMContainerEvent>
      stateMachine; //recovered
  private final ReadLock readLock; //recovered
  private final WriteLock writeLock;//recovered
  private final ContainerId containerId;//recovered
  private final ApplicationAttemptId appAttemptId;//recovered
  private final NodeId nodeId;//recovered
  private final Container container;//recovered
  private final RMContext rmContext;//recovered
  private final EventHandler eventHandler;//recovered
  private final ContainerAllocationExpirer containerAllocationExpirer;
      //recovered
  private final String user;//recovered

  private Resource reservedResource;//recoverd
  private NodeId reservedNode;//recovered
  private Priority reservedPriority;//recovered
  private long startTime;//recovered
  private long finishTime;//recovered
  private ContainerStatus finishedStatus;//recovered

  public RMContainerImpl(Container container, ApplicationAttemptId appAttemptId,
      NodeId nodeId, String user, RMContext rmContext,
      TransactionState transactionState) {
    this.stateMachine = stateMachineFactory.make(this);
    this.containerId = container.getId();
    this.nodeId = nodeId;
    this.container = container;
    this.appAttemptId = appAttemptId;
    this.user = user;
    this.startTime = System.currentTimeMillis();
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.containerAllocationExpirer = rmContext.getContainerAllocationExpirer();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    rmContext.getRMApplicationHistoryWriter()
        .containerStarted(this, transactionState);
  }

  //TORECOVER OPT change to implement recoverable
  @Override
  public void recover(RMContainer hopRMContainer) {
    LOG.debug("recovering container " + hopRMContainer.getContainerIdID() + 
            " in state "+ hopRMContainer.getState());
    this.startTime = hopRMContainer.getStarttime();
    this.stateMachine.setCurrentState(RMContainerState.valueOf(hopRMContainer.
        getState()));
    this.finishTime = hopRMContainer.getFinishtime();

    if (getState().equals(RMContainerState.ACQUIRED)) {
      this.containerAllocationExpirer.register(containerId);
    }
    if (hopRMContainer.getReservedNodeIdID() != null) {
      this.reservedNode = NodeId.newInstance(hopRMContainer.
              getReservedNodeHost(),
              hopRMContainer.getReservedNodePort());
      this.reservedResource = Resource.newInstance(hopRMContainer.
              getReservedMemory(),
              hopRMContainer.getReservedVCores());
      this.reservedPriority = Priority.newInstance(hopRMContainer.
              getReservedPriorityID());
    }
    if (hopRMContainer.getFinishedStatusState() != null) {
      this.finishedStatus = ContainerStatus.newInstance(containerId,
          ContainerState.valueOf(hopRMContainer.getFinishedStatusState()), user,
          hopRMContainer.getExitStatus());
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appAttemptId;
  }

  @Override
  public Container getContainer() {
    return this.container;
  }

  @Override
  public RMContainerState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Resource getReservedResource() {
    return reservedResource;
  }

  @Override
  public NodeId getReservedNode() {
    return reservedNode;
  }

  @Override
  public Priority getReservedPriority() {
    return reservedPriority;
  }

  @Override
  public Resource getAllocatedResource() {
    return container.getResource();
  }

  @Override
  public NodeId getAllocatedNode() {
    return container.getNodeId();
  }

  @Override
  public Priority getAllocatedPriority() {
    return container.getPriority();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getFinishTime() {
    try {
      readLock.lock();
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getDiagnosticsInfo() {
    try {
      readLock.lock();
      if (getFinishedStatus() != null) {
        return getFinishedStatus().getDiagnostics();
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getLogURL() {
    try {
      readLock.lock();
      return WebAppUtils.getRunningLogURL("//" + container.getNodeHttpAddress(),
          ConverterUtils.toString(containerId), user);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getContainerExitStatus() {
    try {
      readLock.lock();
      if (getFinishedStatus() != null) {
        return getFinishedStatus().getExitStatus();
      } else {
        return 0;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {
    try {
      readLock.lock();
      if (getFinishedStatus() != null) {
        return getFinishedStatus().getState();
      } else {
        return ContainerState.RUNNING;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String toString() {
    return containerId.toString();
  }

  @Override
  public void handle(RMContainerEvent event) {
    LOG.debug("Processing " + event.getContainerId() + " of type " + event.
        getType());
    try {
      writeLock.lock();
      RMContainerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
        if(stateMachine.getCurrentState()!=oldState){
          if (event.getTransactionState() != null) {
            ((TransactionStateImpl) event.getTransactionState()).
                addRMContainerToUpdate(this);
          }
        }
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + " on container " +
            this.containerId);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public ContainerStatus getFinishedStatus() {
    return finishedStatus;
  }

  private static class BaseTransition
      implements SingleArcTransition<RMContainerImpl, RMContainerEvent> {

    @Override
    public void transition(RMContainerImpl cont, RMContainerEvent event) {

    }
  }

  private static final class ContainerReservedTransition
      extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      RMContainerReservedEvent e = (RMContainerReservedEvent) event;
      container.reservedResource = e.getReservedResource();
      container.reservedNode = e.getReservedNode();
      container.reservedPriority = e.getReservedPriority();
      ((TransactionStateImpl) event.getTransactionState()).
                addRMContainerToUpdate(container);
    }
  }

  private static final class ContainerStartedTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      container.eventHandler.handle(
          new RMAppAttemptContainerAllocatedEvent(container.appAttemptId,
              event.getTransactionState()));
    }
  }

  private static final class AcquiredTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Register with containerAllocationExpirer.
      container.containerAllocationExpirer.register(container.getContainerId());

      // Tell the appAttempt
      container.eventHandler.handle(new RMAppAttemptContainerAcquiredEvent(
          container.getApplicationAttemptId(), container.getContainer(),
          event.getTransactionState()));
    }
  }

  private static final class LaunchedTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer
          .unregister(container.getContainerId());
    }
  }

  private static class FinishedTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      RMContainerFinishedEvent finishedEvent = (RMContainerFinishedEvent) event;

      container.finishTime = System.currentTimeMillis();
      container.finishedStatus = finishedEvent.getRemoteContainerStatus();
      // Inform AppAttempt
      container.eventHandler.handle(
          new RMAppAttemptContainerFinishedEvent(container.appAttemptId,
              finishedEvent.getRemoteContainerStatus(),
              event.getTransactionState()));

      container.rmContext.getRMApplicationHistoryWriter()
          .containerFinished(container, event.getTransactionState());
      
      ((TransactionStateImpl) event.getTransactionState()).
                addRMContainerToUpdate(container);
    }
  }

  private static final class ContainerFinishedAtAcquiredState
      extends FinishedTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer
          .unregister(container.getContainerId());

      // Inform AppAttempt
      super.transition(container, event);
    }
  }

  private static final class KillTransition extends FinishedTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      //the container is already unregistred when the contaier is killed by the expirer
      //plus that cause a deadlock
      if(event.getType()!=RMContainerEventType.EXPIRE){
      // Unregister from containerAllocationExpirer.
        container.containerAllocationExpirer
                .unregister(container.getContainerId());
      }
      // Inform node
      container.eventHandler.handle(
          new RMNodeCleanContainerEvent(container.nodeId, container.containerId,
              event.
                  getTransactionState()));

      // Inform appAttempt
      super.transition(container, event);
    }
  }


  @Override
  public NodeId getNodeId() {
    return nodeId;
  }

  @Override
  public String getUser() {
    return user;
  }


  @Override
  public ContainerReport createContainerReport() {
    this.readLock.lock();
    ContainerReport containerReport = null;
    try {
      containerReport = ContainerReport
          .newInstance(this.getContainerId(), this.getAllocatedResource(),
              this.getAllocatedNode(), this.getAllocatedPriority(),
              this.getStartTime(), this.getFinishTime(),
              this.getDiagnosticsInfo(), this.getLogURL(),
              this.getContainerExitStatus(), this.getContainerState());
    } finally {
      this.readLock.unlock();
    }
    return containerReport;
  }

}
