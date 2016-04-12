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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppStartAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

//TODO 2: remove all the useless time where we store application state in the DB
//through RMStateStore
@SuppressWarnings({"rawtypes", "unchecked"})
public class RMAppImpl implements RMApp, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // Immutable fields
  private final ApplicationId applicationId;//recovered
  private final RMContext rmContext;//recovered
  private final Configuration conf;//recovered
  private final String user;//recovered
  private final String name;//recovered
  private final ApplicationSubmissionContext submissionContext;//recovered
  private final Dispatcher dispatcher;//recovered
  private final YarnScheduler scheduler;//recovered
  private final ApplicationMasterService masterService;//recovered
  private final StringBuilder diagnostics = new StringBuilder();//recovered
  private final int maxAppAttempts; //recovered
  private final ReadLock readLock;//recovered
  private final WriteLock writeLock;//recevered
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts =
      new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>(); //recovered
  private final long submitTime;//recovered
  private final Set<RMNode> updatedNodes = new HashSet<RMNode>();//recovered
  private final String applicationType; //recovered
  private final Set<String> applicationTags;//recovered

  // Mutable fields
  private long startTime;//recovered
  private long finishTime = 0; //recovered
  private long storedFinishTime = 0; //recovered
  private RMAppAttempt currentAttempt; //recovered
  private String queue; //recovered
  private final EventHandler handler;//recovered
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();//recovered

  // These states stored are only valid when app is at killing or final_saving.
  private RMAppState stateBeforeKilling;//recovered
  private RMAppState recoveredFinalState;//recovered

  private static final StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent>
      stateMachineFactory =
      new StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent>(
          RMAppState.NEW)
          // Transitions from NEW state
          .addTransition(RMAppState.NEW, RMAppState.NEW,
              RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
          .addTransition(RMAppState.NEW, RMAppState.SUBMITTED,
              RMAppEventType.START, new AddApplicationToSchedulerTransition())
          .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
              new AppKilledTransition()).addTransition(RMAppState.NEW,
          RMAppState.FAILED, RMAppEventType.APP_REJECTED,
          new AppRejectedTransition())
          // Transitions from SUBMITTED state
          .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
              RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
          .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
              RMAppEventType.MOVE, new RMAppMoveTransition())
          .addTransition(RMAppState.SUBMITTED, RMAppState.FAILED,
              RMAppEventType.APP_REJECTED, new AppRejectedTransition())
          .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
              RMAppEventType.APP_ACCEPTED,
              new StartAppAttemptTransition()).addTransition(
          RMAppState.SUBMITTED, RMAppState.KILLED, RMAppEventType.KILL,
          new AppKilledTransition())
          // Transitions from ACCEPTED state
          .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
              RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
          .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
              RMAppEventType.MOVE, new RMAppMoveTransition())
          .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
              RMAppEventType.ATTEMPT_REGISTERED)
          .addTransition(RMAppState.ACCEPTED,
              EnumSet.of(RMAppState.ACCEPTED, RMAppState.FAILED),
              // ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
              // event because RMAppRecoveredTransition is returning ACCEPTED state
              // directly and waiting for the previous AM to exit.
              RMAppEventType.ATTEMPT_FAILED,
              new AttemptFailedTransition(RMAppState.ACCEPTED))
          .addTransition(RMAppState.ACCEPTED, RMAppState.FINISHED,
              RMAppEventType.ATTEMPT_FINISHED,
              FINISHED_TRANSITION).addTransition(RMAppState.ACCEPTED,
          RMAppState.KILLING, RMAppEventType.KILL, new KillAttemptTransition())
          // ACCECPTED state can once again receive APP_ACCEPTED event, because on
          // recovery the app returns ACCEPTED state and the app once again go
          // through the scheduler and triggers one more APP_ACCEPTED event at
          // ACCEPTED state.
          .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
              RMAppEventType.APP_ACCEPTED)
              // Transitions from RUNNING state
          .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
              RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
          .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
              RMAppEventType.MOVE, new RMAppMoveTransition())
          .addTransition(RMAppState.RUNNING, RMAppState.FINISHING,
              RMAppEventType.ATTEMPT_UNREGISTERED,
              new AttemptUnregisteredTransition())
          .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
              // UnManagedAM directly jumps to finished
              RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
          .addTransition(RMAppState.RUNNING,
              EnumSet.of(RMAppState.ACCEPTED, RMAppState.FAILED),
              RMAppEventType.ATTEMPT_FAILED,
              new AttemptFailedTransition(RMAppState.ACCEPTED)).addTransition(
          RMAppState.RUNNING, RMAppState.KILLING, RMAppEventType.KILL,
          new KillAttemptTransition())
          // Transitions from FINISHING state
          .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
              RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
              // ignorable transitions
          .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
              EnumSet.of(RMAppEventType.NODE_UPDATE,
                  // ignore Kill as we have already saved the final Finished state in
                  // state store.
                  RMAppEventType.KILL))
              // Transitions from KILLING state
          .addTransition(RMAppState.KILLING, RMAppState.KILLED,
              RMAppEventType.ATTEMPT_KILLED,
              new AppKilledTransition()).addTransition(RMAppState.KILLING,
          RMAppState.KILLING, EnumSet
              .of(RMAppEventType.NODE_UPDATE, RMAppEventType.ATTEMPT_REGISTERED,
                  RMAppEventType.ATTEMPT_UNREGISTERED,
                  RMAppEventType.ATTEMPT_FINISHED,
                  RMAppEventType.ATTEMPT_FAILED,
                  RMAppEventType.APP_UPDATE_SAVED, RMAppEventType.KILL))
          // Transitions from FINISHED state
          // ignorable transitions
          .addTransition(RMAppState.FINISHED, RMAppState.FINISHED, EnumSet
                  .of(RMAppEventType.NODE_UPDATE,
                      RMAppEventType.ATTEMPT_UNREGISTERED,
                      RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.KILL))
              // Transitions from FAILED state
              // ignorable transitions
          .addTransition(RMAppState.FAILED, RMAppState.FAILED,
              EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE))
              // Transitions from KILLED state
              // ignorable transitions
          .addTransition(RMAppState.KILLED, RMAppState.KILLED, EnumSet
                  .of(RMAppEventType.APP_ACCEPTED, RMAppEventType.APP_REJECTED,
                      RMAppEventType.KILL, RMAppEventType.ATTEMPT_FINISHED,
                      RMAppEventType.ATTEMPT_FAILED,
                      RMAppEventType.NODE_UPDATE)).installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
      stateMachine;//recovered

  private static final ApplicationResourceUsageReport
      DUMMY_APPLICATION_RESOURCE_USAGE_REPORT = BuilderUtils
      .newApplicationResourceUsageReport(-1, -1,
          Resources.createResource(-1, -1), Resources.createResource(-1, -1),
          Resources.createResource(-1, -1));
  private static final int DUMMY_APPLICATION_ATTEMPT_NUMBER = -1;
  
  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
      ApplicationMasterService masterService, long submitTime,
      String applicationType, Set<String> applicationTags,
      TransactionState transactionState) {

    this.applicationId = applicationId;
    this.name = name;
    this.rmContext = rmContext;
    this.dispatcher = rmContext.getDispatcher();
    this.handler = dispatcher.getEventHandler();
    this.conf = config;
    this.user = user;
    this.queue = queue;
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.submitTime = submitTime;
    this.startTime = System.currentTimeMillis();
    this.applicationType = applicationType;
    this.applicationTags = applicationTags;

    int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    int individualMaxAppAttempts = submissionContext.getMaxAppAttempts();
    if (individualMaxAppAttempts <= 0 ||
        individualMaxAppAttempts > globalMaxAppAttempts) {
      this.maxAppAttempts = globalMaxAppAttempts;
      LOG.warn("The specific max attempts: " + individualMaxAppAttempts +
          " for application: " + applicationId.getId() +
          " is invalid, because it is out of the range [1, " +
          globalMaxAppAttempts + "]. Use the global max attempts instead.");
    } else {
      this.maxAppAttempts = individualMaxAppAttempts;
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    rmContext.getRMApplicationHistoryWriter()
        .applicationStarted(this, transactionState);
  }

  public void addRMAppAttempt(ApplicationAttemptId appAttemptId,
      RMAppAttempt rmAppAttemptId) {
    this.attempts.put(appAttemptId, rmAppAttemptId);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return this.submissionContext;
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      // finish state is obtained based on the state machine's current state
      // as a fall-back in case the application has not been unregistered 
      // ( or if the app never unregistered itself )
      // when the report is requested
      if (currentAttempt != null &&
          currentAttempt.getFinalApplicationStatus() != null) {
        return currentAttempt.getFinalApplicationStatus();
      }
      return createFinalApplicationStatus(this.stateMachine.getCurrentState());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppState getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getUser() {
    return this.user;
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getProgress();
      }
      return 0;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
    this.readLock.lock();

    try {
      return this.attempts.get(appAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getQueue() {
    return this.queue;
  }

  @Override
  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    this.readLock.lock();

    try {
      return this.currentAttempt;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
    this.readLock.lock();

    try {
      return Collections.unmodifiableMap(this.attempts);
    } finally {
      this.readLock.unlock();
    }
  }

  private FinalApplicationStatus createFinalApplicationStatus(
      RMAppState state) {
    switch (state) {
      case NEW:
      case SUBMITTED:
      case ACCEPTED:
      case RUNNING:
      case KILLING:
        return FinalApplicationStatus.UNDEFINED;
      // finished without a proper final state is the same as failed  
      case FINISHING:
      case FINISHED:
      case FAILED:
        return FinalApplicationStatus.FAILED;
      case KILLED:
        return FinalApplicationStatus.KILLED;
    }
    throw new YarnRuntimeException("Unknown state passed!");
  }

  @Override
  public int pullRMNodeUpdates(Collection<RMNode> updatedNodes,
      TransactionState ts) {
    this.writeLock.lock();
    try {
      int updatedNodeCount = this.updatedNodes.size();
      updatedNodes.addAll(this.updatedNodes);
      if (ts != null) {
        ((TransactionStateImpl) ts).addUpdatedNodeToRemove(this.applicationId, 
                this.updatedNodes);
      }
      this.updatedNodes.clear();
      return updatedNodeCount;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ApplicationReport createAndGetApplicationReport(String clientUserName,
      boolean allowAccess) {
    this.readLock.lock();

    try {
      ApplicationAttemptId currentApplicationAttemptId = null;
      org.apache.hadoop.yarn.api.records.Token clientToAMToken = null;
      String trackingUrl = UNAVAILABLE;
      String host = UNAVAILABLE;
      String origTrackingUrl = UNAVAILABLE;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      FinalApplicationStatus finishState = getFinalApplicationStatus();
      String diags = UNAVAILABLE;
      float progress = 0.0f;
      org.apache.hadoop.yarn.api.records.Token amrmToken = null;
      if (allowAccess) {
        if (this.currentAttempt != null) {
          currentApplicationAttemptId = this.currentAttempt.getAppAttemptId();
          trackingUrl = this.currentAttempt.getTrackingUrl();
          origTrackingUrl = this.currentAttempt.getOriginalTrackingUrl();
          if (UserGroupInformation.isSecurityEnabled()) {
            // get a token so the client can communicate with the app attempt
            // NOTE: token may be unavailable if the attempt is not running
            Token<ClientToAMTokenIdentifier> attemptClientToAMToken =
                this.currentAttempt.createClientToken(clientUserName);
            if (attemptClientToAMToken != null) {
              clientToAMToken = BuilderUtils
                  .newClientToAMToken(attemptClientToAMToken.getIdentifier(),
                      attemptClientToAMToken.getKind().toString(),
                      attemptClientToAMToken.getPassword(),
                      attemptClientToAMToken.getService().toString());
            }
          }
          host = this.currentAttempt.getHost();
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
          progress = currentAttempt.getProgress();
        }
        diags = this.diagnostics.toString();

        if (currentAttempt != null &&
            currentAttempt.getState() == RMAppAttemptState.LAUNCHED) {
          if (getApplicationSubmissionContext().getUnmanagedAM() &&
              clientUserName != null && getUser().equals(clientUserName)) {
            Token<AMRMTokenIdentifier> token = currentAttempt.getAMRMToken();
            if (token != null) {
              amrmToken = BuilderUtils.newAMRMToken(token.getIdentifier(),
                  token.getKind().toString(), token.getPassword(),
                  token.getService().toString());
            }
          }
        }
      }

      if (currentApplicationAttemptId == null) {
        currentApplicationAttemptId = BuilderUtils
            .newApplicationAttemptId(this.applicationId,
                DUMMY_APPLICATION_ATTEMPT_NUMBER);
      }

      return BuilderUtils
          .newApplicationReport(this.applicationId, currentApplicationAttemptId,
              this.user, this.queue, this.name, host, rpcPort, clientToAMToken,
              createApplicationState(), diags, trackingUrl, this.startTime,
              this.finishTime, finishState, appUsageReport, origTrackingUrl,
              progress, this.applicationType, amrmToken, applicationTags);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    this.readLock.lock();

    try {
      return this.finishTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getStartTime() {
    this.readLock.lock();

    try {
      return this.startTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getSubmitTime() {
    return this.submitTime;
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();

    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getTrackingUrl();
      }
      return null;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public StringBuilder getDiagnostics() {
    this.readLock.lock();

    try {
      return this.diagnostics;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getMaxAppAttempts() {
    return this.maxAppAttempts;
  }

  @Override
  public void handle(RMAppEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId appID = event.getApplicationId();
      LOG.debug(
          "Processing event for " + appID + " of type " + event.getType());
      final RMAppState oldState = getState();
      try {
        /*
         * keep the master in sync with the state machine
         */
        this.stateMachine.doTransition(event.getType(), event);
        if (event.getTransactionState() != null) {          
          ((TransactionStateImpl) event.getTransactionState()).
              addApplicationToAdd(this);
        }
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /*
         * TODO fail the application on the failed transition
         */
      }

      if (oldState != getState()) {
        LOG.info(
            appID + " State change from " + oldState + " to " + getState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) throws IOException {
    ApplicationState appState =
        state.getApplicationState().get(getApplicationId());
    this.recoveredFinalState = appState.getState();
    if (recoveredFinalState.equals(RMAppState.KILLING)) {
      this.stateBeforeKilling = appState.getStateBeforeKilling();
    }
    LOG.info("Recovering app: " + getApplicationId() + " with " +
        +appState.getAttemptCount() + " attempts and final state = " +
        this.recoveredFinalState);
    this.diagnostics.append(appState.getDiagnostics());
    this.storedFinishTime = appState.getFinishTime();
    this.startTime = appState.getStartTime();
    this.finishTime = appState.getFinishTime();
    this.stateMachine.setCurrentState(appState.getState());
    for (int i = 0; i < appState.getAttemptCount(); ++i) {
      // create attempt
      createNewAttempt(null);
      ((RMAppAttemptImpl) this.currentAttempt).recover(state);
    }

    for (NodeId nodeId : appState.getUpdatedNodes()) {
      this.updatedNodes.add(rmContext.getActiveRMNodes().get(nodeId));
    }
    if (getState().equals(RMAppState.FAILED) ||
        getState().equals(RMAppState.FINISHED) ||
        getState().equals(RMAppState.KILLED)) {
      this.handler.handle(new RMAppManagerEvent(applicationId,
              RMAppManagerEventType.APP_COMPLETED, null));
    }
  }

  private void createNewAttempt(TransactionState ts) {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);
    RMAppAttempt attempt =
        new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,
            submissionContext, conf, maxAppAttempts == attempts.size());
    attempts.put(appAttemptId, attempt);
    if (ts != null) {
      ((TransactionStateImpl) ts).addAppAttempt(attempt);
    }
    currentAttempt = attempt;
  }

  private void createAndStartNewAttempt(
      boolean transferStateFromPreviousAttempt,
      TransactionState transactionState) {
    createNewAttempt(transactionState);
    handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(),
        transferStateFromPreviousAttempt, transactionState));
  }

  private void processNodeUpdate(RMAppNodeUpdateType type, RMNode node, TransactionState ts) {
    NodeState nodeState = node.getState();
    updatedNodes.add(node);
    if (ts != null) {
      ((TransactionStateImpl) ts).addUpdatedNodeToAdd(applicationId, node);
    }
    LOG.debug("Received node update event:" + type + " for node:" + node +
        " with state:" + nodeState);
  }

  private static class RMAppTransition
      implements SingleArcTransition<RMAppImpl, RMAppEvent> {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
    }

    ;

  }

  private static final class RMAppNodeUpdateTransition extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent) event;
      app.processNodeUpdate(nodeUpdateEvent.getUpdateType(),
          nodeUpdateEvent.getNode(), event.getTransactionState());
    }

    ;

  }

  /**
   * Move an app to a new queue.
   * This transition must set the result on the Future in the RMAppMoveEvent,
   * either as an exception for failure or null for success, or the client will
   * be left waiting forever.
   */
  private static final class RMAppMoveTransition extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppMoveEvent moveEvent = (RMAppMoveEvent) event;
      try {
        app.queue = app.scheduler
            .moveApplication(app.applicationId, moveEvent.getTargetQueue());
      } catch (YarnException ex) {
        moveEvent.getResult().setException(ex);
        return;
      }

      // TODO: Write out change to state store (YARN-1558)
      // Also take care of RM failover
      moveEvent.getResult().set(null);
    }
  }

  private static final class AddApplicationToSchedulerTransition
      extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (event instanceof RMAppNewSavedEvent) {
        RMAppNewSavedEvent storeEvent = (RMAppNewSavedEvent) event;
        // For HA this exception needs to be handled by giving up
        // master status if we got fenced
        if (((RMAppNewSavedEvent) event).getStoredException() != null) {
          LOG.error("Failed to store application: " + storeEvent.
                  getApplicationId(), storeEvent.getStoredException());
          ExitUtil.terminate(1, storeEvent.getStoredException());
        }
      }
      app.handler.handle(new AppAddedSchedulerEvent(app.applicationId,
          app.submissionContext.getQueue(), app.user, event.
          getTransactionState()));
    }
  }

  private static final class StartAppAttemptTransition extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.createAndStartNewAttempt(false, event.getTransactionState());
    }

    ;

  }

  private String getAppAttemptFailedDiagnostics(RMAppEvent event) {
    String msg = null;
    RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
    if (this.submissionContext.getUnmanagedAM()) {
      // RM does not manage the AM. Do not retry
      msg = "Unmanaged application " + this.getApplicationId() +
          " failed due to " + failedEvent.getDiagnostics() +
          ". Failing the application.";
    } else if (this.attempts.size() >= this.maxAppAttempts) {
      msg = "Application " + this.getApplicationId() + " failed " +
          this.maxAppAttempts + " times due to " +
          failedEvent.getDiagnostics() + ". Failing the application.";
    }
    return msg;
  }

  private static class AttemptUnregisteredTransition extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.finishTime = app.storedFinishTime;
    }
  }

  private static class AppFinishedTransition extends FinalTransition {

    public AppFinishedTransition() {
      super(RMAppState.FINISHED);
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppFinishedAttemptEvent finishedEvent =
          (RMAppFinishedAttemptEvent) event;
      app.diagnostics.append(finishedEvent.getDiagnostics());
      super.transition(app, event);
    }

    ;

  }

  private static class AppFinishedFinalStateSavedTransition
      extends RMAppTransition {

    RMAppEvent attemptUnregistered;

    public AppFinishedFinalStateSavedTransition(
        RMAppEvent attemptUnregistered) {
      this.attemptUnregistered = attemptUnregistered;
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      new AttemptUnregisteredTransition().transition(app, attemptUnregistered);
      FINISHED_TRANSITION.transition(app, event);
    }

    ;

  }


  private static class AppKilledTransition extends FinalTransition {

    public AppKilledTransition() {
      super(RMAppState.KILLED);
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append(getAppKilledDiagnostics());
      super.transition(app, event);
    }

    ;

  }

  private static String getAppKilledDiagnostics() {
    return "Application killed by user.";
  }

  private static class KillAttemptTransition extends RMAppTransition {

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.stateBeforeKilling = app.getState();
      app.handler.handle(
          new RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
              RMAppAttemptEventType.KILL, event.
              getTransactionState()));
    }
  }

  private static final class AppRejectedTransition extends FinalTransition {

    public AppRejectedTransition() {
      super(RMAppState.FAILED);
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent) event;
      app.diagnostics.append(rejectedEvent.getMessage());
      super.transition(app, event);
    }

    ;

  }

  private static class FinalTransition extends RMAppTransition {

    private final RMAppState finalState;

    public FinalTransition(RMAppState finalState) {
      this.finalState = finalState;
    }

    private Set<NodeId> getNodesOnWhichAttemptRan(RMAppImpl app) {
      Set<NodeId> nodes = new HashSet<NodeId>();
      for (RMAppAttempt attempt : app.attempts.values()) {
        nodes.addAll(attempt.getRanNodes());
      }
      return nodes;
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
      for (NodeId nodeId : nodes) {
        app.handler
            .handle(new RMNodeCleanAppEvent(nodeId, app.applicationId, event.
                    getTransactionState()));
      }
      app.finishTime = app.storedFinishTime;
      if (app.finishTime == 0) {
        app.finishTime = System.currentTimeMillis();
      }
      app.handler.handle(
          new AppRemovedSchedulerEvent(app.applicationId, finalState,
              event.getTransactionState()));
      app.handler.handle(new RMAppManagerEvent(app.applicationId,
              RMAppManagerEventType.APP_COMPLETED, event.
              getTransactionState()));

      app.rmContext.getRMApplicationHistoryWriter()
          .applicationFinished(app, finalState, event.getTransactionState());
    }

    ;

  }

  private static final class AttemptFailedTransition
      implements MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    private final RMAppState initialState;

    public AttemptFailedTransition(RMAppState initialState) {
      this.initialState = initialState;
    }

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {
      if (!app.submissionContext.getUnmanagedAM() &&
          app.attempts.size() < app.maxAppAttempts) {
        boolean transferStateFromPreviousAttempt;
        RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
        transferStateFromPreviousAttempt =
            failedEvent.getTransferStateFromPreviousAttempt();

        RMAppAttempt oldAttempt = app.currentAttempt;
        app.createAndStartNewAttempt(transferStateFromPreviousAttempt, event.
            getTransactionState());
        // Transfer the state from the previous attempt to the current attempt.
        // Note that the previous failed attempt may still be collecting the
        // container events from the scheduler and update its data structures
        // before the new attempt is created.
        if (transferStateFromPreviousAttempt) {
          ((RMAppAttemptImpl) app.currentAttempt)
              .transferStateFromPreviousAttempt(oldAttempt, event.
                  getTransactionState());
        }
        return initialState;
      } else {

        String msg = app.getAppAttemptFailedDiagnostics(event);

        LOG.info(msg);
        app.diagnostics.append(msg);
        // Inform the node for app-finish
        new FinalTransition(RMAppState.FAILED).transition(app, event);

        return RMAppState.FAILED;
      }
    }
  }

  @Override
  public String getApplicationType() {
    return this.applicationType;
  }

  @Override
  public Set<String> getApplicationTags() {
    return this.applicationTags;
  }

  @Override
  public boolean isAppFinalStateStored() {
    RMAppState state = getState();
    return state.equals(RMAppState.FINISHING) ||
        state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED) ||
        state.equals(RMAppState.KILLED);
  }

  @Override
  public YarnApplicationState createApplicationState() {
    RMAppState rmAppState = getState();
    // If App is in FINAL_SAVING state, return its previous state.

    if (rmAppState.equals(RMAppState.KILLING)) {
      rmAppState = stateBeforeKilling;
    }
    return RMServerUtils.createApplicationState(rmAppState);
  }

  public static boolean isAppInFinalState(RMApp rmApp) {
    RMAppState appState = ((RMAppImpl) rmApp).getRecoveredFinalState();
    return appState == RMAppState.FAILED || appState == RMAppState.FINISHED ||
        appState == RMAppState.KILLED;
  }

  private RMAppState getRecoveredFinalState() {
    return this.recoveredFinalState;
  }

  public RMAppState getStateBeforeKilling() {
    return stateBeforeKilling;
  }

  public List<NodeId> getUpdatedNodesId() {
    List<NodeId> updatedNodesId = new ArrayList<NodeId>();
    for (RMNode node : updatedNodes) {
      updatedNodesId.add(node.getNodeID());
    }
    return updatedNodesId;
  }

}
