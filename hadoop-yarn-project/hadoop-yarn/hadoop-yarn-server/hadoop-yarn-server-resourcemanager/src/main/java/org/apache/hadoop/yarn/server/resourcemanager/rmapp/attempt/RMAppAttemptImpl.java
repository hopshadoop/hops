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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import com.google.common.annotations.VisibleForTesting;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFinishedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME;
import static org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.AM_RM_TOKEN_SERVICE;
import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMAppAttemptImpl implements RMAppAttempt, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppAttemptImpl.class);

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY =
      recordFactory.newRecordInstance(Priority.class);

  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private final StateMachine<RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent>
      stateMachine; //recovered

  private final RMContext rmContext;//recovered
  private final EventHandler eventHandler;//recovered
  private final YarnScheduler scheduler;//recovered
  private final ApplicationMasterService masterService;//recovered

  private final ReadLock readLock;//recovered
  private final WriteLock writeLock;//recovered

  private final ApplicationAttemptId applicationAttemptId;//recovered
  private final ApplicationSubmissionContext submissionContext;//recovered
  private Token<AMRMTokenIdentifier> amrmToken = null;//recovered
  private SecretKey clientTokenMasterKey = null;//recovered

  //nodes on while this attempt's containers ran
  private Set<NodeId> ranNodes = new HashSet<NodeId>();//recovered
  private List<ContainerStatus> justFinishedContainers =
      new ArrayList<ContainerStatus>();//recovered
  private Container masterContainer;//recovered

  private float progress = 0;//recovered
  private String host = "N/A";//recovered
  private int rpcPort = -1;//recovered
  private String originalTrackingUrl = "N/A";//recovered
  private String proxiedTrackingUrl = "N/A";//recovered
  private long startTime = 0;//recovered

  // Set to null initially. Will eventually get set 
  // if an RMAppAttemptUnregistrationEvent occurs
  private FinalApplicationStatus finalStatus = null;//recovered
  private final StringBuilder diagnostics = new StringBuilder();//recovered

  private final Configuration conf;//recovered
  private final boolean isLastAttempt;//recovered
  private static final ExpiredTransition EXPIRED_TRANSITION =
      new ExpiredTransition();

  private static final StateMachineFactory<RMAppAttemptImpl, RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent>
      stateMachineFactory =
      new StateMachineFactory<RMAppAttemptImpl, RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent>(
          RMAppAttemptState.NEW)
          // Transitions from NEW State
          .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.SUBMITTED,
              RMAppAttemptEventType.START, new AttemptStartedTransition())
          .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.KILLED,
              RMAppAttemptEventType.KILL,
              new BaseFinalTransition(RMAppAttemptState.KILLED)).addTransition(
          RMAppAttemptState.NEW, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.REGISTERED,
          new UnexpectedAMRegisteredTransition())
          // Transitions from SUBMITED State
          .addTransition(RMAppAttemptState.SUBMITTED, EnumSet
                  .of(RMAppAttemptState.LAUNCHED, RMAppAttemptState.SCHEDULED),
              RMAppAttemptEventType.ATTEMPT_ADDED, new ScheduleTransition())
          .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.KILLED,
              RMAppAttemptEventType.KILL,
              new BaseFinalTransition(RMAppAttemptState.KILLED)).addTransition(
          RMAppAttemptState.SUBMITTED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.REGISTERED,
          new UnexpectedAMRegisteredTransition())
          // Transitions from SCHEDULED State
          .addTransition(RMAppAttemptState.SCHEDULED, EnumSet
                  .of(RMAppAttemptState.ALLOCATED, RMAppAttemptState.SCHEDULED),
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              new AMContainerAllocatedTransition()).addTransition(
          RMAppAttemptState.SCHEDULED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))
          // Transitions from ALLOCATED State
          .addTransition(RMAppAttemptState.ALLOCATED,
              RMAppAttemptState.ALLOCATED,
              RMAppAttemptEventType.CONTAINER_ACQUIRED,
              new ContainerAcquiredTransition())
          .addTransition(RMAppAttemptState.ALLOCATED,
              RMAppAttemptState.LAUNCHED, RMAppAttemptEventType.LAUNCHED,
              new AMLaunchedTransition())
          .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FAILED,
              RMAppAttemptEventType.LAUNCH_FAILED, new LaunchFailedTransition())
          .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.KILLED,
              RMAppAttemptEventType.KILL,
              new KillAllocatedAMTransition()).addTransition(
          RMAppAttemptState.ALLOCATED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new AMContainerCrashedTransition())
          // Transitions from LAUNCHED State
          .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.RUNNING,
              RMAppAttemptEventType.REGISTERED, new AMRegisteredTransition()).
          addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              new AMContainerCrashedTransition())
          .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
              RMAppAttemptEventType.EXPIRE, EXPIRED_TRANSITION).addTransition(
          RMAppAttemptState.LAUNCHED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))
          // Transitions from RUNNING State
          .addTransition(RMAppAttemptState.RUNNING, EnumSet
                  .of(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED),
              RMAppAttemptEventType.UNREGISTERED,
              new AMUnregisteredTransition())
          .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
              RMAppAttemptEventType.STATUS_UPDATE, new StatusUpdateTransition())
          .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
              RMAppAttemptEventType.CONTAINER_ALLOCATED)
          .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
              RMAppAttemptEventType.CONTAINER_ACQUIRED,
              new ContainerAcquiredTransition())
          .addTransition(RMAppAttemptState.RUNNING, EnumSet.
                  of(RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED),
              RMAppAttemptEventType.CONTAINER_FINISHED,
              new ContainerFinishedTransition())
          .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED,
              RMAppAttemptEventType.EXPIRE, EXPIRED_TRANSITION).addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))
          // Transitions from FAILED State
          // For work-preserving AM restart, failed attempt are still capturing
          // CONTAINER_FINISHED event and record the finished containers for the
          // use by the next new attempt.
          .addTransition(RMAppAttemptState.FAILED, RMAppAttemptState.FAILED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              new ContainerFinishedAtFailedTransition()).addTransition(
          RMAppAttemptState.FAILED, RMAppAttemptState.FAILED, EnumSet
              .of(RMAppAttemptEventType.EXPIRE, RMAppAttemptEventType.KILL,
                  RMAppAttemptEventType.UNREGISTERED,
                  RMAppAttemptEventType.STATUS_UPDATE,
                  RMAppAttemptEventType.CONTAINER_ALLOCATED))
          // Transitions from FINISHING State
          .addTransition(RMAppAttemptState.FINISHING, EnumSet
                  .of(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED),
              RMAppAttemptEventType.CONTAINER_FINISHED,
              new AMFinishingContainerFinishedTransition())
          .addTransition(RMAppAttemptState.FINISHING,
              RMAppAttemptState.FINISHED, RMAppAttemptEventType.EXPIRE,
              new FinalTransition(RMAppAttemptState.FINISHED)).addTransition(
          RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHING, EnumSet
              .of(RMAppAttemptEventType.UNREGISTERED,
                  RMAppAttemptEventType.STATUS_UPDATE,
                  RMAppAttemptEventType.CONTAINER_ALLOCATED,
                  // ignore Kill as we have already saved the final Finished state in
                  // state store.
                  RMAppAttemptEventType.KILL))
          // Transitions from FINISHED State
          .addTransition(RMAppAttemptState.FINISHED, RMAppAttemptState.FINISHED,
              EnumSet.of(RMAppAttemptEventType.EXPIRE,
                  RMAppAttemptEventType.UNREGISTERED,
                  RMAppAttemptEventType.CONTAINER_ALLOCATED,
                  RMAppAttemptEventType.CONTAINER_FINISHED,
                  RMAppAttemptEventType.KILL))
              // Transitions from KILLED State
          .addTransition(RMAppAttemptState.KILLED, RMAppAttemptState.KILLED,
              EnumSet.of(RMAppAttemptEventType.ATTEMPT_ADDED,
                  RMAppAttemptEventType.EXPIRE, RMAppAttemptEventType.LAUNCHED,
                  RMAppAttemptEventType.LAUNCH_FAILED,
                  RMAppAttemptEventType.EXPIRE,
                  RMAppAttemptEventType.REGISTERED,
                  RMAppAttemptEventType.CONTAINER_ALLOCATED,
                  RMAppAttemptEventType.CONTAINER_FINISHED,
                  RMAppAttemptEventType.UNREGISTERED,
                  RMAppAttemptEventType.KILL,
                  RMAppAttemptEventType.STATUS_UPDATE)).installTopology();

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      RMContext rmContext, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationSubmissionContext submissionContext, Configuration conf,
      boolean isLastAttempt) {
    this.conf = conf;
    this.applicationAttemptId = appAttemptId;
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.proxiedTrackingUrl = generateProxyUriWithScheme(null);
    this.isLastAttempt = isLastAttempt;
    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public ApplicationAttemptId getAppAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  public ApplicationSubmissionContext getSubmissionContext() {
    return this.submissionContext;
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      return this.finalStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getHost() {
    this.readLock.lock();

    try {
      return this.host;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getRpcPort() {
    this.readLock.lock();

    try {
      return this.rpcPort;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();
    try {
      return (getSubmissionContext().getUnmanagedAM()) ?
          this.originalTrackingUrl : this.proxiedTrackingUrl;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getOriginalTrackingUrl() {
    this.readLock.lock();
    try {
      return this.originalTrackingUrl;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getWebProxyBase() {
    this.readLock.lock();
    try {
      return ProxyUriUtils.getPath(applicationAttemptId.getApplicationId());
    } finally {
      this.readLock.unlock();
    }
  }

  private String generateProxyUriWithScheme(
      final String trackingUriWithoutScheme) {
    this.readLock.lock();
    try {
      final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
      URI trackingUri = StringUtils.isEmpty(trackingUriWithoutScheme) ? null :
          ProxyUriUtils.getUriFromAMUrl(scheme, trackingUriWithoutScheme);
      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme, proxy);
      URI result = ProxyUriUtils.getProxyUri(trackingUri, proxyUri,
          applicationAttemptId.getApplicationId());
      return result.toASCIIString();
    } catch (URISyntaxException e) {
      LOG.warn("Could not proxify " + trackingUriWithoutScheme, e);
      return trackingUriWithoutScheme;
    } finally {
      this.readLock.unlock();
    }
  }

  private void setTrackingUrlToRMAppPage() {
    originalTrackingUrl =
        pjoin(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf), "cluster",
            "app", getAppAttemptId().getApplicationId());
    proxiedTrackingUrl = originalTrackingUrl;
  }

  private void invalidateAMHostAndPort() {
    this.host = "N/A";
    this.rpcPort = -1;
  }

  // This is only used for RMStateStore. Normal operation must invoke the secret
  // manager to get the key and not use the local key directly.
  @Override
  public SecretKey getClientTokenMasterKey() {
    return this.clientTokenMasterKey;
  }

  @Override
  public Token<AMRMTokenIdentifier> getAMRMToken() {
    return this.amrmToken;
  }

  @Override
  public Token<ClientToAMTokenIdentifier> createClientToken(String client) {
    this.readLock.lock();

    try {
      Token<ClientToAMTokenIdentifier> token = null;
      ClientToAMTokenSecretManagerInRM secretMgr =
          this.rmContext.getClientToAMTokenSecretManager();
      if (client != null &&
          secretMgr.getMasterKey(this.applicationAttemptId) != null) {
        token = new Token<ClientToAMTokenIdentifier>(
            new ClientToAMTokenIdentifier(this.applicationAttemptId, client),
            secretMgr);
      }
      return token;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getDiagnostics() {
    this.readLock.lock();

    try {
      return this.diagnostics.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      return this.progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> getJustFinishedContainers() {
    this.readLock.lock();
    try {
      return this.justFinishedContainers;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> pullJustFinishedContainers(TransactionState ts) {
    this.writeLock.lock();

    try {
      List<ContainerStatus> returnList =
          new ArrayList<ContainerStatus>(this.justFinishedContainers.size());
      returnList.addAll(this.justFinishedContainers);
      this.justFinishedContainers.clear();
      if (ts != null) {
        ((TransactionStateImpl) ts).addAppAttempt(this);
      }
      return returnList;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Set<NodeId> getRanNodes() {
    return ranNodes;
  }

  @Override
  public Container getMasterContainer() {
    this.readLock.lock();

    try {
      return this.masterContainer;
    } finally {
      this.readLock.unlock();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public void setMasterContainer(Container container) {
    masterContainer = container;
  }

  @Override
  public void handle(RMAppAttemptEvent event) {

    this.writeLock.lock();

    try {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      LOG.debug("Processing event for " + appAttemptID + " of type " +
          event.getType() + " current state " + this.stateMachine.
          getCurrentState() + " " + event.getTransactionState());
      final RMAppAttemptState oldState = getState();
      try {
        /*
         * keep the master in sync with the state machine
         */
        this.stateMachine.doTransition(event.getType(), event);
        if (event.getTransactionState() != null) {
          ((TransactionStateImpl) event.getTransactionState()).
              addAppAttempt(this);
        }
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /*
         * TODO fail the application on the failed transition
         */
      }

      if (oldState != getState()) {
        LOG.info(appAttemptID + " State change from " + oldState + " to " +
            getState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ApplicationResourceUsageReport getApplicationResourceUsageReport() {
    this.readLock.lock();
    try {
      ApplicationResourceUsageReport report =
          scheduler.getAppResourceUsageReport(this.getAppAttemptId());
      if (report == null) {
        Resource none = Resource.newInstance(0, 0);
        report =
            ApplicationResourceUsageReport.newInstance(0, 0, none, none, none);
      }
      return report;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) throws Exception {
    ApplicationState appState =
        state.getApplicationState().get(getAppAttemptId().
            getApplicationId());
    ApplicationAttemptState attemptState =
        appState.getAttempt(getAppAttemptId());
    assert attemptState != null;
    LOG.info(
        "Recovering attempt: " + getAppAttemptId() + " with final state: " +
            attemptState.getState());
    diagnostics.append("Attempt recovered after RM restart");
    diagnostics.append(attemptState.getDiagnostics());
    setMasterContainer(attemptState.getMasterContainer());
    recoverAppAttemptCredentials(attemptState.getAppAttemptCredentials());
    this.originalTrackingUrl = attemptState.getFinalTrackingUrl();
    this.proxiedTrackingUrl = generateProxyUriWithScheme(originalTrackingUrl);
    this.finalStatus = attemptState.getFinalApplicationStatus();
    this.startTime = attemptState.getStartTime();
    this.progress = attemptState.getProgress();
    this.host = attemptState.getHost();
    this.rpcPort = attemptState.getRpcPort();
    this.ranNodes = attemptState.getRanNodes();
    this.justFinishedContainers = attemptState.getJustFinishedContainers();
    this.stateMachine.setCurrentState(attemptState.getState());
    switch (getState()) {
      case LAUNCHED:
      case RUNNING:
        this.rmContext.getAMLivelinessMonitor().register(applicationAttemptId);
      case NEW:
      case SUBMITTED:
      case SCHEDULED:
      case ALLOCATED:
        this.masterService.registerAppAttempt(applicationAttemptId, null);
        this.masterService.recoverAllocateResponse(applicationAttemptId, state.
            getAllocateResponses().get(applicationAttemptId));
        break;
      case FINISHING:
        this.rmContext.getAMFinishingMonitor().register(applicationAttemptId);
      case FINISHED:
      case KILLED:
      case FAILED:
        break;
      default:
        LOG.error("recovering an unexisting state " + getState());
    }

  }

  public void transferStateFromPreviousAttempt(RMAppAttempt attempt,
      TransactionState ts) {
    this.justFinishedContainers = attempt.getJustFinishedContainers();
    this.ranNodes = attempt.getRanNodes();
    ApplicationAttemptState appAttemptState =
        new ApplicationAttemptState(this.applicationAttemptId, masterContainer,
            getCredentials(), startTime, this.stateMachine.getCurrentState(),
            originalTrackingUrl, "", finalStatus, progress, host, rpcPort,
            ranNodes, justFinishedContainers);
    ((TransactionStateImpl) ts).addAppAttempt(this);
  }

  private void recoverAppAttemptCredentials(Credentials appAttemptTokens)
      throws IOException {
    if (appAttemptTokens == null) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      byte[] clientTokenMasterKeyBytes = appAttemptTokens
          .getSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME);
      clientTokenMasterKey = rmContext.getClientToAMTokenSecretManager()
          .registerMasterKey(applicationAttemptId, clientTokenMasterKeyBytes);
    }

    // Only one AMRMToken is stored per-attempt, so this should be fine. Can't
    // use TokenSelector as service may change - think fail-over.
    this.amrmToken = (Token<AMRMTokenIdentifier>) appAttemptTokens
        .getToken(RMStateStore.AM_RM_TOKEN_SERVICE);
    rmContext.getAMRMTokenSecretManager().addPersistedPassword(this.amrmToken);
  }

  private static class BaseTransition
      implements SingleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent> {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
    }

  }

  private static final class AttemptStartedTransition extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      boolean transferStateFromPreviousAttempt = false;
      if (event instanceof RMAppStartAttemptEvent) {
        transferStateFromPreviousAttempt = ((RMAppStartAttemptEvent) event)
            .getTransferStateFromPreviousAttempt();
      }
      appAttempt.startTime = System.currentTimeMillis();

      // Register with the ApplicationMasterService
      appAttempt.masterService
          .registerAppAttempt(appAttempt.applicationAttemptId, event.
              getTransactionState());

      if (UserGroupInformation.isSecurityEnabled()) {
        appAttempt.clientTokenMasterKey =
            appAttempt.rmContext.getClientToAMTokenSecretManager()
                .createMasterKey(appAttempt.applicationAttemptId);
      }

      // create AMRMToken
      AMRMTokenIdentifier id =
          new AMRMTokenIdentifier(appAttempt.applicationAttemptId);
      appAttempt.amrmToken = new Token<AMRMTokenIdentifier>(id,
          appAttempt.rmContext.getAMRMTokenSecretManager());

      // Add the applicationAttempt to the scheduler and inform the scheduler
      // whether to transfer the state from previous attempt.
      appAttempt.eventHandler.handle(
          new AppAttemptAddedSchedulerEvent(appAttempt.applicationAttemptId,
              transferStateFromPreviousAttempt, event.getTransactionState()));
    }
  }

  private static final List<ContainerId> EMPTY_CONTAINER_RELEASE_LIST =
      new ArrayList<ContainerId>();

  private static final List<ResourceRequest> EMPTY_CONTAINER_REQUEST_LIST =
      new ArrayList<ResourceRequest>();

  private static final class ScheduleTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      if (!appAttempt.submissionContext.getUnmanagedAM()) {
        // Request a container for the AM.
        ResourceRequest request = BuilderUtils
            .newResourceRequest(AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                appAttempt.getSubmissionContext().getResource(), 1);

        // SchedulerUtils.validateResourceRequests is not necessary because
        // AM resource has been checked when submission
        LOG.debug("allocate container for AM");
        Allocation amContainerAllocation = appAttempt.scheduler
            .allocate(appAttempt.applicationAttemptId,
                Collections.singletonList(request),
                EMPTY_CONTAINER_RELEASE_LIST, null, null,
                event.getTransactionState());
        if (amContainerAllocation != null &&
            amContainerAllocation.getContainers() != null) {
          assert (amContainerAllocation.getContainers().isEmpty());
        }
        return RMAppAttemptState.SCHEDULED;
      } else {
        // save state and then go to LAUNCHED state
        new AMLaunchedTransition().transition(appAttempt, event);
        return RMAppAttemptState.LAUNCHED;
      }
    }
  }

  private static final class AMContainerAllocatedTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Acquire the AM container from the scheduler.
      LOG.debug("allocate container for AM");
      Allocation amContainerAllocation = appAttempt.scheduler
          .allocate(appAttempt.applicationAttemptId,
              EMPTY_CONTAINER_REQUEST_LIST, EMPTY_CONTAINER_RELEASE_LIST, null,
              null, event.getTransactionState());
      // There must be at least one container allocated, because a
      // CONTAINER_ALLOCATED is emitted after an RMContainer is constructed,
      // and is put in SchedulerApplication#newlyAllocatedContainers.

      // Note that YarnScheduler#allocate is not guaranteed to be able to
      // fetch it since container may not be fetchable for some reason like
      // DNS unavailable causing container token not generated. As such, we
      // return to the previous state and keep retry until am container is
      // fetched.
      if (amContainerAllocation.getContainers().isEmpty()) {
        appAttempt.retryFetchingAMContainer(appAttempt, event.
            getTransactionState());
        return RMAppAttemptState.SCHEDULED;
      }

      // Set the masterContainer
      appAttempt
          .setMasterContainer(amContainerAllocation.getContainers().get(0));
      // The node set in NMTokenSecrentManager is used for marking whether the
      // NMToken has been issued for this node to the AM.
      // When AM container was allocated to RM itself, the node which allocates
      // this AM container was marked as the NMToken already sent. Thus,
      // clear this node set so that the following allocate requests from AM are
      // able to retrieve the corresponding NMToken.
      appAttempt.rmContext.getNMTokenSecretManager()
          .clearNodeSetForAttempt(appAttempt.applicationAttemptId);
      appAttempt.getSubmissionContext()
          .setResource(appAttempt.getMasterContainer().getResource());
      appAttempt.launchAttempt(event.getTransactionState());
      return RMAppAttemptState.ALLOCATED;
    }
  }

  private void retryFetchingAMContainer(final RMAppAttemptImpl appAttempt,
      final TransactionState transactionState) {
    // start a new thread so that we are not blocking main dispatcher thread.
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting to resend the" +
              " ContainerAllocated Event.");
        }
        appAttempt.eventHandler.handle(new RMAppAttemptContainerAllocatedEvent(
            appAttempt.applicationAttemptId, transactionState));
      }
    }.start();
  }

  private static class BaseFinalTransition extends BaseTransition {

    private final RMAppAttemptState finalAttemptState;

    public BaseFinalTransition(RMAppAttemptState finalAttemptState) {
      this.finalAttemptState = finalAttemptState;
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptId = appAttempt.getAppAttemptId();

      // Tell the AMS. Unregister from the ApplicationMasterService
      appAttempt.masterService
          .unregisterAttempt(appAttemptId, event.getTransactionState());

      // Tell the application and the scheduler
      ApplicationId applicationId = appAttemptId.getApplicationId();
      RMAppEvent appEvent = null;
      boolean keepContainersAcrossAppAttempts = false;
      switch (finalAttemptState) {
        case FINISHED: {
          appEvent = new RMAppFinishedAttemptEvent(applicationId,
              appAttempt.getDiagnostics(), event.getTransactionState());
        }
        break;
        case KILLED: {
          // don't leave the tracking URL pointing to a non-existent AM
          appAttempt.setTrackingUrlToRMAppPage();
          appAttempt.invalidateAMHostAndPort();
          appEvent = new RMAppFailedAttemptEvent(applicationId,
              RMAppEventType.ATTEMPT_KILLED, "Application killed by user.",
              false, event.
              getTransactionState());
        }
        break;
        case FAILED: {
          // don't leave the tracking URL pointing to a non-existent AM
          appAttempt.setTrackingUrlToRMAppPage();
          appAttempt.invalidateAMHostAndPort();
          if (appAttempt.submissionContext
              .getKeepContainersAcrossApplicationAttempts() &&
              !appAttempt.isLastAttempt &&
              !appAttempt.submissionContext.getUnmanagedAM()) {
            keepContainersAcrossAppAttempts = true;
          }
          LOG.debug("BaseFinalTransition " + event.getType() + " " + event.
              getTransactionState());
          appEvent = new RMAppFailedAttemptEvent(applicationId,
              RMAppEventType.ATTEMPT_FAILED, appAttempt.
              getDiagnostics(), keepContainersAcrossAppAttempts, event.
              getTransactionState());

        }
        break;
        default: {
          LOG.error("Cannot get this state!! Error!!");
        }
        break;
      }

      appAttempt.eventHandler.handle(appEvent);
      //HOP: we do not remove the event anymore, the event should have been restored
      //and continue as if nothing had happen
      appAttempt.eventHandler.handle(
          new AppAttemptRemovedSchedulerEvent(appAttemptId, finalAttemptState,
              keepContainersAcrossAppAttempts, event.getTransactionState()));
      appAttempt.removeCredentials(appAttempt);

      appAttempt.rmContext.getRMApplicationHistoryWriter()
          .applicationAttemptFinished(appAttempt, finalAttemptState, event.
              getTransactionState());
    }
  }

  private static class AMLaunchedTransition extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Register with AMLivelinessMonitor
      appAttempt.attemptLaunched();

      // register the ClientTokenMasterKey after it is saved in the store,
      // otherwise client may hold an invalid ClientToken after RM restarts.
      appAttempt.rmContext.getClientToAMTokenSecretManager()
          .registerApplication(appAttempt.getAppAttemptId(),
              appAttempt.getClientTokenMasterKey());
    }
  }

  private static final class LaunchFailedTransition
      extends BaseFinalTransition {

    public LaunchFailedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Use diagnostic from launcher
      RMAppAttemptLaunchFailedEvent launchFaileEvent =
          (RMAppAttemptLaunchFailedEvent) event;
      appAttempt.diagnostics.append(launchFaileEvent.getMessage());

      // Tell the app, scheduler
      super.transition(appAttempt, event);

    }
  }

  private static final class KillAllocatedAMTransition
      extends BaseFinalTransition {

    public KillAllocatedAMTransition() {
      super(RMAppAttemptState.KILLED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Tell the application and scheduler
      super.transition(appAttempt, event);

      // Tell the launcher to cleanup.
      appAttempt.eventHandler.handle(
          new AMLauncherEvent(AMLauncherEventType.CLEANUP, appAttempt, event.
              getTransactionState()));

    }
  }

  private static final class AMRegisteredTransition extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptRegistrationEvent registrationEvent =
          (RMAppAttemptRegistrationEvent) event;
      appAttempt.host = registrationEvent.getHost();
      appAttempt.rpcPort = registrationEvent.getRpcport();
      appAttempt.originalTrackingUrl =
          sanitizeTrackingUrl(registrationEvent.getTrackingurl());
      appAttempt.proxiedTrackingUrl =
          appAttempt.generateProxyUriWithScheme(appAttempt.originalTrackingUrl);

      // Let the app know
      appAttempt.eventHandler.handle(
          new RMAppEvent(appAttempt.getAppAttemptId().getApplicationId(),
              RMAppEventType.ATTEMPT_REGISTERED, event.getTransactionState()));

      // TODO:FIXME: Note for future. Unfortunately we only do a state-store
      // write at AM launch time, so we don't save the AM's tracking URL anywhere
      // as that would mean an extra state-store write. For now, we hope that in
      // work-preserving restart, AMs are forced to reregister.
      appAttempt.rmContext.getRMApplicationHistoryWriter()
          .applicationAttemptStarted(appAttempt, event.getTransactionState());
    }
  }

  private static final class AMContainerCrashedTransition
      extends BaseFinalTransition {

    public AMContainerCrashedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent finishEvent =
          ((RMAppAttemptContainerFinishedEvent) event);

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor()
          .unregister(appAttempt.getAppAttemptId());

      // Setup diagnostic message
      appAttempt.diagnostics
          .append(getAMContainerCrashedDiagnostics(finishEvent));
      // Tell the app, scheduler
      super.transition(appAttempt, finishEvent);
    }
  }

  private static String getAMContainerCrashedDiagnostics(
      RMAppAttemptContainerFinishedEvent finishEvent) {
    ContainerStatus status = finishEvent.getContainerStatus();
    String diagnostics =
        "AM Container for " + finishEvent.getApplicationAttemptId() +
            " exited with " + " exitCode: " + status.getExitStatus() +
            " due to: " + status.getDiagnostics() + "." +
            "Failing this attempt.";
    return diagnostics;
  }

  private static class FinalTransition extends BaseFinalTransition {

    public FinalTransition(RMAppAttemptState finalAttemptState) {
      super(finalAttemptState);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      appAttempt.progress = 1.0f;

      // Tell the app and the scheduler
      super.transition(appAttempt, event);

      // UnRegister from AMLivelinessMonitor. Perhaps for
      // FAILING/KILLED/UnManaged AMs
      appAttempt.rmContext.getAMLivelinessMonitor()
          .unregister(appAttempt.getAppAttemptId());
      appAttempt.rmContext.getAMFinishingMonitor()
          .unregister(appAttempt.getAppAttemptId());

      if (!appAttempt.submissionContext.getUnmanagedAM()) {
        // Tell the launcher to cleanup.
        appAttempt.eventHandler.handle(
            new AMLauncherEvent(AMLauncherEventType.CLEANUP, appAttempt, event.
                getTransactionState()));
      }
    }
  }

  private static class ExpiredTransition extends FinalTransition {

    public ExpiredTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      appAttempt.diagnostics.append(getAMExpiredDiagnostics(event));
      super.transition(appAttempt, event);
    }
  }

  private static String getAMExpiredDiagnostics(RMAppAttemptEvent event) {
    String diag =
        "ApplicationMaster for attempt " + event.getApplicationAttemptId() +
            " timed out";
    return diag;
  }

  private static class UnexpectedAMRegisteredTransition
      extends BaseFinalTransition {

    public UnexpectedAMRegisteredTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      assert appAttempt.submissionContext.getUnmanagedAM();
      appAttempt.diagnostics.append(getUnexpectedAMRegisteredDiagnostics());
      super.transition(appAttempt, event);
    }

  }

  private static String getUnexpectedAMRegisteredDiagnostics() {
    return "Unmanaged AM must register after AM attempt reaches LAUNCHED state.";
  }

  private static final class StatusUpdateTransition extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptStatusupdateEvent statusUpdateEvent =
          (RMAppAttemptStatusupdateEvent) event;

      // Update progress
      appAttempt.progress = statusUpdateEvent.getProgress();

      // Ping to AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor()
          .receivedPing(statusUpdateEvent.getApplicationAttemptId());
    }
  }

  private static final class AMUnregisteredTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Tell the app
      if (appAttempt.getSubmissionContext().getUnmanagedAM()) {
        // Unmanaged AMs have no container to wait for, so they skip
        // the FINISHING state and go straight to FINISHED.
        appAttempt.updateInfoOnAMUnregister(event);
        new FinalTransition(RMAppAttemptState.FINISHED)
            .transition(appAttempt, event);
        return RMAppAttemptState.FINISHED;
      }
      // Saving the attempt final state
      new FinalStateSavedAfterAMUnregisterTransition()
          .transition(appAttempt, event);
      ApplicationId applicationId =
          appAttempt.getAppAttemptId().getApplicationId();

      // Tell the app immediately that AM is unregistering so that app itself
      // can save its state as soon as possible. Whether we do it like this, or
      // we wait till AppAttempt is saved, it doesn't make any difference on the
      // app side w.r.t failure conditions. The only event going out of
      // AppAttempt to App after this point of time is AM/AppAttempt Finished.
      appAttempt.eventHandler.handle(
          new RMAppEvent(applicationId, RMAppEventType.ATTEMPT_UNREGISTERED,
              event.getTransactionState()));
      return RMAppAttemptState.FINISHING;
    }
  }

  private static class FinalStateSavedAfterAMUnregisterTransition
      extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Unregister from the AMlivenessMonitor and register with AMFinishingMonitor
      appAttempt.rmContext.getAMLivelinessMonitor()
          .unregister(appAttempt.applicationAttemptId);
      appAttempt.rmContext.getAMFinishingMonitor()
          .register(appAttempt.applicationAttemptId);

      // Do not make any more changes to this transition code. Make all changes
      // to the following method. Unless you are absolutely sure that you have
      // stuff to do that shouldn't be used by the callers of the following
      // method.
      appAttempt.updateInfoOnAMUnregister(event);
    }
  }

  private void updateInfoOnAMUnregister(RMAppAttemptEvent event) {
    progress = 1.0f;
    RMAppAttemptUnregistrationEvent unregisterEvent =
        (RMAppAttemptUnregistrationEvent) event;
    diagnostics.append(unregisterEvent.getDiagnostics());
    originalTrackingUrl = sanitizeTrackingUrl(unregisterEvent.
        getFinalTrackingUrl());
    proxiedTrackingUrl = generateProxyUriWithScheme(originalTrackingUrl);
    finalStatus = unregisterEvent.getFinalApplicationStatus();
  }

  private static final class ContainerAcquiredTransition
      extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerAcquiredEvent acquiredEvent =
          (RMAppAttemptContainerAcquiredEvent) event;
      appAttempt.ranNodes.add(acquiredEvent.getContainer().getNodeId());
    }
  }

  private static final class ContainerFinishedTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the AmContainer? If the finished container is same as
      // the AMContainer, AppAttempt fails
      if (appAttempt.masterContainer != null &&
          appAttempt.masterContainer.getId()
              .equals(containerStatus.getContainerId())) {
        // Remember the follow up transition and save the final attempt state.
        new ContainerFinishedFinalStateSavedTransition()
            .transition(appAttempt, event);
        return RMAppAttemptState.FAILED;
      }

      // Normal container.Put it in completedcontainers list
      appAttempt.justFinishedContainers.add(containerStatus);
      return RMAppAttemptState.RUNNING;
    }
  }

  private static final class ContainerFinishedAtFailedTransition
      extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();
      // Normal container. Add it in completed containers list
      appAttempt.justFinishedContainers.add(containerStatus);
    }
  }

  private static class ContainerFinishedFinalStateSavedTransition
      extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      // container associated with AM. must not be unmanaged
      assert appAttempt.submissionContext.getUnmanagedAM() == false;
      // Setup diagnostic message
      appAttempt.diagnostics
          .append(getAMContainerCrashedDiagnostics(containerFinishedEvent));
      new FinalTransition(RMAppAttemptState.FAILED)
          .transition(appAttempt, event);
    }
  }

  private static final class AMFinishingContainerFinishedTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      LOG.debug("AMFinishingContainerFinishedTransition " + appAttempt.
          getAppAttemptId());
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the ApplicationMaster container?
      if (appAttempt.masterContainer.getId()
          .equals(containerStatus.getContainerId())) {
        new FinalTransition(RMAppAttemptState.FINISHED)
            .transition(appAttempt, containerFinishedEvent);
        return RMAppAttemptState.FINISHED;
      }
      // Normal container.
      appAttempt.justFinishedContainers.add(containerStatus);
      return RMAppAttemptState.FINISHING;
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
  public RMAppAttemptState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public YarnApplicationAttemptState createApplicationAttemptState() {
    RMAppAttemptState state = getState();
    // If AppAttempt is in FINAL_SAVING state, return its previous state.
    return RMServerUtils.createApplicationAttemptState(state);
  }

  private void launchAttempt(TransactionState transactionState) {
    // Send event to launch the AM Container
    eventHandler.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH, this,
        transactionState));
  }

  private void attemptLaunched() {
    // Register with AMLivelinessMonitor
    rmContext.getAMLivelinessMonitor().register(getAppAttemptId());
  }

  private void removeCredentials(RMAppAttemptImpl appAttempt) {
    // Unregister from the ClientToAMTokenSecretManager
    if (UserGroupInformation.isSecurityEnabled()) {
      appAttempt.rmContext.getClientToAMTokenSecretManager()
          .unRegisterApplication(appAttempt.getAppAttemptId());
    }

    // Remove the AppAttempt from the AMRMTokenSecretManager
    appAttempt.rmContext.getAMRMTokenSecretManager()
        .applicationMasterFinished(appAttempt.getAppAttemptId());
  }

  private static String sanitizeTrackingUrl(String url) {
    return (url == null || url.trim().isEmpty()) ? "N/A" : url;
  }

  @Override
  public ApplicationAttemptReport createApplicationAttemptReport() {
    this.readLock.lock();
    ApplicationAttemptReport attemptReport = null;
    try {
      // AM container maybe not yet allocated. and also unmangedAM doesn't have
      // am container.
      ContainerId amId =
          masterContainer == null ? null : masterContainer.getId();
      attemptReport = ApplicationAttemptReport
          .newInstance(this.getAppAttemptId(), this.getHost(),
              this.getRpcPort(), this.getTrackingUrl(), this.getDiagnostics(),
              YarnApplicationAttemptState.valueOf(this.getState().toString()),
              amId);
    } finally {
      this.readLock.unlock();
    }
    return attemptReport;
  }

  @Override
  public Credentials getCredentials() {
    Credentials credentials = new Credentials();
    Token<AMRMTokenIdentifier> appToken = getAMRMToken();
    if (appToken != null) {
      credentials.addToken(AM_RM_TOKEN_SERVICE, appToken);
    }
    SecretKey clientTokenMasterKey = getClientTokenMasterKey();
    if (clientTokenMasterKey != null) {
      credentials.addSecretKey(AM_CLIENT_TOKEN_MASTER_KEY_NAME,
          clientTokenMasterKey.getEncoded());
    }
    return credentials;
  }
}
