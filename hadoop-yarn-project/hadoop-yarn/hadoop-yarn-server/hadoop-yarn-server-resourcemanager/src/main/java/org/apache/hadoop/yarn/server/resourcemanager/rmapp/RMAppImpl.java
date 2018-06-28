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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateCryptoMaterialForAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppCertificateManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppCertificateManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.DisabledBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.SimpleBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppStartAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RMAppImpl implements RMApp, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // Immutable fields
  private final ApplicationId applicationId;
  private final RMContext rmContext;
  private final Configuration conf;
  private final String user;
  private final String name;
  private final ApplicationSubmissionContext submissionContext;
  private final Dispatcher dispatcher;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxAppAttempts;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
  private final long submitTime;
  private final Set<RMNode> updatedNodes = new HashSet<RMNode>();
  private final String applicationType;
  private final Set<String> applicationTags;

  private final long attemptFailuresValidityInterval;
  private boolean amBlacklistingEnabled = false;
  private float blacklistDisableThreshold;

  private Clock systemClock;

  private boolean isNumAttemptsBeyondThreshold = false;

  // Mutable fields
  private long startTime;
  private long finishTime = 0;
  private long storedFinishTime = 0;
  // This field isn't protected by readlock now.
  private volatile RMAppAttempt currentAttempt;
  private String queue;
  private EventHandler handler;
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();
  private Set<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

  private final boolean logAggregationEnabled;
  private long logAggregationStartTime = 0;
  private final long logAggregationStatusTimeout;
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new ConcurrentHashMap<NodeId, LogAggregationReport>();
  private volatile LogAggregationStatus logAggregationStatusForAppReport;
  private int logAggregationSucceed = 0;
  private int logAggregationFailed = 0;
  private Map<NodeId, List<String>> logAggregationDiagnosticsForNMs =
      new HashMap<NodeId, List<String>>();
  private Map<NodeId, List<String>> logAggregationFailureMessagesForNMs =
      new HashMap<NodeId, List<String>>();
  private final int maxLogAggregationDiagnosticsInMemory;
  
  private byte[] keyStore = null;
  private char[] keyStorePassword = null;
  private byte[] trustStore = null;
  private char[] trustStorePassword = null;
  private long certificateExpiration = -1;
  // Crypto material version is incremented only on certificate rotation
  private Integer cryptoMaterialVersion = 0;
  
  // These states stored are only valid when app is at killing or final_saving.
  private RMAppState stateBeforeKilling;
  private RMAppState stateBeforeFinalSaving;
  private RMAppEvent eventCausingFinalSaving;
  private RMAppState targetedFinalState;
  private RMAppState recoveredFinalState;
  private ResourceRequest amReq;
  
  private CallerContext callerContext;

  Object transitionTodo;

  private static final StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent> stateMachineFactory
                               = new StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent>(RMAppState.NEW)


     // Transitions from NEW state
    .addTransition(RMAppState.NEW, RMAppState.NEW,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
        RMAppEventType.START, new RMAppNewlySavingTransition())
     
    .addTransition(RMAppState.NEW, EnumSet.of(RMAppState.GENERATING_CERTS, RMAppState.SUBMITTED,
            RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
            RMAppState.KILLED, RMAppState.FINAL_SAVING),
        RMAppEventType.RECOVER, new RMAppRecoveredTransition())
    .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
        new AppKilledTransition())
    .addTransition(RMAppState.NEW, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
        new FinalSavingTransition(new AppRejectedTransition(),
          RMAppState.FAILED))
      
    // Transitions from NEW_SAVING state
    .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
      
    .addTransition(RMAppState.NEW_SAVING, RMAppState.GENERATING_CERTS,
        RMAppEventType.APP_NEW_SAVED, new RMAppGeneratingCertsTransition())
      
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
      
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
          new FinalSavingTransition(new AppRejectedTransition(),
            RMAppState.FAILED))
      
    .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
        RMAppEventType.MOVE, new RMAppMoveTransition())
      
     // Transitions from GENERATING_CERTS state
     .addTransition(RMAppState.GENERATING_CERTS, RMAppState.GENERATING_CERTS,
         RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
     .addTransition(RMAppState.GENERATING_CERTS, RMAppState.SUBMITTED,
         RMAppEventType.CERTS_GENERATED, new AddApplicationToSchedulerTransition())
     .addTransition(RMAppState.GENERATING_CERTS, RMAppState.FINAL_SAVING,
         RMAppEventType.KILL, new FinalSavingTransition(
             new AppKilledTransition(), RMAppState.KILLED))
      
     // Transitions from SUBMITTED state
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
        new FinalSavingTransition(
          new AppRejectedTransition(), RMAppState.FAILED))
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED, new StartAppAttemptTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.CERTS_RENEWED, new RMAppCertificatesRenewedTransition())

     // Transitions from ACCEPTED state
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED, new RMAppStateUpdateTransition(
            YarnApplicationState.RUNNING))
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        // ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
        // event because RMAppRecoveredTransition is returning ACCEPTED state
        // directly and waiting for the previous AM to exit.
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new FinalSavingTransition(FINISHED_TRANSITION, RMAppState.FINISHED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_KILLED,
        new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.CERTS_RENEWED, new RMAppCertificatesRenewedTransition())

     // Transitions from RUNNING state
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_UNREGISTERED,
        new FinalSavingTransition(
          new AttemptUnregisteredTransition(),
          RMAppState.FINISHING, RMAppState.FINISHED))
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
      // UnManagedAM directly jumps to finished
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.CERTS_RENEWED, new RMAppCertificatesRenewedTransition())

     // Transitions from FINAL_SAVING state
    .addTransition(RMAppState.FINAL_SAVING,
      EnumSet.of(RMAppState.FINISHING, RMAppState.FAILED,
        RMAppState.KILLED, RMAppState.FINISHED), RMAppEventType.APP_UPDATE_SAVED,
        new FinalStateSavedTransition())
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new AttemptFinishedAtFinalSavingTransition())
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    // ignorable transitions
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
        EnumSet.of(RMAppEventType.NODE_UPDATE, RMAppEventType.KILL,
          RMAppEventType.APP_NEW_SAVED, RMAppEventType.MOVE))

     // Transitions from FINISHING state
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHING, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    // ignorable transitions
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
      EnumSet.of(RMAppEventType.NODE_UPDATE,
        // ignore Kill/Move as we have already saved the final Finished state
        // in state store.
        RMAppEventType.KILL, RMAppEventType.MOVE))

     // Transitions from KILLING state
    .addTransition(RMAppState.KILLING, RMAppState.KILLING, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_KILLED,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_UNREGISTERED,
        new FinalSavingTransition(
          new AttemptUnregisteredTransition(),
          RMAppState.FINISHING, RMAppState.FINISHED))
    .addTransition(RMAppState.KILLING, RMAppState.FINISHED,
      // UnManagedAM directly jumps to finished
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.KILLING,
        EnumSet.of(RMAppState.FINAL_SAVING),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.KILLING))

    .addTransition(RMAppState.KILLING, RMAppState.KILLING,
        EnumSet.of(
            RMAppEventType.NODE_UPDATE,
            RMAppEventType.ATTEMPT_REGISTERED,
            RMAppEventType.APP_UPDATE_SAVED,
            RMAppEventType.KILL, RMAppEventType.MOVE))

     // Transitions from FINISHED state
     // ignorable transitions
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
        EnumSet.of(
            RMAppEventType.NODE_UPDATE,
            RMAppEventType.ATTEMPT_UNREGISTERED,
            RMAppEventType.ATTEMPT_FINISHED,
            RMAppEventType.KILL, RMAppEventType.MOVE))

     // Transitions from FAILED state
     // ignorable transitions
    .addTransition(RMAppState.FAILED, RMAppState.FAILED, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.FAILED, RMAppState.FAILED,
        EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE,
            RMAppEventType.MOVE))

     // Transitions from KILLED state
     // ignorable transitions
    .addTransition(RMAppState.KILLED, RMAppState.KILLED, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(
        RMAppState.KILLED,
        RMAppState.KILLED,
        EnumSet.of(RMAppEventType.APP_ACCEPTED,
            RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
            RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
            RMAppEventType.NODE_UPDATE, RMAppEventType.MOVE))

     .installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
                                                                 stateMachine;

  private static final int DUMMY_APPLICATION_ATTEMPT_NUMBER = -1;
  private static final float MINIMUM_AM_BLACKLIST_THRESHOLD_VALUE = 0.0f;
  private static final float MAXIMUM_AM_BLACKLIST_THRESHOLD_VALUE = 1.0f;

  private AtomicBoolean isAppRotatingCryptoMaterial;
  private Set<NodeId> rmNodesThatUpdatedCryptoMaterial;
  private AtomicLong materialRotationStartTime;
  
  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
      ApplicationMasterService masterService, long submitTime,
      String applicationType, Set<String> applicationTags, 
      ResourceRequest amReq) throws
      IOException {

    this.systemClock = new SystemClock();

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
    this.startTime = this.systemClock.getTime();
    this.applicationType = applicationType;
    this.applicationTags = applicationTags;
    this.amReq = amReq;
    
    int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    int individualMaxAppAttempts = submissionContext.getMaxAppAttempts();
    if (individualMaxAppAttempts <= 0 ||
        individualMaxAppAttempts > globalMaxAppAttempts) {
      this.maxAppAttempts = globalMaxAppAttempts;
      LOG.warn("The specific max attempts: " + individualMaxAppAttempts
          + " for application: " + applicationId.getId()
          + " is invalid, because it is out of the range [1, "
          + globalMaxAppAttempts + "]. Use the global max attempts instead.");
    } else {
      this.maxAppAttempts = individualMaxAppAttempts;
    }

    this.attemptFailuresValidityInterval =
        submissionContext.getAttemptFailuresValidityInterval();
    if (this.attemptFailuresValidityInterval > 0) {
      LOG.info("The attemptFailuresValidityInterval for the application: "
          + this.applicationId + " is " + this.attemptFailuresValidityInterval
          + ".");
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    this.callerContext = CallerContext.getCurrent();

    long localLogAggregationStatusTimeout =
        conf.getLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (localLogAggregationStatusTimeout <= 0) {
      this.logAggregationStatusTimeout =
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
    } else {
      this.logAggregationStatusTimeout = localLogAggregationStatusTimeout;
    }
    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    if (this.logAggregationEnabled) {
      this.logAggregationStatusForAppReport = LogAggregationStatus.NOT_START;
    } else {
      this.logAggregationStatusForAppReport = LogAggregationStatus.DISABLED;
    }
    maxLogAggregationDiagnosticsInMemory = conf.getInt(
        YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
        YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);

    // amBlacklistingEnabled can be configured globally
    // Just use the global values
    amBlacklistingEnabled =
        conf.getBoolean(
          YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
          YarnConfiguration.DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_ENABLED);
    if (amBlacklistingEnabled) {
      blacklistDisableThreshold = conf.getFloat(
          YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD,
          YarnConfiguration.
          DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD);
      // Verify whether blacklistDisableThreshold is valid. And for invalid
      // threshold, reset to global level blacklistDisableThreshold
      // configured.
      if (blacklistDisableThreshold < MINIMUM_AM_BLACKLIST_THRESHOLD_VALUE ||
          blacklistDisableThreshold > MAXIMUM_AM_BLACKLIST_THRESHOLD_VALUE) {
        blacklistDisableThreshold = YarnConfiguration.
            DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD;
      }
    }
    
    isAppRotatingCryptoMaterial = new AtomicBoolean(false);
    materialRotationStartTime = new AtomicLong(-1L);
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
    // finish state is obtained based on the state machine's current state
    // as a fall-back in case the application has not been unregistered
    // ( or if the app never unregistered itself )
    // when the report is requested
    if (currentAttempt != null
        && currentAttempt.getFinalApplicationStatus() != null) {
      return currentAttempt.getFinalApplicationStatus();
    }
    return createFinalApplicationStatus(this.stateMachine.getCurrentState());
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
    RMAppAttempt attempt = this.currentAttempt;
    if (attempt != null) {
      return attempt.getProgress();
    }
    return 0;
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
    return this.currentAttempt;
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

  private FinalApplicationStatus createFinalApplicationStatus(RMAppState state) {
    switch(state) {
    case NEW:
    case GENERATING_CERTS:
    case NEW_SAVING:
    case SUBMITTED:
    case ACCEPTED:
    case RUNNING:
    case FINAL_SAVING:
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
  public int pullRMNodeUpdates(Collection<RMNode> updatedNodes) {
    this.writeLock.lock();
    try {
      int updatedNodeCount = this.updatedNodes.size();
      updatedNodes.addAll(this.updatedNodes);
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
      LogAggregationStatus logAggregationStatus = null;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      FinalApplicationStatus finishState = getFinalApplicationStatus();
      String diags = UNAVAILABLE;
      float progress = 0.0f;
      org.apache.hadoop.yarn.api.records.Token amrmToken = null;
      if (allowAccess) {
        trackingUrl = getDefaultProxyTrackingUrl();
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
              clientToAMToken = BuilderUtils.newClientToAMToken(
                  attemptClientToAMToken.getIdentifier(),
                  attemptClientToAMToken.getKind().toString(),
                  attemptClientToAMToken.getPassword(),
                  attemptClientToAMToken.getService().toString());
            }
          }
          host = this.currentAttempt.getHost();
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
          progress = currentAttempt.getProgress();
          logAggregationStatus = this.getLogAggregationStatusForAppReport();
        }
        //if the diagnostics is not already set get it from attempt
        diags = getDiagnostics().toString();

        if (currentAttempt != null && 
            currentAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
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

        RMAppMetrics rmAppMetrics = getRMAppMetrics();
        appUsageReport.setMemorySeconds(rmAppMetrics.getMemorySeconds());
        appUsageReport.setVcoreSeconds(rmAppMetrics.getVcoreSeconds());
        appUsageReport.setGPUSeconds(rmAppMetrics.getGPUSeconds());
        appUsageReport.
            setPreemptedMemorySeconds(rmAppMetrics.
                getPreemptedMemorySeconds());
        appUsageReport.
            setPreemptedVcoreSeconds(rmAppMetrics.
                getPreemptedVcoreSeconds());
        appUsageReport.
            setPreemptedGPUSeconds(rmAppMetrics.
                getPreemptedGPUSeconds());
      }

      if (currentApplicationAttemptId == null) {
        currentApplicationAttemptId = 
            BuilderUtils.newApplicationAttemptId(this.applicationId, 
                DUMMY_APPLICATION_ATTEMPT_NUMBER);
      }

      ApplicationReport report = BuilderUtils.newApplicationReport(
          this.applicationId, currentApplicationAttemptId, this.user,
          this.queue, this.name, host, rpcPort, clientToAMToken,
          createApplicationState(), diags, trackingUrl, this.startTime,
          this.finishTime, finishState, appUsageReport, origTrackingUrl,
          progress, this.applicationType, amrmToken, applicationTags,
          this.submissionContext.getPriority());
      report.setLogAggregationStatus(logAggregationStatus);
      report.setUnmanagedApp(submissionContext.getUnmanagedAM());
      report.setAppNodeLabelExpression(getAppNodeLabelExpression());
      report.setAmNodeLabelExpression(getAmNodeLabelExpression());
      return report;
    } finally {
      this.readLock.unlock();
    }
  }

  private String getDefaultProxyTrackingUrl() {
    try {
      final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme, proxy);
      URI result = ProxyUriUtils.getProxyUri(null, proxyUri, applicationId);
      return result.toASCIIString();
    } catch (URISyntaxException e) {
      LOG.warn("Could not generate default proxy tracking URL for "
          + applicationId);
      return UNAVAILABLE;
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
    RMAppAttempt attempt = this.currentAttempt;
    if (attempt != null) {
      return attempt.getTrackingUrl();
    }
    return null;
  }

  @Override
  public String getOriginalTrackingUrl() {
    RMAppAttempt attempt = this.currentAttempt;
    if (attempt != null) {
      return attempt.getOriginalTrackingUrl();
    }
    return null;
  }

  @Override
  public StringBuilder getDiagnostics() {
    this.readLock.lock();
    try {
      if (diagnostics.length() == 0 && getCurrentAppAttempt() != null) {
        String appAttemptDiagnostics = getCurrentAppAttempt().getDiagnostics();
        if (appAttemptDiagnostics != null) {
          return new StringBuilder(appAttemptDiagnostics);
        }
      }
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
      LOG.debug("Processing event for " + appID + " of type "
          + event.getType());
      final RMAppState oldState = getState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getState()) {
        LOG.info(appID + " State change from " + oldState + " to "
            + getState() + " on event=" + event.getType());
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) {
    ApplicationStateData appState =
        state.getApplicationState().get(getApplicationId());
    this.recoveredFinalState = appState.getState();
    LOG.info("Recovering app: " + getApplicationId() + " with " + 
        + appState.getAttemptCount() + " attempts and final state = "
        + this.recoveredFinalState );
    this.diagnostics.append(null == appState.getDiagnostics() ? "" : appState
        .getDiagnostics());
    this.storedFinishTime = appState.getFinishTime();
    this.startTime = appState.getStartTime();
    this.callerContext = appState.getCallerContext();
    this.keyStore = appState.getKeyStore();
    this.keyStorePassword = appState.getKeyStorePassword();
    this.trustStore = appState.getTrustStore();
    this.trustStorePassword = appState.getTrustStorePassword();
    this.cryptoMaterialVersion = appState.getCryptoMaterialVersion();
    this.certificateExpiration = appState.getCertificateExpiration();
    this.isAppRotatingCryptoMaterial.set(appState.isDuringMaterialRotation());
    this.materialRotationStartTime.set(appState.getMaterialRotationStartTime());
    
    // send the ATS create Event during RM recovery.
    // NOTE: it could be duplicated with events sent before RM get restarted.
    sendATSCreateEvent();

    for(int i=0; i<appState.getAttemptCount(); ++i) {
      // create attempt
      createNewAttempt();
      ((RMAppAttemptImpl)this.currentAttempt).recover(state);
    }
  }
  
  private void createNewAttempt() {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);

    BlacklistManager currentAMBlacklistManager;
    if (currentAttempt != null) {
      // Transfer over the blacklist from the previous app-attempt.
      currentAMBlacklistManager = currentAttempt.getAMBlacklistManager();
    } else {
      if (amBlacklistingEnabled) {
        currentAMBlacklistManager = new SimpleBlacklistManager(
            scheduler.getNumClusterNodes(), blacklistDisableThreshold);
      } else {
        currentAMBlacklistManager = new DisabledBlacklistManager();
      }
    }
    RMAppAttempt attempt =
        new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,
          submissionContext, conf,
          // The newly created attempt maybe last attempt if (number of
          // previously failed attempts(which should not include Preempted,
          // hardware error and NM resync) + 1) equal to the max-attempt
          // limit.
          maxAppAttempts == (getNumFailedAppAttempts() + 1), amReq,
          currentAMBlacklistManager);
    attempts.put(appAttemptId, attempt);
    currentAttempt = attempt;
  }
  
  private void
      createAndStartNewAttempt(boolean transferStateFromPreviousAttempt) {
    createNewAttempt();
    handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(),
      transferStateFromPreviousAttempt));
  }

  private void processNodeUpdate(RMAppNodeUpdateType type, RMNode node) {
    NodeState nodeState = node.getState();
    updatedNodes.add(node);
    LOG.debug("Received node update event:" + type + " for node:" + node
        + " with state:" + nodeState);
  }

  private static class RMAppTransition implements
      SingleArcTransition<RMAppImpl, RMAppEvent> {
    public void transition(RMAppImpl app, RMAppEvent event) {
    };

  }

  private static final class RMAppNodeUpdateTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent) event;
      app.processNodeUpdate(nodeUpdateEvent.getUpdateType(),
          nodeUpdateEvent.getNode());
    };
  }

  private static final class RMAppStateUpdateTransition
      extends RMAppTransition {
    private YarnApplicationState stateToATS;

    public RMAppStateUpdateTransition(YarnApplicationState state) {
      stateToATS = state;
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      app.rmContext.getSystemMetricsPublisher().appStateUpdated(
          app, stateToATS, app.systemClock.getTime());
    };
  }

  private static final class AppRunningOnNodeTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRunningOnNodeEvent nodeAddedEvent = (RMAppRunningOnNodeEvent) event;
      
      // if final state already stored, notify RMNode
      if (isAppInFinalState(app)) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeAddedEvent.getNodeId(), nodeAddedEvent
                .getApplicationId()));
        return;
      }
      
      // otherwise, add it to ranNodes for further process
      boolean newNode = app.ranNodes.add(nodeAddedEvent.getNodeId());
      if (newNode && app.isAppRotatingCryptoMaterial.get()) {
        LOG.debug("Sending UPDATE_CRYPTO_EVENT to new running node: " + nodeAddedEvent.getNodeId());
        RMNodeUpdateCryptoMaterialForAppEvent updateEvent =
            new RMNodeUpdateCryptoMaterialForAppEvent(nodeAddedEvent.getNodeId(), app.applicationId, app.keyStore,
                app.keyStorePassword, app.trustStore, app.trustStorePassword, app.cryptoMaterialVersion);
        app.handler.handle(updateEvent);
      }

      if (!app.logAggregationStatus.containsKey(nodeAddedEvent.getNodeId())) {
        app.logAggregationStatus.put(nodeAddedEvent.getNodeId(),
          LogAggregationReport.newInstance(app.applicationId,
            app.logAggregationEnabled ? LogAggregationStatus.NOT_START
                : LogAggregationStatus.DISABLED, ""));
      }
    };
  }

  /**
   * Move an app to a new queue.
   * This transition must set the result on the Future in the RMAppMoveEvent,
   * either as an exception for failure or null for success, or the client will
   * be left waiting forever.
   */
  private static final class RMAppMoveTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppMoveEvent moveEvent = (RMAppMoveEvent) event;
      try {
        app.queue = app.scheduler.moveApplication(app.applicationId,
            moveEvent.getTargetQueue());
      } catch (YarnException ex) {
        moveEvent.getResult().setException(ex);
        return;
      }

      app.rmContext.getSystemMetricsPublisher().appUpdated(app,
          app.systemClock.getTime());

      // TODO: Write out change to state store (YARN-1558)
      // Also take care of RM failover
      moveEvent.getResult().set(null);
    }
  }

  // synchronously recover attempt to ensure any incoming external events
  // to be processed after the attempt processes the recover event.
  private void recoverAppAttempts() {
    for (RMAppAttempt attempt : getAppAttempts().values()) {
      attempt.handle(new RMAppAttemptEvent(attempt.getAppAttemptId(),
        RMAppAttemptEventType.RECOVER));
    }
  }

  private static final class RMAppRecoveredTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {

      RMAppRecoverEvent recoverEvent = (RMAppRecoverEvent) event;
      app.recover(recoverEvent.getRMState());
      // The app has completed.
      if (app.recoveredFinalState != null) {
        app.recoverAppAttempts();
        new FinalTransition(app.recoveredFinalState).transition(app, event);
        return app.recoveredFinalState;
      }

      if (UserGroupInformation.isSecurityEnabled()) {
        // asynchronously renew delegation token on recovery.
        try {
          app.rmContext.getDelegationTokenRenewer()
              .addApplicationAsyncDuringRecovery(app.getApplicationId(),
                  app.parseCredentials(),
                  app.submissionContext.getCancelTokensWhenComplete(),
                  app.getUser());
        } catch (Exception e) {
          String msg = "Failed to fetch user credentials from application:"
              + e.getMessage();
          app.diagnostics.append(msg);
          LOG.error(msg, e);
        }
      }
  
      Integer cryptoMaterialVersionToRevoke = app.cryptoMaterialVersion + 1;
      // No existent attempts means the attempt associated with this app was not
      // started or started but not yet saved.
      if (app.attempts.isEmpty()) {
        
        if (app.isCryptoMaterialPresent()) {
          // ResourceManager may have crashed after it has renewed the certificate but before updating
          // RMApp state, so revoke the current version plus 1 to be sure no missed certificate is valid
          app.rmContext.getRMAppCertificateManager()
              .revokeCertificateSynchronously(app.applicationId, app.user, cryptoMaterialVersionToRevoke);
          app.rmContext.getRMAppCertificateManager()
              .registerWithCertificateRenewer(app.applicationId, app.user, app.cryptoMaterialVersion,
                  app.certificateExpiration);
          try {
            app.materializeCertificates();
          } catch (InterruptedException ex) {
            LOG.error("Could not localize certificates for application " + app.applicationId + " during recovery");
          }
          
          app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
              app.submissionContext, false));
          return RMAppState.SUBMITTED;
        } else {
          RMAppCertificateManagerEvent revokeAndGenerateEvent = new RMAppCertificateManagerEvent(
              app.applicationId, app.user, app.cryptoMaterialVersion,
              RMAppCertificateManagerEventType.REVOKE_GENERATE_CERTIFICATE);
          app.handler.handle(revokeAndGenerateEvent);
          // RMApp should not register with the certificate renewer here as it will register
          // when it receives CERTS_GENERATED event in GENERATING_CERTS state
          return RMAppState.GENERATING_CERTS;
        }
      }
  
      // ResourceManager may have crashed after it has renewed the certificate but before updating
      // RMApp state, so revoke the current version plus 1 to be sure no missed certificate is valid
      app.rmContext.getRMAppCertificateManager()
          .revokeCertificateSynchronously(app.applicationId, app.user, cryptoMaterialVersionToRevoke);
      app.rmContext.getRMAppCertificateManager()
          .registerWithCertificateRenewer(app.applicationId, app.user, app.cryptoMaterialVersion,
              app.certificateExpiration);
  
      try {
        app.materializeCertificates();
      } catch (InterruptedException ex) {
        LOG.error("Could not localize certificates for application " + app.applicationId + " during recovery");
      }
      
      // Add application to scheduler synchronously to guarantee scheduler
      // knows applications before AM or NM re-registers.
      app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
          app.submissionContext, true));

      // recover attempts
      app.recoverAppAttempts();

      // YARN-1507 is saving the application state after the application is
      // accepted. So after YARN-1507, an app is saved meaning it is accepted.
      // Thus we return ACCECPTED state on recovery.
      return RMAppState.ACCEPTED;
    }
  }
  
  private void materializeCertificates() throws InterruptedException {
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      rmContext.getCertificateLocalizationService().materializeCertificates(user, applicationId.toString(), user,
          createByteBufferFromByteArray(keyStore), String.valueOf(keyStorePassword),
          createByteBufferFromByteArray(trustStore), String.valueOf(trustStorePassword));
    }
  }
  
  private ByteBuffer createByteBufferFromByteArray(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      buffer.put(bytes[i]);
    }
    buffer.flip();
    return buffer;
  }

  private boolean isCryptoMaterialPresent() {
    return keyStore != null && keyStorePassword != null
        && trustStore != null && trustStorePassword != null;
  }
  
  private void updateApplicationWithCryptoMaterial(RMAppCertificateGeneratedEvent event) {
    keyStore = event.getKeyStore();
    keyStorePassword = event.getKeyStorePassword();
    trustStore = event.getTrustStore();
    trustStorePassword = event.getTrustStorePassword();
    certificateExpiration = event.getExpirationEpoch();
  }
  
  private static final class AddApplicationToSchedulerTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (event instanceof RMAppCertificateGeneratedEvent) {
        app.updateApplicationWithCryptoMaterial((RMAppCertificateGeneratedEvent) event);
        ApplicationStateData appNewState =
            ApplicationStateData.newInstance(app.submitTime, app.startTime, app.submissionContext, app.user,
                app.callerContext, app.keyStore, app.keyStorePassword,
                app.trustStore, app.trustStorePassword, app.cryptoMaterialVersion, app.certificateExpiration,
                app.isAppRotatingCryptoMaterial.get(), app.materialRotationStartTime.get());
        app.rmContext.getStateStore().updateApplicationStateNoNotify(appNewState);
        app.rmContext.getRMAppCertificateManager()
            .registerWithCertificateRenewer(app.applicationId, app.user, app.cryptoMaterialVersion,
                app.certificateExpiration);
      }
      
      app.handler.handle(new AppAddedSchedulerEvent(app.user,
          app.submissionContext, false));
      // send the ATS create Event
      app.sendATSCreateEvent();
    }
  }
  
  private static final class RMAppCertificatesRenewedTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      LOG.info("Received new certificate");
      app.updateApplicationWithCryptoMaterial((RMAppCertificateGeneratedEvent) event);
      app.cryptoMaterialVersion++;
      app.isAppRotatingCryptoMaterial.compareAndSet(false, true);
      app.materialRotationStartTime.set(app.systemClock.getTime());
      
      ApplicationStateData appNewState =
          ApplicationStateData.newInstance(app.submitTime, app.startTime, app.submissionContext, app.user,
              app.callerContext, app.keyStore, app.keyStorePassword,
              app.trustStore, app.trustStorePassword, app.cryptoMaterialVersion, app.certificateExpiration,
              app.isAppRotatingCryptoMaterial.get(), app.materialRotationStartTime.get());
      app.rmContext.getStateStore().updateApplicationStateNoNotify(appNewState);
      
      if (app.rmNodesThatUpdatedCryptoMaterial == null) {
        app.rmNodesThatUpdatedCryptoMaterial = new HashSet<>(app.ranNodes.size());
      }
      
      for (NodeId nodeId : app.ranNodes) {
        RMNodeUpdateCryptoMaterialForAppEvent updateEvent =
            new RMNodeUpdateCryptoMaterialForAppEvent(nodeId, app.applicationId, app.keyStore, app.keyStorePassword,
                app.trustStore, app.trustStorePassword, app.cryptoMaterialVersion);
        app.handler.handle(updateEvent);
      }
      
      app.rmContext.getRMAppCertificateManager()
          .registerWithCertificateRenewer(app.applicationId, app.user, app.cryptoMaterialVersion, app.certificateExpiration);
    }
  }

  private static final class StartAppAttemptTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.createAndStartNewAttempt(false);
    };
  }

  private static final class FinalStateSavedTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {
      if (app.transitionTodo instanceof SingleArcTransition) {
        ((SingleArcTransition) app.transitionTodo).transition(app,
          app.eventCausingFinalSaving);
      } else if (app.transitionTodo instanceof MultipleArcTransition) {
        ((MultipleArcTransition) app.transitionTodo).transition(app,
          app.eventCausingFinalSaving);
      }
      return app.targetedFinalState;

    }
  }

  private static class AttemptFailedFinalStateSavedTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      String msg = null;
      if (event instanceof RMAppFailedAttemptEvent) {
        msg = app.getAppAttemptFailedDiagnostics(event);
      }
      LOG.info(msg);
      app.diagnostics.append(msg);
      // Inform the node for app-finish
      new FinalTransition(RMAppState.FAILED).transition(app, event);
    }
  }

  private String getAppAttemptFailedDiagnostics(RMAppEvent event) {
    String msg = null;
    RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
    if (this.submissionContext.getUnmanagedAM()) {
      // RM does not manage the AM. Do not retry
      msg = "Unmanaged application " + this.getApplicationId()
              + " failed due to " + failedEvent.getDiagnosticMsg()
              + ". Failing the application.";
    } else if (this.isNumAttemptsBeyondThreshold) {
      int globalLimit = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      msg = String.format(
        "Application %s failed %d times%s%s due to %s. Failing the application.",
          getApplicationId(),
          maxAppAttempts,
          (attemptFailuresValidityInterval <= 0 ? ""
               : (" in previous " + attemptFailuresValidityInterval
                  + " milliseconds")),
          (globalLimit == maxAppAttempts) ? ""
              : (" (global limit =" + globalLimit
                 + "; local limit is =" + maxAppAttempts + ")"),
          failedEvent.getDiagnosticMsg());
    }
    return msg;
  }
  
  private static final class RMAppNewlySavingTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {

      // If recovery is enabled then store the application information in a
      // non-blocking call so make sure that RM has stored the information
      // needed to restart the AM after RM restart without further client
      // communication
      
      LOG.info("Storing application with id " + app.applicationId);
      app.rmContext.getStateStore().storeNewApplication(app);
    }
  }
  
  private static final class RMAppGeneratingCertsTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      LOG.info("Generating certificates for application " + app.applicationId);
      RMAppCertificateManagerEvent genCertsEvent = new RMAppCertificateManagerEvent(app.applicationId,
          app.user, app.cryptoMaterialVersion, RMAppCertificateManagerEventType.GENERATE_CERTIFICATE);
      app.handler.handle(genCertsEvent);
    }
  }
  
  private void rememberTargetTransitions(RMAppEvent event,
      Object transitionToDo, RMAppState targetFinalState) {
    transitionTodo = transitionToDo;
    targetedFinalState = targetFinalState;
    eventCausingFinalSaving = event;
  }

  private void rememberTargetTransitionsAndStoreState(RMAppEvent event,
      Object transitionToDo, RMAppState targetFinalState,
      RMAppState stateToBeStored) {
    rememberTargetTransitions(event, transitionToDo, targetFinalState);
    this.stateBeforeFinalSaving = getState();
    this.storedFinishTime = this.systemClock.getTime();

    LOG.info("Updating application " + this.applicationId
        + " with final state: " + this.targetedFinalState);
    // we lost attempt_finished diagnostics in app, because attempt_finished
    // diagnostics is sent after app final state is saved. Later on, we will
    // create GetApplicationAttemptReport specifically for getting per attempt
    // info.
    String diags = null;
    switch (event.getType()) {
    case APP_REJECTED:
    case ATTEMPT_FINISHED:
    case ATTEMPT_KILLED:
      diags = event.getDiagnosticMsg();
      break;
    case ATTEMPT_FAILED:
      RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
      diags = getAppAttemptFailedDiagnostics(failedEvent);
      break;
    default:
      break;
    }

    ApplicationStateData appState =
        ApplicationStateData.newInstance(this.submitTime, this.startTime,
            this.user, this.submissionContext,
            stateToBeStored, diags, this.storedFinishTime, this.callerContext);
    this.rmContext.getStateStore().updateApplicationState(appState);
  }

  private static final class FinalSavingTransition extends RMAppTransition {
    Object transitionToDo;
    RMAppState targetedFinalState;
    RMAppState stateToBeStored;

    public FinalSavingTransition(Object transitionToDo,
        RMAppState targetedFinalState) {
      this(transitionToDo, targetedFinalState, targetedFinalState);
    }

    public FinalSavingTransition(Object transitionToDo,
        RMAppState targetedFinalState, RMAppState stateToBeStored) {
      this.transitionToDo = transitionToDo;
      this.targetedFinalState = targetedFinalState;
      this.stateToBeStored = stateToBeStored;
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if ((transitionToDo instanceof AppKilledTransition) &&
          (app.getState().equals(RMAppState.GENERATING_CERTS) || app.getState().equals(RMAppState.ACCEPTED))) {
        app.sendCertificateRevocationEvent();
      }
      // In any other state later than SUBMITTED, the revocation will be done be the RMAppAttempt
      if (app.getState().equals(RMAppState.SUBMITTED)) {
        app.sendCertificateRevocationEvent();
      }
      app.rememberTargetTransitionsAndStoreState(event, transitionToDo,
        targetedFinalState, stateToBeStored);
    }
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

    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append(event.getDiagnosticMsg());
      super.transition(app, event);
    };
  }

  private static class AttemptFinishedAtFinalSavingTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (app.targetedFinalState.equals(RMAppState.FAILED)
          || app.targetedFinalState.equals(RMAppState.KILLED)) {
        // Ignore Attempt_Finished event if we were supposed to reach FAILED
        // FINISHED state
        return;
      }

      // pass in the earlier attempt_unregistered event, as it is needed in
      // AppFinishedFinalStateSavedTransition later on
      app.rememberTargetTransitions(event,
        new AppFinishedFinalStateSavedTransition(app.eventCausingFinalSaving),
        RMAppState.FINISHED);
    };
  }

  private static class AppFinishedFinalStateSavedTransition extends
      RMAppTransition {
    RMAppEvent attemptUnregistered;

    public AppFinishedFinalStateSavedTransition(RMAppEvent attemptUnregistered) {
      this.attemptUnregistered = attemptUnregistered;
    }
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      new AttemptUnregisteredTransition().transition(app, attemptUnregistered);
      FINISHED_TRANSITION.transition(app, event);
    };
  }

  /**
   * Log the audit event for kill by client.
   *
   * @param event
   *          The {@link RMAppEvent} to be logged
   */
  static void auditLogKillEvent(RMAppEvent event) {
    if (event instanceof RMAppKillByClientEvent) {
      RMAppKillByClientEvent killEvent = (RMAppKillByClientEvent) event;
      UserGroupInformation callerUGI = killEvent.getCallerUGI();
      String userName = null;
      if (callerUGI != null) {
        userName = callerUGI.getShortUserName();
      }
      InetAddress remoteIP = killEvent.getIp();
      RMAuditLogger.logSuccess(userName, AuditConstants.KILL_APP_REQUEST,
          "RMAppImpl", event.getApplicationId(), remoteIP);
    }
  }

  private static class AppKilledTransition extends FinalTransition {
    public AppKilledTransition() {
      super(RMAppState.KILLED);
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append(event.getDiagnosticMsg());
      super.transition(app, event);
      RMAppImpl.auditLogKillEvent(event);
    };
  }

  private static class KillAttemptTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.stateBeforeKilling = app.getState();
      // Forward app kill diagnostics in the event to kill app attempt.
      // These diagnostics will be returned back in ATTEMPT_KILLED event sent by
      // RMAppAttemptImpl.
      app.handler.handle(
          new RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
              RMAppAttemptEventType.KILL, event.getDiagnosticMsg()));
      RMAppImpl.auditLogKillEvent(event);
    }
  }

  private static final class AppRejectedTransition extends
      FinalTransition{
    public AppRejectedTransition() {
      super(RMAppState.FAILED);
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append(event.getDiagnosticMsg());
      super.transition(app, event);
    };
  }

  private static class FinalTransition extends RMAppTransition {

    private final RMAppState finalState;

    public FinalTransition(RMAppState finalState) {
      this.finalState = finalState;
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      app.logAggregationStartTime = System.currentTimeMillis();
      for (NodeId nodeId : app.getRanNodes()) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
      }
      app.finishTime = app.storedFinishTime;
      if (app.finishTime == 0 ) {
        app.finishTime = app.systemClock.getTime();
      }
      // Recovered apps that are completed were not added to scheduler, so no
      // need to remove them from scheduler.
      if (app.recoveredFinalState == null) {
        app.handler.handle(new AppRemovedSchedulerEvent(app.applicationId,
          finalState));
      }
      app.handler.handle(
          new RMAppManagerEvent(app.applicationId,
          RMAppManagerEventType.APP_COMPLETED));

      app.rmContext.getRMApplicationHistoryWriter()
          .applicationFinished(app, finalState);
      app.rmContext.getSystemMetricsPublisher()
          .appFinished(app, finalState, app.finishTime);
    }
  }

  private int getNumFailedAppAttempts() {
    int completedAttempts = 0;
    long endTime = this.systemClock.getTime();
    // Do not count AM preemption, hardware failures or NM resync
    // as attempt failure.
    for (RMAppAttempt attempt : attempts.values()) {
      if (attempt.shouldCountTowardsMaxAttemptRetry()) {
        if (this.attemptFailuresValidityInterval <= 0
            || (attempt.getFinishTime() > endTime
                - this.attemptFailuresValidityInterval)) {
          completedAttempts++;
        }
      }
    }
    return completedAttempts;
  }

  private static final class AttemptFailedTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    private final RMAppState initialState;

    public AttemptFailedTransition(RMAppState initialState) {
      this.initialState = initialState;
    }

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {
      int numberOfFailure = app.getNumFailedAppAttempts();
      LOG.info("The number of failed attempts"
          + (app.attemptFailuresValidityInterval > 0 ? " in previous "
              + app.attemptFailuresValidityInterval + " milliseconds " : " ")
          + "is " + numberOfFailure + ". The max attempts is "
          + app.maxAppAttempts);
      if (!app.submissionContext.getUnmanagedAM()
          && numberOfFailure < app.maxAppAttempts) {
        if (initialState.equals(RMAppState.KILLING)) {
          // If this is not last attempt, app should be killed instead of
          // launching a new attempt
          app.rememberTargetTransitionsAndStoreState(event,
            new AppKilledTransition(), RMAppState.KILLED, RMAppState.KILLED);
          return RMAppState.FINAL_SAVING;
        }

        boolean transferStateFromPreviousAttempt;
        RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
        transferStateFromPreviousAttempt =
            failedEvent.getTransferStateFromPreviousAttempt();

        RMAppAttempt oldAttempt = app.currentAttempt;
        app.createAndStartNewAttempt(transferStateFromPreviousAttempt);
        // Transfer the state from the previous attempt to the current attempt.
        // Note that the previous failed attempt may still be collecting the
        // container events from the scheduler and update its data structures
        // before the new attempt is created. We always transferState for
        // finished containers so that they can be acked to NM,
        // but when pulling finished container we will check this flag again.
        ((RMAppAttemptImpl) app.currentAttempt)
          .transferStateFromAttempt(oldAttempt);
        return initialState;
      } else {
        if (numberOfFailure >= app.maxAppAttempts) {
          app.isNumAttemptsBeyondThreshold = true;
        }
        app.sendCertificateRevocationEvent();
        
        app.rememberTargetTransitionsAndStoreState(event,
          new AttemptFailedFinalStateSavedTransition(), RMAppState.FAILED,
          RMAppState.FAILED);
        return RMAppState.FINAL_SAVING;
      }
    }
  }

  private void sendCertificateRevocationEvent() {
    handler.handle(new RMAppCertificateManagerEvent(applicationId, user, cryptoMaterialVersion,
        RMAppCertificateManagerEventType.REVOKE_CERTIFICATE));
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
    return state.equals(RMAppState.FINISHING)
        || state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED)
        || state.equals(RMAppState.KILLED);
  }

  @Override
  public YarnApplicationState createApplicationState() {
    RMAppState rmAppState = getState();
    // If App is in FINAL_SAVING state, return its previous state.
    if (rmAppState.equals(RMAppState.FINAL_SAVING)) {
      rmAppState = stateBeforeFinalSaving;
    }
    if (rmAppState.equals(RMAppState.KILLING)) {
      rmAppState = stateBeforeKilling;
    }
    return RMServerUtils.createApplicationState(rmAppState);
  }
  
  public static boolean isAppInFinalState(RMApp rmApp) {
    RMAppState appState = ((RMAppImpl) rmApp).getRecoveredFinalState();
    if (appState == null) {
      appState = rmApp.getState();
    }
    return appState == RMAppState.FAILED || appState == RMAppState.FINISHED
        || appState == RMAppState.KILLED;
  }
  
  public RMAppState getRecoveredFinalState() {
    return this.recoveredFinalState;
  }

  @Override
  public Set<NodeId> getRanNodes() {
    return ranNodes;
  }
  
  @Override
  public RMAppMetrics getRMAppMetrics() {
    Resource resourcePreempted = Resource.newInstance(0, 0);
    int numAMContainerPreempted = 0;
    int numNonAMContainerPreempted = 0;
    long memorySeconds = 0;
    long vcoreSeconds = 0;
    long gpuSeconds = 0;
    long preemptedMemorySeconds = 0;
    long preemptedVcoreSeconds = 0;
    long preemptedGPUSeconds = 0;
    for (RMAppAttempt attempt : attempts.values()) {
      if (null != attempt) {
        RMAppAttemptMetrics attemptMetrics =
            attempt.getRMAppAttemptMetrics();
        Resources.addTo(resourcePreempted,
            attemptMetrics.getResourcePreempted());
        numAMContainerPreempted += attemptMetrics.getIsPreempted() ? 1 : 0;
        numNonAMContainerPreempted +=
            attemptMetrics.getNumNonAMContainersPreempted();
        // getAggregateAppResourceUsage() will calculate resource usage stats
        // for both running and finished containers.
        AggregateAppResourceUsage resUsage =
            attempt.getRMAppAttemptMetrics().getAggregateAppResourceUsage();
        memorySeconds += resUsage.getMemorySeconds();
        vcoreSeconds += resUsage.getVcoreSeconds();
        gpuSeconds += resUsage.getGPUSeconds();
        preemptedMemorySeconds += attemptMetrics.getPreemptedMemory();
        preemptedVcoreSeconds += attemptMetrics.getPreemptedVcore();
        preemptedGPUSeconds += attemptMetrics.getPreemptedGPU();
      }
    }

    return new RMAppMetrics(resourcePreempted,
        numNonAMContainerPreempted, numAMContainerPreempted,
        memorySeconds, vcoreSeconds, gpuSeconds,
        preemptedMemorySeconds, preemptedVcoreSeconds, preemptedGPUSeconds);
  }

  @Private
  @VisibleForTesting
  public void setSystemClock(Clock clock) {
    this.systemClock = clock;
  }

  @Override
  public ReservationId getReservationId() {
    return submissionContext.getReservationID();
  }
  
  @Override
  public ResourceRequest getAMResourceRequest() {
    return this.amReq; 
  }

  protected Credentials parseCredentials() throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = submissionContext.getAMContainerSpec().getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    return credentials;
  }

  @Override
  public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
    try {
      this.readLock.lock();
      if (!isLogAggregationFinished() && isAppInFinalState(this) &&
          System.currentTimeMillis() > this.logAggregationStartTime
          + this.logAggregationStatusTimeout) {
        for (Entry<NodeId, LogAggregationReport> output :
            logAggregationStatus.entrySet()) {
          if (!output.getValue().getLogAggregationStatus()
            .equals(LogAggregationStatus.TIME_OUT)
              && !output.getValue().getLogAggregationStatus()
                .equals(LogAggregationStatus.SUCCEEDED)
              && !output.getValue().getLogAggregationStatus()
                .equals(LogAggregationStatus.FAILED)) {
            output.getValue().setLogAggregationStatus(
              LogAggregationStatus.TIME_OUT);
          }
        }
      }
      return Collections.unmodifiableMap(logAggregationStatus);
    } finally {
      this.readLock.unlock();
    }
  }

  public void aggregateLogReport(NodeId nodeId, LogAggregationReport report) {
    try {
      this.writeLock.lock();
      if (this.logAggregationEnabled && !isLogAggregationFinished()) {
        LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
        boolean stateChangedToFinal = false;
        if (curReport == null) {
          this.logAggregationStatus.put(nodeId, report);
          if (isLogAggregationFinishedForNM(report)) {
            stateChangedToFinal = true;
          }
        } else {
          if (isLogAggregationFinishedForNM(report)) {
            if (!isLogAggregationFinishedForNM(curReport)) {
              stateChangedToFinal = true;
            }
          }
          if (report.getLogAggregationStatus() != LogAggregationStatus.RUNNING
              || curReport.getLogAggregationStatus() !=
                  LogAggregationStatus.RUNNING_WITH_FAILURE) {
            if (curReport.getLogAggregationStatus()
                == LogAggregationStatus.TIME_OUT
                && report.getLogAggregationStatus()
                    == LogAggregationStatus.RUNNING) {
            // If the log aggregation status got from latest nm heartbeat
            // is Running, and current log aggregation status is TimeOut,
            // based on whether there are any failure messages for this NM,
            // we will reset the log aggregation status as RUNNING or
            // RUNNING_WITH_FAILURE
              if (logAggregationFailureMessagesForNMs.get(nodeId) != null &&
                  !logAggregationFailureMessagesForNMs.get(nodeId).isEmpty()) {
                report.setLogAggregationStatus(
                    LogAggregationStatus.RUNNING_WITH_FAILURE);
              }
            }
            curReport.setLogAggregationStatus(report
              .getLogAggregationStatus());
          }
        }
        updateLogAggregationDiagnosticMessages(nodeId, report);
        if (isAppInFinalState(this) && stateChangedToFinal) {
          updateLogAggregationStatus(nodeId);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public LogAggregationStatus getLogAggregationStatusForAppReport() {
    try {
      this.readLock.lock();
      if (! logAggregationEnabled) {
        return LogAggregationStatus.DISABLED;
      }
      if (isLogAggregationFinished()) {
        return this.logAggregationStatusForAppReport;
      }
      Map<NodeId, LogAggregationReport> reports =
          getLogAggregationReportsForApp();
      if (reports.size() == 0) {
        return this.logAggregationStatusForAppReport;
      }
      int logNotStartCount = 0;
      int logCompletedCount = 0;
      int logTimeOutCount = 0;
      int logFailedCount = 0;
      int logRunningWithFailure = 0;
      for (Entry<NodeId, LogAggregationReport> report : reports.entrySet()) {
        switch (report.getValue().getLogAggregationStatus()) {
          case NOT_START:
            logNotStartCount++;
            break;
          case RUNNING_WITH_FAILURE:
            logRunningWithFailure ++;
            break;
          case SUCCEEDED:
            logCompletedCount++;
            break;
          case FAILED:
            logFailedCount++;
            logCompletedCount++;
            break;
          case TIME_OUT:
            logTimeOutCount++;
            logCompletedCount++;
            break;
          default:
            break;
        }
      }
      if (logNotStartCount == reports.size()) {
        return LogAggregationStatus.NOT_START;
      } else if (logCompletedCount == reports.size()) {
        // We should satisfy two condition in order to return SUCCEEDED or FAILED
        // 1) make sure the application is in final state
        // 2) logs status from all NMs are SUCCEEDED/FAILED/TIMEOUT
        // The SUCCEEDED/FAILED status is the final status which means
        // the log aggregation is finished. And the log aggregation status will
        // not be updated anymore.
        if (logFailedCount > 0 && isAppInFinalState(this)) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.FAILED;
          return LogAggregationStatus.FAILED;
        } else if (logTimeOutCount > 0) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.TIME_OUT;
          return LogAggregationStatus.TIME_OUT;
        }
        if (isAppInFinalState(this)) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.SUCCEEDED;
          return LogAggregationStatus.SUCCEEDED;
        }
      } else if (logRunningWithFailure > 0) {
        return LogAggregationStatus.RUNNING_WITH_FAILURE;
      }
      return LogAggregationStatus.RUNNING;
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean isLogAggregationFinished() {
    return this.logAggregationStatusForAppReport
      .equals(LogAggregationStatus.SUCCEEDED)
        || this.logAggregationStatusForAppReport
          .equals(LogAggregationStatus.FAILED)
        || this.logAggregationStatusForAppReport
          .equals(LogAggregationStatus.TIME_OUT);

  }

  private boolean isLogAggregationFinishedForNM(LogAggregationReport report) {
    return report.getLogAggregationStatus() == LogAggregationStatus.SUCCEEDED
        || report.getLogAggregationStatus() == LogAggregationStatus.FAILED;
  }

  private void updateLogAggregationDiagnosticMessages(NodeId nodeId,
      LogAggregationReport report) {
    if (report.getDiagnosticMessage() != null
        && !report.getDiagnosticMessage().isEmpty()) {
      if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING ) {
        List<String> diagnostics = logAggregationDiagnosticsForNMs.get(nodeId);
        if (diagnostics == null) {
          diagnostics = new ArrayList<String>();
          logAggregationDiagnosticsForNMs.put(nodeId, diagnostics);
        } else {
          if (diagnostics.size()
              == maxLogAggregationDiagnosticsInMemory) {
            diagnostics.remove(0);
          }
        }
        diagnostics.add(report.getDiagnosticMessage());
        this.logAggregationStatus.get(nodeId).setDiagnosticMessage(
          StringUtils.join(diagnostics, "\n"));
      } else if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING_WITH_FAILURE) {
        List<String> failureMessages =
            logAggregationFailureMessagesForNMs.get(nodeId);
        if (failureMessages == null) {
          failureMessages = new ArrayList<String>();
          logAggregationFailureMessagesForNMs.put(nodeId, failureMessages);
        } else {
          if (failureMessages.size()
              == maxLogAggregationDiagnosticsInMemory) {
            failureMessages.remove(0);
          }
        }
        failureMessages.add(report.getDiagnosticMessage());
      }
    }
  }

  private void updateLogAggregationStatus(NodeId nodeId) {
    LogAggregationStatus status =
        this.logAggregationStatus.get(nodeId).getLogAggregationStatus();
    if (status.equals(LogAggregationStatus.SUCCEEDED)) {
      this.logAggregationSucceed++;
    } else if (status.equals(LogAggregationStatus.FAILED)) {
      this.logAggregationFailed++;
    }
    if (this.logAggregationSucceed == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport =
          LogAggregationStatus.SUCCEEDED;
      // Since the log aggregation status for this application for all NMs
      // is SUCCEEDED, it means all logs are aggregated successfully.
      // We could remove all the cached log aggregation reports
      this.logAggregationStatus.clear();
      this.logAggregationDiagnosticsForNMs.clear();
      this.logAggregationFailureMessagesForNMs.clear();
    } else if (this.logAggregationSucceed + this.logAggregationFailed
        == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
      // We have collected the log aggregation status for all NMs.
      // The log aggregation status is FAILED which means the log
      // aggregation fails in some NMs. We are only interested in the
      // nodes where the log aggregation is failed. So we could remove
      // the log aggregation details for those succeeded NMs
      for (Iterator<Map.Entry<NodeId, LogAggregationReport>> it =
          this.logAggregationStatus.entrySet().iterator(); it.hasNext();) {
        Map.Entry<NodeId, LogAggregationReport> entry = it.next();
        if (entry.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.SUCCEEDED)) {
          it.remove();
        }
      }
      // the log aggregation has finished/failed.
      // and the status will not be updated anymore.
      this.logAggregationDiagnosticsForNMs.clear();
    }
  }

  public String getLogAggregationFailureMessagesForNM(NodeId nodeId) {
    try {
      this.readLock.lock();
      List<String> failureMessages =
          this.logAggregationFailureMessagesForNMs.get(nodeId);
      if (failureMessages == null || failureMessages.isEmpty()) {
        return StringUtils.EMPTY;
      }
      return StringUtils.join(failureMessages, "\n");
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getAppNodeLabelExpression() {
    String appNodeLabelExpression =
        getApplicationSubmissionContext().getNodeLabelExpression();
    appNodeLabelExpression = (appNodeLabelExpression == null)
        ? NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET : appNodeLabelExpression;
    appNodeLabelExpression = (appNodeLabelExpression.trim().isEmpty())
        ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : appNodeLabelExpression;
    return appNodeLabelExpression;
  }

  @Override
  public String getAmNodeLabelExpression() {
    String amNodeLabelExpression = null;
    if (!getApplicationSubmissionContext().getUnmanagedAM()) {
      amNodeLabelExpression = getAMResourceRequest().getNodeLabelExpression();
      amNodeLabelExpression = (amNodeLabelExpression == null)
          ? NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET : amNodeLabelExpression;
      amNodeLabelExpression = (amNodeLabelExpression.trim().isEmpty())
          ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : amNodeLabelExpression;
    }
    return amNodeLabelExpression;
  }
  
  @Override
  public CallerContext getCallerContext() {
    return callerContext;
  }

  private void sendATSCreateEvent() {
    rmContext.getRMApplicationHistoryWriter().applicationStarted(this);
    rmContext.getSystemMetricsPublisher().appCreated(this, this.startTime);
  }
  
  @Override
  public byte[] getKeyStore() {
    return keyStore;
  }
  
  @Override
  public char[] getKeyStorePassword() {
    return keyStorePassword;
  }
  
  @Override
  public byte[] getTrustStore() {
    return trustStore;
  }
  
  @Override
  public char[] getTrustStorePassword() {
    return trustStorePassword;
  }
  
  @Override
  public long getCertificateExpiration() {
    return certificateExpiration;
  }
  
  @Override
  public Integer getCryptoMaterialVersion() {
    return cryptoMaterialVersion;
  }
  
  @VisibleForTesting
  public Set<NodeId> getRMNodesUpdatedCryptoMaterial() {
    return rmNodesThatUpdatedCryptoMaterial;
  }
  
  @VisibleForTesting
  public boolean isAppRotatingCryptoMaterial() {
    return isAppRotatingCryptoMaterial.get();
  }
  
  @Override
  public long getMaterialRotationStartTime() {
    return materialRotationStartTime.get();
  }
  
  @Override
  public void rmNodeHasUpdatedCryptoMaterial(NodeId nodeId) {
    this.writeLock.lock();
    try {
      if (rmNodesThatUpdatedCryptoMaterial == null) {
        LOG.warn("Received RMNode updated crypto material but Set is null!");
        return;
      }
      if (isAppRotatingCryptoMaterial.get()) {
        rmNodesThatUpdatedCryptoMaterial.add(nodeId);
        if (rmNodesThatUpdatedCryptoMaterial.containsAll(ranNodes)) {
          int cryptoVersionToRevoke = cryptoMaterialVersion - 1;
          RMAppCertificateManagerEvent event = new RMAppCertificateManagerEvent(applicationId, user,
              cryptoVersionToRevoke, RMAppCertificateManagerEventType.REVOKE_CERTIFICATE_AFTER_ROTATION);
          handler.handle(event);
          rmNodesThatUpdatedCryptoMaterial = null;
          isAppRotatingCryptoMaterial.compareAndSet(true, false);
          materialRotationStartTime.set(-1L);
          ApplicationStateData appNewState =
              ApplicationStateData.newInstance(submitTime, startTime, submissionContext, user,
                  callerContext, keyStore, keyStorePassword,
                  trustStore, trustStorePassword, cryptoMaterialVersion, certificateExpiration,
                  isAppRotatingCryptoMaterial.get(), -1L);
          rmContext.getStateStore().updateApplicationStateNoNotify(appNewState);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void resetCryptoRotationMetrics() {
    isAppRotatingCryptoMaterial.set(false);
    materialRotationStartTime.set(-1L);
    this.writeLock.lock();
    try {
      rmNodesThatUpdatedCryptoMaterial = null;
      ApplicationStateData appNewState =
          ApplicationStateData.newInstance(submitTime, startTime, submissionContext, user,
              callerContext, keyStore, keyStorePassword,
              trustStore, trustStorePassword, cryptoMaterialVersion, certificateExpiration,
              isAppRotatingCryptoMaterial.get(), -1L);
      rmContext.getStateStore().updateApplicationStateNoNotify(appNewState);
    } finally {
      this.writeLock.unlock();
    }
  }
}
