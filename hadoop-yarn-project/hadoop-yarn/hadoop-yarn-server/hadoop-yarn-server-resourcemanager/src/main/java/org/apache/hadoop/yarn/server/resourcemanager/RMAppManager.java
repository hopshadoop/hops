/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;

/**
 * This class manages the list of applications for the resource manager.
 */
public class RMAppManager
    implements EventHandler<RMAppManagerEvent>, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppManager.class);

  private final int maxCompletedAppsInMemory; //recoverd through config file
  private int maxCompletedAppsInStateStore;//recoverd through config file
  protected int completedAppsInStateStore = 0;//recovered when app recovered
  private final LinkedList<ApplicationId> completedApps =
      new LinkedList<ApplicationId>();//recovered when app recovered

  private final RMContext rmContext;//recovered
  private final ApplicationMasterService masterService;//recovered
  private final YarnScheduler scheduler;//recovered
  private final ApplicationACLsManager applicationACLsManager;//recovered
  private final Configuration conf;//recovered

  public RMAppManager(RMContext context, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationACLsManager applicationACLsManager, Configuration conf) {
    this.rmContext = context;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.applicationACLsManager = applicationACLsManager;
    this.conf = conf;
    this.maxCompletedAppsInMemory =
        conf.getInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
            YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
    this.maxCompletedAppsInStateStore =
        conf.getInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS,
            YarnConfiguration.DEFAULT_RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS);
    if (this.maxCompletedAppsInStateStore > this.maxCompletedAppsInMemory) {
      this.maxCompletedAppsInStateStore = this.maxCompletedAppsInMemory;
    }
  }

  /**
   * This class is for logging the application summary.
   */
  static class ApplicationSummary {

    static final Log LOG = LogFactory.getLog(ApplicationSummary.class);

    // Escape sequences 
    static final char EQUALS = '=';
    static final char[] charsToEscape =
        {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

    static class SummaryBuilder {

      final StringBuilder buffer = new StringBuilder();

      // A little optimization for a very common case
      SummaryBuilder add(String key, long value) {
        return _add(key, Long.toString(value));
      }

      <T> SummaryBuilder add(String key, T value) {
        String escapedString = StringUtils
            .escapeString(String.valueOf(value), StringUtils.ESCAPE_CHAR,
                charsToEscape).
                replaceAll("\n", "\\\\n").replaceAll("\r", "\\\\r");
        return _add(key, escapedString);
      }

      SummaryBuilder add(SummaryBuilder summary) {
        if (buffer.length() > 0) {
          buffer.append(StringUtils.COMMA);
        }
        buffer.append(summary.buffer);
        return this;
      }

      SummaryBuilder _add(String key, String value) {
        if (buffer.length() > 0) {
          buffer.append(StringUtils.COMMA);
        }
        buffer.append(key).append(EQUALS).append(value);
        return this;
      }

      @Override
      public String toString() {
        return buffer.toString();
      }
    }

    /**
     * create a summary of the application's runtime.
     * <p/>
     *
     * @param app
     *     {@link RMApp} whose summary is to be created, cannot
     *     be <code>null</code>.
     */
    public static SummaryBuilder createAppSummary(RMApp app) {
      String trackingUrl = "N/A";
      String host = "N/A";
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        trackingUrl = attempt.getTrackingUrl();
        host = attempt.getHost();
      }
      SummaryBuilder summary =
          new SummaryBuilder().add("appId", app.getApplicationId())
              .add("name", app.getName()).add("user", app.getUser())
              .add("queue", app.getQueue()).add("state", app.getState())
              .add("trackingUrl", trackingUrl).add("appMasterHost", host)
              .add("startTime", app.getStartTime())
              .add("finishTime", app.getFinishTime())
              .add("finalStatus", app.getFinalApplicationStatus());
      return summary;
    }

    /**
     * Log a summary of the application's runtime.
     * <p/>
     *
     * @param app
     *     {@link RMApp} whose summary is to be logged
     */
    public static void logAppSummary(RMApp app) {
      if (app != null) {
        LOG.info(createAppSummary(app));
      }
    }
  }

  @VisibleForTesting
  public void logApplicationSummary(ApplicationId appId) {
    ApplicationSummary.logAppSummary(rmContext.getRMApps().get(appId));
  }

  protected synchronized int getCompletedAppsListSize() {
    return this.completedApps.size();
  }

  protected synchronized void finishApplication(ApplicationId applicationId,
      TransactionState transactionState) {
    if (applicationId == null) {
      LOG.error("RMAppManager received completed appId of null, skipping");
    } else {
      // Inform the DelegationTokenRenewer
      if (UserGroupInformation.isSecurityEnabled()) {
        rmContext.getDelegationTokenRenewer()
            .applicationFinished(applicationId, transactionState);
      }

      completedApps.add(applicationId);
      completedAppsInStateStore++;
      writeAuditLog(applicationId);
    }
  }

  protected void writeAuditLog(ApplicationId appId) {
    RMApp app = rmContext.getRMApps().get(appId);
    String operation = "UNKONWN";
    boolean success = false;
    switch (app.getState()) {
      case FAILED:
        operation = AuditConstants.FINISH_FAILED_APP;
        break;
      case FINISHED:
        operation = AuditConstants.FINISH_SUCCESS_APP;
        success = true;
        break;
      case KILLED:
        operation = AuditConstants.FINISH_KILLED_APP;
        success = true;
        break;
      default:
    }

    if (success) {
      RMAuditLogger.logSuccess(app.getUser(), operation, "RMAppManager",
          app.getApplicationId());
    } else {
      StringBuilder diag = app.getDiagnostics();
      String msg = diag == null ? null : diag.toString();
      RMAuditLogger.logFailure(app.getUser(), operation, msg, "RMAppManager",
          "App failed with state: " + app.getState(), appId);
    }
  }

  /*
   * check to see if hit the limit for max # completed apps kept
   */
  protected synchronized void checkAppNumCompletedLimit(
      TransactionState transactionState) {
    // check apps kept in state store.
    while (completedAppsInStateStore > this.maxCompletedAppsInStateStore) {
      ApplicationId removeId =
          completedApps.get(completedApps.size() - completedAppsInStateStore);
      RMApp removeApp = rmContext.getRMApps().get(removeId);
      LOG.info("Max number of completed apps kept in state store met:" +
          " maxCompletedAppsInStateStore = " + maxCompletedAppsInStateStore +
          ", removing app " + removeApp.getApplicationId() +
          " from state store.");
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState)
            .addApplicationToRemove(removeId);
      }
      //      rmContext.getStateStore().removeApplication(removeApp, transactionState);
      completedAppsInStateStore--;
    }

    // check apps kept in memorty.
    while (completedApps.size() > this.maxCompletedAppsInMemory) {
      ApplicationId removeId = completedApps.remove();
      LOG.info("Application should be expired, max number of completed apps" +
          " kept in memory met: maxCompletedAppsInMemory = " +
          this.maxCompletedAppsInMemory + ", removing app " + removeId +
          " from memory: ");
      rmContext.getRMApps().remove(removeId);
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState)
            .addApplicationToRemove(removeId);
      }
      this.applicationACLsManager.removeApplication(removeId);
    }
  }

  @SuppressWarnings("unchecked")
  protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user, TransactionState transactionState) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();

    RMAppImpl application =
        createAndPopulateNewRMApp(submissionContext, submitTime, user,
            transactionState);
    ApplicationId appId = submissionContext.getApplicationId();

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials;
      try {
        credentials = parseCredentials(submissionContext);
        this.rmContext.getDelegationTokenRenewer()
            .addApplicationAsync(appId, credentials,
                submissionContext.getCancelTokensWhenComplete(),
                transactionState);
      } catch (Exception e) {
        LOG.warn("Unable to parse credentials.", e);
        // Sending APP_REJECTED is fine, since we assume that the
        // RMApp is in NEW state and thus we haven't yet informed the
        // scheduler about the existence of the application
        assert application.getState() == RMAppState.NEW;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppRejectedEvent(applicationId, e.getMessage(),
                transactionState));
        throw RPCUtil.getRemoteException(e);
      }
    } else {
      // Dispatcher is not yet started at this time, so these START events
      // enqueued should be guaranteed to be first processed when dispatcher
      // gets started.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(applicationId, RMAppEventType.START,
              transactionState));
    }
  }

  @SuppressWarnings("unchecked")
  protected void recoverApplication(ApplicationState appState, RMState rmState,
      TransactionState transactionState) throws YarnException, IOException {
    ApplicationSubmissionContext appContext = appState.
        getApplicationSubmissionContext();
    ApplicationId appId = appState.getAppId();

    // create and recover app.
    RMAppImpl application = createAndPopulateNewRMApp(appContext, appState.
            getSubmitTime(), appState.getUser(), transactionState);
    application.recover(rmState);

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials;
      try {
        credentials = parseCredentials(appContext);
        // synchronously renew delegation token on recovery.
        rmContext.getDelegationTokenRenewer()
            .addApplicationSync(appId, credentials,
                appContext.getCancelTokensWhenComplete(), null);
        //        application.handle(new RMAppEvent(appId, RMAppEventType.RECOVER, null));
      } catch (IOException e) {
        LOG.warn("Unable to parse and renew delegation tokens.", e);
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppRejectedEvent(appId, e.getMessage(), null));
        throw e;
      }
    }
  }

  private RMAppImpl createAndPopulateNewRMApp(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user, TransactionState transactionState) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();
    validateResourceRequest(submissionContext);
    // Create RMApp
    RMAppImpl application = new RMAppImpl(applicationId, rmContext, this.conf,
        submissionContext.getApplicationName(), user,
        submissionContext.getQueue(), submissionContext, this.scheduler,
        this.masterService, submitTime, submissionContext.getApplicationType(),
        submissionContext.getApplicationTags(), transactionState);

    // Concurrent app submissions with same applicationId will fail here
    // Concurrent app submissions with different applicationIds will not
    // influence each other
    if (rmContext.getRMApps().putIfAbsent(applicationId, application) != null) {
      String message = "Application with id " + applicationId +
          " is already present! Cannot add a duplicate!";
      LOG.warn(message);
      throw RPCUtil.getRemoteException(message);
    }
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .addApplicationToAdd(application);
    }
    // Inform the ACLs Manager
    this.applicationACLsManager.addApplication(applicationId,
        submissionContext.getAMContainerSpec().getApplicationACLs());
    return application;
  }

  private void validateResourceRequest(
      ApplicationSubmissionContext submissionContext)
      throws InvalidResourceRequestException {
    // Validation of the ApplicationSubmissionContext needs to be completed
    // here. Only those fields that are dependent on RM's configuration are
    // checked here as they have to be validated whether they are part of new
    // submission or just being recovered.

    // Check whether AM resource requirements are within required limits
    if (!submissionContext.getUnmanagedAM()) {
      ResourceRequest amReq = BuilderUtils
          .newResourceRequest(RMAppAttemptImpl.AM_CONTAINER_PRIORITY,
              ResourceRequest.ANY, submissionContext.getResource(), 1);
      try {
        SchedulerUtils.validateResourceRequest(amReq,
            scheduler.getMaximumResourceCapability());
      } catch (InvalidResourceRequestException e) {
        LOG.warn("RM app submission failed in validating AM resource request" +
            " for application " + submissionContext.getApplicationId(), e);
        throw e;
      }
    }
  }

  private Credentials parseCredentials(ApplicationSubmissionContext application)
      throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = application.getAMContainerSpec().getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    return credentials;
  }

  @Override
  public void recover(RMState state) throws YarnException, IOException {
    RMStateStore store = rmContext.getStateStore();
    assert store != null;
    // recover applications
    Map<ApplicationId, ApplicationState> appStates =
        state.getApplicationState();
    LOG.info("Recovering " + appStates.size() + " applications");
    for (ApplicationState appState : appStates.values()) {
      recoverApplication(appState, state, null);
    }
  }

  @Override
  public void handle(RMAppManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.debug(
        "RMAppManager processing event for " + applicationId + " of type " +
            event.getType());
    switch (event.getType()) {
      case APP_COMPLETED: {
        finishApplication(applicationId, event.getTransactionState());
        logApplicationSummary(applicationId);
        checkAppNumCompletedLimit(event.getTransactionState());
      }
      break;
      default:
        LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }
}
