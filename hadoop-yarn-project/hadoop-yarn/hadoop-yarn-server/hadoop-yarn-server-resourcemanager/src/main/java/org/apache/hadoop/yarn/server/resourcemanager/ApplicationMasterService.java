/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.entity.appmasterrpc.AllocateRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncreaseRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceIncreaseRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService
    implements ApplicationMasterProtocol {

  private static final Log LOG =
      LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private final YarnScheduler rScheduler;
  private InetSocketAddress bindAddress;
  private Server server;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock>
      responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  private final AllocateResponse resync =
      recordFactory.newRecordInstance(AllocateResponse.class);
  private final RMContext rmContext;

  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    super(ApplicationMasterService.class.getName());
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.resync.setAMCommand(AMCommand.AM_RESYNC);
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress masterServiceAddress =
        conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    // If the auth is not-simple, enforce it to be token-based.
    Configuration serverConf = new Configuration(conf);
    serverConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server = rpc.getServer(ApplicationMasterProtocol.class, this,
        masterServiceAddress, serverConf,
        this.rmContext.getAMRMTokenSecretManager(), serverConf
            .getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf
        .getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
            false)) {
      InputStream inputStream = this.rmContext.getConfigurationProvider()
          .getConfigurationInputStream(conf,
              YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    this.bindAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
            server.getListenerAddress());
    super.serviceStart();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  // Obtain the needed AMRMTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to be
  // sure.
  private AMRMTokenIdentifier selectAMRMTokenIdentifier(
      UserGroupInformation remoteUgi) throws IOException {
    AMRMTokenIdentifier result = null;
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }

    return result;
  }

  private ApplicationAttemptId authorizeRequest() throws YarnException {

    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg =
          "Cannot obtain the user-name for authorizing ApplicationMaster. " +
              "Got exception: " + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }

    boolean tokenFound = false;
    String message = "";
    AMRMTokenIdentifier appTokenIdentifier = null;
    try {
      appTokenIdentifier = selectAMRMTokenIdentifier(remoteUgi);
      if (appTokenIdentifier == null) {
        tokenFound = false;
        message = "No AMRMToken found for user " + remoteUgi.getUserName();
      } else {
        tokenFound = true;
      }
    } catch (IOException e) {
      tokenFound = false;
      message = "Got exception while looking for AMRMToken for user " +
          remoteUgi.getUserName();
    }

    if (!tokenFound) {
      LOG.warn(message);
      throw RPCUtil.getRemoteException(message);
    }

    return appTokenIdentifier.getApplicationAttemptId();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      return registerApplicationMaster(request, null);
    } catch (InterruptedException ex) {
      throw new YarnException(ex);
    }
  }

  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request, Integer rpcID)
      throws YarnException, IOException, InterruptedException {
    ApplicationAttemptId applicationAttemptId = authorizeRequest();


     ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM,
          "Application doesn't exist in cache " + applicationAttemptId,
          "ApplicationMasterService", "Error in registering application master",
          appID, applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        String message = "Application Master is already registered : " +
            applicationAttemptId.getApplicationId();
        LOG.warn(message);
        RMAuditLogger.logFailure(this.rmContext.getRMApps()
                .get(applicationAttemptId.getApplicationId()).getUser(),
            AuditConstants.REGISTER_AM, "", "ApplicationMasterService", message,
            applicationAttemptId.getApplicationId(), applicationAttemptId);
        throw new InvalidApplicationMasterRequestException(message);
        //TORECOVER OPT save request response and resend it if the request is received again after a recover
      }
      
    
    if (rpcID == null) {
      rpcID = HopYarnAPIUtilities.getRPCID();
      byte[] regAMRequestData =
          ((RegisterApplicationMasterRequestPBImpl) request).getProto().
              toByteArray();

      RMUtilities.persistAppMasterRPC(rpcID, RPC.Type.RegisterApplicationMaster,
          regAMRequestData, applicationAttemptId.toString());
      
    }
    TransactionState transactionState = rmContext.getTransactionStateManager().getCurrentTransactionStateNonPriority(rpcID,
                    "registerApplicationMaster");
    

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      RMApp app = this.rmContext.getRMApps().get(appID);

      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);
      ((TransactionStateImpl) transactionState)
          .addAllocateResponse(applicationAttemptId, lock);
      LOG.debug("AM registration " + applicationAttemptId);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRegistrationEvent(applicationAttemptId,
              request.getHost(), request.getRpcPort(), request.
              getTrackingUrl(), transactionState));
      RMAuditLogger.logSuccess(app.getUser(), AuditConstants.REGISTER_AM,
          "ApplicationMasterService", appID, applicationAttemptId);

      // Pick up min/max resource from scheduler...
      RegisterApplicationMasterResponse response = recordFactory
          .newRecordInstance(RegisterApplicationMasterResponse.class);
      response.setMaximumResourceCapability(
          rScheduler.getMaximumResourceCapability());
      response.setApplicationACLs(
          app.getRMAppAttempt(applicationAttemptId).getSubmissionContext()
              .getAMContainerSpec().getApplicationACLs());
      response.setQueue(app.getQueue());
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Setting client token master key");
        response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(
            rmContext.getClientToAMTokenSecretManager()
                .getMasterKey(applicationAttemptId).getEncoded()));
      }

      // For work-preserving AM restart, retrieve previous attempts' containers
      // and corresponding NM tokens.
      List<Container> transferredContainers =
          ((AbstractYarnScheduler) rScheduler)
              .getTransferredContainers(applicationAttemptId);
      if (!transferredContainers.isEmpty()) {
        response.setContainersFromPreviousAttempts(transferredContainers);
        List<NMToken> nmTokens = new ArrayList<NMToken>();
        for (Container container : transferredContainers) {
          try {
            nmTokens.add(rmContext.getNMTokenSecretManager()
                .createAndGetNMToken(app.getUser(), applicationAttemptId,
                    container));
          } catch (IllegalArgumentException e) {
            // if it's a DNS issue, throw UnknowHostException directly and that
            // will be automatically retried by RMProxy in RPC layer.
            if (e.getCause() instanceof UnknownHostException) {
              transactionState.decCounter(TransactionState.TransactionType.INIT);
              throw (UnknownHostException) e.getCause();
            }
          }
        }
        response.setNMTokensFromPreviousAttempts(nmTokens);
        LOG.info("Application " + appID + " retrieved " +
            transferredContainers.size() + " containers from previous" +
            " attempts and " + nmTokens.size() + " NM tokens.");
      }
      transactionState.decCounter(TransactionState.TransactionType.INIT);
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      return finishApplicationMaster(request, null);
    } catch (InterruptedException ex) {
      throw new YarnException(ex);
    }
  }

  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request, Integer rpcID)
      throws YarnException, IOException, InterruptedException {

    ApplicationAttemptId applicationAttemptId = authorizeRequest();


    if (rpcID == null) {
      rpcID = HopYarnAPIUtilities.getRPCID();
      byte[] finAMRequestData =
          ((FinishApplicationMasterRequestPBImpl) request).getProto().
              toByteArray();

      RMUtilities.persistAppMasterRPC(rpcID, RPC.Type.FinishApplicationMaster,
          finAMRequestData, applicationAttemptId.toString());
      
    }
    TransactionState transactionState = 
          rmContext.getTransactionStateManager().getCurrentTransactionStateNonPriority(rpcID,
                    "finishApplicationMaster");
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      transactionState.decCounter(TransactionState.TransactionType.INIT);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: " +
                applicationAttemptId.getApplicationId();
        LOG.error(message);
        RMAuditLogger.logFailure(this.rmContext.getRMApps()
                .get(applicationAttemptId.getApplicationId()).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, applicationAttemptId.getApplicationId(),
            applicationAttemptId);
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        throw new InvalidApplicationMasterRequestException(message);
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      RMApp rmApp = rmContext.getRMApps().get(applicationAttemptId.
          getApplicationId());

      if (rmApp.isAppFinalStateStored()) {
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        return FinishApplicationMasterResponse.newInstance(true);
      }

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptUnregistrationEvent(applicationAttemptId,
              request.getTrackingUrl(), request.getFinalApplicationStatus(),
              request.getDiagnostics(), transactionState));

      // For UnmanagedAMs, return true so they don't retry
      transactionState.decCounter(TransactionState.TransactionType.INIT);
      return FinishApplicationMasterResponse.newInstance(
          rmApp.getApplicationSubmissionContext().getUnmanagedAM());
    }
  }

  private void throwApplicationDoesNotExistInCacheException(
      ApplicationAttemptId appAttemptId)
      throws InvalidApplicationMasterRequestException {
    String message = "Application doesn't exist in cache " + appAttemptId;
    LOG.error(message);
    throw new InvalidApplicationMasterRequestException(message);
  }

  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponseLock lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getAllocateResponse() != null &&
            lastResponse.getAllocateResponse().getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    try {
      return allocate(request, null);
    } catch (InterruptedException ex) {
      throw new YarnException(ex);
    }
  }

  public AllocateResponse allocate(AllocateRequest request, Integer rpcID)
      throws YarnException, IOException, InterruptedException {

    ApplicationAttemptId appAttemptId = authorizeRequest();


    if (rpcID == null) {
      rpcID = HopYarnAPIUtilities.getRPCID();
      byte[] allAMRequestData = ((AllocateRequestPBImpl) request).getProto().
              toByteArray();

      List<String> releaseList = new ArrayList<String>();
      for (ContainerId containerId : request.getReleaseList()) {
        releaseList.add(containerId.toString());
      }

      Map<String, byte[]> ask = new HashMap<String, byte[]>();
      for (ResourceRequest resourceRequest : request.getAskList()) {
        resourceRequest.getCapability();

        ask.put(resourceRequest.getResourceName(),
                ((ResourceRequestPBImpl) resourceRequest).getProto().
                toByteArray());
      }

      Map<String, byte[]> resourceIncreaseRequest
              = new HashMap<String, byte[]>();
      for (ContainerResourceIncreaseRequest incRequest : request.
              getIncreaseRequests()) {
        resourceIncreaseRequest.put(incRequest.getContainerId().toString(),
                ((ContainerResourceIncreaseRequestPBImpl) incRequest).getProto().
                toByteArray());
      }

      List<String> blackListAddition = new ArrayList<String>();
      List<String> blackListRemovals = new ArrayList<String>();
      if (request.getResourceBlacklistRequest() != null) {
        blackListAddition.addAll(request.getResourceBlacklistRequest().
                getBlacklistAdditions());

        blackListRemovals.addAll(request.getResourceBlacklistRequest().
                getBlacklistRemovals());
      }
      AllocateRPC rpc = new AllocateRPC(rpcID, request.getResponseId(), request.
              getProgress(), releaseList, ask, resourceIncreaseRequest,
              blackListAddition, blackListRemovals);

      RMUtilities
              .persistAllocateRPC(rpc, appAttemptId.toString());

    }
    TransactionState transactionState = 
           rmContext.getTransactionStateManager().getCurrentTransactionStateNonPriority(rpcID,
                    "allocate");
    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /*
     * check if its in cache
     */
    AllocateResponseLock lock = responseMap.get(appAttemptId);
    if (lock == null) {
      LOG.error("AppAttemptId doesnt exist in cache " + appAttemptId);
      transactionState.decCounter(TransactionState.TransactionType.INIT);
      return resync;
    }
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (!hasApplicationMasterRegistered(appAttemptId)) {
        String message =
            "Application Master is trying to allocate before registering for: " +
                appAttemptId.getApplicationId();
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps().get(appAttemptId.getApplicationId())
                .getUser(), AuditConstants.REGISTER_AM, "",
            "ApplicationMasterService", message,
            appAttemptId.getApplicationId(), appAttemptId);
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        throw new InvalidApplicationMasterRequestException(message);
      }

      if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
        /*
         * old heartbeat
         */
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        return lastResponse;
      } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
        LOG.error("Invalid responseid from appAttemptId " + appAttemptId);
        // Oh damn! Sending reboot isn't enough. RM state is corrupted. TODO:
        // Reboot is not useful since after AM reboots, it will send register
        // and
        // get an exception. Might as well throw an exception here.
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        return resync;
      }

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request.getProgress(),
              transactionState));
      
      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();

      ResourceBlacklistRequest blacklistRequest =
          request.getResourceBlacklistRequest();
      List<String> blacklistAdditions = (blacklistRequest != null) ?
          blacklistRequest.getBlacklistAdditions() : Collections.EMPTY_LIST;
      List<String> blacklistRemovals =
          (blacklistRequest != null) ? blacklistRequest.getBlacklistRemovals() :
              Collections.EMPTY_LIST;

      // sanity check
      try {
        RMServerUtils.validateResourceRequests(ask,
            rScheduler.getMaximumResourceCapability());
      } catch (InvalidResourceRequestException e) {
        LOG.warn("Invalid resource ask by application " + appAttemptId, e);
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        throw e;
      }

      try {
        RMServerUtils.validateBlacklistRequest(blacklistRequest);
      } catch (InvalidResourceBlacklistRequestException e) {
        LOG.warn("Invalid blacklist request by application " + appAttemptId, e);
        transactionState.decCounter(TransactionState.TransactionType.INIT);
        throw e;
      }

      RMApp app =
          this.rmContext.getRMApps().get(appAttemptId.getApplicationId());
      // In the case of work-preserving AM restart, it's possible for the
      // AM to release containers from the earlier attempt.
      if (!app.getApplicationSubmissionContext()
          .getKeepContainersAcrossApplicationAttempts()) {
        try {
          RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
        } catch (InvalidContainerReleaseException e) {
          LOG.
              warn("Invalid container release by application " + appAttemptId,
                  e);
          transactionState.decCounter(TransactionState.TransactionType.INIT);
          throw e;
        }
      }

      // Send new requests to appAttempt.
      LOG.debug("allocate on AM request");
      Allocation allocation = this.rScheduler
          .allocate(appAttemptId, ask, release, blacklistAdditions,
              blacklistRemovals, transactionState);

      if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
        LOG.info(
            "blacklist are updated in Scheduler." + "blacklistAdditions: " +
                blacklistAdditions + ", " + "blacklistRemovals: " +
                blacklistRemovals);
      }
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
      AllocateResponse allocateResponse =
          recordFactory.newRecordInstance(AllocateResponse.class);
      if (!allocation.getContainers().isEmpty()) {
        allocateResponse.setNMTokens(allocation.getNMTokens());
      }

      // update the response with the deltas of node status changes
      List<RMNode> updatedNodes = new ArrayList<RMNode>();
      if (app.pullRMNodeUpdates(updatedNodes, transactionState) > 0) {
        List<NodeReport> updatedNodeReports = new ArrayList<NodeReport>();
        for (RMNode rmNode : updatedNodes) {
          SchedulerNodeReport schedulerNodeReport =
              rScheduler.getNodeReport(rmNode.getNodeID());
          Resource used = BuilderUtils.newResource(0, 0);
          int numContainers = 0;
          if (schedulerNodeReport != null) {
            used = schedulerNodeReport.getUsedResource();
            numContainers = schedulerNodeReport.getNumContainers();
          }
          NodeReport report = BuilderUtils
              .newNodeReport(rmNode.getNodeID(), rmNode.getState(),
                  rmNode.getHttpAddress(), rmNode.getRackName(), used,
                  rmNode.getTotalCapability(), numContainers,
                  rmNode.getHealthReport(), rmNode.getLastHealthReportTime());

          updatedNodeReports.add(report);
        }
        allocateResponse.setUpdatedNodes(updatedNodeReports);
      }

      allocateResponse.setAllocatedContainers(allocation.getContainers());
      allocateResponse.setCompletedContainersStatuses(
          appAttempt.pullJustFinishedContainers(transactionState));
      allocateResponse.setResponseId(lastResponse.getResponseId() + 1);
      allocateResponse.setAvailableResources(allocation.getResourceLimit());

      allocateResponse.setNumClusterNodes(this.rScheduler.getNumClusterNodes());

      // add preemption to the allocateResponse message (if any)
      allocateResponse
          .setPreemptionMessage(generatePreemptionMessage(allocation));

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      lock.setAllocateResponse(allocateResponse);
      ((TransactionStateImpl) transactionState).
          addAllocateResponse(appAttemptId, lock);
      transactionState.decCounter(TransactionState.TransactionType.INIT);
      return allocateResponse;
    }
  }

  private PreemptionMessage generatePreemptionMessage(Allocation allocation) {
    PreemptionMessage pMsg = null;
    // assemble strict preemption request
    if (allocation.getStrictContainerPreemptions() != null) {
      pMsg = recordFactory.newRecordInstance(PreemptionMessage.class);
      StrictPreemptionContract pStrict =
          recordFactory.newRecordInstance(StrictPreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getStrictContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      pStrict.setContainers(pCont);
      pMsg.setStrictContract(pStrict);
    }

    // assemble negotiable preemption request
    if (allocation.getResourcePreemptions() != null &&
        allocation.getResourcePreemptions().size() > 0 &&
        allocation.getContainerPreemptions() != null &&
        allocation.getContainerPreemptions().size() > 0) {
      if (pMsg == null) {
        pMsg = recordFactory.newRecordInstance(PreemptionMessage.class);
      }
      PreemptionContract contract =
          recordFactory.newRecordInstance(PreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      List<PreemptionResourceRequest> pRes =
          new ArrayList<PreemptionResourceRequest>();
      for (ResourceRequest crr : allocation.getResourcePreemptions()) {
        PreemptionResourceRequest prr =
            recordFactory.newRecordInstance(PreemptionResourceRequest.class);
        prr.setResourceRequest(crr);
        pRes.add(prr);
      }
      contract.setContainers(pCont);
      contract.setResourceRequest(pRes);
      pMsg.setContract(contract);
    }

    return pMsg;
  }

  public void recoverAllocateResponse(ApplicationAttemptId attemptId,
          AllocateResponse allocateResponse, RMStateStore.RMState state) throws
          IOException {
    if (allocateResponse != null) {
      List<NMToken> allocatedNMTokens
              = new ArrayList<NMToken>();
      for (org.apache.hadoop.yarn.api.records.Container container
              : allocateResponse.
              getAllocatedContainers()) {
        NMToken nmToken = rmContext.getNMTokenSecretManager()
                .createAndGetNMToken(state.getAppSchedulingInfo(
                                attemptId.getApplicationId().toString()).get(
                                attemptId.toString()).
                        getUser(),
                        attemptId,
                        container);
        if (nmToken != null) {
          LOG.debug("set allocated nm token for: " + attemptId + ", " + nmToken);
          allocatedNMTokens.add(nmToken);
        }
      }
      allocateResponse.setNMTokens(allocatedNMTokens);
      LOG.debug(
              "recovering AllocateResponse " + attemptId + " "
              + allocateResponse);
      responseMap.get(attemptId).setAllocateResponse(allocateResponse);
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId,
      TransactionState transactionState) {
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(-1);
    LOG.info("Registering app attempt : " + attemptId);
    AllocateResponseLock lock = new AllocateResponseLock(response);
    responseMap.put(attemptId, lock);
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .addAllocateResponse(attemptId, lock);
    }
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId,
      TransactionState transactionState) {
    LOG.info("Unregistering app attempt : " + attemptId);
    AllocateResponseLock lock = responseMap.remove(attemptId);
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState)
          .removeAllocateResponse(attemptId, lock.getAllocateResponse().getResponseId());
    }
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }

  public static class AllocateResponseLock {

    private AllocateResponse response;

    public AllocateResponseLock(AllocateResponse response) {
      this.response = response;
    }

    public synchronized AllocateResponse getAllocateResponse() {
      return response;
    }

    public synchronized void setAllocateResponse(AllocateResponse response) {
      this.response = response;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
}
