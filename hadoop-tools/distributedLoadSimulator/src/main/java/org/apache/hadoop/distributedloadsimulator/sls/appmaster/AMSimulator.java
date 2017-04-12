/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.distributedloadsimulator.sls.appmaster;

/**
 *
 * @author sri
 */
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.distributedloadsimulator.sls.AMNMCommonObject;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.SLSRunner;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.ConfiguredLeaderFailoverHAProxyProvider;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

public abstract class AMSimulator extends TaskRunner.Task {

  // resource manager
  protected ResourceManager rm;
  // main
  protected SLSRunner se;
  // application
  protected ApplicationId appId;
  protected ApplicationAttemptId appAttemptId;
  protected String oldAppId;    // jobId from the jobhistory file
  // record factory
  protected final static RecordFactory recordFactory
          = RecordFactoryProvider.getRecordFactory(null);
  // response queue
  protected final BlockingQueue<AllocateResponse> responseQueue;
  protected static int RESPONSE_ID = 1;
  // user name
  protected String user;
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS;
  // whether tracked in Metrics
  protected boolean isTracked;
  // progress
  protected int totalContainers;
  protected int finishedContainers;

  private String machineIp;

  protected final Logger LOG = Logger.getLogger(AMSimulator.class);
  private int amId;
  protected AMNMCommonObject primaryRemoteConnection;
  protected List<AMNMCommonObject> RemoteConnections
          = new ArrayList<AMNMCommonObject>();
  private ApplicationClientProtocol applicationClient;
  private YarnClient rmClient;
  private boolean amCompleted = false;
  private static final long AM_STATE_WAIT_TIMEOUT_MS = 600000;
  protected Configuration conf;
  Token<AMRMTokenIdentifier> amRMToken;
  Credentials credentials;
  boolean submited = false;
  protected long applicationStartTime;

  public AMSimulator() {
    this.responseQueue = new LinkedBlockingQueue<AllocateResponse>();
  }

  public void init(int id, int heartbeatInterval,
          List task, ResourceManager rm, SLSRunner se,
          long traceStartTime, long traceFinishTime, String user, String queue,
          boolean isTracked, String oldAppId, String[] listOfRemoteSimIp, int rmiPort,
          YarnClient rmClient, Configuration conf) throws IOException {
    super.init(traceStartTime, traceStartTime + 1000000L * heartbeatInterval,
            heartbeatInterval);
    this.conf = conf;
    conf.setClass(YarnConfiguration.LEADER_CLIENT_FAILOVER_PROXY_PROVIDER,
            ConfiguredLeaderFailoverHAProxyProvider.class,
            RMFailoverProxyProvider.class);

    this.user = user;
    this.amId = id;
    this.rm = rm;
    this.se = se;
    this.user = user;
    this.queue = queue;
    this.oldAppId = oldAppId;
    this.isTracked = isTracked;
    this.traceStartTimeMS = traceStartTime;
    this.traceFinishTimeMS = traceFinishTime;
    this.applicationClient = ClientRMProxy
            .createRMProxy(this.conf, ApplicationClientProtocol.class, true);
    this.rmClient = rmClient;

    Registry primaryRegistry;
    Registry secondryRegistry;
    try {
      primaryRegistry = LocateRegistry.getRegistry("127.0.0.1", rmiPort);
      primaryRemoteConnection = (AMNMCommonObject) primaryRegistry.lookup(
              "AMNMCommonObject");
      RemoteConnections.add(primaryRemoteConnection);
    } catch (RemoteException ex) {
      LOG.error("Remote exception:", ex);
    } catch (NotBoundException ex) {
      LOG.error("Unable to bind exception:", ex);
    }

    //Note: no need to put sleep here, because , remote connection is already up in this point, so just go and 
    //get the connection
    // if ip is 127.0.0.1 , then we are considering as standalone simulator so dont try to create rmi connection with other
    // simulator.
    for (String remoteIp : listOfRemoteSimIp) {
      if (!(remoteIp.equals("127.0.0.1"))) {
        try {
          secondryRegistry = LocateRegistry.getRegistry(remoteIp, rmiPort);
          AMNMCommonObject secondryConnections
                  = (AMNMCommonObject) secondryRegistry.lookup(
                          "AMNMCommonObject");
          RemoteConnections.add(secondryConnections);
        } catch (RemoteException ex) {
          LOG.error("Remote exception at AMSimulator :", ex);
        } catch (NotBoundException ex) {
          LOG.error("Unable to bind exception:", ex);
        }
      }
    }

  }

  /**
   * register with RM
   *
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   * @throws java.lang.InterruptedException
   */
  @Override
  public void firstStep()
          throws YarnException, IOException, InterruptedException {
    simulateStartTimeMS = System.currentTimeMillis()
            - SLSRunner.getApplicationRunner().getStartTimeMS();

    // submit application, waiting until ACCEPTED
    submitApp();
    if (submited) {
      // register application master
      registerAM();

      // track app metrics
      trackApp();
    }
  }

  @Override
  public void middleStep()
          throws InterruptedException, YarnException, IOException {
    // process responses in the queue
    processResponseQueue();

    // send out request
    sendContainerRequest();

    // check whether finish
    checkStop();
  }

  public String getMachineIp() {
    try {
      machineIp = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException ex) {
      java.util.logging.Logger.getLogger(AMSimulator.class.getName()).log(
              Level.SEVERE, null, ex);
    }

    return machineIp;

  }

  @Override
  public void lastStep() throws YarnException {

    LOG.info(MessageFormat.format(
            "Simulation is done from {0}  and Application {1} is going to be killed!",
            getMachineIp(), appId));
    // unregister tracking
    if (isTracked) {
      untrackApp();
    }
    // unregister application master
    final FinishApplicationMasterRequest finishAMRequest = recordFactory
            .newRecordInstance(FinishApplicationMasterRequest.class);
    finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    try {

      UserGroupInformation ugi = UserGroupInformation.createProxyUser(
              appAttemptId.toString(), UserGroupInformation.getCurrentUser(),
              false);
      ugi.setAuthenticationMethod(SaslRpcServer.AuthMethod.TOKEN);
      ugi.addCredentials(credentials);
      ugi.addToken(amRMToken);
      ugi.addTokenIdentifier(amRMToken.decodeIdentifier());
      ugi.doAs(
              new PrivilegedExceptionAction<FinishApplicationMasterResponse>() {

                @Override
                public FinishApplicationMasterResponse run() throws Exception {
                  UserGroupInformation.getCurrentUser().addToken(amRMToken);
                  InetSocketAddress resourceManagerAddress = conf.
                  getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
                  SecurityUtil.setTokenService(amRMToken,
                          resourceManagerAddress);
                  ApplicationMasterProtocol appMasterProtocol = ClientRMProxy.
                  createRMProxy(conf, ApplicationMasterProtocol.class, true);
                  appMasterProtocol.finishApplicationMaster(finishAMRequest);
                  RPC.stopProxy(appMasterProtocol);
                  return null;
                }
              });

    } catch (IOException ex) {
      LOG.error("Exception in calling finish applicatoin master ", ex);
    } catch (InterruptedException ex) {
      LOG.error(ex, ex);
    }
    if (rm != null) {
      simulateFinishTimeMS = System.currentTimeMillis()
              - SLSRunner.getApplicationRunner().getStartTimeMS();
    }
    try {
      primaryRemoteConnection.decreseApplicationCount(appId.toString(), false);
    } catch (RemoteException ex) {
      java.util.logging.Logger.getLogger(AMSimulator.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

  protected ResourceRequest createResourceRequest(
          Resource resource, String host, int priority, int numContainers) {
    ResourceRequest request = recordFactory
            .newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
          List<ContainerId> toRelease) {
    AllocateRequest allocateRequest
            = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setResponseId(RESPONSE_ID++);
    allocateRequest.setAskList(ask);
    allocateRequest.setReleaseList(toRelease);
    return allocateRequest;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
    return createAllocateRequest(ask, new ArrayList<ContainerId>());
  }

  protected abstract void processResponseQueue()
          throws InterruptedException, YarnException, IOException;

  protected abstract void sendContainerRequest()
          throws YarnException, IOException, InterruptedException;

  protected abstract void checkStop();

  public void registerAM() throws YarnException, IOException,
          InterruptedException {
    final RegisterApplicationMasterRequest amRegisterRequest
            = Records.newRecord(RegisterApplicationMasterRequest.class);
    amRegisterRequest.setHost("localhost");
    amRegisterRequest.setRpcPort(1000);
    amRegisterRequest.setTrackingUrl("localhost:1000");
    LOG.info("register application master for " + appId);

    UserGroupInformation ugi = UserGroupInformation.createProxyUser(
            appAttemptId.toString(), UserGroupInformation.getCurrentUser(),
            false);
    ugi.setAuthenticationMethod(SaslRpcServer.AuthMethod.TOKEN);
    ugi.addCredentials(credentials);
    ugi.addToken(amRMToken);
    ugi.addTokenIdentifier(amRMToken.decodeIdentifier());

    ugi.doAs(
            new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {

              @Override
              public RegisterApplicationMasterResponse run() throws Exception {
                UserGroupInformation.getCurrentUser().addToken(amRMToken);
                InetSocketAddress resourceManagerAddress = conf.getSocketAddr(
                        YarnConfiguration.RM_SCHEDULER_ADDRESS,
                        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
                SecurityUtil.
                setTokenService(amRMToken, resourceManagerAddress);
                ApplicationMasterProtocol appMasterProtocol = ClientRMProxy.
                createRMProxy(conf, ApplicationMasterProtocol.class, true);
                RegisterApplicationMasterResponse response
                = appMasterProtocol.registerApplicationMaster(
                        amRegisterRequest);
                RPC.stopProxy(appMasterProtocol);
                return response;
              }
            });

    String whichMachine = InetAddress.getLocalHost().getHostAddress();
    LOG.info("Registered application master from  : " + whichMachine
            + " appid : " + appId + " success");
    primaryRemoteConnection.registerApplicationTimeStamp();
  }

  private void submitApp()
          throws YarnException, InterruptedException, IOException {
    // ask for new application
    GetNewApplicationRequest newAppRequest = Records.newRecord(
            GetNewApplicationRequest.class);
    LOG.info("get new applications " + oldAppId);
    applicationStartTime = System.currentTimeMillis();
    GetNewApplicationResponse newAppResponse = applicationClient.
            getNewApplication(newAppRequest);
    appId = newAppResponse.getApplicationId();
    LOG.info("got new applications for " + oldAppId + " id: " + appId);
    // submit the application
    final SubmitApplicationRequest subAppRequest = Records.newRecord(
            SubmitApplicationRequest.class);
    ApplicationSubmissionContext appSubContext = Records.newRecord(
            ApplicationSubmissionContext.class);
    appSubContext.setApplicationId(appId);
    appSubContext.setMaxAppAttempts(1);
    appSubContext.setQueue(queue);
    appSubContext.setPriority(Priority.newInstance(0));
    ContainerLaunchContext conLauContext = Records.newRecord(
            ContainerLaunchContext.class);
    conLauContext.setApplicationACLs(
            new HashMap<ApplicationAccessType, String>());
    conLauContext.setCommands(new ArrayList<String>());
    conLauContext.setEnvironment(new HashMap<String, String>());
    conLauContext.setLocalResources(new HashMap<String, LocalResource>());
    conLauContext.setServiceData(new HashMap<String, ByteBuffer>());
    appSubContext.setAMContainerSpec(conLauContext);
    appSubContext.setUnmanagedAM(true);
    subAppRequest.setApplicationSubmissionContext(appSubContext);
    SubmitApplicationResponse submitesponse = applicationClient.
            submitApplication(subAppRequest);

    LOG.info(MessageFormat.format("Submit a new application {0}", appId));
    LOG.info("wait for app to be accepted " + appId);
    ApplicationReport appReport
            = monitorApplication(appId, EnumSet.
                    of(YarnApplicationState.ACCEPTED,
                            YarnApplicationState.KILLED,
                            YarnApplicationState.FAILED,
                            YarnApplicationState.FINISHED));

    if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
      LOG.info("app accepted " + appId);
      // Monitor the application attempt to wait for launch state
      ApplicationAttemptReport attemptReport
              = monitorCurrentAppAttempt(appId,
                      YarnApplicationAttemptState.LAUNCHED);
      if (attemptReport != null) {

        appAttemptId
                = attemptReport.getApplicationAttemptId();

        credentials = new Credentials();
        amRMToken
                = rmClient.getAMRMToken(appAttemptId.getApplicationId());
        LOG.info("got token " + appId + " " + amRMToken);
        // Service will be empty but that's okay, we are just passing down only
        // AMRMToken down to the real AM which eventually sets the correct
        // service-address.
        credentials.addToken(amRMToken.getService(), amRMToken);

        UserGroupInformation
                .getCurrentUser().addToken(amRMToken);
        InetSocketAddress resourceManagerAddress = conf.getSocketAddr(
                YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
        SecurityUtil.setTokenService(amRMToken, resourceManagerAddress);
//        this.appMasterProtocol = createSchedulerProxy();
        LOG.info("HOP ::  Launched application master appAttempId : "
                + appAttemptId.getApplicationId());
        // launch AM
        submited = true;
        return;
      }
    }
    try {
      primaryRemoteConnection.decreseApplicationCount(appId.toString(), true);
    } catch (RemoteException ex) {
      java.util.logging.Logger.getLogger(AMSimulator.class.getName()).log(
              Level.SEVERE, null, ex);
    }
  }

  public void trackApp() {
    if (isTracked) {
      // if we are running load simulator alone, rm is null
      if (rm != null) {
      }
    }
  }

  public void untrackApp() {
    if (isTracked) {
      // if we are running load simulator alone, rm is null
      if (rm != null) {
      }
    }
  }

  protected List<ResourceRequest> packageRequests(
          List<ContainerSimulator> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap
            = new HashMap<String, ResourceRequest>();
    Map<String, ResourceRequest> nodeLocalRequestMap
            = new HashMap<String, ResourceRequest>();
    ResourceRequest anyRequest = null;
    for (ContainerSimulator cs : csList) {
      String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
      // check rack local
      String rackname = rackHostNames[0];
      if (rackLocalRequestMap.containsKey(rackname)) {
        rackLocalRequestMap.get(rackname).setNumContainers(
                rackLocalRequestMap.get(rackname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), rackname, priority, 1);
        rackLocalRequestMap.put(rackname, request);
      }
      // check node local
      String hostname = rackHostNames[1];
      if (nodeLocalRequestMap.containsKey(hostname)) {
        nodeLocalRequestMap.get(hostname).setNumContainers(
                nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), hostname, priority, 1);
        nodeLocalRequestMap.put(hostname, request);
      }
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(
                cs.getResource(), ResourceRequest.ANY, priority, 1);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.addAll(nodeLocalRequestMap.values());
    ask.addAll(rackLocalRequestMap.values());
    if (anyRequest != null) {
      ask.add(anyRequest);
    }
    return ask;
  }

  public String getQueue() {
    return queue;
  }

  public String getAMType() {
    return amtype;
  }

  public long getDuration() {
    return simulateFinishTimeMS - simulateStartTimeMS;
  }

  public int getNumTasks() {
    return totalContainers;
  }

  /**
   * Monitor the submitted application for completion. Kill application if time
   * expires.
   *
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private ApplicationReport monitorApplication(ApplicationId appId,
          Set<YarnApplicationState> finalState) throws YarnException,
          IOException {

    long foundAMCompletedTime = 0;
    StringBuilder expectedFinalState = new StringBuilder();
    boolean first = true;
    for (YarnApplicationState state : finalState) {
      if (first) {
        first = false;
        expectedFinalState.append(state.name());
      } else {
        expectedFinalState.append("," + state.name());
      }
    }

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = rmClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
              + appId.getId() + ", appAttemptId="
              + report.getCurrentApplicationAttemptId() + ", clientToAMToken="
              + report.getClientToAMToken() + ", appDiagnostics="
              + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
              + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
              + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
              + ", yarnAppState=" + report.getYarnApplicationState().toString()
              + ", distributedFinalState="
              + report.getFinalApplicationStatus().toString()
              + ", appTrackingUrl="
              + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        return report;
      }

      // wait for 10 seconds after process has completed for app report to
      // come back
      if (amCompleted) {
        if (foundAMCompletedTime == 0) {
          foundAMCompletedTime = System.currentTimeMillis();
        } else if ((System.currentTimeMillis() - foundAMCompletedTime)
                > AM_STATE_WAIT_TIMEOUT_MS) {
          LOG.warn("Waited " + AM_STATE_WAIT_TIMEOUT_MS / 1000
                  + " seconds after process completed for AppReport"
                  + " to reach desired final state. Not waiting anymore."
                  + "CurrentState = " + state
                  + ", ExpectedStates = " + expectedFinalState.toString());
          throw new RuntimeException("Failed to receive final expected state"
                  + " in ApplicationReport"
                  + ", CurrentState=" + state
                  + ", ExpectedStates=" + expectedFinalState.toString());
        }
      }
    }
  }

  private ApplicationAttemptReport monitorCurrentAppAttempt(
          ApplicationId appId, YarnApplicationAttemptState attemptState)
          throws YarnException, IOException {
    long startTime = System.currentTimeMillis();
    ApplicationAttemptId attemptId = null;
    while (true) {
      if (attemptId == null) {
        attemptId
                = rmClient.getApplicationReport(appId)
                .getCurrentApplicationAttemptId();
      }
      ApplicationAttemptReport attemptReport = null;
      if (attemptId != null) {
        attemptReport = rmClient.getApplicationAttemptReport(attemptId);
        if (attemptState.equals(attemptReport.getYarnApplicationAttemptState())) {
          return attemptReport;
        }
      }
      LOG.info("Current attempt state of " + appId + " is " + (attemptReport
              == null
                      ? " N/A " : attemptReport.getYarnApplicationAttemptState())
              + ", waiting for current attempt to reach " + attemptState);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for current attempt of " + appId
                + " to reach " + attemptState);
      }
      if (System.currentTimeMillis() - startTime > AM_STATE_WAIT_TIMEOUT_MS) {
        long wait = System.currentTimeMillis() - startTime;
        String errmsg
                = "Timeout for waiting current attempt of " + appId
                + " to reach "
                + attemptState + " waited " + wait;
        LOG.error(errmsg);
        return null;
        //throw new RuntimeException(errmsg);
      }
    }
  }
  
    long totalContainersDuration=0;
    
  public long getTotalContainersDuration(){
    return totalContainersDuration;
  }
}
