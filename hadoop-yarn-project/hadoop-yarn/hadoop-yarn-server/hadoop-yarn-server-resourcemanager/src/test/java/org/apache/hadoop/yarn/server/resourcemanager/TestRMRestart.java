/*
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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreAMRMTokenEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreRMDTEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreRMDTMasterKeyEvent;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRMRestart extends ParameterizedSchedulerTestBase {
  private static final Log LOG = LogFactory.getLog(TestRMRestart.class);
  private final static File TEMP_DIR = new File(System.getProperty(
    "test.build.data", "/tmp"), "decommision");
  private File hostFile = new File(TEMP_DIR + File.separator + "hostFile.txt");
  private YarnConfiguration conf;

  // Fake rmAddr for token-renewal
  private static InetSocketAddress rmAddr;
  private List<MockRM> rms = new ArrayList<MockRM>();
  private FileSystem fs;
  private Path tmpDir;
  
  public TestRMRestart(SchedulerType type) {
    super(type);
  }

  @Before
  public void setup() throws Exception {
    conf = getConf();
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    UserGroupInformation.setConfiguration(conf);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);
    fs = FileSystem.get(conf);
    tmpDir = new Path(new File("target", this.getClass().getSimpleName()
        + "-tmpDir").getAbsolutePath());
    fs.delete(tmpDir, true);
    fs.mkdirs(tmpDir);
    conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI,tmpDir.toString());
    conf.set(YarnConfiguration.RM_STORE, FileSystemRMStateStore.class.getName());
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
    rmAddr = new InetSocketAddress("localhost", 8032);
    Assert.assertTrue(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS > 1);
  }

  @After
  public void tearDown() throws IOException {
    for (MockRM rm : rms) {
      rm.stop();
    }
    rms.clear();
    fs.delete(tmpDir, true);
    TEMP_DIR.delete();
  }

  /**
   *
   * @return a new MockRM that will be stopped at the end of the test.
   */
  private MockRM createMockRM(YarnConfiguration conf, RMStateStore store) {
    MockRM rm = new MockRM(conf, store);
    rms.add(rm);
    return rm;
  }

  private MockRM createMockRM(YarnConfiguration conf) {
    MockRM rm = new MockRM(conf);
    rms.add(rm);
    return rm;
  }
  
  @Test
  public void testRMRestartWithCryptoMaterial() throws Exception {
    conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    // This should not kick off certificate rotation
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "1ms");
    // This should not kick off JWT rotation
    conf.set(YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY, "1s");
    
    // Start RM
    MockRM rm1 = createMockRM(conf);
    rm1.start();
    MockNM nm1 = new MockNM("127.0.0.1:1337", 1024 * 10, rm1.getResourceTrackerService());
    nm1.registerNode();
    
    // Submit application
    RMApp app0 = rm1.submitApp(1024);
    nm1.nodeHeartbeat(true);
    
    // RMState should have the application with the crypto material
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState = rmState.getApplicationState();
    assertCryptoMaterialStateNotEmpty(rmAppState.get(app0.getApplicationId()));
    
    // Start second RM
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.nodeHeartbeat(true);
    Map<ApplicationId, RMApp> rm2Apps = rm2.getRMContext().getRMApps();
    Assert.assertEquals(1, rm2Apps.size());
    
    // Verify crypto material are recovered correctly
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    assertCryptoMaterialStateNotEmpty(rmAppState.get(app0.getApplicationId()));
    RMApp recoveredApp0 = rm2Apps.get(app0.getApplicationId());
    assertAppCryptoMaterialNotEmpty(recoveredApp0);
    
    Assert.assertArrayEquals(app0.getKeyStore(), recoveredApp0.getKeyStore());
    Assert.assertArrayEquals(app0.getKeyStorePassword(), recoveredApp0.getKeyStorePassword());
    Assert.assertArrayEquals(app0.getTrustStore(), recoveredApp0.getTrustStore());
    Assert.assertArrayEquals(app0.getTrustStorePassword(), recoveredApp0.getTrustStorePassword());
    Assert.assertEquals(app0.getJWT(), recoveredApp0.getJWT());
    Assert.assertEquals(app0.getJWTExpiration(), recoveredApp0.getJWTExpiration());
  }
  
  @SuppressWarnings("rawtypes")
  @Test (timeout=180000)
  public void testRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    // Do not kick off renewer for the duration of the test
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "1ms");
    
    
    // PHASE 1: create state in an RM
    
    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    
    // start like normal because state is empty
    rm1.start();
    
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    MockNM nm2 =
        new MockNM("127.0.0.2:5678", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode(); // nm2 will not heartbeat with RM1
    
    // create app that will finish and the final state should be saved.
    RMApp app0 = rm1.submitApp(200);
    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();
    // spot check that app is saved
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    Assert.assertEquals(1, rmAppState.size());
    nm1.nodeHeartbeat(true);
    MockAM am0 = rm1.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    finishApplicationMaster(app0, rm1, nm1, am0);

    // create app that gets launched and does allocate before RM restart
    RMApp app1 = rm1.submitApp(200);
    // assert app1 info is saved
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app1.getApplicationSubmissionContext()
        .getApplicationId());
    
    assertCryptoMaterialStateNotEmpty(appState);
    

    //kick the scheduling to allocate AM container
    nm1.nodeHeartbeat(true);
    
    // assert app1 attempt is saved
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    appState = rmAppState.get(app1.getApplicationId());
    Assert.assertEquals(1, appState.getAttemptCount());
    ApplicationAttemptStateData attemptState =
                                appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());
    
    // launch the AM
    MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // AM request for containers
    am1.allocate("127.0.0.1" , 1000, 1, new ArrayList<ContainerId>());   
    // kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    
    // create app that does not get launched by RM before RM restart
    RMApp app2 = rm1.submitApp(200);

    // assert app2 info is saved
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    appState = rmAppState.get(app2.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // create unmanaged app
    RMApp appUnmanaged = rm1.submitApp(200, "someApp", "someUser", null, true,
        null, conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
    ApplicationAttemptId unmanagedAttemptId = 
                        appUnmanaged.getCurrentAppAttempt().getAppAttemptId();
    // assert appUnmanaged info is saved
    ApplicationId unmanagedAppId = appUnmanaged.getApplicationId();
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    appState = rmAppState.get(unmanagedAppId);
    Assert.assertNotNull(appState);
    // wait for attempt to reach LAUNCHED state 
    rm1.waitForState(unmanagedAttemptId, RMAppAttemptState.LAUNCHED);
    rm1.waitForState(unmanagedAppId, RMAppState.ACCEPTED);
    // assert unmanaged attempt info is saved
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    appState = rmAppState.get(unmanagedAppId);
    Assert.assertEquals(1, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), appUnmanaged.getApplicationSubmissionContext()
        .getApplicationId());  
    
    // PHASE 2: create new RM and start from old state
    
    // create new RM to represent restart and recover state
    MockRM rm2 = createMockRM(conf);
    
    // start new RM
    rm2.start();
    
    // change NM to point to new RM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());

    // verify load of old state
    // 4 apps are loaded.
    // FINISHED app and attempt is also loaded back.
    // Unmanaged app state is still loaded back but it cannot be restarted by
    // the RM. this will change with work preserving RM restart in which AMs/NMs
    // are not rebooted.
    Assert.assertEquals(4, rm2.getRMContext().getRMApps().size());
    // check that earlier finished app and attempt is also loaded back and move
    // to finished state.
    rm2.waitForState(app0.getApplicationId(), RMAppState.FINISHED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    // verify correct number of attempts and other data
    RMApp loadedApp1 = rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    Assert.assertNotNull(loadedApp1);
    Assert.assertEquals(1, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(app1.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp1.getApplicationSubmissionContext()
        .getApplicationId());
    
    RMApp loadedApp2 = rm2.getRMContext().getRMApps().get(app2.getApplicationId());
    Assert.assertNotNull(loadedApp2);
    //Assert.assertEquals(0, loadedApp2.getAppAttempts().size());
    Assert.assertEquals(app2.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // verify state machine kicked into expected states
    rm2.waitForState(loadedApp1.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(loadedApp2.getApplicationId(), RMAppState.ACCEPTED);
    
    // verify attempts for apps
    // The app for which AM was started will wait for previous am
    // container finish event to arrive. However for an application for which
    // no am container was running will start new application attempt.
    Assert.assertEquals(1, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(1, loadedApp2.getAppAttempts().size());
    
    // Verify crypto material for recovered apps
    assertAppCryptoMaterialNotEmpty(loadedApp1);
    assertAppCryptoMaterialNotEmpty(loadedApp2);
    
    
    // verify old AM is not accepted
    // change running AM to talk to new RM
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    try {
      am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationAttemptNotFoundException e) {
      Assert.assertTrue(e instanceof ApplicationAttemptNotFoundException);
    }
    
    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    
    // new NM to represent NM re-register
    nm1 = new MockNM("127.0.0.1:1234", 15120, rm2.getResourceTrackerService());
    nm2 = new MockNM("127.0.0.2:5678", 15120, rm2.getResourceTrackerService());

    NMContainerStatus status =
        TestRMRestart
          .createNMContainerStatus(loadedApp1.getCurrentAppAttempt()
              .getAppAttemptId(), 1, ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(status), null);
    nm2.registerNode();
    
    rm2.waitForState(loadedApp1.getApplicationId(), RMAppState.ACCEPTED);
    // wait for the 2nd attempt to be started.
    int timeoutSecs = 0;
    while (loadedApp1.getAppAttempts().size() != 2 && timeoutSecs++ < 40) {;
      Thread.sleep(200);
    }

    // verify no more reboot response sent
    hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC != hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC != hbResponse.getNodeAction());
    
    // assert app1 attempt is saved
    attempt1 = loadedApp1.getCurrentAppAttempt();
    attemptId1 = attempt1.getAppAttemptId();
    rm2.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    appState = rmAppState.get(loadedApp1.getApplicationId());
    attemptState = appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());

    // Nodes on which the AM's run 
    MockNM am1Node = nm1;
    if (attemptState.getMasterContainer().getNodeId().toString()
        .contains("127.0.0.2")) {
      am1Node = nm2;
    }

    // assert app2 attempt is saved
    RMAppAttempt attempt2 = loadedApp2.getCurrentAppAttempt();
    ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
    rm2.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    appState = rmAppState.get(loadedApp2.getApplicationId());
    attemptState = appState.getAttempt(attemptId2);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId2, 1), 
                        attemptState.getMasterContainer().getId());

    MockNM am2Node = nm1;
    if (attemptState.getMasterContainer().getNodeId().toString()
        .contains("127.0.0.2")) {
      am2Node = nm2;
    }
    
    // start the AM's
    am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    
    MockAM am2 = rm2.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();

    //request for containers
    am1.allocate("127.0.0.1" , 1000, 3, new ArrayList<ContainerId>());
    am2.allocate("127.0.0.2" , 1000, 1, new ArrayList<ContainerId>());
    
    // verify container allocate continues to work
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    // finish the AMs
    finishApplicationMaster(loadedApp1, rm2, am1Node, am1);
    finishApplicationMaster(loadedApp2, rm2, am2Node, am2);

    // stop RM's
    rm2.stop();
    rm1.stop();
    
    // completed apps are not removed immediately after app finish
    // And finished app is also loaded back.
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    Assert.assertEquals(4, rmAppState.size());
  }
  
  private void assertCryptoMaterialStateNotEmpty(ApplicationStateData appState) {
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      Assert.assertNotNull(appState.getKeyStore());
      Assert.assertNotEquals(0, appState.getKeyStore().length);
      Assert.assertNotNull(appState.getKeyStorePassword());
      Assert.assertNotEquals(0, appState.getKeyStorePassword());
      Assert.assertNotNull(appState.getTrustStore());
      Assert.assertNotEquals(0, appState.getTrustStore().length);
      Assert.assertNotNull(appState.getTrustStore());
      Assert.assertNotEquals(0, appState.getTrustStorePassword().length);
    }
    if (conf.getBoolean(YarnConfiguration.RM_JWT_ENABLED, YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      Assert.assertNotNull(appState.getJWT());
      Assert.assertNotEquals(0, appState.getJWT().length());
      Assert.assertNotEquals(-1L, appState.getJWTExpiration());
    }
  }
  
  private void assertAppCryptoMaterialNotEmpty(RMApp app) {
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      Assert.assertNotNull(app.getKeyStore());
      Assert.assertNotEquals(0, app.getKeyStore().length);
      Assert.assertNotNull(app.getKeyStorePassword());
      Assert.assertNotEquals(0, app.getKeyStorePassword());
      Assert.assertNotNull(app.getTrustStore());
      Assert.assertNotEquals(0, app.getTrustStore().length);
      Assert.assertNotNull(app.getTrustStore());
      Assert.assertNotEquals(0, app.getTrustStorePassword().length);
    }
    if (conf.getBoolean(YarnConfiguration.RM_JWT_ENABLED, YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      Assert.assertNotNull(app.getJWT());
      Assert.assertNotEquals(0, app.getJWT());
      Assert.assertNotNull(app.getJWTExpiration());
    }
  }
  
  /**
   * Special edge case:
   * 1. Start an application
   * 2. Finish application
   * 3. RM gets killed
   * 4. RM recovers
   * 5. NM resyncs - NM hasn't received that application has finished and report it as running
   *    together with its local crypto material state
   * 6. RM receives the running applications and tries to infer if NM has missed
   *    a crypto material update by comparing RM's crypto material state with the state
   *    NM has sent
   * 7. NPE because the RMApp has finished and JWT expiration date is null
   */
  @Test
  public void testRMRestartAfterAppFinishedPushingCryptoMaterial() throws Exception {
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "40s");
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    conf.set(YarnConfiguration.RM_JWT_VALIDITY_PERIOD, "10s");
    conf.set(YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY, "6s");
    
    // Start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    
    MockNM nm = new MockNM("127.0.0.1:1337", 20 * 2014, rm1.getResourceTrackerService());
    nm.registerNode();
    
    RMApp app = rm1.submitApp(1024);
    RMAppAttempt appAttempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm1.sendAMLaunched(appAttempt.getAppAttemptId());
    am.registerAppAttempt();
  
    rm1.waitForState(app.getApplicationId(), RMAppState.RUNNING);
  
    AllocateResponse allocateResponse = am.allocate("127.0.0.1", 2 * 1024, 2, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
  
    while (allocateResponse.getAllocatedContainers().size() < 2) {
      nm.nodeHeartbeat(true);
      allocateResponse = am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      TimeUnit.MILLISECONDS.sleep(100);
    }
    LOG.info("Containers allocated");
    finishApplicationMaster(app, rm1, nm, am);
    LOG.info("App finished");
    
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    nm.setResourceTrackerService(rm2.getResourceTrackerService());
    LOG.info("NM heartbeating to new RM");
    NodeHeartbeatResponse hbResponse = nm.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());
    Map<ApplicationId, UpdatedCryptoForApp> runningApps = new HashMap<>();
    UpdatedCryptoForApp uca = UpdatedCryptoForApp.newInstance(1, System.currentTimeMillis());
    runningApps.put(app.getApplicationId(), uca);
    nm.registerNode(runningApps);
    hbResponse = nm.nodeHeartbeat(true);
    Assert.assertEquals(0, hbResponse.getUpdatedCryptoForApps().size());
  }
  
  @Test(timeout = 50000)
  public void testRMRestartBeforeSendingCryptoUpdateToNM() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "40s");
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    conf.set(YarnConfiguration.RM_JWT_VALIDITY_PERIOD, "10s");
    conf.set(YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY, "6s");
    
    // Start RM 1
    MockRM rm1 = new RMWithCustomRTService(conf);
    rms.add(rm1);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    
    // Start NM
    MockNM nm = new MockNM("127.0.0.1:1337", 20 * 1024, rm1.getResourceTrackerService());
    nm.registerNode();
    // Launch application
    RMApp app = rm1.submitApp(1024);
    RMAppAttempt appAttempt = app.getCurrentAppAttempt();
    NodeHeartbeatResponse nmHeartbeatResponse = nm.nodeHeartbeat(true);
    Assert.assertTrue(nmHeartbeatResponse.getUpdatedCryptoForApps().isEmpty());
    MockAM am = rm1.sendAMLaunched(appAttempt.getAppAttemptId());
    am.registerAppAttempt();
    rm1.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    
    AllocateResponse allocateResponse = am.allocate("127.0.0.1", 2 * 1024, 2, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    
    while (allocateResponse.getAllocatedContainers().size() < 2) {
      nm.nodeHeartbeat(true);
      allocateResponse = am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      TimeUnit.MILLISECONDS.sleep(100);
    }
    LOG.info("Containers allocated");
    
    // Trigger Certificate renewal but without sending event to NM
    RMNode rmNode = rm1.getRMContext().getRMNodes().get(nm.getNodeId());
    
    LOG.info("Sleeping until the renewal is scheduled");
    while (rmNode.getAppX509ToUpdate().isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    while (rmNode.getAppJWTToUpdate().isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    nmHeartbeatResponse = nm.nodeHeartbeat(true);
    // This should be empty because RM 1 is fixed not to send update crypto events
    Assert.assertTrue(nmHeartbeatResponse.getUpdatedCryptoForApps().isEmpty());
    
    rm1.stop();
    // Start RM 2
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    // Update NM RT to point to RM 2
    nm.setResourceTrackerService(rm2.getResourceTrackerService());
    nmHeartbeatResponse = nm.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, nmHeartbeatResponse.getNodeAction());
    app = rm2.getRMContext().getRMApps().get(app.getApplicationId());
    
    // new NM to represent re-registration
    nm = new MockNM("127.0.0.1:1337", 20 * 1024, rm2.getResourceTrackerService());
    Map<ApplicationId, UpdatedCryptoForApp> runningApps = new HashMap<>();
    UpdatedCryptoForApp upc = UpdatedCryptoForApp.newInstance(app.getCryptoMaterialVersion(),
        DateUtils.localDateTime2UnixEpoch(app.getJWTExpiration()));
    runningApps.put(app.getApplicationId(), upc);
    nm.registerNode(runningApps);
    nmHeartbeatResponse = nm.nodeHeartbeat(true);
    // Since crypto material didn't change the heartbeat should not contain updated crypto material
    Assert.assertEquals(0, nmHeartbeatResponse.getUpdatedCryptoForApps().size());
    
    rmNode = rm2.getRMContext().getRMNodes().get(nm.getNodeId());
    rmNode.getAppX509ToUpdate().clear();
    rmNode.getAppJWTToUpdate().clear();
    
    LOG.info("Sleeping until the renewal triggers again");
    while (rmNode.getAppX509ToUpdate().isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
  
    while (rmNode.getAppJWTToUpdate().isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    
    // New NM to represent re-registration
    nm = new MockNM("127.0.0.1:1337", 20 * 1024, rm2.getResourceTrackerService());
    nm.registerNode(runningApps);
    Assert.assertEquals(1, rmNode.getAppX509ToUpdate().size());
    Assert.assertEquals(1, rmNode.getAppJWTToUpdate().size());
    Assert.assertTrue(rmNode.getAppX509ToUpdate().containsKey(app.getApplicationId()));
    Assert.assertTrue(rmNode.getAppJWTToUpdate().containsKey(app.getApplicationId()));
    nmHeartbeatResponse = nm.nodeHeartbeat(true);
    // Crypto material has changed so heartbeat should contain updated crypto material
    Assert.assertEquals(1, nmHeartbeatResponse.getUpdatedCryptoForApps().size());
    Assert.assertTrue(nmHeartbeatResponse.getUpdatedCryptoForApps().containsKey(app.getApplicationId()));
    Assert.assertEquals(app.getCryptoMaterialVersion(),
        new Integer(nmHeartbeatResponse.getUpdatedCryptoForApps().get(app
        .getApplicationId()).getVersion()));
    Assert.assertEquals(DateUtils.localDateTime2UnixEpoch(app.getJWTExpiration()),
        nmHeartbeatResponse.getUpdatedCryptoForApps().get(app.getApplicationId()).getJWTExpiration());
    
    rm2.stop();
  }
  
  private class RMWithCustomRTService extends MockRM {
  
    private RMWithCustomRTService(Configuration conf) {
      super(conf);
    }
    
    @Override
    protected ResourceTrackerService createResourceTrackerService() {
      RMContainerTokenSecretManager containerTokenSecretManager =
          getRMContext().getContainerTokenSecretManager();
      containerTokenSecretManager.rollMasterKey();
      NMTokenSecretManagerInRM nmTokenSecretManager =
          getRMContext().getNMTokenSecretManager();
      nmTokenSecretManager.rollMasterKey();
      return new RTServiceNotUpdatingCryptoMaterial(getRMContext(), nodesListManager, this.nmLivelinessMonitor,
          containerTokenSecretManager, nmTokenSecretManager);
    }
    
    private class RTServiceNotUpdatingCryptoMaterial extends ResourceTrackerService {
  
      public RTServiceNotUpdatingCryptoMaterial(RMContext rmContext,
          NodesListManager nodesListManager,
          NMLivelinessMonitor nmLivelinessMonitor,
          RMContainerTokenSecretManager containerTokenSecretManager,
          NMTokenSecretManagerInRM nmTokenSecretManager) {
        super(rmContext, nodesListManager, nmLivelinessMonitor, containerTokenSecretManager, nmTokenSecretManager);
      }
  
      @Override
      protected Map<ApplicationId, UpdatedCryptoForApp> mergeNewSecurityMaterialForApps(RMNode rmNode) {
        return new HashMap<>();
      }
  
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }
  
      @Override
      protected void serviceStop() {
        // don't do anything
      }
    }
  }
  
  @Test (timeout = 60000)
  public void testRMRestartAppRunningAMFailed() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", true, true);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FAILED);

    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState = rmState.
            getApplicationState();
    ApplicationStateData appState = rmAppState.get(app0.getApplicationId());
    // assert the AM failed state is saved.
    Assert.assertEquals(RMAppAttemptState.FAILED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());

    // assert app state has not been saved.
    Assert.assertNull(rmAppState.get(app0.getApplicationId()).getState());

    // new AM started but not registered, app still stays at ACCECPTED state.
    rm1.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);

    // start new RM
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    // assert the previous AM state is loaded back on RM recovery.

    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FAILED);
  }

  @Test (timeout = 60000)
  public void testRMRestartWaitForPreviousAMToFinish() throws Exception {
    // testing 3 cases
    // After RM restarts
    // 1) New application attempt is not started until previous AM container
    // finish event is reported back to RM as a part of nm registration.
    // 2) If previous AM container finish event is never reported back (i.e.
    // node manager on which this AM container was running also went down) in
    // that case AMLivenessMonitor should time out previous attempt and start
    // new attempt.
    // 3) If all the stored attempts had finished then new attempt should
    // be started immediately.
    YarnConfiguration conf = new YarnConfiguration(this.conf);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 40);
    
    // start RM
    final MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234" , 16382, rm1.getResourceTrackerService());
    nm1.registerNode();
     
    // submitting app
    RMApp app1 = rm1.submitApp(200);
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am1 = launchAM(app1, rm1, nm1);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    // Fail first AM.
    am1.waitForState(RMAppAttemptState.FAILED);
    
    // launch another AM.
    MockAM am2 = launchAM(app1, rm1, nm1);
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    Assert.assertEquals(1, rmAppState.size());
    Assert.assertEquals(app1.getState(), RMAppState.RUNNING);
    Assert.assertEquals(app1.getAppAttempts()
        .get(app1.getCurrentAppAttempt().getAppAttemptId())
        .getAppAttemptState(), RMAppAttemptState.RUNNING);

    //  start new RM.
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NodeHeartbeatResponse res = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, res.getNodeAction());
    
    RMApp rmApp = rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    // application should be in ACCEPTED state
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    
    Assert.assertEquals(RMAppState.ACCEPTED, rmApp.getState());
    // new attempt should not be started
    Assert.assertEquals(2, rmApp.getAppAttempts().size());
    // am1 attempt should be in FAILED state where as am2 attempt should be in
    // LAUNCHED state
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm2.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.LAUNCHED);
    Assert.assertEquals(RMAppAttemptState.FAILED,
        rmApp.getAppAttempts().get(am1.getApplicationAttemptId())
            .getAppAttemptState());
    Assert.assertEquals(RMAppAttemptState.LAUNCHED,
        rmApp.getAppAttempts().get(am2.getApplicationAttemptId())
            .getAppAttemptState());

    NMContainerStatus status =
        TestRMRestart.createNMContainerStatus(
            am2.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(status), null);
    rm2.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    launchAM(rmApp, rm2, nm1);
    Assert.assertEquals(3, rmApp.getAppAttempts().size());
    rm2.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.RUNNING);
    // Now restart RM ...
    // Setting AMLivelinessMonitor interval to be 10 Secs. 
    conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 10000);
    MockRM rm3 = createMockRM(conf);
    rm3.start();
    
    // Wait for RM to process all the events as a part of rm recovery.
    nm1.setResourceTrackerService(rm3.getResourceTrackerService());
    
    rmApp = rm3.getRMContext().getRMApps().get(app1.getApplicationId());
    // application should be in ACCEPTED state
    rm3.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    Assert.assertEquals(rmApp.getState(), RMAppState.ACCEPTED);
    // new attempt should not be started
    Assert.assertEquals(3, rmApp.getAppAttempts().size());
    // am1 and am2 attempts should be in FAILED state where as am3 should be
    // in LAUNCHED state
    rm3.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm3.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    ApplicationAttemptId latestAppAttemptId =
        rmApp.getCurrentAppAttempt().getAppAttemptId();
    rm3.waitForState(latestAppAttemptId, RMAppAttemptState.LAUNCHED);
    Assert.assertEquals(RMAppAttemptState.FAILED,
        rmApp.getAppAttempts().get(am1.getApplicationAttemptId())
            .getAppAttemptState());
    Assert.assertEquals(RMAppAttemptState.FAILED,
        rmApp.getAppAttempts().get(am2.getApplicationAttemptId())
            .getAppAttemptState());
    Assert.assertEquals(RMAppAttemptState.LAUNCHED,rmApp.getAppAttempts()
        .get(latestAppAttemptId).getAppAttemptState());

    rm3.waitForState(latestAppAttemptId, RMAppAttemptState.FAILED);
    rm3.waitForState(rmApp.getApplicationId(), RMAppState.ACCEPTED);
    final int maxRetry = 10;
    final RMApp rmAppForCheck = rmApp;
    GenericTestUtils.waitFor(
        new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return new Boolean(rmAppForCheck.getAppAttempts().size() == 4);
          }
        },
        100, maxRetry);
    Assert.assertEquals(RMAppAttemptState.FAILED,
        rmApp.getAppAttempts().get(latestAppAttemptId).getAppAttemptState());
    
    latestAppAttemptId = rmApp.getCurrentAppAttempt().getAppAttemptId();
    
    // The 4th attempt has started but is not yet saved into RMStateStore
    // It will be saved only when we launch AM.

    // submitting app but not starting AM for it.
    RMApp app2 = rm3.submitApp(200);
    rm3.waitForState(app2.getApplicationId(), RMAppState.ACCEPTED);
    Assert.assertEquals(1, app2.getAppAttempts().size());
    rmState = rm3.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    Assert.assertEquals(0,
        rmAppState.get(app2.getApplicationId())
            .getAttemptCount());

    MockRM rm4 = createMockRM(conf);
    rm4.start();
    
    rmApp = rm4.getRMContext().getRMApps().get(app1.getApplicationId());
    rm4.waitForState(rmApp.getApplicationId(), RMAppState.ACCEPTED);
    // wait for the attempt to be created.
    int timeoutSecs = 0;
    while (rmApp.getAppAttempts().size() != 2 && timeoutSecs++ < 40) {
      Thread.sleep(200);
    }
    Assert.assertEquals(4, rmApp.getAppAttempts().size());
    Assert.assertEquals(RMAppState.ACCEPTED, rmApp.getState());
    rm4.waitForState(latestAppAttemptId, RMAppAttemptState.SCHEDULED);
    Assert.assertEquals(RMAppAttemptState.SCHEDULED, rmApp.getAppAttempts()
        .get(latestAppAttemptId).getAppAttemptState());
    
    // The initial application for which an AM was not started should be in
    // ACCEPTED state with one application attempt started.
    app2 = rm4.getRMContext().getRMApps().get(app2.getApplicationId());
    rm4.waitForState(app2.getApplicationId(), RMAppState.ACCEPTED);
    Assert.assertEquals(RMAppState.ACCEPTED, app2.getState());
    Assert.assertEquals(1, app2.getAppAttempts().size());
    rm4.waitForState(app2.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.SCHEDULED);
    Assert.assertEquals(RMAppAttemptState.SCHEDULED, app2
        .getCurrentAppAttempt().getAppAttemptState());
  }

  // Test RM restarts after previous attempt succeeded and was saved into state
  // store but before the RMAppAttempt notifies RMApp that it has succeeded. On
  // recovery, RMAppAttempt should send the AttemptFinished event to RMApp so
  // that RMApp can recover its state.
  @Test (timeout = 60000)
  public void testRMRestartWaitForPreviousSucceededAttempt() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    FileSystemRMStateStore fileStore = new FileSystemRMStateStore(){
      int count = 0;

      @Override
      public void updateApplicationStateInternal(ApplicationId appId,
          ApplicationStateData appStateData) throws Exception {
        if (count == 0) {
          // do nothing; simulate app final state is not saved.
          LOG.info(appId + " final state is not saved.");
          count++;
        } else {
          super.updateApplicationStateInternal(appId, appStateData);
        }
      }
    };
    fileStore.init(conf);
    
    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    ((RMContextImpl)rm1.getRMContext()).setStateStore(fileStore);
    fileStore.setRMDispatcher(rm1.getRMContext().getDispatcher());
    fileStore.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 15120);
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "", "");
    am0.unregisterAppAttempt(req, true);
    am0.waitForState(RMAppAttemptState.FINISHING);
    // app final state is not saved. This guarantees that RMApp cannot be
    // recovered via its own saved state, but only via the event notification
    // from the RMAppAttempt on recovery.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    Assert.assertNull(rmAppState.get(app0.getApplicationId()).getState());

    // start RM
    MockRM rm2 = createMockRM(conf);
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
    
    rm2.waitForState(app0.getCurrentAppAttempt().getAppAttemptId(),
      RMAppAttemptState.FINISHED);
    rm2.waitForState(app0.getApplicationId(), RMAppState.FINISHED);
    // app final state is saved via the finish event from attempt.
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    Assert.assertEquals(RMAppState.FINISHED,
        rmAppState.get(app0.getApplicationId()).getState());
  }

  @Test (timeout = 60000)
  public void testRMRestartFailedApp() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);

    // start RM
    MockRM rm1 = createMockRM(conf);
    rm1.start();
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app0.getApplicationId(), RMAppState.FAILED);

    // assert the app/attempt failed state is saved.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app0.getApplicationId());
    Assert.assertEquals(RMAppState.FAILED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.FAILED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());

    // start new RM
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    RMApp loadedApp0 = rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    rm2.waitForState(app0.getApplicationId(), RMAppState.FAILED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // no new attempt is created.
    Assert.assertEquals(1, loadedApp0.getAppAttempts().size());

    verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertTrue(app0.getDiagnostics().toString()
      .contains("Failing the application."));
    // failed diagnostics from attempt is lost because the diagnostics from
    // attempt is not yet available by the time app is saving the app state.
  }

  @Test (timeout = 60000)
  public void testRMRestartKilledApp() throws Exception{
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    // start RM
    MockRM rm1 = createMockRM(conf);
    rm1.start();
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // kill the app.
    rm1.killApp(app0.getApplicationId());
    rm1.waitForState(app0.getApplicationId(), RMAppState.KILLED);
    rm1.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    // killed state is saved.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app0.getApplicationId());
    Assert.assertEquals(RMAppState.KILLED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.KILLED,
      appState.getAttempt(am0.getApplicationAttemptId()).getState());
    String trackingUrl = app0.getCurrentAppAttempt().getOriginalTrackingUrl();
    Assert.assertNotNull(trackingUrl);

    // restart rm
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    RMApp loadedApp0 = rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    rm2.waitForState(app0.getApplicationId(), RMAppState.KILLED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.KILLED);
    // no new attempt is created.
    Assert.assertEquals(1, loadedApp0.getAppAttempts().size());

    ApplicationReport appReport = verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertEquals(app0.getDiagnostics().toString(),
        appReport.getDiagnostics());
    Assert.assertEquals(trackingUrl, loadedApp0.getCurrentAppAttempt()
        .getOriginalTrackingUrl());
  }

  @Test (timeout = 60000)
  public void testRMRestartKilledAppWithNoAttempts() throws Exception {
    FileSystemRMStateStore fileStore = new FileSystemRMStateStore(){
      @Override
      public synchronized void storeApplicationAttemptStateInternal(
          ApplicationAttemptId attemptId,
          ApplicationAttemptStateData attemptStateData) throws Exception {
        // ignore attempt saving request.
      }

      @Override
      public synchronized void updateApplicationAttemptStateInternal(
          ApplicationAttemptId attemptId,
          ApplicationAttemptStateData attemptStateData) throws Exception {
        // ignore attempt saving request.
      }
    };
    fileStore.init(conf);

    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    ((RMContextImpl)rm1.getRMContext()).setStateStore(fileStore);
    fileStore.setRMDispatcher(rm1.getRMContext().getDispatcher());
    fileStore.start();
    // create app
    RMApp app0 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", false);
    // kill the app.
    rm1.killApp(app0.getApplicationId());
    rm1.waitForState(app0.getApplicationId(), RMAppState.KILLED);

    // restart rm
    MockRM rm2 = createMockRM(conf);
    rm2.start();
    RMApp loadedApp0 =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    rm2.waitForState(loadedApp0.getApplicationId(), RMAppState.KILLED);
    Assert.assertTrue(loadedApp0.getAppAttempts().size() == 0);
  }

  @Test (timeout = 60000)
  public void testRMRestartSucceededApp() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create an app and finish the app.
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // unregister am
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "diagnostics", "trackingUrl");
    finishApplicationMaster(app0, rm1, nm1, am0, req);
 
    // check the state store about the unregistered info.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app0.getApplicationId());
    ApplicationAttemptStateData attemptState0 =
      appState.getAttempt(am0.getApplicationAttemptId());
    Assert.assertEquals("diagnostics", attemptState0.getDiagnostics());
    Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
      attemptState0.getFinalApplicationStatus());
    Assert.assertEquals("trackingUrl", attemptState0.getFinalTrackingUrl());
    Assert.assertEquals(app0.getFinishTime(), appState.getFinishTime());

    // restart rm
    MockRM rm2 = createMockRM(conf);
    rm2.start();

    // verify application report returns the same app info as the app info
    // before RM restarts.
    ApplicationReport appReport = verifyAppReportAfterRMRestart(app0, rm2);
    Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
      appReport.getFinalApplicationStatus());
    Assert.assertEquals("trackingUrl", appReport.getOriginalTrackingUrl());
  }

  @Test (timeout = 60000)
  public void testRMRestartGetApplicationList() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);

    // start RM
    MockRM rm1 = new MockRM(conf) {
      @Override
      protected SystemMetricsPublisher createSystemMetricsPublisher() {
        return spy(super.createSystemMetricsPublisher());
      }
    };
    rms.add(rm1);
    rm1.start();
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // a succeeded app.
    RMApp app0 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am0 = launchAM(app0, rm1, nm1);
    finishApplicationMaster(app0, rm1, nm1, am0);

    // a failed app.
    RMApp app1 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am1 = launchAM(app1, rm1, nm1);
    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);

    // a killed app.
    RMApp app2 = rm1.submitApp(200, "name", "user", null,
      false, "default", 1, null, "myType");
    MockAM am2 = launchAM(app2, rm1, nm1);
    rm1.killApp(app2.getApplicationId());
    rm1.waitForState(app2.getApplicationId(), RMAppState.KILLED);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    verify(rm1.getRMContext().getSystemMetricsPublisher(),Mockito.times(3))
    .appCreated(any(RMApp.class), anyLong());
    // restart rm

    MockRM rm2 = new MockRM(conf) {
      @Override
      protected RMAppManager createRMAppManager() {
        return spy(super.createRMAppManager());
      }

      @Override
      protected SystemMetricsPublisher createSystemMetricsPublisher() {
        return spy(super.createSystemMetricsPublisher());
      }
    };
    rms.add(rm2);
    rm2.start();

    verify(rm2.getRMContext().getSystemMetricsPublisher(),Mockito.times(3))
        .appCreated(any(RMApp.class), anyLong());

    GetApplicationsRequest request1 =
        GetApplicationsRequest.newInstance(EnumSet.of(
          YarnApplicationState.FINISHED, YarnApplicationState.KILLED,
          YarnApplicationState.FAILED));
    GetApplicationsResponse response1 =
        rm2.getClientRMService().getApplications(request1);
    List<ApplicationReport> appList1 = response1.getApplicationList();

    // assert all applications exist according to application state after RM
    // restarts.
    boolean forApp0 = false, forApp1 = false, forApp2 = false;
    for (ApplicationReport report : appList1) {
      if (report.getApplicationId().equals(app0.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.FINISHED,
          report.getYarnApplicationState());
        forApp0 = true;
      }
      if (report.getApplicationId().equals(app1.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.FAILED,
          report.getYarnApplicationState());
        forApp1 = true;
      }
      if (report.getApplicationId().equals(app2.getApplicationId())) {
        Assert.assertEquals(YarnApplicationState.KILLED,
          report.getYarnApplicationState());
        forApp2 = true;
      }
    }
    Assert.assertTrue(forApp0 && forApp1 && forApp2);

    // assert all applications exist according to application type after RM
    // restarts.
    Set<String> appTypes = new HashSet<String>();
    appTypes.add("myType");
    GetApplicationsRequest request2 =
        GetApplicationsRequest.newInstance(appTypes);
    GetApplicationsResponse response2 =
        rm2.getClientRMService().getApplications(request2);
    List<ApplicationReport> appList2 = response2.getApplicationList();
    Assert.assertTrue(3 == appList2.size());

    // check application summary is logged for the completed apps with timeout
    // to make sure APP_COMPLETED events are processed, after RM restart.
    verify(rm2.getRMAppManager(), timeout(1000).times(3)).
        logApplicationSummary(isA(ApplicationId.class));
  }

  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  private ApplicationReport verifyAppReportAfterRMRestart(RMApp app, MockRM rm)
      throws Exception {
    GetApplicationReportRequest reportRequest =
        GetApplicationReportRequest.newInstance(app.getApplicationId());
    GetApplicationReportResponse response =
        rm.getClientRMService().getApplicationReport(reportRequest);
    ApplicationReport report = response.getApplicationReport();
    Assert.assertEquals(app.getStartTime(), report.getStartTime());
    Assert.assertEquals(app.getFinishTime(), report.getFinishTime());
    Assert.assertEquals(app.createApplicationState(),
      report.getYarnApplicationState());
    Assert.assertTrue(1 == report.getProgress());
    return response.getApplicationReport();
  }

  private void finishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm,
      MockAM am) throws Exception {
    final FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "", "");
    finishApplicationMaster(rmApp, rm, nm, am, req);
  }

  private void finishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm,
      MockAM am, FinishApplicationMasterRequest req) throws Exception {
    am.unregisterAppAttempt(req,true);
    am.waitForState(RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
    // check that app/attempt is saved with the final state
    RMState rmState = rm.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(rmApp.getApplicationId());
    Assert
      .assertEquals(RMAppState.FINISHED, appState.getState());
    Assert.assertEquals(RMAppAttemptState.FINISHED,
      appState.getAttempt(am.getApplicationAttemptId()).getState());
  }

  @Test (timeout = 60000)
  public void testRMRestartOnMaxAppAttempts() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);


    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit an app with maxAppAttempts equals to 1
    RMApp app1 = rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", 1,
          null);
    // submit an app with maxAppAttempts equals to -1
    RMApp app2 = rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null);

    // assert app1 info is saved
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app1.getApplicationSubmissionContext()
        .getApplicationId());

    // Allocate the AM
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    appState = rmAppState.get(app1.getApplicationId());
    Assert.assertEquals(1, appState.getAttemptCount());
    ApplicationAttemptStateData attemptState =
                                appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());

    // Setting AMLivelinessMonitor interval to be 3 Secs.
    conf.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 3000);
    // start new RM   
    MockRM rm2 = createMockRM(conf);
    rm2.start();

    // verify that maxAppAttempts is set to global value
    Assert.assertEquals(2, 
        rm2.getRMContext().getRMApps().get(app2.getApplicationId())
        .getMaxAppAttempts());

    // app1 and app2 are loaded back, but app1 failed because it's
    // hitting max-retry.
    Assert.assertEquals(2, rm2.getRMContext().getRMApps().size());
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    rm2.waitForState(app2.getApplicationId(), RMAppState.ACCEPTED);

    // app1 failed state is saved in state store. app2 final saved state is not
    // determined yet.
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    Assert.assertEquals(RMAppState.FAILED,
      rmAppState.get(app1.getApplicationId()).getState());
    Assert.assertNull(rmAppState.get(app2.getApplicationId()).getState());
  }

  @Test (timeout = 60000)
  public void testDelegationTokenRestoredInDelegationTokenRenewer()
      throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm1 = new TestSecurityMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();

    HashSet<Token<RMDelegationTokenIdentifier>> tokenSet =
        new HashSet<Token<RMDelegationTokenIdentifier>>();

    // create an empty credential
    Credentials ts = new Credentials();

    // create tokens and add into credential
    Text userText1 = new Text("user1");
    RMDelegationTokenIdentifier dtId1 =
        new RMDelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    Token<RMDelegationTokenIdentifier> token1 =
        new Token<RMDelegationTokenIdentifier>(dtId1,
          rm1.getRMContext().getRMDelegationTokenSecretManager());
    SecurityUtil.setTokenService(token1, rmAddr);
    ts.addToken(userText1, token1);
    tokenSet.add(token1);

    Text userText2 = new Text("user2");
    RMDelegationTokenIdentifier dtId2 =
        new RMDelegationTokenIdentifier(userText2, new Text("renewer2"),
          userText2);
    Token<RMDelegationTokenIdentifier> token2 =
        new Token<RMDelegationTokenIdentifier>(dtId2,
          rm1.getRMContext().getRMDelegationTokenSecretManager());
    SecurityUtil.setTokenService(token2, rmAddr);
    ts.addToken(userText2, token2);
    tokenSet.add(token2);

    // submit an app with customized credential
    RMApp app = rm1.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1, ts);

    // assert app info is saved
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app.getApplicationId());
    Assert.assertNotNull(appState);

    // assert delegation tokens exist in rm1 DelegationTokenRenewr
    Assert.assertEquals(tokenSet, rm1.getRMContext()
      .getDelegationTokenRenewer().getDelegationTokens());

    // assert delegation tokens are saved
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    securityTokens.rewind();
    Assert.assertEquals(securityTokens, appState
      .getApplicationSubmissionContext().getAMContainerSpec()
      .getTokens());

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf);
    rm2.start();

    // Need to wait for a while as now token renewal happens on another thread
    // and is asynchronous in nature.
    waitForTokensToBeRenewed(rm2, tokenSet);

    // verify tokens are properly populated back to rm2 DelegationTokenRenewer
    Assert.assertEquals(tokenSet, rm2.getRMContext()
      .getDelegationTokenRenewer().getDelegationTokens());
  }

  private void waitForTokensToBeRenewed(MockRM rm2,
      HashSet<Token<RMDelegationTokenIdentifier>> tokenSet) throws Exception {
    // Max wait time to get the token renewal can be kept as 1sec (100 * 10ms)
    int waitCnt = 100;
    while (waitCnt-- > 0) {
      if (tokenSet.equals(rm2.getRMContext().getDelegationTokenRenewer()
          .getDelegationTokens())) {
        // Stop waiting as tokens are populated to DelegationTokenRenewer.
        break;
      } else {
        Thread.sleep(10);
      }
    }
  }

  @Test (timeout = 60000)
  public void testAppAttemptTokensRestoredOnRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm1 = new TestSecurityMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("0.0.0.0:4321", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit an app
    RMApp app1 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), "default");

    // assert app info is saved
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
                                                  rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app1.getApplicationId());
    Assert.assertNotNull(appState);

    // Allocate the AM
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);

    // assert attempt info is saved
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    appState = rmAppState.get(app1.getApplicationId());
    ApplicationAttemptStateData attemptState = appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1),
      attemptState.getMasterContainer().getId());

    // the clientTokenMasterKey that are generated when
    // RMAppAttempt is created,
    byte[] clientTokenMasterKey =
        attempt1.getClientTokenMasterKey().getEncoded();

    // assert application credentials are saved
    Credentials savedCredentials = attemptState.getAppAttemptTokens();
    Assert.assertArrayEquals("client token master key not saved",
        clientTokenMasterKey, savedCredentials.getSecretKey(
            RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf);
    rm2.start();

    RMApp loadedApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = loadedApp1.getRMAppAttempt(attemptId1);

    // assert loaded attempt recovered
    Assert.assertNotNull(loadedAttempt1);

    // assert client token master key is recovered back to api-versioned
    // client token master key
    Assert.assertEquals("client token master key not restored",
        attempt1.getClientTokenMasterKey(),
        loadedAttempt1.getClientTokenMasterKey());

    // assert ClientTokenSecretManager also knows about the key
    Assert.assertArrayEquals(clientTokenMasterKey,
        rm2.getClientToAMTokenSecretManager().getMasterKey(attemptId1)
            .getEncoded());

    // assert AMRMTokenSecretManager also knows about the AMRMToken password
    Token<AMRMTokenIdentifier> amrmToken = loadedAttempt1.getAMRMToken();
    Assert.assertArrayEquals(amrmToken.getPassword(),
      rm2.getRMContext().getAMRMTokenSecretManager().retrievePassword(
        amrmToken.decodeIdentifier()));
  }

  @Test (timeout = 60000)
  public void testRMDelegationTokenRestoredOnRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm1 = new TestSecurityMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();

    // create an empty credential
    Credentials ts = new Credentials();

    // request a token and add into credential
    GetDelegationTokenRequest request1 =
        GetDelegationTokenRequest.newInstance("renewer1");
    UserGroupInformation.getCurrentUser().setAuthenticationMethod(
        AuthMethod.KERBEROS);
    GetDelegationTokenResponse response1 =
        rm1.getClientRMService().getDelegationToken(request1);
    org.apache.hadoop.yarn.api.records.Token delegationToken1 =
        response1.getRMDelegationToken();
    Token<RMDelegationTokenIdentifier> token1 =
        ConverterUtils.convertFromYarn(delegationToken1, rmAddr);
    RMDelegationTokenIdentifier dtId1 = token1.decodeIdentifier();

    HashSet<RMDelegationTokenIdentifier> tokenIdentSet =
        new HashSet<RMDelegationTokenIdentifier>();
    ts.addToken(token1.getService(), token1);
    tokenIdentSet.add(dtId1);

    // submit an app with customized credential
    RMApp app = rm1.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1, ts);

    // assert app info is saved
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();
    ApplicationStateData appState = rmAppState.get(app.getApplicationId());
    Assert.assertNotNull(appState);

    // assert all master keys are saved
    Set<DelegationKey> allKeysRM1 = rm1.getRMContext()
      .getRMDelegationTokenSecretManager().getAllMasterKeys();
    rmState = rm1.getRMContext().getStateStore().loadState();
    Set<DelegationKey> rmDTMasterKeyState =
        rmState.getRMDTSecretManagerState().getMasterKeyState();
    for(DelegationKey expectedKey:allKeysRM1){
      boolean foundIt=false;
      for(DelegationKey gotKey:rmDTMasterKeyState){
        if(expectedKey.getKeyId()== gotKey.getKeyId() &&
                Arrays.equals(expectedKey.getEncodedKey(),gotKey.getEncodedKey())){
          foundIt=true;
          break;
        }
      }
      Assert.assertTrue(foundIt);
    }

    // assert all tokens are saved
    Map<RMDelegationTokenIdentifier, Long> allTokensRM1 =
        rm1.getRMContext().getRMDelegationTokenSecretManager().getAllTokens();
    Assert.assertEquals(tokenIdentSet, allTokensRM1.keySet());
    rmState = rm1.getRMContext().getStateStore().loadState();
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        rmState.getRMDTSecretManagerState().getTokenState();
    Assert.assertEquals(allTokensRM1.size(), rmDTState.size());
    for(RMDelegationTokenIdentifier identifier:allTokensRM1.keySet()){
      Assert.assertEquals(allTokensRM1.get(identifier),
              rmDTState.get(identifier));
    }
    
    // assert sequence number is saved
    rmState = rm1.getRMContext().getStateStore().loadState();
    Assert.assertEquals(rm1.getRMContext().getRMDelegationTokenSecretManager()
      .getLatestDTSequenceNumber(), rmState.getRMDTSecretManagerState()
      .getDTSequenceNumber());

    // request one more token
    GetDelegationTokenRequest request2 =
        GetDelegationTokenRequest.newInstance("renewer2");
    GetDelegationTokenResponse response2 =
        rm1.getClientRMService().getDelegationToken(request2);
    org.apache.hadoop.yarn.api.records.Token delegationToken2 =
        response2.getRMDelegationToken();
    Token<RMDelegationTokenIdentifier> token2 =
        ConverterUtils.convertFromYarn(delegationToken2, rmAddr);
    RMDelegationTokenIdentifier dtId2 = token2.decodeIdentifier();

    // cancel token2
    try{
      rm1.getRMContext().getRMDelegationTokenSecretManager().cancelToken(token2,
        UserGroupInformation.getCurrentUser().getUserName());
    } catch(Exception e) {
      Assert.fail();
    }

    // Assert the token which has the latest delegationTokenSequenceNumber is removed
    Assert.assertEquals(rm1.getRMContext().getRMDelegationTokenSecretManager()
      .getLatestDTSequenceNumber(), dtId2.getSequenceNumber());
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmDTState = rmState.getRMDTSecretManagerState().getTokenState();
    Assert.assertFalse(rmDTState.containsKey(dtId2));

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf);
    rm2.start();

    // assert master keys and tokens are populated back to DTSecretManager
    Map<RMDelegationTokenIdentifier, Long> allTokensRM2 =
        rm2.getRMContext().getRMDelegationTokenSecretManager().getAllTokens();
    Assert.assertEquals(allTokensRM2.keySet(), allTokensRM1.keySet());
    // rm2 has its own master keys when it starts, we use containsAll here
    rmDTMasterKeyState =rm2.getRMContext().getRMDelegationTokenSecretManager()
      .getAllMasterKeys();
    for(DelegationKey expectedKey:allKeysRM1){
      boolean foundIt=false;
      for(DelegationKey gotKey:rmDTMasterKeyState){
        if(expectedKey.getKeyId()== gotKey.getKeyId() &&
                Arrays.equals(expectedKey.getEncodedKey(),gotKey.getEncodedKey())){
          foundIt=true;
          break;
        }
      }
      Assert.assertTrue(foundIt);
    }

    // assert sequenceNumber is properly recovered,
    // even though the token which has max sequenceNumber is not stored
    Assert.assertEquals(rm1.getRMContext().getRMDelegationTokenSecretManager()
      .getLatestDTSequenceNumber(), rm2.getRMContext()
      .getRMDelegationTokenSecretManager().getLatestDTSequenceNumber());

    // renewDate before renewing
    Long renewDateBeforeRenew = allTokensRM2.get(dtId1);
    try{
      // Sleep for one millisecond to make sure renewDataAfterRenew is greater
      Thread.sleep(1);
      // renew recovered token
      rm2.getRMContext().getRMDelegationTokenSecretManager().renewToken(
          token1, "renewer1");
    } catch(Exception e) {
      Assert.fail();
    }

    allTokensRM2 = rm2.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens();
    Long renewDateAfterRenew = allTokensRM2.get(dtId1);
    // assert token is renewed
    Assert.assertTrue(renewDateAfterRenew > renewDateBeforeRenew);

    // assert new token is added into state store
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmDTState = rmState.getRMDTSecretManagerState().getTokenState();
    Assert.assertTrue(rmDTState.containsValue(renewDateAfterRenew));
    // assert old token is removed from state store
    Assert.assertFalse(rmDTState.containsValue(renewDateBeforeRenew));

    try{
      rm2.getRMContext().getRMDelegationTokenSecretManager().cancelToken(token1,
        UserGroupInformation.getCurrentUser().getUserName());
    } catch(Exception e) {
      Assert.fail();
    }

    // assert token is removed from state after its cancelled
    allTokensRM2 = rm2.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens();
    Assert.assertFalse(allTokensRM2.containsKey(dtId1));
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmDTState = rmState.getRMDTSecretManagerState().getTokenState();
    Assert.assertFalse(rmDTState.containsKey(dtId1));
  }

  // This is to test submit an application to the new RM with the old delegation
  // token got from previous RM.
  @Test (timeout = 60000)
  public void testAppSubmissionWithOldDelegationTokenAfterRMRestart()
      throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm1 = new TestSecurityMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();

    GetDelegationTokenRequest request1 =
        GetDelegationTokenRequest.newInstance("renewer1");
    UserGroupInformation.getCurrentUser().setAuthenticationMethod(
        AuthMethod.KERBEROS);
    GetDelegationTokenResponse response1 =
        rm1.getClientRMService().getDelegationToken(request1);
    Token<RMDelegationTokenIdentifier> token1 =
        ConverterUtils.convertFromYarn(response1.getRMDelegationToken(), rmAddr);

    // start new RM
    MockRM rm2 = new TestSecurityMockRM(conf);
    rm2.start();

    // submit an app with the old delegation token got from previous RM.
    Credentials ts = new Credentials();
    ts.addToken(token1.getService(), token1);
    RMApp app = rm2.submitApp(200, "name", "user",
        new HashMap<ApplicationAccessType, String>(), false, "default", 1, ts);
    rm2.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
  }

  @Test (timeout = 60000)
  public void testRMStateStoreDispatcherDrainedOnRMStop() throws Exception {
    FileSystemRMStateStore fileStore = new FileSystemRMStateStore(){
      volatile boolean wait = true;
      @Override
      public void serviceStop() throws Exception {
        // Unblock app saving request.
        wait = false;
        super.serviceStop();
      }

      @Override
      protected void handleStoreEvent(RMStateStoreEvent event) {
        // Block app saving request.
        // Skip if synchronous updation of DTToken
        if (!(event instanceof RMStateStoreAMRMTokenEvent)
            && !(event instanceof RMStateStoreRMDTEvent)
            && !(event instanceof RMStateStoreRMDTMasterKeyEvent)) {
          while (wait);
        }
        super.handleStoreEvent(event);
      }
    };
    fileStore.init(conf);

    // start RM
    final MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    ((RMContextImpl)rm1.getRMContext()).setStateStore(fileStore);
    fileStore.setRMDispatcher(rm1.getRMContext().getDispatcher());
    fileStore.start();
    // create apps.
    final ArrayList<RMApp> appList = new ArrayList<RMApp>();
    final int NUM_APPS = 5;

    for (int i = 0; i < NUM_APPS; i++) {
      RMApp app = rm1.submitApp(200, "name", "user",
            new HashMap<ApplicationAccessType, String>(), false,
            "default", -1, null, "MAPREDUCE", false);
      appList.add(app);
      rm1.waitForState(app.getApplicationId(), RMAppState.NEW_SAVING);
    }
    // all apps's saving request are now enqueued to RMStateStore's dispatcher
    // queue, and will be processed once rm.stop() is called.

    // Nothing exist in state store before stop is called.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();
    Assert.assertTrue(rmAppState.size() == 0);

    // stop rm
    rm1.stop();

    // Assert app info is still saved even if stop is called with pending saving
    // request on dispatcher.
    for (RMApp app : appList) {
      rmState = rm1.getRMContext().getStateStore().loadState();
      rmAppState =rmState.getApplicationState();
      ApplicationStateData appState = rmAppState.get(app.getApplicationId());
      Assert.assertNotNull(appState);
      Assert.assertEquals(0, appState.getAttemptCount());
      Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app.getApplicationSubmissionContext()
        .getApplicationId());
    }
    rmState = rm1.getRMContext().getStateStore().loadState();
    rmAppState =rmState.getApplicationState();
    Assert.assertTrue(rmAppState.size() == NUM_APPS);
  }

  @Test (timeout = 60000)
  public void testFinishedAppRemovalAfterRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 1);

    // start RM
    MockRM rm1 = createMockRM(conf);
    rm1.start();
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create an app and finish the app.
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);
    finishApplicationMaster(app0, rm1, nm1, am0);

    MockRM rm2 = createMockRM(conf);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1 = rm2.registerNode("127.0.0.1:1234", 15120);

    RMState rmState = rm2.getRMContext().getStateStore().loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();

    // app0 exits in both state store and rmContext
    Assert.assertEquals(RMAppState.FINISHED,
      rmAppState.get(app0.getApplicationId()).getState());
    rm2.waitForState(app0.getApplicationId(), RMAppState.FINISHED);

    // create one more app and finish the app.
    RMApp app1 = rm2.submitApp(200);
    MockAM am1 = launchAM(app1, rm2, nm1);
    finishApplicationMaster(app1, rm2, nm1, am1);

    // the first app0 get kicked out from both rmContext and state store
    Assert.assertNull(rm2.getRMContext().getRMApps()
      .get(app0.getApplicationId()));
    rmState = rm2.getRMContext().getStateStore().loadState();
    rmAppState = rmState.getApplicationState();
    Assert.assertNull(rmAppState.get(app0.getApplicationId()));
  }

  // This is to test RM does not get hang on shutdown.
  @Ignore //the test need to be reimplemented to work with the distributed RT
  @Test (timeout = 10000)
  public void testRMShutdown() throws Exception {
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MemoryRMStateStore memStore = new MemoryRMStateStore() {
      @Override
      public synchronized void checkVersion()
          throws Exception {
        throw new Exception("Invalid version.");
      }
    };
    // start RM
    memStore.init(conf);
    MockRM rm1 = null;
    try {
      rm1 = createMockRM(conf, memStore);
      Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
      rm1.start();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid version."));
    }
    Assert.assertTrue(rm1.getServiceState() == STATE.STOPPED);
  }

  // This is to test Killing application should be able to wait until app
  // reaches killed state and also check that attempt state is saved before app
  // state is saved.
  @Test (timeout = 60000)
  public void testClientRetryOnKillingApplication() throws Exception {
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MemoryRMStateStore memStore = new TestMemoryRMStateStore();
    memStore.init(conf);

    // start RM
    MockRM rm1 = createMockRM(conf, memStore);
    rm1.start();
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    memStore = new TestMemoryRMStateStore();
    memStore.init(conf);
    ((RMContextImpl)rm1.getRMContext()).setStateStore(memStore);
    memStore.setRMDispatcher(rm1.getRmDispatcher());
    memStore.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app1 =
        rm1.submitApp(200, "name", "user", null, false, "default", 1, null,
          "myType");
    MockAM am1 = launchAM(app1, rm1, nm1);

    KillApplicationResponse response;
    int count = 0;
    while (true) {
      response = rm1.killApp(app1.getApplicationId());
      if (response.getIsKillCompleted()) {
        break;
      }
      Thread.sleep(100);
      count++;
    }
    // we expect at least 2 calls for killApp as the first killApp always return
    // false.
    Assert.assertTrue(count >= 1);

    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.KILLED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.KILLED);
    Assert.assertEquals(1, ((TestMemoryRMStateStore) memStore).updateAttempt);
    Assert.assertEquals(2, ((TestMemoryRMStateStore) memStore).updateApp);
  }

  // Test Application that fails on submission is saved in state store.
  @Test (timeout = 20000)
  public void testAppFailedOnSubmissionSavedInStateStore() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    MockRM rm1 = new TestSecurityMockRM(conf) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new TestRMAppManager(this.rmContext, this.scheduler,
          this.masterService, this.applicationACLsManager, conf);
      }

      class TestRMAppManager extends RMAppManager {

        public TestRMAppManager(RMContext context, YarnScheduler scheduler,
            ApplicationMasterService masterService,
            ApplicationACLsManager applicationACLsManager, Configuration conf) {
          super(context, scheduler, masterService, applicationACLsManager, conf);
        }

        @Override
        protected Credentials parseCredentials(
            ApplicationSubmissionContext application) throws IOException {
          throw new IOException("Parsing credential error.");
        }
      }
    };
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    RMApp app1 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", false);
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    // Check app staet is saved in state store.
    RMState rmState = rm1.getRMContext().getStateStore().loadState();
    Assert.assertEquals(RMAppState.FAILED, rmState
      .getApplicationState().get(app1.getApplicationId()).getState());

    MockRM rm2 = new TestSecurityMockRM(conf);
    rm2.start();
    // Restarted RM has the failed app info too.
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
  }

  @Test (timeout = 20000)
  public void testAppRecoveredInOrderOnRMRestart() throws Exception {
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    for (int i = 10; i > 0; i--) {
      ApplicationStateData appState = mock(ApplicationStateData.class);
      ApplicationSubmissionContext context =
          mock(ApplicationSubmissionContext.class);
      when(appState.getApplicationSubmissionContext()).thenReturn(context);
      when(context.getApplicationId()).thenReturn(
          ApplicationId.newInstance(1234, i));
      memStore.getState().getApplicationState().put(
          appState.getApplicationSubmissionContext().getApplicationId(),
          appState);
    }

    MockRM rm1 = new MockRM(conf, memStore) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new TestRMAppManager(this.rmContext, this.scheduler,
          this.masterService, this.applicationACLsManager, conf);
      }

      class TestRMAppManager extends RMAppManager {
        ApplicationId prevId = ApplicationId.newInstance(1234, 0);

        public TestRMAppManager(RMContext context, YarnScheduler scheduler,
            ApplicationMasterService masterService,
            ApplicationACLsManager applicationACLsManager, Configuration conf) {
          super(context, scheduler, masterService, applicationACLsManager, conf);
        }

        @Override
        protected void recoverApplication(ApplicationStateData appState,
            RMState rmState) throws Exception {
          // check application is recovered in order.
          Assert.assertTrue(rmState.getApplicationState().size() > 0);
          Assert.assertTrue(appState.getApplicationSubmissionContext()
              .getApplicationId().compareTo(prevId) > 0);
          prevId =
              appState.getApplicationSubmissionContext().getApplicationId();
        }
      }
    };
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    try {
      rm1.start();
    } finally {
      rm1.stop();
    }
  }

  @SuppressWarnings("resource")
  @Test (timeout = 60000)
  public void testQueueMetricsOnRMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

    // PHASE 1: create state in an RM
    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    QueueMetrics qm1 = rm1.getResourceScheduler().getRootQueueMetrics();
    resetQueueMetrics(qm1);
    assertQueueMetrics(qm1, 0, 0, 0, 0);

    // create app that gets launched and does allocate before RM restart
    RMApp app1 = rm1.submitApp(200);
    // Need to wait first for AppAttempt to be started (RMAppState.ACCEPTED)
    // and then for it to reach RMAppAttemptState.SCHEDULED
    // inorder to ensure appsPending metric is incremented
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.SCHEDULED);
    assertQueueMetrics(qm1, 1, 1, 0, 0);

    nm1.nodeHeartbeat(true);
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    am1.allocate("127.0.0.1" , 1000, 1, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    assertQueueMetrics(qm1, 1, 0, 1, 0);

    // PHASE 2: create new RM and start from old state
    // create new RM to represent restart and recover state
    MockRM rm2 = createMockRM(conf);
    QueueMetrics qm2 = rm2.getResourceScheduler().getRootQueueMetrics();
    resetQueueMetrics(qm2);
    assertQueueMetrics(qm2, 0, 0, 0, 0);

    rm2.start();
    resetCarryOn();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // recover app
    RMApp loadedApp1 = rm2.getRMContext().getRMApps().get(app1.getApplicationId());

    nm1.nodeHeartbeat(true);
    nm1 = new MockNM("127.0.0.1:1234", 15120, rm2.getResourceTrackerService());

    NMContainerStatus status =
        TestRMRestart
          .createNMContainerStatus(loadedApp1.getCurrentAppAttempt()
              .getAppAttemptId(), 1, ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(status), null);

    while (loadedApp1.getAppAttempts().size() != 2) {
      Thread.sleep(200);
    }
    attempt1 = loadedApp1.getCurrentAppAttempt();
    attemptId1 = attempt1.getAppAttemptId();
    rm2.waitForState(attemptId1, RMAppAttemptState.SCHEDULED);
    qm2 = rm2.getResourceScheduler().getRootQueueMetrics();
    assertQueueMetrics(qm2, 1, 1, 0, 0);
    nm1.nodeHeartbeat(true);
    rm2.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    assertQueueMetrics(qm2, 1, 0, 1, 0);
    am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    am1.allocate("127.0.0.1" , 1000, 3, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }

    // finish the AMs
    finishApplicationMaster(loadedApp1, rm2, nm1, am1);
    // now AppAttempt and App becomes FINISHED,
    // we should also grant APP_ATTEMPT_REMOVE/APP_REMOVE event
    // had processed by scheduler
    rm2.waitForAppRemovedFromScheduler(loadedApp1.getApplicationId());
    assertQueueMetrics(qm2, 1, 0, 0, 1);
  }


  // The metrics has some carry-on value from the previous RM, because the
  // test case is in-memory, for the same queue name (e.g. root), there's
  // always a singleton QueueMetrics object.
  private int appsSubmittedCarryOn = 0;
  private int appsPendingCarryOn = 0;
  private int appsRunningCarryOn = 0;
  private int appsCompletedCarryOn = 0;

  private void resetQueueMetrics(QueueMetrics qm) {
    appsSubmittedCarryOn = qm.getAppsSubmitted();
    appsPendingCarryOn = qm.getAppsPending();
    appsRunningCarryOn = qm.getAppsRunning();
    appsCompletedCarryOn = qm.getAppsCompleted();
  }

  //when a new rm become leader the carry on are reseted.
  private void resetCarryOn() {
    appsSubmittedCarryOn = 0;
    appsPendingCarryOn = 0;
    appsRunningCarryOn = 0;
    appsCompletedCarryOn = 0;
  }
  
  private void assertQueueMetrics(QueueMetrics qm, int appsSubmitted,
      int appsPending, int appsRunning, int appsCompleted) {
    Assert.assertEquals(appsSubmitted + appsSubmittedCarryOn,
        qm.getAppsSubmitted());
    Assert.assertEquals(appsPending + appsPendingCarryOn,
        qm.getAppsPending());
    Assert.assertEquals(appsRunning + appsRunningCarryOn,
        qm.getAppsRunning());
    Assert.assertEquals(appsCompleted + appsCompletedCarryOn,
        qm.getAppsCompleted());
  }

  @Test (timeout = 60000)
  public void testDecomissionedNMsMetricsOnRMRestart() throws Exception {
    YarnConfiguration conf = new YarnConfiguration(this.conf);
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
      hostFile.getAbsolutePath());
    writeToHostsFile("");
    MockRM rm1 = null, rm2 = null;
    try {
      rm1 = new MockRM(conf);
      Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
      rm1.start();
      MockNM nm1 = rm1.registerNode("localhost:1234", 8000);
      MockNM nm2 = rm1.registerNode("host2:1234", 8000);
      Resource expectedCapability =
          Resource.newInstance(nm1.getMemory(), nm1.getvCores());
      String expectedVersion = nm1.getVersion();
      Assert
          .assertEquals(0,
              ClusterMetrics.getMetrics().getNumDecommisionedNMs());
      String ip = NetUtils.normalizeHostName("localhost");
      // Add 2 hosts to exclude list.
      writeToHostsFile("host2", ip);

      // refresh nodes
      rm1.getNodesListManager().refreshNodes(conf);
      NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
      Assert
          .assertTrue(
              NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));
      nodeHeartbeat = nm2.nodeHeartbeat(true);
      Assert.assertTrue("The decommisioned metrics are not updated",
          NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));

      rm1.drainEvents();
      Assert
          .assertEquals(2,
              ClusterMetrics.getMetrics().getNumDecommisionedNMs());
      verifyNodesAfterDecom(rm1, 2, expectedCapability, expectedVersion);
      rm1.stop();
      rm1 = null;
      Assert
          .assertEquals(0,
              ClusterMetrics.getMetrics().getNumDecommisionedNMs());

      // restart RM.
      rm2 = new MockRM(conf);
      rm2.start();
      rm2.drainEvents();
      Assert
          .assertEquals(2,
              ClusterMetrics.getMetrics().getNumDecommisionedNMs());
      verifyNodesAfterDecom(rm2, 2, Resource.newInstance(0, 0), "unknown");
    } finally {
      if (rm1 != null) {
        rm1.stop();
      }
      if (rm2 != null) {
        rm2.stop();
      }
    }
  }

  private void verifyNodesAfterDecom(MockRM rm, int numNodes,
                                     Resource expectedCapability,
                                     String expectedVersion) {
    ConcurrentMap<NodeId, RMNode> inactiveRMNodes =
        rm.getRMContext().getInactiveRMNodes();
    Assert.assertEquals(numNodes, inactiveRMNodes.size());
    for (RMNode rmNode : inactiveRMNodes.values()) {
      Assert.assertEquals(expectedCapability, rmNode.getTotalCapability());
      Assert.assertEquals(expectedVersion, rmNode.getNodeManagerVersion());
    }
  }

  // Test Delegation token is renewed synchronously so that recover events
  // can be processed before any other external incoming events, specifically
  // the ContainerFinished event on NM re-registraton.
  @Test (timeout = 20000)
  public void testSynchronouslyRenewDTOnRecovery() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");

    // start RM
    MockRM rm1 = createMockRM(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app0 = rm1.submitApp(200);
    final MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    MockRM rm2 = new MockRM(conf) {
      @Override
      protected ResourceTrackerService createResourceTrackerService() {
        return new ResourceTrackerService(this.rmContext,
          this.nodesListManager, this.nmLivelinessMonitor,
          this.rmContext.getContainerTokenSecretManager(),
          this.rmContext.getNMTokenSecretManager()) {
          @Override
          protected void serviceStart() throws Exception {
            // send the container_finished event as soon as the
            // ResourceTrackerService is started.
            super.serviceStart();
            nm1.setResourceTrackerService(getResourceTrackerService());
            NMContainerStatus status =
                TestRMRestart.createNMContainerStatus(
                    am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
            nm1.registerNode(Arrays.asList(status), null);
          }
        };
      }
    };

    try {
      // Re-start RM
      rm2.start();

      // wait for the 2nd attempt to be started.
      RMApp loadedApp0 =
          rm2.getRMContext().getRMApps().get(app0.getApplicationId());
      int timeoutSecs = 0;
      while (loadedApp0.getAppAttempts().size() != 2 && timeoutSecs++ < 40) {
        Thread.sleep(200);
      }
      MockAM am1 = MockRM.launchAndRegisterAM(loadedApp0, rm2, nm1);
      MockRM.finishAMAndVerifyAppState(loadedApp0, rm2, nm1, am1);
    } finally {
      rm2.stop();
    }
  }

  private void writeToHostsFile(String... hosts) throws IOException {
    if (!hostFile.exists()) {
      TEMP_DIR.mkdirs();
      hostFile.createNewFile();
    }
    FileOutputStream fStream = null;
    try {
      fStream = new FileOutputStream(hostFile);
      for (int i = 0; i < hosts.length; i++) {
        fStream.write(hosts[i].getBytes());
        fStream.write(System.getProperty("line.separator").getBytes());
      }
    } finally {
      if (fStream != null) {
        IOUtils.closeStream(fStream);
        fStream = null;
      }
    }
  }
  
  public static NMContainerStatus createNMContainerStatus(
      ApplicationAttemptId appAttemptId, int id, ContainerState containerState) {
    return createNMContainerStatus(appAttemptId, id, containerState,
        RMNodeLabelsManager.NO_LABEL);
  }

  public static NMContainerStatus createNMContainerStatus(
      ApplicationAttemptId appAttemptId, int id, ContainerState containerState,
      String nodeLabelExpression) {
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, id);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, 0, containerState,
            Resource.newInstance(1024, 1), "recover container", 0,
            Priority.newInstance(0), 0, nodeLabelExpression);
    return containerReport;
  }

  public class TestMemoryRMStateStore extends MemoryRMStateStore {
    int count = 0;
    public int updateApp = 0;
    public int updateAttempt = 0;

    @Override
    public void updateApplicationStateInternal(ApplicationId appId,
        ApplicationStateData appStateData) throws Exception {
      updateApp = ++count;
      super.updateApplicationStateInternal(appId, appStateData);
    }

    @Override
    public synchronized void
        updateApplicationAttemptStateInternal(
            ApplicationAttemptId attemptId,
            ApplicationAttemptStateData attemptStateData)
            throws Exception {
      updateAttempt = ++count;
      super.updateApplicationAttemptStateInternal(attemptId,
        attemptStateData);
    }
  }

  public static class TestSecurityMockRM extends MockRM {

    public TestSecurityMockRM(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    public TestSecurityMockRM(Configuration conf) {
      super(conf);
    }
    
    @Override
    public void init(Configuration conf) {
      // reset localServiceAddress.
      RMDelegationTokenIdentifier.Renewer.setSecretManager(null, null);
      super.init(conf);
    }

    @Override
    protected ClientRMService createClientRMService() {
      return new ClientRMService(getRMContext(), getResourceScheduler(),
          rmAppManager, applicationACLsManager, null,
          getRMContext().getRMDelegationTokenSecretManager()){
        @Override
        protected void serviceStart() throws Exception {
          // do nothing
        }

        @Override
        protected void serviceStop() throws Exception {
          //do nothing
        }
      };
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Do nothing.
    }
  }

  // Test does following verification
  // 1. Start RM1 with store patch /tmp
  // 2. Add/remove/replace labels to cluster and node lable and verify
  // 3. Start RM2 with store patch /tmp only
  // 4. Get cluster and node lobel, it should be present by recovering it
  @Test(timeout = 20000)
  public void testRMRestartRecoveringNodeLabelManager() throws Exception {
    // Initial FS node label store root dir to a random tmp dir
    File nodeLabelFsStoreDir =
        new File("target", this.getClass().getSimpleName()
            + "-testRMRestartRecoveringNodeLabelManager");
    if (nodeLabelFsStoreDir.exists()) {
      FileUtils.deleteDirectory(nodeLabelFsStoreDir);
    }
    nodeLabelFsStoreDir.deleteOnExit();
    
    String nodeLabelFsStoreDirURI = nodeLabelFsStoreDir.toURI().toString(); 
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        nodeLabelFsStoreDirURI);
    
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    MockRM rm1 = new MockRM(conf, memStore) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new RMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    rm1.init(conf);
    Assume.assumeFalse(rm1.getResourceScheduler() instanceof FairScheduler);
    rm1.start();

    RMNodeLabelsManager nodeLabelManager =
        rm1.getRMContext().getNodeLabelManager();

    Set<String> clusterNodeLabels = new HashSet<String>();
    clusterNodeLabels.add("x");
    clusterNodeLabels.add("y");
    clusterNodeLabels.add("z");
    // Add node label x,y,z
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(clusterNodeLabels);

    // Add node Label to Node h1->x
    NodeId n1 = NodeId.newInstance("h1", 0);
    nodeLabelManager.addLabelsToNode(ImmutableMap.of(n1, toSet("x")));

    clusterNodeLabels.remove("z");
    // Remove cluster label z
    nodeLabelManager.removeFromClusterNodeLabels(toSet("z"));

    // Replace nodelabel h1->x,y
    nodeLabelManager.replaceLabelsOnNode(ImmutableMap.of(n1, toSet("y")));

    // Wait for updating store.It is expected NodeStore update should happen
    // very fast since it has separate dispatcher. So waiting for max 5 seconds,
    // which is sufficient time to update NodeStore.
    int count = 10;
    while (count-- > 0) {
      if (nodeLabelManager.getNodeLabels().size() > 0) {
        break;
      }
      Thread.sleep(500);
    }

    Assert.assertEquals(clusterNodeLabels.size(), nodeLabelManager
        .getClusterNodeLabelNames().size());

    Map<NodeId, Set<String>> nodeLabels = nodeLabelManager.getNodeLabels();
    Assert.assertEquals(1, nodeLabelManager.getNodeLabels().size());
    Assert.assertTrue(nodeLabels.get(n1).equals(toSet("y")));

    MockRM rm2 = new MockRM(conf, memStore) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new RMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    rm2.init(conf);
    rm2.start();

    nodeLabelManager = rm2.getRMContext().getNodeLabelManager();
    Assert.assertEquals(clusterNodeLabels.size(),
        nodeLabelManager.getClusterNodeLabelNames().size());

    nodeLabels = nodeLabelManager.getNodeLabels();
    Assert.assertEquals(1, nodeLabelManager.getNodeLabels().size());
    Assert.assertTrue(nodeLabels.get(n1).equals(toSet("y")));
    rm1.stop();
    rm2.stop();
  }

  @Test(timeout = 60000)
  public void testRMRestartFailAppAttempt() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    int maxAttempt =
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    RMState rmState = memStore.getState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        rmState.getApplicationState();

    // start RM
    MockRM rm1 = createMockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    ApplicationId applicationId = app0.getApplicationId();
    ApplicationAttemptId appAttemptId1 =
        app0.getCurrentAppAttempt().getAppAttemptId();
    Assert.assertEquals(1, appAttemptId1.getAttemptId());

    // fail the 1st app attempt.
    rm1.failApplicationAttempt(appAttemptId1);

    rm1.waitForState(appAttemptId1, RMAppAttemptState.FAILED);
    rm1.waitForState(applicationId, RMAppState.ACCEPTED);

    ApplicationAttemptId appAttemptId2 =
        app0.getCurrentAppAttempt().getAppAttemptId();
    Assert.assertEquals(2, appAttemptId2.getAttemptId());
    rm1.waitForState(appAttemptId2, RMAppAttemptState.SCHEDULED);

    // restart rm
    MockRM rm2 = createMockRM(conf, memStore);
    rm2.start();
    RMApp loadedApp0 = rm2.getRMContext().getRMApps().get(applicationId);
    rm2.waitForState(applicationId, RMAppState.ACCEPTED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FAILED);


    Assert.assertEquals(2, loadedApp0.getAppAttempts().size());
    rm2.waitForState(appAttemptId2, RMAppAttemptState.SCHEDULED);

    appAttemptId2 = loadedApp0.getCurrentAppAttempt().getAppAttemptId();
    Assert.assertEquals(2, appAttemptId2.getAttemptId());

    // fail 2nd attempt
    rm2.failApplicationAttempt(appAttemptId2);

    rm2.waitForState(appAttemptId2, RMAppAttemptState.FAILED);
    rm2.waitForState(applicationId, RMAppState.FAILED);
    Assert.assertEquals(maxAttempt, loadedApp0.getAppAttempts().size());
  }

  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  @Test(timeout = 20000)
  public void testRMRestartNodeMapping() throws Exception {
    // Initial FS node label store root dir to a random tmp dir
    File nodeLabelFsStoreDir = new File("target",
        this.getClass().getSimpleName() + "-testRMRestartNodeMapping");
    if (nodeLabelFsStoreDir.exists()) {
      FileUtils.deleteDirectory(nodeLabelFsStoreDir);
    }
    nodeLabelFsStoreDir.deleteOnExit();
    String nodeLabelFsStoreDirURI = nodeLabelFsStoreDir.toURI().toString();
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        nodeLabelFsStoreDirURI);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    MockRM rm1 = new MockRM(conf, memStore) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new RMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    rm1.init(conf);
    rm1.start();
    RMNodeLabelsManager nodeLabelManager =
        rm1.getRMContext().getNodeLabelManager();

    Set<String> clusterNodeLabels = new HashSet<String>();
    clusterNodeLabels.add("x");
    clusterNodeLabels.add("y");
    nodeLabelManager
        .addToCluserNodeLabelsWithDefaultExclusivity(clusterNodeLabels);
    // Add node Label to Node h1->x
    NodeId n1 = NodeId.newInstance("h1", 1234);
    NodeId n2 = NodeId.newInstance("h1", 1235);
    NodeId nihost = NodeId.newInstance("h1", 0);
    nodeLabelManager.replaceLabelsOnNode(ImmutableMap.of(n1, toSet("x")));
    nodeLabelManager.replaceLabelsOnNode(ImmutableMap.of(n2, toSet("x")));
    nodeLabelManager.replaceLabelsOnNode(ImmutableMap.of(nihost, toSet("y")));
    nodeLabelManager.replaceLabelsOnNode(ImmutableMap.of(n1, toSet("x")));
    MockRM rm2 = null;
    for (int i = 0; i < 2; i++) {
      rm2 = new MockRM(conf, memStore) {
        @Override
        protected RMNodeLabelsManager createNodeLabelManager() {
          RMNodeLabelsManager mgr = new RMNodeLabelsManager();
          mgr.init(getConfig());
          return mgr;
        }
      };
      rm2.init(conf);
      rm2.start();

      nodeLabelManager = rm2.getRMContext().getNodeLabelManager();
      Map<String, Set<NodeId>> labelsToNodes =
          nodeLabelManager.getLabelsToNodes(toSet("x"));
      Assert.assertEquals(1,
          null == labelsToNodes.get("x") ? 0 : labelsToNodes.get("x").size());
    }
    rm1.stop();
    rm2.stop();
  }

  @Test(timeout = 60000)
  public void testRMRestartAfterNodeLabelDisabled() throws Exception {
    // Skip this test case if it is not CapacityScheduler since NodeLabel is
    // not fully supported yet for FairScheduler and others.
    if (!getSchedulerType().equals(SchedulerType.CAPACITY)) {
      return;
    }
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

    MockRM rm1 = new MockRM(
        TestUtils.getConfigurationWithDefaultQueueLabels(conf), memStore) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new RMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    rm1.start();

    // add node label "x" and set node to label mapping
    Set<String> clusterNodeLabels = new HashSet<String>();
    clusterNodeLabels.add("x");
    RMNodeLabelsManager nodeLabelManager =
        rm1.getRMContext().getNodeLabelManager();
    nodeLabelManager.
        addToCluserNodeLabelsWithDefaultExclusivity(clusterNodeLabels);
    nodeLabelManager.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x

    // submit an application with specifying am node label expression as "x"
    RMApp app1 = rm1.submitApp(200, "someApp", "someUser", null, "a1", "x");
    // check am container allocated with correct node label expression
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    ContainerId  amContainerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    Assert.assertEquals("x", rm1.getRMContext().getScheduler().
        getRMContainer(amContainerId1).getNodeLabelExpression());
    finishApplicationMaster(app1, rm1, nm1, am1);

    // restart rm with node label disabled
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, false);
    MockRM rm2 = new MockRM(
        TestUtils.getConfigurationWithDefaultQueueLabels(conf), memStore) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new RMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };

    // rm should successfully start with app1 loaded back in FAILED state
    // due to node label not enabled but am resource request contains
    // node label expression.
    try {
      rm2.start();
      Assert.assertTrue("RM start successfully", true);
      Assert.assertEquals(1, rm2.getRMContext().getRMApps().size());
      rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    } catch (Exception e) {
      LOG.debug("Exception on start", e);
      Assert.fail("RM should start without any issue");
    } finally {
      rm1.stop();
      rm2.stop();
    }
  }
}
