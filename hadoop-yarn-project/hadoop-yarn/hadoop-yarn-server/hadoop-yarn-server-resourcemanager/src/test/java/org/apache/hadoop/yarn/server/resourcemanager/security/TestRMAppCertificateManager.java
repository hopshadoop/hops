/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.security;

import io.hops.security.HopsUtil;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.DBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppCertificateGeneratedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TestRMAppCertificateManager {
  private static final Log LOG = LogFactory.getLog(TestRMAppCertificateManager.class);
  private static final String BASE_DIR = Paths.get(System.getProperty("test.build.dir",
      Paths.get("target", "test-dir").toString()),
      TestRMAppCertificateManager.class.getSimpleName()).toString();
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  private static String classPath;
  
  private Configuration conf;
  private DrainDispatcher dispatcher;
  private RMContext rmContext;
  private File sslServerFile;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    BASE_DIR_FILE.mkdirs();
    classPath = KeyStoreTestUtil.getClasspathDir(TestRMAppCertificateManager.class);
  }
  
  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY, "https://bbc3.sics.se:33473");
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "5s");
    RMAppCertificateActionsFactory.getInstance().clear();
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
    //conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    dispatcher = new DrainDispatcher();
    rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, null, null, null);
    dispatcher.init(conf);
    dispatcher.start();
  
    String sslConfFileName = TestRMAppCertificateManager.class.getSimpleName() + ".ssl-server.xml";
    sslServerFile = Paths.get(classPath, sslConfFileName).toFile();
    Configuration sslServer = new Configuration(false);
    sslServer.set(HopsworksRMAppCertificateActions.HOPSWORKS_USER_KEY, "agent-user");
    sslServer.set(HopsworksRMAppCertificateActions.HOPSWORKS_PASSWORD_KEY, "agent-password");
    KeyStoreTestUtil.saveConfig(sslServerFile, sslServer);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFileName);
  }
  
  @After
  public void afterTest() throws Exception {
    if (dispatcher != null) {
      dispatcher.stop();
    }
    if (sslServerFile != null) {
      sslServerFile.delete();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
    
    RMAppCertificateActionsFactory.getInstance().clear();
  }
  
  @Test
  public void testSuccessfulCertificateCreationTesting() throws Exception {
    File testSpecificSSLServerFile = null;
    try {
      conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
          "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
  
      RMAppCertificateActions testActor = RMAppCertificateActionsFactory.getInstance().getActor(conf);
      String trustStore = Paths.get(BASE_DIR, "trustStore.jks").toString();
      X509Certificate caCert = ((TestingRMAppCertificateActions) testActor).getCaCert();
      String principal = caCert.getIssuerX500Principal().getName();
      // Principal should be CN=RootCA
      String alias = principal.split("=")[1];
      String password = "password";
  
      String sslServer = TestRMAppCertificateManager.class.getSimpleName() + "-testSuccessfulCertificateCreationTesting.ssl-server.xml";
      testSpecificSSLServerFile = Paths.get(classPath, sslServer)
          .toFile();
  
      conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServer);
  
      createTrustStore(trustStore, password, alias, caCert);
      Configuration sslServerConf = createSSLConfig("", "", "", trustStore, password, "");
      saveConfig(testSpecificSSLServerFile.getAbsoluteFile(), sslServerConf);
  
      MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.CERTS_GENERATED);
      rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
  
      MockRMAppCertificateManager manager = new MockRMAppCertificateManager(true, rmContext);
      manager.init(conf);
      manager.start();
      manager.handle(new RMAppCertificateManagerEvent(
          ApplicationId.newInstance(System.currentTimeMillis(), 1),
          "userA", 1,
          RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
  
      dispatcher.await();
      eventHandler.verifyEvent();
      manager.stop();
    } finally {
      if (testSpecificSSLServerFile != null) {
        testSpecificSSLServerFile.delete();
      }
    }
  }
  
  @Test
  public void testCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    MockRMAppCertificateManager certificateManager = new MockRMAppCertificateManager(false, rmContext);
    certificateManager.init(conf);
    certificateManager.start();
    Instant now = Instant.now();
    Instant expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(now.toEpochMilli(), 1);
    certificateManager.setOldCertificateExpiration(expiration.toEpochMilli());
    
    certificateManager.registerWithCertificateRenewer(appId, "Dolores", 1, expiration.toEpochMilli());
    Map<ApplicationId, ScheduledFuture> tasks = certificateManager.getRenewalTasks();
    ScheduledFuture renewalTask = tasks.get(appId);
    assertFalse(renewalTask.isCancelled());
    assertFalse(renewalTask.isDone());
    
    // Wait until the scheduled task is executed
    TimeUnit.SECONDS.sleep(10);
    assertTrue(renewalTask.isDone());
    assertFalse(certificateManager.getRenewalException());
    assertTrue(tasks.isEmpty());
    certificateManager.stop();
  }
  
  @Test(timeout = 12000)
  public void testFailedCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    MockFailingRMAppCertificateManager certificateManager = new MockFailingRMAppCertificateManager(Integer.MAX_VALUE);
    certificateManager.init(conf);
    certificateManager.start();
  
    Instant now = Instant.now();
    Instant expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(now.toEpochMilli(), 1);
    certificateManager.registerWithCertificateRenewer(appId, "Dolores", 1, expiration.toEpochMilli());
    Map<ApplicationId, ScheduledFuture> tasks = certificateManager.getRenewalTasks();
    // There should be a scheduled task
    ScheduledFuture task = tasks.get(appId);
    assertFalse(task.isCancelled());
    assertFalse(task.isDone());
    assertFalse(certificateManager.hasRenewalFailed());
    assertEquals(0, certificateManager.getNumberOfRenewalFailures());
    
    TimeUnit.SECONDS.sleep(10);
    assertTrue(tasks.isEmpty());
    assertEquals(4, certificateManager.getNumberOfRenewalFailures());
    assertTrue(certificateManager.hasRenewalFailed());
    certificateManager.stop();
  }
  
  @Test(timeout = 12000)
  public void testRetryCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    MockFailingRMAppCertificateManager certificateManager = new MockFailingRMAppCertificateManager(2);
    certificateManager.init(conf);
    certificateManager.start();
  
    Instant now = Instant.now();
    Instant expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(now.toEpochMilli(), 1);
    certificateManager.registerWithCertificateRenewer(appId, "Dolores", 1, expiration.toEpochMilli());
    TimeUnit.SECONDS.sleep(10);
    assertEquals(2, certificateManager.getNumberOfRenewalFailures());
    assertFalse(certificateManager.hasRenewalFailed());
    assertTrue(certificateManager.getRenewalTasks().isEmpty());
    certificateManager.stop();
  }
  
  // This test makes a REST call to Hopsworks using HopsworksRMAppCertificateActions actor class
  // Normally it should be ignored as it requires Hopsworks instance to be running
  @Test
  @Ignore
  public void testSuccessfulCertificateCreationRemote() throws Exception {
    DevHopsworksRMAppCertificateActions mockRemoteActions = Mockito.spy(new DevHopsworksRMAppCertificateActions());
    mockRemoteActions.setConf(conf);
    mockRemoteActions.init();
    RMAppCertificateActionsFactory.getInstance().register(mockRemoteActions);
    MockRMAppCertificateManager manager = new MockRMAppCertificateManager(false, rmContext);
    manager.init(conf);
    manager.start();
    manager.handle(new RMAppCertificateManagerEvent(
        ApplicationId.newInstance(System.currentTimeMillis(), 1),
        "userA", 1,
        RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
    
    dispatcher.await();
    manager.stop();
  }
  
  // This test makes a REST call to Hopsworks using HopsworksRMAppCertificateActions actor class
  // Normally it should be ignored as it requires Hopsworks instance to be running
  @Test
  @Ignore
  public void testCertificateRevocationRemote() throws Exception {
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    
    DevHopsworksRMAppCertificateActions mockRemoteActions = Mockito.spy(new DevHopsworksRMAppCertificateActions());
    mockRemoteActions.setConf(conf);
    mockRemoteActions.init();
    RMAppCertificateActionsFactory.getInstance().register(mockRemoteActions);
    
    MockRMAppCertificateManager manager = Mockito.spy(new MockRMAppCertificateManager(false, rmContext));
    manager.init(conf);
    manager.start();
    String username = "Alice";
    Integer cryptoMaterialVersion = 1;
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    manager.handle(new RMAppCertificateManagerEvent(
        appId, username, cryptoMaterialVersion, RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
  
    dispatcher.await();
    Mockito.verify(mockRemoteActions).sign(Mockito.any(PKCS10CertificationRequest.class));
    
    manager.handle(new RMAppCertificateManagerEvent(
        appId, username, cryptoMaterialVersion, RMAppCertificateManagerEventType.REVOKE_CERTIFICATE));
    
    dispatcher.await();
    Mockito.verify(manager)
        .revokeCertificate(appId, username, cryptoMaterialVersion);
    Mockito.verify(manager)
        .deregisterFromCertificateRenewer(appId);
  
    TimeUnit.SECONDS.sleep(3);
    
    String certificateIdentifier = username + "__" + appId.toString() + "__" + cryptoMaterialVersion;
    Mockito.verify(mockRemoteActions)
        .revoke(Mockito.eq(certificateIdentifier));
    
    manager.stop();
  }
  
  @Test
  public void testFailingCertificateCreationLocal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    
    MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.KILL);
    rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
    
    MockFailingRMAppCertificateManager manager = new MockFailingRMAppCertificateManager(Integer.MAX_VALUE);
    manager.init(conf);
    manager.start();
    manager.handle(new RMAppCertificateManagerEvent(
        ApplicationId.newInstance(System.currentTimeMillis(), 1),
        "userA", 1,
        RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
    dispatcher.await();
    eventHandler.verifyEvent();
    manager.stop();
  }
  
  @Test(timeout = 20000)
  public void testCertificateRevocationMonitor() throws Exception {
    RMAppCertificateActions actor = Mockito.spy(new TestingRMAppCertificateActions());
    actor.init();
    RMAppCertificateActionsFactory.getInstance().register(actor);
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "40s");
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_REVOCATION_MONITOR_INTERVAL, "3s");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    
    MockRM rm = new MyMockRM(conf);
    rm.start();
  
    MockNM nm = new MockNM("127.0.0.1:8032", 15 * 1024, rm.getResourceTrackerService());
    nm.registerNode();
    
    RMApp application = rm.submitApp(1024, "application1", "Phil",
        new HashMap<ApplicationAccessType, String>(), false, "default", 2, null,
        "MAPREDUCE", true, false);
    nm.nodeHeartbeat(true);
    
    // Wait for the renewal to happen
    while (!application.isAppRotatingCryptoMaterial()) {
      TimeUnit.MILLISECONDS.sleep(500);
    }
    
    assertTrue(application.isAppRotatingCryptoMaterial());
    assertNotEquals(-1L, application.getMaterialRotationStartTime());
    
    // No NM heartbeat. NM will not inform about the updated crypto material
    // Wait for the monitor to kick in
    TimeUnit.SECONDS.sleep(6);
    assertFalse(application.isAppRotatingCryptoMaterial());
    assertEquals(-1L, application.getMaterialRotationStartTime());
    String certId = RMAppCertificateManager.getCertificateIdentifier(application.getApplicationId(),
        application.getUser(), application.getCryptoMaterialVersion() - 1);
    Mockito.verify(actor).revoke(Mockito.eq(certId));
    
    // Since NM didn't respond acknowledging the crypto update, revokeCertificate on RMAppCertificateManager
    // should not have been called. The revocation has been handled by the the revocation monitor
    Mockito.verify(rm.getRMContext().getRMAppCertificateManager(), Mockito.never())
        .revokeCertificate(Mockito.any(ApplicationId.class), Mockito.anyString(), Mockito.anyInt(), Mockito.anyBoolean());
    rm.stop();
  }
  
  @Test
  public void testApplicationSubmission() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, DBRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "45s");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    
    MockRM rm  = new MyMockRM(conf);
    rm.start();
  
    MockNM nm = new MockNM("127.0.0.1:8032", 15 * 1024, rm.getResourceTrackerService());
    nm.registerNode();
    
    RMApp application = rm.submitApp(1024, "application1", "Phil",
        new HashMap<ApplicationAccessType, String>(), false, "default", 2, null,
        "MAPREDUCE", true, false);
    
    nm.nodeHeartbeat(true);
    
    assertNotNull(application);
    byte[] keyStore = application.getKeyStore();
    assertNotNull(keyStore);
    assertNotEquals(0, keyStore.length);
    char[] keyStorePassword = application.getKeyStorePassword();
    assertNotNull(keyStorePassword);
    assertNotEquals(0, keyStorePassword.length);
    byte[] trustStore = application.getTrustStore();
    assertNotNull(trustStore);
    assertNotEquals(0, trustStore.length);
    char[] trustStorePassword = application.getTrustStorePassword();
    Integer cryptoMaterialVersion = application.getCryptoMaterialVersion();
    assertNotNull(trustStorePassword);
    assertNotEquals(0, trustStorePassword.length);
    
    // NOTE: The part below is very sensitive to timing issues
    
    // Expiration time for testing TestingRMAppCertificateActions is now + 50 seconds
    TimeUnit.SECONDS.sleep(5);
    // Certificate renewal should have happened by now
    byte[] newKeyStore = application.getKeyStore();
    assertFalse(Arrays.equals(keyStore, newKeyStore));
    assertNotEquals(0, newKeyStore.length);
    byte[] newTrustStore = application.getTrustStore();
    assertFalse(Arrays.equals(trustStore, newTrustStore));
    assertNotEquals(0, newTrustStore.length);
    char[] newKeyStorePass = application.getKeyStorePassword();
    assertFalse(Arrays.equals(keyStorePassword, newKeyStorePass));
    assertNotEquals(0, newKeyStorePass.length);
    char[] newTrustStorePass = application.getTrustStorePassword();
    assertFalse(Arrays.equals(trustStorePassword, newTrustStorePass));
    assertNotEquals(0, newTrustStorePass.length);
    Integer currentCryptoMaterialVersion = application.getCryptoMaterialVersion();
    assertEquals(++cryptoMaterialVersion, currentCryptoMaterialVersion);
    
    ApplicationStateData appState = rm.getRMContext().getStateStore().loadState().getApplicationState()
        .get(application.getApplicationId());
    assertTrue(Arrays.equals(newKeyStore, appState.getKeyStore()));
    assertTrue(Arrays.equals(newTrustStore, appState.getTrustStore()));
    assertTrue(Arrays.equals(newKeyStorePass, appState.getKeyStorePassword()));
    assertTrue(Arrays.equals(newTrustStorePass, appState.getTrustStorePassword()));
    assertEquals(currentCryptoMaterialVersion, appState.getCryptoMaterialVersion());
    // Still NM didn't respond with updated crypto material for app
    assertTrue(appState.isDuringMaterialRotation());
    assertNotEquals(-1L, appState.getMaterialRotationStartTime());
  
    Set<ApplicationId> updatedAppCrypto = new HashSet<>(1);
    updatedAppCrypto.add(application.getApplicationId());
  
    
    nm.nodeHeartbeat(Collections.<ContainerStatus>emptyList(), Collections.<Container>emptyList(), true, nm
        .getNextResponseId(), updatedAppCrypto);
    
    TimeUnit.MILLISECONDS.sleep(100);
    
    RMAppImpl appImpl = (RMAppImpl) application;
    assertNull(appImpl.getRMNodesUpdatedCryptoMaterial());
    assertFalse(application.isAppRotatingCryptoMaterial());
    
    appState = rm.getRMContext().getStateStore().loadState().getApplicationState().get(application.getApplicationId());
    // By now material rotation should have stopped
    assertFalse(appState.isDuringMaterialRotation());
    assertEquals(-1L, appState.getMaterialRotationStartTime());
    // Application should still be registered with the certificate renewer
    assertTrue(rm.getRMContext().getRMAppCertificateManager().getRenewalTasks().containsKey(application.getApplicationId()));
    
    TimeUnit.MILLISECONDS.sleep(100);
    
    Mockito.verify(rm.getRMContext().getRMAppCertificateManager())
        .revokeCertificate(Mockito.eq(application.getApplicationId()), Mockito.eq(application.getUser()),
            Mockito.eq(application.getCryptoMaterialVersion() - 1), Mockito.eq(true));
    rm.stop();
    
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "2d");
    MyMockRM rm2 = new MyMockRM(conf);
    rm2.start();
    nm.setResourceTrackerService(rm2.getResourceTrackerService());
    nm.nodeHeartbeat(true);
    RMApp recoveredApp = rm2.getRMContext().getRMApps().get(application.getApplicationId());
    assertNotNull(recoveredApp);
    assertTrue(Arrays.equals(newKeyStore, recoveredApp.getKeyStore()));
    appState = rm2.getRMContext().getStateStore().loadState().getApplicationState().get(application.getApplicationId());
    // RMApp should not recover in material rotation phase
    assertFalse(appState.isDuringMaterialRotation());
    assertEquals(-1L, appState.getMaterialRotationStartTime());
    assertTrue(rm2.getRMContext().getRMAppCertificateManager().getRenewalTasks()
        .containsKey(application.getApplicationId()));
    
    rm2.killApp(application.getApplicationId());
    rm2.waitForState(application.getApplicationId(), RMAppState.KILLED);
    assertTrue(rm2.getRMContext().getRMAppCertificateManager().getRenewalTasks().isEmpty());
    rm2.stop();
  }
  
  @Test
  public void testContainerAllocationDuringMaterialRotation() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, DBRMStateStore.class.getName());
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "40s");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    
    MockRM rm = new MyMockRM2(conf);
    rm.start();
    MockNM nm1 = new MockNM("127.0.0.1:1234", 2 * 1024, rm.getResourceTrackerService());
    nm1.registerNode();
    
    RMApp app = rm.submitApp(1024);
    // Trigger scheduling for AM
    nm1.nodeHeartbeat(true);
  
    RMAppAttempt appAttempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(appAttempt.getAppAttemptId());
    am.registerAppAttempt(true);
    
    // Allocate one container
    am.allocate("127.0.0.1", 512, 1, Collections.<ContainerId>emptyList());
    
    // Trigger scheduler
    nm1.nodeHeartbeat(true);
    List<Container> allocatedContainers = am.allocate(Collections.<ResourceRequest>emptyList(), Collections.<ContainerId>emptyList())
        .getAllocatedContainers();
    while (allocatedContainers.size() < 1) {
      nm1.nodeHeartbeat(true);
      TimeUnit.MILLISECONDS.sleep(200);
      allocatedContainers = am.allocate(Collections.<ResourceRequest>emptyList(), Collections.<ContainerId>emptyList()).getAllocatedContainers();
    }
    
    // Wait for the renewal to happen
    while (!app.isAppRotatingCryptoMaterial()) {
      TimeUnit.MILLISECONDS.sleep(500);
    }
    
    // Register second NM
    MockNM nm2 = new MockNM("127.0.0.2:1234", 2 * 1024, rm.getResourceTrackerService());
    nm2.registerNode();
  
    assertTrue(app.isAppRotatingCryptoMaterial());
    
    // Allocate second container while app is in Crypto Material rotation phase (see MyMockRM2)
    am.allocate("127.0.0.2", 512, 1, Collections.<ContainerId>emptyList());
    NodeHeartbeatResponse nmResponse = nm2.nodeHeartbeat(true);
    assertTrue(nmResponse.getUpdatedCryptoForApps().isEmpty());
    
    allocatedContainers = am.allocate(Collections.<ResourceRequest>emptyList(), Collections.<ContainerId>emptyList()).getAllocatedContainers();
    while (allocatedContainers.size() < 1) {
      nmResponse = nm2.nodeHeartbeat(true);
      assertTrue(nmResponse.getUpdatedCryptoForApps().isEmpty());
      TimeUnit.MILLISECONDS.sleep(200);
      allocatedContainers = am.allocate(Collections.<ResourceRequest>emptyList(), Collections.<ContainerId>emptyList()).getAllocatedContainers();
    }
    assertEquals(1, allocatedContainers.size());
    assertEquals(nm2.getNodeId(), allocatedContainers.get(0).getNodeId());
    
    TimeUnit.MILLISECONDS.sleep(500);
    RMNodeImpl rmNode2 = (RMNodeImpl) rm.getRMContext().getRMNodes().get(nm2.getNodeId());
    assertNotNull(rmNode2);
    
    int wait = 0;
    while (rmNode2.getAppCryptoMaterialToUpdate().isEmpty() && wait < 10) {
      TimeUnit.MILLISECONDS.sleep(300);
      wait++;
    }
    
    assertFalse(rmNode2.getAppCryptoMaterialToUpdate().isEmpty());
    
    nmResponse = nm2.nodeHeartbeat(true);
    assertTrue(nmResponse.getUpdatedCryptoForApps().containsKey(app.getApplicationId()));
    
    rm.stop();
  }
  
  private RMApp createNewTestApplication(int appId) throws IOException {
    ApplicationId applicationID = MockApps.newAppID(appId);
    String user = MockApps.newUserName();
    String name = MockApps.newAppName();
    String queue = MockApps.newQueue();
    YarnScheduler scheduler = Mockito.mock(YarnScheduler.class);
    ApplicationMasterService appMasterService = new ApplicationMasterService(rmContext, scheduler);
    ApplicationSubmissionContext applicationSubmissionContext = new ApplicationSubmissionContextPBImpl();
    applicationSubmissionContext.setApplicationId(applicationID);
    RMApp app = new RMAppImpl(applicationID, rmContext, conf, name, user, queue, applicationSubmissionContext,
        scheduler, appMasterService, System.currentTimeMillis(), "YARN", null, Mockito.mock(ResourceRequest.class));
    rmContext.getRMApps().put(applicationID, app);
    return app;
  }
  
  private class MockRMAppEventHandler implements EventHandler<RMAppEvent> {
  
    private final RMAppEventType expectedEventType;
    private boolean assertionFailure;
    
    private MockRMAppEventHandler(RMAppEventType expectedEventType) {
      this.expectedEventType = expectedEventType;
      assertionFailure = false;
    }
    
    @Override
    public void handle(RMAppEvent event) {
      if (event == null) {
        assertionFailure = true;
      } else if (!expectedEventType.equals(event.getType())) {
        assertionFailure = true;
      } else if (event.getType().equals(RMAppEventType.CERTS_GENERATED)) {
        if (!(event instanceof RMAppCertificateGeneratedEvent)) {
          assertionFailure = true;
        }
      }
    }
    
    private void verifyEvent() {
      assertFalse(assertionFailure);
    }
    
  }
  
  private class MyMockRM2 extends MockRM {
    public MyMockRM2(Configuration conf) {
      super(conf);
    }
  
    @Override
    protected RMAppManager createRMAppManager() {
      return new MyRMAppManager(this.rmContext, this.scheduler, this.masterService, this.applicationACLsManager,
          this.getConfig());
    }
    
    private class MyRMAppManager extends RMAppManager {
  
      public MyRMAppManager(RMContext context, YarnScheduler scheduler,
          ApplicationMasterService masterService,
          ApplicationACLsManager applicationACLsManager,
          Configuration conf) {
        super(context, scheduler, masterService, applicationACLsManager, conf);
      }
  
      @Override
      protected RMApp createRMApp(ApplicationId applicationId, String user,
          ApplicationSubmissionContext submissionContext,
          long submitTime, ResourceRequest amReq) throws IOException {
        return new MyRMApp(applicationId, rmContext, getConfig(), submissionContext.getApplicationName(),
            user, submissionContext.getQueue(), submissionContext, scheduler, masterService, submitTime,
            submissionContext.getApplicationType(), submissionContext.getApplicationTags(), amReq);
      }
    }
    
    private class MyRMApp extends RMAppImpl {
  
      public MyRMApp(ApplicationId applicationId, RMContext rmContext, Configuration config, String name,
          String user, String queue, ApplicationSubmissionContext submissionContext,
          YarnScheduler scheduler, ApplicationMasterService masterService, long submitTime, String applicationType,
          Set<String> applicationTags, ResourceRequest amReq) throws IOException {
        super(applicationId, rmContext, config, name, user, queue, submissionContext, scheduler, masterService,
            submitTime,
            applicationType, applicationTags, amReq);
      }
      
      @Override
      public void rmNodeHasUpdatedCryptoMaterial(NodeId nodeId) {
        // Do nothing - RMApp will stay in Crypto Material Rotation phase
      }
    }
  }
  
  private class MyMockRM extends MockRM {
  
    public MyMockRM(Configuration conf) {
      super(conf);
    }
  
    @Override
    protected RMAppCertificateManager createRMAppCertificateManager() throws Exception {
      MockRMAppCertificateManager spyCertManager = Mockito.spy(new MockRMAppCertificateManager(false, rmContext));
      return spyCertManager;
    }
  }
  
  private class MockRMAppCertificateManager extends RMAppCertificateManager {
    private final boolean loadTrustStore;
    private final String systemTMP;
    private long oldCertificateExpiration;
  
    public MockRMAppCertificateManager(boolean loadTrustStore, RMContext rmContext) throws Exception {
      super(rmContext);
      this.loadTrustStore = loadTrustStore;
      systemTMP = System.getProperty("java.io.tmpdir");
    }
  
    @Override
    public KeyStore loadSystemTrustStore(Configuration conf) throws GeneralSecurityException, IOException {
      if (loadTrustStore) {
        return super.loadSystemTrustStore(conf);
      }
      KeyStore emptyTrustStore = KeyStore.getInstance("JKS");
      emptyTrustStore.load(null, null);
      return emptyTrustStore;
    }
  
    @Override
    public void generateCertificate(ApplicationId applicationId, String appUser, Integer cryptoMaterialVersion) {
      boolean exceptionThrown = false;
      ByteArrayInputStream bio = null;
      try {
        KeyPair keyPair = generateKeyPair();
        // Generate CSR
        PKCS10CertificationRequest csr = generateCSR(applicationId, appUser, keyPair, cryptoMaterialVersion);
        
        assertEquals(appUser, HopsUtil.extractCNFromSubject(csr.getSubject().toString()));
        assertEquals(applicationId.toString(), HopsUtil.extractOFromSubject(csr.getSubject().toString()));
        assertEquals(String.valueOf(cryptoMaterialVersion), HopsUtil.extractOUFromSubject(csr.getSubject().toString()));
        
        // Sign CSR
        X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
        signedCertificate.checkValidity();
        long expiration = signedCertificate.getNotAfter().getTime();
        long epochNow = Instant.now().toEpochMilli();
        assertTrue(expiration >= epochNow);
        
        RMAppCertificateActions actor = getRmAppCertificateActions();
        
        if (actor instanceof TestingRMAppCertificateActions) {
          X509Certificate caCert = ((TestingRMAppCertificateActions) actor).getCaCert();
          signedCertificate.verify(caCert.getPublicKey(), "BC");
        }
        
        KeyStoresWrapper appKeystoreWrapper = createApplicationStores(signedCertificate, keyPair.getPrivate(),
            appUser, applicationId);
        X509Certificate extractedCert = (X509Certificate) appKeystoreWrapper.getKeystore().getCertificate(appUser);
        byte[] rawKeystore = appKeystoreWrapper.getRawKeyStore(TYPE.KEYSTORE);
        assertNotNull(rawKeystore);
        assertNotEquals(0, rawKeystore.length);
        
        File keystoreFile = Paths.get(systemTMP, appUser + "-" + applicationId.toString() + "_kstore.jks").toFile();
        // Keystore should have been deleted
        assertFalse(keystoreFile.exists());
        char[] keyStorePassword = appKeystoreWrapper.getKeyStorePassword();
        assertNotNull(keyStorePassword);
        assertNotEquals(0, keyStorePassword.length);
        
        byte[] rawTrustStore = appKeystoreWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
        File trustStoreFile = Paths.get(systemTMP, appUser + "-" + applicationId.toString() + "_tstore.jks").toFile();
        // Truststore should have been deleted
        assertFalse(trustStoreFile.exists());
        char[] trustStorePassword = appKeystoreWrapper.getTrustStorePassword();
        assertNotNull(trustStorePassword);
        assertNotEquals(0, trustStorePassword.length);
        
        verifyContentOfAppTrustStore(rawTrustStore, trustStorePassword, appUser, applicationId);
        
        if (actor instanceof TestingRMAppCertificateActions) {
          X509Certificate caCert = ((TestingRMAppCertificateActions) actor).getCaCert();
          extractedCert.verify(caCert.getPublicKey(), "BC");
        }
        assertEquals(appUser, HopsUtil.extractCNFromSubject(extractedCert.getSubjectX500Principal().getName()));
        assertEquals(applicationId.toString(), HopsUtil.extractOFromSubject(
            extractedCert.getSubjectX500Principal().getName()));
        assertEquals(String.valueOf(cryptoMaterialVersion),
            HopsUtil.extractOUFromSubject(extractedCert.getSubjectX500Principal().getName()));
  
        RMAppCertificateGeneratedEvent startEvent = new RMAppCertificateGeneratedEvent(applicationId,
            rawKeystore, keyStorePassword, rawTrustStore, trustStorePassword, expiration, RMAppEventType.CERTS_GENERATED);
        getRmContext().getDispatcher().getEventHandler().handle(startEvent);
      } catch (Exception ex) {
        LOG.error(ex, ex);
        exceptionThrown = true;
      } finally {
        if (bio != null) {
          try {
            bio.close();
          } catch (IOException ex) {
            // Ignore
          }
        }
      }
      assertFalse(exceptionThrown);
    }
    
    @Override
    public void revokeCertificate(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion) {
      try {
        putToQueue(appId, applicationUser, cryptoMaterialVersion);
        waitForQueueToDrain();
      } catch (InterruptedException ex) {
        LOG.error(ex, ex);
        fail("Exception should not be thrown here");
      }
    }
    
    @Override
    public boolean isRPCTLSEnabled() {
      return true;
    }
    
    private void verifyContentOfAppTrustStore(byte[] appTrustStore, char[] password, String appUser,
        ApplicationId appId)
        throws GeneralSecurityException, IOException {
      File trustStoreFile = Paths.get(systemTMP, appUser + "-" + appId.toString() + "_tstore.jks").toFile();
      boolean certificateMissing = false;
      
      try {
        KeyStore systemTrustStore = loadSystemTrustStore(conf);
        FileUtils.writeByteArrayToFile(trustStoreFile, appTrustStore, false);
        KeyStore ts = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
          ts.load(fis, password);
        }
  
        Enumeration<String> sysAliases = systemTrustStore.aliases();
        while (sysAliases.hasMoreElements()) {
          String alias = sysAliases.nextElement();
          
          X509Certificate appCert = (X509Certificate) ts.getCertificate(alias);
          if (appCert == null) {
            certificateMissing = true;
            break;
          }
          
          X509Certificate sysCert = (X509Certificate) systemTrustStore.getCertificate(alias);
          if (!Arrays.equals(sysCert.getSignature(), appCert.getSignature())) {
            certificateMissing = true;
            break;
          }
        }
      } finally {
        FileUtils.deleteQuietly(trustStoreFile);
        assertFalse(certificateMissing);
      }
    }
  
    public void setOldCertificateExpiration(long oldCertificateExpiration) {
      this.oldCertificateExpiration = oldCertificateExpiration;
    }
  
    @Override
    public Runnable createCertificateRenewerTask(ApplicationId appId, String appuser, Integer currentCryptoVersion) {
      return new MockCertificateRenewer(appId, appuser, currentCryptoVersion, 1);
    }
  
    private boolean renewalException = false;
    
    public boolean getRenewalException() {
      return renewalException;
    }
    
    public class MockCertificateRenewer extends CertificateRenewer {
      private final long oldCertificateExpiration;
      
      public MockCertificateRenewer(ApplicationId appId, String appUser, Integer currentCryptoVersion, long oldCertificateExpiration) {
        super(appId, appUser, currentCryptoVersion);
        this.oldCertificateExpiration = oldCertificateExpiration;
      }
      
      @Override
      public void run() {
        try {
          LOG.info("Renewing certificate for application " + appId);
          KeyPair keyPair = generateKeyPair();
          int oldCryptoVersion = currentCryptoVersion;
          PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, ++currentCryptoVersion);
          int newCryptoVersion = Integer.parseInt(HopsUtil.extractOUFromSubject(csr.getSubject().toString()));
          if (++oldCryptoVersion != newCryptoVersion) {
            LOG.error("Crypto version of new certificate is wrong: " + newCryptoVersion);
            renewalException = true;
          }
          X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
          long newCertificateExpiration = signedCertificate.getNotAfter().getTime();
          if (newCertificateExpiration <= oldCertificateExpiration) {
            LOG.error("New certificate expiration time is older than old certificate");
            renewalException = true;
          }
  
          KeyStoresWrapper keyStoresWrapper = createApplicationStores(signedCertificate, keyPair.getPrivate(), appUser,
              appId);
          byte[] rawProtectedKeyStore = keyStoresWrapper.getRawKeyStore(TYPE.KEYSTORE);
          byte[] rawTrustStore = keyStoresWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
  
          getRenewalTasks().remove(appId);
          
          getRmContext().getDispatcher().getEventHandler().handle(new RMAppCertificateGeneratedEvent(appId,
              rawProtectedKeyStore, keyStoresWrapper.getKeyStorePassword(), rawTrustStore, keyStoresWrapper
              .getTrustStorePassword(), newCertificateExpiration, RMAppEventType.CERTS_RENEWED));
          LOG.info("Renewed certificate for application " + appId);
        } catch (Exception ex) {
          LOG.error("Exception while renewing certificate. This should not have happened here", ex);
          renewalException = true;
        }
      }
    }
  }
  
  private class MockFailingRMAppCertificateManager extends RMAppCertificateManager {
    private int numberOfRenewalFailures = 0;
    private boolean renewalFailed = false;
    private final Integer succeedAfterRetries;
    
    public MockFailingRMAppCertificateManager(Integer succeedAfterRetries) {
      super(rmContext);
      this.succeedAfterRetries = succeedAfterRetries;
    }
  
    public int getNumberOfRenewalFailures() {
      return numberOfRenewalFailures;
    }
  
    public boolean hasRenewalFailed() {
      return renewalFailed;
    }
    
    @Override
    public boolean isRPCTLSEnabled() {
      return true;
    }
    
    @Override
    public void generateCertificate(ApplicationId appId, String appUser, Integer cryptoMaterialVersion) {
      getRmContext().getDispatcher().getEventHandler().handle(new RMAppEvent(appId, RMAppEventType.KILL));
    }
    
    @Override
    public Runnable createCertificateRenewerTask(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
      return new MockFailingCertificateRenewer(appId, appUser, currentCryptoVersion, succeedAfterRetries);
    }
    
    public class MockFailingCertificateRenewer extends CertificateRenewer {
  
      private final Integer succeedAfterRetries;
      public MockFailingCertificateRenewer(ApplicationId appId, String appUser, Integer currentCryptoVersion,
          Integer succeedAfterRetries) {
        super(appId, appUser, currentCryptoVersion);
        this.succeedAfterRetries = succeedAfterRetries;
      }
      
      @Override
      public void run() {
        try {
          if (((ExponentialBackOff)backOff).getNumberOfRetries() < succeedAfterRetries) {
            throw new Exception("Ooops something went wrong");
          }
          getRenewalTasks().remove(appId);
          LOG.info("Renewed certificate for application " + appId);
        } catch (Exception ex) {
          getRenewalTasks().remove(appId);
          backOffTime = backOff.getBackOffInMillis();
          if (backOffTime != -1) {
            numberOfRenewalFailures++;
            LOG.warn("Failed to renew certificate for application " + appId + ". Retrying in " + backOffTime + " ms");
            ScheduledFuture task = getScheduler().schedule(this, backOffTime, TimeUnit.MILLISECONDS);
            getRenewalTasks().put(appId, task);
          } else {
            LOG.error("Failed to renew certificate for application " + appId + " Failed more than 4 times, giving up");
            renewalFailed = true;
          }
        }
      }
    }
  }
  
  
  // These methods were taken from KeyStoreTestUtil
  // Cannot use KeyStoreTestUtil because of BouncyCastle version mismatch
  // between hadoop-common and hadoop-yarn-server-resourcemanager and classloader cannot find
  // certain BC classes
  private String getClasspathDir(Class klass) throws Exception {
    String file = klass.getName();
    file = file.replace('.', '/') + ".class";
    URL url = Thread.currentThread().getContextClassLoader().getResource(file);
    String baseDir = url.toURI().getPath();
    baseDir = baseDir.substring(0, baseDir.length() - file.length() - 1);
    return baseDir;
  }
  
  private void createTrustStore(String filename, String password, String alias, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null);
    ks.setCertificateEntry(alias, cert);
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }
  
  private Configuration createSSLConfig(String keystore, String password, String keyPassword, String trustKS,
      String trustPass, String excludeCiphers) {
    SSLFactory.Mode mode = SSLFactory.Mode.SERVER;
    String trustPassword = trustPass;
    Configuration sslConf = new Configuration(false);
    if (keystore != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), keystore);
    }
    if (password != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), password);
    }
    if (keyPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY),
          keyPassword);
    }
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY), "1000");
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY), "MILLISECONDS");
    if (trustKS != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY), trustKS);
    }
    if (trustPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY),
          trustPassword);
    }
    if(null != excludeCiphers && !excludeCiphers.isEmpty()) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
          FileBasedKeyStoresFactory.SSL_EXCLUDE_CIPHER_LIST),
          excludeCiphers);
    }
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), "1000");
    
    return sslConf;
  }
  
  private void saveConfig(File file, Configuration conf)
      throws IOException {
    Writer writer = new FileWriter(file);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
  }
}
