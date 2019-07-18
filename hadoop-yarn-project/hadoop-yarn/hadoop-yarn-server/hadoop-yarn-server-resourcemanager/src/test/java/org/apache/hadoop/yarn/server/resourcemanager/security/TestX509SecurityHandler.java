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
import org.apache.hadoop.util.DateUtils;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestX509SecurityHandler extends RMSecurityHandlersBaseTest {
  private static final Log LOG = LogFactory.getLog(TestX509SecurityHandler.class);
  private static final String BASE_DIR = Paths.get(System.getProperty("test.build.dir",
      Paths.get("target", "test-dir").toString()),
      TestX509SecurityHandler.class.getSimpleName()).toString();
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
    classPath = KeyStoreTestUtil.getClasspathDir(TestX509SecurityHandler.class);
  }
  
  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "5s");
    RMAppSecurityActionsFactory.getInstance().clear();
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
    //conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    dispatcher = new DrainDispatcher();
    rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, null, null, null);
    dispatcher.init(conf);
    dispatcher.start();
  
    String sslConfFileName = TestX509SecurityHandler.class.getSimpleName() + ".ssl-server.xml";
    sslServerFile = Paths.get(classPath, sslConfFileName).toFile();
    Configuration sslServer = new Configuration(false);
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
    
    RMAppSecurityActionsFactory.getInstance().clear();
  }
  
  @Test
  public void testSuccessfulCertificateCreationTesting() throws Exception {
    File testSpecificSSLServerFile = null;
    try {
      conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
          "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
  
      RMAppSecurityActions testActor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
      String trustStore = Paths.get(BASE_DIR, "trustStore.jks").toString();
      X509Certificate caCert = ((TestingRMAppSecurityActions) testActor).getCaCert();
      String principal = caCert.getIssuerX500Principal().getName();
      // Principal should be CN=RootCA
      String alias = principal.split("=")[1];
      String password = "password";
  
      String sslServer = TestX509SecurityHandler.class.getSimpleName() + "-testSuccessfulCertificateCreationTesting.ssl-server.xml";
      testSpecificSSLServerFile = Paths.get(classPath, sslServer).toFile();
  
      conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServer);
  
      createTrustStore(trustStore, password, alias, caCert);
      Configuration sslServerConf = createSSLConfig("", "", "", trustStore, password, "");
      saveConfig(testSpecificSSLServerFile.getAbsoluteFile(), sslServerConf);
  
      MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.SECURITY_MATERIAL_GENERATED);
      rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
      
      RMAppSecurityManager rmAppSecurityManager = new RMAppSecurityManager(rmContext);
      X509SecurityHandler x509SecurityHandler = new MockX509SecurityHandler(rmContext, rmAppSecurityManager, true);
      rmAppSecurityManager.registerRMAppSecurityHandler(x509SecurityHandler);
      rmAppSecurityManager.init(conf);
      rmAppSecurityManager.start();
      ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
      X509SecurityHandler.X509MaterialParameter x509Param =
          new X509SecurityHandler.X509MaterialParameter(
              appId,"Dorothy", 1);
      RMAppSecurityMaterial securityMaterial = new RMAppSecurityMaterial();
      securityMaterial.addMaterial(x509Param);
      RMAppSecurityManagerEvent genSecurityMaterialEvent = new RMAppSecurityManagerEvent(appId,
          securityMaterial, RMAppSecurityManagerEventType.GENERATE_SECURITY_MATERIAL);
      
      rmAppSecurityManager.handle(genSecurityMaterialEvent);
  
      dispatcher.await();
      eventHandler.verifyEvent();
      rmAppSecurityManager.stop();
    } finally {
      if (testSpecificSSLServerFile != null) {
        testSpecificSSLServerFile.delete();
      }
    }
  }
  
  @Test
  public void testCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    RMAppSecurityManager rmAppSecurityManager = new RMAppSecurityManager(rmContext);
    MockX509SecurityHandler x509SecurityHandler = new MockX509SecurityHandler(rmContext, rmAppSecurityManager, false);
    rmAppSecurityManager.registerRMAppSecurityHandler(x509SecurityHandler);
    rmAppSecurityManager.init(conf);
    rmAppSecurityManager.start();
    
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(DateUtils.localDateTime2UnixEpoch(now), 1);
    x509SecurityHandler.setOldCertificateExpiration(DateUtils.localDateTime2UnixEpoch(expiration));
  
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(appId, "Dolores", 1);
    x509Param.setExpiration(DateUtils.localDateTime2UnixEpoch(expiration));
    x509SecurityHandler.registerRenewer(x509Param);
    Map<ApplicationId, ScheduledFuture> tasks = x509SecurityHandler.getRenewalTasks();
    ScheduledFuture renewalTask = tasks.get(appId);
    assertFalse(renewalTask.isCancelled());
    assertFalse(renewalTask.isDone());
    
    // Wait until the scheduled task is executed
    TimeUnit.SECONDS.sleep(10);
    assertTrue(renewalTask.isDone());
    assertFalse(x509SecurityHandler.getRenewalException());
    assertTrue(tasks.isEmpty());
    rmAppSecurityManager.stop();
  }
  
  @Test(timeout = 12000)
  public void testFailedCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    RMAppSecurityManager securityManager = new RMAppSecurityManager(rmContext);
    MockX509SecurityHandler.MockFailingX509SecurityHandler x509Handler =
        new MockX509SecurityHandler.MockFailingX509SecurityHandler(rmContext, securityManager, Integer.MAX_VALUE);
    securityManager.registerRMAppSecurityHandlerWithType(x509Handler, X509SecurityHandler.class);
    securityManager.init(conf);
    securityManager.start();
  
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(DateUtils.localDateTime2UnixEpoch(now), 1);
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(appId, "Dolores", 1);
    x509Param.setExpiration(DateUtils.localDateTime2UnixEpoch(expiration));
    x509Handler.registerRenewer(x509Param);
    
    
    Map<ApplicationId, ScheduledFuture> tasks = x509Handler.getRenewalTasks();
    // There should be a scheduled task
    ScheduledFuture task = tasks.get(appId);
    assertFalse(task.isCancelled());
    assertFalse(task.isDone());
    assertFalse(x509Handler.hasRenewalFailed());
    assertEquals(0, x509Handler.getNumberOfRenewalFailures());
    
    TimeUnit.SECONDS.sleep(10);
    assertTrue(tasks.isEmpty());
    assertEquals(4, x509Handler.getNumberOfRenewalFailures());
    assertTrue(x509Handler.hasRenewalFailed());
    securityManager.stop();
  }
  
  @Test(timeout = 12000)
  public void testRetryCertificateRenewal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    RMAppSecurityManager securityManager = new RMAppSecurityManager(rmContext);
    MockX509SecurityHandler.MockFailingX509SecurityHandler x509Handler =
        new MockX509SecurityHandler.MockFailingX509SecurityHandler(rmContext, securityManager, 2);
    securityManager.registerRMAppSecurityHandlerWithType(x509Handler, X509SecurityHandler.class);
    securityManager.init(conf);
    securityManager.start();
    
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiration = now.plus(10, ChronoUnit.SECONDS);
    ApplicationId appId = ApplicationId.newInstance(DateUtils.localDateTime2UnixEpoch(now), 1);
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(appId, "Dolores", 1);
    x509Param.setExpiration(DateUtils.localDateTime2UnixEpoch(expiration));
    x509Handler.registerRenewer(x509Param);
    TimeUnit.SECONDS.sleep(10);
    assertEquals(2, x509Handler.getNumberOfRenewalFailures());
    assertFalse(x509Handler.hasRenewalFailed());
    assertTrue(x509Handler.getRenewalTasks().isEmpty());
    securityManager.stop();
  }
  
  @Test
  public void testFailingCertificateCreationLocal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    
    MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.KILL);
    rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
    RMAppSecurityManager securityManager = new RMAppSecurityManager(rmContext);
    MockX509SecurityHandler.MockFailingX509SecurityHandler x509Handler =
        new MockX509SecurityHandler.MockFailingX509SecurityHandler(rmContext, securityManager, Integer.MAX_VALUE);
    securityManager.registerRMAppSecurityHandlerWithType(x509Handler, X509SecurityHandler.class);
    securityManager.init(conf);
    securityManager.start();
    
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(appId, "Dolores", 1);
    RMAppSecurityMaterial securityMaterial = new RMAppSecurityMaterial();
    securityMaterial.addMaterial(x509Param);
    securityManager.handle(new RMAppSecurityManagerEvent(appId, securityMaterial,
        RMAppSecurityManagerEventType.GENERATE_SECURITY_MATERIAL));
    
    dispatcher.await();
    eventHandler.verifyEvent();
    securityManager.stop();
  }
  
  @Test(timeout = 20000)
  public void testCertificateRevocationMonitor() throws Exception {
    RMAppSecurityActions actor = Mockito.spy(new TestingRMAppSecurityActions());
    actor.init();
    RMAppSecurityActionsFactory.getInstance().register(actor);
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
    
    LOG.info(">> Rotation has happened");
    assertTrue(application.isAppRotatingCryptoMaterial());
    assertNotEquals(-1L, application.getMaterialRotationStartTime());
    
    // No NM heartbeat. NM will not inform about the updated crypto material
    // Wait for the monitor to kick in
    TimeUnit.SECONDS.sleep(6);
    assertFalse(application.isAppRotatingCryptoMaterial());
    assertEquals(-1L, application.getMaterialRotationStartTime());
    String certId = X509SecurityHandler.getCertificateIdentifier(application.getApplicationId(),
        application.getUser(), application.getCryptoMaterialVersion() - 1);
    Mockito.verify(actor).revoke(Mockito.eq(certId));
    
    // Since NM didn't respond acknowledging the crypto update, revokeSecurityMaterial on RMAppSecurityManager
    // should not have been called. The revocation has been handled by the the revocation monitor
    Mockito.verify(rm.getRMContext().getRMAppSecurityManager(), Mockito.never())
        .revokeSecurityMaterial(Mockito.any(RMAppSecurityManagerEvent.class));
    rm.stop();
  }
  
  @Test
  public void testApplicationSubmission() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, DBRMStateStore.class.getName());
    // Validity period is 50 seconds
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "45s");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    
    MockRM rm  = new MyMockRM(conf);
    rm.start();
  
    MockNM nm = new MockNM("127.0.0.1:8032", 15 * 1024, rm.getResourceTrackerService());
    nm.registerNode();
    
    RMApp application = rm.submitApp(1024, "application1", "Phil",
        new HashMap<ApplicationAccessType, String>(), false, "default", 2, null,
        "MAPREDUCE", true, false);
    
    NodeHeartbeatResponse response = nm.nodeHeartbeat(true);
    int responseId = response.getResponseId();
    
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
    
    // Expiration time for testing TestingRMAppSecurityActions is now + 50 seconds
    TimeUnit.SECONDS.sleep(6);
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
  
    
    nm.nodeHeartbeat(Collections.<ContainerStatus>emptyList(), Collections.<Container>emptyList(), true,
        responseId, updatedAppCrypto);
    
    TimeUnit.MILLISECONDS.sleep(100);
    
    RMAppImpl appImpl = (RMAppImpl) application;
    assertNull(appImpl.getRMNodesUpdatedCryptoMaterial());
    assertFalse(application.isAppRotatingCryptoMaterial());
    
    appState = rm.getRMContext().getStateStore().loadState().getApplicationState().get(application.getApplicationId());
    // By now material rotation should have stopped
    assertFalse(appState.isDuringMaterialRotation());
    assertEquals(-1L, appState.getMaterialRotationStartTime());
    Thread.sleep(1000);
    // Application should still be registered with the certificate renewer
    X509SecurityHandler x509SecurityHandler = (X509SecurityHandler) rm.getRMContext().getRMAppSecurityManager()
        .getSecurityHandler(X509SecurityHandler.class);
    assertTrue(x509SecurityHandler.getRenewalTasks().containsKey(application.getApplicationId()));
    
    TimeUnit.MILLISECONDS.sleep(100);
  
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(application.getApplicationId(), application.getUser(),
            application.getCryptoMaterialVersion() - 1, true);
    
    Mockito.verify(x509SecurityHandler).revokeMaterial(Mockito.eq(x509Param), Mockito.eq(false));
    
    rm.stop();
    
    conf.set(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD, "1s");
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
    x509SecurityHandler = (X509SecurityHandler) rm2.getRMContext().getRMAppSecurityManager()
        .getSecurityHandler(X509SecurityHandler.class);
    assertTrue(x509SecurityHandler.getRenewalTasks().containsKey(application.getApplicationId()));
    
    rm2.killApp(application.getApplicationId());
    rm2.waitForState(application.getApplicationId(), RMAppState.KILLED);
    rm2.stop();
  }
  
  @Test
  public void testContainerAllocationDuringMaterialRotation() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
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
    while (rmNode2.getAppX509ToUpdate().isEmpty() && wait < 10) {
      TimeUnit.MILLISECONDS.sleep(300);
      wait++;
    }
    
    assertFalse(rmNode2.getAppX509ToUpdate().isEmpty());
    
    nmResponse = nm2.nodeHeartbeat(true);
    assertTrue(nmResponse.getUpdatedCryptoForApps().containsKey(app.getApplicationId()));
    
    rm.stop();
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
      protected RMAppImpl createRMApp(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
      ApplicationMasterService masterService, long submitTime,
      String applicationType, Set<String> applicationTags,
      List<ResourceRequest> amReqs, ApplicationPlacementContext
      placementContext, long startTime) {
        return new MyRMApp(applicationId, rmContext, config, name, user, queue, submissionContext, scheduler,
            masterService, submitTime, applicationType, applicationTags, amReqs, placementContext, startTime);
      }
    }
    
    private class MyRMApp extends RMAppImpl {
  
      public MyRMApp(ApplicationId applicationId, RMContext rmContext,
          Configuration config, String name, String user, String queue,
          ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
          ApplicationMasterService masterService, long submitTime,
          String applicationType, Set<String> applicationTags,
          List<ResourceRequest> amReqs, ApplicationPlacementContext placementContext, long startTime) {
        super(applicationId, rmContext, config, name, user, queue, submissionContext, scheduler, masterService,
            submitTime, applicationType, applicationTags, amReqs, placementContext, startTime);
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
    protected RMAppSecurityManager createRMAppSecurityManager() throws Exception {
      RMAppSecurityManager rmAppSecurityManager = Mockito.spy(new RMAppSecurityManager(rmContext) {
        @Override
        protected void clearRMAppSecurityActionsFactory() {
          // Do nothing in this case
        }
      });
      rmAppSecurityManager.registerRMAppSecurityHandlerWithType(createX509SecurityHandler(rmAppSecurityManager),
          X509SecurityHandler.class);
      rmAppSecurityManager.registerRMAppSecurityHandler(createJWTSecurityHandler(rmAppSecurityManager));
      return rmAppSecurityManager;
    }
  
    @Override
    protected RMAppSecurityHandler createX509SecurityHandler(RMAppSecurityManager rmAppSecurityManager) {
      RMAppSecurityHandler<X509SecurityHandler.X509SecurityManagerMaterial, X509SecurityHandler.X509MaterialParameter>
          x509SecurityHandler = Mockito.spy(new MockX509SecurityHandler(rmContext, rmAppSecurityManager, false));
      return x509SecurityHandler;
    }
  
    @Override
    protected RMAppSecurityHandler createJWTSecurityHandler(RMAppSecurityManager rmAppSecurityManager) {
      RMAppSecurityHandler<JWTSecurityHandler.JWTSecurityManagerMaterial, JWTSecurityHandler.JWTMaterialParameter>
          jwtSecurityHandler = new JWTSecurityHandler(rmContext, rmAppSecurityManager);
      return jwtSecurityHandler;
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
