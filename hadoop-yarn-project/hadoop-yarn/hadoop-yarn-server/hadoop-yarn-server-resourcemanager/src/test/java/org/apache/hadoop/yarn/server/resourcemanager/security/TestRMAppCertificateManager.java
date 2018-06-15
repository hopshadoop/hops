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
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppCertificateGeneratedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
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
    conf.set(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY, "https://bbc4.sics.se:38591");
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
          "userA",
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
        "userA",
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
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    manager.handle(new RMAppCertificateManagerEvent(
        appId, username, RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
  
    dispatcher.await();
    Mockito.verify(mockRemoteActions, Mockito.atMost(1)).sign(Mockito.any(PKCS10CertificationRequest.class));
    
    manager.handle(new RMAppCertificateManagerEvent(
        appId, username, RMAppCertificateManagerEventType.REVOKE_CERTIFICATE));
    
    dispatcher.await();
    Mockito.verify(manager, Mockito.atMost(1))
        .revokeCertificate(appId, username);
  
    TimeUnit.SECONDS.sleep(3);
    
    String certificateIdentifier = username + "__" + appId.toString();
    Mockito.verify(mockRemoteActions, Mockito.atMost(1))
        .revoke(Mockito.eq(certificateIdentifier));
    
    manager.stop();
  }
  
  @Test
  public void testFailingCertificateCreationLocal() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    
    MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.KILL);
    rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
    
    MockFailingRMAppCertificateManager manager = new MockFailingRMAppCertificateManager();
    manager.init(conf);
    manager.start();
    manager.handle(new RMAppCertificateManagerEvent(
        ApplicationId.newInstance(System.currentTimeMillis(), 1),
        "userA",
        RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
    dispatcher.await();
    eventHandler.verifyEvent();
    manager.stop();
  }
  
  @Test
  public void testApplicationSubmission() throws Exception {
    conf.set(YarnConfiguration.HOPS_RM_CERTIFICATE_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppCertificateActions");
    
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
    assertNotNull(trustStorePassword);
    assertNotEquals(0, trustStorePassword.length);
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
  
  private class MyMockRM extends MockRM {
  
    public MyMockRM(Configuration conf) {
      super(conf);
    }
  
    @Override
    protected RMAppCertificateManager createRMAppCertificateManager() throws Exception {
      return new MockRMAppCertificateManager(false, rmContext);
    }
  }
  
  private class MockRMAppCertificateManager extends RMAppCertificateManager {
    private final boolean loadTrustStore;
    private final String systemTMP;
  
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
    public void generateCertificate(ApplicationId applicationId, String appUser) {
      boolean exceptionThrown = false;
      ByteArrayInputStream bio = null;
      try {
        KeyPair keyPair = generateKeyPair();
        // Generate CSR
        PKCS10CertificationRequest csr = generateCSR(applicationId, appUser, keyPair);
        
        assertEquals(appUser, HopsUtil.extractCNFromSubject(csr.getSubject().toString()));
        assertEquals(applicationId.toString(), HopsUtil.extractOFromSubject(csr.getSubject().toString()));
        
        // Sign CSR
        X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
        signedCertificate.checkValidity();
        
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
  
        RMAppCertificateGeneratedEvent startEvent = new RMAppCertificateGeneratedEvent(applicationId,
            rawKeystore, keyStorePassword, rawTrustStore, trustStorePassword);
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
    public void revokeCertificate(ApplicationId appId, String applicationUser) {
      try {
        putToQueue(appId, applicationUser);
        waitForQueueToDrain();
      } catch (InterruptedException ex) {
        LOG.error(ex, ex);
        fail("Exception should not be thrown here");
      }
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
  }
  
  private class MockFailingRMAppCertificateManager extends RMAppCertificateManager {
  
    public MockFailingRMAppCertificateManager() {
      super(rmContext);
    }
  
    @Override
    public void generateCertificate(ApplicationId appId, String appUser) {
      getRmContext().getDispatcher().getEventHandler().handle(new RMAppEvent(appId, RMAppEventType.KILL));
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
  
  private void createTrustStore(String filename,
      String password, String alias,
      Certificate cert)
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
