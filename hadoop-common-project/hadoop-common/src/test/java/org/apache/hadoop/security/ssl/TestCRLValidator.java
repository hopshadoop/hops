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
package org.apache.hadoop.security.ssl;

import io.hops.security.SuperuserKeystoresLoader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

public class TestCRLValidator {
  private final static Logger LOG = LogManager.getLogger(TestCRLValidator.class);
  private static final String BASE_DIR = Paths.get(
      System.getProperty("test.build.dir", Paths.get("target", "test-dir").toString()),
      TestCRLValidator.class.getSimpleName())
      .toString();
  
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  private Configuration conf;
  
  private final String keyAlgorithm = "RSA";
  private final String signatureAlgorithm = "SHA256withRSA";
  private final String password = "password";
  
  private static String confDir = null;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @BeforeClass
  public static void setup() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    BASE_DIR_FILE.mkdirs();
    confDir = KeyStoreTestUtil.getClasspathDir(TestCRLValidator.class);
  }
  
  @Before
  public void setupTest() throws Exception {
    conf = new Configuration();
    CRLValidatorFactory.getInstance().clearCache();
    CRLFetcherFactory.getInstance().clearFetcherCache();
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
    File sslServerFile = Paths.get(confDir, TestCRLValidator.class.getSimpleName() + ".ssl-server.xml").toFile();
    if (sslServerFile.exists()) {
      sslServerFile.delete();
    }
  }
  
  @Test
  public void testServerWithCRLValid() throws Exception {
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    Path serverKeystore = Paths.get(BASE_DIR, loader.getSuperKeystoreFilename(currentUGI.getUserName()));
    Path serverTruststore = Paths.get(BASE_DIR, loader.getSuperTruststoreFilename(currentUGI.getUserName()));
    Path serverPasswd = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(currentUGI.getUserName()));
    
    Path crlPath = Paths.get(BASE_DIR, "input.server.crl.pem");
    Path fetchedCrlPath = Paths.get(BASE_DIR, "server.crl.pem");
    
    String clientUsername = "Client_username";
    Path clientKeystore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.KEYSTORE_SUFFIX);
    Path clientTruststore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX);
    Path clientPasswordLocation = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX);
    Server server = null;
    TestRpcBase.TestRpcService proxy = null;
    
    RpcTLSUtils.TLSSetup tlsSetup = new RpcTLSUtils.TLSSetup.Builder()
        .setKeyAlgorithm(keyAlgorithm)
        .setSignatureAlgorithm(signatureAlgorithm)
        .setServerKstore(serverKeystore)
        .setServerTstore(serverTruststore)
        .setServerStorePassword(password)
        .setServerStorePasswordLocation(serverPasswd)
        .setClientKstore(clientKeystore)
        .setClientTstore(clientTruststore)
        .setClientStorePassword(password)
        .setClientPasswordLocation(clientPasswordLocation)
        .setClientUserName(clientUsername)
        .build();
    RpcTLSUtils.TestCryptoMaterial testCryptoMaterial = RpcTLSUtils.setupTLSMaterial(conf, tlsSetup,
        TestCRLValidator.class);
    
    // Generate empty CRL
    X509CRL crl = KeyStoreTestUtil.generateCRL(testCryptoMaterial.getServerCertificate(),
        testCryptoMaterial.getServerKeyPair().getPrivate(), signatureAlgorithm, null, null);
    writeCRLToFile(crl, crlPath);
    
    configureCRL(conf, crlPath, fetchedCrlPath);
    
    // Set RPC engine
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class, ProtobufRpcEngine.class);
    
    // Order here is important
    // First start the CRLFetcher service with short interval
    RevocationListFetcherService crlFetcherService = startCRLFetcherService(conf);
  
    // And then register CRLValidator with short reload interval
    CRLValidator testingValidator = new CRLValidator(conf);
    testingValidator.setReloadTimeunit(TimeUnit.SECONDS);
    testingValidator.setReloadInterval(1);
    CRLValidatorFactory.getInstance().registerValidator(CRLValidatorFactory.TYPE.NORMAL, testingValidator);
    
    // Mock environment variables
    RpcTLSUtils.MockEnvironmentVariables envs = new RpcTLSUtils.MockEnvironmentVariables();
    envs.setEnv(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, BASE_DIR);
    EnvironmentVariablesFactory.setInstance(envs);
    
    // Create Server
    RPC.Builder serverBuilder = TestRpcBase.newServerBuilder(conf)
        .setNumHandlers(1)
        .setSecretManager(null)
        .setnumReaders(2);
    
    try {
      final String message = "Hello, is it me you're looking for?";
      server = TestRpcBase.setupTestServer(serverBuilder);
      UserGroupInformation clientUGI = UserGroupInformation.createRemoteUser(clientUsername);
      TestProtos.EchoResponseProto response = RpcTLSUtils.makeEchoRequest(clientUGI, server.getListenerAddress(), conf,
          message);
      
      Assert.assertEquals(response.getMessage(), message);
      
    } finally {
      if (server != null) {
        server.stop();
      }
      if (crlFetcherService != null) {
        crlFetcherService.serviceStop();
      }
    }
  }
  
  @Test
  public void testServerWithEnabledButMissingCRL() throws Exception {
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    Path serverKeystore = Paths.get(BASE_DIR, loader.getSuperKeystoreFilename(currentUGI.getUserName()));
    Path serverTruststore = Paths.get(BASE_DIR, loader.getSuperTruststoreFilename(currentUGI.getUserName()));
    Path serverPasswd = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(currentUGI.getUserName()));
    
    Path crlPath = Paths.get(BASE_DIR, "input.server.crl.pem");
    Path fetchedCrlPath = Paths.get(BASE_DIR, "server.crl.pem");
  
    String clientUsername = "Client_username";
    Path clientKeystore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.KEYSTORE_SUFFIX);
    Path clientTruststore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX);
    Path clientPasswordLocation = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX);
    
    Server server = null;
    TestRpcBase.TestRpcService proxy = null;
  
    RpcTLSUtils.TLSSetup tlsSetup = new RpcTLSUtils.TLSSetup.Builder()
        .setKeyAlgorithm(keyAlgorithm)
        .setSignatureAlgorithm(signatureAlgorithm)
        .setServerKstore(serverKeystore)
        .setServerTstore(serverTruststore)
        .setServerStorePassword(password)
        .setServerStorePasswordLocation(serverPasswd)
        .setClientKstore(clientKeystore)
        .setClientTstore(clientTruststore)
        .setClientStorePassword(password)
        .setClientPasswordLocation(clientPasswordLocation)
        .setClientUserName(clientUsername)
        .build();
    RpcTLSUtils.TestCryptoMaterial testCryptoMaterial = RpcTLSUtils.setupTLSMaterial(conf, tlsSetup,
        TestCRLValidator.class);
  
    X509CRL crl = KeyStoreTestUtil.generateCRL(testCryptoMaterial.getServerCertificate(),
        testCryptoMaterial.getServerKeyPair().getPrivate(), signatureAlgorithm, null,
        testCryptoMaterial.getClientCertificate().getSerialNumber());
    writeCRLToFile(crl, crlPath);
  
    configureCRL(conf, crlPath, fetchedCrlPath);
  
    // Set RPC engine
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class, ProtobufRpcEngine.class);
  
    // Mock environment variables
    RpcTLSUtils.MockEnvironmentVariables envs = new RpcTLSUtils.MockEnvironmentVariables();
    envs.setEnv(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, BASE_DIR);
    EnvironmentVariablesFactory.setInstance(envs);
  
    // Create Server
    RPC.Builder serverBuilder = TestRpcBase.newServerBuilder(conf)
        .setNumHandlers(1)
        .setSecretManager(null)
        .setnumReaders(2);
  
    try {
      rule.expect(NoSuchFileException.class);
      server = TestRpcBase.setupTestServer(serverBuilder);
    } finally {
      // Server should always be null, but keep it here in case something changes
      if (server != null) {
        server.stop();
      }
    }
  }
  
  @Test
  public void testServerWithCRLInvalid() throws Exception {
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    Path serverKeystore = Paths.get(BASE_DIR, loader.getSuperKeystoreFilename(currentUGI.getUserName()));
    Path serverTruststore = Paths.get(BASE_DIR, loader.getSuperTruststoreFilename(currentUGI.getUserName()));
    Path serverPasswd = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(currentUGI.getUserName()));
    
    Path crlPath = Paths.get(BASE_DIR, "input.server.crl.pem");
    Path fetchedCrlPath = Paths.get(BASE_DIR, "server.crl.pem");
    
    String clientUsername = "Client_username";
    Path clientKeystore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.KEYSTORE_SUFFIX);
    Path clientTruststore = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX);
    Path clientPasswordLocation = Paths.get(BASE_DIR, clientUsername + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX);
    Server server = null;
  
    RpcTLSUtils.TLSSetup tlsSetup = new RpcTLSUtils.TLSSetup.Builder()
        .setKeyAlgorithm(keyAlgorithm)
        .setSignatureAlgorithm(signatureAlgorithm)
        .setServerKstore(serverKeystore)
        .setServerTstore(serverTruststore)
        .setServerStorePassword(password)
        .setServerStorePasswordLocation(serverPasswd)
        .setClientKstore(clientKeystore)
        .setClientTstore(clientTruststore)
        .setClientStorePassword(password)
        .setClientPasswordLocation(clientPasswordLocation)
        .setClientUserName(clientUsername)
        .build();
    RpcTLSUtils.TestCryptoMaterial testCryptoMaterial = RpcTLSUtils.setupTLSMaterial(conf, tlsSetup,
        TestCRLValidator.class);
    
    
    X509CRL crl = KeyStoreTestUtil.generateCRL(testCryptoMaterial.getServerCertificate(),
        testCryptoMaterial.getServerKeyPair().getPrivate(), signatureAlgorithm, null,
        testCryptoMaterial.getClientCertificate().getSerialNumber());
    writeCRLToFile(crl, crlPath);
    
    configureCRL(conf, crlPath, fetchedCrlPath);
    
    // Set RPC engine
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class, ProtobufRpcEngine.class);
  
    // Order here is important
    // First start the CRLFetcher service with short interval
    RevocationListFetcherService crlFetcherService = startCRLFetcherService(conf);
  
    // And then register CRLValidator with short reload interval
    CRLValidator testingValidator = new CRLValidator(conf);
    testingValidator.setReloadTimeunit(TimeUnit.SECONDS);
    testingValidator.setReloadInterval(1);
    testingValidator.startReloadingThread();
    CRLValidatorFactory.getInstance().registerValidator(CRLValidatorFactory.TYPE.NORMAL, testingValidator);
  
    // Mock environment variables
    RpcTLSUtils.MockEnvironmentVariables envs = new RpcTLSUtils.MockEnvironmentVariables();
    envs.setEnv(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, BASE_DIR);
    EnvironmentVariablesFactory.setInstance(envs);
    
    // Create Server
    RPC.Builder serverBuilder = TestRpcBase.newServerBuilder(conf)
        .setNumHandlers(1)
        .setSecretManager(null)
        .setnumReaders(2);
    
    try {
      final String message = "Hello, is it me you're looking for?";
      server = TestRpcBase.setupTestServer(serverBuilder);
      UserGroupInformation clientUGI = UserGroupInformation.createRemoteUser(clientUsername);
      boolean exceptionRaised = false;
      try {
        RpcTLSUtils.makeEchoRequest(clientUGI, server.getListenerAddress(), conf, message);
      } catch (Exception ex) {
        if (ex.getCause().getCause() instanceof RemoteException) {
          if (ex.getCause().getCause().getMessage().contains("HopsCRLValidator: Certificate " + testCryptoMaterial
              .getClientCertificate().getSubjectDN() + " has been revoked by " + crl.getIssuerX500Principal())) {
            // Exception here is normal
            exceptionRaised = true;
          } else {
            throw ex;
          }
        }
      }
      Assert.assertTrue(exceptionRaised);

      LOG.info("Removing client certificate from CRL and wait for the CRL fetcher to pick it up");
      
      // Remove client certificate from CRL
      crl = KeyStoreTestUtil.generateCRL(testCryptoMaterial.getServerCertificate(),
          testCryptoMaterial.getServerKeyPair().getPrivate(), signatureAlgorithm, null, null);
      writeCRLToFile(crl, crlPath);
      
      // Wait for the new CRL to be picked up
      TimeUnit.SECONDS.sleep(crlFetcherService.getFetcherInterval() * 2 + testingValidator.getReloadInterval() * 2);
  
      TestProtos.EchoResponseProto response = RpcTLSUtils.makeEchoRequest(clientUGI, server.getListenerAddress(),
          conf, message);
      
      Assert.assertEquals(response.getMessage(), message);
    } finally {
      if (server != null) {
        server.stop();
      }
      if (crlFetcherService != null) {
        crlFetcherService.serviceStop();
      }
    }
    
  }
  
  @Test
  public void testValidator() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    Path caTruststore = Paths.get(BASE_DIR, loader.getSuperTruststoreFilename(ugi.getUserName()));
    Path passwdLocation = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(ugi.getUserName()));
    FileUtils.writeStringToFile(passwdLocation.toFile(), password);
    
    Path crlPath = Paths.get(BASE_DIR, "crl.pem");
    
    // Generate CA keypair
    KeyPair cakeyPair = KeyStoreTestUtil.generateKeyPair(keyAlgorithm);
    X509Certificate caCert = KeyStoreTestUtil.generateCertificate("CN=rootCA", cakeyPair, 60, signatureAlgorithm, true);
    
    // Generate CA truststore
    KeyStoreTestUtil.createTrustStore(caTruststore.toString(), password, "rootca", caCert);
    
    // Generate client keypair
    KeyPair clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlgorithm);
    X509Certificate clientCert = KeyStoreTestUtil.generateSignedCertificate("CN=client", clientKeyPair, 30,
        signatureAlgorithm, cakeyPair.getPrivate(), caCert);

    // Verify client certificate is signed by CA
    clientCert.verify(cakeyPair.getPublic());
  
    // Generate CRL
    X509CRL crl = KeyStoreTestUtil.generateCRL(caCert, cakeyPair.getPrivate(), signatureAlgorithm, null, null);
    writeCRLToFile(crl, crlPath);
  
    // Validate should pass
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, BASE_DIR);
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, crlPath.toString());
  
    CRLValidator validator = CRLValidatorFactory.getInstance().getValidator(CRLValidatorFactory.TYPE.TESTING, conf,
        conf);
    
    Certificate[] chain = new Certificate[2];
    chain[0] = clientCert;
    chain[1] = caCert;
    
    // At this point the validation should pass
    validator.validate(chain);
    
    // Revoke client certificate and regenerate CRL
    crl = KeyStoreTestUtil.generateCRL(caCert, cakeyPair.getPrivate(), signatureAlgorithm, crl, clientCert.getSerialNumber());
    TimeUnit.SECONDS.sleep(1);
    writeCRLToFile(crl, crlPath);
    
    TimeUnit.SECONDS.sleep(validator.getReloadInterval() * 2);
    
    // This time validation should fail
    rule.expect(CertificateException.class);
    validator.validate(chain);
  }
  
  @Test
  public void testCRLValidatorFactory() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    Path truststore = Paths.get(BASE_DIR, loader.getSuperTruststoreFilename(ugi.getUserName()));
    Path passwdLocation = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(ugi.getUserName()));
    FileUtils.writeStringToFile(passwdLocation.toFile(), password);
    Path crlPath = Paths.get(BASE_DIR, "crl.pem");
    
    // Generate CA keypair
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(keyAlgorithm);
    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=root", keyPair, 60, signatureAlgorithm, true);
    // Generate CA truststore
    KeyStoreTestUtil.createTrustStore(truststore.toString(), password, "root", cert);
    X509CRL crl = KeyStoreTestUtil.generateCRL(cert, keyPair.getPrivate(), signatureAlgorithm, null, null);
    writeCRLToFile(crl, crlPath);
    
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, BASE_DIR);
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, crlPath.toString());
    
    
    CRLValidator normalValidator1 = CRLValidatorFactory.getInstance().getValidator(CRLValidatorFactory.TYPE.NORMAL,
        conf, conf);
    CRLValidator normalValidator2 = CRLValidatorFactory.getInstance().getValidator(CRLValidatorFactory.TYPE.NORMAL,
        conf, conf);
    Assert.assertEquals(normalValidator1, normalValidator2);
    
    CRLValidator testingValidator1 = CRLValidatorFactory.getInstance().getValidator(CRLValidatorFactory.TYPE.TESTING,
        conf, conf);
    CRLValidator testingValidator2 = CRLValidatorFactory.getInstance().getValidator(CRLValidatorFactory.TYPE.TESTING,
        conf, conf);
    Assert.assertEquals(testingValidator1, testingValidator2);
    Assert.assertNotEquals(normalValidator1, testingValidator1);
  }
  
  @Test
  public void testRetryActions() throws Exception {
    boolean exceptionThrown = false;
    try {
      new CRLValidator(conf);
    } catch (NoSuchFileException ex) {
      // Exception here is normal
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
  
  private void writeCRLToFile(X509CRL crl, Path crlPath) throws IOException {
    try (FileWriter fw = new FileWriter(crlPath.toFile(), false)) {
      PEMWriter pw = new PEMWriter(fw);
      pw.writeObject(crl);
      pw.flush();
      fw.flush();
      pw.close();
    }
  }
  
  private void configureCRL(Configuration conf, Path crlPath, Path fetchedCrlPath) {
    
    conf.setBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY, true);
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, "org.apache.hadoop.security.ssl.RemoteCRLFetcher");
    conf.set(CommonConfigurationKeysPublic.HOPS_CRL_FETCHER_INTERVAL_KEY, "1s");
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "file://" + crlPath.toString());
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, fetchedCrlPath.toString());
  }
  
  private RevocationListFetcherService startCRLFetcherService(Configuration conf) throws Exception {
    RevocationListFetcherService crlFetcherService = new RevocationListFetcherService();
    crlFetcherService.setIntervalTimeUnit(TimeUnit.SECONDS);
    crlFetcherService.serviceInit(conf);
    crlFetcherService.serviceStart();
    return crlFetcherService;
  }
}
