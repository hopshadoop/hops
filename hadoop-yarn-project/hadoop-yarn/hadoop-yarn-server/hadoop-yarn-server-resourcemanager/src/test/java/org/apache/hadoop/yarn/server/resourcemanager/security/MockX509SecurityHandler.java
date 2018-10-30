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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialGeneratedEvent;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MockX509SecurityHandler extends X509SecurityHandler {
  private static final Logger LOG = LogManager.getLogger(MockX509SecurityHandler.class);
  private final boolean loadTrustStore;
  private final String systemTMP;
  private boolean renewalException = false;
  private long oldCertificateExpiration;
  
  public MockX509SecurityHandler(RMContext rmContext,
      RMAppSecurityManager rmAppSecurityManager, boolean loadTrustStore) {
    super(rmContext, rmAppSecurityManager);
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
  public X509SecurityManagerMaterial generateMaterial(
      X509MaterialParameter materialParameter) throws Exception {
    ApplicationId appId = materialParameter.getApplicationId();
    String appUser = materialParameter.getAppUser();
    Integer cryptoMaterialVersion = materialParameter.getCryptoMaterialVersion();
  
    KeyPair keyPair = generateKeyPair();
    PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, cryptoMaterialVersion);
    assertEquals(appUser, HopsUtil.extractCNFromSubject(csr.getSubject().toString()));
    assertEquals(appId.toString(), HopsUtil.extractOFromSubject(csr.getSubject().toString()));
    assertEquals(String.valueOf(cryptoMaterialVersion), HopsUtil.extractOUFromSubject(csr.getSubject().toString()));
  
    // Sign CSR
    CertificateBundle certificateBundle = sendCSRAndGetSigned(csr);
    certificateBundle.getCertificate().checkValidity();
    long expiration = certificateBundle.getCertificate().getNotAfter().getTime();
    long epochNow = Instant.now().toEpochMilli();
    assertTrue(expiration >= epochNow);
    assertNotNull(certificateBundle.getIssuer());
    RMAppSecurityActions actor = getRmAppSecurityActions();
    if (actor instanceof TestingRMAppSecurityActions) {
      X509Certificate caCert = ((TestingRMAppSecurityActions) actor).getCaCert();
      certificateBundle.getCertificate().verify(caCert.getPublicKey(), "BC");
    }
    certificateBundle.getCertificate().verify(certificateBundle.getIssuer().getPublicKey(), "BC");
  
    KeyStoresWrapper appKeystores = createApplicationStores(certificateBundle, keyPair.getPrivate(), appUser, appId);
    X509Certificate extractedCert = (X509Certificate) appKeystores.getKeystore().getCertificate(appUser);
    byte[] rawKeystore = appKeystores.getRawKeyStore(TYPE.KEYSTORE);
    assertNotNull(rawKeystore);
    assertNotEquals(0, rawKeystore.length);
  
    File keystoreFile = Paths.get(systemTMP, appUser + "-" + appId.toString() + "_kstore.jks").toFile();
    // Keystore should have been deleted
    assertFalse(keystoreFile.exists());
    char[] keyStorePassword = appKeystores.getKeyStorePassword();
    assertNotNull(keyStorePassword);
    assertNotEquals(0, keyStorePassword.length);
  
  
    byte[] rawTrustStore = appKeystores.getRawKeyStore(TYPE.TRUSTSTORE);
    File trustStoreFile = Paths.get(systemTMP, appUser + "-" + appId.toString() + "_tstore.jks").toFile();
    // Truststore should have been deleted
    assertFalse(trustStoreFile.exists());
    char[] trustStorePassword = appKeystores.getTrustStorePassword();
    assertNotNull(trustStorePassword);
    assertNotEquals(0, trustStorePassword.length);
  
    verifyContentOfAppTrustStore(rawTrustStore, trustStorePassword, appUser, appId);
  
    if (actor instanceof TestingRMAppSecurityActions) {
      X509Certificate caCert = ((TestingRMAppSecurityActions) actor).getCaCert();
      extractedCert.verify(caCert.getPublicKey(), "BC");
    }
    assertEquals(appUser, HopsUtil.extractCNFromSubject(extractedCert.getSubjectX500Principal().getName()));
    assertEquals(appId.toString(), HopsUtil.extractOFromSubject(
        extractedCert.getSubjectX500Principal().getName()));
    assertEquals(String.valueOf(cryptoMaterialVersion),
        HopsUtil.extractOUFromSubject(extractedCert.getSubjectX500Principal().getName()));
    return new X509SecurityManagerMaterial(appId, rawKeystore, appKeystores.getKeyStorePassword(),
        rawTrustStore, appKeystores.getTrustStorePassword(), expiration);
  }
  
  @Override
  public boolean isHopsTLSEnabled() {
    return true;
  }
  
  @Override
  public boolean revokeMaterial(X509MaterialParameter materialParameter, Boolean blocking) {
    ApplicationId appId = materialParameter.getApplicationId();
    String appUser = materialParameter.getAppUser();
    Integer cryptoMaterialVersion = materialParameter.getCryptoMaterialVersion();
    
    try {
      if (!materialParameter.isFromRenewal()) {
        deregisterFromCertificateRenewer(appId);
      }
      putToQueue(appId, appUser, cryptoMaterialVersion);
      waitForQueueToDrain();
      return true;
    } catch (InterruptedException ex) {
      LOG.error(ex, ex);
      fail("Exception should not be thrown here");
      return false;
    }
  }
  
  @Override
  protected Runnable createCertificateRenewerTask(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
    return new MockX509Renewer(appId, appUser, currentCryptoVersion, 1);
  }
  
  public void setOldCertificateExpiration(long oldCertificateExpiration) {
    this.oldCertificateExpiration = oldCertificateExpiration;
  }
  
  public boolean getRenewalException() {
    return renewalException;
  }
  
  private void verifyContentOfAppTrustStore(byte[] appTrustStore, char[] password, String appUser,
      ApplicationId appId)
      throws GeneralSecurityException, IOException {
    File trustStoreFile = Paths.get(systemTMP, appUser + "-" + appId.toString() + "_tstore.jks").toFile();
    boolean certificateMissing = false;
    
    try {
      KeyStore systemTrustStore = loadSystemTrustStore(getConfig());
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
  
  public class MockX509Renewer extends X509Renewer {
    private final long oldCertificateExpiration;
  
    public MockX509Renewer(ApplicationId appId, String appUser, Integer currentCryptoVersion,
        long oldCertificateExpiration) {
      super(appId, appUser, currentCryptoVersion);
      this.oldCertificateExpiration = oldCertificateExpiration;
    }
    
    @Override
    public void run() {
      LOG.info("Renewing certificate for application: " + appId);
      try {
        KeyPair keyPair = generateKeyPair();
        int oldCryptoVersion = currentCryptoVersion;
        PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, ++currentCryptoVersion);
        int newCryptoVersion = Integer.parseInt(HopsUtil.extractOUFromSubject(csr.getSubject().toString()));
        if (++oldCryptoVersion != newCryptoVersion) {
          LOG.error("Crypto version of new certificate is wrong: " + newCryptoVersion);
          renewalException = true;
        }
        CertificateBundle certificateBundle = sendCSRAndGetSigned(csr);
        long newCertificateExpiration = certificateBundle.getCertificate().getNotAfter().getTime();
        if (newCertificateExpiration <= oldCertificateExpiration) {
          LOG.error("New certificate expiration is older than old certificate");
          renewalException = true;
        }
        
        KeyStoresWrapper appKeystores = createApplicationStores(certificateBundle, keyPair.getPrivate(), appUser,
            appId);
        byte[] rawKeystore = appKeystores.getRawKeyStore(TYPE.KEYSTORE);
        byte[] rawTrustStore = appKeystores.getRawKeyStore(TYPE.TRUSTSTORE);
        getRenewalTasks().remove(appId);
        
        X509SecurityManagerMaterial x509Material = new X509SecurityManagerMaterial(
            appId, rawKeystore, appKeystores.getKeyStorePassword(),
            rawTrustStore, appKeystores.getTrustStorePassword(), newCertificateExpiration);
        RMAppSecurityMaterial<X509SecurityManagerMaterial> securityMaterial = new RMAppSecurityMaterial<>();
        securityMaterial.addMaterial(x509Material);
        getRmContext().getDispatcher().getEventHandler()
            .handle(new RMAppSecurityMaterialGeneratedEvent(appId, securityMaterial, RMAppEventType.CERTS_RENEWED));
        LOG.debug("Renewed certificate for application " + appId);
      } catch (Exception ex) {
        LOG.error("Exception while renewing certificate. THis should not have happened here :(", ex);
        renewalException = true;
      }
    }
  }
  
  public static class MockFailingX509SecurityHandler extends X509SecurityHandler {
    private final Integer succeedAfterRetries;
    private int numberOfRenewalFailures = 0;
    private boolean renewalFailed = false;
    
    public MockFailingX509SecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager,
        Integer succeedAfterRetries) {
      super(rmContext, rmAppSecurityManager);
      this.succeedAfterRetries = succeedAfterRetries;
    }
    
    public int getNumberOfRenewalFailures() {
      return numberOfRenewalFailures;
    }
    
    public boolean hasRenewalFailed() {
      return renewalFailed;
    }
    
    @Override
    public boolean isHopsTLSEnabled() {
      return true;
    }
  
    @Override
    protected Runnable createCertificateRenewerTask(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
      return new MockFailingX509Renewer(appId, appUser, currentCryptoVersion, succeedAfterRetries);
    }
    
    @Override
    public X509SecurityManagerMaterial generateMaterial(
        X509MaterialParameter materialParameter) throws Exception {
      throw new IOException("Exception is intended here");
    }
    
    public class MockFailingX509Renewer extends X509Renewer {
      private final Integer succeedAfterRetries;
      
      public MockFailingX509Renewer(ApplicationId appId, String appUser, Integer currentCryptoVersion,
          Integer succeedAfterRetries) {
        super(appId, appUser, currentCryptoVersion);
        this.succeedAfterRetries = succeedAfterRetries;
      }
      
      @Override
      public void run() {
        try {
          if (((ExponentialBackOff) backOff).getNumberOfRetries() < succeedAfterRetries) {
            throw new Exception("Ooops something went wrong");
          }
          getRenewalTasks().remove(appId);
          LOG.info("Renewed certificate for applicaiton " + appId);
        } catch (Exception ex) {
          getRenewalTasks().remove(appId);
          backOffTime = backOff.getBackOffInMillis();
          if (backOffTime != -1) {
            numberOfRenewalFailures++;
            LOG.warn("Failed to renew certificates for application " + appId + ". Retrying in " + backOffTime);;
            ScheduledFuture task = getRenewerScheduler().schedule(this, backOffTime, TimeUnit.MILLISECONDS);
            getRenewalTasks().put(appId, task);
          } else {
            LOG.error("Failed to renew certificate for application " + appId + " Failed more than 4 times, giving up");
            renewalFailed = true;
          }
        }
      }
    }
  }
}
