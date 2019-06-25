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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialRenewedEvent;
import org.apache.hadoop.yarn.server.security.CertificateLocalizationService;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class X509SecurityHandler
    implements RMAppSecurityHandler<X509SecurityHandler.X509SecurityManagerMaterial, X509SecurityHandler.X509MaterialParameter> {
  private final static Log LOG = LogFactory.getLog(X509SecurityHandler.class);
  private final static String SECURITY_PROVIDER = "BC";
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static int KEY_SIZE = 1024;
  private final static int REVOCATION_QUEUE_SIZE = 100;
  
  private final String TMP = System.getProperty("java.io.tmpdir");
  private final RMContext rmContext;
  private final RMAppSecurityManager rmAppSecurityManager;
  private final SecureRandom rng;
  private final EventHandler eventHandler;
  
  private CertificateLocalizationService certificateLocalizationService;
  private RMAppSecurityActions rmAppSecurityActions;
  private KeyPairGenerator keyPairGenerator;
  private boolean hopsTLSEnabled = false;
  private Configuration config;
  
  // For certificate renewal
  private Long amountOfTimeToSubstractFromExpiration = 2L;
  private TemporalUnit renewalUnitOfTime = ChronoUnit.DAYS;
  private final Map<ApplicationId, ScheduledFuture> renewalTasks;
  private ScheduledExecutorService renewalExecutorService;
  
  // For certificate revocation monitor
  private Long revocationMonitorInterval = 10L;
  private TemporalUnit revocationUnitOfInterval = ChronoUnit.HOURS;
  private final BlockingQueue<CertificateRevocationEvent> revocationEvents;
  private Thread revocationEventsHandler;
  private Thread revocationMonitor;
  
  public X509SecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager) {
    this.rmContext = rmContext;
    this.rmAppSecurityManager = rmAppSecurityManager;
    rng = new SecureRandom();
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    revocationEvents = new ArrayBlockingQueue<CertificateRevocationEvent>(REVOCATION_QUEUE_SIZE);
    renewalTasks = new ConcurrentHashMap<>();
  }
  
  @VisibleForTesting
  protected RMContext getRmContext() {
    return rmContext;
  }
  
  @VisibleForTesting
  protected ScheduledExecutorService getRenewerScheduler() {
    return renewalExecutorService;
  }
  
  @Override
  public void init(Configuration config) throws Exception {
    LOG.info("Initializing X.509 Security Handler");
    this.config = config;
    hopsTLSEnabled = config.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
    renewalExecutorService = rmAppSecurityManager.getRenewalExecutorService();
    
    String delayConfiguration = config.get(YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD,
        YarnConfiguration.DEFAULT_RM_APP_CERTIFICATE_RENEWER_DELAY);
    Pair<Long, TemporalUnit> delayIntervalUnit = rmAppSecurityManager.parseInterval(delayConfiguration,
        YarnConfiguration.RM_APP_CERTIFICATE_EXPIRATION_SAFETY_PERIOD);
    amountOfTimeToSubstractFromExpiration = delayIntervalUnit.getFirst();
    renewalUnitOfTime = delayIntervalUnit.getSecond();
    
    String confMonitorInterval = config.get(YarnConfiguration.RM_APP_CERTIFICATE_REVOCATION_MONITOR_INTERVAL,
        YarnConfiguration.DEFAULT_RM_APP_CERTIFICATE_REVOCATION_MONITOR_INTERVAL);
    Pair<Long, TemporalUnit> monitorIntervalUnit = rmAppSecurityManager.parseInterval(confMonitorInterval,
        YarnConfiguration.RM_APP_CERTIFICATE_REVOCATION_MONITOR_INTERVAL);
    revocationMonitorInterval = monitorIntervalUnit.getFirst();
    revocationUnitOfInterval = monitorIntervalUnit.getSecond();
  
    if (isHopsTLSEnabled()) {
      this.certificateLocalizationService = rmContext.getCertificateLocalizationService();
      rmAppSecurityActions = rmAppSecurityManager.getRmAppCertificateActions();
      keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, SECURITY_PROVIDER);
      keyPairGenerator.initialize(KEY_SIZE);
    }
  }
  
  @Override
  public void start() throws Exception {
    LOG.info("Starting X.509 Security Handler");
    if (isHopsTLSEnabled()) {
      revocationEventsHandler = new RevocationEventsHandler();
      revocationEventsHandler.setDaemon(false);
      revocationEventsHandler.setName("X509-RevocationEventsHandler");
      revocationEventsHandler.start();
    
      revocationMonitor = new CertificateRevocationMonitor();
      revocationMonitor.setDaemon(true);
      revocationMonitor.setName("X.509-RevocationMonitor");
      revocationMonitor.start();
    }
  }
  
  @Override
  public void stop() throws Exception {
    LOG.info("Stopping X.509 Security Handler");
    if (revocationMonitor != null) {
      revocationMonitor.interrupt();
    }
    if (revocationEventsHandler != null) {
      revocationEventsHandler.interrupt();
    }
  }
  
  @VisibleForTesting
  protected RMAppSecurityActions getRmAppSecurityActions() {
    return rmAppSecurityActions;
  }
  
  @VisibleForTesting
  protected Configuration getConfig() {
    return config;
  }
  
  @Override
  public X509SecurityManagerMaterial generateMaterial(X509MaterialParameter materialParameter) throws Exception {
    if (!isHopsTLSEnabled()) {
      return null;
    }
    ApplicationId appId = materialParameter.getApplicationId();
    String appUser = materialParameter.appUser;
    Integer cryptoMaterialVersion = materialParameter.cryptoMaterialVersion;
    KeyPair keyPair = generateKeyPair();
    PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, cryptoMaterialVersion);
    CertificateBundle certificateBundle = sendCSRAndGetSigned(csr);
    long expirationEpoch = certificateBundle.certificate.getNotAfter().getTime();
    
    KeyStoresWrapper keyStoresWrapper = createApplicationStores(certificateBundle, keyPair.getPrivate(),
        appUser, appId);
    byte[] rawProtectedKeyStore = keyStoresWrapper.getRawKeyStore(TYPE.KEYSTORE);
    byte[] rawTrustStore = keyStoresWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
    
    certificateLocalizationService.materializeCertificates(appUser, appId.toString(), appUser,
        ByteBuffer.wrap(rawProtectedKeyStore), String.valueOf(keyStoresWrapper.keyStorePassword),
        ByteBuffer.wrap(rawTrustStore), String.valueOf(keyStoresWrapper.trustStorePassword));
    return new X509SecurityManagerMaterial(appId, rawProtectedKeyStore, keyStoresWrapper.keyStorePassword,
        rawTrustStore, keyStoresWrapper.trustStorePassword, expirationEpoch);
  }
  
  @Override
  public void registerRenewer(X509MaterialParameter parameter) {
    if (!isHopsTLSEnabled()) {
      return;
    }
    if (!renewalTasks.containsKey(parameter.getApplicationId())) {
      LocalDateTime now = DateUtils.getNow();
      LocalDateTime expirationDate = DateUtils.unixEpoch2LocalDateTime(parameter.getExpiration());
  
      Duration validityPeriod = Duration.between(now, expirationDate);
      Duration delay = validityPeriod.minus(amountOfTimeToSubstractFromExpiration, renewalUnitOfTime);
      
      ScheduledFuture renewTask = renewalExecutorService.schedule(
          createCertificateRenewerTask(parameter.getApplicationId(), parameter.appUser, parameter.cryptoMaterialVersion),
          delay.getSeconds(), TimeUnit.SECONDS);
      renewalTasks.put(parameter.getApplicationId(), renewTask);
    }
  }
  
  public void deregisterFromCertificateRenewer(ApplicationId appId) {
    if (!isHopsTLSEnabled()) {
      return;
    }
    ScheduledFuture task = renewalTasks.remove(appId);
    if (task != null) {
      task.cancel(true);
    }
  }
  
  @VisibleForTesting
  protected Runnable createCertificateRenewerTask(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
    return new X509Renewer(appId, appUser, currentCryptoVersion);
  }
  
  @Override
  public boolean revokeMaterial(X509MaterialParameter materialParameter, Boolean blocking) {
    if (!isHopsTLSEnabled()) {
      return true;
    }
    ApplicationId appId = materialParameter.getApplicationId();
    String appUser = materialParameter.appUser;
    Integer cryptoMaterialVersion = materialParameter.cryptoMaterialVersion;
    LOG.info("Revoking certificate for application: " + appId + " with version " + cryptoMaterialVersion);
    try {
      if (!materialParameter.isFromRenewal) {
        // Deregister application from certificate renewal, if it exists
        deregisterFromCertificateRenewer(appId);
        if (certificateLocalizationService != null) {
          certificateLocalizationService.removeX509Material(appUser, appId.toString());
        }
      }
      if (blocking) {
        return revokeInternal(getCertificateIdentifier(appId, appUser, cryptoMaterialVersion));
      }
      putToQueue(appId, appUser, cryptoMaterialVersion);
    } catch (InterruptedException ex) {
      LOG.warn("Shutting down while putting revocation event for user " + appUser + " and application " + appId, ex);
    }
    // Return value here doesn't really matter
    return false;
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void putToQueue(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion)
      throws InterruptedException {
    revocationEvents.put(new CertificateRevocationEvent(getCertificateIdentifier(appId, applicationUser, cryptoMaterialVersion)));
  }
  
  public static String getCertificateIdentifier(ApplicationId appId, String user, Integer cryptoMaterialVersion) {
    return user + "__" + appId.toString() + "__" + cryptoMaterialVersion;
  }
  
  public char[] generateRandomPassword() {
    return RandomStringUtils.random(20, 0, 0, true, true, null, rng)
        .toCharArray();
  }
  
  protected enum TYPE {
    KEYSTORE,
    TRUSTSTORE
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public KeyStore loadSystemTrustStore(Configuration conf) throws GeneralSecurityException, IOException {
    String sslConfName = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    Configuration sslConf = new Configuration();
    sslConf.addResource(sslConfName);
    String trustStoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
    String trustStorePassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
    String trustStoreType = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_TYPE_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
    
    KeyStore trustStore = KeyStore.getInstance(trustStoreType);
    try (FileInputStream fis = new FileInputStream(trustStoreLocation)) {
      trustStore.load(fis, trustStorePassword.toCharArray());
    }
    
    return trustStore;
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected KeyPair generateKeyPair() {
    return keyPairGenerator.genKeyPair();
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected PKCS10CertificationRequest generateCSR(ApplicationId appId, String applicationUser, KeyPair keyPair,
      Integer cryptoMaterialVersion)
      throws OperatorCreationException {
    LOG.info("Generating certificate for application: " + appId);
    // Create X500 subject CN=USER, O=APPLICATION_ID
    X500Name subject = createX500Subject(appId, applicationUser, cryptoMaterialVersion);
    // Create Certificate Signing Request
    return createCSR(subject, keyPair);
  }
  
  private X500Name createX500Subject(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion) {
    if (appId == null || applicationUser == null) {
      throw new IllegalArgumentException("ApplicationID and application user cannot be null");
    }
    X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    x500NameBuilder.addRDN(BCStyle.CN, applicationUser);
    x500NameBuilder.addRDN(BCStyle.O, appId.toString());
    x500NameBuilder.addRDN(BCStyle.OU, cryptoMaterialVersion.toString());
    return x500NameBuilder.build();
  }
  
  private PKCS10CertificationRequest createCSR(X500Name subject, KeyPair keyPair) throws OperatorCreationException {
    PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(
        subject, keyPair.getPublic());
    return csrBuilder.build(
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(SECURITY_PROVIDER).build(keyPair.getPrivate()));
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected CertificateBundle sendCSRAndGetSigned(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    return rmAppSecurityActions.sign(csr);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected KeyStoresWrapper createApplicationStores(CertificateBundle certificateBundle, PrivateKey privateKey,
      String appUser, ApplicationId appId)
      throws GeneralSecurityException, IOException {
    char[] password = generateRandomPassword();
    
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    X509Certificate[] chain = new X509Certificate[2];
    chain[0] = certificateBundle.certificate;
    chain[1] = certificateBundle.issuer;
    keyStore.setKeyEntry(appUser, privateKey, password, chain);
    
    KeyStore systemTrustStore = loadSystemTrustStore(config);
    KeyStore appTrustStore = KeyStore.getInstance("JKS");
    appTrustStore.load(null, null);
    
    Enumeration<String> aliases = systemTrustStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      X509Certificate cert = (X509Certificate) systemTrustStore.getCertificate(alias);
      appTrustStore.setCertificateEntry(alias, cert);
    }
    
    return new KeyStoresWrapper(keyStore, password, appTrustStore, password, appUser, appId);
  }
  
  private boolean revokeInternal(String certificateIdentifier) {
    if (isHopsTLSEnabled()) {
      try {
        rmAppSecurityActions.revoke(certificateIdentifier);
        return true;
      } catch (URISyntaxException | IOException | GeneralSecurityException ex) {
        LOG.error("Could not revoke certificate " + certificateIdentifier, ex);
        return false;
      }
    }
    return true;
  }
  
  // Used only for testing
  @VisibleForTesting
  protected void waitForQueueToDrain() throws InterruptedException {
    if (revocationEventsHandler == null || !revocationEventsHandler.isAlive()) {
      return;
    }
    while (revocationEvents.peek() != null) {
      TimeUnit.MILLISECONDS.sleep(30);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public boolean isHopsTLSEnabled() {
    return hopsTLSEnabled;
  }
  
  public static class X509SecurityManagerMaterial extends RMAppSecurityManager.SecurityManagerMaterial {
    private final byte[] keyStore;
    private final char[] keyStorePassword;
    private final byte[] trustStore;
    private final char[] trustStorePassword;
    private final Long expirationEpoch;
    private Integer cryptoMaterialVersion;
  
    public X509SecurityManagerMaterial(ApplicationId applicationId, byte[] keyStore, char[] keyStorePassword, byte[] trustStore,
        char[] trustStorePassword, Long expirationEpoch) {
      super(applicationId);
      this.keyStore = keyStore;
      this.keyStorePassword = keyStorePassword;
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.expirationEpoch = expirationEpoch;
    }
  
    public byte[] getKeyStore() {
      return keyStore;
    }
  
    public char[] getKeyStorePassword() {
      return keyStorePassword;
    }
  
    public byte[] getTrustStore() {
      return trustStore;
    }
  
    public char[] getTrustStorePassword() {
      return trustStorePassword;
    }
  
    public Long getExpirationEpoch() {
      return expirationEpoch;
    }
    
    public Integer getCryptoMaterialVersion() {
      return cryptoMaterialVersion;
    }
    
    public void setCryptoMaterialVersion(Integer cryptoMaterialVersion) {
      this.cryptoMaterialVersion = cryptoMaterialVersion;
    }
  }
  
  public static class X509MaterialParameter extends RMAppSecurityManager.SecurityManagerMaterial {
    private final String appUser;
    private final Integer cryptoMaterialVersion;
    private final boolean isFromRenewal;
    private Long expiration;
  
    public X509MaterialParameter(ApplicationId applicationId, String appUser, Integer cryptoMaterialVersion) {
      this(applicationId, appUser, cryptoMaterialVersion, false);
    }
    
    public X509MaterialParameter(ApplicationId applicationId, String appUser, Integer cryptoMaterialVersion,
        boolean isFromRenewal) {
      super(applicationId);
      this.appUser = appUser;
      this.cryptoMaterialVersion = cryptoMaterialVersion;
      this.isFromRenewal = isFromRenewal;
    }
  
    public String getAppUser() {
      return appUser;
    }
  
    public Integer getCryptoMaterialVersion() {
      return cryptoMaterialVersion;
    }
  
    public boolean isFromRenewal() {
      return isFromRenewal;
    }
  
    public Long getExpiration() {
      return expiration;
    }
  
    public void setExpiration(Long expiration) {
      this.expiration = expiration;
    }
    
    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof X509MaterialParameter) {
        X509MaterialParameter x509Param = (X509MaterialParameter) other;
        return this.getApplicationId().equals(x509Param.getApplicationId())
            && this.appUser.equals(x509Param.appUser)
            && this.cryptoMaterialVersion.equals(x509Param.cryptoMaterialVersion)
            && this.isFromRenewal == x509Param.isFromRenewal;
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + getApplicationId().hashCode();
      result = 31 * result + appUser.hashCode();
      result = 31 * result + cryptoMaterialVersion.hashCode();
      result = 31 * result + (isFromRenewal ? 1 : 0);
      return result;
    }
  }
  
  /**
   * Revocation of certificate should normally happen when all NM running the application
   * have confirmed that they have updated their X509 material with the new one.
   * In case a NM crashes, we can't wait for ever to revoke the previous certificate. This thread
   * goes through all the applications. If there is an application in *X509 rotating phase* and
   * sufficient time has passed since the rotation phase has started, it triggers a revocation event.
   */
  private class CertificateRevocationMonitor extends Thread {
    private final Map<ChronoUnit, TimeUnit> CHRONO_MAPPING = new HashMap<>();
    private final TimeUnit intervalForSleep;
    
    private CertificateRevocationMonitor() {
      CHRONO_MAPPING.put(ChronoUnit.MILLIS, TimeUnit.MILLISECONDS);
      CHRONO_MAPPING.put(ChronoUnit.SECONDS, TimeUnit.SECONDS);
      CHRONO_MAPPING.put(ChronoUnit.MINUTES, TimeUnit.MINUTES);
      CHRONO_MAPPING.put(ChronoUnit.HOURS, TimeUnit.HOURS);
      CHRONO_MAPPING.put(ChronoUnit.DAYS, TimeUnit.DAYS);
      intervalForSleep = CHRONO_MAPPING.get(revocationUnitOfInterval);
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          LocalDateTime now = DateUtils.getNow();
          for (Map.Entry<ApplicationId, RMApp> entry : rmContext.getRMApps().entrySet()) {
            RMApp app = entry.getValue();
            if (app.isAppRotatingCryptoMaterial()) {
              if (DateUtils.unixEpoch2LocalDateTime(app.getMaterialRotationStartTime())
                  .minus(revocationMonitorInterval, revocationUnitOfInterval).isBefore(now)) {
                Integer versionToRevoke = app.getCryptoMaterialVersion() - 1;
                LOG.debug("Revoking certificate for app " + entry.getKey() + " with version " + versionToRevoke);
                putToQueue(app.getApplicationId(), app.getUser(), versionToRevoke);
                ((RMAppImpl) app).resetCryptoRotationMetrics();
              }
            }
          }
          
          intervalForSleep.sleep(revocationMonitorInterval);
        } catch (InterruptedException ex) {
          LOG.info("Certificate revocation monitor stopping");
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
  @VisibleForTesting
  public Map<ApplicationId, ScheduledFuture> getRenewalTasks() {
    return renewalTasks;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected class X509Renewer implements Runnable {
    protected final ApplicationId appId;
    protected final String appUser;
    protected final BackOff backOff;
    protected Integer currentCryptoVersion;
    protected long backOffTime = 0L;
    
    public X509Renewer(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
      this.appId = appId;
      this.appUser = appUser;
      this.currentCryptoVersion = currentCryptoVersion;
      this.backOff = rmAppSecurityManager.createBackOffPolicy();
    }
    
    @Override
    public void run() {
      try {
        LOG.debug("Renewing certificate for application " + appId);
        KeyPair keyPair = generateKeyPair();
        PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, ++currentCryptoVersion);
        CertificateBundle certificateBundle = sendCSRAndGetSigned(csr);
        long expiration = certificateBundle.certificate.getNotAfter().getTime();
        
        KeyStoresWrapper keyStoresWrapper = createApplicationStores(certificateBundle, keyPair.getPrivate(), appUser,
            appId);
        byte[] rawProtectedKeyStore = keyStoresWrapper.getRawKeyStore(TYPE.KEYSTORE);
        byte[] rawTrustStore = keyStoresWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
        
        rmContext.getCertificateLocalizationService().updateX509(appUser, appId.toString(),
            ByteBuffer.wrap(rawProtectedKeyStore), String.valueOf(keyStoresWrapper.keyStorePassword),
            ByteBuffer.wrap(rawTrustStore), String.valueOf(keyStoresWrapper.trustStorePassword));
        
        renewalTasks.remove(appId);
        X509SecurityManagerMaterial x509Material = new X509SecurityManagerMaterial(
            appId, rawProtectedKeyStore, keyStoresWrapper.keyStorePassword,
            rawTrustStore, keyStoresWrapper.trustStorePassword, expiration);
        eventHandler.handle(new RMAppSecurityMaterialRenewedEvent<>(appId, x509Material));
        LOG.debug("Renewed certificate for application " + appId);
      } catch (Exception ex) {
        renewalTasks.remove(appId);
        backOffTime = backOff.getBackOffInMillis();
        if (backOffTime != -1) {
          LOG.warn("Failed to renew certificate for application " + appId + ". Retrying in " + backOffTime + " ms");
          ScheduledFuture task = renewalExecutorService.schedule(this, backOffTime, TimeUnit.MILLISECONDS);
          renewalTasks.put(appId, task);
        } else {
          LOG.error("Failed to renew certificate for application " + appId + ". Failed more than 4 times, giving " +
              "up", ex);
        }
      }
    }
  }
  
  protected static class CertificateBundle {
    private final X509Certificate certificate;
    private final X509Certificate issuer;
    
    protected CertificateBundle(X509Certificate certificate, X509Certificate issuer) {
      this.certificate = certificate;
      this.issuer = issuer;
    }
    
    public X509Certificate getCertificate() {
      return certificate;
    }
    
    public X509Certificate getIssuer() {
      return issuer;
    }
  }
  
  protected class KeyStoresWrapper {
    private final KeyStore keystore;
    private final char[] keyStorePassword;
    private final KeyStore trustStore;
    private final char[] trustStorePassword;
    private final String appUser;
    private final ApplicationId appId;
    
    private KeyStoresWrapper(KeyStore keyStore, char[] keyStorePassword, KeyStore trustStore, char[] trustStorePassword,
        String appUser, ApplicationId appId) {
      this.keystore = keyStore;
      this.keyStorePassword = keyStorePassword;
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.appUser = appUser;
      this.appId = appId;
    }
    
    protected KeyStore getKeystore() {
      return keystore;
    }
    
    protected char[] getKeyStorePassword() {
      return keyStorePassword;
    }
    
    protected KeyStore getTrustStore() {
      return trustStore;
    }
    
    protected char[] getTrustStorePassword() {
      return trustStorePassword;
    }
    
    protected byte[] getRawKeyStore(TYPE type) throws GeneralSecurityException, IOException {
      File target;
      char[] password;
      KeyStore keyStore;
      if (type.equals(TYPE.KEYSTORE)) {
        target = Paths.get(TMP, appUser + "-" + appId.toString() + "_kstore.jks").toFile();
        password = keyStorePassword;
        keyStore = this.keystore;
      } else {
        target = Paths.get(TMP, appUser + "-" + appId.toString() + "_tstore.jks").toFile();
        password = trustStorePassword;
        keyStore = this.trustStore;
      }
      
      try (FileOutputStream fos = new FileOutputStream(target, false)) {
        keyStore.store(fos, password);
      }
      
      byte[] rawKeyStore = Files.readAllBytes(target.toPath());
      FileUtils.deleteQuietly(target);
      return rawKeyStore;
    }
  }
  
  private class CertificateRevocationEvent {
    private final String identifier;
    
    private CertificateRevocationEvent(String identifier) {
      this.identifier = identifier;
    }
  }
  
  private class RevocationEventsHandler extends Thread {
    
    private void drain() {
      List<CertificateRevocationEvent> events = new ArrayList<>(revocationEvents.size());
      revocationEvents.drainTo(events);
      for (CertificateRevocationEvent event : events) {
        revokeInternal(event.identifier);
      }
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          CertificateRevocationEvent event = revocationEvents.take();
          revokeInternal(event.identifier);
        } catch (InterruptedException ex) {
          LOG.info("RevocationEventsHandler interrupted. Exiting...");
          drain();
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
