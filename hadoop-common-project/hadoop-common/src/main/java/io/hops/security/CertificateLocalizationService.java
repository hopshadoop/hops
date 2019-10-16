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
package io.hops.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.SecurityMaterial;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.JWTSecurityMaterial;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.mortbay.util.ajax.JSON;

import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.LimitedPrivate({"Hive"})
public class CertificateLocalizationService extends AbstractService
    implements CertificateLocalization, CertificateLocalizationMBean {
  
  
  private final Logger LOG = LogManager.getLogger
      (CertificateLocalizationService.class);
  
  private final String SYSTEM_TMP = System.getProperty("java.io.tmpdir",
      "/tmp");
  private final String LOCALIZATION_DIR_NAME = "certLoc";
  private Path materializeDir;
  private String superKeystoreLocation;
  private String superKeystorePass;
  private String superKeyPassword;
  private String superTrustStoreLocation;
  private String superTruststorePass;

  private final Map<StorageKey, SecurityMaterial> materialLocation =
      new ConcurrentHashMap<>();
  private ObjectName mbeanObjectName;
  private final ServiceType service;
  public static final String JMX_MATERIALIZED_KEY = "materialized";
  public static final String JMX_FORCE_REMOVE_OP = "forceRemoveMaterial";
  private final ReentrantLock lock = new ReentrantLock(true);
  private final BlockingQueue<LocalizationEvent> localizationEventsQ;
  
  private Thread localizationEventsHandler;
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected interface LocalizationEvent {
  }
  
  public enum ServiceType {
    RM("RM"),
    NM("NM"),
    HS2("HS2"),
    HM("HM"),
    LLAP("LLAP"),
    DN("DN");

    private final String service;

    ServiceType(String service) {
      this.service = service;
    }

    @Override
    public String toString() {
      return service;
    }
  }

  public CertificateLocalizationService(ServiceType service) {
    super(CertificateLocalizationService.class.getName());
    this.service = service;
    localizationEventsQ = new ArrayBlockingQueue<>(100);
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    parseSuperuserMaterial(conf);
    String localizationDir = service.toString() +  "_" + LOCALIZATION_DIR_NAME;
    materializeDir = Paths.get(SYSTEM_TMP, localizationDir);
    File fileMaterializeDir = materializeDir.toFile();
    if (!fileMaterializeDir.exists()) {
      fileMaterializeDir.mkdir();
      Set<PosixFilePermission> materializeDirPerm;
      if (service == ServiceType.NM) {
        // the nm user should have full access to the directory, everyone else should have only execute access
        // to traverse the directory
        materializeDirPerm = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE);
      } else {
        // Only the rm user should access to this directory
        materializeDirPerm = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
      }
      Files.setPosixFilePermissions(materializeDir, materializeDirPerm);
    }
  
    LOG.debug("Initialized at dir: " + materializeDir.toString());
    
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    localizationEventsHandler = createLocalizationEventsHandler();
    localizationEventsHandler.setDaemon(true);
    localizationEventsHandler.setName("CertificateLocalizationEvents handler");
    localizationEventsHandler.start();
    
    StandardMBean mbean = new StandardMBean(this, CertificateLocalizationMBean.class);
    mbeanObjectName = MBeans.register(service.toString(), "CertificateLocalizer", mbean);

    super.serviceStart();
  }

  private void parseSuperuserMaterial(Configuration conf) {
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
        "ssl-server.xml"));
    superKeystoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY));
    superKeystorePass = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY));
    superKeyPassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY));
    superTrustStoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
    superTruststorePass = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
  }
  
  // This method is accessible only from RM or NM. In any other case
  // CertificateLocalizationService is null
  @Override
  public String getSuperKeystoreLocation() {
    return superKeystoreLocation;
  }
  
  // This method is accessible only from RM or NM. In any other case
  // CertificateLocalizationService is null
  @Override
  public String getSuperKeystorePass() {
    return superKeystorePass;
  }
  
  // This method is accessible only from RM or NM. In any other case
  // CertificateLocalizationService is null
  @Override
  public String getSuperKeyPassword() {
    return superKeyPassword;
  }
  
  // This method is accessible only from RM or NM. In any other case
  // CertificateLocalizationService is null
  @Override
  public String getSuperTruststoreLocation() {
    return superTrustStoreLocation;
  }
  
  // This method is accessible only from RM or NM. In any other case
  // CertificateLocalizationService is null
  @Override
  public String getSuperTruststorePass() {
    return superTruststorePass;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (mbeanObjectName != null) {
      MBeans.unregister(mbeanObjectName);
    }
  
    if (localizationEventsHandler != null) {
      localizationEventsHandler.interrupt();
    }
    
    if (null != materializeDir) {
      FileUtils.deleteQuietly(materializeDir.toFile());
    }
    
    LOG.debug("Stopped CertificateLocalization service");
    super.serviceStop();
  }
  
  @VisibleForTesting
  public Path getMaterializeDirectory() {
    return materializeDir;
  }

  @Override
  public void materializeCertificates(String username, String userFolder,
      ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws InterruptedException {
    materializeCertificates(username, null, userFolder,
        keyStore, keyStorePassword, trustStore, trustStorePassword);
  }
  
  @Override
  public void materializeCertificates(String username, String applicationId, String userFolder,
      ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws InterruptedException {
    
    StorageKey key = StorageKey.newX509Instance(username, applicationId);
    
    try {
      lock.lock();
      SecurityMaterial material = materialLocation.get(key);
      if (material != null) {
        material.incrementRequestedApplications();
        LOG.debug("Incrementing requested application to " + material.getRequestedApplications() + " for key " + key);
      } else {
        Path appDirPath;
        if (applicationId != null) {
          appDirPath = Paths.get(materializeDir.toString(), userFolder, applicationId);
        } else {
          appDirPath = Paths.get(materializeDir.toString(), userFolder);
        }
  
        Path keyStorePath = Paths.get(appDirPath.toFile().getAbsolutePath(), username + HopsSSLSocketFactory
            .KEYSTORE_SUFFIX);
        Path trustStorePath = Paths.get(appDirPath.toFile().getAbsolutePath(), username + HopsSSLSocketFactory
            .TRUSTSTORE_SUFFIX);
        Path passwdPath = Paths.get(appDirPath.toFile().getAbsolutePath(), username + HopsSSLSocketFactory
            .PASSWD_FILE_SUFFIX);
        
        SecurityMaterial cryptoMaterial = new X509SecurityMaterial(appDirPath, keyStorePath, trustStorePath, passwdPath,
            keyStore, keyStorePassword, trustStore, trustStorePassword);
        materialLocation.put(key, cryptoMaterial);
        MaterializeEvent event = new MaterializeEvent(key, cryptoMaterial);
        dispatchEvent(event);
        LOG.debug("Dispatch materialize event for key " + key);
      }
    } finally {
     lock.unlock();
    }
  }
  
  private SecurityMaterial prepareMaterialForChange(StorageKey key)
      throws MaterialNotFoundException, InterruptedException {
    try {
      lock.lock();
      SecurityMaterial material = materialLocation.get(key);
      if (material == null) {
        throw new MaterialNotFoundException("Material for key " + key + " is missing");
      }
      synchronized (material) {
        while (!material.getState().equals(SecurityMaterial.STATE.FINISHED)) {
          material.wait();
        }
        material.changeState(SecurityMaterial.STATE.ONGOING);
      }
      return material;
    } finally {
      lock.unlock();
    }
  }
  
  @Override
  public void updateX509(String username, String applicationId, ByteBuffer keyStore,
      String keyStorePassword, ByteBuffer trustStore, String trustStorePassword)
    throws InterruptedException {
    StorageKey key = StorageKey.newX509Instance(username, applicationId);
    try {
      X509SecurityMaterial x509Material = (X509SecurityMaterial) prepareMaterialForChange(key);
      x509Material.updateKeyStoreMem(keyStore);
      x509Material.updateKeyStorePass(keyStorePassword);
      x509Material.updateTrustStoreMem(trustStore);
      x509Material.updateTrustStorePass(trustStorePassword);
      // Dispatch updateEvent
      UpdateEvent updateEvent = new UpdateEvent(key, x509Material);
      dispatchEvent(updateEvent);
      LOG.debug("Dispatched update event for key " + key);
    } catch (MaterialNotFoundException ex) {
      LOG.warn("Requested X.509 update but got an exception", ex);
    }
  }
  
  
  @Override
  public void updateJWT(String username, String applicationId, String jwt)
      throws InterruptedException {
    StorageKey key = StorageKey.newJWTInstance(username, applicationId);
    try {
      JWTSecurityMaterial jwtMaterial = (JWTSecurityMaterial) prepareMaterialForChange(key);
      jwtMaterial.updateToken(jwt);
      UpdateEvent updateEvent = new UpdateEvent(key, jwtMaterial);
      dispatchEvent(updateEvent);
      LOG.debug("Dispatched JWT update event for key " + key);
    } catch (MaterialNotFoundException ex) {
      LOG.warn("Requested JWT update but got an exception", ex);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void updateInternal(UpdateEvent event) {
    String errorMsg = "";
    try {
      SecurityMaterial storedMaterial = materialLocation.get(event.key);
      if (storedMaterial == null) {
        LOG.warn("Requested X.509 update for key " + event.key + " but material is missing or has been removed");
        return;
      }
      if (event.cryptoMaterial instanceof X509SecurityMaterial) {
        errorMsg = "Could not update X.509 security material for key " + event.key;
        updateInternalX509((X509SecurityMaterial) event.cryptoMaterial);
      } else if (event.cryptoMaterial instanceof JWTSecurityMaterial) {
        errorMsg = "Could not update JWT security material for key " + event.key;
        updateInternalJWT((JWTSecurityMaterial) event.cryptoMaterial);
      } else {
        errorMsg = "Unknown security material type " + event.cryptoMaterial.getClass().getCanonicalName();
      }
    } catch (IOException ex) {
      LOG.error("Error updating security material: " + errorMsg, ex);
    } finally {
      if (event.cryptoMaterial != null) {
        synchronized (event.cryptoMaterial) {
          event.cryptoMaterial.changeState(SecurityMaterial.STATE.FINISHED);
          event.cryptoMaterial.notifyAll();
        }
      }
    }
  }
  
  private void updateInternalX509(X509SecurityMaterial securityMaterial) throws IOException {
    writeX509ToLocalFS(securityMaterial.getKeyStoreMem(), securityMaterial.getKeyStoreLocation().toFile(),
        securityMaterial.getTrustStoreMem(), securityMaterial.getTrustStoreLocation().toFile(),
        securityMaterial.getKeyStorePass(), securityMaterial.getPasswdLocation().toFile());
  }
  
  private void updateInternalJWT(JWTSecurityMaterial securityMaterial) throws IOException {
    FileUtils.writeStringToFile(securityMaterial.getTokenLocation().toFile(), securityMaterial.getToken());
  }
  
  @Override
  public void materializeJWT(String username, String applicationId, String userFolder, String jwt)
    throws InterruptedException {
    StorageKey key = StorageKey.newJWTInstance(username, applicationId);
    try {
      lock.lock();
      SecurityMaterial material = materialLocation.get(key);
      if (material != null) {
        material.incrementRequestedApplications();
        LOG.debug("Incrementing requested application to " + material.getRequestedApplications() + " for key " + key);
      } else {
        Path appDirPath;
        if (applicationId != null) {
          appDirPath = Paths.get(materializeDir.toString(), userFolder, applicationId);
        } else {
          appDirPath = Paths.get(materializeDir.toString(), userFolder);
        }
        
        Path jwtPath = Paths.get(appDirPath.toFile().getAbsolutePath(), username + JWTSecurityMaterial
            .JWT_FILE_SUFFIX);
        material = new JWTSecurityMaterial(appDirPath, jwtPath, jwt);
        materialLocation.put(key, material);
        MaterializeEvent event = new MaterializeEvent(key, material);
        dispatchEvent(event);
        LOG.debug("Dispatch materialize event for key " + key);
      }
    } finally {
      lock.unlock();
    }
  }
  
  private void writeX509ToLocalFS(ByteBuffer keyStore, File keyStoreLocation, ByteBuffer trustStore, File
      trustStoreLocation, String password, File passwordFileLocation) throws IOException {
    FileChannel keyStoreChannel = new FileOutputStream(keyStoreLocation, false).getChannel();
    keyStoreChannel.write(keyStore);
    keyStoreChannel.close();
  
    FileChannel trustStoreChannel = new FileOutputStream(trustStoreLocation, false).getChannel();
    trustStoreChannel.write(trustStore);
    trustStoreChannel.close();
  
    FileUtils.writeStringToFile(passwordFileLocation, password);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected boolean materializeInternal(MaterializeEvent event) {
    synchronized (event.cryptoMaterial) {
      if (event.cryptoMaterial.hasBeenCanceled()) {
        LOG.debug("Tried to materialize " + event.key + " but it has already been canceled, ignoring...");
        event.cryptoMaterial.changeState(SecurityMaterial.STATE.FINISHED);
        event.cryptoMaterial.notifyAll();
        return false;
      }
      event.cryptoMaterial.changeState(SecurityMaterial.STATE.ONGOING);
    }
    LOG.debug("Materializing internal for " + event.key);
    File appDirFile = event.cryptoMaterial.getCertFolder().toFile();
    if (!appDirFile.exists()) {
      appDirFile.mkdirs();
    }
    
    try {
      if (event.cryptoMaterial instanceof X509SecurityMaterial) {
        materializeInternalX509((X509SecurityMaterial) event.cryptoMaterial);
      } else if (event.cryptoMaterial instanceof JWTSecurityMaterial) {
        materializeInternalJWT((JWTSecurityMaterial) event.cryptoMaterial);
      }
      
      synchronized (event.cryptoMaterial) {
        event.cryptoMaterial.changeState(SecurityMaterial.STATE.FINISHED);
        event.cryptoMaterial.notifyAll();
      }
      LOG.debug("Finished materialization for " + event.key);
      return true;
    } catch (IOException ex) {
      LOG.error(ex, ex);
      try {
        lock.lock();
        materialLocation.remove(event.key);
        FileUtils.deleteQuietly(event.cryptoMaterial.getCertFolder().toFile());
        synchronized (event.cryptoMaterial) {
          event.cryptoMaterial.changeState(SecurityMaterial.STATE.FINISHED);
          event.cryptoMaterial.notifyAll();
        }
        return false;
      } finally {
        lock.unlock();
      }
    }
  }
  
  private void materializeInternalX509(X509SecurityMaterial material) throws IOException {
    writeX509ToLocalFS(material.getKeyStoreMem(), material.getKeyStoreLocation().toFile(),
        material.getTrustStoreMem(), material.getTrustStoreLocation().toFile(),
        material.getKeyStorePass(), material.getPasswdLocation().toFile());
  
    if (service == ServiceType.NM) {
      Set<PosixFilePermission> materialPermissions =
          EnumSet
              .of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                  PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE);
    
      Files.setPosixFilePermissions(material.getCertFolder(), materialPermissions);
      Files.setPosixFilePermissions(material.getKeyStoreLocation(), materialPermissions);
      Files.setPosixFilePermissions(material.getTrustStoreLocation(), materialPermissions);
      Files.setPosixFilePermissions(material.getPasswdLocation(), materialPermissions);
    }
  }
  
  private void materializeInternalJWT(JWTSecurityMaterial material) throws IOException {
    FileUtils.writeStringToFile(material.getTokenLocation().toFile(), material.getToken());
    if (service == ServiceType.NM) {
      Set<PosixFilePermission> materialPermissions =
          EnumSet
              .of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                  PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE);
    
      Files.setPosixFilePermissions(material.getCertFolder(), materialPermissions);
      Files.setPosixFilePermissions(material.getTokenLocation(), materialPermissions);
    }
  }
  
  private void dispatchEvent(LocalizationEvent event) throws InterruptedException {
    localizationEventsQ.put(event);
  }
  
  @Override
  public void removeX509Material(String username) throws InterruptedException {
    removeX509Material(username, null);
  }
  
  @Override
  public void removeX509Material(String username, String applicationId) throws InterruptedException {
    StorageKey key = StorageKey.newX509Instance(username, applicationId);
    
    checkAndRemoveAsync(key);
  }
  
  @Override
  public void removeJWTMaterial(String username, String applicationId) throws InterruptedException {
    StorageKey key = StorageKey.newJWTInstance(username, applicationId);
    checkAndRemoveAsync(key);
  }
  
  private void checkAndRemoveAsync(StorageKey key) throws InterruptedException {
    try {
      lock.lock();
      SecurityMaterial material = materialLocation.get(key);
      if (material != null) {
        material.decrementRequestedApplications();
        LOG.debug("Decrementing requested applications to " + material.getRequestedApplications() + " for key " + key);
        if (material.isSafeToRemove()) {
          material.changeState(SecurityMaterial.STATE.ONGOING);
          RemoveEvent event = new RemoveEvent(material);
          materialLocation.remove(key);
          dispatchEvent(event);
          LOG.debug("Dispatching remove event for key " + key);
        }
      }
    } finally {
      lock.unlock();
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void removeInternal(RemoveEvent event) {
    if (event.cryptoMaterial instanceof X509SecurityMaterial) {
      removeInternalX509((X509SecurityMaterial) event.cryptoMaterial);
    } else if (event.cryptoMaterial instanceof JWTSecurityMaterial) {
      removeInternalJWT((JWTSecurityMaterial) event.cryptoMaterial);
    }
  }
  
  private void removeInternalX509(X509SecurityMaterial material) {
    FileUtils.deleteQuietly(material.getKeyStoreLocation().toFile());
    LOG.debug("Deleted " + material.getKeyStoreLocation());
  
    FileUtils.deleteQuietly(material.getTrustStoreLocation().toFile());
    LOG.debug("Deleted " + material.getTrustStoreLocation());
    
    FileUtils.deleteQuietly(material.getPasswdLocation().toFile());
    LOG.debug("Deleted " + material.getPasswdLocation());
    
    deleteAppDirectoryIfEmpty(material.getCertFolder().toFile());
  }
  
  private void removeInternalJWT(JWTSecurityMaterial material) {
    FileUtils.deleteQuietly(material.getTokenLocation().toFile());
    LOG.debug("Deleted " + material.getTokenLocation());
    
    deleteAppDirectoryIfEmpty(material.getCertFolder().toFile());
  }
  
  private void deleteAppDirectoryIfEmpty(File appDirectory) {
    String[] content = appDirectory.list();
    if (content != null && content.length == 0) {
      FileUtils.deleteQuietly(appDirectory);
      LOG.debug("Deleted app directory " + appDirectory);
    }
  }
  
  @Override
  public X509SecurityMaterial getX509MaterialLocation(String username)
      throws FileNotFoundException, InterruptedException {
    return getX509MaterialLocation(username, null);
  }
  
  @Override
  public X509SecurityMaterial getX509MaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException {
    StorageKey key = StorageKey.newX509Instance(username, applicationId);
    
    return (X509SecurityMaterial) getSecurityMaterialInternal(key);
  }
  
  @Override
  public JWTSecurityMaterial getJWTMaterialLocation(String username, String applicationId)
    throws FileNotFoundException, InterruptedException {
    StorageKey key = StorageKey.newJWTInstance(username, applicationId);
    
    return (JWTSecurityMaterial) getSecurityMaterialInternal(key);
  }
  
  private SecurityMaterial getSecurityMaterialInternal(StorageKey key)
      throws FileNotFoundException, InterruptedException {
    SecurityMaterial material = materialLocation.get(key);
    LOG.debug("Trying to get material for key " + key);
    if (material == null) {
      throw new FileNotFoundException("Materialized crypto material could not be found for user " + key);
    }
  
    synchronized (material) {
      while (!material.getState().equals(SecurityMaterial.STATE.FINISHED)) {
        LOG.debug("Waiting to get notified for key " + key);
        material.wait();
      }
  
      LOG.debug("Notified for key " + key);
      // In case materializer could not materialize certificate, it will remove it from the local cache
      // so if the material does not exist after being notified, it means something went wrong
      material = materialLocation.get(key);
      if (material == null) {
        throw new FileNotFoundException("Materializer could not materialize certificate for " + key);
      }
      return material;
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected LocalizationEvent dequeue() throws InterruptedException {
    return localizationEventsQ.take();
  }
  
  /**
   * JMX section
   */
  private class ReturnState<S, T> extends HashMap<S, T> {
    private static final long serialVersionUID = 1L;
  }
  
  /**
   * Method invoked by a JMX client to get the state of the CertificateLocalization service.
   * Under the attributes tab.
   * @return It returns a map with the name of the material and the number of references.
   */
  @Override
  public String getState() {
    ImmutableMap<StorageKey, SecurityMaterial> state;
    try {
      lock.lock();
      state = ImmutableMap.copyOf(materialLocation);
    } finally {
      lock.unlock();
    }
    
    ReturnState<String, String> returnState = new ReturnState<>();
    
    ReturnState<String, Integer> internalState = new ReturnState<>();
    for (Map.Entry<StorageKey, SecurityMaterial> entry : state.entrySet()) {
      internalState.put(entry.getKey().compactToString(), entry.getValue().getRequestedApplications());
    }
    returnState.put(JMX_MATERIALIZED_KEY, JSON.toString(internalState));
    
    return JSON.toString(returnState);
  }
  
  /**
   * This method SHOULD be used *wisely*!!!
   *
   * This method is invoked by a JMX client to force remove crypto material associated with that username.
   * Under the operations tab.
   * @param username User of the crypto material
   * @return True if the material exists in the store. False otherwise
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Override
  public boolean forceRemoveMaterial(String username)
      throws InterruptedException, ExecutionException {
    return forceRemoveMaterial(username, null);
  }
  
  /**
   * This method SHOULD be used *wisely*!!!
   *
   * This method is invoked by a JMX client to force remove crypto material associated with that username.
   * Under the operations tab.
   * @param username User of the crypto material
   * @param applicationId Application ID associated with this crypto material
   * @return True if the material exists in the store. False otherwise
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Override
  public boolean forceRemoveMaterial(String username, String applicationId)
      throws InterruptedException, ExecutionException {
    StorageKey key = StorageKey.newX509Instance(username, applicationId);
    SecurityMaterial material = materialLocation.remove(key);
    if (material != null) {
      boolean managedToCancel;
      synchronized (material) {
        managedToCancel = material.tryToCancel();
      }
      if (managedToCancel) {
        // Managed to cancel materialization
        FileUtils.deleteQuietly(material.getCertFolder().toFile());
      } else {
        // Materialization has already been started, wait to finish
        synchronized (material) {
          while (!material.getState().equals(SecurityMaterial.STATE.FINISHED)) {
            material.wait();
          }
          FileUtils.deleteQuietly(material.getCertFolder().toFile());
        }
      }
      return true;
    }
    
    return false;
  }
  
  /**
   * End of JMX section
   */
  
  enum STORAGE_KEY_TYPE {
    X509,
    JWT
  }
  
  private static class StorageKey {
    private final String username;
    private final String applicationId;
    private final STORAGE_KEY_TYPE type;
    
    private static StorageKey newX509Instance(String username, String applicationId) {
      return new StorageKey(username, applicationId, STORAGE_KEY_TYPE.X509);
    }
    
    private static StorageKey newJWTInstance(String username, String applicationId) {
      return new StorageKey(username, applicationId, STORAGE_KEY_TYPE.JWT);
    }
    
    public StorageKey(String username, String applicationId, STORAGE_KEY_TYPE type) {
      this.username = username;
      this.applicationId = applicationId;
      this.type = type;
    }
    
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      
      if (!(other instanceof StorageKey)) {
        return false;
      }
      
      if (applicationId != null) {
        return username.equals(((StorageKey) other).username)
            && applicationId.equals(((StorageKey) other).applicationId)
            && type.equals(((StorageKey) other).type);
      }
      
      return username.equals(((StorageKey) other).username);
    }
    
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + username.hashCode();
      if (applicationId != null) {
        result = 31 * result + applicationId.hashCode();
      }
      result =31 * result + type.ordinal();
      
      return result;
    }
    
    @Override
    public String toString() {
      String appId = applicationId != null ? applicationId : "unknown";
      return "CryptoKey <" + username + ", " + appId + ", " + type + ">";
    }
    
    public String compactToString() {
      String appId = applicationId != null ? applicationId : "unknown";
      return username + "/" + appId + "/" + type;
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected class MaterializeEvent implements LocalizationEvent {
    private final StorageKey key;
    private final SecurityMaterial cryptoMaterial;
    
    private MaterializeEvent(StorageKey key, SecurityMaterial cryptoMaterial) {
      this.key = key;
      this.cryptoMaterial = cryptoMaterial;
    }
  }
  
  protected class UpdateEvent implements LocalizationEvent {
    private final StorageKey key;
    private final SecurityMaterial cryptoMaterial;
    
    private UpdateEvent(StorageKey key, SecurityMaterial cryptoMaterial) {
      this.key = key;
      this.cryptoMaterial = cryptoMaterial;
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected class RemoveEvent implements LocalizationEvent {
    private final SecurityMaterial cryptoMaterial;
    
    private RemoveEvent(SecurityMaterial cryptoMaterial) {
      this.cryptoMaterial = cryptoMaterial;
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected Thread createLocalizationEventsHandler() {
    return new LocalizationEventsHandler();
  }
  
  private class LocalizationEventsHandler extends Thread {
  
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          LocalizationEvent event = localizationEventsQ.take();
          if (event instanceof MaterializeEvent) {
            materializeInternal((MaterializeEvent) event);
          } else if (event instanceof RemoveEvent) {
            removeInternal((RemoveEvent) event);
          } else if (event instanceof UpdateEvent) {
            updateInternal((UpdateEvent) event);
          } else {
            LOG.warn("Unknown event type");
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("Stopping localization events handler thread");
    }
  }
  
  private class MaterialNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public MaterialNotFoundException(String reason) {
      super(reason);
    }
    
    public MaterialNotFoundException(String reason, Throwable throwable) {
      super(reason, throwable);
    }
    
    public MaterialNotFoundException(Throwable throwable) {
      super(throwable);
    }
  }
}
