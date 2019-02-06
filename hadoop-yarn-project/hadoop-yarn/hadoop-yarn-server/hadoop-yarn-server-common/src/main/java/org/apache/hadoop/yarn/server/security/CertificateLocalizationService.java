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
package org.apache.hadoop.yarn.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
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

  private final Map<StorageKey, CryptoMaterial> materialLocation =
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
    LLAP("LLAP");

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
    
    StorageKey key = new StorageKey(username, applicationId);
    
    try {
      lock.lock();
      CryptoMaterial material = materialLocation.get(key);
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
        
        CryptoMaterial cryptoMaterial = new CryptoMaterial(appDirPath, keyStorePath, trustStorePath, passwdPath,
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
  
  private void writeToLocalFS(ByteBuffer keyStore, File keyStoreLocation, ByteBuffer trustStore, File
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
        event.cryptoMaterial.changeState(CryptoMaterial.STATE.FINISHED);
        event.cryptoMaterial.notifyAll();
        return false;
      }
      event.cryptoMaterial.changeState(CryptoMaterial.STATE.ONGOING);
    }
    LOG.debug("Materializing internal for " + event.key);
    File appDirFile = event.cryptoMaterial.getCertFolder().toFile();
    if (!appDirFile.exists()) {
      appDirFile.mkdirs();
    }
    
    try {
      writeToLocalFS(event.cryptoMaterial.getKeyStoreMem(), event.cryptoMaterial.getKeyStoreLocation().toFile(),
          event.cryptoMaterial.getTrustStoreMem(), event.cryptoMaterial.getTrustStoreLocation().toFile(),
          event.cryptoMaterial.getKeyStorePass(), event.cryptoMaterial.getPasswdLocation().toFile());
  
      if (service == ServiceType.NM) {
        Set<PosixFilePermission> materialPermissions =
            EnumSet
                .of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE);
    
        Files.setPosixFilePermissions(event.cryptoMaterial.getCertFolder(), materialPermissions);
        Files.setPosixFilePermissions(event.cryptoMaterial.getKeyStoreLocation(), materialPermissions);
        Files.setPosixFilePermissions(event.cryptoMaterial.getTrustStoreLocation(), materialPermissions);
        Files.setPosixFilePermissions(event.cryptoMaterial.getPasswdLocation(), materialPermissions);
      }
      
      synchronized (event.cryptoMaterial) {
        event.cryptoMaterial.changeState(CryptoMaterial.STATE.FINISHED);
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
          event.cryptoMaterial.changeState(CryptoMaterial.STATE.FINISHED);
          event.cryptoMaterial.notifyAll();
        }
        return false;
      } finally {
        lock.unlock();
      }
    }
  }
  
  private void dispatchEvent(LocalizationEvent event) throws InterruptedException {
    localizationEventsQ.put(event);
  }
  
  @Override
  public void removeMaterial(String username) throws InterruptedException {
    removeMaterial(username, null);
  }
  
  @Override
  public void removeMaterial(String username, String applicationId) throws InterruptedException {
    StorageKey key = new StorageKey(username, applicationId);
    
    try {
      lock.lock();
      CryptoMaterial material = materialLocation.get(key);
      if (material != null) {
        material.decrementRequestedApplications();
        LOG.debug("Decrementing requested applications to " + material.getRequestedApplications() + " for key " + key);
        if (material.isSafeToRemove()) {
          RemoveEvent event = new RemoveEvent(material.getCertFolder());
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
    LOG.debug("Purging directory " + event.certificatesDirectory);
    FileUtils.deleteQuietly(event.certificatesDirectory.toFile());
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(String username)
      throws FileNotFoundException, InterruptedException {
    return getMaterialLocation(username, null);
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException {
    StorageKey key = new StorageKey(username, applicationId);
    
    CryptoMaterial material = materialLocation.get(key);
    
    LOG.debug("Trying to get material for key " + key);
    if (material == null) {
      throw new FileNotFoundException("Materialized crypto material could not be found for user " + key);
    }
    
    synchronized (material) {
      while (!material.getState().equals(CryptoMaterial.STATE.FINISHED)) {
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
  
  @Override
  public void updateCryptoMaterial(String username, String applicationId, ByteBuffer keyStore,
      String keyStorePassword, ByteBuffer trustStore, String trustStorePassword)
      throws IOException, InterruptedException {
    StorageKey key = new StorageKey(username, applicationId);
    CryptoMaterial material = materialLocation.get(key);
    if (material == null) {
      LOG.warn("Requested to update crypto material for " + key + " but material is missing");
      return;
    }
    
    synchronized (material) {
      while (!material.getState().equals(CryptoMaterial.STATE.FINISHED)) {
        material.wait();
      }
      material.changeState(CryptoMaterial.STATE.ONGOING);
    }
    
    try {
      // Lock to ensure no remove event is being processed
      lock.lock();
      material = materialLocation.get(key);
      if (material == null) {
        // Oops material has been removed
        return;
      }
      
      writeToLocalFS(keyStore.asReadOnlyBuffer(), material.getKeyStoreLocation().toFile(),
          trustStore.asReadOnlyBuffer(), material.getTrustStoreLocation().toFile(),
          keyStorePassword, material.getPasswdLocation().toFile());
      
      material.updateKeyStoreMem(keyStore);
      material.updateKeyStorePass(keyStorePassword);
      material.updateTrustStoreMem(trustStore);
      material.updateTrustStorePass(trustStorePassword);
    } finally {
      if (material != null) {
        synchronized (material) {
          material.changeState(CryptoMaterial.STATE.FINISHED);
          material.notifyAll();
        }
      }
      lock.unlock();
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
    ImmutableMap<StorageKey, CryptoMaterial> state;
    try {
      lock.lock();
      state = ImmutableMap.copyOf(materialLocation);
    } finally {
      lock.unlock();
    }
    
    ReturnState<String, String> returnState = new ReturnState<>();
    
    ReturnState<String, Integer> internalState = new ReturnState<>();
    for (Map.Entry<StorageKey, CryptoMaterial> entry : state.entrySet()) {
      internalState.put(entry.getKey().username, entry.getValue().getRequestedApplications());
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
    StorageKey key = new StorageKey(username, applicationId);
    CryptoMaterial material = materialLocation.remove(key);
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
          while (!material.getState().equals(CryptoMaterial.STATE.FINISHED)) {
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
  
  private class StorageKey {
    private final String username;
    private final String applicationId;
    
    public StorageKey(String username, String applicationId) {
      this.username = username;
      this.applicationId = applicationId;
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
            && applicationId.equals(((StorageKey) other).applicationId);
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
      return result;
    }
    
    @Override
    public String toString() {
      String appId = applicationId != null ? applicationId : "unknown";
      return "CryptoKey <" + username + ", " + appId + ">";
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected class MaterializeEvent implements LocalizationEvent {
    private final StorageKey key;
    private final CryptoMaterial cryptoMaterial;
    
    private MaterializeEvent(StorageKey key, CryptoMaterial cryptoMaterial) {
      this.key = key;
      this.cryptoMaterial = cryptoMaterial;
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected class RemoveEvent implements LocalizationEvent {
    private final Path certificatesDirectory;
    
    private RemoveEvent(Path certificatesDirectory) {
      this.certificatesDirectory = certificatesDirectory;
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
}
