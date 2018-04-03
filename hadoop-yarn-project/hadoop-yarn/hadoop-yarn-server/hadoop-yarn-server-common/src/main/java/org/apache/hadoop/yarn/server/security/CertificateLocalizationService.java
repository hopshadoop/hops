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
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.factories.RecordFactory;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.LimitedPrivate({"Hive"})
public class CertificateLocalizationService extends AbstractService
    implements CertificateLocalization, CertificateLocalizationMBean {
  
  private final Logger LOG = LogManager.getLogger
      (CertificateLocalizationService.class);
  
  private final String SYSTEM_TMP = System.getProperty("java.io.tmpdir",
      "/tmp");
  private final String LOCALIZATION_DIR_NAME = "certLoc";
  private String LOCALIZATION_DIR;
  private Path materializeDir;
  private String superKeystoreLocation;
  private String superKeystorePass;
  private String superTrustStoreLocation;
  private String superTruststorePass;

  private final Map<StorageKey, CryptoMaterial> materialLocation =
      new ConcurrentHashMap<>();
  private final Map<StorageKey, Future<CryptoMaterial>> futures =
      new ConcurrentHashMap<>();
  private final ExecutorService execPool = Executors.newFixedThreadPool(5);
  private Thread eventProcessor = null;
  private volatile boolean stopped = false;
  private ObjectName mbeanObjectName;
  private final ServiceType service;
  public static final String JMX_MATERIALIZED_KEY = "materialized";
  public static final String JMX_FORCE_REMOVE_OP = "forceRemoveMaterial";
  private final ReentrantLock lock = new ReentrantLock(true);
  
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
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOCALIZATION_DIR = service.toString() +  "_" + LOCALIZATION_DIR_NAME;

    parseSuperuserMaterial(conf);
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    materializeDir = Paths.get(SYSTEM_TMP, LOCALIZATION_DIR);
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
    
    if (null != materializeDir) {
      FileUtils.deleteQuietly(materializeDir.toFile());
    }
    
    if (null != eventProcessor) {
      eventProcessor.interrupt();
      stopped = true;
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
      ByteBuffer keyStore, String keyStorePass,
      ByteBuffer trustStore, String trustStorePass) throws IOException {
    materializeCertificates(username, null, userFolder, keyStore, keyStorePass, trustStore, trustStorePass);
  }
  
  @Override
  public void materializeCertificates(String username, String applicationId, String userFolder,
      ByteBuffer keyStore, String keyStorePass,
      ByteBuffer trustStore, String trustStorePass) throws IOException {

    StorageKey key = new StorageKey(username, applicationId);
    CryptoMaterial material = materialLocation.get(key);
    if (null != material) {
      material.incrementRequestedApplications();
      return;
    }
  
    Future<CryptoMaterial> future = execPool.submit(new Materializer(key,
        userFolder, keyStore, keyStorePass, trustStore, trustStorePass));
    futures.put(key, future);
    // Put the CryptoMaterial lazily in the materialLocation map
  
    LOG.debug("Materializing for user " + username + " kstore: " +
        keyStore.capacity() + " tstore: " + trustStore.capacity());
  }
  
  @Override
  public void removeMaterial(String username)
      throws InterruptedException, ExecutionException {
    removeMaterial(username, null);
  }
  
  @Override
  public void removeMaterial(String username, String applicationId)
      throws InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username, applicationId);
    CryptoMaterial material = null;
  
    Future<CryptoMaterial> future = futures.remove(key);
    if (future != null) {
      material = future.get();
    } else {
      material = materialLocation.get(key);
    }
  
    if (null == material) {
      LOG.warn("Certificates do not exists for user " + username);
      return;
    }
    
    material.decrementRequestedApplications();
    
    if (material.isSafeToRemove()) {
      execPool.execute(new Remover(key, material));
      LOG.debug("Removing crypto material for user " + key.username);
    } else {
      LOG.debug("There are " + material.getRequestedApplications()
          + " applications using the crypto material. " +
          "They will not be removed now!");
    }
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(String username)
      throws FileNotFoundException, InterruptedException, ExecutionException {
    return getMaterialLocation(username, null);
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username, applicationId);
  
    CryptoMaterial material = null;
    Future<CryptoMaterial> future = futures.remove(key);
  
    // There is an async operation for this username
    if (future != null) {
      material = future.get();
    } else {
      // Materialization has already been finished
      material = materialLocation.get(key);
    }
  
    if (material == null) {
      throw new FileNotFoundException("Materialized crypto material could not" +
          " be found for user " + username);
    }
  
    return material;
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
    CryptoMaterial cryptoMaterial = null;
    Future<CryptoMaterial> future = futures.remove(key);
    if (future != null) {
      cryptoMaterial = future.get();
    } else {
      cryptoMaterial = materialLocation.get(key);
    }
    
    if (cryptoMaterial == null) {
      return false;
    }
    
    Remover remover = new Remover(key, cryptoMaterial);
    remover.run();
    return true;
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
  }
  
  private class Materializer implements Callable<CryptoMaterial> {
    private final StorageKey key;
    private final String userFolder;
    private final ByteBuffer kstore;
    private final String kstorePass;
    private final ByteBuffer tstore;
    private final String tstorePass;
    
    private Materializer(StorageKey key, String userFolder, ByteBuffer kstore, String kstorePass,
        ByteBuffer tstore, String tstorePass) {
      this.key = key;
      this.userFolder = userFolder;
      this.kstore = kstore;
      this.kstorePass = kstorePass;
      this.tstore = tstore;
      this.tstorePass = tstorePass;
    }
    
    @Override
    public CryptoMaterial call() throws IOException {

      Path appDirPath;
      if (key.applicationId != null) {
        appDirPath = Paths.get(materializeDir.toString(), userFolder, key.applicationId);
      } else {
        appDirPath = Paths.get(materializeDir.toString(), userFolder);
      }
      
      File appDir = appDirPath.toFile();
      if (!appDir.exists()) {
        appDir.mkdirs();
      }

      Path kStorePath = Paths.get(appDir.getAbsolutePath(),
          key.username + HopsSSLSocketFactory.KEYSTORE_SUFFIX);
      Path tStorePath = Paths.get(appDir.getAbsolutePath(),
          key.username + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX);
      Path passwdPath = Paths.get(appDir.getAbsolutePath(),
          key.username + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX);

      FileChannel kstoreChannel = new FileOutputStream(kStorePath.toFile(), false)
          .getChannel();
      kstoreChannel.write(kstore);
      kstore.rewind(); // Set the position to 0 so other clients can read the buffer later on
      kstoreChannel.close();
      FileChannel tstoreChannel = new FileOutputStream(tStorePath.toFile(), false)
          .getChannel();
      tstoreChannel.write(tstore);
      tstore.rewind(); // Set the position to 0 so other clients can read the buffer later on
      tstoreChannel.close();
      FileUtils.writeStringToFile(passwdPath.toFile(), kstorePass);

      if (service == ServiceType.NM) {
        Set<PosixFilePermission> materialPermissions =
            EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
        PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE);

        Files.setPosixFilePermissions(appDirPath, materialPermissions);
        Files.setPosixFilePermissions(kStorePath, materialPermissions);
        Files.setPosixFilePermissions(tStorePath, materialPermissions);
        Files.setPosixFilePermissions(passwdPath, materialPermissions);
      }

      CryptoMaterial material = new CryptoMaterial(appDirPath.toString(),
          kStorePath.toString(), tStorePath.toString(), passwdPath.toString(),
          kstore, kstorePass, tstore, tstorePass);
      try {
        // We lock to get a consistent state if JMX getState is called at the same time
        lock.lock();
        materialLocation.put(key, material);
      } finally {
        lock.unlock();
      }
      futures.remove(key);

      return material;
    }
  }
  
  private class Remover implements Runnable {
    private final StorageKey key;
    private final CryptoMaterial material;
    
    private Remover(StorageKey key, CryptoMaterial material) {
      this.key = key;
      this.material = material;
    }
    
    @Override
    public void run() {
      File appDir = new File(material.getCertFolder());
      FileUtils.deleteQuietly(appDir);
      try {
        lock.lock();
        materialLocation.remove(key);
      } finally {
        lock.unlock();
      }
    }
  }
}
