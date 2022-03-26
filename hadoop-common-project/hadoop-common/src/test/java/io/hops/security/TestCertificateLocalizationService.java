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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.SecurityMaterial;
import org.apache.hadoop.security.ssl.JWTSecurityMaterial;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class TestCertificateLocalizationService {
  
  private static final String BASE_DIR = Paths.get(System.getProperty("test.build.dir",
      Paths.get("target", "test-dir").toString()),
      TestCertificateLocalizationService.class.getSimpleName()).toString();
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  
  @Parameterized.Parameters
  public static Collection parameters() {
    return Arrays.asList(new Object[][] {
        {CertificateLocalizationService.ServiceType.NM},
        {CertificateLocalizationService.ServiceType.RM}
    });
  }

  private final Logger LOG = LogManager.getLogger
      (TestCertificateLocalizationService.class);
  private CertificateLocalizationService certLocSrv;
  private Configuration conf;

  @Parameterized.Parameter
  public CertificateLocalizationService.ServiceType service;

  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @BeforeClass
  public static void beforeAll() throws IOException {
    BASE_DIR_FILE.mkdirs();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(null);
    Path passwdFileLocation = Paths.get(BASE_DIR, loader.getSuperMaterialPasswdFilename(ugi.getUserName()));
    FileUtils.writeStringToFile(passwdFileLocation.toFile(), "password");
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, BASE_DIR);
    certLocSrv = new CertificateLocalizationService(service);
    certLocSrv.serviceInit(conf);
    certLocSrv.serviceStart();
  }
  
  @After
  public void tearDown() throws Exception {
    if (null != certLocSrv) {
      certLocSrv.serviceStop();
    }
  }
  
  @AfterClass
  public static void afterAll() throws IOException {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
  }

  private void verifyMaterialExistOrNot(CertificateLocalizationService certLocSrv,
      String username, String userFolder, String kstorePass, String tstorePass, boolean exist)
      throws Exception {
    
    String certLoc = certLocSrv.getMaterializeDirectory().toString();
    Path expectedKPath, expectedTPath;
    if (service == CertificateLocalizationService.ServiceType.NM) {
      expectedKPath = Paths.get(certLoc, userFolder, username + "__kstore.jks");
      expectedTPath = Paths.get(certLoc, userFolder, username + "__tstore.jks");
    } else {
      expectedKPath = Paths.get(certLoc, username, username + "__kstore.jks");
      expectedTPath = Paths.get(certLoc, username, username + "__tstore.jks");
    }

    if (exist) {
      X509SecurityMaterial material = certLocSrv.getX509MaterialLocation(username);
      assertEquals(expectedKPath, material.getKeyStoreLocation());
      assertEquals(expectedTPath, material.getTrustStoreLocation());
      assertTrue(expectedKPath.toFile().exists());
      assertTrue(expectedTPath.toFile().exists());
      assertEquals(kstorePass, material.getKeyStorePass());
      assertEquals(tstorePass, material.getTrustStorePass());
    } else {
      SecurityMaterial material = null;
      try {
        material = certLocSrv.getX509MaterialLocation(username);
      } catch (FileNotFoundException ex) {
        LOG.info("Exception here is normal");
        assertNull(material);
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(expectedKPath.toFile().exists());
        assertFalse(expectedTPath.toFile().exists());
      }
    }
  }

  private void materializeCertificateUtil(CertificateLocalizationService certLocSrv, String username,
      String userFolder, ByteBuffer bfk, String keyStorePass, ByteBuffer bft, String trustStorePass)
      throws InterruptedException {

    if (service == CertificateLocalizationService.ServiceType.NM) {
      certLocSrv.materializeCertificates(username, userFolder, bfk, keyStorePass, bft, trustStorePass);
    } else {
      certLocSrv.materializeCertificates(username, username, bfk, keyStorePass, bft, trustStorePass);
    }
  }
  
  private void materializeJWTUtil(CertificateLocalizationService certLocSrv, String username, String userFolder,
      String token) throws InterruptedException {
    if (service == CertificateLocalizationService.ServiceType.NM) {
      certLocSrv.materializeJWT(username, null, userFolder, token);
    } else {
      certLocSrv.materializeJWT(username, null, username, token);
    }
  }
  
  @Test
  public void testLocalizationDirectory() throws Exception {
    Path materializeDir = certLocSrv.getMaterializeDirectory();
    Set<PosixFilePermission> permissionSet = Files.getPosixFilePermissions(materializeDir);

    Set<PosixFilePermission> expectedPermissionSet = null;
    if (service == CertificateLocalizationService.ServiceType.NM) {
      expectedPermissionSet = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE);
    } else {
      expectedPermissionSet = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);
    }
    assertTrue(permissionSet.containsAll(expectedPermissionSet));
  }
  
  @Test
  public void testMaterialization() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String userFolder = "userfolder";
    String keyStorePass = "keyStorePass";
    String trustStorePass = "trustStorePass";

    materializeCertificateUtil(certLocSrv, username, userFolder, bfk, keyStorePass, bft, trustStorePass);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass, trustStorePass, true);

    certLocSrv.removeX509Material(username);
    // Deletion is asynchronous so we have to wait
    TimeUnit.MILLISECONDS.sleep(10);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass, trustStorePass, false);
  }
  
  @Test
  public void testJWTMaterialization() throws Exception {
    String jwt = "random_jwt";
    String username = "Dorothy";
    
    certLocSrv.materializeJWT(username, null, username, jwt);
    JWTSecurityMaterial material = certLocSrv.getJWTMaterialLocation(username, null);
    assertEquals(jwt, material.getToken());
    assertTrue(material.getTokenLocation().toFile().exists());
  }
  
  @Test
  public void testJWTDeletion() throws Exception {
    String jwt = "a_token";
    String username = "Dorothy";
    
    certLocSrv.materializeJWT(username, null, username, jwt);
    JWTSecurityMaterial material = certLocSrv.getJWTMaterialLocation(username, null);
    assertTrue(material.getTokenLocation().toFile().exists());
    
    certLocSrv.removeJWTMaterial(username, null);
    
    rule.expect(FileNotFoundException.class);
    JWTSecurityMaterial newMaterial = certLocSrv.getJWTMaterialLocation(username, null);
  }
  
  @Test
  public void testUpdateMaterialNotification() throws Exception {
    if (certLocSrv != null) {
      certLocSrv.stop();
      certLocSrv = null;
    }
    
    DelayedCertificateLocalizationService delayedCertLoc = new DelayedCertificateLocalizationService(service, 1000);
    try {
      delayedCertLoc.init(conf);
      delayedCertLoc.start();
      
      ByteBuffer keyStore = ByteBuffer.wrap("keyStore0".getBytes());
      ByteBuffer trustStore = ByteBuffer.wrap("trustStore0".getBytes());
      String keyStorePassword = "kpassword0";
      String trustStorePassword = "tpassword0";
      String username = "Dolores";
      String userFolder = "userFolder";
      String jwt = "Something that's supposed to be a token";
  
      ByteBuffer newKeyStore = ByteBuffer.wrap("keyStore1".getBytes());
      ByteBuffer newTrustStore = ByteBuffer.wrap("trustStore1".getBytes());
      String newKeyStorePassword = "kpassword1";
      String newTrustStorePassword = "tpassword1";
      String newJWT = "Another token";
      materializeCertificateUtil(delayedCertLoc, username, userFolder, keyStore, keyStorePassword, trustStore, trustStorePassword);
      materializeJWTUtil(delayedCertLoc, username, userFolder, jwt);
      
      delayedCertLoc.updateX509(username, null, newKeyStore, newKeyStorePassword, newTrustStore, newTrustStorePassword);
      delayedCertLoc.updateJWT(username, null, newJWT);
      X509SecurityMaterial material = delayedCertLoc.getX509MaterialLocation(username);
      
      // Make some basic checks here. Extended checks are in testUpdateMaterial
      assertNotEquals(keyStore, material.getKeyStoreMem());
      assertNotEquals(keyStorePassword, material.getKeyStorePass());
      assertEquals(newKeyStore, material.getKeyStoreMem());
      assertEquals(newKeyStorePassword, material.getKeyStorePass());
      
      JWTSecurityMaterial jwtMaterial = delayedCertLoc.getJWTMaterialLocation(username, null);
      assertNotEquals(jwt, jwtMaterial.getToken());
    } finally {
      delayedCertLoc.stop();
    }
  }
  
  @Test
  public void testUpdateMaterial() throws Exception {
    ByteBuffer keyStore = ByteBuffer.wrap("keyStore0".getBytes());
    ByteBuffer trustStore = ByteBuffer.wrap("trustStore0".getBytes());
    String keyStorePassword = "kpassword0";
    String trustStorePassword = "tpassword0";
    String username = "Dolores";
    String userFolder = "userFolder";
    String jwt = "A not very random string";
    
    materializeCertificateUtil(certLocSrv, username, userFolder, keyStore, keyStorePassword,
        trustStore, trustStorePassword);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePassword, trustStorePassword, true);
    materializeJWTUtil(certLocSrv, username, userFolder, jwt);
    
    X509SecurityMaterial oldMaterial = certLocSrv.getX509MaterialLocation(username);
    JWTSecurityMaterial oldJWTMaterial = certLocSrv.getJWTMaterialLocation(username, null);
    assertNotNull(oldJWTMaterial);
    
    ByteBuffer newKeyStore = ByteBuffer.wrap("keyStore1".getBytes());
    ByteBuffer newTrustStore = ByteBuffer.wrap("trustStore1".getBytes());
    String newKeyStorePassword = "kpassword1";
    String newTrustStorePassword = "tpassword1";
    String newJWT = "Again not a very random token";
    certLocSrv.updateX509(username, null, newKeyStore, newKeyStorePassword,
        newTrustStore, newTrustStorePassword);
    certLocSrv.updateJWT(username, null, newJWT);
    
    X509SecurityMaterial newMaterial = certLocSrv.getX509MaterialLocation(username);
    assertEquals(newKeyStore, newMaterial.getKeyStoreMem());
    assertEquals(newTrustStore, newMaterial.getTrustStoreMem());
    assertEquals(newKeyStorePassword, newMaterial.getKeyStorePass());
    assertEquals(newTrustStorePassword, newMaterial.getTrustStorePass());
    assertEquals(oldMaterial.getCertFolder(), newMaterial.getCertFolder());
    assertEquals(oldMaterial.getKeyStoreLocation().toString(), newMaterial.getKeyStoreLocation().toString());
    assertEquals(oldMaterial.getTrustStoreLocation().toString(), newMaterial.getTrustStoreLocation().toString());
    assertEquals(oldMaterial.getPasswdLocation().toString(), newMaterial.getPasswdLocation().toString());
    assertEquals(oldMaterial.getRequestedApplications(), newMaterial.getRequestedApplications());
    assertEquals(SecurityMaterial.STATE.FINISHED, newMaterial.getState());
    
    assertFalse(keyStore.equals(newMaterial.getKeyStoreMem()));
    assertFalse(trustStore.equals(newMaterial.getTrustStoreMem()));
    assertNotEquals(keyStorePassword, newMaterial.getKeyStorePass());
    assertNotEquals(trustStorePassword, newMaterial.getTrustStorePass());
    
    JWTSecurityMaterial newJWTMaterial = certLocSrv.getJWTMaterialLocation(username, null);
    assertEquals(newJWT, newJWTMaterial.getToken());
    assertNotEquals(jwt, newJWTMaterial.getToken());
    assertEquals(oldJWTMaterial.getTokenLocation(), newJWTMaterial.getTokenLocation());
    
    certLocSrv.removeX509Material(username);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, newKeyStorePassword, newTrustStorePassword, false);
  }
  
  @Test
  public void testMaterializationRemoval() throws Exception {
    ByteBuffer store = ByteBuffer.wrap("some_bytes".getBytes());
    String username = "Dolores";
    String userFolder = "userFolder";
    String password = "password";
    materializeCertificateUtil(certLocSrv, username, userFolder, store, password, store, password);
    certLocSrv.removeX509Material(username);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, password, password, false);
  }

  @Test
  public void testMultipleReadsCryptoBuffers() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String userFolder = "userfolder";
    String keyStorePass = "keyStorePass";
    String trustStorePass = "trustStorePass";

    materializeCertificateUtil(certLocSrv, username, userFolder, bfk, keyStorePass, bft, trustStorePass);
    X509SecurityMaterial cryptoMaterial = certLocSrv.getX509MaterialLocation(username);

    assertTrue(bfk.equals(cryptoMaterial.getKeyStoreMem()));
    assertTrue(bft.equals(cryptoMaterial.getTrustStoreMem()));

    // Read twice to make sure reads are idempotent
    assertTrue(bfk.equals(cryptoMaterial.getKeyStoreMem()));
    assertTrue(bft.equals(cryptoMaterial.getTrustStoreMem()));

    // Cleanup
    certLocSrv.removeX509Material(username);
  }



  @Test
  public void testMaterializationWithMultipleApplications() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String userFolder = "userFolder";
    String keyStorePass0 = "keyStorePass0";
    String trustStorePass0 = "trustStorePass0";


    materializeCertificateUtil(certLocSrv, username, userFolder, bfk, keyStorePass0, bft, trustStorePass0);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass0, trustStorePass0, true);

    // Make a second materialize certificates call which happen when a second
    // application is launched
    materializeCertificateUtil(certLocSrv, username, userFolder, bfk, keyStorePass0, bft, trustStorePass0);
    SecurityMaterial cryptoMaterial = certLocSrv.getX509MaterialLocation(username);
    assertEquals(2, cryptoMaterial.getRequestedApplications());
    
    certLocSrv.removeX509Material(username);
    TimeUnit.MILLISECONDS.sleep(10);
    cryptoMaterial = certLocSrv.getX509MaterialLocation(username);
    assertEquals(1, cryptoMaterial.getRequestedApplications());
    assertFalse(cryptoMaterial.isSafeToRemove());

    // Check that the certificates are still materialized
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass0, trustStorePass0, true);

    certLocSrv.removeX509Material(username);
    TimeUnit.MILLISECONDS.sleep(10);
    
    // Check that the certificates have been cleaned up
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass0, trustStorePass0, false);

    rule.expect(FileNotFoundException.class);
    certLocSrv.getX509MaterialLocation(username);
  }
  
  @Test
  public void testMaterialNotFound() throws Exception {
    rule.expect(FileNotFoundException.class);
    certLocSrv.getX509MaterialLocation("username");
  }
  
  @Test
  public void testDelayedMaterializationAndForceRemoval() throws Exception {
    if (certLocSrv != null) {
      certLocSrv.serviceStop();
      certLocSrv = null;
    }
    DelayedCertificateLocalizationService delayedCertLoc = new DelayedCertificateLocalizationService(service, 1000);
    try {
      delayedCertLoc.serviceInit(conf);
      delayedCertLoc.serviceStart();
    
      String username = "username";
      String userFolder = "userFolder";
      String password = "password";
      ByteBuffer byteBuffer = ByteBuffer.wrap("some_bytes".getBytes());
      materializeCertificateUtil(delayedCertLoc, username, userFolder, byteBuffer, password, byteBuffer, password);
      delayedCertLoc.forceRemoveMaterial(username);
      verifyMaterialExistOrNot(delayedCertLoc, username, userFolder, password, password, false);
    
      TimeUnit.MILLISECONDS.sleep(1000);
      assertNotNull(delayedCertLoc.getHasBeenMaterialized());
      assertFalse(delayedCertLoc.getHasBeenMaterialized());
    } finally {
      delayedCertLoc.serviceStop();
    }
  }
  
  private class DelayedCertificateLocalizationService extends CertificateLocalizationService {
  
    private final long sleepInMS;
    private Boolean hasBeenMaterialized = null;
    
    public DelayedCertificateLocalizationService(ServiceType service, long sleepInMS) {
      super(service);
      this.sleepInMS = sleepInMS;
    }
    
    public Boolean getHasBeenMaterialized() {
      return hasBeenMaterialized;
    }
    
    @Override
    protected Thread createLocalizationEventsHandler() {
      return new Thread() {
  
        @Override
        public void run() {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              LocalizationEvent event = dequeue();
              TimeUnit.MILLISECONDS.sleep(sleepInMS);
              if (event instanceof MaterializeEvent) {
                hasBeenMaterialized = materializeInternal((MaterializeEvent) event);
              } else if (event instanceof RemoveEvent) {
                removeInternal((RemoveEvent) event);
              } else if (event instanceof UpdateEvent) {
                updateInternal((UpdateEvent) event);
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          }
        }
      };
    }
  }
  
  @Test
  public void testMXBeanForceRemove() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbean = new ObjectName("Hadoop:service=" + service.toString() +",name=CertificateLocalizer");
    ObjectMapper objectMapper = new ObjectMapper();
    // Materialize crypto material for a user
    byte[] someBytes = "some_bytes".getBytes();
    ByteBuffer keyStore = ByteBuffer.wrap(someBytes);
    ByteBuffer trustStore = ByteBuffer.wrap(someBytes);
    String userA = "userA";
    String userFolderA = "userFolderA";
    String userAStateKey = userA + "/unknown/" + CertificateLocalizationService.STORAGE_KEY_TYPE.X509;
    
    String userB = "userB";
    String userFolderB = "userFolderB";

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    String jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    Map<String, String> returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    Map<String, Integer> materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userAStateKey));
    assertEquals(1, materializedMap.get(userAStateKey).intValue());

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    invokeJMXRemoveOperation(mbs, mxbean, userA);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(0, materializedMap.size());

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");

    // Materialize for another user
    materializeCertificateUtil(certLocSrv, userB, userFolderB, keyStore, "another_pass", trustStore, "another_pass");
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.size());
    
    invokeJMXRemoveOperation(mbs, mxbean, userA);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertFalse(materializedMap.containsKey(userAStateKey));
  }
  
  private boolean invokeJMXRemoveOperation(MBeanServer mbs, ObjectName mxbean, String username) throws Exception {
    MBeanInfo mBeanInfo = mbs.getMBeanInfo(mxbean);
    for (MBeanOperationInfo op : mBeanInfo.getOperations()) {
      if (op.getName().equals(CertificateLocalizationService.JMX_FORCE_REMOVE_OP)) {
        boolean returnValue = (boolean) mbs.invoke(mxbean, CertificateLocalizationService.JMX_FORCE_REMOVE_OP,
            new Object[] {username}, new String[] {String.class.getName()});
        return returnValue;
      }
    }
    return false;
  }
  
  @Test
  public void testMXBeanState() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbean = new ObjectName("Hadoop:service=" + service.toString() +",name=CertificateLocalizer");
    ObjectMapper objectMapper = new ObjectMapper();
    String jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    Map<String, String> returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    assertTrue("Return Map should contain key " + CertificateLocalizationService.JMX_MATERIALIZED_KEY,
        returnMap.containsKey(CertificateLocalizationService.JMX_MATERIALIZED_KEY));
    
    Map<String, Integer> materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertTrue(materializedMap.isEmpty());
    
    // Materialize crypto material for a user
    byte[] someBytes = "some_bytes".getBytes();
    ByteBuffer keyStore = ByteBuffer.wrap(someBytes);
    ByteBuffer trustStore = ByteBuffer.wrap(someBytes);
    String userA = "userA";
    String userFolderA = "userFolderA";
    String userAStateKey = userA + "/unknown/" + CertificateLocalizationService.STORAGE_KEY_TYPE.X509;

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userAStateKey));
    assertEquals(1, materializedMap.get(userAStateKey).intValue());
  
    // Now the reference counter should be 2
    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.get(userAStateKey).intValue());
    
    // Materialize another user
    String userB = "userB";
    String userFolderB = "userFolderB";
    String userBStateKey = userB + "/unknown/" + CertificateLocalizationService.STORAGE_KEY_TYPE.X509;
    materializeCertificateUtil(certLocSrv, userB, userFolderB, keyStore, "some_pass", trustStore, "some_pass");
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.size());
    assertTrue(materializedMap.containsKey(userAStateKey));
    assertTrue(materializedMap.containsKey(userBStateKey));
    assertEquals(2, materializedMap.get(userAStateKey).intValue());
    assertEquals(1, materializedMap.get(userBStateKey).intValue());
    
    // Remove userA. It should be called twice since it was requested two times
    certLocSrv.removeX509Material(userA);
    certLocSrv.removeX509Material(userA);
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertFalse(materializedMap.containsKey(userAStateKey));
    assertTrue(materializedMap.containsKey(userBStateKey));
    assertEquals(1, materializedMap.get(userBStateKey).intValue());
    
    // Remove userB
    certLocSrv.removeX509Material(userB);
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertTrue(materializedMap.isEmpty());
  }
  
  @SuppressWarnings("unchecked")
  private Map<String, String> parseCertificateLocalizerState(String jmxReturn, ObjectMapper objectMapper)
      throws IOException {
    return objectMapper.readValue(jmxReturn, HashMap.class);
  }
  
  @SuppressWarnings("unchecked")
  private Map<String, Integer> getMaterializedStateOutOfString(Map<String, String> state, ObjectMapper objectMapper)
      throws IOException {
    String materializedStr = state.get(CertificateLocalizationService.JMX_MATERIALIZED_KEY);
    return objectMapper.readValue(materializedStr, HashMap.class);
  }
}
