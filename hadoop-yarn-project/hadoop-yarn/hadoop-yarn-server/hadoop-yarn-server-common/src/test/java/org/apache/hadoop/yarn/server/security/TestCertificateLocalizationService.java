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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestCertificateLocalizationService {
  
  private final Logger LOG = LogManager.getLogger
      (TestCertificateLocalizationService.class);
  private CertificateLocalizationService certLocSrv;
  private Configuration conf;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    certLocSrv = new CertificateLocalizationService(false, "test0");
    certLocSrv.serviceInit(conf);
    certLocSrv.serviceStart();
  }
  
  @After
  public void tearDown() throws Exception {
    if (null != certLocSrv) {
      certLocSrv.serviceStop();
      File fd = certLocSrv.getMaterializeDirectory().toFile();
      assertFalse(fd.exists());
    }
  }
  
  private void configure(String currentRMId, Configuration conf) {
    conf.set(YarnConfiguration.RM_HA_ID, currentRMId);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1,rm2");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm0", "0.0.0.0:8812");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm1", "0.0.0.0:8813");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm2", "0.0.0.0:8814");
  }
  
  private void verifyMaterialExistOrNot(CertificateLocalizationService certLocSrv,
      String username, String kstorePass, String tstorePass, boolean exist)
      throws Exception {
    
    String certLoc = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(certLoc, username, username + "__kstore" +
        ".jks").toString();
    String expectedTPath = Paths.get(certLoc, username, username + "__tstore" +
        ".jks").toString();
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
    
    if (exist) {
      CryptoMaterial material = certLocSrv.getMaterialLocation(username);
      assertEquals(expectedKPath, material.getKeyStoreLocation());
      assertEquals(expectedTPath, material.getTrustStoreLocation());
      assertTrue(kfd.exists());
      assertTrue(tfd.exists());
      assertEquals(kstorePass, material.getKeyStorePass());
      assertEquals(tstorePass, material.getTrustStorePass());
    } else {
      CryptoMaterial material = null;
      try {
        material = certLocSrv.getMaterialLocation(username);
      } catch (FileNotFoundException ex) {
        LOG.info("Exception here is normal");
        assertNull(material);
        assertFalse(kfd.exists());
        assertFalse(tfd.exists());
      }
    }
  }
  
  @Test(timeout = 10000)
  public void testMaterialSyncService() throws Exception {
    // Stop the Service started without HA
    if (null != certLocSrv) {
      certLocSrv.serviceStop();
    }
    boolean exist = true;
    boolean doesNotExist = false;
    
    CertificateLocalizationService certSyncLeader = new CertificateLocalizationService
        (true, "test1");
    configure("rm0", conf);
    certSyncLeader.serviceInit(conf);
    certSyncLeader.serviceStart();
    certSyncLeader.transitionToActive();
    
    Configuration conf2 = new Configuration();
    CertificateLocalizationService certSyncSlave = new
        CertificateLocalizationService(true, "test2");
    configure("rm1", conf2);
    certSyncSlave.serviceInit(conf2);
    certSyncSlave.serviceStart();
    certSyncSlave.transitionToStandby();
  
    Configuration conf3 = new Configuration();
    configure("rm2", conf3);
    CertificateLocalizationService certSyncSlave2 = new
        CertificateLocalizationService(true, "test3");
    certSyncSlave2.serviceInit(conf3);
    certSyncSlave2.serviceStart();
    certSyncSlave2.transitionToStandby();
    
    String username = "Dr.Who";
    ByteBuffer kstore = ByteBuffer.wrap("some bytes".getBytes());
    ByteBuffer tstore = ByteBuffer.wrap("some bytes".getBytes());
    String kstorePass = "kstorePass";
    String tstorePass = "tstorePass";
    
    certSyncLeader.materializeCertificates(username, kstore, kstorePass, tstore,
        tstorePass);
    
    TimeUnit.SECONDS.sleep(2);
    
    verifyMaterialExistOrNot(certSyncSlave, username, kstorePass, tstorePass,
        exist);
    verifyMaterialExistOrNot(certSyncSlave2, username, kstorePass,
        tstorePass, exist);
    
    certSyncLeader.removeMaterial(username);
    
    TimeUnit.SECONDS.sleep(2);
    
    verifyMaterialExistOrNot(certSyncSlave, username, kstorePass,
        tstorePass, doesNotExist);
    verifyMaterialExistOrNot(certSyncSlave2, username, kstorePass, tstorePass,
        doesNotExist);
    
    LOG.info("Switching roles");
    // Switch roles
    certSyncSlave.transitionToActive();
    certSyncLeader.transitionToStandby();
    
    username = "Amy";
    kstorePass = "kstorePass2";
    tstorePass = "tstorePass2";
    certSyncSlave.materializeCertificates(username, kstore, kstorePass, tstore,
        tstorePass);
    
    TimeUnit.SECONDS.sleep(2);
    verifyMaterialExistOrNot(certSyncLeader, username, kstorePass,
        tstorePass, exist);
    verifyMaterialExistOrNot(certSyncSlave2, username, kstorePass,
        tstorePass, exist);
    
    certSyncSlave.removeMaterial(username);
    TimeUnit.SECONDS.sleep(2);
    
    verifyMaterialExistOrNot(certSyncLeader, username, kstorePass, tstorePass,
        doesNotExist);
    verifyMaterialExistOrNot(certSyncSlave2, username, kstorePass, tstorePass,
        doesNotExist);
    
    certSyncSlave.serviceStop();
    certSyncLeader.serviceStop();
    certSyncSlave2.serviceStop();
  }
  
  @Test
  public void testLocalizationDirectory() {
    File tmpDir = certLocSrv.getTmpDir();
    assertTrue(tmpDir.exists());
    assertTrue(tmpDir.canWrite());
    assertTrue(tmpDir.canExecute());
    assertFalse(tmpDir.canRead());
    
    Path materializeDir = certLocSrv.getMaterializeDirectory();
    File fd = materializeDir.toFile();
    assertTrue(fd.exists());
    assertTrue(fd.canWrite());
    assertTrue(fd.canExecute());
    assertTrue(fd.canRead());
  }
  
  @Test
  public void testMaterialization() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String applicationId = "tardis";
    String keyStorePass = "keyStorePass";
    String trustStorePass = "trustStorePass";
    
    certLocSrv.materializeCertificates(username, bfk, keyStorePass, bft, trustStorePass);
  
    CryptoMaterial cryptoMaterial = certLocSrv
        .getMaterialLocation(username);
    String materializeDir = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(materializeDir, username, username
        + "__kstore.jks").toString();
    String expectedTPath = Paths.get(materializeDir, username, username
        + "__tstore.jks").toString();
    
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    assertEquals(keyStorePass, cryptoMaterial.getKeyStorePass());
    assertEquals(trustStorePass, cryptoMaterial.getTrustStorePass());
    
    certLocSrv.removeMaterial(username);
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
  
    // Deletion is asynchronous so we have to wait
    TimeUnit.MILLISECONDS.sleep(10);
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
  }
  
  @Test
  public void testMaterializationWithMultipleApplications() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String applicationId = "tardis";
    String keyStorePass0 = "keyStorePass0";
    String trustStorePass0 = "trustStorePass0";
  
    certLocSrv.materializeCertificates(username, bfk, keyStorePass0, bft,
        trustStorePass0);
  
    CryptoMaterial cryptoMaterial = certLocSrv
        .getMaterialLocation(username);
    String materializeDir = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(materializeDir, username, username
        + "__kstore.jks").toString();
    String expectedTPath = Paths.get(materializeDir, username, username
        + "__tstore.jks").toString();
  
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
  
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    
    // Make a second materialize certificates call which happen when a second
    // application is launched
    certLocSrv.materializeCertificates(username, bfk, keyStorePass0, bft,
        trustStorePass0);
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(2, cryptoMaterial.getRequestedApplications());
    
    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(1, cryptoMaterial.getRequestedApplications());
    assertFalse(cryptoMaterial.isSafeToRemove());
  
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
    assertTrue(kfd.exists());
    assertTrue(tfd.exists());
  
    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    
    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation(username);
    
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
  }
  
  @Test
  public void testMaterialNotFound() throws Exception {
    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation("username");
  }
  
  @Test
  public void testMXBeanForceRemove() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbean = new ObjectName("Hadoop:service=test0,name=CertificateLocalizer");
    ObjectMapper objectMapper = new ObjectMapper();
    // Materialize crypto material for a user
    byte[] someBytes = "some_bytes".getBytes();
    ByteBuffer keyStore = ByteBuffer.wrap(someBytes);
    ByteBuffer trustStore = ByteBuffer.wrap(someBytes);
    String userA = "userA";
    String userB = "userB";
  
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    String jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    Map<String, String> returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    Map<String, Integer> materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userA));
    assertEquals(1, materializedMap.get(userA).intValue());
  
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    invokeJMXRemoveOperation(mbs, mxbean, userA);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(0, materializedMap.size());
  
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    
    // Materialize for another user
    certLocSrv.materializeCertificates(userB, keyStore, "another_pass", trustStore, "another_pass");
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
    assertFalse(materializedMap.containsKey(userA));
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
    ObjectName mxbean = new ObjectName("Hadoop:service=test0,name=CertificateLocalizer");
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
    
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userA));
    assertEquals(1, materializedMap.get(userA).intValue());
  
    // Now the reference counter should be 2
    certLocSrv.materializeCertificates(userA, keyStore, "some_pass", trustStore, "some_pass");
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.get(userA).intValue());
    
    // Materialize another user
    String userB = "userB";
    certLocSrv.materializeCertificates(userB, keyStore, "another_pass", trustStore, "another_pass");
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.size());
    assertTrue(materializedMap.containsKey(userA));
    assertTrue(materializedMap.containsKey(userB));
    assertEquals(2, materializedMap.get(userA).intValue());
    assertEquals(1, materializedMap.get(userB).intValue());
    
    // Remove userA. It should be called twice since it was requested two times
    certLocSrv.removeMaterial(userA);
    certLocSrv.removeMaterial(userA);
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertFalse(materializedMap.containsKey(userA));
    assertTrue(materializedMap.containsKey(userB));
    assertEquals(1, materializedMap.get(userB).intValue());
    
    // Remove userB
    certLocSrv.removeMaterial(userB);
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
