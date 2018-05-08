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
import org.apache.hadoop.security.UserGroupInformation;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY, UserGroupInformation.getLoginUser().getUserName());
    certLocSrv = new CertificateLocalizationService(service);
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

  private void verifyMaterialExistOrNot(CertificateLocalizationService certLocSrv,
      String username, String userFolder, String kstorePass, String tstorePass, boolean exist)
      throws Exception {
    
    String certLoc = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath, expectedTPath;
    if (service == CertificateLocalizationService.ServiceType.NM) {
      expectedKPath = Paths.get(certLoc, userFolder, username + "__kstore" +
          ".jks").toString();
      expectedTPath = Paths.get(certLoc, userFolder, username + "__tstore" +
          ".jks").toString();
    } else {
      expectedKPath = Paths.get(certLoc, username, username + "__kstore.jks").toString();
      expectedTPath = Paths.get(certLoc, username, username + "__tstore.jks").toString();
    }

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

  private void materializeCertificateUtil(CertificateLocalizationService certLocSrv, String username,
      String userFolder, ByteBuffer bfk, String keyStorePass, ByteBuffer bft, String trustStorePass) throws IOException {

    if (service == CertificateLocalizationService.ServiceType.NM) {
      certLocSrv.materializeCertificates(username, userFolder, bfk, keyStorePass, bft, trustStorePass);
    } else {
      certLocSrv.materializeCertificates(username, username, bfk, keyStorePass, bft, trustStorePass);
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

    certLocSrv.removeMaterial(username);
    // Deletion is asynchronous so we have to wait
    TimeUnit.MILLISECONDS.sleep(10);
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass, trustStorePass, false);
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
    CryptoMaterial cryptoMaterial = certLocSrv.getMaterialLocation(username);

    assertTrue(bfk.equals(cryptoMaterial.getKeyStoreMem()));
    assertTrue(bft.equals(cryptoMaterial.getTrustStoreMem()));

    // Read twice to make sure reads are idempotent
    assertTrue(bfk.equals(cryptoMaterial.getKeyStoreMem()));
    assertTrue(bft.equals(cryptoMaterial.getTrustStoreMem()));

    // Cleanup
    certLocSrv.removeMaterial(username);
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
    CryptoMaterial cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(2, cryptoMaterial.getRequestedApplications());
    
    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(1, cryptoMaterial.getRequestedApplications());
    assertFalse(cryptoMaterial.isSafeToRemove());

    // Check that the certificates are still materialized
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass0, trustStorePass0, true);

    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    
    // Check that the certificates have been cleaned up
    verifyMaterialExistOrNot(certLocSrv, username, userFolder, keyStorePass0, trustStorePass0, false);

    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation(username);
  }
  
  @Test
  public void testMaterialNotFound() throws Exception {
    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation("username");
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
    String userB = "userB";
    String userFolderB = "userFolderB";

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    String jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    Map<String, String> returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    Map<String, Integer> materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userA));
    assertEquals(1, materializedMap.get(userA).intValue());

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

    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    // Wait a bit, it's an async operation
    TimeUnit.MILLISECONDS.sleep(300);
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(1, materializedMap.size());
    assertTrue(materializedMap.containsKey(userA));
    assertEquals(1, materializedMap.get(userA).intValue());
  
    // Now the reference counter should be 2
    materializeCertificateUtil(certLocSrv, userA, userFolderA, keyStore, "some_pass", trustStore, "some_pass");
    jmxReturn = (String) mbs.getAttribute(mxbean, "State");
    returnMap = parseCertificateLocalizerState(jmxReturn, objectMapper);
    materializedMap = getMaterializedStateOutOfString(returnMap, objectMapper);
    assertEquals(2, materializedMap.get(userA).intValue());
    
    // Materialize another user
    String userB = "userB";
    String userFolderB = "userFolderB";
    materializeCertificateUtil(certLocSrv, userB, userFolderB, keyStore, "some_pass", trustStore, "some_pass");
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
