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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestReloadingX509KeyManager {
  private static final String BASE_DIR = GenericTestUtils
      .getTempPath(TestReloadingX509KeyManager.class.getSimpleName());
  private static final File baseDirFile = new File(BASE_DIR);
  
  private final Log LOG = LogFactory.getLog(TestReloadingX509KeyManager.class);
  private final String KEY_PAIR_ALGORITHM = "RSA";
  private final String CERTIFICATE_ALGORITHM = "SHA1withRSA";
  private final String KEYSTORE_PASSWORD = "password";
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @BeforeClass
  public static void setUp() throws IOException {
    FileUtils.deleteDirectory(baseDirFile);
    baseDirFile.mkdirs();
  }
  
  @Before
  public void beforeTest() {
    KeyManagersReloaderThreadPool.getInstance(true).clearListOfTasks();
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    FileUtils.deleteDirectory(baseDirFile);
  }
  
  @Test(timeout = 4000)
  public void testReload() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(KEY_PAIR_ALGORITHM);
    X509Certificate cert1 = KeyStoreTestUtil.generateCertificate("CN=cert1", keyPair, 2, CERTIFICATE_ALGORITHM);
    String keyStoreLocation = Paths.get(BASE_DIR, "testKeystore.jks").toString();
    KeyStoreTestUtil.createKeyStore(keyStoreLocation, KEYSTORE_PASSWORD, "cert1", keyPair.getPrivate(), cert1);
    
    ReloadingX509KeyManager keyManager = new ReloadingX509KeyManager("jks", keyStoreLocation, KEYSTORE_PASSWORD,
        KEYSTORE_PASSWORD, 10, TimeUnit.MILLISECONDS);
    
    try {
      keyManager.init();
  
      TimeUnit reloadTimeUnit = keyManager.getReloadTimeUnit();
      long reloadInterval = keyManager.getReloadInterval();
      
      X509Certificate[] certChain = keyManager.getCertificateChain("cert1");
      assertNotNull("Certificate chain should not be null for alias cert1", certChain);
      assertEquals("Certificate chain should be 1", 1, certChain.length);
      assertEquals("DN for cert1 should be CN=cert1", cert1.getSubjectDN().getName(),
          certChain[0].getSubjectDN().getName());
      
      // Wait a bit for the modification time to be different
      reloadTimeUnit.sleep(reloadInterval);
      TimeUnit.SECONDS.sleep(1);
      
      // Replace keystore with a new one with a different DN
      X509Certificate cert2 = KeyStoreTestUtil.generateCertificate("CN=cert2", keyPair, 2, CERTIFICATE_ALGORITHM);
      KeyStoreTestUtil.createKeyStore(keyStoreLocation, KEYSTORE_PASSWORD, "cert2", keyPair.getPrivate(), cert2);
      
      reloadTimeUnit.sleep(reloadInterval * 2);
      
      certChain = keyManager.getCertificateChain("cert1");
      assertNull("Certificate chain for alias cert1 should be null", certChain);
      certChain = keyManager.getCertificateChain("cert2");
      assertNotNull("Certificate chain should not be null for alias cert2", certChain);
      assertEquals("Certificate chain should be 1", 1, certChain.length);
      assertEquals("DN for cert2 should be CN=cert2", cert2.getSubjectDN().getName(),
          certChain[0].getSubjectDN().getName());
      
    } finally {
      keyManager.stop();
    }
  }
  
  @Test
  public void testLoadMissingKeyStore() throws Exception {
    String keyStoreLocation = Paths.get(BASE_DIR, "testKeystore.jks").toString();
    
    rule.expect(IOException.class);
    ReloadingX509KeyManager keyManager = new ReloadingX509KeyManager("jks", keyStoreLocation, "", "", 10,
        TimeUnit.MILLISECONDS);
    try {
      keyManager.init();
    } finally {
      keyManager.stop();
    }
  }
  
  @Test
  public void testLoadCorruptedKeyStore() throws Exception {
    String keyStoreLocation = Paths.get(BASE_DIR, "corrupterTestKeystore.jks").toString();
    FileOutputStream outputStream = new FileOutputStream(keyStoreLocation);
    outputStream.write("something".getBytes());
    outputStream.close();
    
    rule.expect(IOException.class);
    rule.expectMessage("Invalid keystore format");
    ReloadingX509KeyManager keyManager = new ReloadingX509KeyManager("jks", keyStoreLocation, "", "", 10,
        TimeUnit.MILLISECONDS);
    try {
      keyManager.init();
    } finally {
      keyManager.stop();
    }
  }
  
  @Test(timeout = 4000)
  public void testReloadMissingKeyStore() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(KEY_PAIR_ALGORITHM);
    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=cert", keyPair, 2, CERTIFICATE_ALGORITHM);
    String keyStoreLocation = Paths.get(BASE_DIR, "testKeystore.jks").toString();
    KeyStoreTestUtil.createKeyStore(keyStoreLocation, KEYSTORE_PASSWORD, "cert", keyPair.getPrivate(), cert);
    
    ReloadingX509KeyManager keyManager = new ReloadingX509KeyManager("jks", keyStoreLocation, KEYSTORE_PASSWORD,
        KEYSTORE_PASSWORD, 10, TimeUnit.MILLISECONDS);
    try {
      keyManager.init();
      X509Certificate[] certChain = keyManager.getCertificateChain("cert");
      assertNotNull("Certificate chain should not be null for alias cert", certChain);
      
      // Delete the keystore
      FileUtils.forceDelete(new File(keyStoreLocation));
      
      keyManager.getReloadTimeUnit().sleep(keyManager.getReloadInterval());
      TimeUnit.SECONDS.sleep(1);
  
      AtomicBoolean fileExists = keyManager.getFileExists();
      assertFalse("Key manager should detect file does not exist", fileExists.get());
      
      certChain = keyManager.getCertificateChain("cert");
      assertNotNull("Certificate chain should not be null for alias cert", certChain);
    } finally {
      keyManager.stop();
    }
  }
  
  @Test(timeout = 4000)
  public void testReloadCorruptedKeyStore() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(KEY_PAIR_ALGORITHM);
    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=cert", keyPair, 2, CERTIFICATE_ALGORITHM);
    String keyStoreLocation = Paths.get(BASE_DIR, "testKeystore.jks").toString();
    KeyStoreTestUtil.createKeyStore(keyStoreLocation, KEYSTORE_PASSWORD, "cert", keyPair.getPrivate(), cert);
    
    ReloadingX509KeyManager keyManager = new ReloadingX509KeyManager("jks", keyStoreLocation, KEYSTORE_PASSWORD,
        KEYSTORE_PASSWORD, 10, TimeUnit.MILLISECONDS);
    
    try {
      keyManager.init();
      X509Certificate[] certChain = keyManager.getCertificateChain("cert");
      assertNotNull("Certificate chain should not be null for alias cert", certChain);
      
      keyManager.getReloadTimeUnit().sleep(keyManager.getReloadInterval());
      TimeUnit.SECONDS.sleep(1);
      
      FileOutputStream outputStream = new FileOutputStream(keyStoreLocation);
      outputStream.write("something".getBytes());
      outputStream.close();
  
      keyManager.getReloadTimeUnit().sleep(keyManager.getReloadInterval());
      TimeUnit.SECONDS.sleep(1);
  
      certChain = keyManager.getCertificateChain("cert");
      assertNotNull("Certificate chain should not be null for alias cert", certChain);
      assertEquals("DN for cert should be CN=cert", cert.getSubjectDN().getName(),
          certChain[0].getSubjectDN().getName());
      List<ScheduledFuture> reloadTasks =  KeyManagersReloaderThreadPool.getInstance(true).getListOfTasks();
      // Reloading thread should have been cancelled after unsuccessful reload
      for (ScheduledFuture task : reloadTasks) {
        assertTrue(task.isCancelled());
      }
      assertEquals(KeyManagersReloaderThreadPool.MAX_NUMBER_OF_RETRIES + 1, keyManager.getNumberOfFailures());
    } finally {
      keyManager.stop();
    }
  }
}
