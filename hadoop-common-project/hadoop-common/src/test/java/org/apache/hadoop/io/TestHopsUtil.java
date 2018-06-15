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
package org.apache.hadoop.io;

import io.hops.security.HopsUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestHopsUtil {
  private static final Log LOG = LogFactory.getLog(TestHopsUtil.class);
  private static final String BASE_DIR = Paths.get(System.getProperty("test.build.dir",
      Paths.get("target", "test-dir").toString()),
      TestHopsUtil.class.getSimpleName()).toString();
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  private static String CLASSPATH;
  private File sslClientFile;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    BASE_DIR_FILE.mkdirs();
    CLASSPATH = KeyStoreTestUtil.getClasspathDir(TestHopsUtil.class);
  }
  
  @After
  public void afterTest() throws Exception {
    if (sslClientFile != null) {
      sslClientFile.delete();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
  }
  
  @Test
  public void testGenerateContainerSSLServer() throws Exception {
    Configuration systemConf = new Configuration(false);
    Map<String, String> expected = new HashMap<>();
    File passwdFile = Paths.get(BASE_DIR_FILE.getAbsolutePath(),
        HopsSSLSocketFactory.LOCALIZED_PASSWD_FILE_NAME).toFile();
    String password = "password";
    FileUtils.writeStringToFile(passwdFile, password);
    
    String keyStorePasswordKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY);
    expected.put(keyStorePasswordKey, password);
    systemConf.set(keyStorePasswordKey, password);
    
    String keyStoreKeyPasswordKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY);
    expected.put(keyStoreKeyPasswordKey, password);
    systemConf.set(keyStoreKeyPasswordKey, password);
    
    sslClientFile = Paths.get(CLASSPATH, "ssl-client.xml").toFile();
    Configuration sslClientConf = new Configuration(false);
    
    String keyStoreReloadIntevalKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.CLIENT,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY);
    String keyStoreReloadIntervalValue = "400";
    expected.put(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY), keyStoreReloadIntervalValue);
    sslClientConf.set(keyStoreReloadIntevalKey, keyStoreReloadIntervalValue);
    
    String keyStoreReloadUnitKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.CLIENT,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY);
    String keyStoreReloadUnitValue = "d";
    expected.put(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY), keyStoreReloadUnitValue);
    sslClientConf.set(keyStoreReloadUnitKey, keyStoreReloadUnitValue);
    
    String trustStorePasswordKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY);
    expected.put(trustStorePasswordKey, password);
    systemConf.set(trustStorePasswordKey, password);
    
    String trustStoreReloadIntervalKey = FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.CLIENT,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY);
    String trustStoreReloadIntervalValue = "3000";
    expected.put(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), trustStoreReloadIntervalValue);
    sslClientConf.set(trustStoreReloadIntervalKey, trustStoreReloadIntervalValue);
    
    try (FileWriter fw = new FileWriter(sslClientFile, false)) {
      sslClientConf.writeXml(fw);
    }
    
    HopsUtil.generateContainerSSLServerConfiguration(passwdFile, systemConf);
    File sslServerConf = Paths.get(BASE_DIR_FILE.getAbsolutePath(), "ssl-server.xml").toFile();
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(new FileInputStream(sslServerConf));
    
    assertSSLConfValues(expected, sslConf);
  }
  
  @Test
  public void testGenerateContainerSSLServerConfDefaults() throws IOException {
    Configuration systemConf = new Configuration(false);
    File passwdFile = Paths.get(BASE_DIR_FILE.getAbsolutePath(), HopsSSLSocketFactory.LOCALIZED_PASSWD_FILE_NAME).toFile();
    String password = "password";
    FileUtils.writeStringToFile(passwdFile, password);
    HopsUtil.generateContainerSSLServerConfiguration(passwdFile, systemConf);
    File sslServerConf = Paths.get(BASE_DIR_FILE.getAbsolutePath(), "ssl-server.xml").toFile();
    Assert.assertTrue(sslServerConf.exists());
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(new FileInputStream(sslServerConf));
    
    Map<String, String> expected = new HashMap<>();
    expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
            .SSL_KEYSTORE_LOCATION_TPL_KEY),
        HopsSSLSocketFactory.LOCALIZED_KEYSTORE_FILE_NAME);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_TRUSTSTORE_LOCATION_TPL_KEY),
       HopsSSLSocketFactory.LOCALIZED_TRUSTSTORE_FILE_NAME);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_KEYSTORE_PASSWORD_TPL_KEY), password);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_KEYSTORE_KEYPASSWORD_TPL_KEY), password);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_TRUSTSTORE_PASSWORD_TPL_KEY), password);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY),
       String.valueOf(FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL));
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
       FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY),
       FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT);
   expected.put(FileBasedKeyStoresFactory.resolvePropertyName(
       SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory
           .SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
       String.valueOf(FileBasedKeyStoresFactory
           .DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL));
   
    assertSSLConfValues(expected, sslConf);
  }
  
  @Test
  public void testGenerateContainerSSLServerConfWithDifferentName() throws IOException {
    Configuration systemConf = new Configuration(false);
    File passwdFile = Paths.get(BASE_DIR_FILE.getAbsolutePath(), HopsSSLSocketFactory.LOCALIZED_PASSWD_FILE_NAME)
        .toFile();
    FileUtils.writeStringToFile(passwdFile, "password");
    systemConf.set(SSLFactory.SSL_SERVER_CONF_KEY, "client-ssl-server.xml");
    systemConf.set(SSLFactory.SSL_CLIENT_CONF_KEY, "client-ssl-client.xml");
    sslClientFile = Paths.get(CLASSPATH, "client-ssl-client.xml").toFile();
    Configuration sslClientSSLConf = new Configuration(false);
    sslClientSSLConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.CLIENT,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY), "500");
    try (FileWriter fw = new FileWriter(sslClientFile, false)) {
      sslClientSSLConf.writeXml(fw);
    }
    
    HopsUtil.generateContainerSSLServerConfiguration(passwdFile, systemConf);
    Map<String, String> expected = new HashMap<>();
    expected.put(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY), "500");
    File sslServerConf = Paths.get(BASE_DIR_FILE.getAbsolutePath(), "client-ssl-server.xml").toFile();
    Assert.assertTrue(sslServerConf.exists());
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(new FileInputStream(sslServerConf));
    
    assertSSLConfValues(expected, sslConf);
  }
  
  private void assertSSLConfValues(Map<String, String> expected,
      Configuration conf) {
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      String key = entry.getKey();
      LOG.info("Asserting key: " + key);
      Assert.assertEquals(entry.getValue(), conf.get(key));
    }
  }
}
