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
package org.apache.hadoop.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.ssl.CRLFetcherFactory;
import org.apache.hadoop.security.ssl.CRLValidatorFactory;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;

public class TestDFSStartupWithCRL {
  private static final String BASE_DIR = Paths.get(
      System.getProperty("test.build.dir", Paths.get("target", "test-dir").toString()),
      TestDFSStartupWithCRL.class.getSimpleName())
      .toString();
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  
  private final String keyAlgorithm = "RSA";
  private final String signatureAlgorithm = "SHA256withRSA";
  private final String password = "password";
  
  private static String confDir = null;
  
  private Configuration conf;
  private MiniDFSCluster cluster;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    BASE_DIR_FILE.mkdirs();
    confDir = KeyStoreTestUtil.getClasspathDir(TestDFSStartupWithCRL.class);
  }
  
  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    String testDataPath = System
        .getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "build/test/data");
    File testDataCluster1 = new File(testDataPath, "dfs_cluster");
    String c1Path = testDataCluster1.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
    CRLValidatorFactory.getInstance().clearCache();
    CRLFetcherFactory.getInstance().clearFetcherCache();
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
    File sslServerConf = Paths.get(confDir, TestDFSStartupWithCRL.class.getSimpleName() + ".ssl-server.xml").toFile();
    if (sslServerConf.exists()) {
      sslServerConf.delete();
    }
  }
  
  @Test(timeout = 20000)
  public void testDFSStartup() throws Exception {
    String hostname = NetUtils.getLocalCanonicalHostname();
    Path keyStore = Paths.get(BASE_DIR, hostname + "__kstore.jks");
    Path trustStore = Paths.get(BASE_DIR, hostname + "__tstore.jks");
    Path sslServerConfPath = Paths.get(confDir, TestDFSStartupWithCRL.class.getSimpleName() + ".ssl-server.xml");
    Path inputCRLPath = Paths.get(BASE_DIR, "input.crl.pem");
    Path fetchedCRLPath = Paths.get(BASE_DIR, "fetched.crl.pem");
  
    // Generate server certificate
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(keyAlgorithm);
    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=" + hostname, keyPair, 10, signatureAlgorithm);
  
    // Create server keystore and truststore
    KeyStoreTestUtil.createKeyStore(keyStore.toString(), password, "server", keyPair.getPrivate(), cert);
    KeyStoreTestUtil.createTrustStore(trustStore.toString(), password, "server", cert);
    
    // Generate CRL
    X509CRL crl = KeyStoreTestUtil.generateCRL(cert, keyPair.getPrivate(), signatureAlgorithm, null, null);
    FileWriter fw = new FileWriter(inputCRLPath.toFile(), false);
    PemWriter pw = new PemWriter(fw);
    pw.writeObject(new JcaMiscPEMGenerator(crl));
    pw.flush();
    fw.flush();
    pw.close();
    fw.close();
  
    // RPC TLS with CRL configuration
    conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "org.apache.hadoop.net.HopsSSLSocketFactory");
    conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    String superUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + superUser, "*");
    conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2,TLSv1.1");
    conf.set(HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL.getValue(), "TLSv1.2");
  
    Configuration sslServerConf = KeyStoreTestUtil.createServerSSLConfig(keyStore.toString(), password,
        password, trustStore.toString(), password, "");
    KeyStoreTestUtil.saveConfig(sslServerConfPath.toFile(), sslServerConf);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, TestDFSStartupWithCRL.class.getSimpleName() + ".ssl-server.xml");
  
    conf.setBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY, true);
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, "org.apache.hadoop.security.ssl.RemoteCRLFetcher");
    conf.set(CommonConfigurationKeysPublic.HOPS_CRL_FETCHER_INTERVAL_KEY, "1s");
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "file://" + inputCRLPath.toString());
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, fetchedCRLPath.toString());
    
    // Start MiniDFS cluster
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    Assert.assertEquals(1, cluster.getDataNodes().size());
  }
}
