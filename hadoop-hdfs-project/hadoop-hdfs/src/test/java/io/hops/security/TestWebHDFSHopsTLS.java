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

import io.hops.common.security.HopsworksFsSecurityActions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.security.cert.X509Certificate;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class TestWebHDFSHopsTLS extends HopsSSLTestUtils {
  private static final Log LOG = LogFactory.getLog(TestWebHDFSHopsTLS.class);
  private static String classpathDir;
  private static Random rand;
  
  private Configuration conf;
  private MiniDFSCluster cluster;
  private UserGroupInformation ugi;
  private Pair<KeyPair, X509Certificate> caMaterial;
  
  public TestWebHDFSHopsTLS() {
    super.error_mode = CERT_ERR.NO_ERROR;
  }
  
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    classpathDir = KeyStoreTestUtil.getClasspathDir(TestWebHDFSHopsTLS.class);
    rand = new Random();
  }
  
  @Before
  public void before() throws Exception {
    conf = WebHdfsTestUtil.createConf();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "HTTPS_ONLY");
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY, true);
    ugi = UserGroupInformation.createRemoteUser("project__user");
    caMaterial = generateCAMaterial("CN=CARoot");
    
    filesToPurge = prepareCryptoMaterial(classpathDir, caMaterial);
    setCryptoConfig(conf, classpathDir);
    conf.set(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
    conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, true);
    
    conf.set(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY, "io.hops.security.TestingFsSecurityActions");
    
    String testDataPath = System.getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "build/test/data");
    File testDataCluster = new File(testDataPath, "dfs_cluster");
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataCluster.getAbsolutePath());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }
  
  @After
  public void after() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    HopsSecurityActionsFactory.getInstance().clear();
  }
  
  @Test
  public void testOps() throws Exception {
    prepareFS();
    
    // Generate certificates for user project__name
    Pair<KeyPair, X509Certificate> clientMaterial = createClientCertificate("CN=" + ugi.getUserName());
  
    java.nio.file.Path c_keystore = Paths.get(classpathDir, ugi.getUserName() + "_kstore.jks");
    java.nio.file.Path c_truststore = Paths.get(classpathDir, ugi.getUserName() + "_tstore.jks");
    filesToPurge.add(c_keystore);
    filesToPurge.add(c_truststore);
    KeyStoreTestUtil.createKeyStore(c_keystore.toString(), passwd, passwd, ugi.getUserName(),
        clientMaterial.getFirst().getPrivate(), clientMaterial.getSecond());
    KeyStoreTestUtil.createTrustStore(c_truststore.toString(), passwd, "CARoot", caMaterial.getSecond());
  
    Pair<String, String> keystoresBase64 = readStoresBase64(c_keystore, c_truststore);
    
    TestingFsSecurityActions actor = (TestingFsSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY));
    HopsworksFsSecurityActions.X509CredentialsDTO credentialsDTO = new HopsworksFsSecurityActions.X509CredentialsDTO();
    credentialsDTO.setFileExtension("jks");
    credentialsDTO.setkStore(keystoresBase64.getFirst());
    credentialsDTO.settStore(keystoresBase64.getSecond());
    credentialsDTO.setPassword(passwd);
    
    actor.setX509Credentials(ugi.getUserName(), credentialsDTO);
    
    // Set client configuration
    final Configuration clientConfiguration = new Configuration(conf);
    createClientSSLConf(c_keystore, c_truststore, clientConfiguration);
    
    final Path testPath = new Path("/testfile");
    final byte[] data = new byte[64 * 1024];
    
    rand.nextBytes(data);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(clientConfiguration, WebHdfsConstants.SWEBHDFS_SCHEME);
        FSDataOutputStream out = fs.create(testPath);
        out.write(data, 0, data.length);
        out.hflush();
        out.close();
        TimeUnit.SECONDS.sleep(1);
        Assert.assertTrue(fs.exists(testPath));
        fs.setPermission(testPath, FsPermission.valueOf("-rwxrwxrwx"));
        return null;
      }
    });
    
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(clientConfiguration, WebHdfsConstants.SWEBHDFS_SCHEME);
        byte[] buffer = new byte[data.length];
        FSDataInputStream in = fs.open(testPath);
        in.readFully(buffer);
        return null;
      }
    });
    
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(clientConfiguration, WebHdfsConstants.SWEBHDFS_SCHEME);
        FSDataOutputStream out = fs.append(testPath);
        out.write(data, 0, data.length);
        out.hflush();
        out.close();
        TimeUnit.MILLISECONDS.sleep(500);
        return null;
      }
    });
  }
  
  @Test
  public void testWithMissingClientMaterial() throws Exception {
    prepareFS();
  
    // Generate certificates for user project__name
    Pair<KeyPair, X509Certificate> clientMaterial = createClientCertificate("CN=" + ugi.getUserName());
  
    java.nio.file.Path c_keystore = Paths.get(classpathDir, ugi.getUserName() + "_kstore.jks");
    java.nio.file.Path c_truststore = Paths.get(classpathDir, ugi.getUserName() + "_tstore.jks");
    filesToPurge.add(c_keystore);
    filesToPurge.add(c_truststore);
    KeyStoreTestUtil.createKeyStore(c_keystore.toString(), passwd, passwd, ugi.getUserName(),
        clientMaterial.getFirst().getPrivate(), clientMaterial.getSecond());
    KeyStoreTestUtil.createTrustStore(c_truststore.toString(), passwd, "CARoot", caMaterial.getSecond());
  
    final Configuration clientConfiguration = new Configuration(conf);
    createClientSSLConf(c_keystore, c_truststore, clientConfiguration);
    
    final byte[] data = new byte[64];
    rand.nextBytes(data);
    expectedException.expect(IOException.class);
    expectedException.expectMessage("Could not find X.509 credentials for " + ugi.getUserName());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(clientConfiguration, WebHdfsConstants.SWEBHDFS_SCHEME);
        FSDataOutputStream out = fs.create(new Path("/testfile"));
        out.write(data, 0, data.length);
        out.hflush();
        out.close();
        return null;
      }
    });
  }
  
  @Test
  public void testWrongCN() throws Exception {
    prepareFS();
  
    // Generate certificates for user project__name
    Pair<KeyPair, X509Certificate> clientMaterial = createClientCertificate("CN=" + "WRONG" + ugi.getUserName());
  
    java.nio.file.Path c_keystore = Paths.get(classpathDir, "WRONG" + ugi.getUserName() + "_kstore.jks");
    java.nio.file.Path c_truststore = Paths.get(classpathDir, "WRONG" + ugi.getUserName() + "_tstore.jks");
    filesToPurge.add(c_keystore);
    filesToPurge.add(c_truststore);
    KeyStoreTestUtil.createKeyStore(c_keystore.toString(), passwd, passwd, ugi.getUserName(),
        clientMaterial.getFirst().getPrivate(), clientMaterial.getSecond());
    KeyStoreTestUtil.createTrustStore(c_truststore.toString(), passwd, "CARoot", caMaterial.getSecond());
  
    final Configuration clientConfiguration = new Configuration(conf);
    createClientSSLConf(c_keystore, c_truststore, clientConfiguration);
  
    final byte[] data = new byte[64];
    rand.nextBytes(data);
    expectedException.expect(HopsX509AuthenticationException.class);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(clientConfiguration, WebHdfsConstants.SWEBHDFS_SCHEME);
        FSDataOutputStream out = fs.create(new Path("/testfile"));
        out.write(data, 0, data.length);
        out.hflush();
        out.close();
        return null;
      }
    });
  }
  
  private void prepareFS() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path rootPath = new Path("/");
    fs.setPermission(rootPath, FsPermission.valueOf("-rwxrwxrwx"));
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.addUser(ugi.getUserName());
    dfs.addGroup(ugi.getUserName());
    dfs.addUserToGroup(ugi.getUserName(), ugi.getUserName());
  }
  
  private Pair<KeyPair, X509Certificate> createClientCertificate(String cn) throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
    X509Certificate x509 = KeyStoreTestUtil.generateSignedCertificate(cn, keyPair, 42, SIGN_ALG,
        caMaterial.getFirst().getPrivate(), caMaterial.getSecond());
    return new Pair<>(keyPair, x509);
  }
  
  private Pair<String, String> readStoresBase64(java.nio.file.Path keystore, java.nio.file.Path truststore) throws IOException {
    byte[] keystoreBytes = Files.readAllBytes(keystore);
    byte[] truststoreBytes = Files.readAllBytes(truststore);
    return new Pair<>(Base64.encodeBase64String(keystoreBytes), Base64.encodeBase64String(truststoreBytes));
  }
  
  private void createClientSSLConf(java.nio.file.Path keystore, java.nio.file.Path truststore,
      Configuration clientConf) throws IOException {
    Configuration sslClientConf = KeyStoreTestUtil.createClientSSLConfig(
        keystore.toString(), passwd, passwd, truststore.toString(), passwd, "");
    sslClientConf.set(SSLFactory.SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1");
    
    java.nio.file.Path sslClientPath = Paths.get(classpathDir,
        TestWebHDFSHopsTLS.class.getSimpleName() + ".ssl-client.xml");
    filesToPurge.add(sslClientPath);
    File sslClientFile = new File(sslClientPath.toUri());
    KeyStoreTestUtil.saveConfig(sslClientFile, sslClientConf);
    clientConf.set(SSLFactory.SSL_CLIENT_CONF_KEY, TestWebHDFSHopsTLS.class.getSimpleName() + ".ssl-client.xml");
    clientConf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
  }
}
