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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;

public class TestTLSRPCServer {
  private static final String BASE_DIR = Paths.get(System.getProperty("test.build.dir",
      Paths.get("target", "test-dir").toString()),
      TestTLSRPCServer.class.getSimpleName()).toString();
  private static final File BASE_DIR_FILE = new File(BASE_DIR);
  private static final int KB = 1024;
  
  private final String keyAlgorithm = "RSA";
  private final String signatureAlgorithm = "SHA256withRSA";
  private final String password = "password";
  
  private static String classPathDir;
  private Configuration conf;
  private Server server;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    BASE_DIR_FILE.mkdirs();
    classPathDir = KeyStoreTestUtil.getClasspathDir(TestTLSRPCServer.class);
  }
  
  @Before
  public void beforeTest() {
    conf = new Configuration();
  }
  
  @After
  public void afterTest() {
    if (server != null) {
      server.stop();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (BASE_DIR_FILE.exists()) {
      FileUtils.deleteDirectory(BASE_DIR_FILE);
    }
  }
  
  @Test
  public void testLargeResponse() throws Exception {
    String clientName = "Alice";
    setupTLSMaterial(clientName);
    
    // Set RPC engine
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class, ProtobufRpcEngine.class);
  
    RpcTLSUtils.MockEnvironmentVariables envs = new RpcTLSUtils.MockEnvironmentVariables();
    envs.setEnv(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, BASE_DIR);
    EnvironmentVariablesFactory.setInstance(envs);
    
    RPC.Builder serverBuilder = TestRpcBase.newServerBuilder(conf)
        .setNumHandlers(1)
        .setSecretManager(null)
        .setnumReaders(2);
    
    server = TestRpcBase.setupTestServer(serverBuilder);
    char[] payload = new char[6 * KB * KB];
    String payloadString = new String(payload);
    UserGroupInformation clientUGI = UserGroupInformation.createRemoteUser(clientName);
    TestProtos.EchoResponseProto response = RpcTLSUtils.makeEchoRequest(clientUGI, server.getListenerAddress(),
        conf, payloadString);
    
    Assert.assertEquals(payloadString, response.getMessage());
  }
  
  private RpcTLSUtils.TLSSetup setupTLSMaterial(String clientName) throws GeneralSecurityException, IOException {
    Path serverKeystore = Paths.get(BASE_DIR, "server.kstore.jks");
    Path serverTruststore = Paths.get(BASE_DIR, "server.tstore.jks");
    Path sslServerConfPath = Paths.get(classPathDir, TestTLSRPCServer.class.getSimpleName() + ".ssl-server.xml");
    
    Path clientKeystore = Paths.get(BASE_DIR, clientName + HopsSSLSocketFactory.KEYSTORE_SUFFIX);
    Path clientTruststore = Paths.get(BASE_DIR, clientName + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX);
    Path clientPasswordLocation = Paths.get(BASE_DIR, clientName + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX);
    
  
    RpcTLSUtils.TLSSetup tlsSetup = new RpcTLSUtils.TLSSetup.Builder()
        .setKeyAlgorithm(keyAlgorithm)
        .setSignatureAlgorithm(signatureAlgorithm)
        .setServerKstore(serverKeystore)
        .setServerTstore(serverTruststore)
        .setServerStorePassword(password)
        .setClientKstore(clientKeystore)
        .setClientTstore(clientTruststore)
        .setClientStorePassword(password)
        .setClientPasswordLocation(clientPasswordLocation)
        .setClientUserName(clientName)
        .setSslServerConf(sslServerConfPath)
        .build();
    
    RpcTLSUtils.setupTLSMaterial(conf, tlsSetup, TestTLSRPCServer.class);
    
  
    conf.setBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY, false);
    
    return tlsSetup;
  }
}
