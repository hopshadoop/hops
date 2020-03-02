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

import io.hops.security.HopsFileBasedKeyStoresFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.envVars.EnvironmentVariables;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

class RpcTLSUtils {
  
  protected static TestProtos.EchoResponseProto makeEchoRequest(UserGroupInformation ugi, InetSocketAddress
      serverAddress,
      Configuration conf, String message) throws Exception {
    return ugi.doAs(
        new ParameterizedPrivilegedExceptionAction<TestProtos.EchoResponseProto>(serverAddress, conf, message) {
          @Override
          public TestProtos.EchoResponseProto run() throws Exception {
            TestRpcBase.TestRpcService proxy = TestRpcBase.getClient(this.serverAddress, this.conf);
            try {
              TestProtos.EchoRequestProto request = TestProtos.EchoRequestProto.newBuilder().setMessage(message)
                  .build();
              TestProtos.EchoResponseProto response = proxy.echo(null, request);
              return response;
            } finally {
              if (proxy != null) {
                RPC.stopProxy(proxy);
              }
            }
          }
        });
  }
  
  public static TestCryptoMaterial setupTLSMaterial(Configuration conf, TLSSetup tlsSetup, Class callingClass)
      throws GeneralSecurityException, IOException {
    // Generate Server certificate
    KeyPair serverKeyPair = KeyStoreTestUtil.generateKeyPair(tlsSetup.keyAlgorithm);
    X509Certificate serverCert = KeyStoreTestUtil.generateCertificate("CN=Server", serverKeyPair, 60,
        tlsSetup.signatureAlgorithm);
  
    // Create server keystore and truststore
    KeyStoreTestUtil.createKeyStore(tlsSetup.serverKstore.toString(), tlsSetup.serverStorePassword, "server",
        serverKeyPair.getPrivate(), serverCert);
    KeyStoreTestUtil.createTrustStore(tlsSetup.serverTstore.toString(), tlsSetup.serverStorePassword, "server",
        serverCert);
  
    FileUtils.writeStringToFile(tlsSetup.serverStorePasswordLocation.toFile(), tlsSetup.serverStorePassword);
    
    // Generate client certificate signed by server
    KeyPair clientKeyPair = KeyStoreTestUtil.generateKeyPair(tlsSetup.keyAlgorithm);
    X509Certificate clientCert = KeyStoreTestUtil.generateSignedCertificate("CN=" + tlsSetup.clientUserName,
        clientKeyPair, 30, tlsSetup.signatureAlgorithm, serverKeyPair.getPrivate(), serverCert);
  
    // Create client keystore and truststore
    KeyStoreTestUtil.createKeyStore(tlsSetup.clientKstore.toString(), tlsSetup.clientStorePassword, "client",
        clientKeyPair.getPrivate(), clientCert);
    Map<String, X509Certificate> clientTrustedCerts = new HashMap<>(2);
    clientTrustedCerts.put("client", clientCert);
    clientTrustedCerts.put("server", serverCert);
    KeyStoreTestUtil.createTrustStore(tlsSetup.clientTstore.toString(), tlsSetup.clientStorePassword, clientTrustedCerts);
  
    FileUtils.writeStringToFile(tlsSetup.clientPasswordLocation.toFile(), tlsSetup.clientStorePassword);
  
    conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "org.apache.hadoop.net.HopsSSLSocketFactory");
    conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    String superUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + superUser, "*");
    conf.set(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, HopsFileBasedKeyStoresFactory.class.getCanonicalName());
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, tlsSetup.serverKstore.getParent().toString());
    
    return new TestCryptoMaterial(serverKeyPair, serverCert, clientKeyPair, clientCert);
  }
  
  public static class TLSSetup {
    private final String keyAlgorithm;
    private final String signatureAlgorithm;
    
    private final Path serverKstore;
    private final Path serverTstore;
    private final Path serverStorePasswordLocation;
    private final String serverStorePassword;
    
    private final Path clientKstore;
    private final Path clientTstore;
    private final Path clientPasswordLocation;
    private final String clientStorePassword;
    
    private final String clientUserName;
    
    private TLSSetup(Builder builder) {
      this.serverKstore = builder.serverKstore;
      this.serverTstore = builder.serverTstore;
      this.serverStorePassword = builder.serverStorePassword;
      this.serverStorePasswordLocation = builder.serverStorePasswordLocation;
      
      this.clientKstore = builder.clientKstore;
      this.clientTstore = builder.clientTstore;
      this.clientPasswordLocation = builder.clientPasswordLocation;
      this.clientStorePassword = builder.clientStorePassword;
      
      this.clientUserName = builder.clientUserName;
      this.keyAlgorithm = builder.keyAlgorithm;
      this.signatureAlgorithm = builder.signatureAlgorithm;
    }
  
    public Path getServerKstore() {
      return serverKstore;
    }
  
    public Path getServerTstore() {
      return serverTstore;
    }
  
    public Path getClientKstore() {
      return clientKstore;
    }
  
    public Path getClientTstore() {
      return clientTstore;
    }
    
    public String getClientUserName() {
      return clientUserName;
    }
  
    /**
     * Builder section
     */
    public static class Builder {
      private String keyAlgorithm;
      private String signatureAlgorithm;
      
      private Path serverKstore;
      private Path serverTstore;
      private Path serverStorePasswordLocation;
      private String serverStorePassword;
      
      private Path clientKstore;
      private Path clientTstore;
      private Path clientPasswordLocation;
      private String clientStorePassword;
      
      private String clientUserName;
      
      public Builder() {
      }
      
      public Builder setKeyAlgorithm(String keyAlgorithm) {
        this.keyAlgorithm = keyAlgorithm;
        return this;
      }
      
      public Builder setSignatureAlgorithm(String signatureAlgorithm) {
        this.signatureAlgorithm = signatureAlgorithm;
        return this;
      }
      
      public Builder setServerKstore(Path serverKstore) {
        this.serverKstore = serverKstore;
        return this;
      }
      
      public Builder setServerTstore(Path serverTstore) {
        this.serverTstore = serverTstore;
        return this;
      }
      
      public Builder setServerStorePasswordLocation(Path serverStorePasswordLocation) {
        this.serverStorePasswordLocation = serverStorePasswordLocation;
        return this;
      }
      
      public Builder setClientKstore(Path clientKstore) {
        this.clientKstore = clientKstore;
        return this;
      }
      
      public Builder setClientTstore(Path clientTstore) {
        this.clientTstore = clientTstore;
        return this;
      }
      
      public Builder setClientUserName(String clientUserName) {
        this.clientUserName = clientUserName;
        return this;
      }
      
      public Builder setServerStorePassword(String serverStorePassword) {
        this.serverStorePassword = serverStorePassword;
        return this;
      }
      
      public Builder setClientPasswordLocation(Path clientPasswordLocation) {
        this.clientPasswordLocation = clientPasswordLocation;
        return this;
      }
      
      public Builder setClientStorePassword(String clientStorePassword) {
        this.clientStorePassword = clientStorePassword;
        return this;
      }
      
      public TLSSetup build() {
        return new TLSSetup(this);
      }
    }
  }
  
  private static abstract class ParameterizedPrivilegedExceptionAction<T> implements PrivilegedExceptionAction<T> {
    public final InetSocketAddress serverAddress;
    public final Configuration conf;
    public final String message;
    
    private ParameterizedPrivilegedExceptionAction(InetSocketAddress serverAddress, Configuration conf,
        String message) {
      this.serverAddress = serverAddress;
      this.conf = conf;
      this.message = message;
    }
  }
  
  static class TestCryptoMaterial {
    private final KeyPair serverKeyPair;
    private final X509Certificate serverCertificate;
    private final KeyPair clientKeyPair;
    private final X509Certificate clientCertificate;
  
    public TestCryptoMaterial(KeyPair serverKeyPair, X509Certificate serverCertificate, KeyPair clientKeyPair,
        X509Certificate clientCertificate) {
      this.serverKeyPair = serverKeyPair;
      this.serverCertificate = serverCertificate;
      this.clientKeyPair = clientKeyPair;
      this.clientCertificate = clientCertificate;
    }
  
    public KeyPair getServerKeyPair() {
      return serverKeyPair;
    }
  
    public X509Certificate getServerCertificate() {
      return serverCertificate;
    }
  
    public KeyPair getClientKeyPair() {
      return clientKeyPair;
    }
  
    public X509Certificate getClientCertificate() {
      return clientCertificate;
    }
  }
  
  protected static class MockEnvironmentVariables implements EnvironmentVariables {
    
    private final Map<String, String> envs;
    
    protected MockEnvironmentVariables() {
      envs = new HashMap<>();
    }
    
    protected void setEnv(String name, String value) {
      envs.put(name, value);
    }
    
    @Override
    public String getEnv(String variableName) {
      return envs.get(variableName);
    }
  }
}
