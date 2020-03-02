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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.X509Certificate;


public class TestHopsX509Authenticator {
  private Configuration conf;
  private HopsX509AuthenticatorFactory authFactory;
  
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  
  @Before
  public void before() {
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    authFactory = HopsX509AuthenticatorFactory.getInstance(conf);
  }
  
  @After
  public void after() {
    if (authFactory != null) {
      authFactory.clearFactory();
    }
  }
  
  @Test
  public void TestAuthenticatedNormalUser() throws Exception {
    String o = "application_id";
    X509Certificate clientCertificate = generateX509Certificate("CN=bob, O=" + o);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
    InetAddress remoteAddress = InetAddress.getLocalHost();
    
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
    Assert.assertEquals(o, ugi.getApplicationId());
  }
  
  @Test
  public void TestAuthenticatedNormalUserWebHDFS() throws Exception {
    String o = "application_id";
    X509Certificate clientCertificate = generateX509Certificate("CN=bob, O=" + o);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
    InetAddress remoteAddress = InetAddress.getLocalHost();
    
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress, "WebHDFS");
    Assert.assertNull(ugi.getApplicationId());
  }
  
  @Test
  public void TestNotAuthenticatedNormalUser() throws Exception {
    X509Certificate clientCertificate = generateX509Certificate("CN=bob");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("trudy");
    InetAddress remoteAddress = InetAddress.getLocalHost();
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    expectedException.expect(HopsX509AuthenticationException.class);
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
  }
  
  // In Hops super user's X.509 CN contains the FQDN/hostname of the machine and L her username
  @Test
  public void TestAuthenticatedSuperUser() throws Exception {
    InetAddress remoteAddress = InetAddress.getLocalHost();
    String o = "application_id";
    X509Certificate clientCertificate = generateX509Certificate("CN=" + remoteAddress.getCanonicalHostName()
      + ", O=" + o + ", L=alice");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
    Assert.assertEquals(o, ugi.getApplicationId());
  }
  
  @Test
  public void TestNotAuthenticatedSuperUser() throws Exception {
    InetAddress remoteAddress = InetAddress.getLocalHost();
    X509Certificate clientCertificate = generateX509Certificate("CN=i_hope_this_is_not_routable,L=real_super");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("chuck");
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    expectedException.expect(HopsX509AuthenticationException.class);
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
  }

  @Test
  public void TestNotAuthenticatedSuperUserWithResolvableCN() throws Exception {
    InetAddress remoteAddress = InetAddress.getLocalHost();
    X509Certificate clientCertificate = generateX509Certificate("CN=" + remoteAddress.getCanonicalHostName() +
            ", L=real_super");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("chuck");
    HopsX509Authenticator authenticator = authFactory.getAuthenticator();
    expectedException.expect(HopsX509AuthenticationException.class);
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
  }
  @Test
  public void TestFQDNCache() throws Exception {
    InetAddress remoteAddress = InetAddress.getLocalHost();
    X509Certificate clientCertificate = generateX509Certificate("CN=" + remoteAddress.getCanonicalHostName()
      + ",L=alice");
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    CustomHopsX509Authenticator authenticator = new CustomHopsX509Authenticator(conf);
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
    // First time, it should not have been cached
    Assert.assertFalse(authenticator.iscached);
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress);
    // Second time, is should have been cached
    Assert.assertTrue(authenticator.iscached);
  }
  
  private X509Certificate generateX509Certificate(String subjectDN) throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    return KeyStoreTestUtil.generateCertificate(subjectDN, keyPair, 30, "SHA1withRSA");
  }
  
  private class CustomHopsX509Authenticator extends HopsX509Authenticator {
  
    private boolean iscached = false;
    
    CustomHopsX509Authenticator(Configuration conf) {
      super(conf);
    }
    
    @Override
    protected InetAddress isTrustedFQDN(String fqdn) {
      InetAddress address = super.isTrustedFQDN(fqdn);
      iscached = address != null;
      return address;
    }
  }
}
