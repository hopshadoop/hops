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
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;


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

  @Test
  public void TestMultipleResolvedAddresses() throws UnknownHostException, HopsX509AuthenticationException {
    HopsX509Authenticator realAuth = new HopsX509Authenticator(conf);
    HopsX509Authenticator auth = Mockito.spy(realAuth);
    String cn = "example.hopsworks.ai";
    Set<InetAddress> mockAddresses = new HashSet<>();
    byte[] ipAddr1 = new byte[]{10, 0, 1, 1};
    InetAddress addr1 = InetAddress.getByAddress(ipAddr1);
    mockAddresses.add(addr1);

    byte[] ipAddr2 = new byte[]{10, 0, 1, 2};
    InetAddress addr2 = InetAddress.getByAddress(ipAddr2);
    mockAddresses.add(addr2);

    byte[] ipAddr3 = new byte[]{10, 0, 1, 3};
    InetAddress addr3 = InetAddress.getByAddress(ipAddr3);
    mockAddresses.add(addr3);

    Mockito.doReturn(mockAddresses).when(auth).getAllInetAddressesAsSet(Mockito.matches(cn));

    boolean authResult = auth.isTrustedConnection(addr2, cn, "alice", "alice");
    Assert.assertTrue(authResult);

    byte[] wrongIpAddr = new byte[]{10, 0, 2, 1};
    InetAddress wrongAddr = InetAddress.getByAddress(wrongIpAddr);
    authResult = auth.isTrustedConnection(wrongAddr, cn, "alice", "alice");
    Assert.assertFalse(authResult);
  }

  @Test
  public void TestSimpleAuthentication() throws UnknownHostException, HopsX509AuthenticationException {
    Configuration conf = new Configuration(true);
    conf.set(CommonConfigurationKeys.HOPS_RPC_AUTH_MODE, "SIMPLE");
    HopsX509Authenticator realAuth = new HopsX509Authenticator(conf);
    HopsX509Authenticator auth = Mockito.spy(realAuth);
    String cn = "example.hopsworks.ai";
    String unresolvableDomainName = "unresolvable.domain.name";
    Set<InetAddress> mockAddresses = new HashSet<>();
    byte[] ipAddr1 = new byte[]{10, 0, 1, 1};
    InetAddress addr1 = InetAddress.getByAddress(ipAddr1);
    mockAddresses.add(addr1);

    Mockito.doReturn(mockAddresses).when(auth).getAllInetAddressesAsSet(Mockito.matches(cn));
    Mockito.doThrow(new UnknownHostException(cn))
        .when(auth).getAllInetAddressesAsSet(unresolvableDomainName);

    byte[] clientIP = new byte[]{10, 1, 0, 1};
    boolean authResult = auth.isTrustedConnection(InetAddress.getByAddress(clientIP), cn, "alice", "alice");
    Assert.assertTrue(authResult);

    expectedException.expect(HopsX509AuthenticationException.class);
    auth.isTrustedConnection(InetAddress.getByAddress(clientIP), unresolvableDomainName, "alice", "alice");
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
    protected Set<InetAddress> isTrustedFQDN(String fqdn) {
      Set<InetAddress> addresses = super.isTrustedFQDN(fqdn);
      iscached = addresses != null;
      return addresses;
    }
  }
}
