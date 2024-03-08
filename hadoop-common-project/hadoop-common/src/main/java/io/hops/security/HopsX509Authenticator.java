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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HopsX509Authenticator {
  private static final Log LOG = LogFactory.getLog(HopsX509Authenticator.class);
  
  private final Configuration conf;
  private final Cache<String, Set<InetAddress>> trustedHostnames;
  
  public HopsX509Authenticator(Configuration conf) {
    this.conf = conf;
    trustedHostnames = CacheBuilder.newBuilder()
        .maximumSize(500)
        .expireAfterWrite(30, TimeUnit.MINUTES)
        .build();
  }
  
  public void authenticateConnection(UserGroupInformation user, X509Certificate clientCertificate,
      InetAddress remoteAddress) throws HopsX509AuthenticationException {
    authenticateConnection(user, clientCertificate, remoteAddress, null);
  }
  
  public void authenticateConnection(UserGroupInformation user, X509Certificate clientCertificate,
      InetAddress remoteAddress, String protocolName) throws HopsX509AuthenticationException {
    if (!isHopsTLS()) {
      return;
    }
    
    Preconditions.checkNotNull(user, "UserGroupInformation should not be null");
    Preconditions.checkNotNull(clientCertificate, "Client X.509 certificate should not be null");
    
    LOG.debug("Authenticating user: " + user.getUserName());
    String username = user.getUserName();
    if (username == null) {
      throw new HopsX509AuthenticationException("Could not extract username from UGI");
    }
    String subjectDN = clientCertificate.getSubjectX500Principal().getName("RFC2253");
    String cn = HopsUtil.extractCNFromSubject(subjectDN);
    if (cn == null) {
      throw new HopsX509AuthenticationException("Problematic CN in client certificate: " + subjectDN);
    }
    
    // Hops X.509 certificates use O field for ApplicationID
    String org = HopsUtil.extractOFromSubject(subjectDN);
    if (org != null && (Strings.isNullOrEmpty(protocolName) || !protocolName.equalsIgnoreCase("WebHDFS"))) {
      user.addApplicationId(org);
    }
    
    if (username.equals(cn)) {
      LOG.debug("Authenticated user " + username + " - Username matches CN");
      return;
    }
    
    // CN could also be the machine FQDN.
    // These certificates will be used by Hops services RM, NM, NN, DN
    // Assume that if a domain name is resolvable and matches the incoming
    // IP address the connection is trusted
    Preconditions.checkNotNull(remoteAddress, "Remote address should not be null");
    // In the case of system users certificates the L field of the Subject is the username
    String locality = HopsUtil.extractLFromSubject(subjectDN);
    if (locality == null) {
      throw new HopsX509AuthenticationException("Incoming RPC claims to be a from a system user but the Locality (L) " +
              "field of its X.509 is null or cannot be parsed");
    }
    if (isTrustedConnection(remoteAddress, cn, username, locality)) {
      return;
    }

    // Could not authenticate incoming connection
    StringBuilder sb = new StringBuilder();
    sb.append("Could not authenticate client with CN ")
        .append(cn)
        .append(" remote IP ")
        .append(remoteAddress)
        .append(" and username ")
        .append(username);
    if (protocolName != null) {
      sb.append(" for protocol ").append(protocolName);
    }
    
    throw new HopsX509AuthenticationException(sb.toString());
  }

  /**
   * Used in Hive
   *
   * @param remoteAddress
   * @param cnFQDN
   * @return
   * @throws HopsX509AuthenticationException
   */
  public boolean isTrustedConnection(InetAddress remoteAddress, String cnFQDN) throws HopsX509AuthenticationException {
    return isTrustedConnection(remoteAddress, cnFQDN, null, null);
  }

  public boolean isTrustedConnection(InetAddress remoteAddress, String cnFQDN, String username, String locality)
    throws HopsX509AuthenticationException {
    Set<InetAddress> addresses = isTrustedFQDN(cnFQDN);
    if (addresses != null && isTrustedConnectionInternal(remoteAddress, addresses, username, locality)) {
      LOG.debug("CN " + cnFQDN + " is an FQDN and it has already been authenticated");
      return true;
    }

    try {
      addresses = getAllInetAddressesAsSet(cnFQDN);
      if (isTrustedConnectionInternal(remoteAddress, addresses, username, locality)) {
        trustedHostnames.put(cnFQDN, addresses);
        LOG.debug("CN " + cnFQDN + " is an FQDN and we managed to resolve it and it matches the remote address");
        return true;
      }
    } catch (UnknownHostException ex) {
      LOG.error("Could not resolve host " + cnFQDN, ex);
      throw new HopsX509AuthenticationException("Hostname " + cnFQDN + " is not resolvable and could not authenticate " +
              "user " + username);
    }
    return false;
  }

  @VisibleForTesting
  protected Set<InetAddress> getAllInetAddressesAsSet(String commonName) throws UnknownHostException {
    InetAddress[] addrs = InetAddress.getAllByName(commonName);
    Set<InetAddress> addresses = new HashSet<>(addrs.length);
    addresses.addAll(Arrays.asList(addrs));
    return addresses;
  }

  private boolean isHopsTLS() {
    return conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  }

  private boolean isTrustedConnectionInternal(InetAddress actualAddress, Set<InetAddress> expectedAddresses,
          String expectedUsername, String actualUsername) {
    if (expectedUsername == null && actualUsername == null) {
      return doesAddressMatch(expectedAddresses, actualAddress);
    }
    return doesAddressMatch(expectedAddresses, actualAddress) && doesUsernameMatch(expectedUsername, actualUsername);
  }

  private boolean doesAddressMatch(Set<InetAddress> expected, InetAddress actual) {
    // If both are loopback addresses skip comparing them, one might be 127.0.0.1 and another 127.0.1.1 depending
    // on hosts configuration
    if (isAnyLoopback(expected) && actual.isLoopbackAddress()) {
      return true;
    }
    return expected.contains(actual);
  }

  private boolean isAnyLoopback(Set<InetAddress> addresses) {
    for (InetAddress addr : addresses) {
      if (addr.isLoopbackAddress()) {
        return true;
      }
    }
    return false;
  }

  private boolean doesUsernameMatch(String expected, String actual) {
    return expected.equals(actual);
  }
  @VisibleForTesting
  protected Set<InetAddress> isTrustedFQDN(String fqdn) {
    return trustedHostnames.getIfPresent(fqdn);
  }
}
