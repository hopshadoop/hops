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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

public class HopsX509Authenticator {
  private static final Log LOG = LogFactory.getLog(HopsX509Authenticator.class);
  
  private final Configuration conf;
  private final Cache<InetAddress, String> trustedHostnames;
  
  HopsX509Authenticator(Configuration conf) {
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
    if (org != null) {
      user.addApplicationId(org);
    }
    
    if (username.equals(cn)) {
      LOG.debug("Authenticated user " + username + " - Username matches CN");
      return;
    }
    
    // The CN could also be the machine FQDN.
    // These certificates will be used by Hops services RM, NM, NN, DN
    // Assume that reverse DNS will succeed only for machines that we
    // trust
    // Try reverse DNS of the address to check if the CN matches
    Preconditions.checkNotNull(remoteAddress, "Remote address should not be null");
    String fqdn = isTrustedAddress(remoteAddress);
    if (fqdn != null) {
      LOG.debug("CN " + cn + " is an FQDN and it has already been authenticated");
      return;
    }
    fqdn = remoteAddress.getCanonicalHostName();
    if (cn.equals(fqdn)) {
      trustedHostnames.put(remoteAddress, fqdn);
      LOG.debug("CN " + cn + " is an FQDN and we managed to resolve it and it matches the remote address FQDN");
      return;
    } else {
      // Check also the hostname for backward compatibility
      String hostname = remoteAddress.getHostName();
      if (cn.equals(hostname)) {
        trustedHostnames.put(remoteAddress, hostname);
        LOG.debug("CN " + cn + " is a hostname and we managed to resolve it and it matches the remote address " +
            "Hostname");
        return;
      }
    }
    
    // Could not authenticate incoming connection
    StringBuilder sb = new StringBuilder();
    sb.append("Could not authenticate client with CN ")
        .append(cn)
        .append(" and username ")
        .append(username);
    if (protocolName != null) {
      sb.append(" for protocol ").append(protocolName);
    }
    
    throw new HopsX509AuthenticationException(sb.toString());
  }
  
  private boolean isHopsTLS() {
    return conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  }
  
  @VisibleForTesting
  protected String isTrustedAddress(InetAddress address) {
    return trustedHostnames.getIfPresent(address);
  }
}
