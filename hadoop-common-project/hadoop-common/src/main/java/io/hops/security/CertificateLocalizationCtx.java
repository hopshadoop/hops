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
import org.apache.hadoop.security.authorize.ProxyUsers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CertificateLocalizationCtx {
  private static volatile CertificateLocalizationCtx instance = null;
  private CertificateLocalization certificateLocalization = null;
  private Set<String> proxySuperusers = null;
  
  private CertificateLocalizationCtx() {
  }
  
  public static CertificateLocalizationCtx getInstance() {
    if (instance == null) {
      synchronized (CertificateLocalizationCtx.class) {
        if (instance == null) {
          instance = new CertificateLocalizationCtx();
        }
      }
    }
    return instance;
  }
  
  public synchronized void setCertificateLocalization
      (CertificateLocalization certificateLocalization) {
    this.certificateLocalization = certificateLocalization;
  }
  
  public CertificateLocalization getCertificateLocalization() {
    return certificateLocalization;
  }
  
  public synchronized void setProxySuperusers(Configuration conf) {
    if (this.proxySuperusers == null) {
      this.proxySuperusers = getSuperusersFromConf(conf);
    }
  }
  
  public Set<String> getProxySuperusers() {
    return proxySuperusers;
  }
  
  private Set<String> getSuperusersFromConf(Configuration conf) {
    Set<String> superusers = new HashSet<>();
    // Get the superusers
    for (Map.Entry<String, String> entry : conf) {
      String propName = entry.getKey();
      if (propName.startsWith(ProxyUsers.CONF_HADOOP_PROXYUSER)) {
        String[] tokens = propName.split("\\.");
        // Configuration property is in the form of hadoop.proxyuser.USERNAME.{hosts,groups}
        superusers.add(tokens[2]);
      }
    }
    return superusers;
  }
}
