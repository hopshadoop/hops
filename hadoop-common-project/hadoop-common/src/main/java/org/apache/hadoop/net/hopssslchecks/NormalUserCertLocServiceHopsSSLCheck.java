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
package org.apache.hadoop.net.hopssslchecks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;

import java.io.IOException;
import java.util.Set;

/**
 * Checks for crypto material information provided by the CertificateLocalizationService of RM or NM
 * It succeeds when HopsSSLSocketFactory is called from RM or NM for a normal user.
 */
public class NormalUserCertLocServiceHopsSSLCheck extends AbstractHopsSSLCheck {
  public NormalUserCertLocServiceHopsSSLCheck() {
    super(105);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization)
      throws IOException {
    String username = ugi.getUserName();
    if (username.matches(HopsSSLSocketFactory.USERNAME_PATTERN)
        || !proxySuperUsers.contains(username)) {
  
      if (certificateLocalization != null) {
        try {
          X509SecurityMaterial material =
              certificateLocalization.getX509MaterialLocation(username, ugi.getApplicationId());
          
          return new HopsSSLCryptoMaterial(
              material.getKeyStoreLocation().toString(),
              material.getKeyStorePass(),
              material.getKeyStorePass(),
              material.getTrustStoreLocation().toString(),
              material.getTrustStorePass(),
              material.getPasswdLocation().toString(),
              true);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }
    }
    
    return null;
  }
}
