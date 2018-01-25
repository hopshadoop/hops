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
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Checks for crypto material information provided by the CertificateLocalizationService of RM or NM
 * It succeeds when HopsSSLSocketFactory is called from RM or NM for a normal user.
 */
public class NormalUserCertLocServiceHopsSSLCheck extends AbstractHopsSSLCheck {
  public NormalUserCertLocServiceHopsSSLCheck() {
    super(90);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(String username, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization)
      throws IOException, SSLMaterialAlreadyConfiguredException {
    if (username.matches(HopsSSLSocketFactory.USERNAME_PATTERN)
        || !proxySuperUsers.contains(username)) {
  
      isConfigurationNeededForNormalUser(username, configuration);
      
      if (certificateLocalization != null) {
        try {
          CryptoMaterial material = certificateLocalization.getMaterialLocation(username);
          
          return new HopsSSLCryptoMaterial(
              material.getKeyStoreLocation(),
              material.getKeyStorePass(),
              material.getTrustStoreLocation(),
              material.getTrustStorePass());
        } catch (InterruptedException | ExecutionException ex) {
          throw new IOException(ex);
        }
      }
    }
    
    return null;
  }
}
