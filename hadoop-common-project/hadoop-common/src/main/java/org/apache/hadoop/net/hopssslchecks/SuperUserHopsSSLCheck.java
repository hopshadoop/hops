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

import io.hops.security.HopsUtil;
import io.hops.security.SuperuserKeystoresLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import io.hops.security.CertificateLocalization;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Checks for valid crypto material information in the case of a proxy superuser.
 * Superusers are defined in core-site.xml
 * It succeeds when HopsSSLSocketFactory is called and the current user is a superuser
 */
public class SuperUserHopsSSLCheck extends AbstractHopsSSLCheck {
  private final static Log LOG = LogFactory.getLog(SuperUserHopsSSLCheck.class);
  
  public SuperUserHopsSSLCheck() {
    super(-1);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization) throws IOException {
  
    String username = ugi.getUserName();
    if (proxySuperUsers.contains(username)) {
      try {
        isConfigurationNeededForSuperUser(username, configuration);
      } catch (SSLMaterialAlreadyConfiguredException ex) {
        // Already configured, return
        return new HopsSSLCryptoMaterial(
            configuration.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()),
            configuration.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()),
            configuration.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()),
            configuration.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()),
            configuration.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
      }
    
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found crypto material with the hostname");
      }
    
      if (certificateLocalization != null) {
        return new HopsSSLCryptoMaterial(
            certificateLocalization.getSuperKeystoreLocation(),
            certificateLocalization.getSuperKeystorePass(),
            certificateLocalization.getSuperKeyPassword(),
            certificateLocalization.getSuperTruststoreLocation(),
            certificateLocalization.getSuperTruststorePass(),
            certificateLocalization.getSuperMaterialPasswordFile(), true);
      }
    
      if (LOG.isDebugEnabled()) {
        LOG.debug("*** Called setTlsConfiguration for superuser but CertificateLocalization is NULL");
      }
    
      return getSuperuserMaterialFromFile(configuration);
    }
  
    return null;
  }
  
  private HopsSSLCryptoMaterial getSuperuserMaterialFromFile(Configuration conf) throws IOException {
    SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    X509SecurityMaterial material = loader.loadSuperUserMaterial();
    if (!fileExists(material.getKeyStoreLocation()) || !fileExists(material.getTrustStoreLocation())
      || !fileExists(material.getPasswdLocation())) {
      throw new IOException("Could not load Keystore/Truststore/Password file from "
          + material.getKeyStoreLocation().getParent() + " . Check your permissions or configuration");
    }
    String password = HopsUtil.readCryptoMaterialPassword(material.getPasswdLocation().toFile());
    return new HopsSSLCryptoMaterial(
        material.getKeyStoreLocation().toString(),
        password,
        password,
        material.getTrustStoreLocation().toString(),
        password,
        material.getPasswdLocation().toString(),
        true);
  }
  
  private boolean fileExists(Path path2file) {
    return path2file.toFile().exists();
  }
}
