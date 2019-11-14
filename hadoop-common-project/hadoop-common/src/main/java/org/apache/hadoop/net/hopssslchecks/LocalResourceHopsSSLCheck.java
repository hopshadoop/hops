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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.hadoop.security.ssl.SSLFactory;

/**
 * Checks for valid crypto material in the current working directory.
 * That is the case when HopsSSLSocketFactory is called from a container
 */
public class LocalResourceHopsSSLCheck extends AbstractHopsSSLCheck {
  private final static Log LOG = LogFactory.getLog(LocalResourceHopsSSLCheck.class);
  
  public LocalResourceHopsSSLCheck() {
    super(100);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperusers, Configuration configuration,
      CertificateLocalization certificateLocalization) throws IOException {
    
    // First check in the configured path
    String configuredKeystorePath = configuration.get(SSLFactory.LOCALIZED_KEYSTORE_FILE_PATH_KEY,
        SSLFactory.DEFAULT_LOCALIZED_KEYSTORE_FILE_PATH);
    String configuredTrustorePath = configuration.get(SSLFactory.LOCALIZED_TRUSTSTORE_FILE_PATH_KEY,
        SSLFactory.DEFAULT_LOCALIZED_TRUSTSTORE_FILE_PATH);
    String configuredPasswordPath = configuration.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY,
        SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH);
    File localizedKeystore = new File(configuredKeystorePath);
    File trustore = new File(configuredTrustorePath);
    File password = new File(configuredPasswordPath);
    if (localizedKeystore.exists() && trustore.exists() && password.exists()) {
      LOG.debug("Crypto material found att configured path");
      return constructCryptoMaterial(localizedKeystore, trustore, password);
    }

    // not in configured path search in CWD
    String keystoreName = Paths.get(configuredKeystorePath).getFileName().toString();
    String trustoreName = Paths.get(configuredTrustorePath).getFileName().toString();
    String passwordName = Paths.get(configuredPasswordPath).getFileName().toString();
    localizedKeystore = new File(keystoreName);
    if (localizedKeystore.exists()) {
      LOG.debug("Crypto material found in container's CWD");
      return constructCryptoMaterial(localizedKeystore, new File(trustoreName),
          new File(passwordName));
    }
    
    // User might have changed directory, use PWD environment variable to construct the full path
    String pwd = EnvironmentVariablesFactory.getInstance().getEnv("PWD");
    if (pwd != null) {
      Path localizedKeystorePath = Paths.get(pwd, keystoreName);
      if (localizedKeystorePath.toFile().exists()) {
        LOG.debug("Crypto material found in PWD");
        return constructCryptoMaterial(localizedKeystorePath.toFile(),
            Paths.get(pwd, trustoreName).toFile(),
            Paths.get(pwd, passwordName).toFile());
      }
    }
    return null;
  }
  
  private HopsSSLCryptoMaterial constructCryptoMaterial(File keystoreLocation, File truststoreLocation,
      File passwordFileLocation) throws IOException {
    String password = HopsUtil.readCryptoMaterialPassword(passwordFileLocation);
    return new HopsSSLCryptoMaterial(keystoreLocation.toString(), password, password, truststoreLocation.toString(),
        password, passwordFileLocation.toString(), true);
  }
}
