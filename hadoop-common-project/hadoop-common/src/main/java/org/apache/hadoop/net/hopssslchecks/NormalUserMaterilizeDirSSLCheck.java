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
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Checks for valid crypto material in Hopsworks materialize directory.
 * Hopsworks CertificateMaterializer should have materilized them in the configured directory.
 * It succeeds when HopsSSLSocketFactory is called by a client from Hopsworks
 */
public class NormalUserMaterilizeDirSSLCheck extends AbstractHopsSSLCheck {
  private final static Log LOG = LogFactory.getLog(NormalUserMaterilizeDirSSLCheck.class);
  
  public NormalUserMaterilizeDirSSLCheck() {
    super(95);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization)
      throws IOException {
    String username = ugi.getUserName();
    if (username.matches(HopsSSLSocketFactory.USERNAME_PATTERN)
      || !proxySuperUsers.contains(username)) {
      
      String hopsworksMaterializeDir = configuration.get(
          HopsSSLSocketFactory.CryptoKeys.CLIENT_MATERIALIZE_DIR.getValue(),
          HopsSSLSocketFactory.CryptoKeys.CLIENT_MATERIALIZE_DIR.getDefaultValue());
      
      // Client from Hopsworks is trying to create a SecureSocket
      // The crypto material should be in hopsworksMaterializeDir
      File fd = Paths.get(hopsworksMaterializeDir, username + HopsSSLSocketFactory.KEYSTORE_SUFFIX).toFile();
      if (fd.exists()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Crypto material found in " + hopsworksMaterializeDir);
        }
  
  
        // That is a special case when RM and Hopsworks co-locate in the same machine
        // during development. In that special case, although the material would exist
        // in the materialized directory - Hopsworks has taken care of this - the
        // CertificateLocalizationService of RM would have a reference too. If that is
        // the case prefer the later than reading files.
        String password;
        String passwordFileLocation;
        if (certificateLocalization != null) {
          try {
            X509SecurityMaterial material = certificateLocalization.getX509MaterialLocation(username);
            password = material.getKeyStorePass();
            passwordFileLocation = material.getPasswdLocation().toString();
          } catch (InterruptedException ex) {
            throw new IOException(ex);
          }
        } else {
          File passwdFile = Paths.get(hopsworksMaterializeDir, username + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX)
              .toFile();
          password = HopsUtil.readCryptoMaterialPassword(passwdFile);
          passwordFileLocation = passwdFile.toString();
        }
  
        return new HopsSSLCryptoMaterial(
            Paths.get(hopsworksMaterializeDir, username + HopsSSLSocketFactory.KEYSTORE_SUFFIX).toString(),
            password,
            password,
            Paths.get(hopsworksMaterializeDir, username + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX).toString(),
            password, passwordFileLocation, true);
      }
    }
    
    return null;
  }
}
