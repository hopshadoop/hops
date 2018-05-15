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

import java.io.File;
import java.io.IOException;
import java.util.Set;

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
    File localizedKeystore = new File(HopsSSLSocketFactory.LOCALIZED_KEYSTORE_FILE_NAME);
    if (localizedKeystore.exists()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Crypto material found in container localized directory");
      }
      File passwordFile = new File(HopsSSLSocketFactory.LOCALIZED_PASSWD_FILE_NAME);
      String password = HopsUtil.readCryptoMaterialPassword(passwordFile);
      return new HopsSSLCryptoMaterial(HopsSSLSocketFactory.LOCALIZED_KEYSTORE_FILE_NAME, password, password,
          HopsSSLSocketFactory.LOCALIZED_TRUSTSTORE_FILE_NAME, password, HopsSSLSocketFactory
          .LOCALIZED_PASSWD_FILE_NAME, true);
    }
    return null;
  }
}
