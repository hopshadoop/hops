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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import io.hops.security.CertificateLocalization;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Checks for valid crypto material in the directory specified by the environment variable
 * HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR
 */
public class EnvVariableHopsSSLCheck extends AbstractHopsSSLCheck {
  public EnvVariableHopsSSLCheck() {
    super(110);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization) throws IOException {
    
    String cryptoMaterialDir = EnvironmentVariablesFactory.getInstance().getEnv(HopsSSLSocketFactory
        .CRYPTO_MATERIAL_ENV_VAR);
    
    if (cryptoMaterialDir != null) {
      String username = ugi.getUserName();
      File keystoreFd = Paths.get(cryptoMaterialDir, username + HopsSSLSocketFactory.KEYSTORE_SUFFIX).toFile();
      File trustStoreFd = Paths.get(cryptoMaterialDir, username + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX).toFile();
      File passwordFd = Paths.get(cryptoMaterialDir, username + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX).toFile();
      
      if (!keystoreFd.exists() || !trustStoreFd.exists()) {
        throw new IOException("Crypto material for user <" + username + "> could not be found in " + cryptoMaterialDir);
      }
      
      String password = HopsUtil.readCryptoMaterialPassword(passwordFd);
      return new HopsSSLCryptoMaterial(keystoreFd.getAbsolutePath(), password, password, trustStoreFd.getAbsolutePath(),
          password, passwordFd.getAbsolutePath(), true);
    }
    
    return null;
  }
}
