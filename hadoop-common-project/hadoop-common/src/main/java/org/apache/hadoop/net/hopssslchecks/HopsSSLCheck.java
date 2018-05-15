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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;

import java.io.IOException;
import java.util.Set;

/**
 * Interface for HopsSSLSocketFactory auto-configuration for cryptographic material
 */
public interface HopsSSLCheck {
  /**
   * Method that checks for proper keystore, truststore depending the username of the current user.
   *
   * @param ugi Current user username
   * @param proxySuperUsers Set of proxy superusers defined in core-site.xml
   * @param configuration Hadoop configuration
   * @param certificateLocalization If in ResourceManager or NodeManager, a CertificateLocalizationService reference,
   *                               otherwise null
   * @return HopsSSLCryptoMaterial if able to pass the tests and find proper material, null otherwise.
   * @throws IOException
   */
  HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization)
      throws IOException;
  Integer getPriority();
}
