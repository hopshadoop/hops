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
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Abstract class that provides common functionality for all HopsSSLChecks
 */
public abstract class AbstractHopsSSLCheck implements HopsSSLCheck, Comparable<HopsSSLCheck> {
  
  // Priority of the check. High priority checks will run first
  private final Integer priority;
  
  public AbstractHopsSSLCheck(Integer priority) {
    this.priority = priority;
  }
  
  public abstract HopsSSLCryptoMaterial check(UserGroupInformation ugi, Set<String> proxySuperUsers,
      Configuration configuration, CertificateLocalization certificateLocalization)
      throws IOException;
  
  @Override
  public Integer getPriority() {
    return priority;
  }
  
  /**
   * HopsSSLCheck object with wither priority should be ordered first
   */
  @Override
  public int compareTo(HopsSSLCheck hopsSSLCheck) {
    if (priority < hopsSSLCheck.getPriority()) {
      return 1;
    }
    if (priority > hopsSSLCheck.getPriority()) {
      return -1;
    }
    return 0;
  }

  /**
   * Checks if the RPC TLS properties of the supplied configuration are already configured for proxy superusers
   * @param username Username of the current superuser
   * @param hostname Hostname of the local machine
   * @param configuration Hadoop configuration
   * @throws SSLMaterialAlreadyConfiguredException If the supplied Hadoop configuration is already configured
   */
  protected void isConfigurationNeededForSuperUser(String username, String hostname, Configuration configuration)
    throws SSLMaterialAlreadyConfiguredException {
    if (isCryptoMaterialSet(configuration, username)
        || isHostnameInCryptoMaterial(hostname, configuration)
        && !configuration.getBoolean(HopsSSLSocketFactory.FORCE_CONFIGURE,
        HopsSSLSocketFactory.DEFAULT_FORCE_CONFIGURE)) {
      throw new SSLMaterialAlreadyConfiguredException("Crypto material for user <" + username + "> has already been" +
          " configured");
    }
  }
  
  /**
   * Reads cryptographic material configuration from ssl-server.xml
   * @param configuration Hadoop configuration
   * @return HopsSSLCryptoMaterial object with the values read from ssl-server.xml
   * @throws IOException
   */
  protected HopsSSLCryptoMaterial readSuperuserMaterialFromFile(Configuration configuration) throws IOException {
    Configuration sslConf = new Configuration(false);
    String sslConfResource = configuration.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
  
    sslConf.addResource(sslConfResource);
  
    String keystoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY));
    String keystorePassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY));
    String keyPassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY));
    String truststoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
    String truststorePassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
  
    File keystoreFd = new File(keystoreLocation);
    File truststoreFd = new File(truststoreLocation);
    if (!keystoreFd.exists() || !truststoreFd.exists()) {
      throw new IOException("Keystore or Truststore specified in " + sslConfResource + " does not exist! " +
          "Check your configuration.");
    }
  
    return new HopsSSLCryptoMaterial(keystoreLocation, keystorePassword, keyPassword, truststoreLocation,
        truststorePassword);
  }
  
  private boolean isCryptoMaterialSet(Configuration conf, String username) {
    for (HopsSSLSocketFactory.CryptoKeys key : HopsSSLSocketFactory.CryptoKeys.values()) {
      String propValue = conf.get(key.getValue(), key.getDefaultValue());
      if (checkForDefaultInProperty(key, propValue)
        || !checkUsernameInProperty(username, propValue, key.getType())) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Examines a TLS configuration property if it has the default value
   *
   * @param key TLS configuration property key
   * @param propValue Type of property
   * @return If the property type is not LITERAL and has the default value return true, otherwise false
   */
  private boolean checkForDefaultInProperty(HopsSSLSocketFactory.CryptoKeys key, String propValue) {
    if (key.getType() != HopsSSLSocketFactory.PropType.LITERAL) {
      if (key.getDefaultValue().equals(propValue)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Checks if the username is part of the TLS property value. For example,
   * projectName__userName should be part of value /tmp/projectName__userName__kstore.jks
   *
   * @param username Current user's username
   * @param propValue Configuration property value
   * @param propType Configuration property type
   * @return True if the property of type FILEPATH contains the username or is not of that type
   * and false if the property is of type FILEPATH but does not contain the username.
   */
  private boolean checkUsernameInProperty(String username, String propValue, HopsSSLSocketFactory.PropType propType) {
    if (propType == HopsSSLSocketFactory.PropType.FILEPATH) {
      return propValue.contains(username);
    }
    
    return true;
  }
  
  /**
   * For services (RM, NM, NN, DN) the CN of their certificate should contain their hostname
   * @param hostname Localhost hostname
   * @param configuration Hadoop configuration to be examined
   * @return True if the TLS properties of type FILEPATH contain the hostname, otherwise false
   */
  private boolean isHostnameInCryptoMaterial(String hostname, Configuration configuration) {
    for (HopsSSLSocketFactory.CryptoKeys key : HopsSSLSocketFactory.CryptoKeys.values()) {
      String propValue = configuration.get(key.getValue(), key.getDefaultValue());
      if (key.getType() == HopsSSLSocketFactory.PropType.FILEPATH
        && !propValue.contains(hostname)) {
        return false;
      }
    }
    return true;
  }
}
