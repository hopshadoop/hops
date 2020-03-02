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

import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.hopssslchecks.HopsSSLCryptoMaterial;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.apache.hadoop.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_PASSWORDFILE_LOCATION_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY;

public class HopsFileBasedKeyStoresFactory implements KeyStoresFactory {
  private static final Log LOG = LogFactory.getLog(HopsFileBasedKeyStoresFactory.class);
  
  private Configuration sslConf;
  private Configuration systemConf;
  private ReloadingX509KeyManager keyManager;
  private KeyManager[] keyManagers;
  private ReloadingX509TrustManager trustManager;
  private TrustManager[] trustManagers;
  
  @Override
  public void init(SSLFactory.Mode mode) throws IOException, GeneralSecurityException {
    HopsSSLCryptoMaterial material = loadCryptoMaterial(mode);
    createKeyManagers(mode, material);
    createTrustManagers(mode, material);
  }
  
  public HopsSSLCryptoMaterial loadCryptoMaterial(SSLFactory.Mode mode) throws IOException {
    try {
      CertificateLocalizationCtx certificateLocalizationCtx = CertificateLocalizationCtx.getInstance();
      certificateLocalizationCtx.setProxySuperusers(systemConf);
      Configuration x509MaterialConf = new Configuration(false);
      x509MaterialConf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY,
          systemConf.get(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, ""));
      // Create a HopsSSLSocketFactory to use its functionality of identifying the correct security material
      // We don't use the socket factory anywhere else in this class
      HopsSSLSocketFactory hopsSSLSocketFactory = new HopsSSLSocketFactory();
      hopsSSLSocketFactory.setConf(x509MaterialConf);
      return hopsSSLSocketFactory.configureCryptoMaterial(
          certificateLocalizationCtx.getCertificateLocalization(), certificateLocalizationCtx.getProxySuperusers());
    } catch (Exception ex) {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      LOG.warn("Could not locate cryptographic material for <" + currentUser.getUserName()
        + "> Falling back to ssl-{client,server}.xml");
      // Fallback to old-school ssl-{client,server}.xml
      String keystoreLocationProperty = FileBasedKeyStoresFactory.resolvePropertyName(mode,
          SSL_KEYSTORE_LOCATION_TPL_KEY);
      String keystoreLocation = sslConf.get(keystoreLocationProperty);
      String keystorePasswordProperty = FileBasedKeyStoresFactory.resolvePropertyName(mode,
          SSL_KEYSTORE_PASSWORD_TPL_KEY);
      String keystorePassword = sslConf.get(keystorePasswordProperty);
      String keyPasswordProperty =
          FileBasedKeyStoresFactory.resolvePropertyName(mode, SSL_KEYSTORE_KEYPASSWORD_TPL_KEY);
      String keyPassword = sslConf.get(keyPasswordProperty, keystorePassword);
      
      String truststoreLocationProperty = FileBasedKeyStoresFactory.resolvePropertyName(mode,
          SSL_TRUSTSTORE_LOCATION_TPL_KEY);
      String truststoreLocation = sslConf.get(truststoreLocationProperty);
      String passwordFileLocationProperty =
          FileBasedKeyStoresFactory.resolvePropertyName(mode,
              SSL_PASSWORDFILE_LOCATION_TPL_KEY);
      String passwordFileLocation = sslConf.get(passwordFileLocationProperty, null);
      if (Strings.isNullOrEmpty(keystoreLocation) || Strings.isNullOrEmpty(truststoreLocation)
        || Strings.isNullOrEmpty(keystorePassword) || Strings.isNullOrEmpty(keyPassword)) {
        throw new IOException("Failed to determine cryptographic material for user <" + currentUser.getUserName()
          + ">. Exhausted all methods!");
      }
      return new HopsSSLCryptoMaterial(
          keystoreLocation, keystorePassword, keyPassword,
          truststoreLocation, keystorePassword,
          passwordFileLocation, true);
    }
  }
  
  @Override
  public void destroy() {
    if (trustManager != null) {
      trustManager.destroy();
      trustManager = null;
      trustManagers = null;
    }
    if (keyManager != null) {
      keyManager.stop();
      keyManager = null;
      keyManagers = null;
    }
  }
  
  @Override
  public KeyManager[] getKeyManagers() {
    return keyManagers;
  }
  
  @Override
  public TrustManager[] getTrustManagers() {
    return trustManagers;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.sslConf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return sslConf;
  }
  
  public void setSystemConf(Configuration conf) {
    this.systemConf = conf;
  }
  
  public Configuration getSystemConf() {
    return systemConf;
  }
  
  private void createKeyManagers(SSLFactory.Mode mode, HopsSSLCryptoMaterial material)
      throws IOException, GeneralSecurityException {
    boolean requireClientCert = sslConf.getBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY,
        SSLFactory.SSL_REQUIRE_CLIENT_CERT_DEFAULT);
    
    String keystoreType = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(mode, FileBasedKeyStoresFactory.SSL_KEYSTORE_TYPE_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
    
    if (requireClientCert || mode == SSLFactory.Mode.SERVER) {
      
      String keystoreLocation = material.getKeyStoreLocation();
      if (Strings.isNullOrEmpty(keystoreLocation)) {
        throw new GeneralSecurityException("Could not identify correct keystore");
      }
      String keystorePassword = material.getKeyStorePassword();
      if (Strings.isNullOrEmpty(keystorePassword)) {
        throw new GeneralSecurityException("Could not load keystore password");
      }
      String keyPassword = material.getKeyPassword();
      if (Strings.isNullOrEmpty(keyPassword)) {
        throw new GeneralSecurityException("Could not load key password");
      }
  
      long keyStoreReloadInterval = sslConf.getLong(
          FileBasedKeyStoresFactory.resolvePropertyName(mode, SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY),
          DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL);
      String timeUnitStr = sslConf.get(
          FileBasedKeyStoresFactory.resolvePropertyName(mode, SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY),
          DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT);
      TimeUnit reloadTimeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
      
      String passwordFileLocation = material.getPasswordFileLocation();
      keyManager = new ReloadingX509KeyManager(keystoreType, keystoreLocation, keystorePassword, passwordFileLocation,
          keyPassword, keyStoreReloadInterval, reloadTimeUnit);
      
      keyManager.init();
      if (LOG.isDebugEnabled()) {
        LOG.debug(mode.toString() + " Loaded KeyStore: " + keystoreLocation);
      }
      keyManagers = new KeyManager[]{keyManager};
    } else {
      KeyStore keyStore = KeyStore.getInstance(keystoreType);
      keyStore.load(null, null);
      KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
      keyMgrFactory.init(keyStore, null);
      keyManagers = keyMgrFactory.getKeyManagers();
    }
  }
  
  private void createTrustManagers(SSLFactory.Mode mode, HopsSSLCryptoMaterial material)
      throws IOException, GeneralSecurityException {
    String truststoreType = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(mode, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_TYPE_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
    String truststoreLocation = material.getTrustStoreLocation();
    if (Strings.isNullOrEmpty(truststoreLocation)) {
      throw new GeneralSecurityException("Could not identify correct truststore");
    }
    String truststorePassword = material.getTrustStorePassword();
    if (Strings.isNullOrEmpty(truststorePassword)) {
      throw new GeneralSecurityException("Could not load truststore password");
    }
    String passwordFileLocation = material.getPasswordFileLocation();
    long truststoreReloadInterval =
        sslConf.getLong(
            FileBasedKeyStoresFactory.resolvePropertyName(mode, SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
            DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);
    if (LOG.isDebugEnabled()) {
      LOG.debug(mode.toString() + " TrustStore: " + truststoreLocation);
    }
    trustManager = new ReloadingX509TrustManager(truststoreType,
        truststoreLocation, truststorePassword, passwordFileLocation, truststoreReloadInterval);
    trustManager.init();
    if (LOG.isDebugEnabled()) {
      LOG.debug(mode.toString() + " Loaded TrustStore: " + truststoreLocation);
    }
    trustManagers = new TrustManager[]{trustManager};
  }
}
