/*
 * Copyright 2016 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.hopssslchecks.EnvVariableHopsSSLCheck;
import org.apache.hadoop.net.hopssslchecks.HopsSSLCheck;
import org.apache.hadoop.net.hopssslchecks.HopsSSLCryptoMaterial;
import org.apache.hadoop.net.hopssslchecks.LocalResourceHopsSSLCheck;
import org.apache.hadoop.net.hopssslchecks.NormalUserCertLocServiceHopsSSLCheck;
import org.apache.hadoop.net.hopssslchecks.NormalUserMaterilizeDirSSLCheck;
import org.apache.hadoop.net.hopssslchecks.SuperUserHopsSSLCheck;
import org.apache.hadoop.security.UserGroupInformation;
import io.hops.security.CertificateLocalization;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class HopsSSLSocketFactory extends SocketFactory implements Configurable {
  
  public static final String FORCE_CONFIGURE = "client.rpc.ssl.force.configure";
  public static final boolean DEFAULT_FORCE_CONFIGURE = false;
  
  private static final String KEY_STORE_FILEPATH_DEFAULT = "client.keystore.jks";
  private static final String KEY_STORE_PASSWORD_DEFAULT = "";
  private static final String KEY_PASSWORD_DEFAULT = "";
  private static final String TRUST_STORE_FILEPATH_DEFAULT = "client.truststore.jks";
  private static final String TRUST_STORE_PASSWORD_DEFAULT = "";
  private static final String SOCKET_ENABLED_PROTOCOL_DEFAULT = "TLSv1.2";
  
  
  public static final String PASSWD_FILE_SUFFIX = "__cert.key";
  public static final String KEYSTORE_SUFFIX = "__kstore.jks";
  public static final String TRUSTSTORE_SUFFIX = "__tstore.jks";
  public static final String CRYPTO_MATERIAL_ENV_VAR = "MATERIAL_DIRECTORY";
  private static final String PASSPHRASE = "adminpw";
  private static final String SOCKET_FACTORY_NAME = HopsSSLSocketFactory.class.getCanonicalName();
  
  private static final String SERVICE_CERTS_DIR_DEFAULT = "/srv/hops/kagent-certs/keystores";
  private static final String CLIENT_MATERIALIZE_DIR_DEFAULT = "/srv/hops/domains/domain1/kafkacerts";
  
  // Hopsworks project specific username pattern - projectName__username
  public static final String USERNAME_PATTERN = "\\w*__\\w*";
  
  private final static Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);
  
  // Configuration checks
  private final static HopsSSLCheck ENV_VARIABLE_CHECK = new EnvVariableHopsSSLCheck();
  private final static HopsSSLCheck LOCAL_RESOURCE = new LocalResourceHopsSSLCheck();
  private final static HopsSSLCheck NORMAL_USER_MATERIALIZE_DIR = new NormalUserMaterilizeDirSSLCheck();
  private final static HopsSSLCheck NORMAL_USER_CERTIFICATE_LOCALIZATION = new NormalUserCertLocServiceHopsSSLCheck();
  private final static HopsSSLCheck SUPER_USER = new SuperUserHopsSSLCheck();
  private final static Set<HopsSSLCheck> HOPS_SSL_CHECKS = new TreeSet<>();
  
  private HopsSSLCryptoMaterial configuredCryptoMaterial = null;
  private ReloadingX509KeyManager reloadingKeyManager = null;
  private ReloadingX509TrustManager reloadingTrustManager = null;
  
  /**
   * Configuration checks will run according to their priority
   * ENV_VARIABLE_CHECK - Priority: 110 - Checks if the crypto material exist in the specified environment variable
   * NORMAL_USER_CERTIFICATE_LOCALIZATION - Priority: 105 - Checks if the crypto material has been localized by
   *    the CertificateLocalizationService of ResourceManager or NodeManager
   * LOCAL_RESOURCE - Priority: 100 - Checks if the crypto material exist in the container's CWD
   * NORMAL_USER_MATERIALIZE_DIR - Priority: 95 - Checks if the crypto material exist in Hopsworks materialize
   *    directory. Certificates are materialized there by Hopsworks
   * SUPER_USER - Priority: -1 - Checks if the user is a super user and picks the machine certificates.
   *    NOTE: It should have the lowest priority
   */
  static {
    HOPS_SSL_CHECKS.add(ENV_VARIABLE_CHECK);
    HOPS_SSL_CHECKS.add(NORMAL_USER_CERTIFICATE_LOCALIZATION);
    HOPS_SSL_CHECKS.add(LOCAL_RESOURCE);
    HOPS_SSL_CHECKS.add(NORMAL_USER_MATERIALIZE_DIR);
    HOPS_SSL_CHECKS.add(SUPER_USER);
  }
  
  public enum PropType {
    FILEPATH,
    LITERAL
  }
  
  // Log a warning for deprecated Crypto configuration keys
  // <Deprecated CryptoKey, Warning message to print>
  private static Map<CryptoKeys, String> DEPRECATED_CRYPTO_KEYS = new HashMap<>();
  
  public enum CryptoKeys {
    
    KEY_STORE_FILEPATH_KEY("client.rpc.ssl.keystore.filepath", KEY_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
    KEY_STORE_PASSWORD_KEY("client.rpc.ssl.keystore.password", KEY_STORE_PASSWORD_DEFAULT, PropType.LITERAL),
    KEY_PASSWORD_KEY("client.rpc.ssl.keypassword", KEY_PASSWORD_DEFAULT, PropType.LITERAL),
    TRUST_STORE_FILEPATH_KEY("client.rpc.ssl.truststore.filepath", TRUST_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
    TRUST_STORE_PASSWORD_KEY("client.rpc.ssl.truststore.password", TRUST_STORE_PASSWORD_DEFAULT, PropType.LITERAL),
    SOCKET_ENABLED_PROTOCOL("client.rpc.ssl.enabled.protocol", SOCKET_ENABLED_PROTOCOL_DEFAULT, PropType.LITERAL),
    CLIENT_MATERIALIZE_DIR("client.materialize.directory", CLIENT_MATERIALIZE_DIR_DEFAULT, PropType.LITERAL);
    
    private final String value;
    private final String defaultValue;
    private final PropType type;
    
    CryptoKeys(String value, String defaultValue, PropType type) {
      this.value = value;
      this.defaultValue = defaultValue;
      this.type = type;
    }
    
    public String getValue() {
      handleDeprecation();
      return this.value;
    }
    
    public String getDefaultValue() {
      return this.defaultValue;
    }
    
    public PropType getType() {
      return type;
    }
    
    private void handleDeprecation() {
      String msg = null;
      if ((msg = DEPRECATED_CRYPTO_KEYS.get(this)) != null) {
        LOG.warn(msg);
      }
    }
  }
  
  private Configuration conf;
  private Configuration sslClientConf;
  private String keyStoreFilePath;
  
  public HopsSSLSocketFactory() {
  }
  
  @Override
  public void setConf(Configuration conf) {
    // Here we clone the configuration object as the client might pass the same configuration object
    // for different users. The `configureCryptoMaterial` changes the configuration object with user
    // specific configuration. If the configuration object is the same, they will conflict with each other.
    this.conf = new Configuration(conf);
    sslClientConf = new Configuration(false);
    String sslConfResource = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    sslClientConf.addResource(sslConfResource);
  }
  
  public HopsSSLCryptoMaterial configureCryptoMaterial(CertificateLocalization certificateLocalization, Set<String> proxySuperusers)
      throws SSLCertificateException {
    
    UserGroupInformation currentUser = null;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
      for (HopsSSLCheck checks : HOPS_SSL_CHECKS) {
        // Checks return null if they were not able to discover proper crypto material
        configuredCryptoMaterial = checks.check(currentUser, proxySuperusers, conf, certificateLocalization);
        if (configuredCryptoMaterial != null) {
          break;
        }
      }
      
      if (configuredCryptoMaterial == null) {
        String message = "> HopsSSLSocketFactory could not determine cryptographic material for user <" + currentUser.getUserName() +
            ">. Check your configuration!";
        SSLCertificateException ex = new SSLCertificateException(message);
        LOG.error(message, ex);
        throw ex;
      }
  
      setTlsConfiguration(
          configuredCryptoMaterial.getKeyStoreLocation(),
          configuredCryptoMaterial.getKeyStorePassword(),
          configuredCryptoMaterial.getKeyPassword(),
          configuredCryptoMaterial.getTrustStoreLocation(),
          configuredCryptoMaterial.getTrustStorePassword(),
          conf);
      
      // *ClientCache* caches client instances based on their socket factory.
      // In order to distinguish two client with the same socket factory but
      // with different certificates, the hashCode is computed by the
      // keystore filepath as well
      this.keyStoreFilePath = conf.get(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(),
          KEY_STORE_FILEPATH_DEFAULT);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finally, the keystore that is used is: " + keyStoreFilePath);
      }
      conf.setBoolean(FORCE_CONFIGURE, false);
      return configuredCryptoMaterial;
    } catch (IOException ex) {
      String user = currentUser != null ? currentUser.getUserName() : "Could not find user from UGI";
      LOG.error("Error while configuring SocketFactory for user <" + user + "> " + ex.getMessage(), ex);
      throw new SSLCertificateException(ex);
    }
  }
  
  public static void configureTlsClient(String filePrefix, String username,
      Configuration conf) {
    String pref = Paths.get(filePrefix, username).toString();
    setTlsConfiguration(pref + KEYSTORE_SUFFIX, pref + TRUSTSTORE_SUFFIX, conf);
  }
  
  public static void configureTlsClient(String kstorePath, String
      kstorePass, String keyPass, String tstorePath, String tstorePass,
      Configuration conf) {
    conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstorePath);
    conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
    conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), keyPass);
    conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstorePath);
    conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);
    conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, SOCKET_FACTORY_NAME);
  }
  
  private static void setTlsConfiguration(String kstorePath, String tstorePath,
      Configuration conf) {
    setTlsConfiguration(kstorePath, PASSPHRASE, PASSPHRASE, tstorePath, PASSPHRASE, conf);
  }
  
  public static void setTlsConfiguration(String kstorePath, String
      kstorePass, String tstorePath, String tstorePass, Configuration conf) {
    setTlsConfiguration(kstorePath, kstorePass, kstorePass, tstorePath, tstorePass, conf);
  }
  
  public static void setTlsConfiguration(String kstorePath, String kstorePass, String keyPassword, String tstorePath,
      String tstorePass, Configuration conf) {
    conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstorePath);
    conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
    conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), keyPassword);
    conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstorePath);
    conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);
    conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, SOCKET_FACTORY_NAME);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public void stopReloadingKeyManagers() {
    if (reloadingKeyManager != null) {
      reloadingKeyManager.stop();
    }
    
    if (reloadingTrustManager != null) {
      reloadingTrustManager.destroy();
    }
  }
  
  public HopsSSLSocketWrapper createHopsSSLSocket() throws IOException, UnknownHostException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating Hops SSL client socket");
    }
    if (conf.getBoolean(FORCE_CONFIGURE, false)) {
      setConf(conf);
    }
    SSLContextMaterial sslContextMaterial = initializeSSLContext();
    SSLSocketFactory socketFactory = sslContextMaterial.sslContext.getSocketFactory();
    Socket s = socketFactory.createSocket();
    return new HopsSSLSocketWrapper(s, sslContextMaterial.keyManagers, sslContextMaterial.trustManagers);
  }

  private class SSLContextMaterial {
    private final KeyManager[] keyManagers;
    private final TrustManager[] trustManagers;
    private final SSLContext sslContext;

    private SSLContextMaterial(KeyManager[] keyManagers, TrustManager[] trustManagers, SSLContext sslContext) {
      this.keyManagers = keyManagers;
      this.trustManagers = trustManagers;
      this.sslContext = sslContext;
    }
  }

  private SSLContextMaterial initializeSSLContext() throws IOException {
    try {
      String enabledProtocol = conf.get(HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL.getValue(),
          HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL.getDefaultValue());
      SSLContext sslCtx = SSLContext.getInstance(enabledProtocol);
      
      long keyStoreReloadInterval = FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL;
      String timeUnitStr = FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT;
      long trustStoreReloadInterval = FileBasedKeyStoresFactory.DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL;
      if (sslClientConf != null) {
        try {
          keyStoreReloadInterval = sslClientConf.getLong(FileBasedKeyStoresFactory.resolvePropertyName(
              SSLFactory.Mode.CLIENT, FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY),
              FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL);
          timeUnitStr = sslClientConf.get(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.CLIENT,
              FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY),
              FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT);
          trustStoreReloadInterval = sslClientConf.getLong(FileBasedKeyStoresFactory.resolvePropertyName(
              SSLFactory.Mode.CLIENT, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
              FileBasedKeyStoresFactory.DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);
        } catch (RuntimeException ex) {
          // RuntimeException will be thrown if configuration file exists but we can't read it
          // In that case just fall back to default values
          LOG.warn("ssl-client.xml exists but we can't read it, falling back to default configuration");
        }
      }
      TimeUnit timeUnit = TimeUnit.valueOf(timeUnitStr);
      KeyManager[] keyManagers = createKeyManagers(keyStoreReloadInterval, timeUnit);
      TrustManager[] trustManagers = createTrustManagers(trustStoreReloadInterval);
      sslCtx.init(keyManagers, trustManagers, null);
      return new SSLContextMaterial(keyManagers, trustManagers, sslCtx);
    } catch (GeneralSecurityException ex) {
      String keyStore = conf.get(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue());
      LOG.error("Could not initialize SSLContext with keystore " + keyStore, ex);
      throw new IOException("Error initializing SSLContext", ex);
    }
  }
  
  private KeyManager[] createKeyManagers(long reloadInterval, TimeUnit timeUnit)
      throws GeneralSecurityException, IOException {
    reloadingKeyManager = new ReloadingX509KeyManager("JKS", configuredCryptoMaterial.getKeyStoreLocation(),
        configuredCryptoMaterial.getKeyStorePassword(),
        configuredCryptoMaterial.getPasswordFileLocation(),
        configuredCryptoMaterial.getKeyStorePassword(), reloadInterval, timeUnit);
    if (configuredCryptoMaterial.needsReloading()) {
      reloadingKeyManager.init();
    }
    return new KeyManager[]{reloadingKeyManager};
  }
  
  private TrustManager[] createTrustManagers(long reloadInterval) throws GeneralSecurityException, IOException {
    reloadingTrustManager = new ReloadingX509TrustManager("JKS", configuredCryptoMaterial.getTrustStoreLocation(),
        configuredCryptoMaterial.getTrustStorePassword(), configuredCryptoMaterial.getPasswordFileLocation(), reloadInterval);
    if (configuredCryptoMaterial.needsReloading()) {
      reloadingTrustManager.init();
    }
    return new TrustManager[]{reloadingTrustManager};
  }
  
  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    Socket socket = createSocket();
    socket.connect(new InetSocketAddress(host, port));
    return socket;
  }
  
  @Override
  public Socket createSocket(String host, int port, InetAddress localAddress,
      int localPort) throws IOException, UnknownHostException {
    Socket socket = createSocket();
    socket.bind(new InetSocketAddress(localAddress, localPort));
    socket.connect(new InetSocketAddress(host, port));
    return socket;
  }
  
  @Override
  public Socket createSocket(InetAddress inetAddress, int port) throws IOException {
    Socket socket = createSocket();
    socket.connect(new InetSocketAddress(inetAddress, port));
    return socket;
  }
  
  @Override
  public Socket createSocket(InetAddress inetAddress, int port, InetAddress localAddress, int localPort)
      throws IOException {
    Socket socket = createSocket();
    socket.bind(new InetSocketAddress(localAddress, localPort));
    socket.connect(new InetSocketAddress(inetAddress, port));
    return socket;
  }
  
  public String getKeyStoreFilePath() {
    return keyStoreFilePath;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HopsSSLSocketFactory) {
      
      return this == obj || ((HopsSSLSocketFactory) obj).getKeyStoreFilePath().equals(this.getKeyStoreFilePath());
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    int result = 3;
    result = 37 * result + this.getClass().hashCode();
    // See comment at setConf
    result = 37 * result + this.keyStoreFilePath.hashCode();
    
    return result;
  }
}
