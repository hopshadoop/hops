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

import com.google.common.annotations.VisibleForTesting;
import io.hops.security.HopsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RpcSSLEngineAbstr;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.codehaus.jettison.json.JSONException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class HopsSSLSocketFactory extends SocketFactory implements Configurable {

    public static final String FORCE_CONFIGURE = "client.rpc.ssl.force.configure";
    public static final boolean DEFAULT_FORCE_CONFIGURE = false;
  
    public static final String HOPSWORKS_REST_ENDPOINT_KEY =
        "client.hopsworks.rest.endpoint";
    
    private static final String KEY_STORE_FILEPATH_DEFAULT = "client.keystore.jks";
    private static final String KEY_STORE_PASSWORD_DEFAULT = "";
    private static final String KEY_PASSWORD_DEFAULT = "";
    private static final String TRUST_STORE_FILEPATH_DEFAULT = "client.truststore.jks";
    private static final String TRUST_STORE_PASSWORD_DEFAULT = "";
    private static final String SOCKET_ENABLED_PROTOCOL_DEFAULT = "TLSv1";
    
    public static final String KEYSTORE_SUFFIX = "__kstore.jks";
    public static final String TRUSTSTORE_SUFFIX = "__tstore.jks";
    private static final String PASSPHRASE = "adminpw";
    private static final String SOCKET_FACTORY_NAME = HopsSSLSocketFactory
        .class.getCanonicalName();
    
    private static final String SERVICE_CERTS_DIR_DEFAULT =
        "/srv/hops/kagent-certs/keystores";
    private static final String CLIENT_MATERIALIZE_DIR_DEFAULT =
        "/srv/hops/domains/domain1/kafkacerts";
    
    // Hopsworks project specific username pattern - projectName__username
    public static final String USERNAME_PATTERN = "\\w*__\\w*";
    private String cryptoPassword = null;
  
    private final Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);

    private enum PropType {
        FILEPATH,
        LITERAL
    }

    public enum CryptoKeys {

        KEY_STORE_FILEPATH_KEY("client.rpc.ssl.keystore.filepath", KEY_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
        KEY_STORE_PASSWORD_KEY("client.rpc.ssl.keystore.password", KEY_STORE_PASSWORD_DEFAULT, PropType.LITERAL),
        KEY_PASSWORD_KEY("client.rpc.ssl.keypassword", KEY_PASSWORD_DEFAULT, PropType.LITERAL),
        TRUST_STORE_FILEPATH_KEY("client.rpc.ssl.truststore.filepath", TRUST_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
        TRUST_STORE_PASSWORD_KEY("client.rpc.ssl.truststore.password",
            TRUST_STORE_PASSWORD_DEFAULT, PropType.LITERAL),
        SOCKET_ENABLED_PROTOCOL("client.rpc.ssl.enabled.protocol",
            SOCKET_ENABLED_PROTOCOL_DEFAULT, PropType.LITERAL),
        SERVICE_CERTS_DIR("hops.service.certificates.directory",
            SERVICE_CERTS_DIR_DEFAULT, PropType.LITERAL),
        CLIENT_MATERIALIZE_DIR("client.materialize.directory",
            CLIENT_MATERIALIZE_DIR_DEFAULT, PropType.LITERAL);

        private final String value;
        private final String defaultValue;
        private final PropType type;

        CryptoKeys(String value, String defaultValue, PropType type) {
            this.value = value;
            this.defaultValue = defaultValue;
            this.type = type;
        }

        public String getValue() {
            return this.value;
        }

        public String getDefaultValue() {
            return this.defaultValue;
        }

        public PropType getType() {
            return type;
        }
    }

    private Configuration conf;
    private String keyStoreFilePath;

    public HopsSSLSocketFactory() {
    }
    
    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  
    // ONLY for testing
    @VisibleForTesting
    public void setPaswordFromHopsworks(String password) {
      this.cryptoPassword = password;
    }
    
    public String getPasswordFromHopsworks(String username, String
        keystorePath) throws JSONException, IOException {
      
      if (null != cryptoPassword) {
        return cryptoPassword;
      }
      
      cryptoPassword = HopsUtil
          .getCertificatePasswordFromHopsworks(keystorePath, username, conf);
          
      return cryptoPassword;
    }
  
  public void configureCryptoMaterial(CertificateLocalization
      certificateLocalization, Set<String> proxySuperusers)
      throws SSLCertificateException {
    boolean cryptoConfigured = false;
    String username = null;
    try {
      username = UserGroupInformation.getCurrentUser().getUserName();
      String localHostname = NetUtils.getLocalHostname();
      boolean forceConfigure = conf.getBoolean(FORCE_CONFIGURE,
          DEFAULT_FORCE_CONFIGURE);
      
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Current user: " + username + " - Hostname: " + localHostname);
      }
      // Application running in a container is trying to create a
      // SecureSocket. The crypto material should have already been
      // localized.
      // KeyStore -> k_certificate
      // trustStore -> t_certificate
      File localized = new File("k_certificate");
      if (localized.exists()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Crypto material found in NM localized directory");
        }
        
        String password = getPasswordFromHopsworks(username, localized
            .toString());
        setTlsConfiguration("k_certificate", password, "t_certificate",
            password, conf);
        cryptoConfigured = true;
      } else {
        if (username.matches(USERNAME_PATTERN) ||
            !proxySuperusers.contains(username)) {
          // It's a normal user
          if (!isCryptoMaterialSet(conf, username)
              || forceConfigure) {
            
            String hopsworksMaterializeDir = conf.get(CryptoKeys
                .CLIENT_MATERIALIZE_DIR.getValue(), CryptoKeys
                .CLIENT_MATERIALIZE_DIR.getDefaultValue());
            
            // Client from HopsWorks is trying to create a SecureSocket
            // The crypto material should be in hopsworksMaterialzieDir
            File fd = Paths.get(hopsworksMaterializeDir, username +
                KEYSTORE_SUFFIX).toFile();
            if (fd.exists()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Crypto material exists found in " +
                    hopsworksMaterializeDir);
              }
              
              // If certificateMaterializer is not null
              // make the REST call otherwise take the material from the
              // service. In a testing env, RM is located at the same
              // machine as Hopsworks so it follows this execution path
              // but we don't want to make the REST call
              
              String password;
              
              if (null != certificateLocalization) {
                CryptoMaterial material = certificateLocalization
                    .getMaterialLocation(username);
                password = material.getKeyStorePass();
              } else {
                password = getPasswordFromHopsworks(username, fd
                    .toString());
              }
              
              setTlsConfiguration(Paths.get(hopsworksMaterializeDir,
                  username + KEYSTORE_SUFFIX).toString(),
                  password,
                  Paths.get(hopsworksMaterializeDir, username +
                      TRUSTSTORE_SUFFIX).toString(),
                  password,
                  conf);
              cryptoConfigured = true;
            } else {
              // Fallback to /tmp directory
              // In the future certificates should not exist there
              fd = Paths.get("/tmp", username + KEYSTORE_SUFFIX)
                  .toFile();
              if (fd.exists()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Crypto material exists in /tmp");
                }
                configureTlsClient("/tmp", username, conf);
                cryptoConfigured = true;
              } else {
                // Client from other services RM or NM is trying to
                // create a SecureSocket. Crypto material is already
                // materialized with the CertificateLocalizerDeprecated
                if (null != certificateLocalization) {
                  CryptoMaterial material =
                      certificateLocalization.getMaterialLocation(username);
                  
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Gotten crypto material from the " +
                        "CertificateLocalizationService");
                  }
                  setTlsConfiguration(material.getKeyStoreLocation(),
                      material.getKeyStorePass(),
                      material.getTrustStoreLocation(),
                      material.getTrustStorePass(),
                      conf);
                  cryptoConfigured = true;
                }
              }
            }
          } else {
            cryptoConfigured = true;
          }
        } else {
          // It's a superuser
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "It's superuser - force configure: " + forceConfigure);
          }
          if ((!isCryptoMaterialSet(conf, username)
              && !isHostnameInCryptoMaterial(conf, localHostname))
              || forceConfigure) {
            
            String serviceCertificateDir = conf.get(CryptoKeys
                .SERVICE_CERTS_DIR.getValue(), CryptoKeys
                .SERVICE_CERTS_DIR.getDefaultValue());
            
            // First check if the hostname keystore exists
            File fd = new File(
                Paths.get(serviceCertificateDir, localHostname +
                    KEYSTORE_SUFFIX).toString());
            if (fd.exists()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Found crypto material with the hostname");
              }
              
              String prefix = Paths.get(serviceCertificateDir,
                  localHostname).toString();
              String kstoreFile = prefix + KEYSTORE_SUFFIX;
              String tstoreFile = prefix + TRUSTSTORE_SUFFIX;
              String kstorePass, tstorePass;
              
              if (null != certificateLocalization) {
                // Socket factory has been called from RM or NM
                // In any other case, ie Hopsworks,
                // CertificateLocalizationService would be null
                kstorePass = certificateLocalization
                    .getSuperKeystorePass();
                tstorePass = certificateLocalization
                    .getSuperTruststorePass();
              } else {
                LOG.debug("*** Called setTlsConfiguration for superuser " +
                    "but no passwords provided ***");
                String[] keystore_truststore =
                    readSuperPasswordFromFile(conf);
                kstorePass = keystore_truststore[0];
                tstorePass = keystore_truststore[1];
              }
              setTlsConfiguration(kstoreFile, kstorePass, tstoreFile,
                  tstorePass, conf);
              cryptoConfigured = true;
            }
          } else {
            cryptoConfigured = true;
          }
        }
      }
    } catch (Exception ex) {
      LOG.error(ex, ex);
      throw new SSLCertificateException("Certificate could not be found!",
          ex);
    }
    
    if (!cryptoConfigured) {
      String message = "> HopsSSLSocketFactory could not " +
          "determine cryptographic material for user <" + username +
          ">. Check your configuration!";
      SSLCertificateException ex = new SSLCertificateException(message);
      LOG.error(message, ex);
      throw ex;
    }
    
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
  }
  
    private String[] readSuperPasswordFromFile(Configuration conf)
      throws IOException {
      Configuration sslConf = new Configuration(false);
      String sslConfResource = conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
          "ssl-server.xml");
      
      File sslConfFile = new File(sslConfResource);
      if (!sslConfFile.exists()) {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
          hadoopConfDir = System.getProperty("HADOOP_CONF_DIR");
        }
        if (hadoopConfDir == null) {
          throw new IOException("JVM property -DHADOOP_CONF_DIR or " +
              "environment variable is not exported and " + sslConfResource +
              " is not in classpath");
        }
        Path sslConfPath = Paths.get(hadoopConfDir, sslConfResource);
        sslConfFile = sslConfPath.toFile();
      }
      
      if (!sslConfFile.exists()) {
        throw new IOException("Could not locate ssl-server.xml. Export " +
            "JVM property -DHADOOP_CONF_DIR or environment variable or add " +
            sslConfResource + " to classpath.");
      }
      FileInputStream sslConfIn = null;
      String[] keystore_truststore = new String[2];
      
      try {
        try {
          sslConfIn = new FileInputStream(sslConfFile);
          sslConf.addResource(sslConfIn);
        } catch (IOException ex) {
          sslConf.addResource(sslConfFile.getAbsolutePath());
        }
        
        keystore_truststore[0] = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY));
        keystore_truststore[1] = sslConf.get(
            FileBasedKeyStoresFactory
                .resolvePropertyName(SSLFactory.Mode.SERVER,
                    FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
      } finally {
        if (null != sslConfIn) {
          try {
            sslConfIn.close();
          } catch (IOException ex) {
            LOG.warn("Error closing file", ex);
          }
        }
      }
      return keystore_truststore;
    }
    
    public static void configureTlsClient(String filePrefix, String username,
        Configuration conf) {
        String pref = Paths.get(filePrefix, username).toString();
        setTlsConfiguration(pref + KEYSTORE_SUFFIX, pref +
            TRUSTSTORE_SUFFIX, conf);
    }
    
    public static void configureTlsClient(String kstorePath, String
        kstorePass, String keyPass, String tstorePath, String tstorePass,
        Configuration conf) {
      conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstorePath);
      conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
      conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), keyPass);
      conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstorePath);
      conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);
      conf.set(CommonConfigurationKeys
          .HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
          SOCKET_FACTORY_NAME);
    }
    
    private static void setTlsConfiguration(String kstorePath, String tstorePath,
        Configuration conf) {
      setTlsConfiguration(kstorePath, PASSPHRASE, tstorePath, PASSPHRASE, conf);
    }
    
    public static void setTlsConfiguration(String kstorePath, String
        kstorePass, String tstorePath, String tstorePass, Configuration conf) {
        conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstorePath);
        conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
        conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), kstorePass);
        conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstorePath);
        conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);
        conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
            SOCKET_FACTORY_NAME);
    }

    private boolean isCryptoMaterialSet(Configuration conf, String username) {
        for (CryptoKeys key : CryptoKeys.values()) {
            String propValue = conf.get(key.getValue(), key.getDefaultValue());
            if (checkForDefaultInProperty(key, propValue)
                    || !checkUsernameInProperty(username, propValue, key.getType())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Services like RM, NM, NN, DN their certificate file name will contain their hostname
     *
     * @param conf
     * @param hostname
     * @return
     */
    private boolean isHostnameInCryptoMaterial(Configuration conf, String hostname) {
        for (CryptoKeys key : CryptoKeys.values()) {
            String propValue = conf.get(key.getValue(), key.getDefaultValue());
            if (key.getType() == PropType.FILEPATH
                    && !propValue.contains(hostname)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if the username is part of the property value. For example,
     * projectName__userName should be part of value /tmp/projectName__userName__kstore.jks
     * @param username
     * @param propValue
     * @return
     */
    private boolean checkUsernameInProperty(String username, String propValue, PropType propType) {
        if (propType == PropType.FILEPATH) {
            return propValue.contains(username);
        }

        return true;
    }

    private boolean checkForDefaultInProperty(CryptoKeys key, String propValue) {
      if (key.getType() != PropType.LITERAL) {
        if (key.getDefaultValue().equals(propValue)) {
          return true;
        }
      }
      
      return false;
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }

    public Socket createSocket() throws IOException, UnknownHostException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating SSL client socket");
        }
        if (conf.getBoolean(FORCE_CONFIGURE, false)) {
            setConf(conf);
        }
        SSLContext sslCtx = RpcSSLEngineAbstr.initializeSSLContext(conf);
        SSLSocketFactory socketFactory = sslCtx.getSocketFactory();
        return socketFactory.createSocket();
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

            return this == obj || ((HopsSSLSocketFactory) obj)
                    .getKeyStoreFilePath().equals(this.getKeyStoreFilePath());
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
