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
import org.apache.hadoop.ipc.RpcSSLEngineAbstr;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;

public class HopsSSLSocketFactory extends SocketFactory implements Configurable {

    public static final String FORCE_CONFIGURE = "client.rpc.ssl.force.configure";
    public static final boolean DEFAULT_FORCE_CONFIGURE = false;
    
    private static final String KEY_STORE_FILEPATH_DEFAULT = "client.keystore.jks";
    private static final String KEY_STORE_PASSWORD_DEFAULT = "";
    private static final String KEY_PASSWORD_DEFAULT = "";
    private static final String TRUST_STORE_FILEPATH_DEFAULT = "client.truststore.jks";
    private static final String TRUST_STORE_PASSWORD_DEFAULT = "";
    private static final String SOCKET_ENABLED_PROTOCOL_DEFAULT = "TLSv1";
    
    private static final String KEYSTORE_SUFFIX = "__kstore.jks";
    private static final String TRUSTSTORE_SUFFIX = "__tstore.jks";
    private static final String PASSPHRASE = "adminpw";
    private static final String SOCKET_FACTORY_NAME = HopsSSLSocketFactory
        .class.getCanonicalName();
    
    private static final String SERVICE_CERTS_DIR_DEFAULT =
        "/srv/hops/kagent-certs/keystores";
    private static final String CLIENT_MATERIALIZE_DIR_DEFAULT =
        "/srv/hops/domains/domain1/kafkacerts";
    
    // Hopsworks project specific username pattern - projectName__username
    public static final String USERNAME_PATTERN = "\\w*__\\w*";
  
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
  
    public void configureCryptoMaterial(CertificateLocalization
        certificateLocalization, String proxySuperuser) {
        try {
            String username =
                UserGroupInformation.getCurrentUser().getUserName();
            String localHostname = NetUtils.getLocalHostname();
            boolean forceConfigure = conf.getBoolean(FORCE_CONFIGURE,
                DEFAULT_FORCE_CONFIGURE);
          
            if (LOG.isDebugEnabled()) {
              LOG.debug("Current user: " + username + " - Hostname: " + localHostname);
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
            setTlsConfiguration("k_certificate",
                "t_certificate", conf);
          } else {
            if (username.matches(USERNAME_PATTERN) ||
                !username.equals(proxySuperuser)) {
              // It's a normal user
              if (!isCryptoMaterialSet(conf, username)
                  || forceConfigure) {
  
                String hopsworksMaterializeDir = conf.get(CryptoKeys
                    .CLIENT_MATERIALIZE_DIR.getValue(), CryptoKeys
                    .CLIENT_MATERIALIZE_DIR.getDefaultValue());
                
                // Client from HopsWorks is trying to create a SecureSocket
                // The crypto material should be in the CERT_MATERIALIZED_DIR
                //Path pathToCert = Paths.get(CERT_MATERIALIZED_DIR, username);
                File fd = Paths.get(hopsworksMaterializeDir, username +
                    KEYSTORE_SUFFIX).toFile();
                if (fd.exists()) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Crypto material exists found in " + hopsworksMaterializeDir);
                  }
                  configureTlsClient(hopsworksMaterializeDir, username, conf);
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
                          material.getTrustStoreLocation(), conf);
                    }
                  }
                }
              }
            } else {
              // It's a superuser
              if (LOG.isDebugEnabled()) {
                LOG.debug("It's superuser - force configure: " + forceConfigure);
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
                  configureTlsClient(serviceCertificateDir,
                      localHostname, conf);
                }
              }
            }
          }
        } catch(Exception ex){
          LOG.error(ex, ex);
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
    
    public static void configureTlsClient(String filePrefix, String username, Configuration conf) {
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
    
    private static void setTlsConfiguration(String kstorePath, String
        tstorePath, Configuration conf) {
        conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstorePath);
        conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), PASSPHRASE);
        conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), PASSPHRASE);
        conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstorePath);
        conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), PASSPHRASE);
        conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
            SOCKET_FACTORY_NAME);
    }

    private boolean isCryptoMaterialSet(Configuration conf, String username) {
        for (CryptoKeys key : CryptoKeys.values()) {
            String propValue = conf.get(key.getValue(), key.getDefaultValue());
            if (key.getDefaultValue().equals(propValue)
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
