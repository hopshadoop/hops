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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HopsUtil {
  
  private static final Log LOG = LogFactory.getLog(HopsUtil.class);
  
  private static final TrustManager[] trustAll = new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      }
  };
  
  // Assuming that subject attributes do not contain comma
  private static final Pattern CN_PATTERN = Pattern.compile(".*CN=([^,]+).*");
  private static final Pattern O_PATTERN = Pattern.compile(".*O=([^,]+).*");
  private static final Pattern OU_PATTERN = Pattern.compile(".*OU=([^,]+).*");
  
  /**
   * Read password for cryptographic material from a file. The file could be
   * either localized in a container or from Hopsworks certificates transient
   * directory.
   * @param passwdFile Location of the password file
   * @return Password to unlock cryptographic material
   * @throws IOException
   */
  public static String readCryptoMaterialPassword(File passwdFile) throws
      IOException {
    
    if (!passwdFile.exists()) {
      throw new FileNotFoundException("File containing crypto material " +
          "password could not be found");
    }
    
    return FileUtils.readFileToString(passwdFile).trim();
  }
  
  /**
   * Extracts the CommonName (CN) from an X.509 subject
   *
   * NOTE: Used by Hopsworks
   *
   * @param subject X.509 subject
   * @return CommonName or null if it cannot be parsed
   */
  public static String extractCNFromSubject(String subject) {
    Matcher matcher = CN_PATTERN.matcher(subject);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }
  
  /**
   * Extracts the Organization (O) from an X.509 subject
   *
   * NOTE: Used by Hopsworks
   *
   * @param subject X.509 subject
   * @return Organization or null if it cannot be parsed
   */
  public static String extractOFromSubject(String subject) {
    Matcher matcher = O_PATTERN.matcher(subject);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }
  
  public static String extractOUFromSubject(String subject) {
    Matcher matcher = OU_PATTERN.matcher(subject);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return null;
  }
  
  /**
   * Set the default HTTPS trust policy to trust anything.
   *
   * NOTE: Use it only during development or use it wisely!
   */
  public static void trustAllHTTPS() {
    try {
      final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAll, null);
      HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
        @Override
        public boolean verify(String s, SSLSession sslSession) {
          return true;
        }
      });
    } catch (GeneralSecurityException ex) {
      throw new IllegalStateException("Could not initialize SSLContext for CRL fetcher", ex);
    }
  }
  
  /**
   * Generate the client version of ssl-server.xml used by containers that create RPC servers.
   * It reads the password of the crypto material from a well-known password file from container's
   * CWD, ssl-client.xml if exists for keymanagers reloading configuration and generates the ssl-server.xml
   *
   * @param conf System configuration
   * @throws IOException
   */
  public static void generateContainerSSLServerConfiguration(Configuration conf) throws IOException {
    generateContainerSSLServerConfiguration(new File(conf.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY,
        SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH)), conf);
  }
  
  /**
   * It should be used only for testing.
   *
   * Normally the password file should be in container's CWD with the filename specified in
   * @see HopsSSLSocketFactory#LOCALIZED_PASSWD_FILE_NAME
   *
   * @param passwdFile
   * @param conf
   * @throws IOException
   */
  @VisibleForTesting
  public static void generateContainerSSLServerConfiguration(File passwdFile, Configuration conf) throws IOException {
    if (!passwdFile.exists()) {
      String cwd = System.getProperty("user.dir");
      throw new FileNotFoundException("File " + conf.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH) + " does not exist " + "in " + cwd);
    }
    String cryptoMaterialPassword = FileUtils.readFileToString(passwdFile);
    Configuration sslConf = generateSSLServerConf(conf, cryptoMaterialPassword);
    writeSSLConf(sslConf, conf, passwdFile);
  }
  
  private static Configuration generateSSLServerConf(Configuration conf, String cryptoMaterialPassword) {
    Configuration sslConf = new Configuration(false);
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY),
        conf.get(SSLFactory.LOCALIZED_KEYSTORE_FILE_PATH_KEY, SSLFactory.DEFAULT_LOCALIZED_KEYSTORE_FILE_PATH));
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), cryptoMaterialPassword);
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY), cryptoMaterialPassword);
    
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY),
        conf.get(SSLFactory.LOCALIZED_TRUSTSTORE_FILE_PATH_KEY, SSLFactory.DEFAULT_LOCALIZED_TRUSTSTORE_FILE_PATH));
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY), cryptoMaterialPassword);

    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_PASSWORDFILE_LOCATION_TPL_KEY),
        conf.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY, SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH));

    Configuration sslClientConf = new Configuration(false);
    String sslClientResource = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    sslClientConf.addResource(sslClientResource);
    long keyStoreReloadInterval = sslClientConf.getLong(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.CLIENT, FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_INTERVAL);
    String timeUnitStr = sslClientConf.get(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.CLIENT, FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_SSL_KEYSTORE_RELOAD_TIMEUNIT);
    long trustStoreReloadInterval = sslClientConf.getLong(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.CLIENT, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);
    
    sslConf.setLong(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_INTERVAL_TPL_KEY), keyStoreReloadInterval);
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_RELOAD_TIMEUNIT_TPL_KEY), timeUnitStr);
    sslConf.setLong(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), trustStoreReloadInterval);
    
    return sslConf;
  }
  
  private static void writeSSLConf(Configuration sslConf, Configuration systemConf, File passwdFile) throws
      IOException {
    String filename = systemConf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    // Workaround for testing
    String outputFile;
    if (passwdFile.getParentFile() == null) {
      outputFile = filename;
    } else {
      outputFile = Paths.get(passwdFile.getParentFile().getAbsolutePath(), filename).toString();
    }
    
    try (FileWriter fw = new FileWriter(outputFile, false)) {
      sslConf.writeXml(fw);
      fw.flush();
    }
  }
}
