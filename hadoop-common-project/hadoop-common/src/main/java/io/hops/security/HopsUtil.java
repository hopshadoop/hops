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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    
    return FileUtils.readFileToString(passwdFile);
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
}
