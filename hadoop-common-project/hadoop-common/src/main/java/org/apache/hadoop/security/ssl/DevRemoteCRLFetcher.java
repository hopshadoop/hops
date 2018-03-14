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
package org.apache.hadoop.security.ssl;


import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Remote CRL fetcher which trusts any server certificate.
 *
 * NOTE: Use it ONLY for development or use it wisely
 */
public class DevRemoteCRLFetcher extends RemoteCRLFetcher {
  
  public DevRemoteCRLFetcher() {
    final TrustManager[] trustAll = new TrustManager[] {
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
