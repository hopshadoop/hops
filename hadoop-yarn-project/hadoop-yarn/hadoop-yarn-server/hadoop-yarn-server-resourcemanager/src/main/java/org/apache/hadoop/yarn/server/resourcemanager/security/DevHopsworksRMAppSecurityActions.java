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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Application certificate actor which trusts any server certificate.
 *
 * NOTE: Use it ONLY for development or use it wisely
 */
public class DevHopsworksRMAppSecurityActions extends HopsworksRMAppSecurityActions {
  public DevHopsworksRMAppSecurityActions() throws MalformedURLException, GeneralSecurityException {
  }
  
  @Override
  protected CloseableHttpClient createHttpClient() throws GeneralSecurityException, IOException {
    BasicCookieStore cookieStore = new BasicCookieStore();
    SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
    sslContextBuilder.loadTrustMaterial(new TrustStrategy() {
      @Override
      public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        return true;
      }
    });
    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build(),
        NoopHostnameVerifier.INSTANCE);
    return HttpClients.custom().setDefaultCookieStore(cookieStore)
        .setSSLSocketFactory(sslSocketFactory).build();
  }
}
