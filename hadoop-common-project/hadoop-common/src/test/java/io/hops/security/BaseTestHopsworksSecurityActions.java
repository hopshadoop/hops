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


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.regex.Matcher;

/**
 * Base class for testing the interface with Hopsworks
 *
 * Normally tests that extend this class are ignored as they
 * expect a running Hopsworks instance to make HTTP requests.
 *
 * Before running the tests, edit the connection details below
 * accordingly.
 *
 */
public abstract class BaseTestHopsworksSecurityActions {
  protected String HOPSWORKS_ENDPOINT = "https://HOST:PORT";
  private String HOPSWORKS_USER = "USERNAME";
  private String HOPSWORKS_PASSWORD = "PASSWORD";
  private String HOPSWORKS_LOGIN_PATH = "/hopsworks-api/api/auth/service";
  
  protected void setupJWT(Configuration conf, String classpath) throws Exception {
    String sslConfFilename = super.getClass().getSimpleName() + ".ssl-server.xml";
    Path sslServerPath = Paths.get(classpath, sslConfFilename);
    Pair<String, String[]> jwtResponse = loginAndGetJWT();
    
    Configuration sslServer = new Configuration(false);
    sslServer.set(ServiceJWTManager.JWT_MANAGER_MASTER_TOKEN_KEY, jwtResponse.getFirst());
    for (int i = 0; i < jwtResponse.getSecond().length; i++) {
      String renewalConfKey = String.format(ServiceJWTManager.JWT_MANAGER_RENEW_TOKEN_PATTERN, i);
      sslServer.set(renewalConfKey, jwtResponse.getSecond()[i]);
    }
  
    KeyStoreTestUtil.saveConfig(sslServerPath.toFile(), sslServer);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFilename);
    conf.set(CommonConfigurationKeys.HOPS_HOPSWORKS_HOST_KEY, HOPSWORKS_ENDPOINT);
  }
  
  protected Pair<String, String[]> loginAndGetJWT() throws Exception {
    CloseableHttpClient client = null;
    try {
      SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
      sslContextBuilder.loadTrustMaterial(new TrustStrategy() {
        @Override
        public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
          return true;
        }
      });
      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build(),
          NoopHostnameVerifier.INSTANCE);
      
      client = HttpClients.custom().setSSLSocketFactory(sslSocketFactory).build();
      URL loginURL = new URL(new URL(HOPSWORKS_ENDPOINT), HOPSWORKS_LOGIN_PATH);
      HttpUriRequest login = RequestBuilder.post()
          .setUri(loginURL.toURI())
          .addParameter("email", HOPSWORKS_USER)
          .addParameter("password", HOPSWORKS_PASSWORD)
          .build();
      CloseableHttpResponse response = client.execute(login);
      Assert.assertNotNull(response);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      Header[] authHeaders = response.getHeaders(HttpHeaders.AUTHORIZATION);
      
      String masterJWT = null;
      for (Header h : authHeaders) {
        Matcher matcher = AbstractSecurityActions.JWT_PATTERN.matcher(h.getValue());
        if (matcher.matches()) {
          masterJWT = matcher.group(1);
        }
      }
      JsonParser jsonParser = new JsonParser();
      JsonObject json = jsonParser.parse(EntityUtils.toString(response.getEntity())).getAsJsonObject();
      JsonArray array = json.getAsJsonArray("renewTokens");
      String[] renewTokens = new String[array.size()];
      boolean renewalTokensFound = false;
      for (int i = 0; i < renewTokens.length; i++) {
        renewTokens[i] = array.get(i).getAsString();
        renewalTokensFound = true;
      }
      if (masterJWT != null && renewalTokensFound) {
        return new Pair<>(masterJWT, renewTokens);
      }
      
      throw new IOException("Could not get JWT from Hopsworks");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
