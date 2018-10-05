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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObjectGenerator;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HopsworksRMAppCertificateActions implements RMAppCertificateActions, Configurable {
  public static final String HOPSWORKS_USER_KEY = "hops.hopsworks.user";
  public static final String HOPSWORKS_PASSWORD_KEY = "hops.hopsworks.password";
  public static final String REVOKE_CERT_ID_PARAM = "certId";
  
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppCertificateActions.class);
  private static final Set<Integer> ACCEPTABLE_HTTP_RESPONSES = new HashSet<>(2);
  
  private Configuration conf;
  private Configuration sslConf;
  private URL hopsworksHost;
  private URL loginEndpoint;
  private URL signEndpoint;
  private URL revokeEndpoint;
  private String revokePath;
  private CertificateFactory certificateFactory;
  
  public HopsworksRMAppCertificateActions() throws MalformedURLException, GeneralSecurityException {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void init() throws MalformedURLException, GeneralSecurityException {
    hopsworksHost = new URL(conf.get(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY,
        "http://127.0.0.1"));
    loginEndpoint = new URL(hopsworksHost,
        conf.get(YarnConfiguration.HOPS_HOPSWORKS_LOGIN_ENDPOINT_KEY,
            YarnConfiguration.DEFAULT_HOPS_HOPSWORKS_LOGIN_ENDPOINT));
    signEndpoint = new URL(hopsworksHost,
        conf.get(YarnConfiguration.HOPS_HOPSWORKS_SIGN_ENDPOINT_KEY,
            YarnConfiguration.DEFAULT_HOPS_HOPSWORKS_SIGN_ENDPOINT));
    revokePath = conf.get(YarnConfiguration.HOPS_HOPSWORKS_REVOKE_ENDPOINT_KEY,
        YarnConfiguration.DEFAULT_HOPS_HOPSWORKS_REVOKE_ENDPOINT);
    if (revokePath.startsWith("/")) {
      revokePath = "%s" + revokePath;
    } else {
      revokePath = "%s/" + revokePath;
    }
    certificateFactory = CertificateFactory.getInstance("X.509", "BC");
    sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
  }
  
  @Override
  public RMAppCertificateManager.CertificateBundle sign(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpClient httpClient = null;
    try {
      httpClient = createHttpClient();
      login(httpClient);
  
      String csrStr = stringifyCSR(csr);
      JsonObject json = new JsonObject();
      json.addProperty("csr", csrStr);
      
      CloseableHttpResponse signResponse = post(httpClient, json, signEndpoint.toURI(),
          "Hopsworks CA could not sign CSR");
      
      String signResponseEntity = EntityUtils.toString(signResponse.getEntity());
      JsonObject jsonResponse = new JsonParser().parse(signResponseEntity).getAsJsonObject();
      String signedCert = jsonResponse.get("signedCert").getAsString();
      X509Certificate certificate = parseCertificate(signedCert);
      String intermediateCaCert = jsonResponse.get("intermediateCaCert").getAsString();
      X509Certificate issuer = parseCertificate(intermediateCaCert);
      return new RMAppCertificateManager.CertificateBundle(certificate, issuer);
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
  
  @Override
  public int revoke(String certificateIdentifier) throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpClient httpClient = null;
    try {
      httpClient = createHttpClient();
      login(httpClient);
      
      String queryParams = buildQueryParams(new BasicNameValuePair(REVOKE_CERT_ID_PARAM, certificateIdentifier));
      URL revokeUrl = buildUrl(revokePath, queryParams);
      
      CloseableHttpResponse response = delete(httpClient, revokeUrl.toURI(),
          "Hopsworks CA could not revoke certificate " + certificateIdentifier);
      return response.getStatusLine().getStatusCode();
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }
  
  protected CloseableHttpClient createHttpClient() throws GeneralSecurityException, IOException {
    BasicCookieStore cookieStore = new BasicCookieStore();
    return HttpClients.custom().setDefaultCookieStore(cookieStore).build();
  }
  
  private void login(CloseableHttpClient httpClient) throws URISyntaxException, IOException {
    HttpUriRequest login = RequestBuilder.post()
        .setUri(loginEndpoint.toURI())
        .addParameter("email", sslConf.get(HOPSWORKS_USER_KEY))
        .addParameter("password", sslConf.get(HOPSWORKS_PASSWORD_KEY))
        .build();
    CloseableHttpResponse loginResponse = httpClient.execute(login);
    
    checkHTTPResponseCode(loginResponse.getStatusLine().getStatusCode(), "Could not login to Hopsworks");
  }
  
  private CloseableHttpResponse post(CloseableHttpClient httpClient, JsonObject jsonEntity, URI target, String errorMessage)
      throws IOException {
    HttpPost request = new HttpPost(target);
    request.setEntity(new StringEntity(jsonEntity.toString()));
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private CloseableHttpResponse delete(CloseableHttpClient httpClient, URI target, String errorMessage)
      throws IOException {
    HttpDelete request = new HttpDelete(target);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "text/plain");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private URL buildUrl(String apiUrl, String queryParams) throws MalformedURLException {
    String lala = String.format(apiUrl, hopsworksHost.toString()) + queryParams;
    return new URL(lala);
  }
  
  private String buildQueryParams(NameValuePair... params) {
    List<NameValuePair> qparams = new ArrayList<>();
    for (NameValuePair p : params) {
      if (p.getValue() != null) {
        qparams.add(p);
      }
    }
    return URLEncodedUtils.format(qparams, "UTF-8");
  }
  
  private X509Certificate parseCertificate(String certificateStr) throws IOException, GeneralSecurityException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(certificateStr.getBytes())) {
      return (X509Certificate) certificateFactory.generateCertificate(bis);
    }
  }
  
  private void checkHTTPResponseCode(int responseCode, String msg) throws IOException {
    if (!ACCEPTABLE_HTTP_RESPONSES.contains(responseCode)) {
      throw new IOException("HTTP error, response code " + responseCode + " Message: " + msg);
    }
  }
  
  private String stringifyCSR(PKCS10CertificationRequest csr) throws IOException {
    try (StringWriter sw = new StringWriter()) {
      PemWriter pw = new PemWriter(sw);
      PemObjectGenerator pog = new JcaMiscPEMGenerator(csr);
      pw.writeObject(pog.generate());
      pw.flush();
      sw.flush();
      pw.close();
      return sw.toString();
    }
  }
}
