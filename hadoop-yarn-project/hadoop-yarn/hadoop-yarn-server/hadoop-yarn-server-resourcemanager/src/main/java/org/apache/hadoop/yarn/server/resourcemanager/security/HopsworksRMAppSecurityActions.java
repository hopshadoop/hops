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
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
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
import java.util.concurrent.atomic.AtomicReference;

public class HopsworksRMAppSecurityActions implements RMAppSecurityActions, Configurable {
  public static final String HOPSWORKS_JWT_KEY = YarnConfiguration.RM_PREFIX +
      YarnConfiguration.JWT_PREFIX + "token";
  public static final String REVOKE_CERT_ID_PARAM = "certId";
  
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppSecurityActions.class);
  private static final Set<Integer> ACCEPTABLE_HTTP_RESPONSES = new HashSet<>(2);
  private static final String AUTH_HEADER_CONTENT = "Bearer %s";
  private final AtomicReference<Header> authHeader;
  
  private Configuration conf;
  private Configuration sslConf;
  private URL hopsworksHost;
  private URL loginEndpoint;
  // X.509
  private URL signEndpoint;
  private String revokePath;
  private CertificateFactory certificateFactory;
  // JWT
  private URL jwtGeneratePath;
  private URL jwtInvalidatePath;
  private String jwt;
  
  private PoolingHttpClientConnectionManager httpConnectionManager = null;
  protected CloseableHttpClient httpClient = null;
  
  public HopsworksRMAppSecurityActions() throws MalformedURLException, GeneralSecurityException {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
    authHeader = new AtomicReference<>();
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
  public void init() throws MalformedURLException, GeneralSecurityException, IOException {
    httpConnectionManager = new PoolingHttpClientConnectionManager();
    httpConnectionManager.setDefaultMaxPerRoute(50);
    httpClient = createHttpClient(httpConnectionManager);
    
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
    
    jwtGeneratePath = new URL(hopsworksHost,
        conf.get(YarnConfiguration.RM_JWT_GENERATE_PATH,
            YarnConfiguration.DEFAULT_RM_JWT_GENERATE_PATH));
    String jwtInvalidatePathConf = conf.get(YarnConfiguration.RM_JWT_INVALIDATE_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_INVALIDATE_PATH);
    if (!jwtInvalidatePathConf.endsWith("/")) {
      jwtInvalidatePathConf = jwtInvalidatePathConf + "/";
    }
    jwtInvalidatePath = new URL(hopsworksHost, jwtInvalidatePathConf);
    
    certificateFactory = CertificateFactory.getInstance("X.509", "BC");
    sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
    jwt = sslConf.get(HOPSWORKS_JWT_KEY);
    if (jwt == null) {
      throw new GeneralSecurityException("Could not parse JWT from configuration");
    }
    authHeader.set(createAuthenticationHeader(jwt));
  }
  
  @Override
  public void destroy() {
    if (httpConnectionManager != null) {
      httpConnectionManager.shutdown();
    }
  }
  
  @Override
  public X509SecurityHandler.CertificateBundle sign(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpResponse signResponse = null;
    try {
      String csrStr = stringifyCSR(csr);
      JsonObject json = new JsonObject();
      json.addProperty("csr", csrStr);
      
      signResponse = post(json, signEndpoint.toURI(),
          "Hopsworks CA could not sign CSR");
      
      String signResponseEntity = EntityUtils.toString(signResponse.getEntity());
      JsonObject jsonResponse = new JsonParser().parse(signResponseEntity).getAsJsonObject();
      String signedCert = jsonResponse.get("signedCert").getAsString();
      X509Certificate certificate = parseCertificate(signedCert);
      String intermediateCaCert = jsonResponse.get("intermediateCaCert").getAsString();
      X509Certificate issuer = parseCertificate(intermediateCaCert);
      return new X509SecurityHandler.CertificateBundle(certificate, issuer);
    } finally {
      if (signResponse != null) {
        signResponse.close();
      }
    }
  }
  
  @Override
  public int revoke(String certificateIdentifier) throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpResponse response = null;
    try {
      String queryParams = buildQueryParams(new BasicNameValuePair(REVOKE_CERT_ID_PARAM, certificateIdentifier));
      URL revokeUrl = buildUrl(revokePath, queryParams);
      
      response = delete(revokeUrl.toURI(),
          "Hopsworks CA could not revoke certificate " + certificateIdentifier);
      return response.getStatusLine().getStatusCode();
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @Override
  public String generateJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter)
      throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpResponse response = null;
    try {
      JsonObject json = new JsonObject();
      json.addProperty("subject", jwtParameter.getAppUser());
      json.addProperty("keyName", jwtParameter.getApplicationId().toString());
      json.addProperty("audiences", String.join(",", jwtParameter.getAudiences()));
      json.addProperty("expiresAt", jwtParameter.getExpirationDate().toEpochMilli());
      json.addProperty("notBefore", jwtParameter.getRenewNotBefore().getTime());
      json.addProperty("renewable", jwtParameter.isRenewable());
      json.addProperty("expLeeway", jwtParameter.getExpLeeway());
      
      response = post(json, jwtGeneratePath.toURI(),
          "Hopsworks could not generate JWT for " + jwtParameter.getAppUser()
              + "/" + jwtParameter.getApplicationId().toString());
      String responseStr = EntityUtils.toString(response.getEntity());
      JsonObject jsonResponse = new JsonParser().parse(responseStr).getAsJsonObject();
      return jsonResponse.get("token").getAsString();
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @Override
  public void invalidateJWT(String signingKeyName)
      throws URISyntaxException, IOException, GeneralSecurityException {
    CloseableHttpResponse response = null;
    try {
      URL invalidateURL = new URL(jwtInvalidatePath, signingKeyName);
      response = put(invalidateURL.toURI(), "Hopsworks could to invalidate JWT signing key " + signingKeyName);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  // GeneralSecurityException is thrown in DevHopsworksRMAppSecurityActions
  protected synchronized CloseableHttpClient createHttpClient(PoolingHttpClientConnectionManager connectionManager)
      throws GeneralSecurityException, IOException {
    if (httpClient == null) {
      return HttpClients.custom().setConnectionManager(connectionManager).build();
    }
    return httpClient;
  }
  
  private CloseableHttpResponse post(JsonObject jsonEntity, URI target, String errorMessage)
      throws IOException {
    HttpPost request = new HttpPost(target);
    addAuthenticationHeader(request);
    request.setEntity(new StringEntity(jsonEntity.toString()));
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private CloseableHttpResponse delete(URI target, String errorMessage)
      throws IOException {
    HttpDelete request = new HttpDelete(target);
    addAuthenticationHeader(request);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "text/plain");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private CloseableHttpResponse put(URI target, String errorMessage)
    throws IOException {
    HttpPut request = new HttpPut(target);
    addAuthenticationHeader(request);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response.getStatusLine().getStatusCode(), errorMessage);
    return response;
  }
  
  private URL buildUrl(String apiUrl, String queryParams) throws MalformedURLException {
    String url = String.format(apiUrl, hopsworksHost.toString()) + queryParams;
    return new URL(url);
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
  
  private Header createAuthenticationHeader(String jwt) {
    String content = String.format(AUTH_HEADER_CONTENT, jwt);
    return new BasicHeader(HttpHeaders.AUTHORIZATION, content);
  }
  
  private void addAuthenticationHeader(HttpRequest httpRequest) {
    httpRequest.addHeader(authHeader.get());
  }
}
