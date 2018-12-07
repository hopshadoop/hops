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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HopsworksRMAppSecurityActions implements RMAppSecurityActions, Configurable {
  public static final String REVOKE_CERT_ID_PARAM = "certId";
  public static final Pattern JWT_PATTERN = Pattern.compile("^Bearer\\s(.+)");
  
  protected static final int MAX_CONNECTIONS_PER_ROUTE = 50;
  
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppSecurityActions.class);
  private static final Set<Integer> ACCEPTABLE_HTTP_RESPONSES = new HashSet<>(2);
  private static final String AUTH_HEADER_CONTENT = "Bearer %s";
  
  private final AtomicReference<Header> authHeader;
  
  private Configuration conf;
  private Configuration sslConf;
  private URL hopsworksHost;
  // X.509
  private URL signEndpoint;
  private String revokePath;
  private CertificateFactory certificateFactory;
  private boolean x509Configured = false;
  // JWT
  private URL jwtGeneratePath;
  private URL jwtInvalidatePath;
  private URL jwtAlivePath;
  private long jwtAliveIntervalSeconds;
  private boolean jwtConfigured = false;
  
  private PoolingHttpClientConnectionManager httpConnectionManager = null;
  protected CloseableHttpClient httpClient = null;
  private final ExecutorService tokenRenewer;
  
  public HopsworksRMAppSecurityActions() throws MalformedURLException, GeneralSecurityException {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
    authHeader = new AtomicReference<>();
    tokenRenewer = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("JWT renewer thread")
        .setDaemon(true)
        .build());
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
    httpConnectionManager = createConnectionManager();
    httpClient = HttpClients.custom().setConnectionManager(httpConnectionManager).build();
    
    hopsworksHost = new URL(conf.get(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY,
        "http://127.0.0.1"));
    
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      initX509();
    }
    
    if (conf.getBoolean(YarnConfiguration.RM_JWT_ENABLED, YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      initJWT();
    }
  }
  
  protected PoolingHttpClientConnectionManager createConnectionManager() throws GeneralSecurityException {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
    return connectionManager;
  }
  
  private void initX509() throws MalformedURLException, GeneralSecurityException {
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
    x509Configured = true;
  }
  
  private void initJWT() throws MalformedURLException, GeneralSecurityException {
    jwtGeneratePath = new URL(hopsworksHost,
        conf.get(YarnConfiguration.RM_JWT_GENERATE_PATH,
            YarnConfiguration.DEFAULT_RM_JWT_GENERATE_PATH));
    String jwtInvalidatePathConf = conf.get(YarnConfiguration.RM_JWT_INVALIDATE_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_INVALIDATE_PATH);
    if (!jwtInvalidatePathConf.endsWith("/")) {
      jwtInvalidatePathConf = jwtInvalidatePathConf + "/";
    }
    jwtInvalidatePath = new URL(hopsworksHost, jwtInvalidatePathConf);
  
  
    sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
    String jwtConf = sslConf.get(YarnConfiguration.RM_JWT_TOKEN);
    if (jwtConf == null) {
      throw new GeneralSecurityException("Could not parse JWT from configuration");
    }
    authHeader.set(createAuthenticationHeader(jwtConf));
  
    jwtAlivePath = new URL(hopsworksHost,
        conf.get(YarnConfiguration.RM_JWT_ALIVE_PATH, YarnConfiguration.DEFAULT_RM_JWT_ALIVE_PATH));
    jwtAliveIntervalSeconds = conf.getTimeDuration(YarnConfiguration.RM_JWT_ALIVE_INTERVAL,
        YarnConfiguration.DEFAULT_RM_JWT_ALIVE_INTERVAL, TimeUnit.SECONDS);
    tokenRenewer.execute(new TokenRenewer());
    jwtConfigured = true;
  }
  
  @Override
  public void destroy() {
    try {
      tokenRenewer.shutdown();
      if (!tokenRenewer.awaitTermination(1L, TimeUnit.SECONDS)) {
        tokenRenewer.shutdownNow();
      }
    } catch (InterruptedException ex) {
      tokenRenewer.shutdownNow();
    }
    if (httpConnectionManager != null) {
      httpConnectionManager.shutdown();
    }
  }
  
  private void x509NotConfigured(String methodName) throws GeneralSecurityException {
    notConfigured(methodName, "X.509");
  }
  
  private void jwtNotConfigured(String methodName) throws GeneralSecurityException {
    notConfigured(methodName, "JWT");
  }
  
  private void notConfigured(String methodName, String mechanism) throws GeneralSecurityException {
    throw new GeneralSecurityException("Called method " + methodName + " of "
        + HopsworksRMAppSecurityActions.class.getSimpleName() + " but " + mechanism + " is not configured");
  }
  
  @Override
  public X509SecurityHandler.CertificateBundle sign(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    if (!x509Configured) {
      x509NotConfigured("sign");
    }
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
    if (!x509Configured) {
      x509NotConfigured("revoke");
    }
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
    if (!jwtConfigured) {
      jwtNotConfigured("generateJWT");
    }
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
    if (!jwtConfigured) {
      jwtNotConfigured("invalidateJWT");
    }
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
  
  private CloseableHttpResponse get(URI target, String errorMessage) throws IOException {
    HttpGet request = new HttpGet(target);
    addAuthenticationHeader(request);
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
  
  @VisibleForTesting
  protected Header createAuthenticationHeader(String jwt) {
    String content = String.format(AUTH_HEADER_CONTENT, jwt);
    return new BasicHeader(HttpHeaders.AUTHORIZATION, content);
  }
  
  private void addAuthenticationHeader(HttpRequest httpRequest) {
    httpRequest.addHeader(authHeader.get());
  }
  
  protected String getJWTFromResponse() throws IOException, URISyntaxException {
    CloseableHttpResponse response = null;
    try {
      response = get(jwtAlivePath.toURI(), " Could not ping Hopsworks to renew JWT");
      if (!response.containsHeader(HttpHeaders.AUTHORIZATION)) {
        // JWT is sent only when the previous has expired
        return null;
      }
      Header[] authHeaders = response.getHeaders(HttpHeaders.AUTHORIZATION);
      for (Header header : authHeaders) {
        Matcher matcher = JWT_PATTERN.matcher(header.getValue());
        if (matcher.matches()) {
          return matcher.group(1);
        }
      }
      throw new IOException("Could not extract JWT from authentication header");
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  private class TokenRenewer implements Runnable {
    private final BackOff backoff;
    private long backoffTime = 0L;
    
    private TokenRenewer() {
      backoff = new ExponentialBackOff.Builder()
          .setInitialIntervalMillis(800)
          .setMaximumIntervalMillis(5000)
          .setMultiplier(1.5)
          .setMaximumRetries(4)
          .build();
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          String jwt = getJWTFromResponse();
          
          if (jwt != null
              && !authHeader.get().getValue().equals(String.format(AUTH_HEADER_CONTENT, jwt))) {
            authHeader.set(createAuthenticationHeader(jwt));
            sslConf.set(YarnConfiguration.RM_JWT_TOKEN, jwt);
            URL sslServerURL = sslConf.getResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
            File sslServerFile = new File(sslServerURL.getFile());
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(sslServerFile))) {
              sslConf.writeXml(bos);
              bos.flush();
            }
            LOG.debug("Renewed Hopsworks JWT");
          }
          backoff.reset();
          TimeUnit.SECONDS.sleep(jwtAliveIntervalSeconds);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } catch (URISyntaxException ex) {
          // That's fatal error. Keep going until JWT expires
          LOG.fatal(ex, ex);
          Thread.currentThread().interrupt();
        } catch (IOException ex) {
          backoffTime = backoff.getBackOffInMillis();
          if (backoffTime != -1) {
            LOG.warn(ex + "Retrying in " + backoffTime + "ms", ex);
            try {
              TimeUnit.MILLISECONDS.sleep(backoffTime);
            } catch (InterruptedException iex) {
              Thread.currentThread().interrupt();
            }
          } else {
            LOG.fatal(ex, ex);
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }
}
