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
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
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
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
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
  private static final Pattern SUBJECT_USERNAME = Pattern.compile("^(.+)(?>_{2})(.+)$");
  
  private final AtomicReference<Header> authHeader;
  private final Gson jsonParser;
  
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
  private URL jwtRenewPath;
  private URL serviceJWTRenewPath;
  private URL serviceJWTInvalidatePath;
  private long serviceJWTValidityPeriodSeconds;
  private boolean jwtConfigured = false;
  private String masterToken;
  private LocalDateTime masterTokenExpiration;
  private String[] renewalTokens;
  
  private PoolingHttpClientConnectionManager httpConnectionManager = null;
  protected CloseableHttpClient httpClient = null;
  private final ExecutorService tokenRenewer;
  
  public HopsworksRMAppSecurityActions() throws MalformedURLException, GeneralSecurityException {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
    authHeader = new AtomicReference<>();
    GsonBuilder parserBuilder = new GsonBuilder();
    parserBuilder.setFieldNamingPolicy(FieldNamingPolicy.IDENTITY);
    parserBuilder.setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    jsonParser = parserBuilder.create();
    
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
    
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)
        && conf.getBoolean(YarnConfiguration.RM_JWT_ENABLED, YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      initJWT();
    } else if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      initJWT();
      initX509();
    }
  }
  
  @VisibleForTesting
  protected void setMasterToken(String masterToken) {
    this.masterToken = masterToken;
  }
  
  @VisibleForTesting
  protected void setMasterTokenExpiration(LocalDateTime masterTokenExpiration) {
    this.masterTokenExpiration = masterTokenExpiration;
  }
  
  @VisibleForTesting
  protected void setRenewalTokens(String[] renewalTokens) {
    this.renewalTokens = renewalTokens;
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
  
    jwtRenewPath = new URL(hopsworksHost,
        conf.get(YarnConfiguration.RM_JWT_RENEW_PATH,
            YarnConfiguration.DEFAULT_RM_JWT_RENEW_PATH));
    
    sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
    
    loadMasterJWT();
    loadRenewalJWTs();
    
    serviceJWTValidityPeriodSeconds = conf.getTimeDuration(YarnConfiguration.RM_JWT_MASTER_VALIDITY_PERIOD,
        YarnConfiguration.DEFAULT_RM_JWT_MASTER_VALIDITY_PERIOD, TimeUnit.SECONDS);
    if (serviceJWTValidityPeriodSeconds == 0) {
      serviceJWTValidityPeriodSeconds = 30L;
    }
    
    String serviceJWTRenewPathConf = conf.get(YarnConfiguration.RM_JWT_SERVICE_RENEW_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_SERVICE_RENEW_PATH);
    serviceJWTRenewPath = new URL(hopsworksHost, serviceJWTRenewPathConf);
    String serviceJWTInvalidatePathConf = conf.get(YarnConfiguration.RM_JWT_SERVICE_INVALIDATE_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_SERVICE_INVALIDATE_PATH);
    if (!serviceJWTInvalidatePathConf.endsWith("/")) {
      serviceJWTInvalidatePathConf = serviceJWTRenewPathConf + "/";
    }
    serviceJWTInvalidatePath = new URL(hopsworksHost, serviceJWTInvalidatePathConf);
    
    tokenRenewer.execute(new TokenRenewer());
    jwtConfigured = true;
  }
  
  protected void loadMasterJWT() throws GeneralSecurityException {
    masterToken = sslConf.get(YarnConfiguration.RM_JWT_MASTER_TOKEN);
    if (masterToken == null) {
      throw new GeneralSecurityException("Could not parse JWT from configuration");
    }
    authHeader.set(createAuthenticationHeader(masterToken));
    try {
      JWT jwt = JWTParser.parse(masterToken);
      masterTokenExpiration = DateUtils.date2LocalDateTime(jwt.getJWTClaimsSet().getExpirationTime());
    } catch (ParseException ex) {
      throw new GeneralSecurityException("Could not parse master JWT", ex);
    }
  }
  
  protected void loadRenewalJWTs() throws GeneralSecurityException {
    String renewToken = null;
    List<String> renewalTokens = new ArrayList<>();
    int idx = 0;
    while (true) {
      String renewTokenKey = String.format(YarnConfiguration.RM_JWT_RENEW_TOKEN_PATTERN, idx);
      renewToken = sslConf.get(renewTokenKey, "");
      if (renewToken.isEmpty()) {
        break;
      }
      renewalTokens.add(renewToken);
      idx++;
    }
    if (renewalTokens.isEmpty()) {
      throw new GeneralSecurityException("Could not load one-time renewal JWTs");
    }
    this.renewalTokens = renewalTokens.toArray(new String[renewalTokens.size()]);
  }
  
  @Override
  public void destroy() {
    try {
      tokenRenewer.shutdown();
      if (!tokenRenewer.awaitTermination(10L, TimeUnit.SECONDS)) {
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
      CSRDTO request = new CSRDTO();
      request.csr = csrStr;
      String jsonRequest = jsonParser.toJson(request);
      
      signResponse = post(new StringEntity(jsonRequest), signEndpoint.toURI(),"Hopsworks CA could not sign CSR");
      CSRDTO csrResponse = jsonParser.fromJson(EntityUtils.toString(signResponse.getEntity()),
          CSRDTO.class);
      X509Certificate certificate = parseCertificate(csrResponse.signedCert);
      X509Certificate issuer = parseCertificate(csrResponse.intermediateCaCert);
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
      // Application user is the Project Specific User
      // We must extract the username out of the PSU
      // If we fail, then fall-back to application submitter
      // some endpoints in Hopsworks will not work though as
      // that user might not be a registered Hopsworks user.
      Matcher matcher = SUBJECT_USERNAME.matcher(jwtParameter.getAppUser());
      String username = matcher.matches() ? matcher.group(2) : jwtParameter.getAppUser();
      
      JWTDTO request = new JWTDTO();
      request.subject = username;
      request.keyName = jwtParameter.getApplicationId().toString();
      request.audiences = String.join(",", jwtParameter.getAudiences());
      request.expiresAt = DateUtils.localDateTime2Date(jwtParameter.getExpirationDate());
      request.nbf = DateUtils.localDateTime2Date(jwtParameter.getValidNotBefore());
      request.renewable = jwtParameter.isRenewable();
      request.expLeeway = jwtParameter.getExpLeeway();
      
      String jsonRequest = jsonParser.toJson(request);
      response = post(new StringEntity(jsonRequest), jwtGeneratePath.toURI(),
          "Hopsworks could not generate JWT for " + jwtParameter.getAppUser()
              + "/" + jwtParameter.getApplicationId().toString());
      
      JWTDTO jwtResponse = jsonParser.fromJson(EntityUtils.toString(response.getEntity()), JWTDTO.class);
      return jwtResponse.token;
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @Override
  public String renewJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter)
      throws URISyntaxException, IOException, GeneralSecurityException {
    if (!jwtConfigured) {
      jwtNotConfigured("renewJWT");
    }
    CloseableHttpResponse response = null;
    try {
      JWTDTO request = new JWTDTO();
      request.token = jwtParameter.getToken();
      request.expiresAt = DateUtils.localDateTime2Date(jwtParameter.getExpirationDate());
      request.nbf = DateUtils.localDateTime2Date(jwtParameter.getValidNotBefore());
      String jsonRequest = jsonParser.toJson(request);
      response = put(jwtRenewPath.toURI(), new StringEntity(jsonRequest), "Could not renew JWT for "
          + jwtParameter.getAppUser() + "/" + jwtParameter.getApplicationId());
  
      JWTDTO jwtResponse = jsonParser.fromJson(EntityUtils.toString(response.getEntity()), JWTDTO.class);
      return jwtResponse.token;
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
      response = delete(invalidateURL.toURI(), "Hopsworks could to invalidate JWT signing key " + signingKeyName);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @VisibleForTesting
  LocalDateTime getMasterTokenExpiration() {
    return masterTokenExpiration;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected ServiceTokenDTO renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
      LocalDateTime notBefore) throws URISyntaxException, IOException, GeneralSecurityException {
    if (!jwtConfigured) {
      jwtNotConfigured("renewServiceJWT");
    }
    CloseableHttpResponse httpResponse = null;
    try {
      JWTDTO request = new JWTDTO();
      request.token = token;
      request.expiresAt = DateUtils.localDateTime2Date(expiresAt);
      request.nbf = DateUtils.localDateTime2Date(notBefore);
      String jsonRequest = jsonParser.toJson(request);
      HttpPut httpRequest = new HttpPut(serviceJWTRenewPath.toURI());
      Header authHeader = createAuthenticationHeader(oneTimeToken);
      httpRequest.addHeader(authHeader);
      httpRequest.setEntity(new StringEntity(jsonRequest));
      httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      httpResponse = httpClient.execute(httpRequest);
      checkHTTPResponseCode(httpResponse, "Could not make HTTP request to renew service JWT");
      ServiceTokenDTO renewedTokenResponse = jsonParser.fromJson(EntityUtils.toString(httpResponse.getEntity()),
          ServiceTokenDTO.class);
      return renewedTokenResponse;
    } finally {
      if (httpResponse != null) {
        httpResponse.close();
      }
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void invalidateServiceJWT(String token2invalidate)
      throws URISyntaxException, IOException, GeneralSecurityException {
    if (!jwtConfigured) {
      jwtNotConfigured("invalidateServiceToken");
    }
    CloseableHttpResponse httpResponse = null;
    try {
      URL invalidateURL = new URL(serviceJWTInvalidatePath, token2invalidate);
      httpResponse = delete(invalidateURL.toURI(), "Could not invalidate token " + token2invalidate);
    } finally {
      if (httpResponse != null) {
        httpResponse.close();
      }
    }
  }
  
  private CloseableHttpResponse post(HttpEntity httpEntity, URI target, String errorMessage)
      throws IOException {
    HttpPost request = new HttpPost(target);
    addAuthenticationHeader(request);
    request.setEntity(httpEntity);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response, errorMessage);
    return response;
  }
  
  private CloseableHttpResponse get(URI target, String errorMessage) throws IOException {
    HttpGet request = new HttpGet(target);
    addAuthenticationHeader(request);
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response, errorMessage);
    return response;
  }
  
  private CloseableHttpResponse delete(URI target, String errorMessage)
      throws IOException {
    HttpDelete request = new HttpDelete(target);
    addAuthenticationHeader(request);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response, errorMessage);
    return response;
  }
  
  private CloseableHttpResponse put(URI target, String errorMessage) throws IOException {
    return put(target, null, errorMessage);
  }
  
  private CloseableHttpResponse put(URI target, HttpEntity httpEntity, String errorMessage)
    throws IOException {
    HttpPut request = new HttpPut(target);
    addAuthenticationHeader(request);
    if (httpEntity != null) {
      request.setEntity(httpEntity);
    }
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    CloseableHttpResponse response = httpClient.execute(request);
    checkHTTPResponseCode(response, errorMessage);
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
  
  private void checkHTTPResponseCode(HttpResponse response, String msg) throws IOException {
    int statusCode = response.getStatusLine().getStatusCode();
    if (!ACCEPTABLE_HTTP_RESPONSES.contains(statusCode)) {
      throw new IOException("HTTP error, response code " + statusCode + " Reason: " + response.getStatusLine()
          .getReasonPhrase() + " Message: " + msg);
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
  
  protected boolean isTime2Renew(LocalDateTime now, LocalDateTime tokenExpiration) {
    return now.isAfter(tokenExpiration) || now.isEqual(tokenExpiration);
  }
  
  private class TokenRenewer implements Runnable {
    private final BackOff backoff;
    private final long sleepPeriodSeconds;
    
    private TokenRenewer() {
      int maximumRetries = Math.max(1, renewalTokens.length);
      backoff = new ExponentialBackOff.Builder()
          .setInitialIntervalMillis(1000)
          .setMaximumIntervalMillis(7000)
          .setMultiplier(2)
          .setMaximumRetries(maximumRetries)
          .build();
      sleepPeriodSeconds = serviceJWTValidityPeriodSeconds / 2;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          // Check if it is time to renew
          LocalDateTime now = DateUtils.getNow();
          if (isTime2Renew(now, masterTokenExpiration)) {
            backoff.reset();
            LocalDateTime expiresAt = DateUtils.getNow().plus(serviceJWTValidityPeriodSeconds, ChronoUnit.SECONDS);
            int renewalTokenIdx = 0;
            while (renewalTokenIdx < renewalTokens.length) {
              try {
                // Use one-time token to authenticate
                ServiceTokenDTO renewedTokens = renewServiceJWT(masterToken, renewalTokens[renewalTokenIdx],
                    expiresAt, now);
                String oldMasterToken = masterToken;
                masterToken = renewedTokens.jwt.token;
                masterTokenExpiration = DateUtils.date2LocalDateTime(renewedTokens.jwt.expiresAt);
                authHeader.set(createAuthenticationHeader(masterToken));
                renewalTokens = renewedTokens.renewTokens;
                try {
                  // Since renewal of master JWT has gone through, invalidate the old one
                  invalidateServiceJWT(oldMasterToken);
                } catch (Exception ex) {
                  // Do not retry if we failed to invalidate old master token
                  LOG.warn("Failed to invalidate old service master JWT. Continue...");
                }
                sslConf.set(YarnConfiguration.RM_JWT_MASTER_TOKEN, masterToken);
                for (int i = 0; i < renewalTokens.length; i++) {
                  String confKey = String.format(YarnConfiguration.RM_JWT_RENEW_TOKEN_PATTERN, i);
                  sslConf.set(confKey, renewalTokens[i]);
                }
                URL sslServerURL = sslConf.getResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
                File sslServerFile = new File(sslServerURL.getFile());
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(sslServerFile))) {
                  sslConf.writeXml(bos);
                  bos.flush();
                }
                LOG.info("Updated service JWT");
                break;
              } catch (URISyntaxException ex) {
                LOG.error("There is an error in service JWT renewal URI: " + serviceJWTRenewPath.toString(), ex);
                break;
              } catch (Exception ex) {
                // If for some reason we fail to parse the new token,
                // we retry and invalidate the failed token
                renewalTokenIdx++;
                long backoffTimeout = backoff.getBackOffInMillis();
                if (backoffTimeout != -1) {
                  LOG.warn("Error while trying to renew service JWT. Retrying in " + backoffTimeout + " ms", ex);
                  TimeUnit.MILLISECONDS.sleep(backoffTimeout);
                } else {
                  LOG.error("Could not renew service JWT. Manual update is necessary!", ex);
                  break;
                }
              }
            }
          }
          TimeUnit.SECONDS.sleep(sleepPeriodSeconds);
        } catch (InterruptedException ex) {
          LOG.warn("Service JWT renewer has been interrupted");
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
  /**
   * Classes to serialize HTTP responses from Hopsworks
   *
   * Fields name should match the response from Hopsworks
   */
  
  private class CSRDTO {
    private String csr;
    private String signedCert;
    private String intermediateCaCert;
    private String rootCaCert;
  }
  
  protected class JWTDTO {
    private String token;
    private String subject;
    private String keyName;
    private String audiences;
    private Boolean renewable;
    private Integer expLeeway;
    private Date expiresAt;
    private Date nbf;
  
    public String getToken() {
      return token;
    }
  
    public void setToken(String token) {
      this.token = token;
    }
  
    public String getSubject() {
      return subject;
    }
  
    public void setSubject(String subject) {
      this.subject = subject;
    }
  
    public String getKeyName() {
      return keyName;
    }
  
    public void setKeyName(String keyName) {
      this.keyName = keyName;
    }
  
    public String getAudiences() {
      return audiences;
    }
  
    public void setAudiences(String audiences) {
      this.audiences = audiences;
    }
  
    public Boolean getRenewable() {
      return renewable;
    }
  
    public void setRenewable(Boolean renewable) {
      this.renewable = renewable;
    }
  
    public Integer getExpLeeway() {
      return expLeeway;
    }
  
    public void setExpLeeway(Integer expLeeway) {
      this.expLeeway = expLeeway;
    }
  
    public Date getExpiresAt() {
      return expiresAt;
    }
  
    public void setExpiresAt(Date expiresAt) {
      this.expiresAt = expiresAt;
    }
  
    public Date getNbf() {
      return nbf;
    }
  
    public void setNbf(Date nbf) {
      this.nbf = nbf;
    }
  }
  
  protected class ServiceTokenDTO {
    private JWTDTO jwt;
    private String[] renewTokens;
  
    public JWTDTO getJwt() {
      return jwt;
    }
  
    public void setJwt(JWTDTO jwt) {
      this.jwt = jwt;
    }
  
    public String[] getRenewTokens() {
      return renewTokens;
    }
  
    public void setRenewTokens(String[] renewTokens) {
      this.renewTokens = renewTokens;
    }
  }
}
