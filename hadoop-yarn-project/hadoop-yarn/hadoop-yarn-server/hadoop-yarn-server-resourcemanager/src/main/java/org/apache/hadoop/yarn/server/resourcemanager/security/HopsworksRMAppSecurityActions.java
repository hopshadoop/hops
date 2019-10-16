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

import io.hops.security.AbstractSecurityActions;
import io.hops.security.ServiceJWTManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObjectGenerator;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HopsworksRMAppSecurityActions extends AbstractSecurityActions implements RMAppSecurityActions {
  public static final String REVOKE_CERT_ID_PARAM = "certId";
  
  protected static final int MAX_CONNECTIONS_PER_ROUTE = 50;
  
  private static final Log LOG = LogFactory.getLog(HopsworksRMAppSecurityActions.class);
  private static final Pattern SUBJECT_USERNAME = Pattern.compile("^(.+)(?>_{2})(.+)$");
  
  
  // X.509
  private URI signEndpoint;
  private URI revokePath;
  private CertificateFactory certificateFactory;
  private boolean x509Configured = false;
  // JWT
  private URI jwtGeneratePath;
  private URI jwtInvalidatePath;
  private URI jwtRenewPath;
  private boolean jwtConfigured = false;
  
  public HopsworksRMAppSecurityActions() {
    super("HopsworksRMAppSecurityActions");
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)
        && conf.getBoolean(YarnConfiguration.RM_JWT_ENABLED, YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      super.serviceInit(conf);
      initJWT(conf);
    } else if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      super.serviceInit(conf);
      initJWT(conf);
      initX509(conf);
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    if (getConfig().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
            CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT) ||
        getConfig().getBoolean(YarnConfiguration.RM_JWT_ENABLED,
            YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      super.serviceStart();
    }
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (getConfig().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
            CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT) ||
        getConfig().getBoolean(YarnConfiguration.RM_JWT_ENABLED,
            YarnConfiguration.DEFAULT_RM_JWT_ENABLED)) {
      super.serviceStop();
    }
  }
  
  private void initX509(Configuration conf) throws URISyntaxException, GeneralSecurityException {
    signEndpoint = new URI(conf.get(YarnConfiguration.HOPS_HOPSWORKS_SIGN_ENDPOINT_KEY,
            YarnConfiguration.DEFAULT_HOPS_HOPSWORKS_SIGN_ENDPOINT));
    
    revokePath = new URI(conf.get(YarnConfiguration.HOPS_HOPSWORKS_REVOKE_ENDPOINT_KEY,
        YarnConfiguration.DEFAULT_HOPS_HOPSWORKS_REVOKE_ENDPOINT));
    
    certificateFactory = CertificateFactory.getInstance("X.509", "BC");
    x509Configured = true;
  }
  
  private void initJWT(Configuration conf) throws URISyntaxException {
    jwtGeneratePath = new URI(conf.get(YarnConfiguration.RM_JWT_GENERATE_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_GENERATE_PATH));
    
    String jwtInvalidatePathConf = conf.get(YarnConfiguration.RM_JWT_INVALIDATE_PATH,
        YarnConfiguration.DEFAULT_RM_JWT_INVALIDATE_PATH);
    if (!jwtInvalidatePathConf.endsWith("/")) {
      jwtInvalidatePathConf = jwtInvalidatePathConf + "/";
    }
    jwtInvalidatePath = new URI(jwtInvalidatePathConf);
  
    jwtRenewPath = new URI(conf.get(YarnConfiguration.RM_JWT_RENEW_PATH,
            YarnConfiguration.DEFAULT_RM_JWT_RENEW_PATH));
    jwtConfigured = true;
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
      throws IOException, GeneralSecurityException {
    if (!x509Configured) {
      x509NotConfigured("sign");
    }
    CloseableHttpResponse signResponse = null;
    try {
      String csrStr = stringifyCSR(csr);
      CSRDTO payload = new CSRDTO();
      payload.csr = csrStr;
      
      HttpPost request = new HttpPost(signEndpoint);
      request.setEntity(new StringEntity(parser.toJson(payload)));
      signResponse = doJSONCall(request, "Hopsworks CA could not sign CSR");
      CSRDTO csrResponse = parser.fromJson(EntityUtils.toString(signResponse.getEntity()),
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
      URI parameterizedURI = new URIBuilder(revokePath)
          .addParameter(REVOKE_CERT_ID_PARAM, certificateIdentifier)
          .build();
      HttpDelete request = new HttpDelete(parameterizedURI);
      response = doTextPlainCall(request, "Hopsworks CA could not revoke certificate " + certificateIdentifier);
      return response.getStatusLine().getStatusCode();
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @Override
  public String generateJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter)
      throws IOException, GeneralSecurityException {
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
      
      ServiceJWTManager.JWTDTO payload = new ServiceJWTManager.JWTDTO();
      payload.setSubject(username);
      payload.setKeyName(jwtParameter.getApplicationId().toString());
      payload.setAudiences(String.join(",", jwtParameter.getAudiences()));
      payload.setExpiresAt(DateUtils.localDateTime2Date(jwtParameter.getExpirationDate()));
      payload.setNbf(DateUtils.localDateTime2Date(jwtParameter.getValidNotBefore()));
      payload.setRenewable(jwtParameter.isRenewable());
      payload.setExpLeeway(jwtParameter.getExpLeeway());
      
      HttpPost request = new HttpPost(jwtGeneratePath);
      request.setEntity(new StringEntity(parser.toJson(payload)));;
      response = doJSONCall(request, "Hopsworks could not generate JWT for " + jwtParameter.getAppUser()
          + "/" + jwtParameter.getApplicationId().toString());
      
      ServiceJWTManager.JWTDTO jwtResponse = parser.fromJson(EntityUtils.toString(response.getEntity()),
          ServiceJWTManager.JWTDTO.class);
      return jwtResponse.getToken();
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  @Override
  public String renewJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter)
      throws IOException, GeneralSecurityException {
    if (!jwtConfigured) {
      jwtNotConfigured("renewJWT");
    }
    CloseableHttpResponse response = null;
    try {
      ServiceJWTManager.JWTDTO payload = new ServiceJWTManager.JWTDTO();
      payload.setToken(jwtParameter.getToken());
      payload.setExpiresAt(DateUtils.localDateTime2Date(jwtParameter.getExpirationDate()));
      payload.setNbf(DateUtils.localDateTime2Date(jwtParameter.getValidNotBefore()));
      
      HttpPut request = new HttpPut(jwtRenewPath);
      request.setEntity(new StringEntity(parser.toJson(payload)));
      response = doJSONCall(request, "Could not renew JWT for "
          + jwtParameter.getAppUser() + "/" + jwtParameter.getApplicationId());
      
      ServiceJWTManager.JWTDTO jwtResponse = parser.fromJson(EntityUtils.toString(response.getEntity()),
          ServiceJWTManager.JWTDTO.class);
      return jwtResponse.getToken();
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
      URI invalidateURI = new URI(jwtInvalidatePath.getPath() + signingKeyName);
      HttpDelete request = new HttpDelete(invalidateURI);
      response = doJSONCall(request, "Hopsworks could to invalidate JWT signing key " + signingKeyName);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  private CloseableHttpResponse doJSONCall(HttpRequest request, String errorMessage) throws IOException {
    addJSONContentType(request);
    return doCall(request, errorMessage);
  }
  
  private CloseableHttpResponse doTextPlainCall(HttpRequest request, String errorMessage) throws IOException {
    addTextPlainContentType(request);
    return doCall(request, errorMessage);
  }
  
  private CloseableHttpResponse doCall(HttpRequest request, String errorMessage) throws IOException {
    addJWTAuthHeader(request, serviceJWTManager.getMasterToken());
    CloseableHttpResponse response = httpClient.execute(remoteHost, request);
    checkHTTPResponseCode(response, errorMessage);
    return response;
  }
  
  private X509Certificate parseCertificate(String certificateStr) throws IOException, GeneralSecurityException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(certificateStr.getBytes())) {
      return (X509Certificate) certificateFactory.generateCertificate(bis);
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
}
