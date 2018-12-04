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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestHopsworksRMAppSecurityActions {
  private final static Log LOG = LogFactory.getLog(TestHopsworksRMAppSecurityActions.class);
  private final static String HOPSWORKS_ENDPOINT = "http://bbc4.sics.se:60675";
  private final static String HOPSWORKS_USER = "agent@hops.io";
  private final static String HOPSWORKS_PASSWORD = "admin";
  
  private static final String HOPSWORKS_LOGIN_PATH = "/hopsworks-api/api/auth/login";
  private static final String CN = UUID.randomUUID().toString();
  private static final String O = "application_id";
  private static final String OU = "1";
  private static final Pattern JWT_PATTERN = Pattern.compile("^Bearer\\s(.+)");
  
  private static String classPath;
  
  private RMAppSecurityActions actions;
  private Path sslServerPath;
  private Configuration conf;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    classPath = KeyStoreTestUtil.getClasspathDir(TestHopsworksRMAppSecurityActions.class);
  }
  
  @Before
  public void beforeTest() throws Exception {
    RMAppSecurityActionsFactory.getInstance().clear();
    conf = new Configuration();
    String sslConfFilename = TestHopsworksRMAppSecurityActions.class.getSimpleName() + ".ssl-server.xml";
    sslServerPath = Paths.get(classPath, sslConfFilename);
    String jwt = loginAndGetJWT();
    
    Configuration sslServer = new Configuration(false);
    sslServer.set(HopsworksRMAppSecurityActions.HOPSWORKS_JWT_KEY, jwt);
    KeyStoreTestUtil.saveConfig(sslServerPath.toFile(), sslServer);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFilename);
    conf.set(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY, HOPSWORKS_ENDPOINT);
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".security.HopsworksRMAppSecurityActions");
  }
  
  @After
  public void afterTest() {
    if (sslServerPath != null) {
      sslServerPath.toFile().delete();
    }
  }
  
  @Test
  public void testSign() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGenerator.initialize(1024);
    KeyPair keyPair = keyPairGenerator.genKeyPair();
  
    X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    x500NameBuilder.addRDN(BCStyle.CN, CN);
    x500NameBuilder.addRDN(BCStyle.O, O);
    x500NameBuilder.addRDN(BCStyle.OU, OU);
    X500Name x500Name = x500NameBuilder.build();
  
    PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(x500Name, keyPair
        .getPublic());
    PKCS10CertificationRequest csr = csrBuilder.build(
        new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC")
            .build(keyPair.getPrivate()));
  
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    X509SecurityHandler.CertificateBundle singedBundle = actor.sign(csr);
    Assert.assertNotNull(singedBundle);
  }
  
  @Test
  public void testRevoke() throws Exception {
    testSign();
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    int response = actor.revoke(CN + "__" + O + "__" + OU);
    Assert.assertEquals(200, response);
  }
  
  @Test
  public void testGenerateJWT() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = new JWTSecurityHandler.JWTMaterialParameter(appId, "Flock");
    jwtParam.setRenewable(false);
    Instant in10Minutes = Instant.now().plus(10, ChronoUnit.MINUTES);
    jwtParam.setExpirationDate(in10Minutes);
    jwtParam.setRenewNotBefore(new Date(in10Minutes.toEpochMilli()));
    jwtParam.setAudiences(new String[]{"job"});
    
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    String jwt = actor.generateJWT(jwtParam);
    Assert.assertNotNull(jwt);
  }
  
  @Test
  public void testInvalidateJWT() throws Exception {
    String signingKeyName = "lala";
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    actor.invalidateJWT(signingKeyName);
  }
  
  private String loginAndGetJWT() throws Exception {
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
  
      for (Header h : authHeaders) {
        Matcher matcher = JWT_PATTERN.matcher(h.getValue());
        if (matcher.matches()) {
          return matcher.group(1);
        }
      }
      throw new IOException("Could not get JWT from Hopsworks");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
