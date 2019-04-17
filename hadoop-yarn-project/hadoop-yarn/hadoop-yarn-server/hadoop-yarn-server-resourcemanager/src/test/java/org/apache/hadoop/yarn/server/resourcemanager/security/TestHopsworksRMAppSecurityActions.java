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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
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
import org.apache.http.util.EntityUtils;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

@Ignore
public class TestHopsworksRMAppSecurityActions {
  private final static Log LOG = LogFactory.getLog(TestHopsworksRMAppSecurityActions.class);
  private final static String HOPSWORKS_ENDPOINT = "https://host:port";
  private final static String HOPSWORKS_USER = "username";
  private final static String HOPSWORKS_PASSWORD = "password";
  
  private static final String HOPSWORKS_LOGIN_PATH = "/hopsworks-api/api/auth/service";
  private static final String O = "application_id";
  private static final String OU = "1";
  
  private static final String KEYSTORE_LOCATION = "/path/to/keystore";
  private static final String KEYSTORE_PASS = "12345";
  
  private static final String JWT_SUBJECT = "ProjectA1__Flock";
  
  private static String classPath;
  private static MockJWTIssuer jwtIssuer;
  
  private Path sslServerPath;
  private Configuration conf;
  private Configuration sslServer;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    classPath = KeyStoreTestUtil.getClasspathDir(TestHopsworksRMAppSecurityActions.class);
    byte[] jwtIssuerSecret = new byte[32];
    Random rand = new Random();
    rand.nextBytes(jwtIssuerSecret);
    jwtIssuer = new MockJWTIssuer(jwtIssuerSecret);
  }
  
  @Before
  public void beforeTest() throws Exception {
    RMAppSecurityActionsFactory.getInstance().clear();
    conf = new Configuration();
    String sslConfFilename = TestHopsworksRMAppSecurityActions.class.getSimpleName() + ".ssl-server.xml";
    sslServerPath = Paths.get(classPath, sslConfFilename);
    JWTResponse jwtResponse = loginAndGetJWT();
    
    sslServer = new Configuration(false);
    sslServer.set(YarnConfiguration.RM_JWT_MASTER_TOKEN, jwtResponse.masterToken);
    for (int i = 0; i < jwtResponse.renewalTokens.length; i++) {
      String renewalConfKey = String.format(YarnConfiguration.RM_JWT_RENEW_TOKEN_PATTERN, i);
      sslServer.set(renewalConfKey, jwtResponse.renewalTokens[i]);
    }
    
    sslServer.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), KEYSTORE_LOCATION);
    sslServer.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), KEYSTORE_PASS);
    
    KeyStoreTestUtil.saveConfig(sslServerPath.toFile(), sslServer);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFilename);
    conf.set(YarnConfiguration.HOPS_HOPSWORKS_HOST_KEY, HOPSWORKS_ENDPOINT);
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".security.DevHopsworksRMAppSecurityActions");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
  }
  
  @After
  public void afterTest() throws Exception {
    RMAppSecurityActionsFactory.getInstance().getActor(conf).destroy();
    if (sslServerPath != null) {
      sslServerPath.toFile().delete();
    }
  }
  
  @Test
  public void testSign() throws Exception {
    PKCS10CertificationRequest csr = generateCSR(UUID.randomUUID().toString());
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    X509SecurityHandler.CertificateBundle singedBundle = actor.sign(csr);
    Assert.assertNotNull(singedBundle);
  }
  
  @Test
  public void testRevoke() throws Exception {
    String cn = UUID.randomUUID().toString();
    PKCS10CertificationRequest csr = generateCSR(cn);
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    actor.sign(csr);
    int response = actor.revoke(cn + "__" + O + "__" + OU);
    Assert.assertEquals(200, response);
  }
  
  private PKCS10CertificationRequest generateCSR(String cn) throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGenerator.initialize(1024);
    KeyPair keyPair = keyPairGenerator.genKeyPair();
    
    X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    x500NameBuilder.addRDN(BCStyle.CN, cn);
    x500NameBuilder.addRDN(BCStyle.O, O);
    x500NameBuilder.addRDN(BCStyle.OU, OU);
    X500Name x500Name = x500NameBuilder.build();
    
    PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(x500Name, keyPair
        .getPublic());
    return  csrBuilder.build(new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC")
        .build(keyPair.getPrivate()));
  }
  
  @Test
  public void testGenerateJWT() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = createJWTParameter(appId);
    
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    String jwt = actor.generateJWT(jwtParam);
    Assert.assertNotNull(jwt);
    String[] tokenizedSubject = JWT_SUBJECT.split("__");
    JWT decoded = JWTParser.parse(jwt);
    String subject = decoded.getJWTClaimsSet().getSubject();
    Assert.assertEquals(tokenizedSubject[1], subject);
  }
  
  @Test
  public void testInvalidateJWT() throws Exception {
    String signingKeyName = "lala";
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    actor.invalidateJWT(signingKeyName);
  }
  
  @Test
  public void testGenerateJWTSameSigningKeyShouldFail() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = createJWTParameter(appId);
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    actor.generateJWT(jwtParam);
    
    rule.expect(IOException.class);
    actor.generateJWT(jwtParam);
  }
  
  @Test
  public void testGenerateJWTInvalidateGenerate() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = createJWTParameter(appId);
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    String jwt0 = actor.generateJWT(jwtParam);
    Assert.assertNotNull(jwt0);
    
    actor.invalidateJWT(appId.toString());
    String jwt1 = actor.generateJWT(jwtParam);
    Assert.assertNotEquals(jwt0, jwt1);
  }
  
  @Test
  public void testRenewJWT() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam0 = createJWTParameter(appId, 2, ChronoUnit.SECONDS);
    RMAppSecurityActions actor = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    String jwt0 = actor.generateJWT(jwtParam0);
  
    TimeUnit.SECONDS.sleep(2);
    
    JWTSecurityHandler.JWTMaterialParameter jwtParam1 = createJWTParameter(appId);
    jwtParam1.setToken(jwt0);
    String jwt1 = actor.renewJWT(jwtParam1);
    Assert.assertNotNull(jwt1);
    Assert.assertNotEquals(jwt0, jwt1);
    LOG.info(jwt0);
    LOG.info(jwt1);
  }
  
  private JWTSecurityHandler.JWTMaterialParameter createJWTParameter(ApplicationId appId) {
    return createJWTParameter(appId, 10, ChronoUnit.MINUTES);
  }
  
  private JWTSecurityHandler.JWTMaterialParameter createJWTParameter(ApplicationId appId, long amountToAdd,
      TemporalUnit unit) {
    JWTSecurityHandler.JWTMaterialParameter jwtParam = new JWTSecurityHandler.JWTMaterialParameter(appId, JWT_SUBJECT);
    jwtParam.setRenewable(false);
    Instant now = Instant.now();
    Instant expiresAt = now.plus(amountToAdd, unit);
    jwtParam.setExpirationDate(expiresAt);
    jwtParam.setValidNotBefore(now);
    jwtParam.setAudiences(new String[]{"job"});
    return jwtParam;
  }
  
  @Test
  public void testConfUpdate() throws Exception {
    LocalDateTime expiration = LocalDateTime.now().plus(10, ChronoUnit.MINUTES);
    JWTClaimsSet masterClaims = new JWTClaimsSet();
    masterClaims.setSubject("master_token");
    masterClaims.setExpirationTime(Date.from(expiration.atZone(ZoneId.systemDefault()).toInstant()));
    String newMasterToken = jwtIssuer.generate(masterClaims);
    Assert.assertNotNull(newMasterToken);
    
    String[] newRenewalTokens = new String[5];
    JWTClaimsSet renewClaims = new JWTClaimsSet();
    renewClaims.setSubject("renew_token");
    renewClaims.setExpirationTime(Date.from(expiration.atZone(ZoneId.systemDefault()).toInstant()));
    for (int i = 0; i < newRenewalTokens.length; i++) {
      String renewToken = jwtIssuer.generate(renewClaims);
      Assert.assertNotNull(renewToken);
      newRenewalTokens[i] = renewToken;
    }
    RMAppSecurityActions actor = new TestingHopsworksActions(newMasterToken, newRenewalTokens);
    ((Configurable) actor).setConf(conf);
    actor.init();
    TimeUnit.MILLISECONDS.sleep(500);
    
    // Renewal must have happened, check new values in ssl-server
    Configuration sslConf = new Configuration();
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY));
    String newMasterTokenConf = sslConf.get(YarnConfiguration.RM_JWT_MASTER_TOKEN, "");
    Assert.assertEquals(newMasterToken, newMasterTokenConf);
    for (int i = 0; i < newRenewalTokens.length; i++) {
      String confKey = String.format(YarnConfiguration.RM_JWT_RENEW_TOKEN_PATTERN, i);
      String newRenewalToken = sslConf.get(confKey, "");
      Assert.assertEquals(newRenewalTokens[i], newRenewalToken);
    }
  
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    Assert.assertEquals(expiration.format(formatter),
        ((HopsworksRMAppSecurityActions)actor).getMasterTokenExpiration().format(formatter));
    actor.destroy();
  }
  
  @Test
  public void testServiceJWTRenewalRetry() throws Exception {
    LocalDateTime expiration = LocalDateTime.now().plus(10, ChronoUnit.MINUTES);
    JWTClaimsSet claims = new JWTClaimsSet();
    claims.setSubject("test");
    claims.setExpirationTime(Date.from(expiration.atZone(ZoneId.systemDefault()).toInstant()));
    String newMasterToken = jwtIssuer.generate(claims);
    String[] renewTokens = new String[5];
    for (int i = 0; i < renewTokens.length; i++) {
      renewTokens[i] = jwtIssuer.generate(claims);
    }
    RMAppSecurityActions actor = new FailingTestHopsworksActions(newMasterToken, renewTokens);
    ((Configurable)actor).setConf(conf);
    actor.init();
    
    int secondsWaited = 0;
    while (!((FailingTestHopsworksActions)actor).succeedRenewing
        && secondsWaited++ < 10) {
      TimeUnit.SECONDS.sleep(1);
    }
    // Renewal should intentionally fail but finally it should succeed
    Assert.assertTrue(((FailingTestHopsworksActions)actor).succeedRenewing);
    Assert.assertTrue(((FailingTestHopsworksActions)actor).usedOneTimeTokens.size() > 1);
    actor.destroy();
  }
  
  private JWTResponse loginAndGetJWT() throws Exception {
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
        Matcher matcher = HopsworksRMAppSecurityActions.JWT_PATTERN.matcher(h.getValue());
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
        return new JWTResponse(masterJWT, renewTokens);
      }
      
      throw new IOException("Could not get JWT from Hopsworks");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
  
  private class JWTResponse {
    private final String masterToken;
    private final String[] renewalTokens;
    
    private JWTResponse(String masterToken, String[] renewalTokens) {
      this.masterToken = masterToken;
      this.renewalTokens = renewalTokens;
    }
  }
  
  private class TestingHopsworksActions extends HopsworksRMAppSecurityActions {
  
    final String newMasterToken;
    final String[] newRenewalTokens;
    private boolean renewed = false;
    
    public TestingHopsworksActions(String newMasterToken, String[] newRenewalTokens)
        throws MalformedURLException, GeneralSecurityException {
      super();
      this.newMasterToken = newMasterToken;
      this.newRenewalTokens = newRenewalTokens;
    }
  
    @Override
    protected Pair<String, String[]> renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
        LocalDateTime notBefore) throws URISyntaxException, IOException, GeneralSecurityException {
      return new Pair<String, String[]>(newMasterToken, newRenewalTokens);
    }
  
    @Override
    protected void invalidateServiceJWT(String token2invalidate)
        throws URISyntaxException, IOException, GeneralSecurityException {
      // NO-OP
    }
  
    @Override
    protected boolean isTime2Renew(LocalDateTime now, LocalDateTime expiration) {
      if (!renewed) {
        renewed = true;
        return true;
      }
      return false;
    }
  }
  
  private class FailingTestHopsworksActions extends TestingHopsworksActions {
    
    private int failures = 0;
    private boolean succeedRenewing = false;
    private Set<String> usedOneTimeTokens = new HashSet<>();
    
    public FailingTestHopsworksActions(String newMasterToken, String[] newRenewTokens)
        throws MalformedURLException, GeneralSecurityException {
      super(newMasterToken, newRenewTokens);
    }
  
    @Override
    protected Pair<String, String[]> renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
        LocalDateTime notBefore) throws URISyntaxException, IOException, GeneralSecurityException {
      usedOneTimeTokens.add(oneTimeToken);
      if (failures++ < 3) {
        throw new IOException("OOoops");
      }
      succeedRenewing = true;
      return new Pair<>(newMasterToken, newRenewalTokens);
    }
  }
}
