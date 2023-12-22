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

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import io.hops.security.AbstractSecurityActions;
import io.hops.security.HopsSecurityActionsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for testing the RMAppSecurityAction interface with Hopsworks
 * By default the tests are ignored as they expect a running Hopsworks
 * instance.
 *
 * You SHOULD ALWAYS run these test manually if you've changed the interface
 *
 */
@Ignore
public class TestHopsworksRMAppSecurityActions {
  private final static Log LOG = LogFactory.getLog(TestHopsworksRMAppSecurityActions.class);
  
  private static final String O = "application_id";
  private static final String OU = "1";
  
  private static final String KEYSTORE_LOCATION = "/path/to/keystore";
  private static final String KEYSTORE_PASS = "12345";
  
  private static final String JWT_SUBJECT = "ProjectA1__Flock";
  private String HOPSWORKS_ENDPOINT = "https://HOST:PORT";
  
  private static String classPath;
  
  private Path sslServerPath;
  private Configuration conf;
  private Configuration sslServer;

  /**
   * You have to add your API key here before running the tests
   */
  private String HOPSWORKS_API_KEY = "";
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    classPath = KeyStoreTestUtil.getClasspathDir(TestHopsworksRMAppSecurityActions.class);
    byte[] jwtIssuerSecret = new byte[32];
    Random rand = new Random();
    rand.nextBytes(jwtIssuerSecret);
  }
  
  @Before
  public void beforeTest() throws Exception {
    HopsSecurityActionsFactory.getInstance().clear(
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    conf = new Configuration();
    String sslConfFilename = TestHopsworksRMAppSecurityActions.class.getSimpleName() + ".ssl-server.xml";
    sslServerPath = Paths.get(classPath, sslConfFilename);
    

    sslServer = new Configuration(false);
    sslServer.set(AbstractSecurityActions.HOPSWORKS_API_kEY_PROP, HOPSWORKS_API_KEY);

    sslServer.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), KEYSTORE_LOCATION);
    sslServer.set(FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), KEYSTORE_PASS);
    
    KeyStoreTestUtil.saveConfig(sslServerPath.toFile(), sslServer);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFilename);
    conf.set(CommonConfigurationKeys.HOPS_HOPSWORKS_HOST_KEY, HOPSWORKS_ENDPOINT);
    conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".security.DevHopsworksRMAppSecurityActions");
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
  }
  
  @After
  public void afterTest() throws Exception {
    AbstractSecurityActions actorService = HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    actorService.stop();
    if (sslServerPath != null) {
      sslServerPath.toFile().delete();
    }
  }
  
  @Test
  public void testSign() throws Exception {
    PKCS10CertificationRequest csr = generateCSR(UUID.randomUUID().toString());
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    X509SecurityHandler.CertificateBundle singedBundle = actor.sign(csr);
    Assert.assertNotNull(singedBundle);
  }
  
  @Test
  public void testRevoke() throws Exception {
    String cn = UUID.randomUUID().toString();
    PKCS10CertificationRequest csr = generateCSR(cn);
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
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
    
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    String jwt = actor.generateJWT(jwtParam);
    Assert.assertNotNull(jwt);
    String[] tokenizedSubject = JWT_SUBJECT.split("__");
    JWT decoded = JWTParser.parse(jwt);
    String subject = decoded.getJWTClaimsSet().getSubject();
    Assert.assertEquals(tokenizedSubject[1], subject);
    
    // Test generate and fall-back to application submitter
    appId = ApplicationId.newInstance(System.currentTimeMillis(), 2);
    jwtParam = new JWTSecurityHandler.JWTMaterialParameter(appId, "dorothy");
    jwtParam.setRenewable(false);
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(10L, ChronoUnit.MINUTES);
    jwtParam.setExpirationDate(expiresAt);
    jwtParam.setValidNotBefore(now);
    jwtParam.setAudiences(new String[]{"job"});
    jwt = actor.generateJWT(jwtParam);
    decoded = JWTParser.parse(jwt);
    subject = decoded.getJWTClaimsSet().getSubject();
    Assert.assertEquals("dorothy", subject);
    
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    LocalDateTime nbfFromToken = DateUtils.date2LocalDateTime(decoded.getJWTClaimsSet().getNotBeforeTime());
    Assert.assertEquals(now.format(formatter), nbfFromToken.format(formatter));
    LocalDateTime expirationFromToken = DateUtils.date2LocalDateTime(decoded.getJWTClaimsSet().getExpirationTime());
    Assert.assertEquals(expiresAt.format(formatter), expirationFromToken.format(formatter));
  }
  
  @Test
  public void testInvalidateJWT() throws Exception {
    String signingKeyName = "lala";
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    actor.invalidateJWT(signingKeyName);
  }
  
  @Test
  public void testGenerateJWTSameSigningKeyShouldFail() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = createJWTParameter(appId);
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    actor.generateJWT(jwtParam);
    
    rule.expect(IOException.class);
    actor.generateJWT(jwtParam);
  }
  
  @Test
  public void testGenerateJWTInvalidateGenerate() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam = createJWTParameter(appId);
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    String jwt0 = actor.generateJWT(jwtParam);
    Assert.assertNotNull(jwt0);
    
    actor.invalidateJWT(appId.toString());
    String jwt1 = actor.generateJWT(jwtParam);
    Assert.assertNotEquals(jwt0, jwt1);
  }
  
  @Test(timeout = 15000)
  public void testRenewJWT() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam0 = createJWTParameter(appId, 2, ChronoUnit.SECONDS);
    RMAppSecurityActions actor = (RMAppSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY, YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT));
    String jwt0 = actor.generateJWT(jwtParam0);
    
    TimeUnit.SECONDS.sleep(10);
    
    JWTSecurityHandler.JWTMaterialParameter jwtParam1 = createJWTParameter(appId);
    jwtParam1.setToken(jwt0);
    String jwt1 = actor.renewJWT(jwtParam1);
    Assert.assertNotNull(jwt1);
    Assert.assertNotEquals(jwt0, jwt1);
  }
  
  private JWTSecurityHandler.JWTMaterialParameter createJWTParameter(ApplicationId appId) {
    return createJWTParameter(appId, 10, ChronoUnit.MINUTES);
  }
  
  private JWTSecurityHandler.JWTMaterialParameter createJWTParameter(ApplicationId appId, long amountToAdd,
      TemporalUnit unit) {
    JWTSecurityHandler.JWTMaterialParameter jwtParam = new JWTSecurityHandler.JWTMaterialParameter(appId, JWT_SUBJECT);
    jwtParam.setRenewable(false);
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(amountToAdd, unit);
    jwtParam.setExpirationDate(expiresAt);
    jwtParam.setValidNotBefore(now);
    jwtParam.setAudiences(new String[]{"job"});
    return jwtParam;
  }
}
