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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestServiceJWTManager {
  private static final Log LOG = LogFactory.getLog(TestServiceJWTManager.class);
  
  private static MockJWTIssuer jwtIssuer;
  private static String sslConfFilename;
  private static Path sslServerPath;
  private static String classpath;
  
  private Configuration conf;
  private ServiceJWTManager jwtManager;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    byte[] jwtIssuerSecret = new byte[32];
    Random rand = new Random();
    rand.nextBytes(jwtIssuerSecret);
    jwtIssuer = new MockJWTIssuer(jwtIssuerSecret);
    classpath = KeyStoreTestUtil.getClasspathDir(TestServiceJWTManager.class);
    
    sslConfFilename = TestServiceJWTManager.class.getSimpleName() + ".ssl-server.xml";
    sslServerPath = Paths.get(classpath, sslConfFilename);
  }
  
  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    Configuration sslConf = new Configuration(false);
    
    Pair<String, String[]> tokens = generateMasterAndRenewTokens(5);
    
    sslConf.set(ServiceJWTManager.JWT_MANAGER_MASTER_TOKEN_KEY, tokens.getFirst());
    
    for (int i = 0; i < tokens.getSecond().length; i++) {
      sslConf.set(String.format(ServiceJWTManager.JWT_MANAGER_RENEW_TOKEN_PATTERN, i), tokens.getSecond()[i]);
    }
    KeyStoreTestUtil.saveConfig(sslServerPath.toFile(), sslConf);
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFilename);
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
  }
  
  @After
  public void after() throws Exception {
    if (jwtManager != null) {
      jwtManager.stop();
    }
    if (sslServerPath != null) {
      sslServerPath.toFile().delete();
    }
  }
  
  @Test
  public void testUpdatingJWTConf() throws Exception {
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(10L, ChronoUnit.MINUTES);
    Pair<String, String[]> tokens = generateMasterAndRenewTokens(5, now, expiresAt);
    
    jwtManager = new TestingServiceJWTManager("TestingJWTManager", tokens.getFirst(),
        tokens.getSecond(), expiresAt);
    jwtManager.init(conf);
    jwtManager.start();
  
    TimeUnit.MILLISECONDS.sleep(500);
    
    // Renewal must have happened by now
    Assert.assertTrue(((TestingServiceJWTManager)jwtManager).renewed);
    
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY));
    String newMasterTokenConf = sslConf.get(ServiceJWTManager.JWT_MANAGER_MASTER_TOKEN_KEY, "");
    Assert.assertEquals(tokens.getFirst(), newMasterTokenConf);
    for (int i = 0; i < tokens.getSecond().length; i++) {
      String newRenewalTokenConf = sslConf.get(
          String.format(ServiceJWTManager.JWT_MANAGER_RENEW_TOKEN_PATTERN, i), "");
      Assert.assertEquals(tokens.getSecond()[i], newRenewalTokenConf);
    }
    Assert.assertTrue(isUpdaterThreadStillRunning(jwtManager.getExecutorService()));
  }
  
  @Test
  public void testUpdateRetryOnFailure() throws Exception {
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(10L, ChronoUnit.MINUTES);
    Pair<String, String[]> tokens = generateMasterAndRenewTokens(5, now, expiresAt);
    jwtManager = new FailingTestingServiceJWTManager("FailingTestingJWTManager", tokens.getFirst(),
        tokens.getSecond(), expiresAt, 3);
    jwtManager.init(conf);
    jwtManager.start();
    
    int secondsWaited = 0;
    while (!((TestingServiceJWTManager)jwtManager).renewed && secondsWaited++ < 10) {
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(((TestingServiceJWTManager)jwtManager).renewed);
    Assert.assertTrue(((FailingTestingServiceJWTManager)jwtManager).usedOneTimeTokens.size() > 1);
    Assert.assertTrue(isUpdaterThreadStillRunning(jwtManager.getExecutorService()));
  }
  
  /**
   * ServiceJWTManager's executor is a single thread pool. If we manage to
   * submit a runnable to the pool, it means the Renewer thread has finished
   */
  private boolean isUpdaterThreadStillRunning(ExecutorService executor) {
    Future future = executor.submit(new Runnable() {
      @Override
      public void run() {
        // Do nothing
      }
    });
    try {
      future.get(500L, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      return true;
    }
    return false;
  }
  
  @Test
  public void testGiveUpAfterRetries() throws Exception {
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(10L, ChronoUnit.MINUTES);
    int numOfRenewTokens = 5;
    Pair<String, String[]> tokens = generateMasterAndRenewTokens(numOfRenewTokens, now, expiresAt);
    jwtManager = new FailingTestingServiceJWTManager("FailingTestingJWTManager", tokens.getFirst(),
        tokens.getSecond(), expiresAt, Integer.MAX_VALUE);
    jwtManager.init(conf);
    jwtManager.start();
    
    int secondsWaited = 0;
    // It will always fail, so wait until it has used all OneTimeTokens
    while (((FailingTestingServiceJWTManager)jwtManager).usedOneTimeTokens.size() < numOfRenewTokens
      && secondsWaited++ < 30) {
      TimeUnit.SECONDS.sleep(1);
    }
  
    Assert.assertFalse(((TestingServiceJWTManager)jwtManager).renewed);
    Assert.assertEquals(numOfRenewTokens, ((FailingTestingServiceJWTManager)jwtManager).usedOneTimeTokens.size());
    Assert.assertFalse(isUpdaterThreadStillRunning(jwtManager.getExecutorService()));
  }
  
  /**
   * Only one ServiceJWTRenewer should run per host, even when there are multiple JVMs.
   * As it is difficult to test this scenario from unit test, test with two different
   * threads - but the result should be the same with two different JVMs
   */
  @Test
  public void onlyOneRenewerShouldRun() throws Exception {
    int numOfRenewTokens = 5;
    LocalDateTime now = DateUtils.getNow();
    LocalDateTime expiresAt = now.plus(10L, ChronoUnit.MINUTES);
    Pair<String, String[]> tokens = generateMasterAndRenewTokens(numOfRenewTokens, now, expiresAt);
    // Set low validity period so second JWTManager will try again to renew in a reasonable time
    conf.set(CommonConfigurationKeys.JWT_MANAGER_MASTER_TOKEN_VALIDITY_PERIOD, "2s");
    ServiceJWTManager jwtManager0 = new TestingServiceJWTManager("TestingServiceJWTMgm0", tokens.getFirst(),
        tokens.getSecond(), expiresAt);
    jwtManager0.init(conf);
    
    jwtManager = new TestingServiceJWTManager("TestingServiceJWTMgm1", tokens.getFirst(),
        tokens.getSecond(), expiresAt);
    jwtManager.init(conf);
    String initialJWTManagerMasterToken = jwtManager.getMasterToken();
    Assert.assertNotEquals("", initialJWTManagerMasterToken);
    
    jwtManager0.start();
    
    int secondsWaited = 0;
    while (!((TestingServiceJWTManager) jwtManager0).renewed && secondsWaited++ < 5) {
      TimeUnit.SECONDS.sleep(1);
    }
    
    // First manager renewed tokens
    Assert.assertTrue(((TestingServiceJWTManager) jwtManager0).renewed);
    
    jwtManager.start();
    
    // Wait until it tries to take the lock
    secondsWaited = 0;
    while (!((TestingServiceJWTManager)jwtManager).tried2lock && secondsWaited++ < 5) {
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(((TestingServiceJWTManager) jwtManager).tried2lock);
    Assert.assertFalse(((TestingServiceJWTManager) jwtManager).renewed);
    
    TimeUnit.SECONDS.sleep(conf.getTimeDuration(CommonConfigurationKeys.JWT_MANAGER_MASTER_TOKEN_VALIDITY_PERIOD, 2L,
        TimeUnit.SECONDS));
    // jwtManager failed to renew tokens nevertheless it should have refreshed them
    Assert.assertNotEquals(initialJWTManagerMasterToken, jwtManager.getMasterToken());
    Assert.assertEquals(tokens.getFirst(), jwtManager.getMasterToken());
    jwtManager0.stop();
    
    // Wait until second JWTManager retries to take the
    secondsWaited = 0;
    while (!((TestingServiceJWTManager) jwtManager).renewed && secondsWaited++ < 5) {
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(((TestingServiceJWTManager) jwtManager).renewed);
  }
  
  private JWTClaimsSet.Builder generateJWTClaimsBuilder(LocalDateTime nbf, LocalDateTime expiration, String subject) {
    JWTClaimsSet.Builder claimsSetBuilder = new JWTClaimsSet.Builder();
    claimsSetBuilder.subject(subject);
    claimsSetBuilder.notBeforeTime(DateUtils.localDateTime2Date(nbf));
    claimsSetBuilder.expirationTime(DateUtils.localDateTime2Date(expiration));
    return claimsSetBuilder;
  }
  
  private Pair<String, String[]> generateMasterAndRenewTokens(int numOfRenewTokens) throws JOSEException {
    LocalDateTime now = DateUtils.getNow();
    return generateMasterAndRenewTokens(numOfRenewTokens, now, now.plus(10L, ChronoUnit.MINUTES));
  }
  
  private Pair<String, String[]> generateMasterAndRenewTokens(int numOfRenewTokens, LocalDateTime nbf,
      LocalDateTime expiresAt) throws JOSEException {
    String masterToken = jwtIssuer.generate(generateJWTClaimsBuilder(nbf, expiresAt, "master_token"));
    Assert.assertNotNull(masterToken);
    String[] renewTokens = new String[numOfRenewTokens];
    for (int i = 0; i < renewTokens.length; i++) {
      String renewToken = jwtIssuer.generate(generateJWTClaimsBuilder(nbf, expiresAt, "renew_token_" + i));
      Assert.assertNotNull(renewToken);
      renewTokens[i] = renewToken;
    }
    return new Pair<>(masterToken, renewTokens);
  }
  
  private class TestingServiceJWTManager extends ServiceJWTManager {
    private final String newMasterToken;
    private final String[] newRenewalTokens;
    private final LocalDateTime expiresAt;
    private boolean renewed = false;
    private boolean tried2lock = false;
    
    public TestingServiceJWTManager(String name, String newMasterToken,
        String[] newRenewalTokens, LocalDateTime expiresAt) {
      super(name);
      this.newMasterToken = newMasterToken;
      this.newRenewalTokens = newRenewalTokens;
      this.expiresAt = expiresAt;
    }
  
    @Override
    protected FileLock tryAndGetLock() {
      tried2lock = true;
      return super.tryAndGetLock();
    }
  
    @Override
    protected ServiceTokenDTO renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
        LocalDateTime notBefore) throws URISyntaxException, IOException {
      JWTDTO jwt = new JWTDTO();
      jwt.setToken(newMasterToken);
      jwt.setNbf(DateUtils.localDateTime2Date(notBefore));
      jwt.setExpiresAt(DateUtils.localDateTime2Date(this.expiresAt));
      
      ServiceTokenDTO serviceTokenResponse = new ServiceTokenDTO();
      serviceTokenResponse.setJwt(jwt);
      serviceTokenResponse.setRenewTokens(newRenewalTokens);
      renewed = true;
      return serviceTokenResponse;
    }
  
    @Override
    protected void invalidateServiceJWT(String token2invalidate) throws URISyntaxException, IOException {
      // NO-OP
    }
  
    @Override
    protected boolean isTime2Renew(LocalDateTime now, LocalDateTime tokenExpiration) {
      return !renewed;
    }
  }
  
  private class FailingTestingServiceJWTManager extends TestingServiceJWTManager {
    private final int succeedAfterRetries;
    
    private int failures = 0;
    private final Set<String> usedOneTimeTokens;
    
    public FailingTestingServiceJWTManager(String name, String newMasterToken,
        String[] newRenewalTokens, LocalDateTime expiresAt, int succeedAfterRetries) {
      super(name, newMasterToken, newRenewalTokens, expiresAt);
      usedOneTimeTokens = new HashSet<>(newRenewalTokens.length);
      this.succeedAfterRetries = succeedAfterRetries;
    }
  
    @Override
    protected ServiceTokenDTO renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
        LocalDateTime notBefore) throws URISyntaxException, IOException {
      usedOneTimeTokens.add(oneTimeToken);
      if (failures++ < succeedAfterRetries) {
        throw new IOException("oops");
      }
      return super.renewServiceJWT(token, oneTimeToken, expiresAt, notBefore);
    }
  }
}
