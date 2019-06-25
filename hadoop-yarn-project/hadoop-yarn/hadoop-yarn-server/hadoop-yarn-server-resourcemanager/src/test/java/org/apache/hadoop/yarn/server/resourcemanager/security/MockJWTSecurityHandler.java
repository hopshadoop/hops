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
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MockJWTSecurityHandler extends JWTSecurityHandler {
  protected final Log LOG = LogFactory.getLog(MockJWTSecurityHandler.class);
  private LocalDateTime now;
  private final Map<ApplicationId, String> app2jwt;
  private AtomicReference<MockJWTRenewer> mockRenewer;
  
  public MockJWTSecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager) {
    super(rmContext, rmAppSecurityManager);
    app2jwt = new HashMap<>();
  }
  
  @Override
  public JWTSecurityManagerMaterial generateMaterial(JWTMaterialParameter parameter) throws Exception {
    if (!isJWTEnabled()) {
      return null;
    }
    ApplicationId appId = parameter.getApplicationId();
    now = DateUtils.getNow();
    prepareJWTGenerationParameters(parameter);
    assertTrue(now.isBefore(parameter.getExpirationDate()));
    Pair<Long, TemporalUnit> validity = getValidityPeriod();
    LocalDateTime expTime = now.plus(validity.getFirst(), validity.getSecond());
    assertEquals(parameter.getExpirationDate(), expTime);
    assertFalse(parameter.isRenewable());
    String jwt = generateInternal(parameter);
    assertNotNull(jwt);
    assertFalse(jwt.isEmpty());
    app2jwt.put(appId, jwt);
    return new JWTSecurityManagerMaterial(appId, jwt, parameter.getExpirationDate());
  }
  
  @Override
  protected LocalDateTime getNow() {
    return now;
  }
  
  @Override
  protected Pair<Long, TemporalUnit> getValidityPeriod() {
    return getRmAppSecurityManager().parseInterval(
        getConfig().get(YarnConfiguration.RM_JWT_VALIDITY_PERIOD,
            YarnConfiguration.DEFAULT_RM_JWT_VALIDITY_PERIOD),
        YarnConfiguration.RM_JWT_VALIDITY_PERIOD);
  }
  
  @Override
  protected Runnable createJWTRenewalTask(ApplicationId appId, String appUser, String token) {
    MockJWTRenewer renewer = new MockJWTRenewer(appId, appUser, token);
    mockRenewer = new AtomicReference<>(renewer);
    return renewer;
  }
  
  public MockJWTRenewer getRenewer() {
    return mockRenewer.get();
  }
  
  protected class MockJWTRenewer implements Runnable {
    private final ApplicationId appId;
    private final String appUser;
    private final String token;
    private final BackOff backOff;
    private long backOffTime = 0L;
    
    private boolean exceptionRaised = true;
    private boolean hasRun = false;
    
    private MockJWTRenewer(ApplicationId appId, String appUser, String token) {
      this.appId = appId;
      this.appUser = appUser;
      this.token = token;
      this.backOff = getRmAppSecurityManager().createBackOffPolicy();
    }
    
    @Override
    public void run() {
      try {
        LOG.info("Renewing JWT for " + appId);
        JWTMaterialParameter jwtParam = new JWTMaterialParameter(appId, appUser);
        jwtParam.setToken(token);
        prepareJWTGenerationParameters(jwtParam);
        String jwt = renewInternal(jwtParam);
        getRenewalTasks().remove(appId);
        JWTSecurityManagerMaterial jwtMaterial = new JWTSecurityManagerMaterial(appId, jwt,
            jwtParam.getExpirationDate());
        String oldJWT = app2jwt.get(appId);
        assertNotNull("You should generate JWT first for app " + appId, oldJWT);
        assertNotEquals(oldJWT, jwtMaterial.getToken());
        LOG.info("Renewed JWT for " + appId);
        exceptionRaised = false;
        hasRun = true;
      } catch (Exception ex) {
        LOG.error("Exception should not have happened here!");
        LOG.error(ex, ex);
        getRenewalTasks().remove(appId);
        backOffTime = backOff.getBackOffInMillis();
        if (backOffTime != -1) {
          LOG.warn("Failed to renew JWT for application " + appId + ". Retrying in " + backOffTime + " ms");
          ScheduledFuture task = getRmAppSecurityManager().getRenewalExecutorService().schedule(this, backOffTime,
              TimeUnit.MILLISECONDS);
          getRenewalTasks().put(appId, task);
        } else {
          LOG.error("Failed to renew JWT for application " + appId + ". Failed more than 4 times, giving up");
        }
      }
    }
    
    public boolean isExceptionRaised() {
      return exceptionRaised;
    }
    
    public boolean hasRun() {
      return hasRun;
    }
  }
  
  
  
  /**
   * This class will block during JWT invalidation
   */
  public static class BlockingInvalidator extends MockJWTSecurityHandler {
    private final Semaphore semaphore;
  
    public BlockingInvalidator(RMContext rmContext,
        RMAppSecurityManager rmAppSecurityManager) {
      super(rmContext, rmAppSecurityManager);
      semaphore = new Semaphore(1);
    }
  
    public Semaphore getSemaphore() {
      return semaphore;
    }
  
    @Override
    protected Thread createInvalidationEventsHandler() {
      return new BlockingInvalidationEventHandler();
    }
  
    protected class BlockingInvalidationEventHandler extends InvalidationEventsHandler {
      @Override
      public void run() {
        try {
          // One shot
          semaphore.acquire();
          JWTInvalidationEvent event = getInvalidationEvents().take();
          revokeInternal(event.getSigningKeyName());
          semaphore.release();
        } catch (InterruptedException ex) {
          LOG.error("It should not have blocked here", ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
