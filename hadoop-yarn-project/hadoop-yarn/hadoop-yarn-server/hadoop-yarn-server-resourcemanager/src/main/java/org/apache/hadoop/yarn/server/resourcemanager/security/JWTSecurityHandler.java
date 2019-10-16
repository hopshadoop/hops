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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialRenewedEvent;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JWTSecurityHandler
    implements RMAppSecurityHandler<JWTSecurityHandler.JWTSecurityManagerMaterial, JWTSecurityHandler
    .JWTMaterialParameter> {
  private static final Log LOG = LogFactory.getLog(JWTSecurityHandler.class);
  
  private final RMContext rmContext;
  private final RMAppSecurityManager rmAppSecurityManager;
  private final EventHandler eventHandler;
  private String[] jwtAudience;
  
  private Configuration config;
  private boolean jwtEnabled;
  private RMAppSecurityActions rmAppSecurityActions;
  private Pair<Long, TemporalUnit> validityPeriod;
  private final Map<ApplicationId, ScheduledFuture> renewalTasks;
  private ScheduledExecutorService renewalExecutorService;
  private Long leeway;
  
  private Thread invalidationEventsHandler;
  private static final int INVALIDATION_EVENTS_QUEUE_SIZE = 100;
  private final BlockingQueue<JWTInvalidationEvent> invalidationEvents;
  private final RandomDataGenerator random;
  
  public JWTSecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager) {
    this.rmContext = rmContext;
    this.rmAppSecurityManager = rmAppSecurityManager;
    this.renewalTasks = new ConcurrentHashMap<>();
    this.invalidationEvents = new ArrayBlockingQueue<JWTInvalidationEvent>(INVALIDATION_EVENTS_QUEUE_SIZE);
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.random = new RandomDataGenerator();
  }
  
  @Override
  public void init(Configuration config) throws Exception {
    LOG.info("Initializing JWT Security Handler");
    this.config = config;
    jwtEnabled = config.getBoolean(YarnConfiguration.RM_JWT_ENABLED,
        YarnConfiguration.DEFAULT_RM_JWT_ENABLED);
    jwtAudience = config.getTrimmedStrings(YarnConfiguration.RM_JWT_AUDIENCE,
        YarnConfiguration.DEFAULT_RM_JWT_AUDIENCE);
    renewalExecutorService = rmAppSecurityManager.getRenewalExecutorService();
    String validity = config.get(YarnConfiguration.RM_JWT_VALIDITY_PERIOD,
        YarnConfiguration.DEFAULT_RM_JWT_VALIDITY_PERIOD);
    validityPeriod = rmAppSecurityManager.parseInterval(validity, YarnConfiguration.RM_JWT_VALIDITY_PERIOD);
    String expirationLeewayConf = config.get(YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY,
        YarnConfiguration.DEFAULT_RM_JWT_EXPIRATION_LEEWAY);
    Pair<Long, TemporalUnit> expirationLeeway = rmAppSecurityManager.parseInterval(expirationLeewayConf,
        YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY);
    if (((ChronoUnit) expirationLeeway.getSecond()).compareTo(ChronoUnit.SECONDS) < 0) {
      throw new IllegalArgumentException("Value of " + YarnConfiguration.RM_JWT_EXPIRATION_LEEWAY
          + " should be at least seconds");
    }
    leeway = Duration.of(expirationLeeway.getFirst(), expirationLeeway.getSecond()).getSeconds();
    if (jwtEnabled) {
      rmAppSecurityActions = rmAppSecurityManager.getRmAppCertificateActions();
    }
  }
  
  @Override
  public void start() throws Exception {
    LOG.info("Starting JWT Security Handler");
    if (isJWTEnabled()) {
      invalidationEventsHandler = createInvalidationEventsHandler();
      invalidationEventsHandler.setDaemon(false);
      invalidationEventsHandler.setName("JWT-InvalidationEventsHandler");
      invalidationEventsHandler.start();
    }
  }
  
  @Override
  public void stop() throws Exception {
    LOG.info("Stopping JWT Security Handler");
    if (invalidationEventsHandler != null) {
      invalidationEventsHandler.interrupt();
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected Thread createInvalidationEventsHandler() {
    return new InvalidationEventsHandler();
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public BlockingQueue<JWTInvalidationEvent> getInvalidationEvents() {
    return invalidationEvents;
  }
  
  @Override
  public JWTSecurityManagerMaterial generateMaterial(JWTMaterialParameter parameter) throws Exception {
    if (!isJWTEnabled()) {
      return null;
    }
    ApplicationId appId = parameter.getApplicationId();
    LOG.debug("Generating JWT for application " + appId);
    prepareJWTGenerationParameters(parameter);
    String jwt = generateInternal(parameter);
    return new JWTSecurityManagerMaterial(appId, jwt, parameter.getExpirationDate());
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void prepareJWTGenerationParameters(JWTMaterialParameter parameter) {
    parameter.setAudiences(jwtAudience);
    LocalDateTime now = getNow();
    LocalDateTime expirationTime = now.plus(validityPeriod.getFirst(), validityPeriod.getSecond());
    parameter.setExpirationDate(expirationTime);
    parameter.setValidNotBefore(now);
    // JWT for applications will not be automatically renewed.
    // JWTSecurityHandler will renew them
    parameter.setRenewable(false);
    parameter.setExpLeeway(leeway.intValue());
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected String generateInternal(JWTMaterialParameter parameter)
      throws URISyntaxException, IOException, GeneralSecurityException {
    return rmAppSecurityActions.generateJWT(parameter);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected LocalDateTime getNow() {
    return DateUtils.getNow();
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected Pair<Long, TemporalUnit> getValidityPeriod() {
    return validityPeriod;
  }
  
  @VisibleForTesting
  protected Map<ApplicationId, ScheduledFuture> getRenewalTasks() {
    return renewalTasks;
  }
  
  @VisibleForTesting
  protected Configuration getConfig() {
    return config;
  }
  
  @VisibleForTesting
  protected RMAppSecurityManager getRmAppSecurityManager() {
    return rmAppSecurityManager;
  }
  
  @Override
  public void registerRenewer(JWTMaterialParameter parameter) {
    if (!isJWTEnabled()) {
      return;
    }
    if (!renewalTasks.containsKey(parameter.getApplicationId())) {
      ScheduledFuture task = renewalExecutorService.schedule(
          createJWTRenewalTask(parameter.getApplicationId(), parameter.appUser, parameter.token),
          computeScheduledDelay(parameter.getExpirationDate()), TimeUnit.SECONDS);
      renewalTasks.put(parameter.getApplicationId(), task);
    }
  }
  
  private long computeScheduledDelay(LocalDateTime expiration) {
    long upperLimit = Math.max(leeway - 5L, 5L);
    // random delay in seconds [3, (leeway - 5)]
    long delayFromExpiration = random.nextLong(3L, upperLimit);
    Duration duration = Duration.between(getNow(), expiration);
    return duration.getSeconds() + delayFromExpiration;
  }
  
  public void deregisterFromRenewer(ApplicationId appId) {
    if (!isJWTEnabled()) {
      return;
    }
    ScheduledFuture task = renewalTasks.get(appId);
    if (task != null) {
      task.cancel(true);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected Runnable createJWTRenewalTask(ApplicationId appId, String appUser, String token) {
    return new JWTRenewer(appId, appUser, token);
  }
  
  @Override
  public boolean revokeMaterial(JWTMaterialParameter parameter, Boolean blocking) {
    // Return value does not matter for JWT
    if (!isJWTEnabled()) {
      return true;
    }
    ApplicationId appId = parameter.getApplicationId();
    try {
      LOG.info("Invalidating JWT for application: " + appId);
      deregisterFromRenewer(appId);
      putToInvalidationQueue(appId);
      return true;
    } catch (InterruptedException ex) {
      LOG.warn("Shutting down while putting invalidation event to queue for application " + appId);
    }
    return false;
  }
  
  private void putToInvalidationQueue(ApplicationId appId) throws InterruptedException {
    invalidationEvents.put(new JWTInvalidationEvent(appId.toString()));
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void revokeInternal(String signingKeyName) {
    if (!isJWTEnabled()) {
      return;
    }
    try {
      rmAppSecurityActions.invalidateJWT(signingKeyName);
    } catch (URISyntaxException | IOException | GeneralSecurityException ex) {
      LOG.error("Could not invalidate JWT with signing key " + signingKeyName, ex);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected String renewInternal(JWTMaterialParameter param) throws URISyntaxException, IOException,
      GeneralSecurityException {
    if (!isJWTEnabled()) {
      return null;
    }
    return rmAppSecurityActions.renewJWT(param);
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected boolean isJWTEnabled() {
    return jwtEnabled;
  }
  
  public class JWTSecurityManagerMaterial extends RMAppSecurityManager.SecurityManagerMaterial {
    private final String token;
    private final LocalDateTime expirationDate;
    
    public JWTSecurityManagerMaterial(ApplicationId applicationId, String token, LocalDateTime expirationDate) {
      super(applicationId);
      this.token = token;
      this.expirationDate = expirationDate;
    }
    
    public String getToken() {
      return token;
    }
    
    public LocalDateTime getExpirationDate() {
      return expirationDate;
    }
  }
  
  public static class JWTMaterialParameter extends RMAppSecurityManager.SecurityManagerMaterial {
    private final String appUser;
    private String token;
    private String[] audiences;
    private LocalDateTime expirationDate;
    private LocalDateTime validNotBefore;
    private boolean renewable;
    private int expLeeway;
    
    public JWTMaterialParameter(ApplicationId applicationId, String appUser) {
      super(applicationId);
      this.appUser = appUser;
    }
    
    public String getAppUser() {
      return appUser;
    }
  
    public String[] getAudiences() {
      return audiences;
    }
  
    public void setAudiences(String[] audiences) {
      this.audiences = audiences;
    }
  
    public LocalDateTime getExpirationDate() {
      return expirationDate;
    }
  
    public void setExpirationDate(LocalDateTime expirationDate) {
      this.expirationDate = expirationDate;
    }
  
    public LocalDateTime getValidNotBefore() {
      return validNotBefore;
    }
  
    public void setValidNotBefore(LocalDateTime validNotBefore) {
      this.validNotBefore = validNotBefore;
    }
  
    public boolean isRenewable() {
      return renewable;
    }
  
    public void setRenewable(boolean renewable) {
      this.renewable = renewable;
    }
  
    public int getExpLeeway() {
      return expLeeway;
    }
  
    public void setExpLeeway(int expLeeway) {
      this.expLeeway = expLeeway;
    }
  
    public String getToken() {
      return token;
    }
    
    public void setToken(String token) {
      this.token = token;
    }
    
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + appUser.hashCode();
      result = 31 * result + getApplicationId().hashCode();
      if (expirationDate != null) {
        result = 31 * result + expirationDate.hashCode();
      }
      return result;
    }
  
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof JWTMaterialParameter) {
        JWTMaterialParameter otherMaterial = (JWTMaterialParameter) o;
        if (expirationDate != null) {
          return appUser.equals(otherMaterial.appUser)
              && getApplicationId().equals(otherMaterial.getApplicationId())
              && expirationDate.equals(otherMaterial.getExpirationDate());
        }
        return appUser.equals(otherMaterial.appUser)
            && getApplicationId().equals(otherMaterial.getApplicationId());
      }
      return false;
    }
  }
  
  private class JWTRenewer implements Runnable {
    private final ApplicationId appId;
    private final String appUser;
    private final String token;
    private final BackOff backOff;
    private long backOffTime = 0L;
    
    public JWTRenewer(ApplicationId appId, String appUser, String token) {
      this.appId = appId;
      this.appUser = appUser;
      this.token = token;
      this.backOff = rmAppSecurityManager.createBackOffPolicy();
    }
  
    @Override
    public void run() {
      try {
        LOG.debug("Renewing JWT for application " + appId);
        JWTMaterialParameter jwtParam = new JWTMaterialParameter(appId, appUser);
        jwtParam.setToken(token);
        prepareJWTGenerationParameters(jwtParam);
        String jwt = renewInternal(jwtParam);
        renewalTasks.remove(appId);
        JWTSecurityManagerMaterial jwtMaterial = new JWTSecurityManagerMaterial(appId, jwt,
            jwtParam.getExpirationDate());
        
        eventHandler.handle(new RMAppSecurityMaterialRenewedEvent<>(appId, jwtMaterial));
        LOG.debug("Renewed JWT for application " + appId);
      } catch (Exception ex) {
        renewalTasks.remove(appId);
        backOffTime = backOff.getBackOffInMillis();
        if (backOffTime != -1) {
          LOG.warn("Failed to renew JWT for application " + appId + ". Retrying in " + backOffTime + " ms");
          ScheduledFuture task = renewalExecutorService.schedule(this, backOffTime, TimeUnit.MILLISECONDS);
          renewalTasks.put(appId, task);
        } else {
          LOG.error("Failed to renew JWT for application " + appId + ". Failed more than 4 times, giving up", ex);
        }
      }
    }
  }
  
  protected static class JWTInvalidationEvent {
    private final String signingKeyName;
    
    protected JWTInvalidationEvent(String signingKeyName) {
      this.signingKeyName = signingKeyName;
    }
  
    protected String getSigningKeyName() {
      return signingKeyName;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      
      if (o instanceof JWTInvalidationEvent) {
        return this.signingKeyName.equals(((JWTInvalidationEvent) o).signingKeyName);
      }
      return false;
    }
  
    @Override
    public int hashCode() {
      return signingKeyName.hashCode();
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected class InvalidationEventsHandler extends Thread {
    
    private void drain() {
      List<JWTInvalidationEvent> events = new ArrayList<>(invalidationEvents.size());
      invalidationEvents.drainTo(events);
      for (JWTInvalidationEvent event : events) {
        revokeInternal(event.signingKeyName);
      }
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          JWTInvalidationEvent event = invalidationEvents.take();
          revokeInternal(event.signingKeyName);
        } catch (InterruptedException ex) {
          LOG.info("JWT InvalidationEventHandler interrupted. Draining queue...");
          drain();
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
