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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialGeneratedEvent;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RMAppSecurityManager extends AbstractService
    implements EventHandler<RMAppSecurityManagerEvent> {
  private final static Log LOG = LogFactory.getLog(RMAppSecurityManager.class);
  private final static Map<String, ChronoUnit> CHRONOUNITS = new HashMap<>();
  static {
    CHRONOUNITS.put("MS", ChronoUnit.MILLIS);
    CHRONOUNITS.put("S", ChronoUnit.SECONDS);
    CHRONOUNITS.put("M", ChronoUnit.MINUTES);
    CHRONOUNITS.put("H", ChronoUnit.HOURS);
    CHRONOUNITS.put("D", ChronoUnit.DAYS);
  }
  private static final Pattern CONF_TIME_PATTERN = Pattern.compile("(^[0-9]+)(\\p{Alpha}+)");
  
  private RMContext rmContext;
  private Configuration conf;
  private EventHandler handler;
  private RMAppSecurityActions rmAppCertificateActions;
  private boolean isRPCTLSEnabled = false;
  private Map<Class, RMAppSecurityHandler> securityHandlersMap;
  
  private static final int RENEWAL_EXECUTOR_SERVICE_POOL_SIZE = 10;
  private ScheduledExecutorService renewalExecutorService;
  
  public RMAppSecurityManager(RMContext rmContext) {
    super(RMAppSecurityManager.class.getName());
    Security.addProvider(new BouncyCastleProvider());
    this.rmContext = rmContext;
    securityHandlersMap = new HashMap();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.debug("Initializing RMAppSecurityManager");
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    rmAppCertificateActions = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    isRPCTLSEnabled = conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  
    renewalExecutorService = Executors.newScheduledThreadPool(RENEWAL_EXECUTOR_SERVICE_POOL_SIZE,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("RMApp Security Material Renewer")
            .build());
    for (RMAppSecurityHandler handler : securityHandlersMap.values()) {
      handler.init(conf);
    }
    
    super.serviceInit(conf);
  }
  
  public void registerRMAppSecurityHandler(RMAppSecurityHandler securityHandler) {
    registerRMAppSecurityHandlerWithType(securityHandler, securityHandler.getClass());
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public void registerRMAppSecurityHandlerWithType(RMAppSecurityHandler securityHandler, Class type) {
    if (securityHandler != null) {
      securityHandlersMap.put(type, securityHandler);
    }
  }
  
  protected Pair<Long, TemporalUnit> parseInterval(String intervalFromConf, String confKey) {
    Matcher matcher = CONF_TIME_PATTERN.matcher(intervalFromConf);
    if (matcher.matches()) {
      Long interval = Long.parseLong(matcher.group(1));
      String unitStr = matcher.group(2);
      TemporalUnit unit = CHRONOUNITS.get(unitStr.toUpperCase());
      if (unit == null) {
        final StringBuilder validUnits = new StringBuilder();
        for (String key : CHRONOUNITS.keySet()) {
          validUnits.append(key).append(", ");
        }
        validUnits.append("\b\b");
        throw new IllegalArgumentException("Could not parse ChronoUnit: " + unitStr + ". Valid values are "
            + validUnits.toString());
      }
      return new Pair<>(interval, unit);
    } else {
      throw new IllegalArgumentException("Could not parse value " + intervalFromConf + " of " + confKey);
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting RMAppSecurityManager");
    for (RMAppSecurityHandler handler : securityHandlersMap.values()) {
      handler.start();
    }
    
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping RMAppCertificateManager");
    for (RMAppSecurityHandler handler : securityHandlersMap.values()) {
      handler.stop();
    }
    if (renewalExecutorService != null) {
      try {
        renewalExecutorService.shutdown();
        if (!renewalExecutorService.awaitTermination(2L, TimeUnit.SECONDS)) {
          renewalExecutorService.shutdownNow();
        }
        if (rmAppCertificateActions != null) {
          rmAppCertificateActions.destroy();
        }
      } catch (InterruptedException ex) {
        renewalExecutorService.shutdownNow();
        if (rmAppCertificateActions != null) {
          rmAppCertificateActions.destroy();
        }
        Thread.currentThread().interrupt();
      }
    }
  }
  
  protected ScheduledExecutorService getRenewalExecutorService() {
    return renewalExecutorService;
  }
  
  protected BackOff createBackOffPolicy() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(200)
        .setMaximumIntervalMillis(5000)
        .setMultiplier(1.5)
        .setMaximumRetries(4)
        .build();
  }
  
  @Override
  public void handle(RMAppSecurityManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.info("Processing event type: " + event.getType() + " for application: " + applicationId);
    if (event.getType().equals(RMAppSecurityManagerEventType.GENERATE_SECURITY_MATERIAL)) {
      generateSecurityMaterial(event);
    } else if (event.getType().equals(RMAppSecurityManagerEventType.REVOKE_SECURITY_MATERIAL)) {
      revokeSecurityMaterial(event);
    } else if (event.getType().equals(RMAppSecurityManagerEventType.REVOKE_CERTIFICATE_AFTER_ROTATION)) {
      revokeX509Only(event);
    } else if (event.getType().equals(RMAppSecurityManagerEventType.REVOKE_GENERATE_MATERIAL)) {
      revokeAndGenerateMaterial(event.getSecurityMaterial());
    } else {
      LOG.warn("Unknown event type " + event.getType());
    }
  }
  
  public <P extends SecurityManagerMaterial> void registerWithMaterialRenewers(P parameter) {
    if (parameter instanceof X509SecurityHandler.X509MaterialParameter) {
      X509SecurityHandler handler = (X509SecurityHandler) securityHandlersMap.get(X509SecurityHandler.class);
      if (handler != null) {
        handler.registerRenewer((X509SecurityHandler.X509MaterialParameter) parameter);
      } else {
        String errorMsg = "Tried to register with X.509 renewer but there is no handler";
        LOG.error(errorMsg);
        throw new NullPointerException(errorMsg);
      }
    } else if (parameter instanceof JWTSecurityHandler.JWTMaterialParameter) {
      JWTSecurityHandler handler = (JWTSecurityHandler) securityHandlersMap.get(JWTSecurityHandler.class);
      if (handler != null) {
        handler.registerRenewer((JWTSecurityHandler.JWTMaterialParameter) parameter);
      } else {
        String errorMsg = "Tried to register with JWT renewer but there is no handler";
        throw new NullPointerException(errorMsg);
      }
    }
  }
  
  @VisibleForTesting
  public RMAppSecurityActions getRmAppCertificateActions() {
    return rmAppCertificateActions;
  }
  
  @VisibleForTesting
  public RMAppSecurityHandler getSecurityHandler(Class type) {
    return securityHandlersMap.get(type);
  }
  
  @VisibleForTesting
  protected RMContext getRmContext() {
    return rmContext;
  }
  
  private void generateSecurityMaterial(RMAppSecurityManagerEvent event) {
    ApplicationId appId = event.getApplicationId();
    RMAppSecurityMaterial rmAppMaterial = new RMAppSecurityMaterial();
    try {
      // This could be a little bit more elegant
      for (RMAppSecurityHandler handler : securityHandlersMap.values()) {
        if (handler instanceof X509SecurityHandler) {
          X509SecurityHandler.X509MaterialParameter x509Param =
              (X509SecurityHandler.X509MaterialParameter) event.getSecurityMaterial()
                  .getMaterial(X509SecurityHandler.X509MaterialParameter.class);
          if (x509Param == null) {
            throw new NullPointerException("Hops TLS is enabled but X.509 parameter is null for " + appId);
          }
          SecurityManagerMaterial material = handler.generateMaterial(x509Param);
          if (material != null) {
            rmAppMaterial.addMaterial(material);
          }
        } else if (handler instanceof JWTSecurityHandler) {
          JWTSecurityHandler.JWTMaterialParameter jwtParam = (JWTSecurityHandler.JWTMaterialParameter) event
              .getSecurityMaterial().getMaterial(JWTSecurityHandler.JWTMaterialParameter.class);
          if (jwtParam == null) {
            throw new NullPointerException("JWT on Yarn is enabled but JWT parameter is null for " + appId);
          }
          SecurityManagerMaterial material = handler.generateMaterial(jwtParam);
          if (material != null) {
            rmAppMaterial.addMaterial(material);
          }
        }
      }
      if (rmAppMaterial.isEmpty()) {
        handler.handle(new RMAppEvent(appId, RMAppEventType.SECURITY_MATERIAL_GENERATED));
      } else {
        handler.handle(
            new RMAppSecurityMaterialGeneratedEvent(appId, rmAppMaterial, RMAppEventType.SECURITY_MATERIAL_GENERATED));
      }
    } catch (Exception ex) {
      LOG.error("Error while generating RMApp security material", ex);
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Error while generating application security " +
          "material for " + appId + " - " + ex.getMessage()));
    }
  }
  
  private void revokeX509Only(RMAppSecurityManagerEvent event) {
    RMAppSecurityHandler x509Handler = securityHandlersMap.get(X509SecurityHandler.class);
    if (x509Handler == null && isRPCTLSEnabled()) {
      LOG.error("Hops TLS is enabled but there is no X509SecurityHandler registered");
    } else {
      revokeX509(event, x509Handler);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public void revokeSecurityMaterial(RMAppSecurityManagerEvent event) {
    for (RMAppSecurityHandler handler : securityHandlersMap.values()) {
      if (handler instanceof X509SecurityHandler) {
        revokeX509(event, handler);
      } else if (handler instanceof JWTSecurityHandler) {
        revokeJWT(event, handler);
      }
    }
  }
  
  private void revokeX509(RMAppSecurityManagerEvent event, RMAppSecurityHandler securityHandler) {
    ApplicationId appId = event.getApplicationId();
    X509SecurityHandler.X509MaterialParameter x509Param = (X509SecurityHandler.X509MaterialParameter) event
        .getSecurityMaterial().getMaterial(X509SecurityHandler.X509MaterialParameter.class);
    if (x509Param != null) {
      securityHandler.revokeMaterial(x509Param, false);
      LOG.debug("Revoked X.509 material for " + appId);
    }
  }
  
  private void revokeJWT(RMAppSecurityManagerEvent event, RMAppSecurityHandler securityHandler) {
    JWTSecurityHandler.JWTMaterialParameter jwtParam = (JWTSecurityHandler.JWTMaterialParameter) event
        .getSecurityMaterial().getMaterial(JWTSecurityHandler.JWTMaterialParameter.class);
    if (jwtParam != null) {
      securityHandler.revokeMaterial(jwtParam, false);
      LOG.debug("Revoked JWT material for " + jwtParam.getApplicationId());
    }
  }
  
  public <P extends SecurityManagerMaterial> void revokeSecurityMaterialSync(P parameter) {
    if (parameter instanceof X509SecurityHandler.X509MaterialParameter) {
      X509SecurityHandler handler = (X509SecurityHandler) securityHandlersMap.get(X509SecurityHandler.class);
      handler.revokeMaterial((X509SecurityHandler.X509MaterialParameter) parameter, true);
    }
  }
  
  // Special case when an RMApp is recovering and crypto material was not recovered for whatever reason
  public void revokeAndGenerateMaterial(RMAppSecurityMaterial securityMaterial) {
    X509SecurityHandler.X509MaterialParameter x509Param = (X509SecurityHandler.X509MaterialParameter) securityMaterial
        .getMaterial(X509SecurityHandler.X509MaterialParameter.class);
    boolean exceptionThrown = false;
    boolean x509Revoked = false;
    ApplicationId applicationId = null;
    RMAppSecurityMaterial newSecurityMaterial = new RMAppSecurityMaterial();
  
    X509SecurityHandler x509Handler = (X509SecurityHandler) securityHandlersMap.get(X509SecurityHandler.class);
    JWTSecurityHandler jwtHandler = (JWTSecurityHandler) securityHandlersMap.get(JWTSecurityHandler.class);
    
    // X.509 material
    if (x509Param != null) {
      applicationId = x509Param.getApplicationId();
      x509Revoked = x509Handler.revokeMaterial(x509Param, true);
    }
    if (x509Revoked && !exceptionThrown) {
      try {
        X509SecurityHandler.X509SecurityManagerMaterial newX509 = x509Handler.generateMaterial(x509Param);
        if (newX509 != null) {
          newSecurityMaterial.addMaterial(newX509);
        }
      } catch (Exception ex) {
        LOG.error("Error when generating X.509 material for " + x509Param.getApplicationId(), ex);
        exceptionThrown = true;
      }
      
      // Add more security materials here
      
      // For JWT we do not need to invalidate the previous
      JWTSecurityHandler.JWTMaterialParameter jwtParam = (JWTSecurityHandler.JWTMaterialParameter) securityMaterial
          .getMaterial(JWTSecurityHandler.JWTMaterialParameter.class);
      if (!exceptionThrown) {
        try {
          if (jwtParam != null) {
            if (applicationId == null) {
              applicationId = jwtParam.getApplicationId();
            }
            JWTSecurityHandler.JWTSecurityManagerMaterial newJwt = jwtHandler.generateMaterial(jwtParam);
            if (newJwt != null) {
              newSecurityMaterial.addMaterial(newJwt);
            }
          }
        } catch (Exception ex) {
          LOG.error("Error when generating JWT material for " + applicationId, ex);
          exceptionThrown = true;
        }
      }
      if (exceptionThrown) {
        /**
         * You should revoke all security materials if something goes wrong
         */
        X509SecurityHandler.X509SecurityManagerMaterial generatedX509 = (X509SecurityHandler.X509SecurityManagerMaterial)
            newSecurityMaterial.getMaterial(X509SecurityHandler.X509SecurityManagerMaterial.class);
        if (generatedX509 != null) {
          // X.509 was generated correctly but something went wrong later
          // We should revoke it
          int version2revoke = x509Param.getCryptoMaterialVersion() + 1;
          X509SecurityHandler.X509MaterialParameter x5092revoke = new X509SecurityHandler.X509MaterialParameter(
              x509Param.getApplicationId(), x509Param.getAppUser(), version2revoke);
          x509Handler.revokeMaterial(x5092revoke, false);
        }
  
        // JWT was generated correctly but something else went wrong
        JWTSecurityHandler.JWTSecurityManagerMaterial generatedJWT = (JWTSecurityHandler.JWTSecurityManagerMaterial)
            newSecurityMaterial.getMaterial(JWTSecurityHandler.JWTSecurityManagerMaterial.class);
        if (generatedJWT != null) {
          jwtHandler.revokeMaterial(jwtParam, false);
        }
        
        handler.handle(new RMAppEvent(applicationId, RMAppEventType.KILL, "Error while revoking and generating new security " +
            "material for " + applicationId));
      } else {
        if (newSecurityMaterial.isEmpty()) {
          handler.handle(new RMAppEvent(applicationId, RMAppEventType.SECURITY_MATERIAL_GENERATED));
        } else {
          handler.handle(
              new RMAppSecurityMaterialGeneratedEvent(applicationId, newSecurityMaterial, RMAppEventType
                  .SECURITY_MATERIAL_GENERATED));
        }
      }
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public boolean isRPCTLSEnabled() {
    return isRPCTLSEnabled;
  }
  
  public abstract static class SecurityManagerMaterial {
    private final ApplicationId applicationId;
    
    protected SecurityManagerMaterial(ApplicationId applicationId) {
      this.applicationId = applicationId;
    }
  
    public ApplicationId getApplicationId() {
      return applicationId;
    }
  }
}
