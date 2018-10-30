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
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.service.AbstractService;
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
  
  public RMAppSecurityManager(RMContext rmContext) {
    super(RMAppSecurityManager.class.getName());
    Security.addProvider(new BouncyCastleProvider());
    this.rmContext = rmContext;
    securityHandlersMap = new HashMap();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.debug("Initializing RMAppCertificateManager");
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    rmAppCertificateActions = RMAppSecurityActionsFactory.getInstance().getActor(conf);
    isRPCTLSEnabled = conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  
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
    LOG.info("Starting RMAppCertificateManager");
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
      handler.registerRenewer((X509SecurityHandler.X509MaterialParameter) parameter);
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
        }
        // This could be a little bit more elegant
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
      }
    }
  }
  
  private void revokeX509(RMAppSecurityManagerEvent event, RMAppSecurityHandler securityHandler) {
    ApplicationId appId = event.getApplicationId();
    X509SecurityHandler.X509MaterialParameter x509Param = (X509SecurityHandler.X509MaterialParameter) event
        .getSecurityMaterial().getMaterial(X509SecurityHandler.X509MaterialParameter.class);
    securityHandler.revokeMaterial(x509Param, false);
    LOG.debug("Revoked X.509 material for " + appId);
  }
  
  public <P extends SecurityManagerMaterial> void revokeSecurityMaterialSync(P parameter) {
    if (parameter instanceof X509SecurityHandler.X509MaterialParameter) {
      X509SecurityHandler handler = (X509SecurityHandler) securityHandlersMap.get(X509SecurityHandler.class);
      handler.revokeMaterial((X509SecurityHandler.X509MaterialParameter) parameter, true);
    }
  }
  
  public void revokeAndGenerateMaterial(RMAppSecurityMaterial securityMaterial) {
    X509SecurityHandler.X509MaterialParameter x509Param = (X509SecurityHandler.X509MaterialParameter) securityMaterial
        .getMaterial(X509SecurityHandler.X509MaterialParameter.class);
    boolean exceptionThrown = false;
    boolean x509Revoked = false;
    ApplicationId applicationId = null;
    RMAppSecurityMaterial newSecurityMaterial = new RMAppSecurityMaterial();
    
    // X.509 material
    X509SecurityHandler x509Handler = (X509SecurityHandler) securityHandlersMap.get(X509SecurityHandler.class);
    if (x509Param != null) {
      applicationId = x509Param.getApplicationId();
      x509Revoked = x509Handler.revokeMaterial(x509Param, true);
    }
    if (x509Revoked && !exceptionThrown) {
      try {
        X509SecurityHandler.X509SecurityManagerMaterial newX509 = x509Handler.generateMaterial(x509Param);
        newSecurityMaterial.addMaterial(newX509);
      } catch (Exception ex) {
        LOG.error("Error when generating X.509 material for " + x509Param.getApplicationId(), ex);
        exceptionThrown = true;
      }
      
      // Add more security materials here
      
      if (exceptionThrown) {
        /**
         * You should revoke all security materials if something goes wrong
         */
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
  
  /*@InterfaceAudience.Private
  @VisibleForTesting
  public void revokeAndGenerateCertificates(ApplicationId appId, String appUser, Integer cryptoMaterialVersion) {
    // Certificate revocation here is blocking
    if (revokeInternal(getCertificateIdentifier(appId, appUser, cryptoMaterialVersion))) {
      generateCertificate(appId, appUser, cryptoMaterialVersion);
    } else {
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Could not revoke previously generated certificate"));
    }
  }*/
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public boolean isRPCTLSEnabled() {
    return isRPCTLSEnabled;
  }
  
  /*public void revokeCertificateSynchronously(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion) {
    if (isRPCTLSEnabled()) {
      LOG.info("Revoking certificate for application: " + appId + " with version " + cryptoMaterialVersion);
      revokeInternal(getCertificateIdentifier(appId, applicationUser, cryptoMaterialVersion));
    }
  }*/
  
  protected abstract static class SecurityManagerMaterial {
    private final ApplicationId applicationId;
    
    protected SecurityManagerMaterial(ApplicationId applicationId) {
      this.applicationId = applicationId;
    }
  
    public ApplicationId getApplicationId() {
      return applicationId;
    }
  }
}
