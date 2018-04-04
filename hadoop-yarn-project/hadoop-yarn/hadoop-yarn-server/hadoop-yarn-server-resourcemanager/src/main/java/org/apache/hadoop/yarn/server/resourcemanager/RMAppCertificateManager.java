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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.security.CertificateLocalizationService;

import java.util.concurrent.ExecutionException;

public class RMAppCertificateManager implements EventHandler<RMAppCertificateManagerEvent> {
  private final static Log LOG = LogFactory.getLog(RMAppCertificateManager.class);
  
  private final RMContext rmContext;
  private final Configuration conf;
  private final EventHandler handler;
  private final CertificateLocalizationService certificateLocalizationService;
  
  public RMAppCertificateManager(RMContext rmContext, Configuration conf) {
    this.rmContext = rmContext;
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.certificateLocalizationService = rmContext.getCertificateLocalizationService();
  }
  
  @SuppressWarnings("unchecked")
  private void generateCertificate(ApplicationId appId) {
    LOG.info("Generating certificate for application: " + appId);
    
    handler.handle(new RMAppEvent(appId, RMAppEventType.START));
  }
  
  private void revokeCertificate(ApplicationId appId, String applicationUser) {
    LOG.info("Revoking certificate for application: " + appId);
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
          CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT) && certificateLocalizationService != null) {
      try {
        certificateLocalizationService.removeMaterial(applicationUser);
      } catch (InterruptedException | ExecutionException ex) {
        LOG.warn("Could not remove material for user " + applicationUser + " and application " + appId, ex);
      }
    }
  }
  
  @Override
  public void handle(RMAppCertificateManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.info("Processing event type: " + event.getType() + " for application: " + applicationId);
    if (event.getType().equals(RMAppCertificateManagerEventType.GENERATE_CERTIFICATE)) {
      generateCertificate(applicationId);
    } else if (event.getType().equals(RMAppCertificateManagerEventType.REVOKE_CERTIFICATE)) {
      String applicationUser = ((RMAppCertificateManagerRevokeEvent) event).getApplicationUser();
      revokeCertificate(applicationId, applicationUser);
    } else {
      LOG.warn("Unknown event type " + event.getType());
    }
  }
}
