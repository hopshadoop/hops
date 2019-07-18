/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls.resourcemanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;

import java.util.Map;

public class MockAMLauncher extends ApplicationMasterLauncher
    implements EventHandler<AMLauncherEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      MockAMLauncher.class);

  Map<String, AMSimulator> amMap;
  SLSRunner se;

  public MockAMLauncher(SLSRunner se, RMContext rmContext,
      Map<String, AMSimulator> amMap) {
    super(rmContext);
    this.amMap = amMap;
    this.se = se;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Do nothing
  }

  @Override
  protected void serviceStart() throws Exception {
    // Do nothing
  }

  @Override
  protected void serviceStop() throws Exception {
    // Do nothing
  }

  private void setupAMRMToken(RMAppAttempt appAttempt) {
    // Setup AMRMToken
    Token<AMRMTokenIdentifier> amrmToken =
        super.context.getAMRMTokenSecretManager().createAndGetAMRMToken(
            appAttempt.getAppAttemptId());
    ((RMAppAttemptImpl) appAttempt).setAMRMToken(amrmToken);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(AMLauncherEvent event) {
    if (AMLauncherEventType.LAUNCH == event.getType()) {
      ApplicationId appId =
          event.getAppAttempt().getAppAttemptId().getApplicationId();

      // find AMSimulator
      for (AMSimulator ams : amMap.values()) {
        if (ams.getApplicationId() != null && ams.getApplicationId().equals(
            appId)) {
          try {
            Container amContainer = event.getAppAttempt().getMasterContainer();

            setupAMRMToken(event.getAppAttempt());

            // Notify RMAppAttempt to change state
            super.context.getDispatcher().getEventHandler().handle(
                new RMAppAttemptEvent(event.getAppAttempt().getAppAttemptId(),
                    RMAppAttemptEventType.LAUNCHED));

            ams.notifyAMContainerLaunched(
                event.getAppAttempt().getMasterContainer());
            LOG.info("Notify AM launcher launched:" + amContainer.getId());

            se.getNmMap().get(amContainer.getNodeId())
                .addNewContainer(amContainer, 100000000L);

            return;
          } catch (Exception e) {
            throw new YarnRuntimeException(e);
          }
        }
      }

      throw new YarnRuntimeException(
          "Didn't find any AMSimulator for applicationId=" + appId);
    }
  }
}
