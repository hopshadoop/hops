/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.security.JWTSecurityHandler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppSecurityHandler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMAppSecurityManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.X509SecurityHandler;
import org.mockito.Mockito;

public class MockRMWithCustomAMLauncher extends MockRM {

  private final ContainerManagementProtocol containerManager;
  private final YarnRPC yarnRPC;
  
  public MockRMWithCustomAMLauncher(ContainerManagementProtocol containerManager) {
    this(new Configuration(), containerManager, null);
  }
  
  public MockRMWithCustomAMLauncher(ContainerManagementProtocol containerManager, YarnRPC yarnRPC) {
    this(new Configuration(), containerManager, yarnRPC);
  }

  public MockRMWithCustomAMLauncher(Configuration conf, ContainerManagementProtocol containerManager) {
    this(conf, containerManager, null);
  }
  
  public MockRMWithCustomAMLauncher(Configuration conf, ContainerManagementProtocol containerManager, YarnRPC yarnRPC) {
    super(conf);
    this.containerManager = containerManager;
    this.yarnRPC = yarnRPC;
  }

  @Override
  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(getRMContext()) {
      @Override
      protected Runnable createRunnableLauncher(RMAppAttempt application,
          AMLauncherEventType event) {
        return new AMLauncher(context, application, event, getConfig()) {
          @Override
          protected ContainerManagementProtocol getContainerMgrProxy(
              ContainerId containerId) {
            this.rpc = yarnRPC;
            return containerManager;
          }
          @Override
          protected Token<AMRMTokenIdentifier> createAndSetAMRMToken() {
            Token<AMRMTokenIdentifier> amRmToken =
                super.createAndSetAMRMToken();
            InetSocketAddress serviceAddr =
                getConfig().getSocketAddr(
                  YarnConfiguration.RM_SCHEDULER_ADDRESS,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
            SecurityUtil.setTokenService(amRmToken, serviceAddr);
            return amRmToken;
          }
        };
      }
    };
  }
}