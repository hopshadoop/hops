/*
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

import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.DBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;


public abstract class RMHATestBase extends ClientBaseWithFixes{

  private static final int ZK_TIMEOUT_MS = 5000;
  private static StateChangeRequestInfo requestInfo =
      new StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);
  protected Configuration configuration = new YarnConfiguration();
  static MockRM rm1 = null;
  static MockRM rm2 = null;
  Configuration confForRM1;
  Configuration confForRM2;

  @Before
  public void setup() throws Exception {
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    configuration.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    configuration.set(YarnConfiguration.RM_STORE,
        DBRMStateStore.class.getName());
    configuration.set(CommonConfigurationKeys.ZK_ADDRESS, hostPort);
    configuration.setInt(CommonConfigurationKeys.ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.set(YarnConfiguration.RM_CLUSTER_ID, "test-yarn-cluster");
    int base = 100;
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, "rm1"), "0.0.0.0:"
          + (base + 20));
      configuration.set(HAUtil.addSuffix(confKey, "rm2"), "0.0.0.0:"
          + (base + 40));
      base = base * 2;
    }

    RMStorageFactory.setConfiguration(configuration);
    YarnAPIStorageFactory.setConfiguration(configuration);
    DBUtility.InitializeDB();

    confForRM1 = new Configuration(configuration);
    confForRM1.set(YarnConfiguration.RM_HA_ID, "rm1");
    confForRM1.set(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            "localhost:8034");
    confForRM2 = new Configuration(configuration);
    confForRM2.set(YarnConfiguration.RM_HA_ID, "rm2");
    confForRM2.set(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS,
            "localhost:8035");
  }

  @After
  public void teardown() {
    if (rm1 != null) {
      rm1.stop();
    }
    if (rm2 != null) {
      rm2.stop();
    }
  }

  protected MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    rm.waitForState(app.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.RUNNING);
    return am;
  }

  private MockRM initMockRMWithOldConf(Configuration confForRM1) {
    return new MockRM(confForRM1, null, false, false) {
      @Override
      protected AdminService createAdminService() {
        return new AdminService(this) {
          @Override
          protected void startServer() {
            // override to not start rpc handler
          }

          @Override
          protected void stopServer() {
            // don't do anything
          }

          @Override
          protected Configuration loadNewConfiguration()
              throws IOException, YarnException {
            return confForRM1;
          }
        };
      }
    };
  }

  protected void startRMs() throws IOException {
    rm1 = initMockRMWithOldConf(confForRM1);
    rm2 = initMockRMWithOldConf(confForRM2);

    startRMs(rm1, confForRM1, rm2, confForRM2);
  }

  protected void startRMsWithCustomizedRMAppManager() throws IOException {
    final Configuration conf1 = new Configuration(confForRM1);

    rm1 = new MockRM(conf1) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new MyRMAppManager(this.rmContext, this.scheduler,
            this.masterService, this.applicationACLsManager, conf1);
      }
    };

    rm2 = new MockRM(confForRM2);

    startRMs(rm1, conf1, rm2, confForRM2);
  }

  private static class MyRMAppManager extends RMAppManager {

    private Configuration conf;
    private RMContext rmContext;

    public MyRMAppManager(RMContext context, YarnScheduler scheduler,
        ApplicationMasterService masterService,
        ApplicationACLsManager applicationACLsManager, Configuration conf) {
      super(context, scheduler, masterService, applicationACLsManager, conf);
      this.conf = conf;
      this.rmContext = context;
    }

    @Override
    protected void submitApplication(
        ApplicationSubmissionContext submissionContext, long submitTime,
        String user) throws YarnException {
      //Do nothing, just add the application to RMContext
      RMAppImpl application =
          new RMAppImpl(submissionContext.getApplicationId(), this.rmContext,
              this.conf, submissionContext.getApplicationName(), user,
              submissionContext.getQueue(), submissionContext,
              this.rmContext.getScheduler(),
              this.rmContext.getApplicationMasterService(),
              submitTime, submissionContext.getApplicationType(),
              submissionContext.getApplicationTags(), null);
      this.rmContext.getRMApps().put(submissionContext.getApplicationId(),
          application);
      //Do not send RMAppEventType.START event
      //so the state of Application will not reach to NEW_SAVING state.
    }
  }

  protected boolean isFinalState(RMAppState state) {
    return state.equals(RMAppState.FINISHING)
    || state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED)
    || state.equals(RMAppState.KILLED);
  }

  protected void explicitFailover() throws IOException {
    rm1.adminService.transitionToStandby(requestInfo);
    rm2.adminService.transitionToActive(requestInfo);
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);
    Assert.assertTrue(rm2.getRMContext().getHAServiceState()
        == HAServiceState.ACTIVE);
  }

  protected void startRMs(MockRM rm1, Configuration confForRM1, MockRM rm2,
      Configuration confForRM2) throws IOException {
    rm1.init(confForRM1);
    rm1.start();
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);

    rm2.init(confForRM2);
    rm2.start();
    Assert.assertTrue(rm2.getRMContext().getHAServiceState()
        == HAServiceState.STANDBY);

    rm1.adminService.transitionToActive(requestInfo);
    Assert.assertTrue(rm1.getRMContext().getHAServiceState()
        == HAServiceState.ACTIVE);
  }
}
