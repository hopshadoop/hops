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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import io.hops.security.CertificateLocalizationService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.*;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Supplier;
import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import static org.junit.Assert.fail;
import org.junit.Before;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;

public class TestApplicationMasterLauncher {

  private static final Log LOG = LogFactory
      .getLog(TestApplicationMasterLauncher.class);

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }
  
  private static final class MyContainerManagerImpl implements
      ContainerManagementProtocol {

    boolean launched = false;
    boolean cleanedup = false;
    String attemptIdAtContainerManager = null;
    String containerIdAtContainerManager = null;
    String nmHostAtContainerManager = null;
    long submitTimeAtContainerManager;
    int maxAppAttempts;

    @Override
    public StartContainersResponse
        startContainers(StartContainersRequest requests)
            throws YarnException {
      StartContainerRequest request = requests.getStartContainerRequests().get(0);
      LOG.info("Container started by MyContainerManager: " + request);
      launched = true;
      Map<String, String> env =
          request.getContainerLaunchContext().getEnvironment();

      Token containerToken = request.getContainerToken();
      ContainerTokenIdentifier tokenId = null;

      try {
        tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }

      ContainerId containerId = tokenId.getContainerID();
      containerIdAtContainerManager = containerId.toString();
      attemptIdAtContainerManager =
          containerId.getApplicationAttemptId().toString();
      nmHostAtContainerManager = tokenId.getNmHostAddress();
      submitTimeAtContainerManager =
          Long.parseLong(env.get(ApplicationConstants.APP_SUBMIT_TIME_ENV));
      maxAppAttempts = YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS;
      return StartContainersResponse.newInstance(
        new HashMap<String, ByteBuffer>(), new ArrayList<ContainerId>(),
        new HashMap<ContainerId, SerializedException>());
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
        throws YarnException {
      LOG.info("Container cleaned up by MyContainerManager");
      cleanedup = true;
      return null;
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException {
      return null;
    }

    @Override
    @Deprecated
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request)
            throws YarnException {
      return null;
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ResourceLocalizationResponse localize(
        ResourceLocalizationRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(
        ReInitializeContainerRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(
        ContainerId containerId) throws YarnException, IOException {
      return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
        request) throws YarnException, IOException {
      return null;
    }
  }

  @Test(timeout = 10000)
  public void testAMLaunchWithCryptoMaterial() throws Exception {
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, false);
    AtomicBoolean testPass = new AtomicBoolean(true);
    MockRM rm = new TestCryptoMockRM(conf, testPass);
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1337", 15 * 1024);
    RMApp app = rm.submitApp(1024);
    nm.nodeHeartbeat(true);
    
    RMAppAttempt appAttempt = app.getCurrentAppAttempt();
  
    MockAM am = rm.sendAMLaunched(appAttempt.getAppAttemptId());
    am.registerAppAttempt(true);
    nm.nodeHeartbeat(true);
    Assert.assertTrue(testPass.get());
    rm.stop();
  }
  
  @Test
  public void testSecurityMaterialRevocationAMCleanupMultipleAppAttempts() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    conf.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    MyContainerManagerImpl containerManager = new MyContainerManagerImpl();
    MockRM rm = new MockRMWithCustomAMLauncher(conf, containerManager);
    rm.start();
    JWTSecurityHandler jwtSecurityHandler = spy(new JWTSecurityHandler(rm.rmContext, rm.rmAppSecurityManager));
    rm.rmAppSecurityManager.registerRMAppSecurityHandlerWithType(jwtSecurityHandler, JWTSecurityHandler.class);
    MockNM nm = rm.registerNode("127.0.0.1:1234", 10 * 1024);
    // Submit application
    RMApp app = rm.submitApp(1024);
    nm.nodeHeartbeat(true);
  
    int waitCount = 0;
    while (containerManager.launched == false && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(containerManager.launched);
    RMAppAttempt appAttempt1 = app.getCurrentAppAttempt();
    MockAM am = new MockAM(rm.getRMContext(), rm.getApplicationMasterService(), appAttempt1.getAppAttemptId());
    am.registerAppAttempt();
    
    nm.nodeHeartbeat(true);
    
    // Fail first attempt
    nm.nodeHeartbeat(appAttempt1.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    waitCount = 0;
    while (containerManager.cleanedup == false && waitCount++ < 20) {
      LOG.info("Waiting for AM Cleanup to happen..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(containerManager.cleanedup);
    
    containerManager.cleanedup = false;
    TimeUnit.SECONDS.sleep(3);

    // Security material should NOT be revoked, there is one more attempt
    verify(jwtSecurityHandler, never()).revokeMaterial(any(JWTSecurityHandler.JWTMaterialParameter.class),
        any(Boolean.class));
  
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt appAttempt2 = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    am = new MockAM(rm.getRMContext(), rm.getApplicationMasterService(), appAttempt2.getAppAttemptId());
    am.registerAppAttempt();
    nm.nodeHeartbeat(true);
    
    rm.waitForState(appAttempt2.getAppAttemptId(), RMAppAttemptState.RUNNING);
    nm.nodeHeartbeat(true);
    
    // Fail second attempt
    nm.nodeHeartbeat(appAttempt2.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    waitCount = 0;
    while (containerManager.cleanedup == false && waitCount++ < 20) {
      LOG.info("Waiting for AM Cleanup to happen..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(containerManager.cleanedup);
    TimeUnit.SECONDS.sleep(3);
    
    // That was the final attempt, security material should have been revoked
    verify(jwtSecurityHandler).revokeMaterial(any(JWTSecurityHandler.JWTMaterialParameter.class),
        any(Boolean.class));
  }
  
  @Test
  public void testAMLaunchAndCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MyContainerManagerImpl containerManager = new MyContainerManagerImpl();
    MockRMWithCustomAMLauncher rm = new MockRMWithCustomAMLauncher(containerManager) {
      protected RMAppSecurityManager createRMAppSecurityManager() throws Exception {
        RMAppSecurityManager rmAppSecurityManager = Mockito.spy(new RMAppSecurityManager(rmContext));
        RMAppSecurityHandler<X509SecurityHandler.X509SecurityManagerMaterial, X509SecurityHandler.X509MaterialParameter> x509SecurityHandler
            = Mockito.spy(new X509SecurityHandler(rmContext, rmAppSecurityManager));
        rmAppSecurityManager.registerRMAppSecurityHandlerWithType(x509SecurityHandler, X509SecurityHandler.class);

        RMAppSecurityHandler<JWTSecurityHandler.JWTSecurityManagerMaterial, JWTSecurityHandler.JWTMaterialParameter> jwtSecurityMaterial
            = Mockito.spy(new JWTSecurityHandler(rmContext, rmAppSecurityManager));
        rmAppSecurityManager.registerRMAppSecurityHandlerWithType(jwtSecurityMaterial, JWTSecurityHandler.class);

        return rmAppSecurityManager;
      }
    };
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);

    RMApp app = rm.submitApp(2000);

    // kick the scheduling
    nm1.nodeHeartbeat(true);

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return containerManager.launched;
        }
      }, 100, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM Launch to happen.");
    }
    Assert.assertTrue(containerManager.launched);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId appAttemptId = attempt.getAppAttemptId();
    Assert.assertEquals(appAttemptId.toString(),
        containerManager.attemptIdAtContainerManager);
    Assert.assertEquals(app.getSubmitTime(),
        containerManager.submitTimeAtContainerManager);
    Assert.assertEquals(app.getRMAppAttempt(appAttemptId)
        .getMasterContainer().getId()
        .toString(), containerManager.containerIdAtContainerManager);
    Assert.assertEquals(nm1.getNodeId().toString(),
        containerManager.nmHostAtContainerManager);
    Assert.assertEquals(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS,
        containerManager.maxAppAttempts);

    MockAM am = new MockAM(rm.getRMContext(), rm
        .getApplicationMasterService(), appAttemptId);
    am.registerAppAttempt();
    am.unregisterAppAttempt();

    //complete the AM container to finish the app normally
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return containerManager.cleanedup;
        }
      }, 100, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM cleanup to happen.");
    }
    Assert.assertTrue(containerManager.cleanedup);

    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
    
    X509SecurityHandler.X509MaterialParameter x509Param =
        new X509SecurityHandler.X509MaterialParameter(app.getApplicationId(), app.getUser(),
            app.getCryptoMaterialVersion());
    verify(rm.rmAppSecurityManager.getSecurityHandler(X509SecurityHandler.class))
        .revokeMaterial(Mockito.eq(x509Param), Mockito.eq(false));
  
    JWTSecurityHandler.JWTMaterialParameter jwtParam =
        new JWTSecurityHandler.JWTMaterialParameter(app.getApplicationId(), app.getUser());
    jwtParam.setExpirationDate(app.getJWTExpiration());
    verify(rm.rmAppSecurityManager.getSecurityHandler(JWTSecurityHandler.class))
        .revokeMaterial(Mockito.eq(jwtParam), anyBoolean());
    rm.stop();
  }

  @Test
  public void testAMCleanupBeforeLaunch() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);
    RMApp app = rm.submitApp(2000);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return attempt.getMasterContainer() != null;
        }
      }, 10, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM Launch to happen.");
    }

    //send kill before launch
    rm.killApp(app.getApplicationId());
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    //Launch after kill
    AMLauncher launcher = new AMLauncher(rm.getRMContext(),
            attempt, AMLauncherEventType.LAUNCH, rm.getConfig()) {
        @Override
        public void onAMLaunchFailed(ContainerId containerId, Exception e) {
          Assert.assertFalse("NullPointerException happens "
                 + " while launching " + containerId,
                   e instanceof NullPointerException);
        }
        @Override
        protected ContainerManagementProtocol getContainerMgrProxy(
            ContainerId containerId) {
          return new MyContainerManagerImpl();
        }
    };
    launcher.run();
    rm.stop();
  }

  @Test
  public void testRetriesOnFailures() throws Exception {
    final ContainerManagementProtocol mockProxy =
        mock(ContainerManagementProtocol.class);
    final StartContainersResponse mockResponse =
        mock(StartContainersResponse.class);
    when(mockProxy.startContainers(any(StartContainersRequest.class)))
        .thenThrow(new NMNotYetReadyException("foo")).thenReturn(mockResponse);
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    conf.setInt(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 1);
    MockRM rm = new MockRMWithCustomAMLauncher(conf, null) {
      @Override
      protected ApplicationMasterLauncher createAMLauncher() {
        return new ApplicationMasterLauncher(getRMContext()) {
          @Override
          protected Runnable createRunnableLauncher(RMAppAttempt application,
              AMLauncherEventType event) {
            return new AMLauncher(context, application, event, getConfig()) {
              @Override
              protected YarnRPC getYarnRPC() {
                YarnRPC mockRpc = mock(YarnRPC.class);

                when(mockRpc.getProxy(
                    any(Class.class),
                    any(InetSocketAddress.class),
                    any(Configuration.class)))
                    .thenReturn(mockProxy);
                return mockRpc;
              }
            };
          }
        };
      }
    };

    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);

    RMApp app = rm.submitApp(2000);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    rm.drainEvents();

    MockRM.waitForState(app.getCurrentAppAttempt(),
        RMAppAttemptState.LAUNCHED, 500);
  }



  @SuppressWarnings("unused")
  @Test(timeout = 100000)
  public void testallocateBeforeAMRegistration() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    boolean thrown = false;
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5000);
    RMApp app = rm.submitApp(2000);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

    // request for containers
    int request = 2;
    AllocateResponse ar = null;
    try {
      ar = am.allocate("h1", 1000, request, new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationMasterNotRegisteredException e) {
    }

    // kick the scheduler
    nm1.nodeHeartbeat(true);

    AllocateResponse amrs = null;
    try {
      amrs = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationMasterNotRegisteredException e) {
    }

    am.registerAppAttempt();
    try {
      am.registerAppAttempt(false);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE
          + attempt.getAppAttemptId().getApplicationId(), e.getMessage());
    }

    // Simulate an AM that was disconnected and app attempt was removed
    // (responseMap does not contain attemptid)
    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1,
        ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    try {
      amrs = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationAttemptNotFoundException e) {
    }
  }

  @Test
  public void testSetupTokens() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5000);
    RMApp app = rm.submitApp(2000);
    /// kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MyAMLauncher launcher = new MyAMLauncher(rm.getRMContext(),
        attempt, AMLauncherEventType.LAUNCH, rm.getConfig());
    DataOutputBuffer dob = new DataOutputBuffer();
    Credentials ts = new Credentials();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(),
        0, dob.getLength());
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(null, null,
            null, null, securityTokens, null);
    ContainerId containerId = ContainerId.newContainerId(
        attempt.getAppAttemptId(), 0L);

    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (Exception e) {
      // ignore the first fake exception
    }
    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (java.io.EOFException e) {
      Assert.fail("EOFException should not happen.");
    }
  }

  static class MyAMLauncher extends AMLauncher {
    int count;
    public MyAMLauncher(RMContext rmContext, RMAppAttempt application,
        AMLauncherEventType eventType, Configuration conf) {
      super(rmContext, application, eventType, conf);
      count = 0;
    }

    protected org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
        createAndSetAMRMToken() {
      count++;
      if (count == 1) {
        throw new RuntimeException("createAndSetAMRMToken failure");
      }
      return null;
    }

    protected void setupTokens(ContainerLaunchContext container,
        ContainerId containerID) throws IOException {
      super.setupTokens(container, containerID);
    }
  }
  
  private class TestCryptoMockRM extends MockRM {
    private final AtomicBoolean testPass;
    
    private TestCryptoMockRM(Configuration conf, AtomicBoolean testPass) {
      super(conf);
      this.testPass = testPass;
    }

    @Override
    protected CertificateLocalizationService createCertificateLocalizationService() {
      return new CertificateLocalizationService(CertificateLocalizationService.ServiceType.RM) {
        @Override
        public char[] readSupersuperPassword() throws IOException {
          return "password".toCharArray();
        }
      };
    }
    
    @Override
    protected ApplicationMasterLauncher createAMLauncher() {
      return new ApplicationMasterLauncher(rmContext) {
        @Override
        protected Runnable createRunnableLauncher(RMAppAttempt application, AMLauncherEventType event) {
          return new TestCryptoAMLauncher(rmContext, application, event, conf, testPass);
        }
      };
    }
  }
  
  private class TestCryptoAMLauncher extends AMLauncher {
    private final AtomicBoolean testPass;
    
    public TestCryptoAMLauncher(RMContext rmContext,
        RMAppAttempt application, AMLauncherEventType eventType, Configuration conf, AtomicBoolean testPass) {
      super(rmContext, application, eventType, conf);
      this.testPass = testPass;
    }
    
    @Override
    protected void setupX509Material(StartContainersRequest request, RMApp application) {
      super.setupX509Material(request, application);
      if (request.getKeyStore() == null || request.getKeyStore().limit() == 0
          || request.getKeyStorePassword() == null
          || request.getTrustStore() == null || request.getTrustStore().limit() == 0
          || request.getTrustStorePassword() == null) {
        testPass.set(false);
      }
    }
    
    @Override
    protected void setupJWTMaterial(StartContainersRequest request, RMApp application) {
      super.setupJWTMaterial(request, application);
      if (request.getJWT() == null || request.getJWT().isEmpty()) {
        testPass.set(false);
      }
    }
  }
}
