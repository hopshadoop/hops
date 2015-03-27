/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RMStateVersionProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.RMStateVersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test class for the NDBRMStateStore class.
 */
public class TestNDBRMStateStore extends RMStateStoreTestBase {

  public static final Log LOG = LogFactory.getLog(TestNDBRMStateStore.class);
  private YarnConfiguration conf;

  class TestNDBRMStateStoreTester implements RMStateStoreHelper {

    TestNDBRMStoreInternal store;

    class TestNDBRMStoreInternal extends NDBRMStateStore {

      public TestNDBRMStoreInternal(Configuration conf) throws IOException {
        try {
          RMStorageFactory.setConfiguration(conf);
          init(conf);
          start();
        } catch (StorageInitializtionException ex) {
          LOG.error("Error initializing Configuration", ex);
        }
      }

      protected boolean applicationIdExists(String applicationId)
          throws IOException {
        boolean exists = false;
        io.hops.metadata.yarn.entity.rmstatestore.ApplicationState
            appStateFound = getApplicationState(applicationId);
        if (appStateFound != null) {
          exists = true;
        }
        return exists;
      }
    }

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      return store = new TestNDBRMStoreInternal(new YarnConfiguration());
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      //TODO: Is returning true valid?
      return true;
    }

    @Override
    public void writeVersion(RMStateVersion version) throws Exception {
      store.storeVersion(((RMStateVersionPBImpl) version));
    }

    @Override
    public RMStateVersion getCurrentVersion() throws Exception {
      return store.getCurrentVersion();
    }

    @Override
    public boolean appExists(RMApp app) throws Exception {
      return store.applicationIdExists(app.getApplicationId().toString());
    }
  }

  /**
   * Basic testing method, iused RMStateStore testing interfaces.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testNDBRMStateStoreRealNDB() throws Exception {
    TestNDBRMStateStoreTester ndbTester = new TestNDBRMStateStoreTester();
    testRMAppStateStore(ndbTester);
    testRMDTSecretManagerStateStore(ndbTester);
    testCheckVersion(ndbTester);
    testAppDeletion(ndbTester);
  }

  /**
   * Runs before individual testing methods to initialize NDB connections.
   */
  @Before
  public void setup() throws IOException {
    LOG.info("HOP :: Setting up Factories");
    conf = new YarnConfiguration();
    try {
      YarnAPIStorageFactory.setConfiguration(conf);
      RMStorageFactory.setConfiguration(conf);
      RMUtilities.InitializeDB();
    } catch (StorageInitializtionException ex) {
      LOG.error("Error initializing Configuration", ex);
    }
  }


  /**
   * Test method for storing/retrieving RMStateVersion.
   *
   * @throws InvalidProtocolBufferException
   */
  @Test
  public void testVersionStoreRetrieve()
      throws InvalidProtocolBufferException, IOException {
    RMStateVersion versionStore = RMStateVersion.newInstance(1, 1);
    byte[] store =
        ((RMStateVersionPBImpl) versionStore).getProto().toByteArray();
    RMUtilities.setRMStateVersionLightweight(store);
    byte[] retrieve = RMUtilities.getRMStateVersionBinaryLightweight(0);
    RMStateVersion versionRetrieve =
        new RMStateVersionPBImpl(RMStateVersionProto.parseFrom(retrieve));
    Assert.assertEquals(versionStore.getMinorVersion(),
        versionRetrieve.getMinorVersion());
    Assert.assertEquals(versionStore.getMajorVersion(),
        versionRetrieve.getMajorVersion());
    Assert.assertEquals(versionStore, versionRetrieve);
  }

  /**
   * Creates appState, appAttemptState, security tokens. Modify before using
   * according to testing preferences.
   *
   * @throws IOException
   * @throws Exception
   */
  @Test
  public void testAppStateAndSecurityStoreRetrieve()
      throws IOException, Exception {
    //Store Application and Attempt
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 2);
    ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl();
    ByteBuffer tokens = ByteBuffer.wrap("BOGUS".getBytes());
    URL url = URL.newInstance("scheme", "host", 1234, "file");
    long time = System.currentTimeMillis();
    LocalResource local = LocalResource.newInstance(url, LocalResourceType.FILE,
        LocalResourceVisibility.PUBLIC, time, time);
    Map<String, LocalResource> map1 = new HashMap<String, LocalResource>();
    map1.put("localresource", local);
    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("test", "test");
    Map<ApplicationAccessType, String> map3 =
        new HashMap<ApplicationAccessType, String>();
    map3.put(ApplicationAccessType.VIEW_APP, "type");
    List<String> commands = new ArrayList<String>();
    commands.add("command1");
    commands.add("command2");
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();
    serviceData.put("BOGUS", ByteBuffer.wrap("BOGUS".getBytes()));
    ContainerLaunchContext clx = ContainerLaunchContext
        .newInstance(map1, map2, commands, serviceData, tokens, map3);
    appState.setApplicationSubmissionContext(ApplicationSubmissionContext
            .newInstance(appId, "appName", "queue", Priority.newInstance(1),
                clx, false, false, 1, Resource.newInstance(5012, 8)));


    ApplicationAttemptStateDataPBImpl appAttemptState =
        new ApplicationAttemptStateDataPBImpl();
    appAttemptState.setAttemptId(attemptId);
    appAttemptState.setDiagnostics("diagnostics");
    appAttemptState.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
    appAttemptState.setFinalTrackingUrl("http://testurl.se");
    String identifier = "identifier";
    String password = "password";

    appAttemptState.setMasterContainer(Container
            .newInstance(ContainerId.newInstance(attemptId, 1),
                NodeId.newInstance("host", 0), "host",
                Resource.newInstance(5012, 8), Priority.newInstance(1), Token
                .newInstance(identifier.getBytes(), "kind", password.getBytes(),
                    "service")));
    byte[] appArray = appState.getProto().toByteArray();
    byte[] appAttemptArray = appAttemptState.getProto().toByteArray();
    String appStateStr = appState.toString();
    String appAttemptStateStr = appAttemptState.toString();
    LOG.info(
        "HOP :: appState=" + appStateStr + ", byte length=" + appArray.length);
    LOG.info("HOP :: appAttemptState=" + appAttemptStateStr + ", byte length=" +
        appAttemptArray.length);
  }


  /**
   * Find applicationState by its applicationId string representation.
   *
   * @param applicationId
   * @return
   * @throws IOException
   */
  public static ApplicationState getApplicationState(final String applicationId)
      throws IOException {
    LightWeightRequestHandler getApplicationStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationStateDataAccess DA =
                (ApplicationStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationStateDataAccess.class);
            ApplicationState appState =
                (ApplicationState) DA.findByApplicationId(applicationId);

            connector.commit();
            return appState;
          }
        };
    return (ApplicationState) getApplicationStateHandler.handle();
  }

}
