/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import io.hops.util.DBUtility;
import io.hops.util.RMStorageFactory;
import io.hops.util.YarnAPIStorageFactory;
import java.io.IOException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.Before;
import org.junit.Test;

public class TestDBRMStateStore extends RMStateStoreTestBase {

  private DBRMStateStore stateStore = null;
  private YarnConfiguration conf;

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    DBUtility.InitializeDB();
  }

  @Test(timeout = 60000)
  public void testApps() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testRMAppStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testClientTokens() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testRMDTSecretManagerStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testVersion() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testCheckVersion(tester);
  }

  @Test(timeout = 60000)
  public void testEpoch() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testEpoch(tester);
  }

  @Test(timeout = 60000)
  public void testAppDeletion() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testAppDeletion(tester);
  }

  @Test(timeout = 60000)
  public void testDeleteStore() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testDeleteStore(tester);
  }

  @Test(timeout = 60000)
  public void testAMTokens() throws Exception {
    dbStateStoreTester tester = new dbStateStoreTester();
    testAMRMTokenSecretManagerStateStore(tester);
  }

  class dbStateStoreTester implements RMStateStoreHelper {

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      if (stateStore != null) {
        stateStore.close();
      }
      stateStore = new DBRMStateStore();
      stateStore.init(conf);
      stateStore.start();
      return stateStore;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      // There should be 5 total entries:
      //   2 entries for app 0010 with one attempt
      //   3 entries for app 0001 with two attempts
      return stateStore.getNumEntriesInDatabase() == 5;
    }

    @Override
    public void writeVersion(Version version) throws Exception {
      byte[] versionDb = ((VersionPBImpl) version).getProto().toByteArray();
      stateStore.storeVersiondb(versionDb);
    }

    @Override
    public Version getCurrentVersion() throws Exception {
      return stateStore.getCurrentVersion();
    }

    @Override
    public boolean appExists(RMApp app) throws Exception {
      return stateStore.loadRMAppState(app.getApplicationId()) != null;
    }
    
    @Override
    public boolean attemptExists(RMAppAttempt attempt) throws Exception {
      return stateStore.loadRMAppAttemptState(attempt.getAppAttemptId())!=null;
    }
  }

}
