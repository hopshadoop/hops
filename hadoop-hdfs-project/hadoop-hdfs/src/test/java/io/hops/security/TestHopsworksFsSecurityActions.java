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
package io.hops.security;

import io.hops.common.security.FsSecurityActions;
import io.hops.common.security.HopsworksFsSecurityActions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for testing the FsSecurityActions interface with Hopsworks
 * By default the tests are ignored as they expect a running Hopsworks
 * instance.
 *
 * You SHOULD ALWAYS run these test manually if you've changed the interface
 *
 * This test assumes there is already a project in Hopsworks.
 * Before running the test change the Project specific username accordingly
 *
 */
@Ignore
public class TestHopsworksFsSecurityActions extends BaseTestHopsworksSecurityActions {
  private static final String USERNAME = "PROJECT__USERNAME";
  private static String classpath;
  
  private Configuration conf;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    classpath = KeyStoreTestUtil.getClasspathDir(TestHopsworksFsSecurityActions.class);
  }
  
  @Before
  public void beforeTest() throws Exception {
    HopsSecurityActionsFactory.getInstance().clear(
        conf.get(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY, DFSConfigKeys.DEFAULT_FS_SECURITY_ACTIONS_ACTOR));
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED, true);
    conf.set(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY,
        "io.hops.common.security.DevHopsworksFsSecurityActions");
    setupJWT(conf, classpath);
  }
  
  @After
  public void afterTest() throws Exception {
    HopsSecurityActionsFactory.getInstance().clear(
        conf.get(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY, DFSConfigKeys.DEFAULT_FS_SECURITY_ACTIONS_ACTOR));
  }
  
  @Test
  public void testGetX509Credentials() throws Exception {
    FsSecurityActions actor = (FsSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY, DFSConfigKeys.DEFAULT_FS_SECURITY_ACTIONS_ACTOR));
    HopsworksFsSecurityActions.X509CredentialsDTO x509DTO = actor.getX509Credentials(USERNAME);
    Assert.assertFalse(x509DTO.getFileExtension().isEmpty());
    Assert.assertFalse(x509DTO.getkStore().isEmpty());
    Assert.assertFalse(x509DTO.gettStore().isEmpty());
    Assert.assertFalse(x509DTO.getPassword().isEmpty());
  }
}
