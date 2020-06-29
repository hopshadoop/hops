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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;
import org.apache.hadoop.util.envVars.EnvironmentVariables;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class TestSuperuserKeystoresLoader {
  private static String USER;
  private final String IMPERSONATE_USER = "alice";

  @BeforeClass
  public static void beforeClass() throws Exception {
    USER = UserGroupInformation.getLoginUser().getUserName();
  }

  @After
  public void afterEach() {
    EnvironmentVariablesFactory.setInstance(null);
  }
  
  @Test
  public void testLoadEnvironmentVariable() throws Exception {
    final String materialDirectory = "/tmp/secrets";
    MockEnvironmentVariables envVars = new MockEnvironmentVariables();
    envVars.setEnv(SuperuserKeystoresLoader.SUPER_MATERIAL_DIRECTORY_ENV_VARIABLE, materialDirectory);
    EnvironmentVariablesFactory.setInstance(envVars);
  
    Configuration conf = new Configuration(false);
    final SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(IMPERSONATE_USER);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        X509SecurityMaterial material = loader.loadSuperUserMaterial();
        assertMaterialEquals(materialDirectory, loader, material);
        return null;
      }
    });
  }
  
  @Test
  public void testLoadConfiguration() throws Exception {
    final String materialDirectory = "/tmp/secure/bin";
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, materialDirectory);
    final SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(IMPERSONATE_USER);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        X509SecurityMaterial material = loader.loadSuperUserMaterial();
        assertMaterialEquals(materialDirectory, loader, material);
        return null;
      }
    });
  }
  
  @Test
  public void testLoadFromHome() throws Exception {
    final String materialDirectory = Paths.get(System.getProperty("user.home"),
        SuperuserKeystoresLoader.SUPER_MATERIAL_HOME_SUBDIRECTORY).toString();
    Configuration conf = new Configuration(false);
    final SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(IMPERSONATE_USER);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        X509SecurityMaterial material = loader.loadSuperUserMaterial();
        assertMaterialEquals(materialDirectory, loader, material);
        return null;
      }
    });
  }

  @Test
  public void testLoadFromHomeWithVariable() throws Exception {
    final String materialDirectory = "${HOME}/.hops_tls";
    final String expectedDirectory = Paths.get(System.getProperty("user.home"), ".hops_tls").toString();
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, materialDirectory);
    final SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(IMPERSONATE_USER);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        X509SecurityMaterial material = loader.loadSuperUserMaterial();
        assertMaterialEquals(expectedDirectory, loader, material);
        return null;
      }
    });
  }

  @Test
  public void testLoadFromDirectoryWithUserVariable() throws Exception {
    final String materialDirectory = "/some/directory/${USER}";
    final String expectedDirectory = Paths.get("/some/directory", USER).toString();
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, materialDirectory);
    final SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(IMPERSONATE_USER);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        X509SecurityMaterial material = loader.loadSuperUserMaterial();
        assertMaterialEquals(expectedDirectory, loader, material);
        return null;
      }
    });
  }
  
  private void assertMaterialEquals(final String materialDirectory, final SuperuserKeystoresLoader loader,
      final X509SecurityMaterial material) {
    assertEquals(Paths.get(materialDirectory).resolve(loader.getSuperKeystoreFilename(USER)),
        material.getKeyStoreLocation());
    assertEquals(Paths.get(materialDirectory).resolve(loader.getSuperTruststoreFilename(USER)),
        material.getTrustStoreLocation());
    assertEquals(Paths.get(materialDirectory).resolve(loader.getSuperMaterialPasswdFilename(USER)),
        material.getPasswdLocation());
  }
  
  private class MockEnvironmentVariables implements EnvironmentVariables {
  
    private final Map<String, String> envs;
    
    private MockEnvironmentVariables() {
      envs = new HashMap<>();
    }
    
    @Override
    public String getEnv(String variableName) {
      return envs.get(variableName);
    }
    
    public void setEnv(String name, String value) {
      envs.put(name, value);
    }
  }
}
