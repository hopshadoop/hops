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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class TestCGroups2HandlerImpl {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  File topLevelCgroup;

  @Before
  public void setup() {
    topLevelCgroup = Paths.get(System.getProperty("test.build.data"), "cgroup").toFile();
    FileUtils.deleteQuietly(topLevelCgroup);

    Assert.assertTrue(topLevelCgroup.mkdirs());
  }

  @After
  public void destroy() throws IOException {
    if (topLevelCgroup != null) {
      FileUtils.deleteDirectory(topLevelCgroup);
    }
  }

  @Test
  public void testInitialize() throws ResourceHandlerException {
    Configuration conf = createConfiguration();
    CGroups2HandlerImpl cGroupsHandler = new CGroups2HandlerImpl(conf);
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

    String nonExistent = "new-hadoop-cgroup";
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, nonExistent);
    FileUtils.deleteQuietly(Paths.get(topLevelCgroup.toString(), nonExistent).toFile());
    Assert.assertFalse(Paths.get(topLevelCgroup.toString(), nonExistent).toFile().exists());

    cGroupsHandler = new CGroups2HandlerImpl(conf);
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

    File cGroupHierarchy = Paths.get(topLevelCgroup.toString(),
        conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY)).toFile();
    Assert.assertTrue(cGroupHierarchy.setWritable(false));

    exceptionRule.expect(ResourceHandlerException.class);
    exceptionRule.expectMessage("but it is not writable");
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);
  }

  @Test
  public void testPathForCgroup() throws ResourceHandlerException {
    Configuration conf = createConfiguration();
    CGroups2HandlerImpl cGroups2Handler = new CGroups2HandlerImpl(conf);
    cGroups2Handler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

    // In cgroup2 there is a single hierarchy and all controllers are in the same level
    String cgroupPath = cGroups2Handler.getPathForCGroup(CGroupsHandler.CGroupController.CPU, "");
    Assert.assertEquals(Paths.get(topLevelCgroup.toString(),
        conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY)).toString(), cgroupPath);

    cgroupPath = cGroups2Handler.getPathForCGroup(CGroupsHandler.CGroupController.CPU, "child");
    Assert.assertEquals(Paths.get(topLevelCgroup.toString(),
        conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY), "child").toString(), cgroupPath);
  }

  @Test
  public void testControllerPath() throws ResourceHandlerException {
    Configuration conf = createConfiguration();
    CGroups2HandlerImpl cGroups2Handler = new CGroups2HandlerImpl(conf);
    cGroups2Handler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

    // In cgroup2 there is a single hierarchy and all controllers are in the same level
    String cgroupPath = cGroups2Handler.getControllerPath(CGroupsHandler.CGroupController.CPU);
    Assert.assertEquals(Paths.get(topLevelCgroup.toString(),
        conf.get(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY)).toString(), cgroupPath);
  }

  private Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, topLevelCgroup.toString());
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "hadoop-yarn");
    return conf;
  }
}
