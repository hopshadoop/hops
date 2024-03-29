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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGroupVersion.*;

/**
 * Unit test for CGroupsMemoryResourceHandlerImpl and CGroups2MemoryResourceHandlerImpl.
 */
@RunWith(Parameterized.class)
public class TestCGroupsMemoryResourceHandlerImpl {

  private CGroupsHandler mockCGroupsHandler;
  private MemoryResourceHandler cGroupsMemoryResourceHandler;

  private CGroupsHandler.CGroupVersion cGroupVersion;

  @Parameterized.Parameters
  public static Collection<Object> params() {
    return TestCGroupsCpuResourceHandlerImpl.params();
  }

  public TestCGroupsMemoryResourceHandlerImpl(CGroupsHandler.CGroupVersion cGroupVersion) {
    this.cGroupVersion = cGroupVersion;
  }

  @Before
  public void setup() {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    when(mockCGroupsHandler.getPathForCGroup(any(), any())).thenReturn(".");
    if (cGroupVersion.equals(V1)) {
      cGroupsMemoryResourceHandler =
          new CGroupsMemoryResourceHandlerImpl(mockCGroupsHandler);
    } else if (cGroupVersion.equals(V2)) {
      cGroupsMemoryResourceHandler =
          new CGroups2MemoryResourceHandlerImpl(mockCGroupsHandler);
    } else {
      throw new RuntimeException("Unsupported cgroup version " + cGroupVersion);
    }
  }

  @Test
  public void testBootstrap() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.bootstrap(conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.MEMORY);
    Assert.assertNull(ret);
    // cgroup2 does not support per cgroup swappiness
    if (cGroupVersion.equals(V1)) {
      Assert.assertEquals("Default swappiness value incorrect", 0,
          ((CGroupsMemoryResourceHandlerImpl) cGroupsMemoryResourceHandler).getSwappiness());
    }

    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, true);
    try {
      cGroupsMemoryResourceHandler.bootstrap(conf);
    } catch(ResourceHandlerException re) {
      Assert.fail("Pmem check should be allowed to run with cgroups");
    }
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, true);
    try {
      cGroupsMemoryResourceHandler.bootstrap(conf);
    } catch(ResourceHandlerException re) {
      Assert.fail("Vmem check should be allowed to run with cgroups");
    }
  }

  @Test
  public void testSwappinessValues() throws Exception {
    Assume.assumeTrue("Per group swappiness is supported only in cgroup1", cGroupVersion.equals(V1));
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    conf.setInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS, -1);
    try {
      cGroupsMemoryResourceHandler.bootstrap(conf);
      Assert.fail("Negative values for swappiness should not be allowed.");
    } catch (ResourceHandlerException re) {
      // do nothing
    }
    try {
      conf.setInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS, 101);
      cGroupsMemoryResourceHandler.bootstrap(conf);
      Assert.fail("Values greater than 100 for swappiness"
          + " should not be allowed.");
    } catch (ResourceHandlerException re) {
      // do nothing
    }
    conf.setInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS, 60);
    cGroupsMemoryResourceHandler.bootstrap(conf);
    Assert.assertEquals("Swappiness value incorrect", 60,
        ((CGroupsMemoryResourceHandlerImpl) cGroupsMemoryResourceHandler).getSwappiness());
  }

  @Test
  public void testPreStart() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    cGroupsMemoryResourceHandler.bootstrap(conf);
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.MEMORY, id))
        .thenReturn(path);
    int memory = 1024;
    when(mockContainer.getResource())
        .thenReturn(Resource.newInstance(memory, 1));
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.MEMORY, id);

    if (cGroupVersion.equals(V1)) {
     verify(mockCGroupsHandler, times(1))
         .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
             CGroupsHandler.MemoryParameters.HARD_LIMIT_BYTES.getName(),
             String.valueOf(memory) + "M");
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SOFT_LIMIT_BYTES.getName(),
              String.valueOf((int) (memory * 0.9)) + "M");
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SWAPPINESS.getName(), String.valueOf(0));
    } else if (cGroupVersion.equals(V2)) {
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_MAX.getName(),
              String.valueOf(memory) + "M");
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_HIGH.getName(),
              String.valueOf((int) (memory * 0.9)) + "M");
      verify(mockCGroupsHandler, never())
          .updateCGroupParam(eq(CGroupsHandler.CGroupController.MEMORY), eq(id),
              eq(CGroupsHandler.MemoryParameters.SWAPPINESS.getName()), anyString());
    }
    Assert.assertNotNull(ret);
    Assert.assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    Assert.assertEquals(1, args.size());
    Assert.assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testPreStartNonEnforced() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED, false);
    cGroupsMemoryResourceHandler.bootstrap(conf);
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.MEMORY, id))
        .thenReturn(path);
    int memory = 1024;
    when(mockContainer.getResource())
        .thenReturn(Resource.newInstance(memory, 1));
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.MEMORY, id);

    if (cGroupVersion.equals(V1)) {
      verify(mockCGroupsHandler, times(0))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.HARD_LIMIT_BYTES.getName(),
              String.valueOf(memory) + "M");
      verify(mockCGroupsHandler, times(0))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SOFT_LIMIT_BYTES.getName(),
              String.valueOf((int) (memory * 0.9)) + "M");
      verify(mockCGroupsHandler, times(0))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SWAPPINESS.getName(), String.valueOf(0));
    } else if (cGroupVersion.equals(V2)) {
      verify(mockCGroupsHandler, times(0))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_MAX.getName(),
              String.valueOf(memory) + "M");
      verify(mockCGroupsHandler, times(0))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_HIGH.getName(),
              String.valueOf((int) (memory * 0.9)) + "M");
      verify(mockCGroupsHandler, never())
          .updateCGroupParam(eq(CGroupsHandler.CGroupController.MEMORY), eq(id),
              eq(CGroupsHandler.MemoryParameters.SWAPPINESS.getName()), anyString());
    }

    Assert.assertNotNull(ret);
    Assert.assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    Assert.assertEquals(1, args.size());
    Assert.assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    Assert.assertNull(
        cGroupsMemoryResourceHandler.reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Assert
        .assertNull(cGroupsMemoryResourceHandler.postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1))
        .deleteCGroup(CGroupsHandler.CGroupController.MEMORY, id);
  }

  @Test
  public void testTeardown() throws Exception {
    Assert.assertNull(cGroupsMemoryResourceHandler.teardown());
  }

  @Test
  public void testOpportunistic() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);

    cGroupsMemoryResourceHandler.bootstrap(conf);
    ContainerTokenIdentifier tokenId = mock(ContainerTokenIdentifier.class);
    when(tokenId.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    Container container = mock(Container.class);
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    when(container.getContainerId()).thenReturn(mockContainerId);
    when(container.getContainerTokenIdentifier()).thenReturn(tokenId);
    when(container.getResource()).thenReturn(Resource.newInstance(1024, 2));
    cGroupsMemoryResourceHandler.preStart(container);

    if (cGroupVersion.equals(V1)) {
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SOFT_LIMIT_BYTES.getName(), "0M");
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.SWAPPINESS.getName(), "100");
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.HARD_LIMIT_BYTES.getName(), "1024M");
    } else if (cGroupVersion.equals(V2)) {
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_HIGH.getName(), "0M");
      verify(mockCGroupsHandler, never())
          .updateCGroupParam(eq(CGroupsHandler.CGroupController.MEMORY), eq(id),
              eq(CGroupsHandler.MemoryParameters.SWAPPINESS.getName()), anyString());
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
              CGroupsHandler.MemoryParameters.MEMORY_MAX.getName(), "1024M");
    }
  }

  @Test
  public void testContainerUnderOom() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);

    cGroupsMemoryResourceHandler.bootstrap(conf);

    ContainerId containerId = mock(ContainerId.class);
    when(containerId.toString()).thenReturn("container_01_01");

    if (cGroupVersion.equals(V1)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.OOM_CONTROL.getName())).thenReturn("under_oom 1");
    } else if (cGroupVersion.equals(V2)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.EVENTS_LOCAL.getName()))
          .thenReturn("low 0\nhigh 0\nmax 0\noom 0\noom_kill 1");
    }
    Optional<Boolean> outOfOom =
        cGroupsMemoryResourceHandler.isUnderOOM(containerId);
    Assert.assertTrue("The container should be reported to run under oom",
        outOfOom.isPresent() && outOfOom.get().equals(true));

    if (cGroupVersion.equals(V1)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.OOM_CONTROL.getName())).thenReturn("");
    } else if (cGroupVersion.equals(V2)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.EVENTS_LOCAL.getName()))
          .thenReturn("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0");
    }
    outOfOom = cGroupsMemoryResourceHandler.isUnderOOM(containerId);
    Assert.assertTrue(
        "The container should not be reported to run under oom",
        outOfOom.isPresent() && outOfOom.get().equals(false));

    if (cGroupVersion.equals(V1)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.OOM_CONTROL.getName())).
          thenThrow(new ResourceHandlerException());
    } else if (cGroupVersion.equals(V2)) {
      when(mockCGroupsHandler.getCGroupParam(
          CGroupsHandler.CGroupController.MEMORY,
          containerId.toString(),
          CGroupsHandler.MemoryParameters.EVENTS_LOCAL.getName())).
          thenThrow(new ResourceHandlerException());
    }
    outOfOom = cGroupsMemoryResourceHandler.isUnderOOM(containerId);
    Assert.assertFalse(
        "No report of the oom status should be available.",
        outOfOom.isPresent());

  }
}
