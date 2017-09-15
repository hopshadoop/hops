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
package org.apache.hadoop.yarn.server.nodemanager.util;

import io.hops.GPUManagementLibrary;
import io.hops.devices.Device;
import io.hops.devices.GPUAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class TestCgroupsLCEResourcesHandlerGPU {
  static File cgroupDir = null;
  
  static class MockClock implements Clock {
    long time;
    
    @Override
    public long getTime() {
      return time;
    }
  }
  
  @Before
  public void setUp() throws Exception {
    cgroupDir =
        new File(System.getProperty("test.build.data",
            System.getProperty("java.io.tmpdir", "target")), this.getClass()
            .getName());
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  @Test
  public void testcheckAndDeleteCgroup() throws Exception {
    CgroupsLCEResourcesHandlerGPU handler = new CgroupsLCEResourcesHandlerGPU();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();
    
    FileUtils.deleteQuietly(cgroupDir);
    // Test 0
    // tasks file not present, should return false
    Assert.assertFalse(handler.checkAndDeleteCgroup(cgroupDir));
    
    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    File fspy = Mockito.spy(cgroupDir);
    
    // Test 1, tasks file is empty
    // tasks file has no data, should return true
    Mockito.stub(fspy.delete()).toReturn(true);
    Assert.assertTrue(handler.checkAndDeleteCgroup(fspy));
    
    // Test 2, tasks file has data
    fos.write("1234".getBytes());
    fos.close();
    // tasks has data, would not be able to delete, should return false
    Assert.assertFalse(handler.checkAndDeleteCgroup(fspy));
    FileUtils.deleteQuietly(cgroupDir);
    
  }
  
  // Verify DeleteCgroup times out if "tasks" file contains data
  @Test
  public void testDeleteCgroup() throws Exception {
    final MockClock clock = new MockClock();
    clock.time = System.currentTimeMillis();
    CgroupsLCEResourcesHandlerGPU handler = new CgroupsLCEResourcesHandlerGPU();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();
    handler.clock = clock;
    
    FileUtils.deleteQuietly(cgroupDir);
    
    // Create a non-empty tasks file
    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    fos.write("1234".getBytes());
    fos.close();
    
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        latch.countDown();
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          //NOP
        }
        clock.time += YarnConfiguration.
            DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT;
      }
    }.start();
    latch.await();
    Assert.assertFalse(handler.deleteCgroup(cgroupDir.getAbsolutePath()));
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  static class MockLinuxContainerExecutor extends LinuxContainerExecutor {
    @Override
    public void mountCgroups(List<String> x, String y) {
    }
  }
  
  static class CustomCgroupsLCEResourceHandlerGPU extends
      CgroupsLCEResourcesHandlerGPU {
    
    String mtabFile;
    GPUAllocator gpuAllocator;
    int[] limits = new int[2];
    boolean generateLimitsMode = false;
    
    @Override
    int[] getOverallLimits(float x) {
      if (generateLimitsMode == true) {
        return super.getOverallLimits(x);
      }
      return limits;
    }
    
    void setMtabFile(String file) {
      mtabFile = file;
    }
    
    @Override
    String getMtabFileName() {
      return mtabFile;
    }
    
    void setGPUAllocator(GPUAllocator gpuAllocator) {
      this.gpuAllocator = gpuAllocator;
    }
    
    @Override
    GPUAllocator getGPUAllocator() {
      return gpuAllocator;
    }
  }
  
  public static File createMockCgroupMount(File parentDir, String type)
      throws IOException {
    return createMockCgroupMount(parentDir, type, "hadoop-yarn");
  }
  
  public static File createMockMTab(File parentDir) throws IOException {
    String cpuMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/cpu cgroup rw,relatime,cpu 0 0\n";
    String devicesMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/devices cgroup rw,relatime,devices 0 0\n";
    
    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(cpuMtabContent);
    mtabWriter.write(devicesMtabContent);
    mtabWriter.close();
    mockMtab.deleteOnExit();
    return mockMtab;
  }
  
  @Test
  public void testContainerGPUAllocation() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandlerGPU handler =
        new CustomCgroupsLCEResourceHandlerGPU();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_GPU_RESOURCE_ENABLED, true);
    final int numGPUs = 8;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();
    
    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");
    File whiteList = new File(cgroupMountDirGPU, "devices.list");
    FileOutputStream fos1 = FileUtils.openOutputStream(whiteList);
    fos1.write(("a *:* rwm\n").getBytes());
    
    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);
    
    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    GPUAllocator gpuAllocator = null;

    conf.setInt(YarnConfiguration.NM_GPUS, 8);

    
    gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), conf);
    
    Device gpu0 = new Device(195, 0);
    Device gpu1 = new Device(195, 1);
    Device gpu2 = new Device(195, 2);
    Device gpu3 = new Device(195, 3);
    Device gpu4 = new Device(195, 4);
    Device gpu5 = new Device(195, 5);
    Device gpu6 = new Device(195, 6);
    Device gpu7 = new Device(195, 7);
    
    
    // setup our handler and call init()
    handler.setGPUAllocator(gpuAllocator);
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);
    File containerDirGPU = new File(cgroupMountDirGPU, id.toString());
    File denyFile = new File(containerDirGPU, "devices.deny");
    Assert.assertFalse(denyFile.exists());
    //FIRST ALLOCATION
    handler.preExecute(id, Resource.newInstance(1024, 1, 2));
    Assert.assertTrue(containerDirGPU.exists());
    Assert.assertTrue(containerDirGPU.isDirectory());
    Assert.assertTrue(denyFile.exists());
    
    Assert.assertTrue(gpuAllocator.getConfiguredAvailableGPUs().size() == 6);
    //SECOND ALLOCATION
    HashSet<Device> deviceAllocation1 = gpuAllocator.allocate
        ("test_container", 2);
    
    HashSet<Device> deniedDevices1 = deviceAllocation1;
    //gpu0 and gpu1 already allocated
    Assert.assertTrue(deniedDevices1.contains(gpu0));
    Assert.assertTrue(deniedDevices1.contains(gpu1));
    Assert.assertFalse(deniedDevices1.contains(gpu2));
    Assert.assertFalse(deniedDevices1.contains(gpu3));
    Assert.assertTrue(deniedDevices1.contains(gpu4));
    Assert.assertTrue(deniedDevices1.contains(gpu5));
    Assert.assertTrue(deniedDevices1.contains(gpu6));
    Assert.assertTrue(deniedDevices1.contains(gpu7));
    
    handler.postExecute(id);
    
    HashSet<Device> deviceAllocation2 = gpuAllocator.allocate
        ("test_container2", 3);
    
    HashSet<Device> deniedDevices2 = deviceAllocation2;
    Assert.assertFalse(deniedDevices2.contains(gpu0)); //allocated in first call
    Assert.assertFalse(deniedDevices2.contains(gpu1));
    Assert.assertTrue(deniedDevices2.contains(gpu2));
    Assert.assertTrue(deniedDevices2.contains(gpu3));
    Assert.assertFalse(deniedDevices2.contains(gpu4));
    Assert.assertTrue(deniedDevices2.contains(gpu5));
    Assert.assertTrue(deniedDevices2.contains(gpu6));
    Assert.assertTrue(deniedDevices2.contains(gpu7));
    
    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testSubsetGPUsSchedulable() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandlerGPU handler =
            new CustomCgroupsLCEResourceHandlerGPU();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_GPU_RESOURCE_ENABLED, true);
    final int numGPUs = 6;
    ResourceCalculatorPlugin plugin =
            Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();

    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");
    File whiteList = new File(cgroupMountDirGPU, "devices.list");
    FileOutputStream fos1 = FileUtils.openOutputStream(whiteList);
    fos1.write(("a *:* rwm\n").getBytes());

    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);

    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    GPUAllocator gpuAllocator = null;

    conf.setInt(YarnConfiguration.NM_GPUS, 6);


    gpuAllocator = new GPUAllocator(new
            CustomGPUmanagementLibrarySubset(), conf);

    Device gpu0 = new Device(195, 0);
    Device gpu1 = new Device(195, 1);
    Device gpu2 = new Device(195, 2);
    Device gpu3 = new Device(195, 3);
    Device gpu4 = new Device(195, 4);
    Device gpu5 = new Device(195, 5);


    // setup our handler and call init()
    handler.setGPUAllocator(gpuAllocator);
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);
    File containerDirGPU = new File(cgroupMountDirGPU, id.toString());
    File denyFile = new File(containerDirGPU, "devices.deny");
    Assert.assertFalse(denyFile.exists());
    //FIRST ALLOCATION
    handler.preExecute(id, Resource.newInstance(1024, 1, 2));
    Assert.assertTrue(containerDirGPU.exists());
    Assert.assertTrue(containerDirGPU.isDirectory());
    Assert.assertTrue(denyFile.exists());

    Assert.assertTrue(gpuAllocator.getConfiguredAvailableGPUs().size() == 4);
    //SECOND ALLOCATION
    HashSet<Device> deviceAllocation1 = gpuAllocator.allocate
            ("test_container", 2);

    HashSet<Device> deniedDevices1 = deviceAllocation1;
    //gpu0 and gpu1 already allocated
    Assert.assertTrue(deniedDevices1.contains(gpu0));
    Assert.assertTrue(deniedDevices1.contains(gpu1));
    Assert.assertFalse(deniedDevices1.contains(gpu2));
    Assert.assertFalse(deniedDevices1.contains(gpu3));
    Assert.assertTrue(deniedDevices1.contains(gpu4));
    Assert.assertTrue(deniedDevices1.contains(gpu5));

    handler.postExecute(id);

    HashSet<Device> deviceAllocation2 = gpuAllocator.allocate
            ("test_container2", 2);

    HashSet<Device> deniedDevices2 = deviceAllocation2;
    Assert.assertFalse(deniedDevices2.contains(gpu0)); //allocated in first call
    Assert.assertFalse(deniedDevices2.contains(gpu1));
    Assert.assertTrue(deniedDevices2.contains(gpu2));
    Assert.assertTrue(deniedDevices2.contains(gpu3));
    Assert.assertTrue(deniedDevices2.contains(gpu4));
    Assert.assertTrue(deniedDevices2.contains(gpu5));

    FileUtils.deleteQuietly(cgroupDir);
  }
  
  private static class CustomGPUmanagementLibrary implements GPUManagementLibrary {
    
    @Override
    public boolean initialize() {
      return true;
    }
    
    @Override
    public boolean shutDown() {
      return true;
    }
    
    @Override
    public int getNumGPUs() {
      return 8;
    }
    
    @Override
    public String queryMandatoryDevices() {
      return "0:1 0:2 0:3";
    }
    
    @Override
    public String queryAvailableDevices(int configuredGPUs) {
      return "195:0 195:1 195:2 195:3 195:4 195:5 195:6 195:7";
    }
  }

  private static class CustomGPUmanagementLibrarySubset implements GPUManagementLibrary {

    @Override
    public boolean initialize() {
      return true;
    }

    @Override
    public boolean shutDown() {
      return true;
    }

    @Override
    public int getNumGPUs() {
      return 8;
    }

    @Override
    public String queryMandatoryDevices() {
      return "0:1 0:2 0:3";
    }

    @Override
    public String queryAvailableDevices(int configuredGPUs) {
      return "195:0 195:1 195:2 195:3 195:4 195:5";
    }
  }
  
  @Test
  public void testContainerRecovery() throws IOException {
    System.out.println("test");
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandlerGPU handler =
        new CustomCgroupsLCEResourceHandlerGPU();
    YarnConfiguration conf = new YarnConfiguration();
    final int numGPUs = 8;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();
    System.out.println("test2");
    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");
    File whiteList = new File(cgroupMountDirGPU, "devices.list");
    FileOutputStream outputStream = FileUtils.openOutputStream(whiteList);
    outputStream.write(("a *:* rwm\n").getBytes());
    System.out.println("test3");
    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);
    handler.setMtabFile(mockMtab.getAbsolutePath());

    conf.setInt(YarnConfiguration.NM_GPUS, 8);
    System.out.println("test4");
    GPUAllocator gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), conf);

    System.out.println("test5");
    
    handler.setGPUAllocator(gpuAllocator);
    handler.init(mockLCE, plugin);
    
    ContainerId id1 = ContainerId.fromString("container_1_1_1_1");
    handler.preExecute(id1, Resource.newInstance(1024, 1, 2));
    File containerDir1 = new File(cgroupMountDirGPU, id1.toString());
    File listFile1 = new File(containerDir1, "devices.list");
    FileOutputStream fos1 = FileUtils.openOutputStream(listFile1);
    fos1.write(("c 195:0 rwm\n" + "c 195:1 rwm\n").getBytes());

    File tasksFile1 = new File(containerDir1, "tasks");
    FileOutputStream tasks1 = FileUtils.openOutputStream(tasksFile1);
    tasks1.write(("12934").getBytes());
    
    ContainerId id2 = ContainerId.fromString("container_1_1_1_2");
    handler.preExecute(id2, Resource.newInstance(1024, 1, 2));
    File containerDir2 = new File(cgroupMountDirGPU, id2.toString());
    File listFile2 = new File(containerDir2, "devices.list");
    FileOutputStream fos2 = FileUtils.openOutputStream(listFile2);
    fos2.write(("c 195:2 rwm\n" + "c 195:3 rwm\n").getBytes());

    File tasksFile2 = new File(containerDir2, "tasks");
    FileOutputStream tasks2 = FileUtils.openOutputStream(tasksFile2);
    tasks2.write(("12934").getBytes());
    
    ContainerId id3 = ContainerId.fromString("container_1_1_1_3");
    handler.preExecute(id3, Resource.newInstance(1024, 1, 2));
    File containerDir3 = new File(cgroupMountDirGPU, id3.toString());
    File listFile3 = new File(containerDir3, "devices.list");
    FileOutputStream fos3 = FileUtils.openOutputStream(listFile3);
    fos3.write(("c 195:4 rwm\n" + "c 195:5 rwm\n").getBytes());

    File tasksFile3 = new File(containerDir3, "tasks");
    FileOutputStream tasks3 = FileUtils.openOutputStream(tasksFile3);
    tasks3.write(("12934").getBytes());

    conf.setInt(YarnConfiguration.NM_GPUS, 8);
    gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), conf);
    gpuAllocator.initialize(conf);
    handler =
        new CustomCgroupsLCEResourceHandlerGPU();
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_GPU_RESOURCE_ENABLED, true);
    plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();
    
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.setGPUAllocator(gpuAllocator);
    handler.init(mockLCE, plugin);
    
    //No recovery yet
    Assert.assertEquals(8, gpuAllocator.getConfiguredAvailableGPUs().size());
    Assert.assertTrue(listFile1.exists());
    handler.recoverDeviceControlSystem(id1);
    Assert.assertEquals(6, gpuAllocator.getConfiguredAvailableGPUs().size());
    Assert.assertTrue(listFile2.exists());
    handler.recoverDeviceControlSystem(id2);
    Assert.assertEquals(4, gpuAllocator.getConfiguredAvailableGPUs().size());
    Assert.assertTrue(listFile3.exists());
    handler.recoverDeviceControlSystem(id3);
    Assert.assertEquals(2, gpuAllocator.getConfiguredAvailableGPUs().size());
    HashSet<Device> availableGPUs = new HashSet<>(gpuAllocator
        .getConfiguredAvailableGPUs());
    Assert.assertTrue(availableGPUs.contains(new Device(195, 6)));
    Assert.assertTrue(availableGPUs.contains(new Device(195, 7)));
    
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  public static File createMockCgroupMount(File parentDir, String type,
      String hierarchy) throws IOException {
    File cgroupMountDir =
        new File(parentDir.getAbsolutePath(), type + "/" + hierarchy);
    FileUtils.deleteQuietly(cgroupMountDir);
    if (!cgroupMountDir.mkdirs()) {
      String message =
          "Could not create dir " + cgroupMountDir.getAbsolutePath();
      throw new IOException(message);
    }
    return cgroupMountDir;
  }
}
