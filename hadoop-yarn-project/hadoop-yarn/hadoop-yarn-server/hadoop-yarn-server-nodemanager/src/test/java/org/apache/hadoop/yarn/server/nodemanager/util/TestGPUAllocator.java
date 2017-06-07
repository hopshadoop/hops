package org.apache.hadoop.yarn.server.nodemanager.util;

import com.google.common.collect.Sets;
import io.hops.GPUManagementLibrary;
import io.hops.devices.Device;
import io.hops.devices.GPUAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class TestGPUAllocator {

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

    private static class CustomGPUAllocator extends GPUAllocator {

        public CustomGPUAllocator(GPUManagementLibrary gpuManagementLibrary) {
            super(gpuManagementLibrary, new YarnConfiguration());
        }
    }


    @Test
    public void testGPUAllocation() throws IOException {

        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.NM_GPUS, 8);
        customGPUAllocator.initialize(conf);
        HashSet<Device> initialAvailableGPUs = customGPUAllocator.getConfiguredAvailableGPUs();
        HashSet<Device> totalGPUs = customGPUAllocator.getTotalGPUs();
        Assert.assertTrue(totalGPUs.containsAll(initialAvailableGPUs));
        Assert.assertTrue(initialAvailableGPUs.containsAll(totalGPUs));
        Assert.assertTrue(totalGPUs.size() == 8);
        Assert.assertTrue(initialAvailableGPUs.size() == 8);
        int numInitialAvailableGPUs = initialAvailableGPUs.size();

        ContainerId firstId = ContainerId.fromString("container_1_1_1_1");
        HashSet<Device> firstAllocation = customGPUAllocator.allocate(firstId.toString(), 4);
        HashSet<Device> currentlyAvailableDevices = new HashSet<>(customGPUAllocator.getConfiguredAvailableGPUs());
        Assert.assertEquals(numInitialAvailableGPUs - 4, currentlyAvailableDevices.size());

        Assert.assertTrue(currentlyAvailableDevices.equals(firstAllocation));

        ContainerId secondId = ContainerId.fromString("container_1_1_1_2");
        HashSet<Device> secondAllocation = customGPUAllocator.allocate(secondId.toString(), 4);

        Assert.assertEquals(4, firstAllocation.size());

        Assert.assertEquals(4, secondAllocation.size());
        Assert.assertEquals(numInitialAvailableGPUs - 8, customGPUAllocator.getConfiguredAvailableGPUs().size());

        customGPUAllocator.release(firstId.toString());
        Assert.assertEquals(numInitialAvailableGPUs - 4, customGPUAllocator.getConfiguredAvailableGPUs().size());

        customGPUAllocator.release(secondId.toString());

        Assert.assertEquals(numInitialAvailableGPUs, customGPUAllocator.getConfiguredAvailableGPUs().size());
    
        ContainerId thirdId = ContainerId.fromString("container_1_1_1_2");
        HashSet<Device> thirdAllocation = customGPUAllocator.allocate(thirdId
            .toString(), 2);
        Assert.assertEquals(6, thirdAllocation.size());

        Assert.assertEquals(6, customGPUAllocator.getConfiguredAvailableGPUs().size());
        Assert.assertTrue(customGPUAllocator.getTotalGPUs().size() == 8);
    }

    @Test
    public void testGPUAllocatorRecovery() throws IOException{
        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.NM_GPUS, 8);
        customGPUAllocator.initialize(conf);

        HashSet<Device> initialAvailableGPUs = new HashSet<>(customGPUAllocator.getConfiguredAvailableGPUs());
        int numInitialAvailableGPUs = initialAvailableGPUs.size();

        //First container was allocated 195:0 and 195:1
        Device device0 = new Device(195, 0);
        Device device1 = new Device(195, 1);
        ContainerId firstId = ContainerId.fromString("container_1_1_1_1");
        String firstDevicesAllow =
                "c " + device0.toString() + " rwm" +"\n" +
                "c " + device1.toString() + " rwm" + "\n" +
                "c 0:1 rwm\n" +
                "c 0:2 rwm\n";
        customGPUAllocator.recoverAllocation(firstId.toString(), firstDevicesAllow);

        HashSet<Device> availableGPUsAfterFirstRecovery = new HashSet<>(customGPUAllocator.getConfiguredAvailableGPUs());
        Assert.assertFalse(initialAvailableGPUs.equals(availableGPUsAfterFirstRecovery));
        Assert.assertEquals(numInitialAvailableGPUs - 2, availableGPUsAfterFirstRecovery.size());
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device0));
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device1));

        //First container was allocated 195:2 and 195:3
        Device device2 = new Device(195, 2);
        Device device3 = new Device(195, 3);
        ContainerId id = ContainerId.fromString("container_1_1_1_2");
        String secondDevicesAllow =
                "c " + device2.toString() + " rwm" +"\n" +
                "c " + device3.toString() + " rwm" + "\n" +
                "c 0:1 rwm\n" +
                "c 0:2 rwm\n";
        customGPUAllocator.recoverAllocation(id.toString(), secondDevicesAllow);

        HashSet<Device> availableGPUsAfterSecondRecovery = new HashSet<>(customGPUAllocator.getConfiguredAvailableGPUs());
        Assert.assertFalse(initialAvailableGPUs.equals(availableGPUsAfterSecondRecovery));
        Assert.assertEquals(numInitialAvailableGPUs - 4, availableGPUsAfterSecondRecovery.size());
        Assert.assertFalse(availableGPUsAfterSecondRecovery.contains(device2));
        Assert.assertFalse(availableGPUsAfterSecondRecovery.contains(device3));

        ContainerId newContainerId = ContainerId.fromString("container_1_1_1_3");
        HashSet<Device> allocation = customGPUAllocator.allocate(newContainerId.toString(), 4);
        Assert.assertEquals(4, allocation.size());
        HashSet<Device> alreadyAllocatedDevices = new HashSet<>();
        alreadyAllocatedDevices.add(device0);
        alreadyAllocatedDevices.add(device1);
        alreadyAllocatedDevices.add(device2);
        alreadyAllocatedDevices.add(device3);
        Assert.assertTrue(allocation.containsAll(alreadyAllocatedDevices));

        HashSet<Device> allowedDevices = new HashSet<>();
        Device device4 = new Device(195, 4);
        Device device5 = new Device(195, 5);
        Device device6 = new Device(195, 6);
        Device device7 = new Device(195, 7);

        allowedDevices.add(device4);
        allowedDevices.add(device5);
        allowedDevices.add(device6);
        allowedDevices.add(device7);

        Assert.assertTrue(customGPUAllocator.getConfiguredAvailableGPUs().isEmpty());
        Assert.assertTrue(customGPUAllocator.getTotalGPUs().size() == 8);
    }

    @Test(expected=IOException.class)
    public void testExceedingGPUResource() throws IOException {

        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.NM_GPUS, 8);
        customGPUAllocator.initialize(conf);

        ContainerId firstContainerId = ContainerId.fromString("container_1_1_1_1");
        HashSet<Device> firstAllocation = customGPUAllocator.allocate(firstContainerId.toString(), 4);

        //Should throw IOException
        ContainerId secondContainerId = ContainerId.fromString("container_1_1_1_2");
        HashSet<Device> secondAllocation = customGPUAllocator.allocate(secondContainerId.toString(), 5);
    }
    
    //Makes sure that if no GPU is requested, all existing GPUs still need to
    // be blocked
    @Test
    public void testZeroGPURequestedZeroGPUAllocated() throws IOException {
        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        YarnConfiguration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.NM_GPUS, 8);
        customGPUAllocator.initialize(conf);

        ContainerId firstContainerId = ContainerId.fromString("container_1_1_1_1");
        HashSet<Device> allocation = customGPUAllocator
            .allocate(firstContainerId.toString(), 0);

        Assert.assertTrue(allocation.containsAll
            (customGPUAllocator.getConfiguredAvailableGPUs()));
    }
}
