package io.hops.devices;



import com.google.common.annotations.VisibleForTesting;
import io.hops.GPUManagementLibrary;
import io.hops.GPUManagementLibraryLoader;
import io.hops.exceptions.GPUManagementLibraryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPUAllocator {
  
  final static Log LOG = LogFactory.getLog(GPUAllocator.class);
  private static final GPUAllocator gpuAllocator = new GPUAllocator();
  
  private HashSet<Device> configuredAvailableGPUs;
  private HashSet<Device> totalGPUs;
  private HashMap<String, HashSet<Device>> containerGPUAllocationMapping;
  private HashSet<Device> mandatoryDrivers;
  private GPUManagementLibrary gpuManagementLibrary;
  private static final String GPU_MANAGEMENT_LIBRARY_CLASSNAME = "io.hops" +
      ".management.nvidia.NvidiaManagementLibrary";
  private static final int NVIDIA_GPU_MAJOR_DEVICE_NUMBER = 195;
  private boolean initialized = false;
  
  public static GPUAllocator getInstance(){
    return gpuAllocator;
  }
  
  private GPUAllocator() {
    configuredAvailableGPUs = new HashSet<>();
    totalGPUs = new HashSet<>();
    containerGPUAllocationMapping = new HashMap<>();
    mandatoryDrivers = new HashSet<>();
    
    try {
      gpuManagementLibrary =
          GPUManagementLibraryLoader.load(GPU_MANAGEMENT_LIBRARY_CLASSNAME);
    } catch(GPUManagementLibraryException | UnsatisfiedLinkError e) {
      LOG.warn("Could not load GPU management library. Is this NodeManager " +
          "supposed to offer its GPUs as a resource? If yes, check " +
          "installation and make sure hopsnvml-1.0 is present in java.library.path");
      
      e.printStackTrace();
    }
  }
  
  @VisibleForTesting
  public GPUAllocator(GPUManagementLibrary gpuManagementLibrary, Configuration conf) {
    configuredAvailableGPUs = new HashSet<>();
    totalGPUs = new HashSet<>();
    containerGPUAllocationMapping = new HashMap<>();
    mandatoryDrivers = new HashSet<>();
    
    this.gpuManagementLibrary = gpuManagementLibrary;
    initialize(conf);
  }
  
  /**
   * Initialize the NVML library to check that it was setup correctly
   * @return boolean for success or not
   */
  public boolean initialize(Configuration conf) {
    if(initialized == false) {
      initialized = gpuManagementLibrary.initialize();
      int numGPUs = NodeManagerHardwareUtils.getNodeGPUs(conf);
      try {
        initMandatoryDrivers();
        initConfiguredGPUs(numGPUs);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
      initTotalGPUs();
    }
    return this.initialized;
  }
  
  public boolean isInitialized() {
    return this.initialized;
  }
  
  /**
   * Shut down NVML by releasing all GPU resources previously allocated with nvmlInit()
   * @return boolean for success or not
   */
  public boolean shutDown() {
    if(initialized) {
      return gpuManagementLibrary.shutDown();
    }
    return false;
  }
  
  /**
   * Queries NVML to discover device numbers for mandatory driver devices that
   * all GPU containers should have access to
   *
   * Mandatory devices for NVIDIA GPUs are:
   * /dev/nvidiactl
   * /dev/nvidia-uvm
   * /dev/nvidia-uvm-tools
   */
  private void initMandatoryDrivers() throws IOException {
    String mandatoryDeviceIds = gpuManagementLibrary
            .queryMandatoryDevices();
    if (!mandatoryDeviceIds.equals("")) {
      String[] mandatoryDeviceIdsArr = mandatoryDeviceIds.split(" ");
      for (int i = 0; i < mandatoryDeviceIdsArr.length; i++) {
        String[] majorMinorPair = mandatoryDeviceIdsArr[i].split(":");
        try {
          Device mandatoryDevice = new Device(
                  Integer.parseInt(majorMinorPair[0]),
                  Integer.parseInt(majorMinorPair[1]));
          mandatoryDrivers.add(mandatoryDevice);
          LOG.info("Found mandatory GPU driver " + mandatoryDevice.toString());
        } catch (NumberFormatException e) {
          LOG.error("Unexpected format for major:minor device numbers: " + majorMinorPair[0] + ":" + majorMinorPair[1]);
        }
      }
    } else {
      throw new IOException("Could not discover device numbers for GPU drivers, " +
              "check driver installation and make sure libnvidia-ml.so.1 is present on " +
              "LD_LIBRARY_PATH, or disable GPUs in the configuration");
    }
  }
  
  /**
   * Queries NVML to discover device numbers for available NVIDIA GPUs that
   * may be scheduled and isolated for containers
   */
  private void initConfiguredGPUs(int configuredGPUs) throws IOException {
    String configuredGPUDeviceIds = gpuManagementLibrary
            .queryAvailableDevices(configuredGPUs);
    if (!configuredGPUDeviceIds.equals("")) {
      String[] configuredGPUDeviceIdsArr = configuredGPUDeviceIds.split(" ");
      for (int i = 0; i < configuredGPUDeviceIdsArr.length; i++) {
        String[] majorMinorPair = configuredGPUDeviceIdsArr[i].split(":");
        try {
          Device gpu = new Device(
                  Integer.parseInt(majorMinorPair[0]),
                  Integer.parseInt(majorMinorPair[1]));
          configuredAvailableGPUs.add(gpu);
          LOG.info("Found available GPU device " + gpu.toString() + " for scheduling");
        } catch (NumberFormatException e) {
          LOG.error("Unexpected format for major:minor device numbers: " + majorMinorPair[0] + ":" + majorMinorPair[1]);
        }
      }
    } else {
      throw new IOException("Could not discover GPU device numbers, either you enabled GPUs but set NM_GPUS to 0," +
              " or there is a problem with the installation, check that libnvidia-ml.so.1 is present on LD_LIBRARY_PATH");
    }
  }

  //If 3 GPUs exist, the numbering is 195:0 195:1 195:2
  private void initTotalGPUs() {
    int totalNumGPUs= gpuManagementLibrary.getNumGPUs();
    while(totalNumGPUs != 0) {
      totalGPUs.add(new Device(NVIDIA_GPU_MAJOR_DEVICE_NUMBER, totalNumGPUs-1));
      totalNumGPUs--;
    }
    LOG.info("Total GPUs present on machine " + totalGPUs);
  }
  
  /**
   * Returns the mandatory devices that all containers making use of Nvidia
   * gpus should be given access to
   *
   * @return HashSet containing mandatory devices for containers making use of
   * gpus
   */
  public HashSet<Device> getMandatoryDrivers() {
    return mandatoryDrivers;
  }
  
  public HashSet<Device> getConfiguredAvailableGPUs() { return configuredAvailableGPUs; }

  public HashSet<Device> getTotalGPUs() { return totalGPUs; }
  
  /**
   * Finds out which gpus are currently allocated to a container
   *
   * @return HashSet containing GPUs currently allocated to containers
   */
  private HashSet<Device> getAllocatedGPUs() {
    HashSet<Device> allocatedGPUs = new HashSet<>();
    Collection<HashSet<Device>> gpuSets = containerGPUAllocationMapping.values();
    Iterator<HashSet<Device>> itr = gpuSets.iterator();
    while(itr.hasNext()) {
      HashSet<Device> allocatedGpuSet = itr.next();
      allocatedGPUs.addAll(allocatedGpuSet);
    }
    return allocatedGPUs;
  }
  
  /**
   * The allocation method will allocate the requested number of gpus.
   * In order to isolate a set of gpus, it is crucial to only allow device
   * access to those particular gpus and deny access to the remaining
   * devices, whether they are currently allocated to some container or not
   * and whether they are used by YARN or not
   *
   * @param containerName the container to allocate gpus for
   * @param gpus the number of gpus to allocate for container
   * @return HashSet containing all GPUs to deny access to, if allocating one
   * GPU then all except the allocated GPU will be returned
   * @throws IOException
   */
  public synchronized HashSet<Device> allocate(String
      containerName, int gpus)
      throws IOException {
    HashSet<Device> gpusToDeny = new HashSet<>(getTotalGPUs());

    if(configuredAvailableGPUs.size() >= gpus) {
      LOG.info("Trying to allocate " + gpus + " GPUs");

      if(gpus > 0) {

        LOG.info("Currently unallocated GPUs: " + configuredAvailableGPUs.toString());
        HashSet<Device> currentlyAllocatedGPUs = getAllocatedGPUs();
        LOG.info("Currently allocated GPUs: " + currentlyAllocatedGPUs);

        //selection method for determining which available GPUs to allocate
        HashSet<Device> gpuAllocation = selectGPUsToAllocate(gpus);

        //remove allocated GPUs from available
        configuredAvailableGPUs.removeAll(gpuAllocation);

        LOG.info("GPUs to allocate for " + containerName + " = " +
                gpuAllocation);

        //save the allocated GPUs
        containerGPUAllocationMapping.put(containerName, gpuAllocation);

        //deny remaining available gpus
        gpusToDeny.removeAll(gpuAllocation);
      }
      
      LOG.info("GPUs to deny for " + containerName + " = " +
              gpusToDeny);
      return gpusToDeny;
      
    } else {
      throw new IOException("Container " + containerName + " requested " +
          gpus + " GPUs when only " + configuredAvailableGPUs.size() + " available");
    }
  }
  
  /**
   * This method selects GPUs to allocate based on a "Fastest GPU first" policy
   * Device compute capability of NVIDIA GPUs are sorted based on the
   * minor device number. - "The lower the number the faster the device.
   *
   * TODO: In the future this method could support different selection policies
   * TODO: such as "Random GPU", "Slowest GPU first" or something
   * TODO: actually useful like topology-based selection with GPUs
   * TODO: interconnected using NVLink.
   * @param gpus number of GPUs to select for allocation
   * @return set of GPU devices that have been allocated
   */
  private synchronized HashSet<Device> selectGPUsToAllocate(int gpus) {
    
    HashSet<Device> gpuAllocation = new HashSet<>();
    Iterator<Device> availableGPUItr = configuredAvailableGPUs.iterator();
    
    TreeSet<Integer> minDeviceNums = new TreeSet<>();
    
    while(availableGPUItr.hasNext()) {
      minDeviceNums.add(availableGPUItr.next().getMinorDeviceNumber());
    }
    
    Iterator<Integer> minGPUDeviceNumItr = minDeviceNums.iterator();
    
    while(minGPUDeviceNumItr.hasNext() && gpus != 0) {
      int gpuMinorNum = minGPUDeviceNumItr.next();
      Device allocatedGPU = new Device(NVIDIA_GPU_MAJOR_DEVICE_NUMBER, gpuMinorNum);
      gpuAllocation.add(allocatedGPU);
      gpus--;
    }
    
    return gpuAllocation;
  }
  
  /**
   * Releases gpus for the given container and allows them to be subsequently
   * allocated by other containers
   *
   * @param containerName
   */
  public synchronized void release(String containerName) {
    if(containerGPUAllocationMapping != null && containerGPUAllocationMapping.containsKey(containerName)) {
      HashSet<Device> gpuAllocation = containerGPUAllocationMapping.
              get(containerName);
      containerGPUAllocationMapping.remove(containerName);
      configuredAvailableGPUs.addAll(gpuAllocation);
      LOG.info("Releasing GPUs " + gpuAllocation + " for container " + containerName);
    }
  }
  
  /**
   * Given the containerId and the Cgroup contents for devices.list
   * extract allocated GPU devices
   * @param devicesWhitelistStr
   */
  public synchronized void recoverAllocation(String containerId, String
      devicesWhitelistStr) {
    HashSet<Device> allocatedGPUsForContainer = findGPUsInWhitelist(devicesWhitelistStr);
    if(allocatedGPUsForContainer.isEmpty()) {
      return;
    }
    configuredAvailableGPUs.removeAll(allocatedGPUsForContainer);
    containerGPUAllocationMapping.put(containerId, allocatedGPUsForContainer);
    LOG.info("Recovering GPUs " + allocatedGPUsForContainer + " for" +
        " container " + containerId);
    LOG.info("Available GPUs after container " + containerId +
        " recovery " + configuredAvailableGPUs);
    LOG.info("So far " + containerGPUAllocationMapping.size() + " recovered containers");
  }
  
  /* We are looking for entries of the form:
   * c 195:0 rwm
   *
   * Use a simple pattern that splits on the two spaces, and
   * grabs the 2nd field
   */
  private static final Pattern DEVICES_LIST_FORMAT = Pattern.compile(
      "([^\\s]+)+\\s([\\d+:\\d]+)+\\s([^\\s]+)");
  
  /**
   * Find GPU devices in the contents of the devices.list file
   * This method is used in the recovery process and will only filter out the
   * NVIDIA GPUs (major device number 195), some NVIDIA device files have 195
   * major device number so we need to make sure these are not included as GPUs
   * @param devicesWhitelistStr
   * @return
   */
  private HashSet<Device> findGPUsInWhitelist(String devicesWhitelistStr) {
    HashSet<Device> recoveredGPUs = new HashSet<>();
    
    Matcher m = DEVICES_LIST_FORMAT.matcher(devicesWhitelistStr);
    
    while (m.find()) {
      String majorMinorDeviceNumber = m.group(2);
      String[] majorMinorPair = majorMinorDeviceNumber.split(":");
      int majorDeviceNumber = Integer.parseInt(majorMinorPair[0]);
      if(majorDeviceNumber == NVIDIA_GPU_MAJOR_DEVICE_NUMBER) {
        int minorDeviceNumber = Integer.parseInt(majorMinorPair[1]);
        Device device = new Device(majorDeviceNumber, minorDeviceNumber);
        //If not actually a GPU (but same major device number), do not return it
        if(!getMandatoryDrivers().contains(device)) {
          recoveredGPUs.add(new Device(majorDeviceNumber, minorDeviceNumber));
        }
      }
    }
    return recoveredGPUs;
  }
}
