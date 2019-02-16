package io.hops.devices;



import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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
  
  private HashSet<GPU> configuredAvailableGPUs;
  private HashSet<GPU> totalGPUs;
  private HashMap<String, HashSet<GPU>> containerGPUAllocationMapping;
  private HashSet<Device> mandatoryDrivers;
  private GPUManagementLibrary gpuManagementLibrary;
  private static String GPU_MANAGEMENT_LIBRARY_CLASSNAME;
  private static int GPU_MAJOR_DEVICE_NUMBER;
  private boolean initialized = false;
  
  public static GPUAllocator getInstance(){
    return gpuAllocator;
  }
  
  private GPUAllocator() {
    configuredAvailableGPUs = new HashSet<>();
    totalGPUs = new HashSet<>();
    containerGPUAllocationMapping = new HashMap<>();
    mandatoryDrivers = new HashSet<>();
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

  @VisibleForTesting
  public HashMap<String, HashSet<GPU>> getAllocations() {
    return new HashMap<>(containerGPUAllocationMapping);
  }
  
  /**
   * Initialize the NVML library to check that it was setup correctly
   * @return boolean for success or not
   */
  public boolean initialize(Configuration conf) {
    if(!initialized) {
      GPU_MANAGEMENT_LIBRARY_CLASSNAME = conf.get(YarnConfiguration.NM_GPU_MANAGEMENT_IMPL,
        YarnConfiguration.DEFAULT_NM_GPU_MANAGEMENT_IMPL);
      LOG.info("Initializing GPUAllocator for " + GPU_MANAGEMENT_LIBRARY_CLASSNAME);
      try {
        if(gpuManagementLibrary == null) {
          gpuManagementLibrary = GPUManagementLibraryLoader.load(GPU_MANAGEMENT_LIBRARY_CLASSNAME);
        }
      } catch(GPUManagementLibraryException | UnsatisfiedLinkError | NoClassDefFoundError e) {
        LOG.error("Could not load GPU management library using provider " + GPU_MANAGEMENT_LIBRARY_CLASSNAME, e);
      }
      initialized = gpuManagementLibrary.initialize();

      if(GPU_MANAGEMENT_LIBRARY_CLASSNAME.equals
        (YarnConfiguration.DEFAULT_NM_GPU_MANAGEMENT_IMPL)) {
        GPU_MAJOR_DEVICE_NUMBER = 195;
      } else if(GPU_MANAGEMENT_LIBRARY_CLASSNAME.equals("io.hops" +
        ".management.amd.AMDManagementLibrary")) {
        GPU_MAJOR_DEVICE_NUMBER = 226;
      } else {
        throw new IllegalArgumentException(GPU_MANAGEMENT_LIBRARY_CLASSNAME + " not recognized by GPUAllocator");
      }
      int numGPUs = NodeManagerHardwareUtils.getNodeGPUs(conf);
      try {
        initMandatoryDrivers();
        LOG.info("Found mandatory drivers: " + getMandatoryDrivers().toString());
        initConfiguredGPUs(numGPUs);
        initTotalGPUs();
      } catch (IOException ioe) {
        LOG.error("Could not initialize GPUAllocator", ioe);
      }
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
    LOG.info("GPU Management Library drivers: " + mandatoryDeviceIds);
    if (!Strings.isNullOrEmpty(mandatoryDeviceIds)) {
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
      throw new IOException("Failed to discover device numbers for GPU drivers using provider: " + GPU_MANAGEMENT_LIBRARY_CLASSNAME);
    }
  }
  
  /**
   * Queries NVML to discover device numbers for available NVIDIA GPUs that
   * may be scheduled and isolated for containers
   */
  private void initConfiguredGPUs(int configuredGPUs) throws IOException {
    LOG.info("Querying GPU Management Library for " + configuredGPUs + " GPUs");
    String configuredGPUDeviceIds = gpuManagementLibrary
            .queryAvailableDevices(configuredGPUs);
    LOG.info("GPU Management Library response: " + configuredGPUDeviceIds);
    if (!configuredGPUDeviceIds.equals("")) {
      String[] gpuEntries = configuredGPUDeviceIds.split(" ");
      for (int i = 0; i < gpuEntries.length; i++) {
        String gpuEntry =  gpuEntries[i];
        if(GPU_MANAGEMENT_LIBRARY_CLASSNAME.equals
          (YarnConfiguration.DEFAULT_NM_GPU_MANAGEMENT_IMPL)) {
          String[] majorMinorPair = gpuEntry.split(":");
          GPU nvidiaGPU = new GPU(new Device(
            Integer.parseInt(majorMinorPair[0]),
            Integer.parseInt(majorMinorPair[1])), null);
          configuredAvailableGPUs.add(nvidiaGPU);
          LOG.info("Found available GPU device " + nvidiaGPU.toString() + " for scheduling");
        } else {
          String [] gpuRenderNodePair = gpuEntry.split("&");
          String gpuMajorMinor = gpuRenderNodePair[0];
          String[] gpuMajorMinorPair = gpuMajorMinor.split(":");
          String renderNode = gpuRenderNodePair[1];
          String[] renderNodeMajorMinorPair = renderNode.split(":");
          GPU amdGPU = new GPU(
            new Device(
              Integer.parseInt(gpuMajorMinorPair[0]),
              Integer.parseInt(gpuMajorMinorPair[1])),
            new Device(
              Integer.parseInt(renderNodeMajorMinorPair[0]),
              Integer.parseInt(renderNodeMajorMinorPair[1])));
          configuredAvailableGPUs.add(amdGPU);
          LOG.info("Found available GPU device " + amdGPU.toString() + " for scheduling");

        }
      }
    } else {
      throw new IOException("Could not discover GPU device numbers using provider " + GPU_MANAGEMENT_LIBRARY_CLASSNAME);
    }
  }

  private void initTotalGPUs() {
    totalGPUs = new HashSet<>(configuredAvailableGPUs);
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
  
  public HashSet<GPU> getConfiguredAvailableGPUs() { return configuredAvailableGPUs; }

  public HashSet<GPU> getTotalGPUs() { return totalGPUs; }
  
  /**
   * Finds out which gpus are currently allocated to a container
   *
   * @return HashSet containing GPUs currently allocated to containers
   */
  private HashSet<GPU> getAllocatedGPUs() {
    HashSet<GPU> allocatedGPUs = new HashSet<>();
    Collection<HashSet<GPU>> gpuSets = containerGPUAllocationMapping.values();
    Iterator<HashSet<GPU>> itr = gpuSets.iterator();
    while(itr.hasNext()) {
      HashSet<GPU> allocatedGpuSet = itr.next();
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
  public synchronized HashSet<GPU> allocate(String
      containerName, int gpus)
      throws IOException {
    HashSet<GPU> gpusToDeny = new HashSet<>(getTotalGPUs());

    if(configuredAvailableGPUs.size() >= gpus) {
      LOG.info("Trying to allocate " + gpus + " GPUs");

      if(gpus > 0) {

        LOG.info("Currently unallocated GPUs: " + configuredAvailableGPUs.toString());
        HashSet<GPU> currentlyAllocatedGPUs = getAllocatedGPUs();
        LOG.info("Currently allocated GPUs: " + currentlyAllocatedGPUs);

        //selection method for determining which available GPUs to allocate
        HashSet<GPU> gpuAllocation = selectGPUsToAllocate(gpus);

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
  private synchronized HashSet<GPU> selectGPUsToAllocate(int gpus) {
    
    HashSet<GPU> gpuAllocation = new HashSet<>();
    Iterator<GPU> availableGPUItr = configuredAvailableGPUs.iterator();
    
    TreeSet<Integer> minDeviceNums = new TreeSet<>();
    
    while(availableGPUItr.hasNext()) {
      minDeviceNums.add(availableGPUItr.next().getGpuDevice().getMinorDeviceNumber());
    }
    
    Iterator<Integer> minGPUDeviceNumItr = minDeviceNums.iterator();
    
    while(minGPUDeviceNumItr.hasNext() && gpus != 0) {
      int gpuMinorNum = minGPUDeviceNumItr.next();

      for(GPU gpu: configuredAvailableGPUs) {
        if(gpu.getGpuDevice().getMinorDeviceNumber() == gpuMinorNum) {
          gpuAllocation.add(gpu);
          gpus--;
        }
      }
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
      HashSet<GPU> gpuAllocation = containerGPUAllocationMapping.
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
    HashSet<GPU> allocatedGPUsForContainer = findGPUsInWhitelist(devicesWhitelistStr);
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
  private HashSet<GPU> findGPUsInWhitelist(String devicesWhitelistStr) {
    HashSet<GPU> recoveredGPUs = new HashSet<>();
    
    Matcher m = DEVICES_LIST_FORMAT.matcher(devicesWhitelistStr);
    
    while (m.find()) {
      String majorMinorDeviceNumber = m.group(2);
      String[] majorMinorPair = majorMinorDeviceNumber.split(":");
      int majorDeviceNumber = Integer.parseInt(majorMinorPair[0]);
      if(majorDeviceNumber == GPU_MAJOR_DEVICE_NUMBER) {
        int minorDeviceNumber = Integer.parseInt(majorMinorPair[1]);
          GPU gpu = recoverGPU(minorDeviceNumber);
          if(gpu != null && !getMandatoryDrivers().contains(gpu.getGpuDevice())) {
            recoveredGPUs.add(gpu);
          }
      }
    }
    return recoveredGPUs;
  }

  private GPU recoverGPU(int minDeviceNumber) {
    for(GPU gpu: configuredAvailableGPUs) {
      if(gpu.getGpuDevice().getMinorDeviceNumber() == minDeviceNumber) {
        return gpu;
      }
    }
    return null;
  }
}
