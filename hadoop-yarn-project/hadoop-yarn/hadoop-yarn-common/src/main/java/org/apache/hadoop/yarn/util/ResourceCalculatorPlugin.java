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

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;

/**
 * Plugin to calculate resource information on the system.
 *
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MAPREDUCE"})
@InterfaceStability.Unstable
public abstract class ResourceCalculatorPlugin extends Configured {

  /**
   * Obtain the total size of the virtual memory present in the system.
   *
   * @return virtual memory size in bytes.
   */
  public abstract long getVirtualMemorySize();

  /**
   * Obtain the total size of the physical memory present in the system.
   *
   * @return physical memory size bytes.
   */
  public abstract long getPhysicalMemorySize();

  /**
   * Obtain the total size of the available virtual memory present
   * in the system.
   *
   * @return available virtual memory size in bytes.
   */
  public abstract long getAvailableVirtualMemorySize();

  /**
   * Obtain the total size of the available physical memory present
   * in the system.
   *
   * @return available physical memory size bytes.
   */
  public abstract long getAvailablePhysicalMemorySize();

  /**
   * Obtain the total number of processors present on the system.
   *
   * @return number of processors
   */
  public abstract int getNumProcessors();

  /**
   * Obtain the CPU frequency of on the system.
   *
   * @return CPU frequency in kHz
   */
  public abstract long getCpuFrequency();

  /**
   * Obtain the cumulative CPU time since the system is on.
   *
   * @return cumulative CPU time in milliseconds
   */
  public abstract long getCumulativeCpuTime();

  /**
   * Obtain the CPU usage % of the machine. Return -1 if it is unavailable
   *
   * @return CPU usage in %
   */
  public abstract float getCpuUsage();
  
  /**
   * Obtain the total number of usable GPUs (in non-erroneous state)
   * @return number of GPUs
   */
  public abstract int getNumGPUs();

  /**
   * Create the ResourceCalculatorPlugin from the class name and configure it. If
   * class name is null, this method will try and return a memory calculator
   * plugin available for this system.
   *
   * @param clazz ResourceCalculator plugin class-name
   * @param conf configure the plugin with this.
   * @return ResourceCalculatorPlugin or null if ResourceCalculatorPlugin is not
   * 		 available for current system
   */
  public static ResourceCalculatorPlugin getResourceCalculatorPlugin(
      Class<? extends ResourceCalculatorPlugin> clazz, Configuration conf) {

    if (clazz != null) {
      return ReflectionUtils.newInstance(clazz, conf);
    }

    // No class given, try a os specific class
    try {
      if (Shell.LINUX) {
        return new LinuxResourceCalculatorPlugin();
      }
      if (Shell.WINDOWS) {
        return new WindowsResourceCalculatorPlugin();
      }
    } catch (SecurityException se) {
      // Failed to get Operating System name.
      return null;
    }

    // Not supported on this system.
    return null;
  }
}
