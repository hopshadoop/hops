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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.File;
import java.io.IOException;

/**
 * An implementation for using CGroups to restrict CPU usage on Linux. The
 * implementation supports 3 different controls - restrict usage of all YARN
 * containers, restrict relative usage of individual YARN containers and
 * restrict usage of individual YARN containers. Admins can set the overall CPU
 * to be used by all YARN containers - this is implemented by setting
 * cpu.cfs_period_us and cpu.cfs_quota_us to the ratio desired. If strict
 * resource usage mode is not enabled, cpu.shares is set for individual
 * containers - this prevents containers from exceeding the overall limit for
 * YARN containers but individual containers can use as much of the CPU as
 * available(under the YARN limit). If strict resource usage is enabled, then
 * container can only use the percentage of CPU allocated to them and this is
 * again implemented using cpu.cfs_period_us and cpu.cfs_quota_us.
 *
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsCpuResourceHandlerImpl extends BaseCGroupsCpuResourceHandler implements CpuResourceHandler {

  @VisibleForTesting
  static final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 2;
  static final String UNLIMITED_QUOTA = "-1";

  CGroupsCpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  void updateCpuBandwidthParams(String groupId, String cpuQuotaUs, String cpuPeriodUs) throws ResourceHandlerException {
    cGroupsHandler
        .updateCGroupParam(CPU, groupId, CGroupsHandler.CpuParameters.QUOTA_US.getName(), cpuQuotaUs);
    if (!cpuPeriodUs.isEmpty()) {
      cGroupsHandler
          .updateCGroupParam(CPU, groupId, CGroupsHandler.CpuParameters.PERIOD_US.getName(), cpuPeriodUs);
    }
  }

  @Override
  String getCpuUnlimitedQuota() {
    return UNLIMITED_QUOTA;
  }
  @Override
  boolean doCpuLimitsExist(String path) throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CpuParameters.QUOTA_US.getName());
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, "UTF-8");
      int quotaUS = Integer.parseInt(contents.trim());
      if (quotaUS != -1) {
        return true;
      }
    }
    return false;
  }

  @InterfaceAudience.Private
  public static boolean cpuLimitsExist(String path)
      throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CpuParameters.QUOTA_US.getName());
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, "UTF-8");
      int quotaUS = Integer.parseInt(contents.trim());
      if (quotaUS != -1) {
        return true;
      }
    }
    return false;
  }

  @Override
  void updateCpuWeightParam(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler
        .updateCGroupParam(CPU, cgroupId,
            CGroupsHandler.CpuParameters.SHARES.getName(),
            String.valueOf(weight));
  }

  @Override
  int getDefaultOpportunisticWeight() {
    return CPU_DEFAULT_WEIGHT_OPPORTUNISTIC;
  }

  @Override
  int getDefaultWeight() {
    return CPU_DEFAULT_WEIGHT;
  }
}
