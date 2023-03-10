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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseCGroupsCpuResourceHandler implements CpuResourceHandler {
  final Logger LOG = LoggerFactory.getLogger(getClass());
  private static final Logger staticLogger = LoggerFactory.getLogger(BaseCGroupsCpuResourceHandler.class);
  @VisibleForTesting
  static final int MAX_QUOTA_US = 1000 * 1000;
  @VisibleForTesting
  static final int MIN_PERIOD_US = 1000;
  static final CGroupsHandler.CGroupController CPU = CGroupsHandler.CGroupController.CPU;
  private static final String TOP_CGROUP = "";

  final CGroupsHandler cGroupsHandler;
  private boolean strictResourceUsageMode = false;
  private float yarnProcessors;
  private float nodeVCores;

  public BaseCGroupsCpuResourceHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    return bootstrap(
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf), conf);
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(CPU, cgroupId);
    updateContainer(container);
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
            .getPathForCGroupTasks(CPU, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @VisibleForTesting
  List<PrivilegedOperation> bootstrap(
      ResourceCalculatorPlugin plugin, Configuration conf)
      throws ResourceHandlerException {
    this.strictResourceUsageMode = conf.getBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    this.cGroupsHandler.initializeCGroupController(CPU);
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    // cap overall usage to the number of cores allocated to YARN
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    boolean existingCpuLimits;
    try {
      existingCpuLimits =
          doCpuLimitsExist(cGroupsHandler.getPathForCGroup(CPU, ""));
    } catch (IOException ie) {
      throw new ResourceHandlerException(ie);
    }
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      updateCpuBandwidthParams(TOP_CGROUP, String.valueOf(limits[1]), String.valueOf(limits[0]));
    } else if (existingCpuLimits) {
      LOG.info("Removing CPU constraints for YARN containers.");
      updateCpuBandwidthParams(TOP_CGROUP, getCpuUnlimitedQuota(), "");
    }
    return null;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public static int[] getOverallLimits(float yarnProcessors) {

    int[] ret = new int[2];

    if (yarnProcessors < 0.01f) {
      throw new IllegalArgumentException("Number of processors can't be <= 0.");
    }

    int quotaUS = MAX_QUOTA_US;
    int periodUS = (int) (MAX_QUOTA_US / yarnProcessors);
    if (yarnProcessors < 1.0f) {
      periodUS = MAX_QUOTA_US;
      quotaUS = (int) (periodUS * yarnProcessors);
      if (quotaUS < MIN_PERIOD_US) {
        staticLogger.warn("The quota calculated for the cgroup was too low."
            + " The minimum value is " + MIN_PERIOD_US
            + ", calculated value is " + quotaUS
            + ". Setting quota to minimum value.");
        quotaUS = MIN_PERIOD_US;
      }
    }

    // cfs_period_us can't be less than 1000 microseconds
    // if the value of periodUS is less than 1000, we can't really use cgroups
    // to limit cpu
    if (periodUS < MIN_PERIOD_US) {
      staticLogger.warn("The period calculated for the cgroup was too low."
          + " The minimum value is " + MIN_PERIOD_US
          + ", calculated value is " + periodUS
          + ". Using all available CPU.");
      periodUS = MAX_QUOTA_US;
      quotaUS = -1;
    }

    ret[0] = periodUS;
    ret[1] = quotaUS;
    return ret;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    Resource containerResource = container.getResource();
    String cgroupId = container.getContainerId().toString();
    File cgroup = new File(cGroupsHandler.getPathForCGroup(CPU, cgroupId));
    if (cgroup.exists()) {
      try {
        int containerVCores = containerResource.getVirtualCores();
        ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
        if (id != null && id.getExecutionType() ==
            ExecutionType.OPPORTUNISTIC) {
          updateCpuWeightParam(cgroupId, getDefaultOpportunisticWeight());
        } else {
          int cpuShares = getDefaultWeight() * containerVCores;
          updateCpuWeightParam(cgroupId, cpuShares);
        }
        if (strictResourceUsageMode) {
          if (nodeVCores != containerVCores) {
            float containerCPU =
                (containerVCores * yarnProcessors) / (float) nodeVCores;
            int[] limits = getOverallLimits(containerCPU);
            updateCpuBandwidthParams(cgroupId, String.valueOf(limits[1]), String.valueOf(limits[0]));
          }
        }
      } catch (ResourceHandlerException re) {
        cGroupsHandler.deleteCGroup(CPU, cgroupId);
        LOG.warn("Could not update cgroup for container", re);
        throw re;
      }
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CPU, containerId.toString());
    return null;
  }

  @Override public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  abstract String getCpuUnlimitedQuota();

  abstract boolean doCpuLimitsExist(String path) throws IOException;
  abstract void updateCpuBandwidthParams(String groupId, String cpuQuotaUs, String cpuPeriodUs)
      throws ResourceHandlerException;
  abstract void updateCpuWeightParam(String cgroupId, int weight) throws ResourceHandlerException;
  abstract int getDefaultOpportunisticWeight();
  abstract int getDefaultWeight();
}
