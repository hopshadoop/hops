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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class BaseCGroupsMemoryResourceHandler implements MemoryResourceHandler {
  final Logger LOG = LoggerFactory.getLogger(getClass());

  final CGroupsHandler cGroupsHandler;

  static final CGroupsHandler.CGroupController MEMORY =
      CGroupsHandler.CGroupController.MEMORY;

  private static final int OPPORTUNISTIC_SWAPPINESS = 100;
  private static final int OPPORTUNISTIC_SOFT_LIMIT = 0;
  private boolean enforce = true;
  private int swappiness = 0;
  private float softLimit = 0.0f;

  BaseCGroupsMemoryResourceHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    this.cGroupsHandler.initializeCGroupController(MEMORY);
    enforce = conf.getBoolean(
        YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENFORCED);
    swappiness = conf
        .getInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS,
            YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS);
    if (swappiness < 0 || swappiness > 100) {
      throw new ResourceHandlerException(
          "Illegal value '" + swappiness + "' for "
              + YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS
              + ". Value must be between 0 and 100.");
    }
    float softLimitPerc = conf.getFloat(
        YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE,
        YarnConfiguration.
            DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE);
    softLimit = softLimitPerc / 100.0f;
    if (softLimitPerc < 0.0f || softLimitPerc > 100.0f) {
      throw new ResourceHandlerException(
          "Illegal value '" + softLimitPerc + "' "
              + YarnConfiguration.
              NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE
              + ". Value must be between 0 and 100.");
    }
    return null;
  }


  @VisibleForTesting
  int getSwappiness() {
    return swappiness;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }
  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(MEMORY, cgroupId);
    updateContainer(container);
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
            + cGroupsHandler.getPathForCGroupTasks(MEMORY, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    File cgroup = new File(cGroupsHandler.getPathForCGroup(MEMORY, cgroupId));
    if (cgroup.exists()) {
      //memory is in MB
      long containerSoftLimit =
          (long) (container.getResource().getMemorySize() * this.softLimit);
      long containerHardLimit = container.getResource().getMemorySize();
      if (enforce) {
        try {
          updateHardLimit(cgroupId, String.valueOf(containerHardLimit) + "M");
          ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
          if (id != null && id.getExecutionType() ==
              ExecutionType.OPPORTUNISTIC) {
            updateSoftLimit(cgroupId, String.valueOf(OPPORTUNISTIC_SOFT_LIMIT) + "M");
            updateSwappiness(cgroupId, String.valueOf(OPPORTUNISTIC_SWAPPINESS));
          } else {
            updateSoftLimit(cgroupId, String.valueOf(containerSoftLimit) + "M");
            updateSwappiness(cgroupId, String.valueOf(swappiness));
          }
        } catch (ResourceHandlerException re) {
          cGroupsHandler.deleteCGroup(MEMORY, cgroupId);
          LOG.warn("Could not update cgroup for container", re);
          throw re;
        }
      }
    }
    return null;
  }

  @Override
  public Optional<Boolean> isUnderOOM(ContainerId containerId) {
    try {
      String status = getOOMStatus(containerId.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("cgroups OOM status for " + containerId + ": " + status);
      }
      if (parseOOMStatus(status)) {
        LOG.warn("Container " + containerId + " under OOM based on cgroups.");
        return Optional.of(true);
      } else {
        return Optional.of(false);
      }
    } catch (ResourceHandlerException e) {
      LOG.warn("Could not read cgroups" + containerId, e);
    }
    return Optional.empty();
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(MEMORY, containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  abstract void updateHardLimit(String cgroupId, String limit) throws ResourceHandlerException;
  abstract void updateSoftLimit(String cgroupId, String limit) throws ResourceHandlerException;
  abstract void updateSwappiness(String cgroupId, String value) throws ResourceHandlerException;
  abstract String getOOMStatus(String cgroupId) throws ResourceHandlerException;
  abstract boolean parseOOMStatus(String status);
}
