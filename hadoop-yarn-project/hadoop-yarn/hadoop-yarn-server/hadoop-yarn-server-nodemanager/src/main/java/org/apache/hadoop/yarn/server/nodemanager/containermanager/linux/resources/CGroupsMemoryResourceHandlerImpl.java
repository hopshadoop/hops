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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Handler class to handle the memory controller. YARN already ships a
 * physical memory monitor in Java but it isn't as
 * good as CGroups. This handler sets the soft and hard memory limits. The soft
 * limit is set to 90% of the hard limit.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsMemoryResourceHandlerImpl extends BaseCGroupsMemoryResourceHandler implements MemoryResourceHandler {

  static final String UNDER_OOM = "under_oom 1";

  CGroupsMemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  void updateHardLimit(String cgroupId, String limit) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.MemoryParameters.HARD_LIMIT_BYTES.getName(), limit);
  }

  @Override
  void updateSoftLimit(String cgroupId, String limit) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.MemoryParameters.SOFT_LIMIT_BYTES.getName(), limit);
  }

  @Override
  void updateSwappiness(String cgroupId, String value) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.MemoryParameters.SWAPPINESS.getName(), value);
  }

  @Override
  String getOOMStatus(String cgroupId) throws ResourceHandlerException {
    return cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cgroupId, CGroupsHandler.MemoryParameters.OOM_CONTROL.getName());
  }

  @Override
  boolean parseOOMStatus(String status) {
    return status.contains(UNDER_OOM);
  }
}
