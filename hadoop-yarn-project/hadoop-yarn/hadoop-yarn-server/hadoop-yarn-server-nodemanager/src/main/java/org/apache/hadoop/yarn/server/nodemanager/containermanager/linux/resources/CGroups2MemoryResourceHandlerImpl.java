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

public class CGroups2MemoryResourceHandlerImpl extends BaseCGroupsMemoryResourceHandler implements MemoryResourceHandler {

  private static final String NO_OOM_KILL = "oom_kill 0";

  CGroups2MemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  void updateHardLimit(String cgroupId, String limit) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.MemoryParameters.MEMORY_MAX.getName(), limit);
  }

  @Override
  void updateSoftLimit(String cgroupId, String limit) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.MemoryParameters.MEMORY_HIGH.getName(), limit);
  }

  @Override
  void updateSwappiness(String cgroupId, String value) throws ResourceHandlerException {
    // cgroup2 does not have swappiness control
    // https://github.com/opencontainers/runtime-spec/issues/1005
  }

  @Override
  String getOOMStatus(String cgroupId) throws ResourceHandlerException {
    return cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cgroupId, CGroupsHandler.MemoryParameters.EVENTS_LOCAL.getName());
  }

  @Override
  boolean parseOOMStatus(String status) {
    return !status.contains(NO_OOM_KILL);
  }
}
