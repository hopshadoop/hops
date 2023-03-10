/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides CGroups functionality. Implementations are expected to be
 * thread-safe
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CGroupsHandler {

  enum CGroupVersion {
    V1,
    V2,
    V1V2
  }

  /**
   * List of supported cgroup subsystem types.
   */
  enum CGroupController {
    CPU("cpu", CGroupVersion.V1V2),
    NET_CLS("net_cls", CGroupVersion.V1),
    BLKIO("blkio", CGroupVersion.V1),
    MEMORY("memory", CGroupVersion.V1V2),
    CPUACCT("cpuacct", CGroupVersion.V1),
    CPUSET("cpuset", CGroupVersion.V1V2),
    FREEZER("freezer", CGroupVersion.V1),
    DEVICES("devices", CGroupVersion.V1);

    private final String name;
    private final CGroupVersion cGroupVersion;

    CGroupController(String name, CGroupVersion cGroupVersion) {
      this.name = name;
      this.cGroupVersion = cGroupVersion;
    }

    public String getName() {
      return name;
    }

    public CGroupVersion getcGroupVersion() {
      return cGroupVersion;
    }

    public static final Function<CGroupController, Boolean> NO_CGROUP_FILTER = (t) -> true;
    public static final Function<CGroupController, Boolean> V1_CGROUP_FILTER =
        (t) -> t.getcGroupVersion().equals(CGroupVersion.V1) || t.getcGroupVersion().equals(CGroupVersion.V1V2);

    /**
     * Get the list of valid cgroup names.
     * @return The set of cgroup name strings
     */
    public static Set<String> getValidCGroups(Function<CGroupController, Boolean> filter) {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        if (filter.apply(controller)) {
          validCgroups.add(controller.getName());
        }
      }
      return validCgroups;
    }
  }

  String CGROUP_FILE_TASKS = "tasks";
  String CGROUP_PARAM_CLASSID = "classid";
  String CGROUP_PARAM_BLKIO_WEIGHT = "weight";

  String CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES = "memsw.limit_in_bytes";
  String CGROUP_PARAM_MEMORY_USAGE_BYTES = "usage_in_bytes";
  String CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES = "memsw.usage_in_bytes";
  String CGROUP_NO_LIMIT = "-1";

  enum CpuParameters {
    // cgroup1
    PERIOD_US("cfs_period_us"),
    QUOTA_US("cfs_quota_us"),
    SHARES("shares"),

    // cgroup2
    MAX("max"),
    WEIGHT("weight");

    private final String name;
    CpuParameters(String name) {
      this.name = name;
    }

    String getName() {
      return this.name;
    }
  }

  enum MemoryParameters {
    // cgroup1
    HARD_LIMIT_BYTES("limit_in_bytes"),
    SOFT_LIMIT_BYTES("soft_limit_in_bytes"),
    SWAPPINESS("swappiness"),
    OOM_CONTROL("oom_control"),

    // cgroup2
    MEMORY_MAX("max"),
    MEMORY_HIGH("high"),
    EVENTS_LOCAL("events.local");

    private final String name;
    MemoryParameters(String name) {
      this.name = name;
    }

    String getName() {
      return this.name;
    }
  }

  /**
   * Mounts or initializes a cgroup controller.
   * @param controller - the controller being initialized
   * @throws ResourceHandlerException the initialization failed due to the
   * environment
   */
  void initializeCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  /**
   * Creates a cgroup for a given controller.
   * @param controller - controller type for which the cgroup is being created
   * @param cGroupId - id of the cgroup being created
   * @return full path to created cgroup
   * @throws ResourceHandlerException creation failed
   */
  String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException;

  /**
   * Deletes the specified cgroup.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup being deleted
   * @throws ResourceHandlerException deletion failed
   */
  void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  /**
   * Gets the absolute path to the specified cgroup controller.
   * @param controller - controller type for the cgroup
   * @return the root of the controller.
   */
  String getControllerPath(CGroupController controller);

  /**
   * Gets the relative path for the cgroup, independent of a controller, for a
   * given cgroup id.
   * @param cGroupId - id of the cgroup
   * @return path for the cgroup relative to the root of (any) controller.
   */
  String getRelativePathForCGroup(String cGroupId);

  /**
   * Gets the full path for the cgroup, given a controller and a cgroup id.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup
   */
  String getPathForCGroup(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for the cgroup's tasks file, given a controller and a
   * cgroup id.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup's tasks file
   */
  String getPathForCGroupTasks(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for a cgroup parameter, given a controller,
   * cgroup id and parameter name.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return full path for the cgroup parameter
   */
  String getPathForCGroupParam(CGroupController controller, String
      cGroupId, String param);

  /**
   * updates a cgroup parameter, given a controller, cgroup id, parameter name.
   * and a parameter value
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @param value - value to be written to the parameter file
   * @throws ResourceHandlerException the operation failed
   */
  void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException;

  /**
   * reads a cgroup parameter value, given a controller, cgroup id, parameter.
   * name
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return parameter value as read from the parameter file
   * @throws ResourceHandlerException the operation failed
   */
  String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException;

  /**
   * Returns CGroup Mount Path.
   * @return parameter value as read from the parameter file
   */
  String getCGroupMountPath();
}
