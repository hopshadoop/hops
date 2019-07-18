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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ContainerStartMonitoringEvent extends ContainersMonitorEvent {

  private final long vmemLimit;
  private final long pmemLimit;
  private final int cpuVcores;
  private final long launchDuration;
  private final long localizationDuration;

  public ContainerStartMonitoringEvent(ContainerId containerId,
      long vmemLimit, long pmemLimit, int cpuVcores, long launchDuration,
      long localizationDuration) {
    super(containerId, ContainersMonitorEventType.START_MONITORING_CONTAINER);
    this.vmemLimit = vmemLimit;
    this.pmemLimit = pmemLimit;
    this.cpuVcores = cpuVcores;
    this.launchDuration = launchDuration;
    this.localizationDuration = localizationDuration;
  }

  public long getVmemLimit() {
    return this.vmemLimit;
  }

  public long getPmemLimit() {
    return this.pmemLimit;
  }

  public int getCpuVcores() {
    return this.cpuVcores;
  }

  public long getLaunchDuration() {
    return this.launchDuration;
  }

  public long getLocalizationDuration() {
    return this.localizationDuration;
  }
}
