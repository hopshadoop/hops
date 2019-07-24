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

package io.hops.yarn.server.resourcemanager.quota;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

public class QuotaComputeEvent extends QuotaEvent {

  private ContainerId containerId;
  private long computeTime;

  public QuotaComputeEvent(ContainerId containerId, long computeTime) {
    super(QuotaEventType.QUOTA_COMPUTE);
    this.containerId = containerId;
    this.computeTime = computeTime;
  }

  @Override
  public int hashCode() {
    return containerId.hashCode();
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public long getComputeTime() {
    return computeTime;
  }

}
