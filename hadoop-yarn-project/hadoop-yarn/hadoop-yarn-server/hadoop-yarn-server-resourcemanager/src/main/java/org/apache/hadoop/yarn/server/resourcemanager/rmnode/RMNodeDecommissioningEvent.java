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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * RMNode Decommissioning Event.
 *
 */
public class RMNodeDecommissioningEvent extends RMNodeEvent {
  // Optional decommissioning timeout in second.
  private final Integer decommissioningTimeout;

  // Create instance with optional timeout
  // (timeout could be null which means use default).
  public RMNodeDecommissioningEvent(NodeId nodeId, Integer timeout) {
    super(nodeId, RMNodeEventType.GRACEFUL_DECOMMISSION);
    this.decommissioningTimeout = timeout;
  }

  public Integer getDecommissioningTimeout() {
    return this.decommissioningTimeout;
  }
}
