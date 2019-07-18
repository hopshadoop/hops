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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

public enum SchedulerEventType {

  // Source: Node
  NODE_ADDED,
  NODE_REMOVED,
  NODE_UPDATE,
  NODE_RESOURCE_UPDATE,
  NODE_LABELS_UPDATE,
  NODE_ATTRIBUTES_UPDATE,

  // Source: RMApp
  APP_ADDED,
  APP_REMOVED,

  // Source: RMAppAttempt
  APP_ATTEMPT_ADDED,
  APP_ATTEMPT_REMOVED,

  // Source: ContainerAllocationExpirer
  CONTAINER_EXPIRED,

  // Source: SchedulerAppAttempt::pullNewlyUpdatedContainer.
  RELEASE_CONTAINER,

  /* Source: SchedulingEditPolicy */
  KILL_RESERVED_CONTAINER,

  // Mark a container for preemption
  MARK_CONTAINER_FOR_PREEMPTION,

  // Mark a for-preemption container killable
  MARK_CONTAINER_FOR_KILLABLE,

  // Cancel a killable container
  MARK_CONTAINER_FOR_NONKILLABLE,

  //Queue Management Change
  MANAGE_QUEUE
}
