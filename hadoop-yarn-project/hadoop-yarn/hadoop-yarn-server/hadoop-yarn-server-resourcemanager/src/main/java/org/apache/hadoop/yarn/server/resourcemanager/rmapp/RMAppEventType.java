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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

public enum RMAppEventType {
  // Source: ClientRMService
  START,
  //gautier done
  //  RECOVER,
  KILL,
  MOVE,
  // Move app to a new queue

  // Source: Scheduler and RMAppManager
  APP_REJECTED,
  //gautier done

  // Source: Scheduler
  APP_ACCEPTED,
  //gautier done

  // Source: RMAppAttempt
  ATTEMPT_REGISTERED,
  ATTEMPT_UNREGISTERED,
  ATTEMPT_FINISHED,
  // Will send the final state //gautier done
  ATTEMPT_FAILED,
  ATTEMPT_KILLED,
  //gautier done
  NODE_UPDATE,

  // Source: RMStateStore
  APP_NEW_SAVED,
  //gautier done
  APP_UPDATE_SAVED, //gautier done
}
