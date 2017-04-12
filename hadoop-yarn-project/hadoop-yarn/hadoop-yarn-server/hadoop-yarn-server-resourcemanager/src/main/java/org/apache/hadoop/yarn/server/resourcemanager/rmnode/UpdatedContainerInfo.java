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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.records.ContainerStatus;

public class UpdatedContainerInfo {
  private static AtomicInteger nextUCIId = new AtomicInteger(0);
  
  private List<ContainerStatus> newlyLaunchedContainers;
  private List<ContainerStatus> completedContainers;
  private final int uciId;
  
  public UpdatedContainerInfo() {
    uciId = nextUCIId.incrementAndGet();
  }

  public UpdatedContainerInfo(List<ContainerStatus> newlyLaunchedContainers
      , List<ContainerStatus> completedContainers) {
    this.newlyLaunchedContainers = newlyLaunchedContainers;
    this.completedContainers = completedContainers;
    uciId=nextUCIId.incrementAndGet();
  } 

  public UpdatedContainerInfo(List<ContainerStatus> newlyLaunchedContainers
      , List<ContainerStatus> completedContainers, int uciId) {
    this.newlyLaunchedContainers = newlyLaunchedContainers;
    this.completedContainers = completedContainers;
    this.uciId= uciId;
  }
  
  public List<ContainerStatus> getNewlyLaunchedContainers() {
    return this.newlyLaunchedContainers;
  }

  public List<ContainerStatus> getCompletedContainers() {
    return this.completedContainers;
  }

  public int getUciId() {
    return uciId;
  }
}
