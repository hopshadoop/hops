/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.metadata.yarn.entity.ContainerStatus;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

public class StreamingRTComps {

  private final Set<org.apache.hadoop.yarn.api.records.ContainerId> containersToClean;
  private final List<org.apache.hadoop.yarn.api.records.ApplicationId> finishedApp;
  private final String nodeId;
  private final boolean nextHeartbeat;
  private final List<ContainerStatus> hopContainersStatusList;
  private final MasterKey currentNMMasterKey;
  private final MasterKey nextNMMasterKey;
  private final MasterKey currentRMContainerMasterKey;
  private final MasterKey nextRMContainerMasterKey;  
  private final float currentPrice;
  private final long currentPriceTick;


  public StreamingRTComps(
          Set<org.apache.hadoop.yarn.api.records.ContainerId> containersToClean,
          List<org.apache.hadoop.yarn.api.records.ApplicationId> finishedApp,
          String nodeId, boolean nextHeartbeat,
          List<ContainerStatus> hopContainersStatusList,
          MasterKey currentNMMasterKey,
          MasterKey nextNMMasterKey,
          MasterKey currentRMContainerMasterKey,
          MasterKey nextRMContainerMasterKey,
          float currentPrice,
          long currentPriceTick
  ) {
    this.containersToClean = containersToClean;
    this.finishedApp = finishedApp;
    this.nodeId = nodeId;
    this.nextHeartbeat = nextHeartbeat;
    this.hopContainersStatusList = hopContainersStatusList;
    this.currentNMMasterKey=currentNMMasterKey;
    this.nextNMMasterKey = nextNMMasterKey;
    this.currentRMContainerMasterKey = currentRMContainerMasterKey;
    this.nextRMContainerMasterKey = nextRMContainerMasterKey;
    this.currentPrice = currentPrice;
    this.currentPriceTick = currentPriceTick;
  }
  
  public float getCurrentPrice() {
    return currentPrice;
  }

  public long getCurrentPriceTick() {
    return currentPriceTick;
  }

  public Set<org.apache.hadoop.yarn.api.records.ContainerId> getContainersToClean() {
    return containersToClean;
  }

  public List<org.apache.hadoop.yarn.api.records.ApplicationId> getFinishedApp() {
    return finishedApp;
  }

  public String getNodeId() {
    return nodeId;
  }

  public boolean isNextHeartbeat() {
    return nextHeartbeat;
  }
  
  public List<ContainerStatus> getHopContainersStatusList() {
      return hopContainersStatusList;
  }

  public MasterKey getCurrentNMMasterKey() {
    return currentNMMasterKey;
  }

  public MasterKey getNextNMMasterKey() {
    return nextNMMasterKey;
  }

  public MasterKey getCurrentRMContainerMasterKey() {
    return currentRMContainerMasterKey;
  }

  public MasterKey getNextRMContainerMasterKey() {
    return nextRMContainerMasterKey;
  }

  
}
