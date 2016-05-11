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


import io.hops.ha.common.TransactionState;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

import java.util.List;
import java.util.Set;

/**
 * Node managers information on available resources
 * and other static information.
 */
public interface RMNode extends Recoverable {

  /**
   * negative value means no timeout
   */
  public static final int OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT = -1;
  
  /**
   * the node id of of this node.
   *
   * @return the node id of this node.
   */
  public NodeId getNodeID();
  
  /**
   * the hostname of this node
   *
   * @return hostname of this node
   */
  public String getHostName();
  
  /**
   * the command port for this node
   *
   * @return command port for this node
   */
  public int getCommandPort();
  
  /**
   * the http port for this node
   *
   * @return http port for this node
   */
  public int getHttpPort();


  /**
   * the ContainerManager address for this node.
   *
   * @return the ContainerManager address for this node.
   */
  public String getNodeAddress();
  
  /**
   * the http-Address for this node.
   *
   * @return the http-url address for this node
   */
  public String getHttpAddress();
  
  /**
   * the latest health report received from this node.
   *
   * @return the latest health report received from this node.
   */
  public String getHealthReport();
  
  /**
   * the time of the latest health report received from this node.
   *
   * @return the time of the latest health report received from this node.
   */
  public long getLastHealthReportTime();

  /**
   * the node manager version of the node received as part of the
   * registration with the resource manager
   */
  public String getNodeManagerVersion();

  /**
   * the total available resource.
   *
   * @return the total available resource.
   */
  public Resource getTotalCapability();
  
  /**
   * Set resource option with total available resource and
   * overCommitTimoutMillis
   *
   * @param resourceOption
   */
  public void setResourceOption(ResourceOption resourceOption);
  
  /**
   * resource option with total available resource and overCommitTimoutMillis
   *
   * @return ResourceOption
   */
  public ResourceOption getResourceOption();
  
  /**
   * The rack name for this node manager.
   *
   * @return the rack name.
   */
  public String getRackName();
 
  /**
   * the {@link Node} information for this node.
   *
   * @return {@link Node} information for this node.
   */
  public Node getNode();
  
  public NodeState getState();

  public List<ContainerId> getContainersToCleanUp();

  public List<ApplicationId> getAppsToCleanup();

  public void setContainersToCleanUp(Set<ContainerId> newSet);

  public void setAppsToCleanup(List<ApplicationId> newList);

  /**
   * Update a {@link NodeHeartbeatResponse} with the list of containers and
   * applications to clean up for this node.
   *
   * @param response
   *     the {@link NodeHeartbeatResponse} to update
   */
  public void updateNodeHeartbeatResponseForCleanup(
      NodeHeartbeatResponse response, TransactionState ts);

  public NodeHeartbeatResponse getLastNodeHeartBeatResponse();
  
  public void setLastNodeHeartBeatResponseId(int id);
  
  /**
   * Get and clear the list of containerUpdates accumulated across NM
   * heartbeats.
   *
   * @return containerUpdates accumulated across NM heartbeats.
   */
  public List<UpdatedContainerInfo> pullContainerUpdates(TransactionState ts);
  public void setNextHeartBeat(boolean nextHeartbeat);
}
