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

package org.apache.hadoop.yarn.sls.nodemanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode
        .UpdatedContainerInfo;
import io.hops.ha.common.TransactionState;
import java.util.Set;

public class NodeInfo {
  private static int NODE_ID = 0;

  public static NodeId newNodeID(String host, int port) {
    return NodeId.newInstance(host, port);
  }

  private static class FakeRMNodeImpl implements RMNode {
    private NodeId nodeId;
    private String hostName;
    private String nodeAddr;
    private String httpAddress;
    private int cmdPort;
    private volatile ResourceOption perNode;
    private String rackName;
    private String healthReport;
    private NodeState state;
    private List<ContainerId> toCleanUpContainers;
    private List<ApplicationId> toCleanUpApplications;
    
    public FakeRMNodeImpl(NodeId nodeId, String nodeAddr, String httpAddress,
        ResourceOption perNode, String rackName, String healthReport,
        int cmdPort, String hostName, NodeState state) {
      this.nodeId = nodeId;
      this.nodeAddr = nodeAddr;
      this.httpAddress = httpAddress;
      this.perNode = perNode;
      this.rackName = rackName;
      this.healthReport = healthReport;
      this.cmdPort = cmdPort;
      this.hostName = hostName;
      this.state = state;
      toCleanUpApplications = new ArrayList<ApplicationId>();
      toCleanUpContainers = new ArrayList<ContainerId>();
    }

    public NodeId getNodeID() {
      return nodeId;
    }
    
    public String getHostName() {
      return hostName;
    }
    
    public int getCommandPort() {
      return cmdPort;
    }
    
    public int getHttpPort() {
      return 0;
    }

    public String getNodeAddress() {
      return nodeAddr;
    }

    public String getHttpAddress() {
      return httpAddress;
    }

    public String getHealthReport() {
      return healthReport;
    }

    public long getLastHealthReportTime() {
      return 0; 
    }

    public Resource getTotalCapability() {
      return perNode.getResource();
    }
    
    public ResourceOption getResourceOption() {
      return perNode;
    }

    public String getRackName() {
      return rackName;
    }

    public Node getNode() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public NodeState getState() {
      return state;
    }

    public List<ContainerId> getContainersToCleanUp() {
      return toCleanUpContainers;
    }

    public List<ApplicationId> getAppsToCleanup() {
      return toCleanUpApplications;
    }

    public void updateNodeHeartbeatResponseForCleanup(
            NodeHeartbeatResponse response) {
    }

    public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {
      return null;
    }

    @Override
    public void setLastNodeHeartBeatResponseId(int id) {
    }
        
    public List<UpdatedContainerInfo> pullContainerUpdates() {
      ArrayList<UpdatedContainerInfo> list = new ArrayList<UpdatedContainerInfo>();
      
      ArrayList<ContainerStatus> list2 = new ArrayList<ContainerStatus>();
      for(ContainerId cId : this.toCleanUpContainers) {
        list2.add(ContainerStatus.newInstance(cId, ContainerState.RUNNING, "", 
          ContainerExitStatus.SUCCESS));
      }
      //TODO check is the value 0 is enough here or if we should find the true value of the id
      list.add(new UpdatedContainerInfo(new ArrayList<ContainerStatus>(), 
        list2, 0));
      return list;
    }

	@Override
	public String getNodeManagerVersion() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public void setResourceOption(ResourceOption resourceOption) {
      perNode = resourceOption;
    }

        @Override
        public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response, TransactionState ts) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<UpdatedContainerInfo> pullContainerUpdates(TransactionState ts) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void recover(RMStateStore.RMState state) throws Exception {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setContainersToCleanUp(Set<ContainerId> newSet) {
        }

        @Override
        public void setAppsToCleanup(List<ApplicationId> newList) {
        }

        @Override
        public void setNextHeartBeat(boolean nextHeartbeat) {
        }
  }
  
  public static RMNode newNodeInfo(String rackName, String hostName,
                              final ResourceOption resourceOption, int port) {
    final NodeId nodeId = newNodeID(hostName, port);
    final String nodeAddr = hostName + ":" + port;
    final String httpAddress = hostName;
    
    return new FakeRMNodeImpl(nodeId, nodeAddr, httpAddress,
        resourceOption, rackName, "Me good",
        port, hostName, null);
  }
  
  public static RMNode newNodeInfo(String rackName, String hostName,
                              final Resource resource) {
    return newNodeInfo(rackName, hostName, ResourceOption.newInstance(resource,
        RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT), NODE_ID++);
  }
}
