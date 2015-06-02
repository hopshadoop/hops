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
package io.hops.ha.common;

import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.entity.Load;
import io.hops.metadata.yarn.entity.Node;
import io.hops.metadata.yarn.entity.RMContextActiveNodes;
import io.hops.metadata.yarn.entity.RMContextInactiveNodes;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMContextInfo {

  private static final Log LOG = LogFactory.getLog(RMContextInfo.class);

  private Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
      activeNodesToAdd;

  private final List<NodeId> activeNodesToRemove = new ArrayList<NodeId>();
  private Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
      inactiveNodeToAdd;
  private final List<NodeId> inactiveNodesToRemove = new ArrayList<NodeId>();
  private int load = -1;
  private String rmHostName;

  public void persist(RMNodeDataAccess rmnodeDA, ResourceDataAccess resourceDA,
      NodeDataAccess nodeDA,
      RMContextInactiveNodesDataAccess rmctxinactvenodesDA)
      throws StorageException {
    persistActiveNodesToAdd(rmnodeDA, resourceDA, nodeDA);
    persistActiveNodeToRemove();
    persistInactiveNodesToAdd(rmctxinactvenodesDA);
    persistInactiveNodesToRemove();
    persistLoad();
  }

  public void toAddInactiveRMNode(NodeId key,
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode val) {
    if (this.inactiveNodeToAdd == null) {
      this.inactiveNodeToAdd = new HashMap<NodeId, org.apache.hadoop.yarn.
          server.resourcemanager.rmnode.RMNode>(1);
    }
    this.inactiveNodeToAdd.put(key, val);
  }

  public void toAddActiveRMNode(NodeId key,
      org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode val) {
    if (this.activeNodesToAdd == null) {
      this.activeNodesToAdd = new HashMap<NodeId, org.apache.hadoop.yarn.server.
          resourcemanager.rmnode.RMNode>(1);
    }
    this.activeNodesToAdd.put(key, val);
  }

  public void toRemoveInactiveRMNode(NodeId key) {
    this.inactiveNodesToRemove.add(key);
  }

  public void toRemoveActiveRMNode(NodeId key) {
    this.activeNodesToRemove.add(key);
  }

  public List<NodeId> getActiveNodeToRemove() {
    return this.activeNodesToRemove;
  }

  public void persistActiveNodeToRemove() throws StorageException {
    if (!activeNodesToRemove.isEmpty()) {
      ArrayList<RMContextActiveNodes> rmctxnodesToRemove =
          new ArrayList<RMContextActiveNodes>();
      for (NodeId activeNodeToRemove : activeNodesToRemove) {
        rmctxnodesToRemove.add(new RMContextActiveNodes(activeNodeToRemove.
            toString()));
      }
      RMContextActiveNodesDataAccess rmctxnodesDA =
          (RMContextActiveNodesDataAccess) RMStorageFactory
              .getDataAccess(RMContextActiveNodesDataAccess.class);
      rmctxnodesDA.removeAll(rmctxnodesToRemove);
    }
  }

  public List<NodeId> getInactiveNodeToRemove() {
    return this.inactiveNodesToRemove;
  }

  public void persistInactiveNodesToRemove() throws StorageException {
    if (!inactiveNodesToRemove.isEmpty()) {
      ArrayList<RMContextInactiveNodes> inactiveToRemove =
          new ArrayList<RMContextInactiveNodes>();
      ArrayList<RMNode> nodesToRemove = new ArrayList<RMNode>();
      ArrayList<Resource> resourceToRemove = new ArrayList<Resource>();

      for (NodeId inactiveNodeToRemove : inactiveNodesToRemove) {
        LOG.debug("HOP :: remove inactive node " + inactiveNodeToRemove);
        inactiveToRemove.add(new RMContextInactiveNodes(inactiveNodeToRemove.
            toString()));
        nodesToRemove.add(new RMNode(inactiveNodeToRemove.toString()));
        resourceToRemove.add(new Resource(inactiveNodeToRemove.toString(),
            Resource.TOTAL_CAPABILITY, Resource.RMNODE));
      }
      RMContextInactiveNodesDataAccess rmctxInactiveNodesDA =
          (RMContextInactiveNodesDataAccess) RMStorageFactory.
              getDataAccess(RMContextInactiveNodesDataAccess.class);
      rmctxInactiveNodesDA.removeAll(inactiveToRemove);

      //clean all the tables depending of RMNode: JustLaunchedContainers,
      //ContainersToClean, FinishedApplications, UpdatedContainerInfo, NodeHeartBeatResponse, ha_rmnode
      //ha_node
      //clearing ha_rmNode is enough because the other tables will be cleared by cascade
      RMNodeDataAccess rmnodeDA = (RMNodeDataAccess) RMStorageFactory.
          getDataAccess(RMNodeDataAccess.class);
      rmnodeDA.removeAll(nodesToRemove);

      //clean ha_resource
      ResourceDataAccess resourceDA = (ResourceDataAccess) YarnAPIStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      resourceDA.removeAll(resourceToRemove);
    }
  }

  public void persistActiveNodesToAdd(RMNodeDataAccess rmnodeDA,
      ResourceDataAccess resourceDA, NodeDataAccess nodeDA)
      throws StorageException {
    if (activeNodesToAdd != null) {
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      ArrayList<Node> nodesToAdd = null;
      ArrayList<RMNode> toAddRMNodes = null;
      ArrayList<RMContextActiveNodes> rmctxnodesToAdd = new ArrayList<RMContextActiveNodes>();
      //First parse the NodeIds
      for (NodeId key : activeNodesToAdd.keySet()) {
        if (activeNodesToRemove == null || !activeNodesToRemove.remove(key)) {
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode val =
              activeNodesToAdd.get(key);
          //Persist Resource
          Resource hopResource = new Resource(val.getNodeID().toString(),
              Resource.TOTAL_CAPABILITY, Resource.RMNODE, val.
              getTotalCapability().getMemory(), val.getTotalCapability().
              getVirtualCores());
          toAddResources.add(hopResource);
          //Persist Node
          if (val.getNode() != null) {
            nodesToAdd = new ArrayList<Node>();
            if (val.getNode().getParent() != null) {
              nodesToAdd.add(new Node(val.getNodeID().toString(), val.
                  getNode().getName(), val.getNode().getNetworkLocation(),
                  val.getNode().getLevel(), val.getNode().getParent().
                  toString()));
            } else {
              nodesToAdd.add(new Node(val.getNodeID().toString(), val.
                  getNode().getName(), val.getNode().getNetworkLocation(),
                  val.getNode().getLevel(), null));
            }
          }
          //Persist RMNode
          RMNode hopRMNode =
              new RMNode(val.getNodeID().toString(), val.getHostName(),
                  val.getCommandPort(), val.getHttpPort(), val.getNodeAddress(),
                  val.getHttpAddress(), val.getHealthReport(),
                  val.getLastHealthReportTime(),
                  ((RMNodeImpl) val).getCurrentState(), val.
                  getNodeManagerVersion(), 0, ((RMNodeImpl) val).
                  getUpdatedContainerInfoId());
          if (toAddRMNodes == null) {
            toAddRMNodes = new ArrayList<RMNode>();
          }
          toAddRMNodes.add(hopRMNode);
          //Persist RMCoxtentNodesMap
          RMContextActiveNodes hopCtxNode = new RMContextActiveNodes(val.
              getNodeID().toString());
          rmctxnodesToAdd.add(hopCtxNode);
        }
      }
      rmnodeDA.addAll(toAddRMNodes);
      resourceDA.addAll(toAddResources);
      nodeDA.addAll(nodesToAdd);
      RMContextActiveNodesDataAccess rmctxnodesDA =
          (RMContextActiveNodesDataAccess) RMStorageFactory
              .getDataAccess(RMContextActiveNodesDataAccess.class);
      rmctxnodesDA.addAll(rmctxnodesToAdd);
    }
  }

  public Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.rmnode.
      RMNode> getInactiveNodesToAdd() {
    return this.inactiveNodeToAdd;
  }

  public void persistInactiveNodesToAdd(
      RMContextInactiveNodesDataAccess rmctxInactiveNodesDA)
      throws StorageException {
    if (inactiveNodeToAdd != null) {
      ArrayList<RMContextInactiveNodes> inactiveToAdd =
          new ArrayList<RMContextInactiveNodes>();
      for (NodeId key : inactiveNodeToAdd.keySet()) {
        if (!inactiveNodesToRemove.remove(key)) {
          inactiveToAdd.add(new RMContextInactiveNodes(key.toString()));
        }
      }
      rmctxInactiveNodesDA.addAll(inactiveToAdd);
    }
  }

  public void updateLoad(String rmHostName, int load) {
    this.load = load;
    this.rmHostName = rmHostName;
  }

  private void persistLoad() throws StorageException {
    if (load < 0) {
      return;
    }
    Load hopLoad = new Load(rmHostName, load);
    RMLoadDataAccess da = (RMLoadDataAccess) RMStorageFactory
        .getDataAccess(RMLoadDataAccess.class);
    da.update(hopLoad);
  }
}
