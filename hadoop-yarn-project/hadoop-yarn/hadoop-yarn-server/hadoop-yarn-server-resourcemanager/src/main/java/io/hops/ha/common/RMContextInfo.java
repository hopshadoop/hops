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
import io.hops.metadata.yarn.entity.RMNodeToAdd;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class RMContextInfo {

  private static final Log LOG = LogFactory.getLog(RMContextInfo.class);

  private Map<NodeId, RMNodeToAdd>
      activeNodesToAdd = new HashMap<NodeId, RMNodeToAdd>();

  private final Set<NodeId> activeNodesToRemove = new ConcurrentSkipListSet<NodeId>();
  private Set<NodeId>
      inactiveNodeToAdd = new HashSet<NodeId>();
  private final Set<NodeId> inactiveNodesToRemove = new ConcurrentSkipListSet<NodeId>();
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

  public void toAddInactiveRMNode(NodeId key) {
    this.inactiveNodeToAdd.add(key);
    this.inactiveNodesToRemove.remove(key);
  }

  public void toAddActiveRMNode(NodeId key,
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode val,
          int pendingEventId) {
    RMNodeToAdd toAdd = new RMNodeToAdd();

    Resource hopResource = new Resource(val.getNodeID().toString(),
            Resource.TOTAL_CAPABILITY, Resource.RMNODE, val.
            getTotalCapability().getMemory(), val.getTotalCapability().
            getVirtualCores(),pendingEventId);
    toAdd.setResources(hopResource);
    //Persist Node
    if (val.getNode() != null) {
      if (val.getNode().getParent() != null) {
        toAdd.setNodeToAdd(new Node(val.getNodeID().toString(), val.
                getNode().getName(), val.getNode().getNetworkLocation(),
                val.getNode().getLevel(), val.getNode().getParent().
                toString(),pendingEventId));
      } else {
        toAdd.setNodeToAdd(new Node(val.getNodeID().toString(), val.
                getNode().getName(), val.getNode().getNetworkLocation(),
                val.getNode().getLevel(), null,pendingEventId));
      }
    }
    //Persist RMNode
    RMNode hopRMNode = new RMNode(val.getNodeID().toString(), val.getHostName(),
            val.getCommandPort(), val.getHttpPort(), val.getNodeAddress(),
            val.getHttpAddress(), val.getHealthReport(),
            val.getLastHealthReportTime(),
            ((RMNodeImpl) val).getCurrentState(), val.
            getNodeManagerVersion(), 0, ((RMNodeImpl) val).
            getUpdatedContainerInfoId(),pendingEventId);
    toAdd.setRMNode(hopRMNode);
    //Persist RMCoxtentNodesMap
    RMContextActiveNodes hopCtxNode = new RMContextActiveNodes(val.
            getNodeID().toString());
    toAdd.setHopCtxNode(hopCtxNode);
    
    this.activeNodesToAdd.put(key, toAdd);
    this.activeNodesToRemove.remove(key);
  }

  public void toRemoveInactiveRMNode(NodeId key) {
    if(!inactiveNodeToAdd.remove(key)){
      this.inactiveNodesToRemove.add(key);
    }
  }

  public void toRemoveActiveRMNode(NodeId key) {
    if(this.activeNodesToAdd.remove(key)==null){
      this.activeNodesToRemove.add(key);
    }
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

  //TOVERIFY removed inactive node are replaced by active node with the same id
  //we should not remove the node and resources corresponding to this id
  public void persistInactiveNodesToRemove() throws StorageException {
    if (!inactiveNodesToRemove.isEmpty()) {
      ArrayList<RMContextInactiveNodes> inactiveToRemove =
          new ArrayList<RMContextInactiveNodes>();

      for (NodeId inactiveNodeToRemove : inactiveNodesToRemove) {
        inactiveToRemove.add(new RMContextInactiveNodes(inactiveNodeToRemove.
            toString()));
      }
      RMContextInactiveNodesDataAccess rmctxInactiveNodesDA =
          (RMContextInactiveNodesDataAccess) RMStorageFactory.
              getDataAccess(RMContextInactiveNodesDataAccess.class);
      rmctxInactiveNodesDA.removeAll(inactiveToRemove);

    }
  }

  public void persistActiveNodesToAdd(RMNodeDataAccess rmnodeDA,
      ResourceDataAccess resourceDA, NodeDataAccess nodeDA)
      throws StorageException {
    if (!activeNodesToAdd.isEmpty()) {
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      ArrayList<Node> nodesToAdd = new ArrayList<Node>();
      ArrayList<RMNode> toAddRMNodes = new ArrayList<RMNode>();
      ArrayList<RMContextActiveNodes> rmctxnodesToAdd = new ArrayList<RMContextActiveNodes>();
      //First parse the NodeIds
      for (NodeId key : activeNodesToAdd.keySet()) {
        RMNodeToAdd val = activeNodesToAdd.get(key);
        toAddResources.add(val.getResources());
        nodesToAdd.add(val.getNodesToAdd());
        toAddRMNodes.add(val.getRmNode());
        rmctxnodesToAdd.add(val.getHopCtxNode());
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


  public void persistInactiveNodesToAdd(
      RMContextInactiveNodesDataAccess rmctxInactiveNodesDA)
      throws StorageException {
    if (inactiveNodeToAdd != null) {
      ArrayList<RMContextInactiveNodes> inactiveToAdd =
          new ArrayList<RMContextInactiveNodes>();
      for (NodeId key : inactiveNodeToAdd) {
          inactiveToAdd.add(new RMContextInactiveNodes(key.toString()));
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
