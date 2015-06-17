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
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.entity.fair.FSSchedulerNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FairSchedulerNodeInfo {
  private Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.
      fair.FSSchedulerNode> fsschedulerNodesToAdd;
  private Map<NodeId, org.apache.hadoop.yarn.server.resourcemanager.scheduler.
      fair.FSSchedulerNode> fsschedulerNodesToRemove;

  void persist(FSSchedulerNodeDataAccess FSSNodeDA) throws StorageException {
    persistFSSchedulerNodesToAdd(FSSNodeDA);
  }

  public void addFSSchedulerNode(NodeId nodeId, org.apache.hadoop.yarn.server.
      resourcemanager.scheduler.fair.FSSchedulerNode fsnode) {
    if (fsschedulerNodesToAdd == null) {
      fsschedulerNodesToAdd = new HashMap<NodeId, org.apache.hadoop.yarn.server.
          resourcemanager.scheduler.fair.FSSchedulerNode>();
    }
    fsschedulerNodesToAdd.put(nodeId, fsnode);
  }

  public void removeFSSchedulerNode(NodeId nodeId, org.apache.hadoop.yarn.
      server.resourcemanager.scheduler.fair.FSSchedulerNode fsnode) {
    if (fsschedulerNodesToRemove == null) {
      fsschedulerNodesToRemove = new HashMap<NodeId, org.apache.hadoop.yarn.
          server.resourcemanager.scheduler.fair.FSSchedulerNode>();
    }
    fsschedulerNodesToRemove.put(nodeId, fsnode);
  }

  private void persistFSSchedulerNodesToAdd(FSSchedulerNodeDataAccess FSSNodeDA)
      throws StorageException {
    if (fsschedulerNodesToAdd != null) {
      List<FSSchedulerNode> toAddFSSchedulerNodes =
          new ArrayList<FSSchedulerNode>();
      for (NodeId nodeId : fsschedulerNodesToAdd.keySet()) {
        org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.
            FSSchedulerNode fssnode = fsschedulerNodesToAdd.get(nodeId);
        toAddFSSchedulerNodes.add(
            new FSSchedulerNode(nodeId.toString(), fssnode.getNumContainers(),
                null, null));
      }

      FSSNodeDA.addAll(toAddFSSchedulerNodes);
    }
  }
}
