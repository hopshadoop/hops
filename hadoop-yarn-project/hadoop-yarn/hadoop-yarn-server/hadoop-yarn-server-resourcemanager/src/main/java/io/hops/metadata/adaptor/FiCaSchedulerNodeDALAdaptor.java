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
package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FiCaSchedulerNodeDALAdaptor extends
    DalAdaptor<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode, FiCaSchedulerNode>
    implements
    FiCaSchedulerNodeDataAccess<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode> {

  private final FiCaSchedulerNodeDataAccess<FiCaSchedulerNode> dataAccess;

  public FiCaSchedulerNodeDALAdaptor(
      FiCaSchedulerNodeDataAccess<FiCaSchedulerNode> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public FiCaSchedulerNode convertHDFStoDAL(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode fiCaSchedulerNode)
      throws StorageException {
    return new FiCaSchedulerNode(
        fiCaSchedulerNode.getRMNode().getNodeID().toString(),
        fiCaSchedulerNode.getNodeName(), fiCaSchedulerNode.getNumContainers());
  }

  @Override
  public org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode convertDALtoHDFS(
      FiCaSchedulerNode dalClass) throws StorageException {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void add(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode toAdd)
      throws StorageException {
    dataAccess.add(convertHDFStoDAL(toAdd));
  }
  
  @Override
  public void addAll(
      Collection<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode> toAdd)
      throws StorageException {
    dataAccess.addAll(convertHDFStoDAL(toAdd));
  }
  
  @Override
  public void removeAll(
      Collection<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode> toRemove)
      throws StorageException {
    dataAccess.removeAll(convertHDFStoDAL(toRemove));
  }

  @Override
  public List<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode> getAll()
      throws StorageException {
    List<FiCaSchedulerNode> hopFiCaSchedulerNodes = dataAccess.getAll();
    List<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>
        fiCaSchedulerNodes =
        new ArrayList<org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode>();
    for (FiCaSchedulerNode hopFiCaSchedulerNode : hopFiCaSchedulerNodes) {
      fiCaSchedulerNodes.add(convertDALtoHDFS(hopFiCaSchedulerNode));
    }
    return fiCaSchedulerNodes;
  }
}
