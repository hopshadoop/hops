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
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains FiCaSchedulerNode related classes, used mainly by scheduler.
 */
public class FiCaSchedulerNodeInfoToUpdate {

  private static final Log LOG =
      LogFactory.getLog(FiCaSchedulerNodeInfoToUpdate.class);
  
  private final TransactionStateImpl transactionState;
  private FiCaSchedulerNode
      infoToUpdate;
  private final String id;
  private Map<String, LaunchedContainers> launchedContainersToAdd
          = new HashMap<String, LaunchedContainers>();
  private Set<String> launchedContainersToRemove = new HashSet<String>();
  private Map<Integer, Resource>
      toUpdateResources = new HashMap<Integer, Resource>();

  public FiCaSchedulerNodeInfoToUpdate(String id, TransactionStateImpl transactionState) {
    this.id = id;
    this.transactionState = transactionState;
  }

  public void agregate(FiCaSchedulerNodeInfoAgregate agregate){
    agregateToUpdateFicaSchedulerNode(agregate);
    agregateToUpdateFiCaSchedulerNodeId(agregate);
  }
  
  public void toUpdateResource(Integer type,
      org.apache.hadoop.yarn.api.records.Resource res) {

    toUpdateResources.put(type, new Resource(id, type, Resource.FICASCHEDULERNODE,
            res.getMemory(),
            res.getVirtualCores(),0));
  }

  public void updateReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    //TOPERSIST reserved container should also be updated??
    transactionState.addRMContainerToUpdate((RMContainerImpl) node.getReservedContainer());
  }

  public void toAddLaunchedContainers(String cid, String rmcon) {
    launchedContainersToAdd.put(cid, new LaunchedContainers(id, cid, rmcon));
    launchedContainersToRemove.remove(cid);
  }

  public void toRemoveLaunchedContainers(String cid) {
    if (launchedContainersToAdd.remove(cid)==null){
      launchedContainersToRemove.add(cid);
    }
  }

  public void infoToUpdate(
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    String reservedContainer = node.getReservedContainer() != null ? node.
            getReservedContainer().toString() : null;

    infoToUpdate = new FiCaSchedulerNode(node.getNodeID().toString(),
            node.getNodeName(), node.
            getNumContainers(), reservedContainer);
  }

  public void agregateToUpdateResources(FiCaSchedulerNodeInfoAgregate agregate){
    if (toUpdateResources != null) {
      agregate.addAllResourcesToUpdate(toUpdateResources.values());
    }
  }
    
  public void agregateToUpdateFiCaSchedulerNodeId(
          FiCaSchedulerNodeInfoAgregate agregate){
    agregateLaunchedContainersToAdd(agregate);
    agregateLaunchedContainersToRemove(agregate);
    agregateToUpdateResources(agregate);
  }
    
  private void agregateLaunchedContainersToAdd(FiCaSchedulerNodeInfoAgregate agregate){
    if (launchedContainersToAdd != null) {
      agregate.addAlllaunchedContainersToAdd(launchedContainersToAdd.values());
    }
  }
    


  private void agregateLaunchedContainersToRemove(
          FiCaSchedulerNodeInfoAgregate agregate){
    if (launchedContainersToRemove != null) {
      ArrayList<LaunchedContainers> toRemoveLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToRemove) {
        toRemoveLaunchedContainers.add(new LaunchedContainers(id, key, null));
      }
      agregate.addAllLaunchedContainersToRemove(toRemoveLaunchedContainers);
    }
  }

  private void agregateToUpdateFicaSchedulerNode(FiCaSchedulerNodeInfoAgregate agregate) {
    if (infoToUpdate != null) {
      agregate.addToUpdateFiCaSchedulerNode(infoToUpdate);
    }
  }
}
