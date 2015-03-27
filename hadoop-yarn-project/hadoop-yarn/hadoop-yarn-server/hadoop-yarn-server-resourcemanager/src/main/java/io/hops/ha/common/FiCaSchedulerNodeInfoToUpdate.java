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
  private org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode
      infoToUpdate;
  private final String id;
  private Map<String, String> launchedContainersToAdd;
  private Set<String> launchedContainersToRemove;
  private org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer
      reservedRMContainerToRemove;
  private boolean addRMContainer = false;
  private Map<Integer, org.apache.hadoop.yarn.api.records.Resource>
      toUpdateResources;

  public FiCaSchedulerNodeInfoToUpdate(String id) {
    this.id = id;
  }

  public void persist(ResourceDataAccess resourceDA,
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {

    persistToUpdateFicaSchedulerNode(ficaNodeDA, rmcontainerDA);
    persistToUpdateFiCaSchedulerNodeId(resourceDA, launchedContainersDA);
  }

  public void toUpdateResource(Integer ficaNodeId,
      org.apache.hadoop.yarn.api.records.Resource res) {
    if (toUpdateResources == null) {
      toUpdateResources =
          new HashMap<Integer, org.apache.hadoop.yarn.api.records.Resource>(3);
    }
    toUpdateResources.put(ficaNodeId, res);
  }

  public boolean isAddRMContainer() {
    return addRMContainer;
  }

  public void addRMContainer(boolean remove) {
    addRMContainer = remove;
  }

  public void toRemoveRMContainer(
      org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer) {
    reservedRMContainerToRemove = rmContainer;
  }

  public void toAddLaunchedContainers(String cid, String rmcon) {
    if (launchedContainersToAdd == null) {
      launchedContainersToAdd = new HashMap<String, String>();
    }
    launchedContainersToAdd.put(cid, rmcon);
  }

  public void toRemoveLaunchedContainers(String cid) {
    if (launchedContainersToRemove == null) {
      launchedContainersToRemove = new HashSet<String>();
    }
    launchedContainersToRemove.add(cid);
  }

  public void infoToUpdate(
      org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    infoToUpdate = node;
  }

  public void persistToUpdateResources(ResourceDataAccess resourceDA)
      throws StorageException {
    if (toUpdateResources != null) {
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      for (Integer type : toUpdateResources.keySet()) {
        toAddResources.add(new Resource(id, type, Resource.FICASCHEDULERNODE,
            toUpdateResources.get(type).getMemory(),
            toUpdateResources.get(type).getVirtualCores()));
      }
      resourceDA.addAll(toAddResources);
    }
  }

  public void persistToUpdateFiCaSchedulerNodeId(ResourceDataAccess resourceDA,
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    persistLaunchedContainersToAdd(launchedContainersDA);
    persistLaunchedContainersToRemove(launchedContainersDA);
    persistToUpdateResources(resourceDA);
  }

  private void persistRmContainerToRemove(RMContainerDataAccess rmcontainerDA)
      throws StorageException {
    if (reservedRMContainerToRemove != null) {
      ArrayList<RMContainer> rmcontainerToRemove = new ArrayList<RMContainer>();
      LOG.debug("remove container " +
          reservedRMContainerToRemove.getContainer().getId().toString());
      rmcontainerToRemove.add(new RMContainer(
          reservedRMContainerToRemove.getContainer().getId().toString(),
          reservedRMContainerToRemove.getApplicationAttemptId().toString(),
          reservedRMContainerToRemove.getNodeId().toString(),
          reservedRMContainerToRemove.getUser(),
          reservedRMContainerToRemove.getStartTime(),
          reservedRMContainerToRemove.getFinishTime(),
          reservedRMContainerToRemove.getState().toString(),
          ((RMContainerImpl) reservedRMContainerToRemove).getContainerState()
              .toString(), ((RMContainerImpl) reservedRMContainerToRemove)
          .getContainerExitStatus()));
      rmcontainerDA.removeAll(rmcontainerToRemove);
    }
  }

  private void persistLaunchedContainersToAdd(
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    if (launchedContainersToAdd != null) {
      ArrayList<LaunchedContainers> toAddLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToAdd.keySet()) {
        if (launchedContainersToRemove == null || !launchedContainersToRemove.
            remove(key)) {

          String val = launchedContainersToAdd.get(key);
          LOG.debug("adding LaunchedContainers " + id + " " + key);
          toAddLaunchedContainers.add(new LaunchedContainers(id, key, val));
        }
      }
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  private void persistLaunchedContainersToRemove(
      LaunchedContainersDataAccess launchedContainersDA)
      throws StorageException {
    if (launchedContainersToRemove != null) {
      ArrayList<LaunchedContainers> toRemoveLaunchedContainers =
          new ArrayList<LaunchedContainers>();
      for (String key : launchedContainersToRemove) {
        LOG.debug("remove LaunchedContainers " + id + " " + key);
        toRemoveLaunchedContainers.add(new LaunchedContainers(id, key, null));
      }
      launchedContainersDA.removeAll(toRemoveLaunchedContainers);
    }
  }


  public void persistToUpdateFicaSchedulerNode(
      FiCaSchedulerNodeDataAccess ficaNodeDA,
      RMContainerDataAccess rmcontainerDA) throws StorageException {
    if (infoToUpdate != null) {

      ficaNodeDA.add(new FiCaSchedulerNode(infoToUpdate.getNodeID().
          toString(), infoToUpdate.getNodeName(), infoToUpdate.
          getNumContainers()));

      if (addRMContainer) {
        rmcontainerDA.add(new RMContainer(
            infoToUpdate.getReservedContainer().getContainerId().toString(),
            infoToUpdate.getReservedContainer().getApplicationAttemptId()
                .toString(),
            infoToUpdate.getReservedContainer().getNodeId().toString(),
            infoToUpdate.getReservedContainer().getUser(),
            infoToUpdate.getReservedContainer().getStartTime(),
            infoToUpdate.getReservedContainer().getFinishTime(),
            infoToUpdate.getReservedContainer().toString(),
            ((RMContainerImpl) infoToUpdate.getReservedContainer())
                .getContainerState().toString(),
            ((RMContainerImpl) infoToUpdate.getReservedContainer())
                .getContainerExitStatus()));
      }
      persistRmContainerToRemove(rmcontainerDA);
    }
  }

}
