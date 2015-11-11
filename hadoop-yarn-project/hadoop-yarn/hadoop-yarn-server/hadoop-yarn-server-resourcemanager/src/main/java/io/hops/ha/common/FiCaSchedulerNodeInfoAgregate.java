/*
 * Copyright 2015 Apache Software Foundation.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FiCaSchedulerNodeInfoAgregate {

  public static final Log LOG = LogFactory.getLog(
          FiCaSchedulerNodeInfoAgregate.class);
  private List<FiCaSchedulerNode> infosToUpdate
          = new ArrayList<FiCaSchedulerNode>();
  private List<RMContainer> reservedContainersToRemove
          = new ArrayList<RMContainer>();
  ArrayList<LaunchedContainers> toAddLaunchedContainers
          = new ArrayList<LaunchedContainers>();
  List<LaunchedContainers> toRemoveLaunchedContainers
          = new ArrayList<LaunchedContainers>();
  List<Resource> toUpdateResources = new ArrayList<Resource>();

  public void addToUpdateFiCaSchedulerNode(FiCaSchedulerNode infoToUpdate) {
    infosToUpdate.add(infoToUpdate);
  }

  public void addReservedRMContainerToRemove(
          RMContainer reservedRMContainerToRemove) {
    reservedContainersToRemove.add(reservedRMContainerToRemove);
  }

  public void addAlllaunchedContainersToAdd(
          ArrayList<LaunchedContainers> toAddLaunchedContainers) {
    this.toAddLaunchedContainers.addAll(toAddLaunchedContainers);
  }

  public void addAllLaunchedContainersToRemove(
          ArrayList<LaunchedContainers> toRemoveLaunchedContainers) {
      this.toRemoveLaunchedContainers.addAll(toRemoveLaunchedContainers);
  }

  public void addAllResourcesToUpdate(Collection<Resource> toUpdateResources) {
    this.toUpdateResources.addAll(toUpdateResources);
  }

  public void persist(ResourceDataAccess resourceDA,
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA,
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    persistToUpdateFicaSchedulerNode(ficaNodeDA, rmcontainerDA);
    persistRmContainerToRemove(rmcontainerDA);
    persistLaunchedContainersToAdd(launchedContainersDA);
    persistLaunchedContainersToRemove(launchedContainersDA);
    persistToUpdateResources(resourceDA);
  }

  public void persistToUpdateFicaSchedulerNode(
          FiCaSchedulerNodeDataAccess ficaNodeDA,
          RMContainerDataAccess rmcontainerDA) throws StorageException {
    if (!infosToUpdate.isEmpty()) {

      ficaNodeDA.addAll(infosToUpdate);
    }
  }

  private void persistRmContainerToRemove(RMContainerDataAccess rmcontainerDA)
          throws StorageException {
    if (!reservedContainersToRemove.isEmpty()) {
      rmcontainerDA.removeAll(reservedContainersToRemove);
    }
  }

  private void persistLaunchedContainersToAdd(
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    if (!toAddLaunchedContainers.isEmpty()) {
      launchedContainersDA.addAll(toAddLaunchedContainers);
    }
  }

  private void persistLaunchedContainersToRemove(
          LaunchedContainersDataAccess launchedContainersDA)
          throws StorageException {
    if (!toRemoveLaunchedContainers.isEmpty()) {
      launchedContainersDA.removeAll(toRemoveLaunchedContainers);
    }
  }

  public void persistToUpdateResources(ResourceDataAccess resourceDA)
          throws StorageException {
    if (!toUpdateResources.isEmpty()) {
      resourceDA.addAll(toUpdateResources);
    }
  }
}
