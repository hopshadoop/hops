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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLiveContainers;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppNewlyAllocatedContainers;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerAppReservations;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * Contains data structures of FiCaSchedulerApp.
 */
public class FiCaSchedulerAppInfo {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerAppInfo.class);
  private SchedulerApplicationAttempt fiCaSchedulerAppToAdd;
  protected ApplicationAttemptId applicationAttemptId;
  private Map<ContainerId, org.apache.hadoop.yarn.server.resourcemanager.
      rmcontainer.RMContainer> liveContainersToAdd = new HashMap<ContainerId, org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer>();
  private Map<ContainerId, org.apache.hadoop.yarn.server.resourcemanager.
      rmcontainer.RMContainer> liveContainersToRemove;
  private List<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.
      RMContainer> newlyAllocatedContainersToAdd;
  private List<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.
      RMContainer> newlyAllocatedContainersToRemove;

  private List<org.apache.hadoop.yarn.api.records.ResourceRequest>
      requestsToAdd;
  private List<org.apache.hadoop.yarn.api.records.ResourceRequest>
      requestsToRemove;

  private Map<Integer, org.apache.hadoop.yarn.api.records.Resource>
      toUpdateResources;

  private List<String> blacklistToAdd = new ArrayList<String>();
  private List<String> blacklistToRemove;

  private boolean update = false;
  private boolean remove = false;

  private List<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer> reservedContainersToAdd;
  private List<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer> reservedContainersToRemove;

  private Map<Priority, Long> lastScheduledContainerToAdd;

  Multiset<Priority> schedulingOpportunitiesToAdd;
  Multiset<Priority> reReservations;

  // Not used by fifo scheduler
  public void persist() throws StorageException {
    persistApplicationToAdd();
    persistReservedContainersToAdd();
    persistReservedContainersToRemove();
    persistLastScheduledContainersToAdd();
    persistSchedulingOpportunitiesToAdd();
    persistReReservations();
    persistNewlyAllocatedContainersToAdd();
    persistNewlyAllocatedContainersToRemove();
    persistLiveContainersToAdd();
    persistLiveContainersToRemove();
    persistRequestsToAdd();
    persistRequestsToRemove();
    persistBlackListsToAdd();
    persistBlackListsToRemove();

    persistToUpdateResources();
    persistRemoval();
  }

  public FiCaSchedulerAppInfo(ApplicationAttemptId applicationAttemptId) {
    this.applicationAttemptId = applicationAttemptId;
  }

  public FiCaSchedulerAppInfo(SchedulerApplicationAttempt schedulerApp) {
    this.fiCaSchedulerAppToAdd = schedulerApp;
    this.applicationAttemptId = schedulerApp.getApplicationAttemptId();
  }

  public void updateAppInfo(SchedulerApplicationAttempt schedulerApp) {
    this.fiCaSchedulerAppToAdd = schedulerApp;
    this.applicationAttemptId = schedulerApp.getApplicationAttemptId();
    update = true;
  }

  public void toUpdateResource(Integer ficaAppId,
      org.apache.hadoop.yarn.api.records.Resource res) {
    if (toUpdateResources == null) {
      toUpdateResources =
          new HashMap<Integer, org.apache.hadoop.yarn.api.records.Resource>(3);
    }
    toUpdateResources.put(ficaAppId, res);
  }

  public void setRequestsToAdd(
      org.apache.hadoop.yarn.api.records.ResourceRequest val) {
    if (this.requestsToAdd == null) {
      this.requestsToAdd =
          new ArrayList<org.apache.hadoop.yarn.api.records.ResourceRequest>();
    }
    this.requestsToAdd.add(val);
  }

  public void setRequestsToRemove(
      org.apache.hadoop.yarn.api.records.ResourceRequest val) {
    if (this.requestsToRemove == null) {
      this.requestsToRemove =
          new ArrayList<org.apache.hadoop.yarn.api.records.ResourceRequest>();
    }
    this.requestsToRemove.add(val);
  }

  public void setBlacklistToRemove(List<String> blacklistToRemove) {
    this.blacklistToRemove = blacklistToRemove;
  }

  public void setBlacklistToAdd(Collection<String> set) {
    if (this.blacklistToAdd == null) {
      this.blacklistToAdd = new ArrayList<String>();
    }
    this.blacklistToAdd.addAll(set);
  }

  public void setLiveContainersToAdd(ContainerId key, org.apache.hadoop.yarn.
      server.resourcemanager.rmcontainer.RMContainer val) {
    if (this.liveContainersToAdd == null) {
      this.liveContainersToAdd = new HashMap<ContainerId, org.apache.hadoop.
          yarn.server.resourcemanager.rmcontainer.RMContainer>();
    }
    this.liveContainersToAdd.put(key, val);
  }

  public void setLiveContainersToRemove(org.apache.hadoop.yarn.server.
      resourcemanager.rmcontainer.RMContainer rmc) {
    if (this.liveContainersToRemove == null) {
      liveContainersToRemove = new HashMap<ContainerId, org.apache.hadoop.yarn.
          server.resourcemanager.rmcontainer.RMContainer>();
    }
    liveContainersToRemove.put(rmc.getContainerId(), rmc);
  }

  public void setNewlyAllocatedContainersToAdd(org.apache.hadoop.yarn.server.
      resourcemanager.rmcontainer.RMContainer rmContainer) {
    if (this.newlyAllocatedContainersToAdd == null) {
      this.newlyAllocatedContainersToAdd = new ArrayList<org.apache.hadoop.yarn.
          server.resourcemanager.rmcontainer.RMContainer>();
    }
    this.newlyAllocatedContainersToAdd.add(rmContainer);
  }

  public void setNewlyAllocatedContainersToRemove(org.apache.hadoop.yarn.server.
      resourcemanager.rmcontainer.RMContainer rmContainer) {
    if (this.newlyAllocatedContainersToRemove == null) {
      this.newlyAllocatedContainersToRemove = new ArrayList<org.apache.hadoop.
          yarn.server.resourcemanager.rmcontainer.RMContainer>();
    }
    this.newlyAllocatedContainersToRemove.add(rmContainer);
  }

  private void persistRequestsToAdd() throws StorageException {
    if (requestsToAdd != null) {
      //Persist AppSchedulingInfo requests map and ResourceRequest
      ResourceRequestDataAccess resRequestDA =
          (ResourceRequestDataAccess) RMStorageFactory
              .getDataAccess(ResourceRequestDataAccess.class);
      List<ResourceRequest> toAddResourceRequests =
          new ArrayList<ResourceRequest>();
      for (org.apache.hadoop.yarn.api.records.ResourceRequest key :
          requestsToAdd) {
        if (requestsToRemove == null || !requestsToRemove.remove(key)) {
          ResourceRequest hopResourceRequest =
              new ResourceRequest(applicationAttemptId.toString(),
                  key.getPriority().getPriority(), key.getResourceName(),
                  ((ResourceRequestPBImpl) key).getProto().toByteArray());
          LOG.debug(
              "adding ha_resourcerequest " + applicationAttemptId.toString());
          toAddResourceRequests.add(hopResourceRequest);
        }
      }
      resRequestDA.addAll(toAddResourceRequests);
    }
  }

  private void persistRequestsToRemove() throws StorageException {
    if (requestsToRemove != null && !requestsToRemove.isEmpty()) {
      //Remove AppSchedulingInfo requests map and ResourceRequest
      ResourceRequestDataAccess resRequestDA =
          (ResourceRequestDataAccess) RMStorageFactory
              .getDataAccess(ResourceRequestDataAccess.class);
      List<ResourceRequest> toRemoveResourceRequests =
          new ArrayList<ResourceRequest>();
      for (org.apache.hadoop.yarn.api.records.ResourceRequest key :
          requestsToRemove) {
        ResourceRequest hopResourceRequest =
            new ResourceRequest(applicationAttemptId.toString(),
                key.getPriority().getPriority(), key.getResourceName(),
                ((ResourceRequestPBImpl) key).getProto().toByteArray());
        LOG.debug(
            "remove ResourceRequest " + applicationAttemptId.toString() + " " +
                key.getPriority().getPriority() + " " + key.getResourceName());
        toRemoveResourceRequests.add(hopResourceRequest);

      }
      resRequestDA.removeAll(toRemoveResourceRequests);
    }
  }

  private void persistApplicationToAdd() throws StorageException {
    if (fiCaSchedulerAppToAdd != null && !remove) {
      //Persist ApplicationAttemptId
      //Persist FiCaScheduler App - SchedulerApplicationAttempt instance of SchedulerApplication object
      //Also here we persist the appSchedulingInfo because it is created at the time the FiCaSchedulerApp object is created
      AppSchedulingInfoDataAccess asinfoDA =
          (AppSchedulingInfoDataAccess) RMStorageFactory
              .getDataAccess(AppSchedulingInfoDataAccess.class);
      ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);

      LOG.debug("adding appscheduling info " + applicationAttemptId.toString() +
          " appid " + applicationAttemptId.getApplicationId().toString());
      AppSchedulingInfo hopAppSchedulingInfo =
          new AppSchedulingInfo(applicationAttemptId.toString(),
              applicationAttemptId.getApplicationId().toString(),
              fiCaSchedulerAppToAdd.getQueueName(),
              fiCaSchedulerAppToAdd.getUser(),
              fiCaSchedulerAppToAdd.getLastContainerId(),
              fiCaSchedulerAppToAdd.isPending(),
              fiCaSchedulerAppToAdd.isStopped());
      asinfoDA.add(hopAppSchedulingInfo);

      if (!update) {
        //Persist Resources here
        List<Resource> toAddResources = new ArrayList<Resource>();
        if (fiCaSchedulerAppToAdd.getCurrentConsumption() != null) {
          toAddResources.add(new Resource(applicationAttemptId.toString(),
              Resource.CURRENTCONSUMPTION, Resource.SCHEDULERAPPLICATIONATTEMPT,
              fiCaSchedulerAppToAdd.getCurrentConsumption().getMemory(),
              fiCaSchedulerAppToAdd.getCurrentConsumption().getVirtualCores()));
        }
        if (fiCaSchedulerAppToAdd.getCurrentReservation() != null) {
          toAddResources.add(new Resource(applicationAttemptId.toString(),
              Resource.CURRENTRESERVATION, Resource.SCHEDULERAPPLICATIONATTEMPT,
              fiCaSchedulerAppToAdd.getCurrentReservation().getMemory(),
              fiCaSchedulerAppToAdd.getCurrentReservation().getVirtualCores()));
        }
        if (fiCaSchedulerAppToAdd.getResourceLimit() != null) {
          toAddResources.add(new Resource(applicationAttemptId.toString(),
              Resource.RESOURCELIMIT, Resource.SCHEDULERAPPLICATIONATTEMPT,
              fiCaSchedulerAppToAdd.getResourceLimit().getMemory(),
              fiCaSchedulerAppToAdd.getResourceLimit().getVirtualCores()));
        }
        if (!fiCaSchedulerAppToAdd.getLiveContainers().isEmpty()) {
          liveContainersToAdd
              .putAll(fiCaSchedulerAppToAdd.getLiveContainersMap());
        }
        if (!fiCaSchedulerAppToAdd.getLastScheduledContainer().isEmpty()) {
          lastScheduledContainerToAdd.putAll(
                  fiCaSchedulerAppToAdd.getLastScheduledContainer());
        }
        if (!fiCaSchedulerAppToAdd.getAppSchedulingInfo().getBlackList()
            .isEmpty()) {
          blacklistToAdd.addAll(
              fiCaSchedulerAppToAdd.getAppSchedulingInfo().getBlackList());
        }
        resourceDA.addAll(toAddResources);
      }
    }
  }

  private void persistLiveContainersToAdd() throws StorageException {
    if (liveContainersToAdd != null) {
      //Persist LiveContainers
      RMContainerDataAccess rmcDA = (RMContainerDataAccess) RMStorageFactory
          .getDataAccess(RMContainerDataAccess.class);
      List<RMContainer> toAddRMContainers = new ArrayList<RMContainer>();
      ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory
          .getDataAccess(ContainerDataAccess.class);
      List<Container> toAddContainers = new ArrayList<Container>();
      FiCaSchedulerAppLiveContainersDataAccess fsalcDA =
          (FiCaSchedulerAppLiveContainersDataAccess) RMStorageFactory.
              getDataAccess(FiCaSchedulerAppLiveContainersDataAccess.class);
      List<FiCaSchedulerAppLiveContainers> toAddLiveContainers =
          new ArrayList<FiCaSchedulerAppLiveContainers>();
      for (ContainerId key : liveContainersToAdd.keySet()) {
        if (liveContainersToRemove == null ||
            liveContainersToRemove.remove(key) == null) {
          LOG.debug("adding LiveContainers " + key + " for " +
              applicationAttemptId.toString());
          toAddLiveContainers.add(new FiCaSchedulerAppLiveContainers(
              applicationAttemptId.toString(), key.toString()));
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer
              rmContainer = liveContainersToAdd.get(key);

          toAddRMContainers.add(createRMContainer(rmContainer));

          Container hopContainer =
              new Container(rmContainer.getContainerId().toString(),
                  ((ContainerPBImpl) rmContainer.getContainer()).getProto()
                      .toByteArray());
          LOG.debug("adding ha_container " + hopContainer.getContainerId());
          toAddContainers.add(hopContainer);
        }
      }
      rmcDA.addAll(toAddRMContainers);
      cDA.addAll(toAddContainers);
      fsalcDA.addAll(toAddLiveContainers);
    }
  }

  private void persistLiveContainersToRemove() throws StorageException {
    if (liveContainersToRemove != null && !liveContainersToRemove.isEmpty()) {
      FiCaSchedulerAppLiveContainersDataAccess fsalcDA =
          (FiCaSchedulerAppLiveContainersDataAccess) RMStorageFactory.
              getDataAccess(FiCaSchedulerAppLiveContainersDataAccess.class);
      List<FiCaSchedulerAppLiveContainers> toRemoveLiveContainers =
          new ArrayList<FiCaSchedulerAppLiveContainers>();
      for (ContainerId key : liveContainersToRemove.keySet()) {
        LOG.debug("remove LiveContainers " + key + " for " +
            applicationAttemptId.toString());
        toRemoveLiveContainers.add(
            new FiCaSchedulerAppLiveContainers(applicationAttemptId.toString(),
                key.toString()));
      }
      fsalcDA.removeAll(toRemoveLiveContainers);
    }
  }

  private void persistNewlyAllocatedContainersToAdd() throws StorageException {
    if (newlyAllocatedContainersToAdd != null) {
      //Persist NewllyAllocatedContainers list
      FiCaSchedulerAppNewlyAllocatedContainersDataAccess fsanDA =
          (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
              .getDataAccess(
                  FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
      List<FiCaSchedulerAppNewlyAllocatedContainers>
          toAddNewlyAllocatedContainersList =
          new ArrayList<FiCaSchedulerAppNewlyAllocatedContainers>();
      //Persist RMContainers
      RMContainerDataAccess rmcDA = (RMContainerDataAccess) RMStorageFactory
          .getDataAccess(RMContainerDataAccess.class);
      List<RMContainer> toAddRMContainers = new ArrayList<RMContainer>();
      //Persist Container
      ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory
          .getDataAccess(ContainerDataAccess.class);
      List<Container> toAddContainers = new ArrayList<Container>();

      for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.
          RMContainer rmContainer : newlyAllocatedContainersToAdd) {
        if (newlyAllocatedContainersToRemove == null ||
            !newlyAllocatedContainersToRemove.remove(rmContainer)) {

	    toAddRMContainers.add(createRMContainer(rmContainer));

          Container hopContainer =
              new Container(rmContainer.getContainerId().toString(),
                  ((ContainerPBImpl) rmContainer.getContainer()).getProto()
                      .toByteArray());
          LOG.debug("adding ha_container " + hopContainer.getContainerId());
          toAddContainers.add(hopContainer);

          FiCaSchedulerAppNewlyAllocatedContainers toAdd =
              new FiCaSchedulerAppNewlyAllocatedContainers(
                  applicationAttemptId.toString(),
                  rmContainer.getContainerId().toString());
          LOG.debug("adding newlyAllocatedContainers " +
              applicationAttemptId.toString() + " container " +
              rmContainer.toString());
          toAddNewlyAllocatedContainersList.add(toAdd);
        }
      }
      fsanDA.addAll(toAddNewlyAllocatedContainersList);
      rmcDA.addAll(toAddRMContainers);
      cDA.addAll(toAddContainers);
    }
  }

  private void persistNewlyAllocatedContainersToRemove()
      throws StorageException {
    if (newlyAllocatedContainersToRemove != null) {
      //Remove NewllyAllocatedContainers list
      FiCaSchedulerAppNewlyAllocatedContainersDataAccess fsanDA =
          (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
              .getDataAccess(
                  FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
      List<FiCaSchedulerAppNewlyAllocatedContainers>
          toRemoveNewlyAllocatedContainersList =
          new ArrayList<FiCaSchedulerAppNewlyAllocatedContainers>();


      for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.
          RMContainer rmContainer : newlyAllocatedContainersToRemove) {
        LOG.debug("remove newlyAllocatedContainers " +
            applicationAttemptId.toString() + " container " +
            rmContainer.toString());
        FiCaSchedulerAppNewlyAllocatedContainers toRemove =
            new FiCaSchedulerAppNewlyAllocatedContainers(
                applicationAttemptId.toString(), rmContainer.toString());
        toRemoveNewlyAllocatedContainersList.add(toRemove);
      }
      fsanDA.removeAll(toRemoveNewlyAllocatedContainersList);
    }
  }

  private void persistBlackListsToAdd() throws StorageException {
    if (blacklistToAdd != null) {
      AppSchedulingInfoBlacklistDataAccess blDA =
          (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory.
              getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);
      List<AppSchedulingInfoBlacklist> toAddblackListed =
          new ArrayList<AppSchedulingInfoBlacklist>();
      for (String blackList : blacklistToAdd) {
        if (!blacklistToRemove.remove(blackList)) {
          AppSchedulingInfoBlacklist hopBlackList =
              new AppSchedulingInfoBlacklist(applicationAttemptId.
                  toString(), blackList);
          LOG.debug("adding ha_appschedulinginfo_blacklist " +
              hopBlackList.getAppschedulinginfo_id());
          toAddblackListed.add(hopBlackList);
        }
      }
      blDA.addAll(toAddblackListed);
    }
  }

  private void persistBlackListsToRemove() throws StorageException {
    if (blacklistToRemove != null && !blacklistToRemove.isEmpty()) {
      AppSchedulingInfoBlacklistDataAccess blDA =
          (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory.
              getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);
      List<AppSchedulingInfoBlacklist> toRemoveblackListed =
          new ArrayList<AppSchedulingInfoBlacklist>();
      for (String blackList : blacklistToRemove) {
        AppSchedulingInfoBlacklist hopBlackList =
            new AppSchedulingInfoBlacklist(applicationAttemptId.
                toString(), blackList);
        LOG.debug("remove BlackLists " + hopBlackList);
        toRemoveblackListed.add(hopBlackList);
      }
      blDA.removeAll(toRemoveblackListed);
    }
  }

  private void persistToUpdateResources() throws StorageException {
    if (toUpdateResources != null) {
      ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory
          .getDataAccess(ResourceDataAccess.class);
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      for (Integer type : toUpdateResources.keySet()) {
        toAddResources.add(new Resource(applicationAttemptId.toString(), type,
            Resource.SCHEDULERAPPLICATIONATTEMPT,
            toUpdateResources.get(type).getMemory(),
            toUpdateResources.get(type).getVirtualCores()));
      }
      resourceDA.addAll(toAddResources);
    }
  }

  public void remove(SchedulerApplicationAttempt app) {
    remove = true;
    fiCaSchedulerAppToAdd = app;
  }

  private void persistRemoval() throws StorageException {
    if (remove) {
      AppSchedulingInfoDataAccess asinfoDA =
          (AppSchedulingInfoDataAccess) RMStorageFactory
              .getDataAccess(AppSchedulingInfoDataAccess.class);
      ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory.
          getDataAccess(ResourceDataAccess.class);

      AppSchedulingInfo hopAppSchedulingInfo =
          new AppSchedulingInfo(applicationAttemptId.toString());

      asinfoDA.remove(hopAppSchedulingInfo);

      List<Resource> toRemoveResources = new ArrayList<Resource>();
      if (fiCaSchedulerAppToAdd.getCurrentConsumption() != null) {
        toRemoveResources.add(new Resource(applicationAttemptId.toString(),
            Resource.CURRENTCONSUMPTION, Resource.SCHEDULERAPPLICATIONATTEMPT));
      }
      if (fiCaSchedulerAppToAdd.getCurrentReservation() != null) {
        toRemoveResources.add(new Resource(applicationAttemptId.toString(),
            Resource.CURRENTRESERVATION, Resource.SCHEDULERAPPLICATIONATTEMPT));
      }
      if (fiCaSchedulerAppToAdd.getResourceLimit() != null) {
        toRemoveResources.add(new Resource(applicationAttemptId.toString(),
            Resource.RESOURCELIMIT, Resource.SCHEDULERAPPLICATIONATTEMPT));
      }
      resourceDA.removeAll(toRemoveResources);
    }
  }

  public void addReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer cont) {
    if (reservedContainersToAdd == null) {
      reservedContainersToAdd
              = new ArrayList<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer>();
    }
    reservedContainersToAdd.add(cont);
  }

  protected void persistReservedContainersToAdd() throws StorageException {
    if (reservedContainersToAdd != null) {
      FiCaSchedulerAppReservedContainersDataAccess reservedContDA
              = (FiCaSchedulerAppReservedContainersDataAccess) RMStorageFactory.
              getDataAccess(FiCaSchedulerAppReservedContainersDataAccess.class);
      List<FiCaSchedulerAppReservedContainers> toAddReservedContainers
              = new ArrayList<FiCaSchedulerAppReservedContainers>();

      RMContainerDataAccess rmcDA = (RMContainerDataAccess) RMStorageFactory.
              getDataAccess(RMContainerDataAccess.class);
      List<io.hops.metadata.yarn.entity.RMContainer> toAddRMContainers
              = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();

      ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory.
              getDataAccess(ContainerDataAccess.class);
      List<Container> toAddContainers = new ArrayList<Container>();

      for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer
              : reservedContainersToAdd) {
        //Persist reservedContainers
        toAddReservedContainers.add(new FiCaSchedulerAppReservedContainers(
                applicationAttemptId.toString(),
                rmContainer.getReservedPriority().getPriority(),
                rmContainer.getReservedNode().toString(),
                rmContainer.getContainerId().toString()));

        boolean isReserved = (rmContainer.getReservedNode() != null)
                && (rmContainer.getReservedPriority() != null);

        String reservedNode = isReserved ? rmContainer.getReservedNode().
                toString() : null;
        int reservedPriority = isReserved ? rmContainer.getReservedPriority().
                getPriority() : Integer.MIN_VALUE;
        int reservedMemory = isReserved ? rmContainer.getReservedResource().
                getMemory() : 0;
        int reservedVCores = isReserved ? rmContainer.getReservedResource().
                getVirtualCores() : 0;

        //Persist rmContainer
        toAddRMContainers.add(new io.hops.metadata.yarn.entity.RMContainer(
                rmContainer.getContainerId().toString(),
                rmContainer.getApplicationAttemptId().toString(),
                rmContainer.getNodeId().toString(),
                rmContainer.getUser(),
                reservedNode,
                reservedPriority,
                reservedMemory,
                reservedVCores,
                rmContainer.getStartTime(),
                rmContainer.getFinishTime(),
                rmContainer.getState().toString(),
                ((RMContainerImpl) rmContainer).getContainerState().toString(),
                ((RMContainerImpl) rmContainer).getContainerExitStatus()
        ));

        LOG.debug("Persist reservedContainer " + applicationAttemptId.toString()
                + " containerid " + rmContainer.getContainerId().toString());

        //Persist Container
        Container hopContainer = new Container(rmContainer.getContainerId().
                toString(),
                ((ContainerPBImpl) rmContainer.getContainer()).getProto().
                toByteArray());
        toAddContainers.add(hopContainer);

      }
      reservedContDA.addAll(toAddReservedContainers);
      rmcDA.addAll(toAddRMContainers);
      cDA.addAll(toAddContainers);
    }
  }

  public void removeReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer cont) {
    if (reservedContainersToRemove == null) {
      reservedContainersToRemove
              = new ArrayList<org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer>();
    }
    reservedContainersToRemove.add(cont);
  }

  protected void persistReservedContainersToRemove() throws StorageException {
    if (reservedContainersToRemove != null && !reservedContainersToRemove.
            isEmpty()) {
      FiCaSchedulerAppReservedContainersDataAccess reservedContDA
              = (FiCaSchedulerAppReservedContainersDataAccess) RMStorageFactory.
              getDataAccess(FiCaSchedulerAppReservedContainersDataAccess.class);
      List<FiCaSchedulerAppReservedContainers> toRemoveReservedContainers
              = new ArrayList<FiCaSchedulerAppReservedContainers>();

      RMContainerDataAccess rmcDA = (RMContainerDataAccess) RMStorageFactory.
              getDataAccess(RMContainerDataAccess.class);
      List<io.hops.metadata.yarn.entity.RMContainer> toRemoveRMContainers
              = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();

      ContainerDataAccess cDA = (ContainerDataAccess) RMStorageFactory.
              getDataAccess(ContainerDataAccess.class);

      for (org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer
              : reservedContainersToRemove) {
        //Remove reservedContainers
        toRemoveReservedContainers.add(new FiCaSchedulerAppReservedContainers(
                applicationAttemptId.toString(),
                rmContainer.getReservedPriority().getPriority(),
                rmContainer.getReservedNode().toString(),
                rmContainer.getContainerId().toString()));

        boolean isReserved = (rmContainer.getReservedNode() != null)
                && (rmContainer.getReservedPriority() != null);

        String reservedNode = isReserved ? rmContainer.getReservedNode().
                toString() : null;
        int reservedPriority = isReserved ? rmContainer.getReservedPriority().
                getPriority() : Integer.MIN_VALUE;
        int reservedMemory = isReserved ? rmContainer.getReservedResource().
                getMemory() : 0;
        int reservedVCores = isReserved ? rmContainer.getReservedResource().
                getVirtualCores() : 0;

        //Remove rmContainer
        toRemoveRMContainers.add(new io.hops.metadata.yarn.entity.RMContainer(
                rmContainer.getContainerId().toString(),
                rmContainer.getApplicationAttemptId().toString(),
                rmContainer.getNodeId().toString(),
                rmContainer.getUser(),
                reservedNode,
                reservedPriority,
                reservedMemory,
                reservedVCores,
                rmContainer.getStartTime(),
                rmContainer.getFinishTime(),
                rmContainer.getState().toString(),
                ((RMContainerImpl) rmContainer).getContainerState().toString(),
                ((RMContainerImpl) rmContainer).getContainerExitStatus()
        ));

        LOG.debug("In FairAppInfo " + rmContainer.getContainerId().toString()
                + " " + rmContainer.getState().toString());

      }
      reservedContDA.removeAll(toRemoveReservedContainers);
      rmcDA.removeAll(toRemoveRMContainers);
    }
  }

  public void addLastScheduledContainer(Priority p, long time) {
    if (lastScheduledContainerToAdd == null) {
      lastScheduledContainerToAdd = new HashMap<Priority, Long>();
    }
    lastScheduledContainerToAdd.put(p, time);
  }

  protected void persistLastScheduledContainersToAdd() throws StorageException {
    if (lastScheduledContainerToAdd != null) {
      FiCaSchedulerAppLastScheduledContainerDataAccess lsDA
              = (FiCaSchedulerAppLastScheduledContainerDataAccess) RMStorageFactory.
              getDataAccess(
                      FiCaSchedulerAppLastScheduledContainerDataAccess.class);
      List<FiCaSchedulerAppLastScheduledContainer> toAddLastScheduledCont
              = new ArrayList<FiCaSchedulerAppLastScheduledContainer>();

      for (Priority p : lastScheduledContainerToAdd.keySet()) {
        //Persist lastScheduledContainers
        toAddLastScheduledCont.add(new FiCaSchedulerAppLastScheduledContainer(
                applicationAttemptId.toString(), p.getPriority(),
                lastScheduledContainerToAdd.get(p)));
      }
      lsDA.addAll(toAddLastScheduledCont);
    }
  }

  public void addSchedulingOppurtunity(Priority p, int count) {
    if (schedulingOpportunitiesToAdd == null) {
      schedulingOpportunitiesToAdd = HashMultiset.create();
    }
    schedulingOpportunitiesToAdd.setCount(p, count);
  }

  protected void persistSchedulingOpportunitiesToAdd() throws StorageException {
    if (schedulingOpportunitiesToAdd != null) {
      FiCaSchedulerAppSchedulingOpportunitiesDataAccess soDA
              = (FiCaSchedulerAppSchedulingOpportunitiesDataAccess) RMStorageFactory.
              getDataAccess(
                      FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class);
      List<FiCaSchedulerAppSchedulingOpportunities> toAddSO
              = new ArrayList<FiCaSchedulerAppSchedulingOpportunities>();

      for (Priority p : schedulingOpportunitiesToAdd.elementSet()) {
        toAddSO.add(new FiCaSchedulerAppSchedulingOpportunities(
                applicationAttemptId.toString(), p.
                getPriority(), schedulingOpportunitiesToAdd.count(p)));
      }
      soDA.addAll(toAddSO);
    }
  }

  public void addReReservation(Priority p) {
    if (reReservations == null) {
      reReservations = HashMultiset.create();
    }
    reReservations.add(p);
  }

  public void resetReReservations(Priority p) {
    if (reReservations != null) {
      reReservations.setCount(p, 0);
    }
  }

  protected void persistReReservations() throws StorageException {
    if (reReservations != null) {
      FiCaSchedulerAppReservationsDataAccess reservationsDA
              = (FiCaSchedulerAppReservationsDataAccess) RMStorageFactory.
              getDataAccess(FiCaSchedulerAppReservationsDataAccess.class);
      List<SchedulerAppReservations> toAddReservations
              = new ArrayList<SchedulerAppReservations>();

      for (Priority p : reReservations.elementSet()) {
        toAddReservations.add(new SchedulerAppReservations(applicationAttemptId.
                toString(), p.
                getPriority(), reReservations.count(p)));
      }
      reservationsDA.addAll(toAddReservations);
    }
  }

  private RMContainer createRMContainer(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer) {

    boolean isReserved = (rmContainer.getReservedNode() != null)
            && (rmContainer.getReservedPriority() != null);
    String reservedNode = isReserved ? rmContainer.getReservedNode().
            toString() : null;
    int reservedPriority = isReserved ? rmContainer.getReservedPriority().
            getPriority() : Integer.MIN_VALUE;
    int reservedMemory = isReserved ? rmContainer.getReservedResource().
            getMemory() : 0;
    int reservedVCores = isReserved ? rmContainer.getReservedResource().
            getVirtualCores() : 0;

    RMContainer hopRMContainer = new RMContainer(rmContainer.getContainerId().
            toString(),
            rmContainer.getApplicationAttemptId().toString(),
            rmContainer.getNodeId().toString(), rmContainer.getUser(),
            reservedNode,
            reservedPriority,
            reservedMemory,
            reservedVCores,
            rmContainer.getStartTime(), rmContainer.getFinishTime(),
            rmContainer.getState().toString(),
            ((RMContainerImpl) rmContainer).getContainerState()
            .toString(),
            ((RMContainerImpl) rmContainer).getContainerExitStatus());
    return hopRMContainer;
  }
}
