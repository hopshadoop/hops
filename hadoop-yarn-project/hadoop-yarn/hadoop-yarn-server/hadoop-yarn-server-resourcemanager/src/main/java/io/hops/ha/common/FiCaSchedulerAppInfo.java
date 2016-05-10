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
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppReservedContainerInfo;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * Contains data structures of FiCaSchedulerApp.
 */
public class FiCaSchedulerAppInfo {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerAppInfo.class);
  private final TransactionStateImpl transactionState;
  private AppSchedulingInfo fiCaSchedulerAppToAdd;
  private Map<Integer, Resource> resourcesToUpdate
          = new HashMap<Integer, Resource>();
  private boolean create = false;
  
  protected ApplicationAttemptId applicationAttemptId;

  private Map<Integer, ResourceRequest> requestsToAdd
          = new HashMap<Integer, ResourceRequest>();
  private Map<Integer, ResourceRequest> requestsToRemove
          = new HashMap<Integer, ResourceRequest>();

  private Map<Integer, Resource> toUpdateResources
          = new HashMap<Integer, Resource>(3);

  private Set<String> blacklistToAdd = new HashSet<String>();
  private Set<String> blacklistToRemove = new HashSet<String>();

  private boolean remove = false;

  private Map<ContainerId, FiCaSchedulerAppReservedContainerInfo> reservedContainersToAdd
          = new HashMap<ContainerId, FiCaSchedulerAppReservedContainerInfo>();
  private Map<ContainerId, FiCaSchedulerAppReservedContainerInfo> reservedContainersToRemove
          = new HashMap<ContainerId, FiCaSchedulerAppReservedContainerInfo>();

  private Map<Priority, Long> lastScheduledContainerToAdd = new HashMap<Priority, Long>();;

  Multiset<Priority> schedulingOpportunitiesToAdd;
  Multiset<Priority> reReservations;

  // Not used by fifo scheduler
  public void agregate(AgregatedAppInfo agregate) throws StorageException {
    agregateApplicationToAdd(agregate);
    //connector.flush();
    agregateReservedContainersToAdd(agregate);
    agregateReservedContainersToRemove(agregate);
    agregateLastScheduledContainersToAdd(agregate);
    agregateSchedulingOpportunitiesToAdd(agregate);
    agregateReReservations(agregate);
    agregateRequestsToAdd(agregate);
    agregateRequestsToRemove(agregate);
    agregateBlackListsToAdd(agregate);
    agregateBlackListsToRemove(agregate);
    agregateToUpdateResources(agregate);
    agregateRemoval(agregate);
  }

  public FiCaSchedulerAppInfo(ApplicationAttemptId applicationAttemptId,
          TransactionStateImpl transactionState) {
    this.transactionState = transactionState;
    this.applicationAttemptId = applicationAttemptId;
  }

  public void createFull(SchedulerApplicationAttempt schedulerApp){
    this.create = true;
    updateFull(schedulerApp);
  }
  
  public void updateFull(SchedulerApplicationAttempt schedulerApp) {
    fiCaSchedulerAppToAdd = new AppSchedulingInfo(applicationAttemptId.
            toString(),
            applicationAttemptId.getApplicationId().toString(),
            schedulerApp.getQueueName(),
            schedulerApp.getUser(),
            schedulerApp.getLastContainerId(),
            schedulerApp.isPending(),
            schedulerApp.isStopped());

    //Persist Resources here
    if (schedulerApp.getCurrentConsumption() != null) {
      resourcesToUpdate.put(Resource.CURRENTCONSUMPTION, new Resource(
              applicationAttemptId.toString(),
              Resource.CURRENTCONSUMPTION, Resource.SCHEDULERAPPLICATIONATTEMPT,
              schedulerApp.getCurrentConsumption().getMemory(),
              schedulerApp.getCurrentConsumption().getVirtualCores(), 0));
    }
    if (schedulerApp.getCurrentReservation() != null) {
      resourcesToUpdate.put(Resource.CURRENTRESERVATION, new Resource(
              applicationAttemptId.toString(),
              Resource.CURRENTRESERVATION, Resource.SCHEDULERAPPLICATIONATTEMPT,
              schedulerApp.getCurrentReservation().getMemory(),
              schedulerApp.getCurrentReservation().getVirtualCores(), 0));
    }
    if (schedulerApp.getResourceLimit() != null) {
      resourcesToUpdate.put(Resource.RESOURCELIMIT, new Resource(
              applicationAttemptId.toString(),
              Resource.RESOURCELIMIT, Resource.SCHEDULERAPPLICATIONATTEMPT,
              schedulerApp.getResourceLimit().getMemory(),
              schedulerApp.getResourceLimit().getVirtualCores(), 0));
    }
    if (!schedulerApp.getLastScheduledContainer().isEmpty()) {
      lastScheduledContainerToAdd.putAll(
              schedulerApp.getLastScheduledContainer());
    }
    if (!schedulerApp.getAppSchedulingInfo().getBlackList()
            .isEmpty()) {
      blacklistToAdd.addAll(
              schedulerApp.getAppSchedulingInfo().getBlackList());
    }

    remove = false;
  }

  public void updateAppInfo(SchedulerApplicationAttempt schedulerApp) {
    this.applicationAttemptId = schedulerApp.getApplicationAttemptId();
    fiCaSchedulerAppToAdd = new AppSchedulingInfo(applicationAttemptId.
            toString(),
            applicationAttemptId.getApplicationId().toString(),
            schedulerApp.getQueueName(),
            schedulerApp.getUser(),
            schedulerApp.getLastContainerId(),
            schedulerApp.isPending(),
            schedulerApp.isStopped());
  }

  public void toUpdateResource(Integer type,
          org.apache.hadoop.yarn.api.records.Resource res) {

    toUpdateResources.put(type, new Resource(applicationAttemptId.toString(),
            type,
            Resource.SCHEDULERAPPLICATIONATTEMPT,
            res.getMemory(),
            res.getVirtualCores(), 0));
  }

  public void setRequestsToAdd(
          org.apache.hadoop.yarn.api.records.ResourceRequest val) {
    ResourceRequest hopResourceRequest = new ResourceRequest(
            applicationAttemptId.toString(),
            val.getPriority().getPriority(), val.getResourceName(),
            ((ResourceRequestPBImpl) val).getProto().toByteArray());
    this.requestsToAdd.put(hopResourceRequest.hashCode(), hopResourceRequest);
    requestsToRemove.remove(hopResourceRequest);
  }

  public void setRequestsToRemove(
          org.apache.hadoop.yarn.api.records.ResourceRequest val) {
    ResourceRequest hopResourceRequest = new ResourceRequest(
            applicationAttemptId.toString(),
            val.getPriority().getPriority(), val.getResourceName(),
            ((ResourceRequestPBImpl) val).getProto().toByteArray());
    if (requestsToAdd.remove(hopResourceRequest.hashCode()) == null) {
      this.requestsToRemove.put(hopResourceRequest.hashCode(),
              hopResourceRequest);
    }
  }

  public void setBlacklistToRemove(List<String> blacklistToRemove) {
      for(String name: blacklistToRemove){
        if (!blacklistToAdd.remove(name)) {
          this.blacklistToRemove.add(name);
        }
      }
  }

  public void setBlacklistToAdd(Collection<String> set) {
    this.blacklistToAdd.addAll(set);
    this.blacklistToRemove.removeAll(set);
  }

  private byte[] getRMContainerBytes(
          org.apache.hadoop.yarn.api.records.Container Container) {
    if (Container instanceof ContainerPBImpl) {
      return ((ContainerPBImpl) Container).getProto()
              .toByteArray();
    } else {
      return new byte[0];
    }
  }


  private void agregateRequestsToAdd(AgregatedAppInfo agregate) throws
          StorageException {
    if (requestsToAdd != null) {
      //Persist AppSchedulingInfo requests map and ResourceRequest
      List<ResourceRequest> toAddResourceRequests
              = new ArrayList<ResourceRequest>();
      for (ResourceRequest key : requestsToAdd.values()) {
        toAddResourceRequests.add(key);
      }
      agregate.addAllResourceRequest(toAddResourceRequests);
    }
  }

  private void agregateRequestsToRemove(AgregatedAppInfo agregate) throws
          StorageException {
    if (requestsToRemove != null && !requestsToRemove.isEmpty()) {
      //Remove AppSchedulingInfo requests map and ResourceRequest
      List<ResourceRequest> toRemoveResourceRequests
              = new ArrayList<ResourceRequest>();
      for (ResourceRequest key : requestsToRemove.values()) {
        toRemoveResourceRequests.add(key);
      }
      agregate.addAllResourceRequestsToRemove(toRemoveResourceRequests);
    }
  }

  private void agregateApplicationToAdd(AgregatedAppInfo agregate) throws
          StorageException {
    if (fiCaSchedulerAppToAdd != null && !remove) {
      agregate.addFiCaSchedulerApp(fiCaSchedulerAppToAdd);

      if (!resourcesToUpdate.isEmpty()) {

        agregate.addAllResources(resourcesToUpdate.values());
      }
    }
  }

  private void agregateBlackListsToAdd(AgregatedAppInfo agregate) throws
          StorageException {
    if (blacklistToAdd != null) {
      List<AppSchedulingInfoBlacklist> toAddblackListed
              = new ArrayList<AppSchedulingInfoBlacklist>();
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
      agregate.addAllBlackListToAdd(toAddblackListed);
    }
  }

  private void agregateBlackListsToRemove(AgregatedAppInfo agregate) throws
          StorageException {
    if (blacklistToRemove != null && !blacklistToRemove.isEmpty()) {
      List<AppSchedulingInfoBlacklist> toRemoveblackListed
              = new ArrayList<AppSchedulingInfoBlacklist>();
      for (String blackList : blacklistToRemove) {
        AppSchedulingInfoBlacklist hopBlackList =
            new AppSchedulingInfoBlacklist(applicationAttemptId.
                        toString(), blackList);
        LOG.debug("remove BlackLists " + hopBlackList);
        toRemoveblackListed.add(hopBlackList);
      }
      agregate.addAllBlackListToRemove(toRemoveblackListed);
    }
  }

  private void agregateToUpdateResources(AgregatedAppInfo agregate) throws
          StorageException {
    if (toUpdateResources != null) {
      ArrayList<Resource> toAddResources = new ArrayList<Resource>();
      for (Resource resource : toUpdateResources.values()) {
        toAddResources.add(resource);
      }
      agregate.addAllResources(toAddResources);
    }
  }

  public void remove(SchedulerApplicationAttempt schedulerApp) {
    if (fiCaSchedulerAppToAdd == null || !create) {
      remove = true;
      fiCaSchedulerAppToAdd = new AppSchedulingInfo(applicationAttemptId.
              toString());

      if (schedulerApp.getCurrentConsumption() != null) {
        resourcesToUpdate.put(Resource.CURRENTCONSUMPTION, new Resource(
                applicationAttemptId.toString(),
                Resource.CURRENTCONSUMPTION,
                Resource.SCHEDULERAPPLICATIONATTEMPT,
                schedulerApp.getCurrentConsumption().getMemory(),
                schedulerApp.getCurrentConsumption().getVirtualCores(), 0));
      }
      if (schedulerApp.getCurrentReservation() != null) {
        resourcesToUpdate.put(Resource.CURRENTRESERVATION, new Resource(
                applicationAttemptId.toString(),
                Resource.CURRENTRESERVATION,
                Resource.SCHEDULERAPPLICATIONATTEMPT,
                schedulerApp.getCurrentReservation().getMemory(),
                schedulerApp.getCurrentReservation().getVirtualCores(), 0));
      }
      if (schedulerApp.getResourceLimit() != null) {
        resourcesToUpdate.put(Resource.RESOURCELIMIT, new Resource(
                applicationAttemptId.toString(),
                Resource.RESOURCELIMIT, Resource.SCHEDULERAPPLICATIONATTEMPT,
                schedulerApp.getResourceLimit().getMemory(),
                schedulerApp.getResourceLimit().getVirtualCores(), 0));
      }
    } else {
      fiCaSchedulerAppToAdd = null;
    }
  }

  private void agregateRemoval(AgregatedAppInfo agregate) throws
          StorageException {
    if (remove) {

      agregate.addFiCaSchedulerAppToRemove(fiCaSchedulerAppToAdd);

      agregate.addAllResourcesToRemove(resourcesToUpdate.values());
    }
  }

  private FiCaSchedulerAppReservedContainerInfo convertToFiCaSchedulerAppReservedContainerInfo(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer) {
    FiCaSchedulerAppReservedContainers fiCaContainer
            = new FiCaSchedulerAppReservedContainers(
                    applicationAttemptId.toString(),
                    rmContainer.getReservedPriority().getPriority(),
                    rmContainer.getReservedNode().toString(),
                    rmContainer.getContainerId().toString());

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
    io.hops.metadata.yarn.entity.RMContainer hopRMContainer
            = new io.hops.metadata.yarn.entity.RMContainer(
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
                    ((RMContainerImpl) rmContainer).getContainerState().
                    toString(),
                    ((RMContainerImpl) rmContainer).getContainerExitStatus()
            );

//     
    Container hopContainer = new Container(rmContainer.getContainerId().
            toString(),
            getRMContainerBytes(rmContainer.getContainer()));

    return new FiCaSchedulerAppReservedContainerInfo(fiCaContainer,
            hopRMContainer, hopContainer);
  }

  public void addReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer rmContainer) {

    FiCaSchedulerAppReservedContainerInfo reservedContainerInfo
            = convertToFiCaSchedulerAppReservedContainerInfo(rmContainer);

    reservedContainersToAdd.put(rmContainer.getContainerId(),
            reservedContainerInfo);
    transactionState.addRMContainerToUpdate((RMContainerImpl) rmContainer);
    reservedContainersToRemove.remove(rmContainer.getContainerId());
  }

  protected void agregateReservedContainersToAdd(AgregatedAppInfo agregate)
          throws StorageException {
    if (reservedContainersToAdd != null) {
      List<FiCaSchedulerAppReservedContainers> toAddReservedContainers
              = new ArrayList<FiCaSchedulerAppReservedContainers>();

      List<Container> toAddContainers = new ArrayList<Container>();

      for (FiCaSchedulerAppReservedContainerInfo rmContainer
              : reservedContainersToAdd.values()) {
        toAddReservedContainers.add(rmContainer.getFiCaContainer());
        toAddContainers.add(rmContainer.getHopContainer());
      }
      agregate.addAllReservedContainers(toAddReservedContainers);
    }
  }

  public void removeReservedContainer(
          org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer cont,
          TransactionState ts) {
    if (reservedContainersToAdd.remove(cont.getContainerId()) == null) {
      FiCaSchedulerAppReservedContainerInfo reservedContainerInfo
              = convertToFiCaSchedulerAppReservedContainerInfo(cont);
      reservedContainersToRemove.put(cont.getContainerId(),
              reservedContainerInfo);
      ((TransactionStateImpl)ts).addRMContainerToRemove(cont);
    }
  }

  protected void agregateReservedContainersToRemove(AgregatedAppInfo agregate)
          throws StorageException {
    if (reservedContainersToRemove != null && !reservedContainersToRemove.
            isEmpty()) {
      List<FiCaSchedulerAppReservedContainers> toRemoveReservedContainers
              = new ArrayList<FiCaSchedulerAppReservedContainers>();

      List<io.hops.metadata.yarn.entity.RMContainer> toRemoveRMContainers
              = new ArrayList<io.hops.metadata.yarn.entity.RMContainer>();

      for (FiCaSchedulerAppReservedContainerInfo rmContainer
              : reservedContainersToRemove.values()) {
        toRemoveReservedContainers.add(rmContainer.getFiCaContainer());
        toRemoveRMContainers.add(rmContainer.getHopRMContainer());

      }
      agregate.addAllReservedContainersToRemove(toRemoveReservedContainers);
    }
  }

  public void addLastScheduledContainer(Priority p, long time) {
    lastScheduledContainerToAdd.put(p, time);
  }

  protected void agregateLastScheduledContainersToAdd(AgregatedAppInfo agregate)
          throws StorageException {
    if (lastScheduledContainerToAdd != null) {
      List<FiCaSchedulerAppLastScheduledContainer> toAddLastScheduledCont
              = new ArrayList<FiCaSchedulerAppLastScheduledContainer>();

      for (Priority p : lastScheduledContainerToAdd.keySet()) {
        //Persist lastScheduledContainers
        toAddLastScheduledCont.add(new FiCaSchedulerAppLastScheduledContainer(
                applicationAttemptId.toString(), p.getPriority(),
                lastScheduledContainerToAdd.get(p)));
      }
      agregate.addAllLastScheduerContainersToAdd(toAddLastScheduledCont);
    }
  }

  public void addSchedulingOppurtunity(Priority p, int count) {
    if (schedulingOpportunitiesToAdd == null) {
      schedulingOpportunitiesToAdd = HashMultiset.create();
    }
    schedulingOpportunitiesToAdd.setCount(p, count);
  }

  protected void agregateSchedulingOpportunitiesToAdd(AgregatedAppInfo agregate)
          throws StorageException {
    if (schedulingOpportunitiesToAdd != null) {
      List<FiCaSchedulerAppSchedulingOpportunities> toAddSO
              = new ArrayList<FiCaSchedulerAppSchedulingOpportunities>();

      for (Priority p : schedulingOpportunitiesToAdd.elementSet()) {
        toAddSO.add(new FiCaSchedulerAppSchedulingOpportunities(
                applicationAttemptId.toString(), p.
                getPriority(), schedulingOpportunitiesToAdd.count(p)));
      }
      agregate.addAllSchedulingOportunitiesToAdd(toAddSO);
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

  protected void agregateReReservations(AgregatedAppInfo agregate) throws
          StorageException {
    if (reReservations != null) {
      List<SchedulerAppReservations> toAddReservations
              = new ArrayList<SchedulerAppReservations>();

      for (Priority p : reReservations.elementSet()) {
        toAddReservations.add(new SchedulerAppReservations(applicationAttemptId.
                toString(), p.
                getPriority(), reReservations.count(p)));
      }
      agregate.addAllReReservateion(toAddReservations);
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
