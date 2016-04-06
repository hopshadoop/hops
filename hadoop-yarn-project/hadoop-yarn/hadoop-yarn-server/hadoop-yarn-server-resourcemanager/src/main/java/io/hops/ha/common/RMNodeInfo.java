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

import static io.hops.ha.common.TransactionState.pendingEventId;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.PendingEventID;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.metadata.yarn.entity.UpdatedContainerInfoToAdd;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;

public class RMNodeInfo {

  private static final Log LOG = LogFactory.getLog(RMNodeInfo.class);
  private String rmnodeId = null;
  //PersistedEvent to persist for distributed RT
  private final Map<PendingEventID, PendingEvent> persistedEventsToAdd
          = new HashMap<PendingEventID, PendingEvent>();
  private final ArrayList<PendingEvent> PendingEventsToRemove
          = new ArrayList<PendingEvent>();
  private Set<org.apache.hadoop.yarn.api.records.ContainerId>
      containerToCleanToAdd =
          new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
  private Set< org.apache.hadoop.yarn.api.records.ContainerId> containerToCleanToRemove= new TreeSet< org.apache.hadoop.yarn.api.records.ContainerId>();
  private Map<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus>
      justLaunchedContainersToAdd =
          new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus>();
  private Map<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus> justLaunchedContainersToRemove = 
          new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, ContainerStatus>();
  private Map<Integer, UpdatedContainerInfoToAdd> nodeUpdateQueueToAdd
          = new ConcurrentHashMap<Integer, UpdatedContainerInfoToAdd>();
  private Map<Integer, UpdatedContainerInfoToAdd> nodeUpdateQueueToRemove
          = new ConcurrentHashMap<Integer, UpdatedContainerInfoToAdd>();
  private Set<ApplicationId> finishedApplicationsToAdd = new ConcurrentSkipListSet<ApplicationId>();;
  private Set<ApplicationId> finishedApplicationsToRemove = new ConcurrentSkipListSet<ApplicationId>();;
  private NodeHBResponse latestNodeHeartBeatResponse;
  private NextHeartbeat nextHeartbeat;
  private int pendingId = -1;

  public RMNodeInfo(String rmnodeId) {
    this.rmnodeId = rmnodeId;
  }

  public void agregate(RMNodeInfoAgregate agregate){
    agregateJustLaunchedContainersToAdd(agregate);
    agregateJustLaunchedContainersToRemove(agregate);
    agregateContainerToCleanToAdd(agregate);
    agregateContainerToCleanToRemove(agregate);
    agregateFinishedApplicationToAdd(agregate);
    agregateFinishedApplicationToRemove(agregate);
    agregateNodeUpdateQueueToAdd(agregate);
    agregateNodeUpdateQueueToRemove(agregate);
    agregateLatestHeartBeatResponseToAdd(agregate);
    agregateNextHeartbeat(agregate);
    agregatePendingEventsToAdd(agregate);
    agregatePendingEventsToRemove(agregate);
  }

  public String getRmnodeId() {
    return rmnodeId;
  }

  public void toAddJustLaunchedContainers(
          org.apache.hadoop.yarn.api.records.ContainerId key,
          org.apache.hadoop.yarn.api.records.ContainerStatus val) {
    ContainerStatus toAdd = new ContainerStatus(val.getContainerId().toString(),
            val.getState().toString(), val.getDiagnostics(),
            val.getExitStatus(), rmnodeId, getPendingId(),
            ContainerStatus.Type.JUST_LAUNCHED);
    this.justLaunchedContainersToAdd.put(key, toAdd);
    justLaunchedContainersToRemove.remove(key);
  }

  public void toRemoveJustLaunchedContainers(
          org.apache.hadoop.yarn.api.records.ContainerId key,
            org.apache.hadoop.yarn.api.records.ContainerStatus val) {
    if (justLaunchedContainersToAdd.remove(key) == null) {
      ContainerStatus toRemove = new ContainerStatus(val.getContainerId().toString(),
              rmnodeId, ContainerStatus.Type.JUST_LAUNCHED, getPendingId());
      this.justLaunchedContainersToRemove.put(key, toRemove);
    }
  }

  public void toAddContainerToClean(
          org.apache.hadoop.yarn.api.records.ContainerId toAdd) {
    this.containerToCleanToAdd.add(toAdd);
    this.containerToCleanToRemove.remove(toAdd);
  }

  public void toRemoveContainerToClean(
          org.apache.hadoop.yarn.api.records.ContainerId toRemove) {
    if (!containerToCleanToAdd.remove(toRemove)) {
      this.containerToCleanToRemove.add(toRemove);
    }
  }

  public void toAddNodeUpdateQueue(
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci) {

    if (uci.getNewlyLaunchedContainers() != null
            && !uci.getNewlyLaunchedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getNewlyLaunchedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId(), getPendingId());

            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                        containerStatus.getState().toString(),
                        containerStatus.getDiagnostics(),
                        containerStatus.getExitStatus(), rmnodeId, getPendingId(),
                        ContainerStatus.Type.UCI);

        UpdatedContainerInfoToAdd uciToAdd = new UpdatedContainerInfoToAdd(
                hopUCI, hopConStatus);
        this.nodeUpdateQueueToAdd.put(uciToAdd.hashCode(), uciToAdd);
        this.nodeUpdateQueueToRemove.remove(uciToAdd);
      }
    }
    if (uci.getCompletedContainers() != null
            && !uci.getCompletedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getCompletedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId(), getPendingId());

            ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                        containerStatus.getState().toString(),
                        containerStatus.getDiagnostics(),
                        containerStatus.getExitStatus(), rmnodeId, getPendingId(),
                        ContainerStatus.Type.UCI);

        UpdatedContainerInfoToAdd uciToAdd = new UpdatedContainerInfoToAdd(
                hopUCI, hopConStatus);
        this.nodeUpdateQueueToAdd.put(uciToAdd.hashCode(), uciToAdd);
        this.nodeUpdateQueueToRemove.remove(uciToAdd);
      }
    }

  }

  public void toRemoveNodeUpdateQueue(
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci) {

    Set<org.apache.hadoop.yarn.api.records.ContainerId> alreadyRemoved
            = new HashSet<org.apache.hadoop.yarn.api.records.ContainerId>();
    if (uci.getNewlyLaunchedContainers() != null && !uci.
            getNewlyLaunchedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getNewlyLaunchedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId(), getPendingId());

        ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    rmnodeId, ContainerStatus.Type.UCI, getPendingId());
        
        UpdatedContainerInfoToAdd uciToRemove = new UpdatedContainerInfoToAdd(
                hopUCI, hopConStatus);
        UpdatedContainerInfoToAdd flag = this.nodeUpdateQueueToAdd.remove(
                uciToRemove.hashCode());
        if (flag == null && alreadyRemoved.add(containerStatus.getContainerId())) {
          this.nodeUpdateQueueToRemove.put(uciToRemove.hashCode(), uciToRemove);
        }
      }
    }
    if (uci.getCompletedContainers() != null && !uci.getCompletedContainers().
            isEmpty()) {

      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci.getCompletedContainers()) {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(rmnodeId,
                containerStatus.getContainerId().
                toString(), uci.
                getUpdatedContainerInfoId(), getPendingId());

        ContainerStatus hopConStatus =
                new ContainerStatus(containerStatus.getContainerId().toString(),
                    rmnodeId, ContainerStatus.Type.UCI, getPendingId());
                
        UpdatedContainerInfoToAdd uciToRemove = new UpdatedContainerInfoToAdd(
                hopUCI, hopConStatus);
        UpdatedContainerInfoToAdd flag = this.nodeUpdateQueueToAdd.remove(
                uciToRemove.hashCode());
        if (flag == null && alreadyRemoved.add(containerStatus.getContainerId())) {
          this.nodeUpdateQueueToRemove.put(uciToRemove.hashCode(), uciToRemove);
        }
      }
    }
  }

  public void toRemoveNodeUpdateQueue(
          Collection<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> uci) {

    for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo c
            : uci) {
      toRemoveNodeUpdateQueue(c);
    }
  }

  public void toAddFinishedApplications(ApplicationId app) {
    this.finishedApplicationsToAdd.add(app);
    this.finishedApplicationsToRemove.remove(app);
  }


  public void toRemoveFinishedApplications(ApplicationId app) {
    if(!finishedApplicationsToAdd.remove(app)){
      this.finishedApplicationsToRemove.add(app);
    }
  }

  public void agregateContainerToCleanToAdd(RMNodeInfoAgregate agregate) {
    if (containerToCleanToAdd != null) {
      ArrayList<ContainerId> toAddHopContainerIdToClean =
          new ArrayList<ContainerId>(containerToCleanToAdd.size());
      for (org.apache.hadoop.yarn.api.records.ContainerId cid : containerToCleanToAdd) {
        if(!containerToCleanToRemove.remove(cid)){
          toAddHopContainerIdToClean
                  .add(new ContainerId(rmnodeId, cid.toString(), getPendingId()));
        }
      }
      agregate.addAllContainersToCleanToAdd(toAddHopContainerIdToClean);
    }
  }

 

   public void agregateContainerToCleanToRemove(RMNodeInfoAgregate agregate){
    if (containerToCleanToRemove != null) {
      ArrayList<ContainerId> toRemoveHopContainerIdToClean
              = new ArrayList<ContainerId>(containerToCleanToRemove.size());
      for (org.apache.hadoop.yarn.api.records.ContainerId cid
              : containerToCleanToRemove) {
        toRemoveHopContainerIdToClean.add(new ContainerId(rmnodeId, cid.
                toString(), getPendingId()));
      }
      agregate.addAllContainerToCleanToRemove(toRemoveHopContainerIdToClean);
    }
  }



  public void agregateJustLaunchedContainersToAdd(RMNodeInfoAgregate agregate) {
    if (justLaunchedContainersToAdd != null &&
        !justLaunchedContainersToAdd.isEmpty()) {
      List<JustLaunchedContainers> toAddHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      List<ContainerStatus> toAddContainerStatus =
          new ArrayList<ContainerStatus>();
      for (ContainerStatus value : justLaunchedContainersToAdd
              .values()) {

        toAddHopJustLaunchedContainers.add(
                new JustLaunchedContainers(rmnodeId,
                        value.getContainerid()));
        toAddContainerStatus.add(value);
      }
      agregate.addAllContainersStatusToAdd(toAddContainerStatus);
      agregate.addAllJustLaunchedContainersToAdd(toAddHopJustLaunchedContainers);

    }
  }




    public void agregateJustLaunchedContainersToRemove(RMNodeInfoAgregate agregate){
    if (justLaunchedContainersToRemove != null &&
        !justLaunchedContainersToRemove.isEmpty()) {
      List<JustLaunchedContainers> toRemoveHopJustLaunchedContainers =
          new ArrayList<JustLaunchedContainers>();
      List<ContainerStatus> toRemoveContainerStatus =
          new ArrayList<ContainerStatus>();
      for (ContainerStatus value : justLaunchedContainersToRemove.values()) {
        toRemoveHopJustLaunchedContainers
                .add(new JustLaunchedContainers(rmnodeId, 
                        value.getContainerid()));
        toRemoveContainerStatus.add(value);
      }
      agregate.addAllContainersStatusToRemove(toRemoveContainerStatus);
      agregate.addAllJustLaunchedContainersToRemove(toRemoveHopJustLaunchedContainers);
    }
  }




  public void agregateNodeUpdateQueueToAdd(RMNodeInfoAgregate agregate){
    if (nodeUpdateQueueToAdd != null && !nodeUpdateQueueToAdd.isEmpty()) {
      //Add row at ha_updatedcontainerinfo
      ArrayList<UpdatedContainerInfo> uciToAdd
              = new ArrayList<UpdatedContainerInfo>();
      ArrayList<ContainerStatus> containerStatusToAdd
              = new ArrayList<ContainerStatus>();
      for (UpdatedContainerInfoToAdd uci : nodeUpdateQueueToAdd.values()) {
        uciToAdd.add(uci.getUci());
        containerStatusToAdd.add(uci.getContainerStatus());
      }
      agregate.addAllContainersStatusToAdd(containerStatusToAdd);
      agregate.addAllUpdatedContainerInfoToAdd(uciToAdd);
    }
  }

  public void agregateNodeUpdateQueueToRemove(RMNodeInfoAgregate agregate) {
    if (nodeUpdateQueueToRemove != null && !nodeUpdateQueueToRemove.isEmpty()) {
      List<UpdatedContainerInfo> uciToRemove
              = new ArrayList<UpdatedContainerInfo>();
      ArrayList<ContainerStatus> containerStatusToRemove
              = new ArrayList<ContainerStatus>();
      for (UpdatedContainerInfoToAdd uci : nodeUpdateQueueToRemove.values()) {
        uciToRemove.add(uci.getUci());
        containerStatusToRemove.add(uci.getContainerStatus());
      }
      agregate.addAllContainersStatusToRemove(containerStatusToRemove);
      agregate.addAllUpdatedContainerInfoToRemove(uciToRemove);
    }
  }

 


   public void agregateFinishedApplicationToAdd(RMNodeInfoAgregate agregate){
    if (finishedApplicationsToAdd != null && !finishedApplicationsToAdd.
            isEmpty()) {
      ArrayList<FinishedApplications> toAddHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (ApplicationId appId : finishedApplicationsToAdd) {

        FinishedApplications hopFinishedApplications
                = new FinishedApplications(rmnodeId, appId.toString(), getPendingId());
        toAddHopFinishedApplications.add(hopFinishedApplications);

      }
      LOG.debug("Finished_applicatons_by_scheduler: "+toAddHopFinishedApplications.toString());
      agregate.addAllFinishedAppToAdd(toAddHopFinishedApplications);
    }
  }

 

public void agregateFinishedApplicationToRemove(RMNodeInfoAgregate agregate){
    if (finishedApplicationsToRemove != null &&
        !finishedApplicationsToRemove.isEmpty()) {
      ArrayList<FinishedApplications> toRemoveHopFinishedApplications =
          new ArrayList<FinishedApplications>();
      for (ApplicationId appId : finishedApplicationsToRemove) {
        FinishedApplications hopFinishedApplications
                = new FinishedApplications(rmnodeId, appId.toString(), getPendingId());
        toRemoveHopFinishedApplications.add(hopFinishedApplications);
      }
      agregate.addAllFinishedAppToRemove(toRemoveHopFinishedApplications);
    }
  }



  public void toAddLatestNodeHeartBeatResponse(NodeHeartbeatResponse resp) {
    if (resp instanceof NodeHeartbeatResponsePBImpl) {
      try {
        this.latestNodeHeartBeatResponse = new NodeHBResponse(rmnodeId,
                ((NodeHeartbeatResponsePBImpl) resp)
                .getProto().toByteArray());
      } catch (RuntimeException e) {
        //TODO find why we get this error that should never happen
        LOG.error("this should never happen : " + e.getMessage(), e);
        this.latestNodeHeartBeatResponse = new NodeHBResponse(rmnodeId,
                new byte[1]);
      }
    } else {
      this.latestNodeHeartBeatResponse = new NodeHBResponse(rmnodeId,
              new byte[1]);
    }
  }

   public void agregateLatestHeartBeatResponseToAdd(RMNodeInfoAgregate agregate){
    if (latestNodeHeartBeatResponse != null) {
      agregate.addLastHeartbeatResponse(latestNodeHeartBeatResponse);
    }
  }

 

  public void toAddNextHeartbeat(String rmnodeid, boolean nextHeartbeat) {
    this.nextHeartbeat = new NextHeartbeat(rmnodeid, nextHeartbeat, getPendingId());
  }

  public void agregateNextHeartbeat(RMNodeInfoAgregate agregate) {
    if (nextHeartbeat != null) {
      agregate.addNextHeartbeat(nextHeartbeat);
    }
    LOG.debug("HOP :: persistNextHeartbeat-FINISH:" + nextHeartbeat);
  }

  private void generatePendingEventId() {
    //lets start the pending event id from 1
    if(pendingId == -1){
      this.pendingId = pendingEventId.incrementAndGet();
    }
  }

  public void setPendingEventId(int pendingEventId) {
    this.pendingId = pendingEventId;
  }

  public int getPendingId() {
    generatePendingEventId();
    return pendingId;
  }

  public void addPendingEventToAdd(String rmnodeId, int type, int status) {
    PendingEvent pendingEvent;
    if (this.persistedEventsToAdd.isEmpty()) {
      pendingEvent = new PendingEvent(rmnodeId, type, status,
              getPendingId());
    } else {
      pendingEvent = new PendingEvent(rmnodeId, type, status,
              pendingEventId.incrementAndGet());
    }
    this.persistedEventsToAdd.put(pendingEvent.getId(),pendingEvent);
  }

  public void addPendingEventToRemove(PendingEvent pendingEvent) {
    this.PendingEventsToRemove
            .add(new PendingEvent(pendingEvent));
  }
  
  public  ArrayList<PendingEvent> getPendingEventToRemove(){
    return  PendingEventsToRemove;
  }
  
  public void agregatePendingEventsToAdd(RMNodeInfoAgregate agregate) {
    if (persistedEventsToAdd != null
            && !persistedEventsToAdd.isEmpty()) {
      LOG.debug("agregating pending event to add: " + persistedEventsToAdd.size());
      agregate.addAllPendingEventsToAdd(persistedEventsToAdd.values());
    }
  }

  public void agregatePendingEventsToRemove(RMNodeInfoAgregate agregate) {
    if (PendingEventsToRemove != null
            && !PendingEventsToRemove.isEmpty()) {
      agregate.addAllPendingEventsToRemove(PendingEventsToRemove);
    }
  }

}
