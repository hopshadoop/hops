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

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RMNodeInfoAgregate {

  public static final Log LOG = LogFactory.getLog(RMNodeInfoAgregate.class);
  List<JustLaunchedContainers> toAddJustLaunchedContainers
          = new ArrayList<JustLaunchedContainers>();
  List<ContainerStatus> toAddContainerStatus = new ArrayList<ContainerStatus>();
  List<JustLaunchedContainers> toRemoveJustLaunchedContainers
          = new ArrayList<JustLaunchedContainers>();
  ArrayList<ContainerId> toAddContainerIdToClean = new ArrayList<ContainerId>();
  ArrayList<ContainerId> toRemoveContainerIdToClean
          = new ArrayList<ContainerId>();
  List<FinishedApplications> toAddFinishedApplications
          = new ArrayList<FinishedApplications>();
  Set<PendingEvent> toAddPendingEvents
          = new HashSet<PendingEvent>();
  Set<PendingEvent> toRemovePendingEvents
          = new HashSet<PendingEvent>();
  List<FinishedApplications> toRemoveFinishedApplications
          = new ArrayList<FinishedApplications>();
  ArrayList<UpdatedContainerInfo> uciToAdd
          = new ArrayList<UpdatedContainerInfo>();
  List<UpdatedContainerInfo> uciToRemove = new ArrayList<UpdatedContainerInfo>();
  List<NodeHBResponse> hbResponseToAdd = new ArrayList<NodeHBResponse>();
  List<NextHeartbeat> nextHeartBeatToUpdate = new ArrayList<NextHeartbeat>();

  public void addAllContainersStatusToAdd(
          List<ContainerStatus> toAddContainerStatus) {
    this.toAddContainerStatus.addAll(toAddContainerStatus);
  }

  public void addAllJustLaunchedContainersToAdd(
          List<JustLaunchedContainers> toAddJustLaunchedContainers) {
    this.toAddJustLaunchedContainers.addAll(toAddJustLaunchedContainers);
  }

    public void addAllJustLaunchedContainersToRemove(
          List<JustLaunchedContainers> toRemoveJustLaunchedContainers) {
    this.toRemoveJustLaunchedContainers.addAll(toRemoveJustLaunchedContainers);
  }
    
  public void addAllPendingEventsToAdd(
          ArrayList<PendingEvent> toAddPendingEvents) {
    this.toAddPendingEvents.addAll(toAddPendingEvents);
  }

  public void addAllPendingEventsToRemove(
          ArrayList<PendingEvent> toRemovePendingEvents) {
    this.toRemovePendingEvents.addAll(toRemovePendingEvents);
  }

  public void addAllContainersToCleanToAdd(
          ArrayList<ContainerId> toAddContainerIdToClean) {
    this.toAddContainerIdToClean.addAll(toAddContainerIdToClean);
  }

  public void addAllContainerToCleanToRemove(
          ArrayList<ContainerId> toRemoveContainerIdToClean) {
    this.toRemoveContainerIdToClean.addAll(toRemoveContainerIdToClean);
  }

  public void addAllFinishedAppToAdd(
          ArrayList<FinishedApplications> toAddFinishedApplications) {
    this.toAddFinishedApplications.addAll(toAddFinishedApplications);
  }

  public void addAllFinishedAppToRemove(
          ArrayList<FinishedApplications> toRemoveFinishedApplications) {
    this.toRemoveFinishedApplications.addAll(toRemoveFinishedApplications);
  }

  public void addAllUpdatedContainerInfoToAdd(
          ArrayList<UpdatedContainerInfo> uciToAdd) {
    this.uciToAdd.addAll(uciToAdd);
  }

  public void addAllUpdatedContainerInfoToRemove(
          List<UpdatedContainerInfo> uciToRemove) {
    this.uciToRemove.addAll(uciToRemove);
  }

  public void addLastHeartbeatResponse(NodeHBResponse toAdd) {
    hbResponseToAdd.add(toAdd);
  }

  public void addNextHeartbeat(NextHeartbeat nextHeartbeat) {
    nextHeartBeatToUpdate.add(nextHeartbeat);
  }

  public void persist(NodeHBResponseDataAccess hbDA,
          ContainerIdToCleanDataAccess cidToCleanDA,
          JustLaunchedContainersDataAccess justLaunchedContainersDA,
          UpdatedContainerInfoDataAccess updatedContainerInfoDA,
          FinishedApplicationsDataAccess faDA, ContainerStatusDataAccess csDA,
          PendingEventDataAccess persistedEventsDA, StorageConnector connector)
          throws StorageException {
    persistContainerStatusToAdd(csDA);
    persistJustLaunchedContainersToAdd(justLaunchedContainersDA);
    persistJustLaunchedContainersToRemove(justLaunchedContainersDA);
    persistContainerToCleanToAdd(cidToCleanDA);
    persistContainerToCleanToRemove(cidToCleanDA);
    persistFinishedApplicationToAdd(faDA);
    persistFinishedApplicationToRemove(faDA);
    persistNodeUpdateQueueToAdd(updatedContainerInfoDA);
    persistNodeUpdateQueueToRemove(updatedContainerInfoDA);
    persistLatestHeartBeatResponseToAdd(hbDA);
    persistNextHeartbeat();
    persistPendingEventsToAdd(persistedEventsDA);
    persistPendingEventsToRemove(persistedEventsDA);
  }

  private void persistContainerStatusToAdd(ContainerStatusDataAccess csDA)
          throws StorageException {
    csDA.addAll(toAddContainerStatus);
  }

  public void persistJustLaunchedContainersToAdd(
          JustLaunchedContainersDataAccess justLaunchedContainersDA) throws
          StorageException {

    justLaunchedContainersDA.addAll(toAddJustLaunchedContainers);

  }

  public void persistJustLaunchedContainersToRemove(
          JustLaunchedContainersDataAccess justLaunchedContainersDA)
          throws StorageException {
    justLaunchedContainersDA.removeAll(toRemoveJustLaunchedContainers);
  }

  public void persistContainerToCleanToAdd(
          ContainerIdToCleanDataAccess cidToCleanDA) throws StorageException {
    cidToCleanDA.addAll(toAddContainerIdToClean);
  }

  public void persistContainerToCleanToRemove(
          ContainerIdToCleanDataAccess cidToCleanDA) throws StorageException {
    cidToCleanDA.removeAll(toRemoveContainerIdToClean);
  }

  public void persistFinishedApplicationToAdd(
          FinishedApplicationsDataAccess faDA) throws StorageException {

    faDA.addAll(toAddFinishedApplications);
  }

  public void persistFinishedApplicationToRemove(
          FinishedApplicationsDataAccess faDA) throws StorageException {
    faDA.removeAll(toRemoveFinishedApplications);
  }

  public void persistNodeUpdateQueueToAdd(
          UpdatedContainerInfoDataAccess updatedContainerInfoDA) throws
          StorageException {
    updatedContainerInfoDA.addAll(uciToAdd);
  }

  public void persistNodeUpdateQueueToRemove(
          UpdatedContainerInfoDataAccess updatedContainerInfoDA) throws
          StorageException {
    updatedContainerInfoDA.removeAll(uciToRemove);
  }

  public void persistPendingEventsToAdd(
          PendingEventDataAccess persistedEventsDA) throws
          StorageException {
    persistedEventsDA.addAll(toAddPendingEvents);
  }

  public void persistPendingEventsToRemove(
          PendingEventDataAccess persistedEventsDA) throws
          StorageException {
    persistedEventsDA.removeAll(toRemovePendingEvents);
  }

  public void persistLatestHeartBeatResponseToAdd(NodeHBResponseDataAccess hbDA)
          throws StorageException {
    hbDA.addAll(hbResponseToAdd);
  }

  public void persistNextHeartbeat() throws StorageException {
    NextHeartbeatDataAccess nextHeartbeatDA
            = (NextHeartbeatDataAccess) RMStorageFactory
            .getDataAccess(NextHeartbeatDataAccess.class);
    nextHeartbeatDA.updateAll(nextHeartBeatToUpdate);
  }
}
