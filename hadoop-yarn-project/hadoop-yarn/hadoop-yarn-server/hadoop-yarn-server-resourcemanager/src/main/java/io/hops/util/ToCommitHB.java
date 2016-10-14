/*
 * Copyright 2016 Apache Software Foundation.
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
package io.hops.util;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.transaction.handler.AsyncLightWeightRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;

public class ToCommitHB {
  private static final Log LOG = LogFactory.getLog(ToCommitHB.class);
  private static AtomicInteger nextPendingEventId = new AtomicInteger(0);

  final String nodeId;
  PendingEvent.Type pendingEventType = null;
  PendingEvent.Status pendingEventStatus;
  int pendingEventContains = 0;
  
  final List<io.hops.metadata.yarn.entity.UpdatedContainerInfo> uciToAdd
          = new ArrayList<>();
  final List<ContainerStatus> containerStatusToAdd
          = new ArrayList<>();
  int uciID;
  int pendingEventId;
  RMNode rmNode = null;
  io.hops.metadata.yarn.entity.Resource rmNodeResource = null;
  NextHeartbeat nextHeartBeat = null;

  public ToCommitHB(String nodeId) {
    this.nodeId = nodeId;
    this.pendingEventId = nextPendingEventId.incrementAndGet();
  }

  public void addPendingEvent(PendingEvent.Type pendingEventType,
          PendingEvent.Status pendingEventStatus) {
    this.pendingEventStatus = pendingEventStatus;
    this.pendingEventType = pendingEventType;
    this.pendingEventContains++;
  }
  
  public void addNodeUpdateQueue(UpdatedContainerInfo uci) {
    this.uciID = uci.getUciId();
    if (uci.getNewlyLaunchedContainers() != null
            && !uci.getNewlyLaunchedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getNewlyLaunchedContainers()) {
        io.hops.metadata.yarn.entity.UpdatedContainerInfo hopUCI
                = new io.hops.metadata.yarn.entity.UpdatedContainerInfo(nodeId,
                        containerStatus.getContainerId().
                        toString(), uciID, pendingEventId);
        uciToAdd.add(hopUCI);
        this.pendingEventContains++;
        
        ContainerStatus hopConStatus = new ContainerStatus(containerStatus.
                getContainerId().toString(),
                containerStatus.getState().toString(),
                containerStatus.getDiagnostics(),
                containerStatus.getExitStatus(), nodeId, pendingEventId,
                uciID);
        containerStatusToAdd.add(hopConStatus);
        this.pendingEventContains++;
      }
    }
    if (uci.getCompletedContainers() != null
            && !uci.getCompletedContainers().isEmpty()) {
      for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
              : uci
              .getCompletedContainers()) {
        io.hops.metadata.yarn.entity.UpdatedContainerInfo hopUCI
                = new io.hops.metadata.yarn.entity.UpdatedContainerInfo(nodeId,
                        containerStatus.getContainerId().
                        toString(), uciID, pendingEventId);
        uciToAdd.add(hopUCI);
        this.pendingEventContains++;
        ContainerStatus hopConStatus = new ContainerStatus(containerStatus.
                getContainerId().toString(),
                containerStatus.getState().toString(),
                containerStatus.getDiagnostics(),
                containerStatus.getExitStatus(), nodeId, pendingEventId,
                uciID);
        containerStatusToAdd.add(hopConStatus);
        this.pendingEventContains++;
      }
    }
  }

  public void addRMNode(String hostName, int commandPort, int httpPort,
          Resource totalCapability,
          String nodeManagerVersion, NodeState currentState, String healthReport,
          long lastHealthReportTime) {
    rmNode = new RMNode(nodeId, hostName, commandPort, httpPort,
            healthReport, lastHealthReportTime, currentState.name(),
            nodeManagerVersion, pendingEventId);
    this.pendingEventContains++;
    rmNodeResource = new io.hops.metadata.yarn.entity.Resource(nodeId,
            totalCapability.getMemory(), totalCapability.getVirtualCores(),
            pendingEventId);
    this.pendingEventContains++;
  }

  public void addNextHeartBeat(boolean nextHeartBeat){
    this.nextHeartBeat = new NextHeartbeat(nodeId, nextHeartBeat);
  }
  
  public void commit() throws IOException {
    if (pendingEventType == null) {
      return;
    }
    final PendingEvent pendingEvent = new PendingEvent(nodeId, pendingEventType,
            pendingEventStatus, pendingEventId, pendingEventContains);
    RequestHandler handler = null;

    if (pendingEvent.getType().equals(PendingEvent.Type.NODE_ADDED)) {
      handler = new LightWeightRequestHandler(
              YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.writeLock();
          persistHeartbeat(pendingEvent);
          connector.commit();
          return null;
        }
      };
    } else {
      handler = new AsyncLightWeightRequestHandler(
              YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.writeLock();
          persistHeartbeat(pendingEvent);
          connector.commit();
          return null;
        }
      };

    }

    handler.handle();
  }

  private void persistHeartbeat(PendingEvent pendingEvent) throws StorageException {
    PendingEventDataAccess peDA = (PendingEventDataAccess) RMStorageFactory
            .getDataAccess(PendingEventDataAccess.class);
    NextHeartbeatDataAccess nextHBDA = (NextHeartbeatDataAccess) RMStorageFactory
            .getDataAccess(NextHeartbeatDataAccess.class);
    UpdatedContainerInfoDataAccess uciDA = (UpdatedContainerInfoDataAccess)
            RMStorageFactory.getDataAccess(UpdatedContainerInfoDataAccess.class);
    ContainerStatusDataAccess contStatDA = (ContainerStatusDataAccess)
            RMStorageFactory.getDataAccess(ContainerStatusDataAccess.class);
    RMNodeDataAccess rmNodeDA = (RMNodeDataAccess) RMStorageFactory
            .getDataAccess(RMNodeDataAccess.class);
    ResourceDataAccess resourceDA = (ResourceDataAccess) RMStorageFactory
            .getDataAccess(ResourceDataAccess.class);

    peDA.add(pendingEvent);

    if (pendingEvent.getType().equals(PendingEvent.Type.NODE_ADDED)) {
      nextHBDA.update(new NextHeartbeat(nodeId, true));
    }

    if (!uciToAdd.isEmpty()) {
      uciDA.addAll(uciToAdd);
      contStatDA.addAll(containerStatusToAdd);
    }

    if (rmNode != null) {
      rmNodeDA.add(rmNode);
      resourceDA.add(rmNodeResource);
    }

    if (nextHeartBeat != null) {
      nextHBDA.update(nextHeartBeat);
    }
  }
}
