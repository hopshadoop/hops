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

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.metadata.yarn.entity.NodeId;
import io.hops.transaction.handler.AsyncLightWeightRequestHandler;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hops.transaction.handler.ThreadPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.transaction.handler.LightWeightRequestHandler;

public class DBUtility {

  private static final Log LOG = LogFactory.getLog(DBUtility.class);

  public static void removeContainersToClean(final Set<ContainerId> containers,
          final org.apache.hadoop.yarn.api.records.NodeId nodeId) throws IOException {
    AsyncLightWeightRequestHandler removeContainerToClean
            = new AsyncLightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        List<io.hops.metadata.yarn.entity.ContainerId> containersToClean
                = new ArrayList<io.hops.metadata.yarn.entity.ContainerId>();
        for (ContainerId cid : containers) {
          containersToClean.add(new io.hops.metadata.yarn.entity.ContainerId(
                  nodeId.toString(), cid.toString()));
        }
        ctcDA.removeAll(containersToClean);
        connector.commit();
        return null;
      }
    };

    removeContainerToClean.handle();
  }

  public static void removeFinishedApplications(
          final List<ApplicationId> finishedApplications, final org.apache.hadoop.yarn.api.records.NodeId nodeId)
          throws IOException {
    AsyncLightWeightRequestHandler removeFinishedApplication
            = new AsyncLightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        FinishedApplicationsDataAccess faDA
                = (FinishedApplicationsDataAccess) RMStorageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
        List<FinishedApplications> finishedApps
                = new ArrayList<FinishedApplications>();
        for (ApplicationId appId : finishedApplications) {
          finishedApps.add(new FinishedApplications(nodeId.toString(), appId.
                  toString()));
        }
        faDA.removeAll(finishedApps);
        connector.commit();

        return null;
      }
    };

    removeFinishedApplication.handle();
  }

  public static void addFinishedApplication(final ApplicationId appId,
          final org.apache.hadoop.yarn.api.records.NodeId nodeId) throws
          IOException {
    AsyncLightWeightRequestHandler addFinishedApplication
            = new AsyncLightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        FinishedApplicationsDataAccess faDA
                = (FinishedApplicationsDataAccess) RMStorageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
        faDA.add(new FinishedApplications(nodeId.toString(), appId.toString()));
        connector.commit();

        return null;
      }
    };

    addFinishedApplication.handle();
  }



  public static RMNode processHopRMNodeCompsForScheduler(RMNodeComps hopRMNodeComps, RMContext rmContext)
          throws InvalidProtocolBufferException {
    org.apache.hadoop.yarn.api.records.NodeId nodeId;
    RMNode rmNode = null;
    if (hopRMNodeComps != null) {
      nodeId = ConverterUtils.toNodeId(hopRMNodeComps.getRMNodeId());
      rmNode = rmContext.getRMNodes().get(nodeId);

      // The first time we are receiving the RMNode, this will happen when the node registers
      if (rmNode == null) {
        // Retrieve heartbeat
        boolean nextHeartbeat = true;

        // Create Resource
        Resource resource = null;
        if (hopRMNodeComps.getHopResource() != null) {
          resource = Resource.newInstance(hopRMNodeComps.getHopResource().getMemory(),
                  hopRMNodeComps.getHopResource().getVirtualCores());
        } else {
          LOG.error("ResourceOption should not be null");
          resource = Resource.newInstance(0, 0);
        }
        /*rmNode = new RMNodeImplDist(nodeId, rmContext, hopRMNodeComps.getHopRMNode().getHostName(),
                hopRMNodeComps.getHopRMNode().getCommandPort(),
                hopRMNodeComps.getHopRMNode().getHttpPort(),
                ResourceTrackerService.resolve(hopRMNodeComps.getHopRMNode().getHostName()),
                resourceOption,
                hopRMNodeComps.getHopRMNode().getNodemanagerVersion(),
                hopRMNodeComps.getHopRMNode().getHealthReport(),
                hopRMNodeComps.getHopRMNode().getLastHealthReportTime(),
                nextHeartbeat);*/

        rmNode = new RMNodeImplDist(nodeId, rmContext, hopRMNodeComps.getHopRMNode().getHostName(),
                hopRMNodeComps.getHopRMNode().getCommandPort(),
                hopRMNodeComps.getHopRMNode().getHttpPort(),
                ResourceTrackerService.resolve(hopRMNodeComps.getHopRMNode().getHostName()),
                resource,
                hopRMNodeComps.getHopRMNode().getNodemanagerVersion());

        // Force Java to put the host in cache
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
      }

      // Update the RMNode
      if (hopRMNodeComps.getHopRMNode() != null) {
        ((RMNodeImplDist)rmNode).setState(hopRMNodeComps.getHopRMNode().getCurrentState());
      }
      if (hopRMNodeComps.getHopUpdatedContainerInfo() != null) {
        List<io.hops.metadata.yarn.entity.UpdatedContainerInfo> hopUpdatedContainerInfoList =
                hopRMNodeComps.getHopUpdatedContainerInfo();

        if (hopUpdatedContainerInfoList != null
                && !hopUpdatedContainerInfoList.isEmpty()) {
          ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
                  updatedContainerInfoQueue =
                  new ConcurrentLinkedQueue<>();

          Map<Integer, org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo> ucis =
                  new HashMap<>();
          LOG.debug(hopRMNodeComps.getRMNodeId() + " getting ucis "
                  + hopUpdatedContainerInfoList.size() + " pending event "
                  + hopRMNodeComps.getPendingEvent().getId().getEventId());

          for (io.hops.metadata.yarn.entity.UpdatedContainerInfo hopUCI : hopUpdatedContainerInfoList) {
            if (!ucis.containsKey(hopUCI.getUpdatedContainerInfoId())) {
              ucis.put(hopUCI.getUpdatedContainerInfoId(),
                      new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                              new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>(),
                              new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>(),
                              hopUCI.getUpdatedContainerInfoId()));
            }

            ContainerId cid = ConverterUtils.toContainerId(hopUCI.getContainerId());
            io.hops.metadata.yarn.entity.ContainerStatus hopContainerStatus = hopRMNodeComps.
                    getHopContainersStatusMap().get(hopUCI.getContainerId());

            org.apache.hadoop.yarn.api.records.ContainerStatus conStatus = org.apache.hadoop.yarn.api.records.ContainerStatus.newInstance(
                    cid,
                    ContainerState.valueOf(hopContainerStatus.getState()),
                    hopContainerStatus.getDiagnostics(),
                    hopContainerStatus.getExitstatus());

            // Check ContainerStatus state to add it in the appropriate list
            if (conStatus != null) {
              LOG.debug("add uci for container " + conStatus.getContainerId()
                      + " status " + conStatus.getState());
              if (conStatus.getState().equals(ContainerState.RUNNING)) {
                ucis.get(hopUCI.getUpdatedContainerInfoId()).
                        getNewlyLaunchedContainers().add(conStatus);
              } else if (conStatus.getState().equals(ContainerState.COMPLETE)) {
                ucis.get(hopUCI.getUpdatedContainerInfoId()).
                        getCompletedContainers().add(conStatus);
              }
            }
          }

          for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo uci :
                  ucis.values()) {
            updatedContainerInfoQueue.add(uci);
          }

          ((RMNodeImplDist)rmNode).setUpdatedContainerInfo(updatedContainerInfoQueue);
        } else {
          LOG.debug(hopRMNodeComps.getRMNodeId()
                  + " hopUpdatedContainerInfoList = null || hopUpdatedContainerInfoList.isEmpty() "
                  + hopRMNodeComps.getPendingEvent().getId().getEventId());
        }
      } else {
        LOG.debug(hopRMNodeComps.getRMNodeId()
                + " hopRMNodeFull.getHopUpdatedContainerInfo()=null "
                + hopRMNodeComps.getPendingEvent().getId().getEventId());
      }
    }

    return rmNode;
  }

  private final static ConcurrentLinkedQueue<PendingEvent> pendingEventsToRemove =
          new ConcurrentLinkedQueue<>();
  private static Thread pendingEventsCommitter = null;
  private final static Semaphore pendingEventsSem =
          new Semaphore(0, true);
  private final static int MIN_NUM_OF_PENDING_EVENTS = 200;

  public static void removePendingEvent(String rmNodeId, PendingEvent.Type type,
          PendingEvent.Status status, int id) {

    pendingEventsToRemove.add(new PendingEvent(rmNodeId, type, status, id));
    pendingEventsSem.release();

    if (pendingEventsCommitter == null) {
      synchronized (DBUtility.class) {
        if (pendingEventsCommitter == null) {
          pendingEventsCommitter = new Thread(new PendingEventsCommitter());
          pendingEventsCommitter.setName("RemovePendingEvents");
          pendingEventsCommitter.start();
          LOG.info("HOP :: RemovePendingEvents committer thread started");
        }
      }
    }
  }

  private static class PendingEventsCommitter implements Runnable {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          pendingEventsSem.tryAcquire(MIN_NUM_OF_PENDING_EVENTS,
                  500, TimeUnit.MILLISECONDS);
          commitRemovePendingEvents();
        } catch (IOException ex) {
          LOG.error(ex, ex);
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static void commitRemovePendingEvents() throws IOException {
    final Iterator<PendingEvent> peIt = pendingEventsToRemove.iterator();
    final List<PendingEvent> toCommit = new ArrayList<>();
    while (peIt.hasNext()) {
      toCommit.add(peIt.next());
    }

    if (!toCommit.isEmpty()) {
      AsyncLightWeightRequestHandler removePendingEvents =
              new AsyncLightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();

                  PendingEventDataAccess peDA = (PendingEventDataAccess) RMStorageFactory
                          .getDataAccess(PendingEventDataAccess.class);
                  peDA.removeAll(toCommit);
                  connector.commit();

                  return null;
                }
              };
      removePendingEvents.handle();

      pendingEventsToRemove.removeAll(toCommit);
    }
  }



  private final static ConcurrentLinkedQueue<io.hops.metadata.yarn.entity.ContainerId> toCommitCidToClean =
          new ConcurrentLinkedQueue<>();
  private static Thread cidToCleanCommitter = null;
  private final static Semaphore toCommitSemaphore = new Semaphore(0, true);
  private final static int MIN_NUM_OF_CONTAINERS = 50;

  public static void addContainerToClean(ContainerId containerId,
          org.apache.hadoop.yarn.api.records.NodeId nodeId) {

    toCommitCidToClean.add(new io.hops.metadata.yarn.entity.ContainerId(nodeId.toString(),
            containerId.toString()));
    toCommitSemaphore.release();

    if (cidToCleanCommitter == null) {
      synchronized (DBUtility.class) {
        if (cidToCleanCommitter == null) {
          cidToCleanCommitter = new Thread(new ContainerToCleanCommitter());
          cidToCleanCommitter.setName("cidToCleanCommitter");
          cidToCleanCommitter.start();
          LOG.info("HOP :: Started ContainerToClean committer thread");
        }
      }
    }
  }

  private static class ContainerToCleanCommitter implements Runnable {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {

        try {
          toCommitSemaphore.tryAcquire(MIN_NUM_OF_CONTAINERS,
                  200, TimeUnit.MILLISECONDS);
          persistCidToClean();
        } catch (IOException ex) {
          LOG.error(ex, ex);
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static void persistCidToClean() throws IOException {
    final Iterator<io.hops.metadata.yarn.entity.ContainerId> cidIt =
            toCommitCidToClean.iterator();
    final List<io.hops.metadata.yarn.entity.ContainerId> toCommit =
            new ArrayList<>();
    while (cidIt.hasNext()) {
      toCommit.add(cidIt.next());
    }

    if (!toCommit.isEmpty()) {
      LightWeightRequestHandler addCidToClean =
              new LightWeightRequestHandler(YARNOperationType.TEST) {
                @Override
                public Object performTask() throws IOException {
                  connector.beginTransaction();
                  connector.writeLock();

                  ContainerIdToCleanDataAccess cidToCleanDA = (ContainerIdToCleanDataAccess)
                          RMStorageFactory.getDataAccess(ContainerIdToCleanDataAccess.class);
                  cidToCleanDA.addAll(toCommit);
                  connector.commit();

                  return null;
                }
              };

      addCidToClean.handle();
      toCommitCidToClean.removeAll(toCommit);
    }
  }

  public static void addNextHB(final boolean nextHB, final String nodeId) throws IOException {
    AsyncLightWeightRequestHandler addNextHB =
            new AsyncLightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                NextHeartbeatDataAccess nhbDA = (NextHeartbeatDataAccess) RMStorageFactory
                        .getDataAccess(NextHeartbeatDataAccess.class);
                nhbDA.update(new NextHeartbeat(nodeId, nextHB));
                connector.commit();
                return null;
              }
            };

    addNextHB.handle();
  }

  public static void removeUCI(List<UpdatedContainerInfo> ContainerInfoList,
          String nodeId) throws IOException {
    final List<io.hops.metadata.yarn.entity.UpdatedContainerInfo> uciToRemove
            = new ArrayList<io.hops.metadata.yarn.entity.UpdatedContainerInfo>();
    final List<ContainerStatus> containerStatusToRemove
            = new ArrayList<ContainerStatus>();
    for (UpdatedContainerInfo uci : ContainerInfoList) {
      if (uci.getNewlyLaunchedContainers() != null
              && !uci.getNewlyLaunchedContainers().isEmpty()) {
        for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
                : uci
                .getNewlyLaunchedContainers()) {
          io.hops.metadata.yarn.entity.UpdatedContainerInfo hopUCI
                  = new io.hops.metadata.yarn.entity.UpdatedContainerInfo(nodeId,
                  containerStatus.getContainerId().toString(), uci.
                  getUciId());
          uciToRemove.add(hopUCI);

          ContainerStatus hopConStatus = new ContainerStatus(containerStatus.
                  getContainerId().toString(), nodeId, ContainerStatus.Type.UCI,
                  uci.getUciId());
          containerStatusToRemove.add(hopConStatus);
        }
      }
      if (uci.getCompletedContainers() != null
              && !uci.getCompletedContainers().isEmpty()) {
        for (org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus
                : uci
                .getCompletedContainers()) {
          io.hops.metadata.yarn.entity.UpdatedContainerInfo hopUCI
                  = new io.hops.metadata.yarn.entity.UpdatedContainerInfo(nodeId,
                  containerStatus.getContainerId().toString(), uci.
                  getUciId());
          uciToRemove.add(hopUCI);
          ContainerStatus hopConStatus = new ContainerStatus(containerStatus.
                  getContainerId().toString(), nodeId, ContainerStatus.Type.UCI,
                  uci.getUciId());
          containerStatusToRemove.add(hopConStatus);
        }
      }
    }

    AsyncLightWeightRequestHandler removeUCIHandler
            = new AsyncLightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        UpdatedContainerInfoDataAccess uciDA
                = (UpdatedContainerInfoDataAccess) RMStorageFactory
                .getDataAccess(UpdatedContainerInfoDataAccess.class);
        uciDA.removeAll(uciToRemove);

        ContainerStatusDataAccess csDA
                = (ContainerStatusDataAccess) RMStorageFactory
                .getDataAccess(ContainerStatusDataAccess.class);
        csDA.removeAll(containerStatusToRemove);
        connector.commit();
        return null;
      }
    };
    removeUCIHandler.handle();
  }

  public static Map<String, Load> getAllLoads() throws IOException {
    LightWeightRequestHandler getLoadHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMLoadDataAccess DA = (RMLoadDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMLoadDataAccess.class);
        Map<String, Load> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String, Load>) getLoadHandler.handle();
  }

  public static void updateLoad(final Load load) throws IOException {
    AsyncLightWeightRequestHandler updateLoadHandler = new AsyncLightWeightRequestHandler(
            YARNOperationType.TEST) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMLoadDataAccess DA = (RMLoadDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMLoadDataAccess.class);
        DA.update(load);
        connector.commit();
        return null;
      }
    };
    updateLoadHandler.handle();
  }

  public static void InitializeDB() throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.formatStorage();
        LOG.debug("HOP :: Format storage has been completed");
        return null;
      }
    };
    setRMDTMasterKeyHandler.handle();
  }
}