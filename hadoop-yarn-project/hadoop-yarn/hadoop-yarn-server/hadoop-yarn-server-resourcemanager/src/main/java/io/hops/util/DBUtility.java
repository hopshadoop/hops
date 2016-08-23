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
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.metadata.yarn.entity.NodeId;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class DBUtility {

  private static final Log LOG = LogFactory.getLog(DBUtility.class);

  public static void removeContainersToClean(final Set<ContainerId> containers,
          final NodeId nodeId) throws IOException {
    LightWeightRequestHandler removeContainerToClean
            = new LightWeightRequestHandler(
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
        ctcDA.removeAll(containers);
        connector.commit();
        return null;
      }
    };
    removeContainerToClean.handle();
  }

  public static void removeFinishedApplications(
          final List<ApplicationId> finishedApplications, final NodeId nodeId)
          throws IOException {
    LightWeightRequestHandler removeFinishedApplication
            = new LightWeightRequestHandler(
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
          final NodeId nodeId) throws
          IOException {
    LightWeightRequestHandler addFinishedApplication
            = new LightWeightRequestHandler(
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

  public static void addContainerToClean(final ContainerId containerId,
          final NodeId nodeId) throws
          IOException {
    LightWeightRequestHandler addContainerToClean
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        ctcDA.add(
                new io.hops.metadata.yarn.entity.ContainerId(nodeId.toString(),
                        containerId));
        connector.commit();
        return null;
      }
    };
    addContainerToClean.handle();
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
        ResourceOption resourceOption = null;
        if (hopRMNodeComps.getHopResource() != null) {
          resourceOption = ResourceOption.newInstance(
                  Resource.newInstance(hopRMNodeComps.getHopResource().getMemory(),
                          hopRMNodeComps.getHopResource().getVirtualCores()),
                  hopRMNodeComps.getHopRMNode().getOvercommittimeout());
        } else {
          LOG.error("ResourceOption should not be null");
          resourceOption = ResourceOption.newInstance(
                  Resource.newInstance(0, 0),
                  hopRMNodeComps.getHopRMNode().getOvercommittimeout());
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
                resourceOption.getResource(),
                hopRMNodeComps.getHopRMNode().getNodemanagerVersion());

        // Force Java to put the host in cache
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
      }

      // Update the RMNode
      if (hopRMNodeComps.getHopRMNode() != null) {
        ((RMNodeImplDist)rmNode).setState(hopRMNodeComps.getHopRMNode().getCurrentState());
      }
      if (hopRMNodeComps.getHopUpdatedContainerInfo() != null) {
        List<UpdatedContainerInfo> hopUpdatedContainerInfoList =
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

          for (UpdatedContainerInfo hopUCI : hopUpdatedContainerInfoList) {
            if (!ucis.containsKey(hopUCI.getUpdatedContainerInfoId())) {
              ucis.put(hopUCI.getUpdatedContainerInfoId(),
                      new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                              new ArrayList<ContainerStatus>(),
                              new ArrayList<ContainerStatus>(),
                              hopUCI.getUpdatedContainerInfoId()));
            }

            ContainerId cid = ConverterUtils.toContainerId(hopUCI.getContainerId());
            io.hops.metadata.yarn.entity.ContainerStatus hopContainerStatus = hopRMNodeComps.
                    getHopContainersStatusMap().get(hopUCI.getContainerId());

            ContainerStatus conStatus = ContainerStatus.newInstance(
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
}
