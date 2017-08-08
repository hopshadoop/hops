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
import io.hops.metadata.yarn.entity.ContainerToSignal;
import io.hops.metadata.yarn.entity.RMNodeApplication;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.streaming.ContainerIdToCleanEvent;
import io.hops.streaming.ContainerToDecreaseEvent;
import io.hops.streaming.ContainerToSignalEvent;
import io.hops.streaming.DBEvent;
import io.hops.streaming.RMNodeApplicationsEvent;
import io.hops.streaming.NextHeartBeatEvent;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class RtStreamingProcessor extends StreamingReceiver {

  public RtStreamingProcessor(RMContext rmContext) {
    super(rmContext, "RT Event retriever");
    setRetrievingRunnable(new RetrievingThread());
  }

  private class RetrievingThread implements Runnable {

    @Override
    public void run() {
      while (running) {
        if (!rmContext.isLeader()) {
          try {
            DBEvent event = DBEvent.receivedEvents.take();
            if (event instanceof ContainerIdToCleanEvent) {
              io.hops.metadata.yarn.entity.ContainerId containerId
                      = ((ContainerIdToCleanEvent) event).getContainerId();
              RMNode node = rmContext.getRMNodes().get(ConverterUtils.toNodeId(
                      containerId.getRmnodeid()));
              ((RMNodeImplDist) node).addContainersToCleanUp(ConverterUtils.
                      toContainerId(containerId.getContainerId()));
            } else if (event instanceof ContainerToSignalEvent) {
              ContainerToSignal containerToSignal
                  = ((ContainerToSignalEvent) event).getContainerToSignal();
              RMNode node = rmContext.getRMNodes().get(NodeId.fromString(containerToSignal.getRmnodeid()));
              ((RMNodeImplDist) node).addContainersToSignal(SignalContainerRequest.newInstance(ContainerId.fromString(
                  containerToSignal.getContainerId()), SignalContainerCommand.valueOf(containerToSignal.getCommand())));
            } else if (event instanceof ContainerToDecreaseEvent) {
              io.hops.metadata.yarn.entity.Container container = ((ContainerToDecreaseEvent) event).getContainer();
              RMNode node = rmContext.getRMNodes().get(NodeId.fromString(container.getNodeId()));
              ((RMNodeImplDist) node).addContainersToDecrease(Container.newInstance(ContainerId.fromString(
                  container.getContainerId()), NodeId.fromString(container.getNodeId()), container.getHttpAddress(),
                  Resource.newInstance(container.getMemSize(), container.getVirtualCores(), container.getGpus()),
                  Priority.newInstance(container.getPriority()), null));
            } else if (event instanceof NextHeartBeatEvent) {
              NextHeartbeat nextHB = ((NextHeartBeatEvent) event).
                      getNextHeartbeat();
              RMNode node = rmContext.getRMNodes().get(ConverterUtils.toNodeId(
                      nextHB.getRmnodeid()));
              if (node != null) {
                ((RMNodeImplDist) node).setNextHeartbeat(nextHB.
                        isNextheartbeat());
              }
            } else if (event instanceof RMNodeApplicationsEvent) {
              RMNodeApplication rmNodeApp = ((RMNodeApplicationsEvent) event).getRmNodeApplication();
              RMNode node = rmContext.getRMNodes().get(NodeId.fromString(rmNodeApp.getRMNodeID()));
              if(rmNodeApp.getStatus().equals(RMNodeApplication.RMNodeApplicationStatus.FINISHED)){
                ((RMNodeImplDist) node).addAppToCleanUp(ApplicationId.fromString(rmNodeApp.getApplicationId()));
              } else if (rmNodeApp.getStatus().equals(RMNodeApplication.RMNodeApplicationStatus.RUNNING)){
                ((RMNodeImplDist) node).addToRunningApps(ApplicationId.fromString(rmNodeApp.getApplicationId()));
              }
            }
//TODO if scale well
//                            if (streamingRTComps.getCurrentNMMasterKey() != null) {
//                                ((NMTokenSecretManagerInRMDist) rmContext.getNMTokenSecretManager())
//                                        .setCurrentMasterKey(streamingRTComps.getCurrentNMMasterKey());
//                            }
//
//                            if (streamingRTComps.getNextNMMasterKey() != null) {
//                                ((NMTokenSecretManagerInRMDist) rmContext.getNMTokenSecretManager())
//                                        .setNextMasterKey(streamingRTComps.getNextNMMasterKey());
//                            }
//
//                            if (streamingRTComps.getCurrentRMContainerMasterKey() != null) {
//                                ((RMContainerTokenSecretManagerDist) rmContext.getContainerTokenSecretManager())
//                                        .setCurrentMasterKey(streamingRTComps.getCurrentRMContainerMasterKey());
//                            }
//
//                            if (streamingRTComps.getNextRMContainerMasterKey() != null) {
//                                ((RMContainerTokenSecretManagerDist) rmContext.getContainerTokenSecretManager())
//                                        .setNextMasterKey(streamingRTComps.getNextRMContainerMasterKey());
//                            }
          } catch (InterruptedException ex) {
            LOG.error(ex, ex);
          }
        }
      }
      LOG.info("HOP :: RT Event retriever interrupted");
    }
  }
}
