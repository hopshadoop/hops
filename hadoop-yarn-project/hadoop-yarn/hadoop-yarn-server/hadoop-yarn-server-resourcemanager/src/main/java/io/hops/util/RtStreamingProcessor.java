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

import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.streaming.ContainerIdToCleanEvent;
import io.hops.streaming.DBEvent;
import io.hops.streaming.FinishedApplicationsEvent;
import io.hops.streaming.NextHeartBeatEvent;
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
            } else if (event instanceof NextHeartBeatEvent) {
              NextHeartbeat nextHB = ((NextHeartBeatEvent) event).
                      getNextHeartbeat();
              RMNode node = rmContext.getRMNodes().get(ConverterUtils.toNodeId(
                      nextHB.getRmnodeid()));
              if (node != null) {
                ((RMNodeImplDist) node).setNextHeartbeat(nextHB.
                        isNextheartbeat());
              }
            } else if (event instanceof FinishedApplicationsEvent) {
              FinishedApplications finishedApp
                      = ((FinishedApplicationsEvent) event).
                      getFinishedApplication();
              RMNode node = rmContext.getRMNodes().get(ConverterUtils.toNodeId(
                      finishedApp.getRMNodeID()));
              ((RMNodeImplDist) node).addAppToCleanUp(ConverterUtils.
                      toApplicationId(finishedApp.getApplicationId()));
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
