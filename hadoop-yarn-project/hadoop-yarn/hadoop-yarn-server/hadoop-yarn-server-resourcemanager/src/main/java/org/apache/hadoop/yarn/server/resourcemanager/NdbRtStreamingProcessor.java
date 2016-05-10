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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author sri
 */
public class NdbRtStreamingProcessor implements Runnable {

  private static final Log LOG = LogFactory.getLog(
          NdbRtStreamingProcessor.class);
  private boolean running = false;
  private final RMContext context;
  private RMNode rmNode;

  public NdbRtStreamingProcessor(RMContext context) {
    this.context = context;
  }

  public void printStreamingRTComps(StreamingRTComps streamingRTComps) {
    List<org.apache.hadoop.yarn.api.records.ApplicationId> applicationIdList
            = streamingRTComps.getFinishedApp();
    for (org.apache.hadoop.yarn.api.records.ApplicationId appId
            : applicationIdList) {
      LOG.debug("<Processor> Finished application : appid : " + appId.toString()
              + "node id : " + streamingRTComps.getNodeId());
    }

    Set<org.apache.hadoop.yarn.api.records.ContainerId> containerIdList
            = streamingRTComps.getContainersToClean();
    for (org.apache.hadoop.yarn.api.records.ContainerId conId : containerIdList) {
      LOG.debug("<Processor> Containers to clean  containerid: " + conId.
              toString());
    }
    LOG.debug("RTReceived: " + streamingRTComps.getNodeId() + " nexthb: "
            + streamingRTComps.isNextHeartbeat());

  }

  @Override
  public void run() {
    running = true;
    while (running) {
      if (!context.isLeader()) {
        try {

          StreamingRTComps streamingRTComps = null;
          streamingRTComps
                  = (StreamingRTComps) NdbRtStreamingReceiver.blockingRTQueue.
                  take();
          if (streamingRTComps != null) {
            if (LOG.isDebugEnabled()) {
              printStreamingRTComps(streamingRTComps);
            }

            
            String streamingRTCompsNodeId = streamingRTComps.getNodeId();
            if(streamingRTCompsNodeId != null) {
                NodeId nodeId = ConverterUtils.
                    toNodeId(streamingRTCompsNodeId);
                rmNode = context.getActiveRMNodes().get(nodeId);
                if (rmNode != null) {
                  rmNode.setContainersToCleanUp(streamingRTComps.
                          getContainersToClean());
                  rmNode.setAppsToCleanup(streamingRTComps.getFinishedApp());
                  rmNode.setNextHeartBeat(streamingRTComps.isNextHeartbeat());
                }
            }
            
            // Processes container statuses for ContainersLogs service
            List<ContainerStatus> hopContainersStatusList 
                    = streamingRTComps.getHopContainersStatusList();
            if (context.isLeadingRT() && context.getContainersLogsService()
                    != null) {
              if (hopContainersStatusList.size() > 0) {
                context.getContainersLogsService()
                        .insertEvent(hopContainersStatusList);
              }
              if (streamingRTComps.getCurrentPrice() > 0) {
                context.getContainersLogsService().setCurrentPrice(
                        streamingRTComps.getCurrentPrice());
              }
            }
            
            if(streamingRTComps.getCurrentNMMasterKey()!=null){
              context.getNMTokenSecretManager().setCurrentMasterKey(
                      streamingRTComps.getCurrentNMMasterKey());
            }
            if(streamingRTComps.getNextNMMasterKey()!=null){
              context.getNMTokenSecretManager().setCurrentMasterKey(
                      streamingRTComps.getNextNMMasterKey());
            }
            if(streamingRTComps.getCurrentRMContainerMasterKey()!=null){
              context.getContainerTokenSecretManager().setCurrentMasterKey(
                      streamingRTComps.getCurrentRMContainerMasterKey());
            }
            if(streamingRTComps.getNextNMMasterKey()!=null){
              context.getContainerTokenSecretManager().setCurrentMasterKey(
                      streamingRTComps.getNextNMMasterKey());
            }
          }
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }

      }
    }

  }

  public void stop() {
    running = false;
  }

}
