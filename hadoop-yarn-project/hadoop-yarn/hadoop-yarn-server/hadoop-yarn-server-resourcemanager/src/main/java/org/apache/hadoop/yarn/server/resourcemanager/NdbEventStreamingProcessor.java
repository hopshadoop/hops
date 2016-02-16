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

import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 *
 * @author sri
 */
public class NdbEventStreamingProcessor extends PendingEventRetrieval {

  public NdbEventStreamingProcessor(RMContext rmContext, Configuration conf) {
    super(rmContext, conf);

  }

  public void printHopsRMNodeComps(RMNodeComps hopRMNodeNDBCompObject) {

    //print hoprmnode 
    LOG.debug(
            "<EvtProcessor_PRINT_START>-------------------------------------------------------------------");
    if (hopRMNodeNDBCompObject.getHopRMNode() != null) {
      LOG.debug("<EvtProcessor> [rmnode] id : " + hopRMNodeNDBCompObject.
              getHopRMNode().getNodeId() + "| peinding id : "
              + hopRMNodeNDBCompObject.getHopRMNode().getPendingEventId());
    }
    //print hopnode
    if (hopRMNodeNDBCompObject.getHopNode() != null) {
      LOG.debug("<EvtProcessor> [node] id : " + hopRMNodeNDBCompObject.
              getHopNode().getId() + "| level : " + hopRMNodeNDBCompObject.
              getHopNode().getLevel());
    }
    //print hopresource 
    if (hopRMNodeNDBCompObject.getHopResource() != null) {
      LOG.debug("<EvtProcessor> [resource] id : " + hopRMNodeNDBCompObject.
              getHopResource().getId() + "| memory : " + hopRMNodeNDBCompObject.
              getHopResource().getMemory());
    }
    if (hopRMNodeNDBCompObject.getPendingEvent() != null) {
      LOG.debug("<EvtProcessor> [pendingevent] id : " + hopRMNodeNDBCompObject.
              getPendingEvent().getId().getNodeId() + "| peinding id : "
              + hopRMNodeNDBCompObject.getPendingEvent().getId() +
              "| type: " + hopRMNodeNDBCompObject.getPendingEvent().getType() +
              "| status: " + hopRMNodeNDBCompObject.getPendingEvent().getStatus());
    }
    List<UpdatedContainerInfo> hopUpdatedContainerInfo = hopRMNodeNDBCompObject.
            getHopUpdatedContainerInfo();
    for (UpdatedContainerInfo hopuc : hopUpdatedContainerInfo) {
      LOG.debug("<EvtProcessor> [updatedcontainerinfo] id : " + hopuc.
              getRmnodeid() + "| container id : " + hopuc.getContainerId());
    }
    List<ContainerStatus> hopContainersStatus = hopRMNodeNDBCompObject.
            getHopContainersStatus();
    for (ContainerStatus hopCS : hopContainersStatus) {
      LOG.debug("<EvtProcessor> [containerstatus] id : " + hopCS.getRMNodeId()
              + "| container id : " + hopCS.getContainerid()
              + "| container status : " + hopCS.getExitstatus());
    }
    LOG.debug(
            "<EvtProcessor_PRINT_END>-------------------------------------------------------------------");
  }

  public void start() {
    if (!active) {
      active = true;
      LOG.info("start retriving thread");
      retrivingThread = new Thread(new RetrivingThread());
      retrivingThread.start();
    } else {
      LOG.error("ndb event retriver is already active");
    }
  }
  
  private class RetrivingThread implements Runnable {

    @Override
    public void run() {
      while (active) {
        try {
          //first ask him sleep

          RMNodeComps hopRMNodeCompObject = null;
          hopRMNodeCompObject
                  = (RMNodeComps) NdbEventStreamingReceiver.blockingQueue.take();
          if (hopRMNodeCompObject != null) {
            if (LOG.isDebugEnabled()) {
              printHopsRMNodeComps(hopRMNodeCompObject);
            }
            if (rmContext.isDistributedEnabled()) {
              RMNode rmNode = null;
              try {
                rmNode = RMUtilities.processHopRMNodeCompsForScheduler(
                        hopRMNodeCompObject,
                        rmContext);
                LOG.debug("HOP :: RMNodeWorker rmNode:" + rmNode);

                if (rmNode != null) {
                  updateRMContext(rmNode);
                  triggerEvent(rmNode, hopRMNodeCompObject.getPendingEvent(),
                          false);
                }
              } catch (Exception ex) {
                LOG.error("HOP :: Error retrieving rmNode:" + ex, ex);
              }
            }
            // Processes container statuses for ContainersLogs service
            List<ContainerStatus> hopContainersStatusList
                    = hopRMNodeCompObject.getHopContainersStatus();
            if (rmContext.isLeadingRT() && hopContainersStatusList.size() > 0 &&
                    rmContext.getContainersLogsService() !=null) {
              rmContext.getContainersLogsService()
                      .insertEvent(hopContainersStatusList);
            }
          }
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }

      }

    }
  }

}
