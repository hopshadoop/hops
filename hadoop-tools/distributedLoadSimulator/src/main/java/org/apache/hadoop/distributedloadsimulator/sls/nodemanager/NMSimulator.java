/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.distributedloadsimulator.sls.nodemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.distributedloadsimulator.sls.SLSRunner;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;

public class NMSimulator extends TaskRunner.Task {

  private ResourceTracker resourceTracker;
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  private MasterKey containerMasterKey;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private DelayQueue<ContainerSimulator> containerQueue;
  private Map<ContainerId, ContainerSimulator> runningContainers;
  private List<ContainerId> amContainerList;
  // resource manager
  private ResourceManager rm;
  private AtomicInteger totalHeartBeat = new AtomicInteger(0);
  private AtomicInteger trueHeartBeat = new AtomicInteger(0);
  // heart beat response id
  private int RESPONSE_ID = 1;
  private final static Logger LOG = Logger.getLogger(NMSimulator.class);
  private boolean isFirstBeat = true;
  private AtomicInteger usedResources = new AtomicInteger(0);

  public void init(String nodeIdStr, int memory, int cores,
          int dispatchTime, int heartBeatInterval, ResourceManager rm,
          ResourceTracker rt)
          throws IOException, YarnException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
            heartBeatInterval);
//    this.resourceTracker = ServerRMProxy.createRMProxy(conf, ResourceTracker.class,
//            conf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
//                    YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
    // create resource
    this.resourceTracker = rt;
    String rackHostName[] = SLSUtils.getRackHostName(nodeIdStr);
    this.node = NodeInfo.newNodeInfo(rackHostName[0], rackHostName[1],
            BuilderUtils.newResource(memory, cores));
    //this.nodeId = NodeId.newInstance(InetAddress.getLocalHost().getHostName(),port);
    //this.rm = rm;
    // init data structures
    completedContainerList
            = Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList
            = Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList
            = Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers
            = new ConcurrentHashMap<ContainerId, ContainerSimulator>();
    // register NM with RM
    RegisterNodeManagerRequest req
            = Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    RegisterNodeManagerResponse response = resourceTracker.registerNodeManager(
            req);
    masterKey = response.getNMTokenMasterKey();
    containerMasterKey = response.getContainerTokenMasterKey();
  }

  @Override
  public void firstStep() throws YarnException, IOException {
    // do nothing
  }

  public int getTotalHeartBeat() {
    return totalHeartBeat.get();
  }

  public int getTotalTrueHeartBeat() {
    return trueHeartBeat.get();
  }

  static AtomicLong hbduration = new AtomicLong(0);
  static AtomicInteger nbhb = new AtomicInteger(0);

  @Override
  public void middleStep() throws YarnException, IOException {
    // we check the lifetime for each running containers
    ContainerSimulator cs = null;
    synchronized (completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        usedResources.decrementAndGet();
      }
    }

    // send heart beat
    NodeHeartbeatRequest beatRequest
            = Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    beatRequest.setLastKnownContainerTokenMasterKey(containerMasterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);

    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    beatRequest.setNodeStatus(ns);
    // only first time , this NM thread will update the beat start sec
    if (isFirstBeat) {
      SLSRunner.measureFirstBeat();
      isFirstBeat = false;
    }
    long start = System.currentTimeMillis();
    NodeHeartbeatResponse beatResponse = resourceTracker.nodeHeartbeat(
            beatRequest);
    long duration = System.currentTimeMillis() - start;
    long totalDuration = hbduration.addAndGet(duration);
    int totalNbHb = nbhb.incrementAndGet();
    long avg = totalDuration / totalNbHb;
    if (duration > 500) {
      LOG.info(node.getNodeID() + " hb duration: " + duration + " avg: " + avg);
    }
    totalHeartBeat.incrementAndGet();
    if (beatResponse.getNextheartbeat()) {
      trueHeartBeat.incrementAndGet();
    }
    if (!beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized (releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()) {
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized (amContainerList) {
              amContainerList.remove(containerId);
            }
          } else {
            cs = runningContainers.remove(containerId);
            usedResources.decrementAndGet();
            if (cs != null) {
              LOG.error("in the simulated scenario the container should not be"
                      + "freed until they have completed their task");
              throw new YarnException("the failover should be transparent");
            }
            containerQueue.remove(cs);
            releasedContainerList.add(containerId);
          }
        }
      }
    }
    if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
      lastStep();
    } else if (beatResponse.getNodeAction() == NodeAction.RESYNC) {
      LOG.error("the failover should be transparent, node:" + node.getNodeID()
              + " message: " + beatResponse.getDiagnosticsMessage());
      throw new YarnException("the failover should be transparent "
              + beatResponse.getDiagnosticsMessage());
    }
    if (beatResponse.getContainerTokenMasterKey() != null) {
      masterKey = beatResponse.getContainerTokenMasterKey();
    }
    if (beatResponse.getContainerTokenMasterKey() != null) {
      containerMasterKey = beatResponse.getContainerTokenMasterKey();
    }
  }

  @Override
  public void lastStep() {
    LOG.info("Last step for nodemanager " + node.getNodeID());
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
    // add running containers
    for (ContainerSimulator container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(),
              ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
    }
    synchronized (amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId,
                ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
      }
    }
    // add complete containers
    synchronized (completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized (releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED));
      }
      releasedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId,
          ContainerState state,
          int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public RMNode getNode() {
    return node;
  }

  /**
   * launch a new container with the given life time
   *
   * @param container
   * @param lifeTimeMS
   */
  public void addNewContainer(Container container, long lifeTimeMS) {
    if (lifeTimeMS != -1) {
      // normal container
      ContainerSimulator cs = new ContainerSimulator(container.getId(),
              container.getResource(), lifeTimeMS + System.currentTimeMillis(),
              lifeTimeMS);
      containerQueue.add(cs);
      runningContainers.put(cs.getId(), cs);
      usedResources.incrementAndGet();
    } else {
      // AM container
      // -1 means AMContainer
      synchronized (amContainerList) {
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   *
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized (amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized (completedContainerList) {
      completedContainerList.add(containerId);
    }
  }

  public int getUsedResources() {
    return usedResources.get();
  }
}
