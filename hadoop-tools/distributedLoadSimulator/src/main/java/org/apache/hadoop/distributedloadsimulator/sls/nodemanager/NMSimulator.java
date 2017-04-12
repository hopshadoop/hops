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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.client.ConfiguredLeastLoadedRMFailoverHAProxyProvider;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;

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
  private int RESPONSE_ID = 0;
  private final static Log LOG = LogFactory.getLog(NMSimulator.class);
  private boolean isFirstBeat = true;
  private AtomicInteger usedResources = new AtomicInteger(0);

  public void init(String nodeIdStr, int memory, int cores,
          int dispatchTime, int heartBeatInterval, ResourceManager rm,
          Configuration conf)
          throws IOException, YarnException, ClassNotFoundException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
            heartBeatInterval);
    conf.setClass(YarnConfiguration.LEADER_CLIENT_FAILOVER_PROXY_PROVIDER,
            ConfiguredLeastLoadedRMFailoverHAProxyProvider.class,
            RMFailoverProxyProvider.class);
    Class<? extends RMFailoverProxyProvider> defaultProviderClass
            = (Class<? extends RMFailoverProxyProvider>) Class.forName(
                    YarnConfiguration.DEFAULT_LEADER_CLIENT_FAILOVER_PROXY_PROVIDER);
    this.resourceTracker = ServerRMProxy.createRMProxy(conf,
            ResourceTracker.class,
            conf.getBoolean(YarnConfiguration.DISTRIBUTED_RM,
                    YarnConfiguration.DEFAULT_DISTRIBUTED_RM));
    // create resource
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
    LOG.info("send registration request " + node.getNodeID());
    RegisterNodeManagerResponse response = resourceTracker.registerNodeManager(
            req);
    LOG.info("registration done " + node.getNodeID());
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

  private long startXP = 0;
  private int nbHb = 0;
  private int nbTrueHb=0;
  private long last = 0;
  private long totalInterval=0;
  private long totalHBDuration=0;
  @Override
  public void middleStep() throws YarnException, IOException {
    if(startXP ==0){
      startXP = System.currentTimeMillis();
      last = System.currentTimeMillis();
    }
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
    ns.setResponseId(RESPONSE_ID);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", System.currentTimeMillis()));
    beatRequest.setNodeStatus(ns);
    // only first time , this NM thread will update the beat start sec
    if (isFirstBeat) {
      SLSRunner.measureFirstBeat();
      isFirstBeat = false;
    }
    long start = System.currentTimeMillis();
    try {
      NodeHeartbeatResponse beatResponse = resourceTracker.nodeHeartbeat(
              beatRequest);
      long duration = System.currentTimeMillis() - start;
      long totalDuration = hbduration.addAndGet(duration);
      int totalNbHb = nbhb.incrementAndGet();
      long avg = totalDuration / totalNbHb;
      nbHb++;
      long theoric=(System.currentTimeMillis()-startXP)/1000;
      float percentHb= (float) nbHb/theoric;
      long dif = nbHb - theoric;
      long interval = System.currentTimeMillis() - last;
      totalHBDuration+=duration;
      totalInterval+=interval;
      long avgHBDuration=totalHBDuration/nbHb;
      long avgInterval = totalInterval/nbHb;
//      if(percentHb<0.97){
//        LOG.error("this node is running behind: " + node.getNodeID() + " : " + percentHb);
//      }
      
      if( interval >1500){
        LOG.debug("this hb was too slow: " + node.getNodeID() + " : " + RESPONSE_ID + " " + interval + " " + duration);
      }
      last = System.currentTimeMillis();
      if (duration > 1000) {
        LOG.
                error(node.getNodeID() + " hb duration: " + duration + " avg: "
                        + avg);
      }
      totalHeartBeat.incrementAndGet();
      if (beatResponse.getNextheartbeat()) {
        trueHeartBeat.incrementAndGet();
        nbTrueHb++;
      }
      float percentTrueHb= (float) nbTrueHb/theoric;
      LOG.debug("percent hb " + node.getNodeID() + " " + percentHb + ", truehb "  + percentTrueHb);
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
        LOG.error("we should not get a sutdown: " + node.getNodeID()
                + " message: " + beatResponse.getDiagnosticsMessage());
        throw new YarnException("there should be no shutdown "
                + beatResponse.getDiagnosticsMessage());
//      lastStep();
      } else if (beatResponse.getNodeAction() == NodeAction.RESYNC) {
      LOG.error("the failover should be transparent, node: " + node.getNodeID()
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
      RESPONSE_ID = beatResponse.getResponseId();
    } catch (Exception ex) {
      if(ex instanceof InterruptedException){
        LOG.warn(ex, ex);
      }else if (ex instanceof IOException){
        throw (IOException) ex;
      }else {
        LOG.error(ex, ex);
              throw new YarnException(ex);
      }
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
    int completed =0;
    // add complete containers
    synchronized (completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completed+=completedContainerList.size();
      completedContainerList.clear();
    }
    // released containers
    synchronized (releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED));
      }
      completed += releasedContainerList.size();
      if (releasedContainerList.size() > 0) {
        LOG.debug(node.getNodeID() + "aborted containers "
                + releasedContainerList.size());
      }
      releasedContainerList.clear();
      if(completed>0){
        LOG.debug(node.getNodeID() + " completed containers " + completed);
      }
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
