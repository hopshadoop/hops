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
package org.apache.hadoop.distributedloadsimulator.sls.appmaster;

/**
 *
 * @author sri
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.distributedloadsimulator.sls.AMNMCommonObject;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.SLSRunner;
import org.apache.hadoop.distributedloadsimulator.sls.conf.SLSConfiguration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

public class MRAMSimulator extends AMSimulator {
  /*
   * Vocabulary Used:
   * pending -> requests which are NOT yet sent to RM
   * scheduled -> requests which are sent to RM but not yet assigned
   * assigned -> requests which are assigned to a container
   * completed -> request corresponding to which container has completed
   *
   * Maps are scheduled as soon as their requests are received. Reduces are
   * scheduled when all maps have finished (not support slow-start currently).
   */

  private static final int PRIORITY_REDUCE = 10;
  private static final int PRIORITY_MAP = 20;

  // pending maps
  private LinkedList<ContainerSimulator> pendingMaps
          = new LinkedList<ContainerSimulator>();

  // pending failed maps
  private LinkedList<ContainerSimulator> pendingFailedMaps
          = new LinkedList<ContainerSimulator>();

  // scheduled maps
  private LinkedList<ContainerSimulator> scheduledMaps
          = new LinkedList<ContainerSimulator>();

  // assigned maps
  private Map<ContainerId, ContainerSimulator> assignedMaps
          = new HashMap<ContainerId, ContainerSimulator>();

  // reduces which are not yet scheduled
  private LinkedList<ContainerSimulator> pendingReduces
          = new LinkedList<ContainerSimulator>();

  // pending failed reduces
  private LinkedList<ContainerSimulator> pendingFailedReduces
          = new LinkedList<ContainerSimulator>();

  // scheduled reduces
  private LinkedList<ContainerSimulator> scheduledReduces
          = new LinkedList<ContainerSimulator>();

  // assigned reduces
  private Map<ContainerId, ContainerSimulator> assignedReduces
          = new HashMap<ContainerId, ContainerSimulator>();

  // all maps & reduces
  private LinkedList<ContainerSimulator> allMaps
          = new LinkedList<ContainerSimulator>();
  private LinkedList<ContainerSimulator> allReduces
          = new LinkedList<ContainerSimulator>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // waiting for AM container 
  private boolean isAMContainerRunning = false;
  private Container amContainer;
  // finished
  private boolean isFinished = false;
  // resource for AM container
  private static int MR_AM_CONTAINER_RESOURCE_MEMORY_MB;
  private static int MR_AM_CONTAINER_RESOURCE_VCORES;

  private static final Log LOG = LogFactory.getLog(MRAMSimulator.class);

  private long applicationMasterWaitTime;
  private List<Long> containerAllocationWaitTime = new ArrayList<Long>();
  private List<Long> containerStartWaitTime = new ArrayList<Long>();
  private long startRequestingContainers;
  private boolean firstRequest = true;
  private List tasks;

  public void init(int id, int heartbeatInterval,
          List tasks, ResourceManager rm, SLSRunner se,
          long traceStartTime, long traceFinishTime, String user, String queue,
          boolean isTracked, String oldAppId,
          String[] remoteSimIp, int rmiPort, YarnClient rmClient, Configuration conf) throws
          IOException {
    super.init(id, heartbeatInterval, tasks, rm, se,
            traceStartTime, traceFinishTime, user, queue,
            isTracked, oldAppId, remoteSimIp, rmiPort,
            rmClient, conf);
    amtype = "mapreduce";

    this.tasks = tasks;

    conf.addResource("sls-runner.xml");
    MR_AM_CONTAINER_RESOURCE_MEMORY_MB = conf.getInt(
            SLSConfiguration.CONTAINER_MEMORY_MB,
            SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    MR_AM_CONTAINER_RESOURCE_VCORES = conf.getInt(
            SLSConfiguration.CONTAINER_VCORES,
            SLSConfiguration.CONTAINER_VCORES_DEFAULT);
  }

  @Override
  public void firstStep()
          throws YarnException, IOException, InterruptedException {
    int containerVCores = conf.getInt(SLSConfiguration.CONTAINER_VCORES,
            SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    int containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
            SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    Resource containerResource = BuilderUtils.newResource(containerMemoryMB,
            containerVCores);

    List<ContainerSimulator> containerList
            = new ArrayList<ContainerSimulator>();
    for (Object o : tasks) {
      Map jsonTask = (Map) o;
      String hostname = jsonTask.get("container.host").toString();
      long taskStart = Long.parseLong(
              jsonTask.get("container.start.ms").toString());
      long taskFinish = Long.parseLong(
              jsonTask.get("container.end.ms").toString());
      long lifeTime = taskFinish - taskStart;
      int priority = Integer.parseInt(
              jsonTask.get("container.priority").toString());
      String type = jsonTask.get("container.type").toString();
      containerList.add(new ContainerSimulator(containerResource,
              lifeTime, hostname, priority, type));
    }

    // get map/reduce tasks
    for (ContainerSimulator cs : containerList) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        pendingMaps.add(cs);
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(cs);
      }
    }
    allMaps.addAll(pendingMaps);
    allReduces.addAll(pendingReduces);
    mapTotal = pendingMaps.size();
    reduceTotal = pendingReduces.size();
    totalContainers = mapTotal + reduceTotal;

    super.firstStep();
    if (submited) {
      requestAMContainer();
    }
  }

  Map<String, Long> ContainersStartTimes = new HashMap<String, Long>();
  /**
   * send out request for AM container
   *
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   * @throws java.lang.InterruptedException
   */
  protected void requestAMContainer()
          throws YarnException, IOException, InterruptedException {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest amRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            ResourceRequest.ANY, 1, 1);
    ask.add(amRequest);
    final AllocateRequest request = this.createAllocateRequest(ask);

    AllocateResponse response = null;
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(
            appAttemptId.toString(), UserGroupInformation.getCurrentUser(),
            false);
    ugi.setAuthenticationMethod(SaslRpcServer.AuthMethod.TOKEN);
    ugi.addCredentials(credentials);
    ugi.addToken(amRMToken);
    ugi.addTokenIdentifier(amRMToken.decodeIdentifier());
    response = ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {

      @Override
      public AllocateResponse run() throws Exception {
        UserGroupInformation.getCurrentUser().addToken(amRMToken);
        InetSocketAddress resourceManagerAddress = conf.getSocketAddr(
                YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
        SecurityUtil.setTokenService(amRMToken, resourceManagerAddress);
        ApplicationMasterProtocol appMasterProtocol = ClientRMProxy.
                createRMProxy(conf, ApplicationMasterProtocol.class, true);
        AllocateResponse response = appMasterProtocol.allocate(request);
        RPC.stopProxy(appMasterProtocol);
        return response;
      }
    });

    // waiting until the AM container is allocated
    while (true) {

      if (response != null && !response.getAllocatedContainers().isEmpty()) {
        // get AM container
        Container container = response.getAllocatedContainers().get(0);
        for (AMNMCommonObject remoteConnection : RemoteConnections) {
          if (remoteConnection.isNodeExist(container.getNodeId().toString())) {
            remoteConnection.addNewContainer(
                    container.getId().toString(),
                    container.getNodeId().toString(),
                    container.getNodeHttpAddress(),
                    container.getResource().getMemory(),
                    container.getResource().getVirtualCores(),
                    container.getPriority().getPriority(), -1L);
          }
        }
        // start AM container
        ContainersStartTimes.put(container.getId().toString(), System.currentTimeMillis());
        amContainer = container;
        isAMContainerRunning = true;
        applicationMasterWaitTime = System.currentTimeMillis()
                - applicationStartTime;
        try {
          primaryRemoteConnection.addApplicationMasterWaitTime(
                  applicationMasterWaitTime);
        } catch (RemoteException e) {
          LOG.error(e, e);
        }
        break;
      }
      // this sleep time is different from HeartBeat
      Thread.sleep(1000);
      // send out empty request
      sendContainerRequest();
      response = responseQueue.take();
    }

  }

  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue()
          throws InterruptedException, YarnException, IOException {
    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            totalContainersDuration = totalContainersDuration + System.currentTimeMillis() - ContainersStartTimes.get(cs.getContainerId().toString());
            if (assignedMaps.containsKey(containerId)) {

              assignedMaps.remove(containerId);
              mapFinished++;
              finishedContainers++;
            } else if (assignedReduces.containsKey(containerId)) {
              assignedReduces.remove(containerId);
              reduceFinished++;
              finishedContainers++;
            } else {
              // am container released event
              isFinished = true;
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              pendingFailedReduces.add(assignedReduces.remove(containerId));
            } else {
              restart();
            }
          }
        }
      }

      // check finished
      if (isAMContainerRunning
              && (mapFinished == mapTotal)
              && (reduceFinished == reduceTotal)) {
        // to release the AM container
        for (AMNMCommonObject remoteConnection : RemoteConnections) {
          if (remoteConnection.isNodeExist(amContainer.getNodeId().toString())) {
            remoteConnection.cleanupContainer(amContainer.getId().toString(),
                    amContainer.getNodeId().toString());

          }
        }
        isAMContainerRunning = false;
        isFinished = true;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        ContainersStartTimes.put(container.getId().toString(), System.currentTimeMillis());
        if (!scheduledMaps.isEmpty()) {
          ContainerSimulator cs = scheduledMaps.remove();
          assignedMaps.put(container.getId(), cs);
          containerAllocationWaitTime.add(System.currentTimeMillis()
                  - startRequestingContainers);
          containerStartWaitTime.add(System.currentTimeMillis()
                  - applicationStartTime);
          try {
            primaryRemoteConnection.addContainerAllocationWaitTime(System.
                    currentTimeMillis() - startRequestingContainers);
            primaryRemoteConnection.addContainerStartWaitTime(System.
                    currentTimeMillis() - applicationStartTime);
          } catch (RemoteException e) {
            LOG.error(e, e);
          }
          for (AMNMCommonObject remoteConnection : RemoteConnections) {
            if (remoteConnection.isNodeExist(container.getNodeId().toString())) {
              remoteConnection.addNewContainer(
                      container.getId().toString(),
                      container.getNodeId().toString(),
                      container.getNodeHttpAddress(),
                      container.getResource().getMemory(),
                      container.getResource().getVirtualCores(),
                      container.getPriority().getPriority(), cs.getLifeTime());
            }
          }
        } else if (!this.scheduledReduces.isEmpty()) {
          ContainerSimulator cs = scheduledReduces.remove();
          assignedReduces.put(container.getId(), cs);
          for (AMNMCommonObject remoteConnection : RemoteConnections) {
            if (remoteConnection.isNodeExist(container.getNodeId().toString())) {
              remoteConnection.addNewContainer(
                      container.getId().toString(),
                      container.getNodeId().toString(),
                      container.getNodeHttpAddress(),
                      container.getResource().getMemory(),
                      container.getResource().getVirtualCores(),
                      container.getPriority().getPriority(), cs.getLifeTime());
            }
          }
        }
      }
    }
  }

  /**
   * restart running because of the am container killed
   */
  private void restart()
          throws YarnException, IOException, InterruptedException {
    // clear 
    finishedContainers = 0;
    isFinished = false;
    mapFinished = 0;
    reduceFinished = 0;
    pendingFailedMaps.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    pendingFailedReduces.clear();
    pendingMaps.addAll(allMaps);
    pendingReduces.addAll(pendingReduces);
    isAMContainerRunning = false;
    amContainer = null;
    // resent am container request
    requestAMContainer();
  }

  @Override
  protected void sendContainerRequest()
          throws YarnException, IOException, InterruptedException {
    if (isFinished) {
      return;
    }

    //LOG.info("HOP :: Send container request ");
    // send out request
    List<ResourceRequest> ask = null;
    if (isAMContainerRunning) {
      if (mapFinished != mapTotal) {
        // map phase
        if (!pendingMaps.isEmpty()) {
          ask = packageRequests(pendingMaps, PRIORITY_MAP);
          scheduledMaps.addAll(pendingMaps);
          pendingMaps.clear();
        } else if (!pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          ask = packageRequests(pendingFailedMaps, PRIORITY_MAP);
          // pendingFailedMaps.size()));
          scheduledMaps.addAll(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
      } else if (reduceFinished != reduceTotal) {
        // reduce phase
        if (!pendingReduces.isEmpty()) {
          ask = packageRequests(pendingReduces, PRIORITY_REDUCE);
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (!pendingFailedReduces.isEmpty()
                && scheduledReduces.isEmpty()) {
          ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE);
          // pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
      if (firstRequest) {
        firstRequest = false;
        startRequestingContainers = System.currentTimeMillis();
      }
    }
    if (ask == null) {
      ask = new ArrayList<ResourceRequest>();
    }

    final AllocateRequest request = createAllocateRequest(ask);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }
    if (ask.size() > 0) {
      int nbContainers = 0;
      for (ResourceRequest r : ask) {
        nbContainers += r.getNumContainers();
      }
      LOG.
              info("application " + appId + " requesting containers "
                      + nbContainers);
    }

    AllocateResponse response = null;
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(
            appAttemptId.toString(), UserGroupInformation.getCurrentUser(),
            false);
    ugi.setAuthenticationMethod(SaslRpcServer.AuthMethod.TOKEN);
    ugi.addCredentials(credentials);
    ugi.addToken(amRMToken);
    ugi.addTokenIdentifier(amRMToken.decodeIdentifier());
    response = ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {

      @Override
      public AllocateResponse run() throws Exception {
        UserGroupInformation.getCurrentUser().addToken(amRMToken);
        InetSocketAddress resourceManagerAddress = conf.getSocketAddr(
                YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
        SecurityUtil.setTokenService(amRMToken, resourceManagerAddress);
        ApplicationMasterProtocol appMasterProtocol = ClientRMProxy.
                createRMProxy(conf, ApplicationMasterProtocol.class, true);
        AllocateResponse response = appMasterProtocol.allocate(request);
        RPC.stopProxy(appMasterProtocol);
        return response;
      }
    });

    if (response != null) {
      responseQueue.put(response);
    }
  }

  @Override
  protected void checkStop() {
    if (isFinished) {
      super.setEndTime(System.currentTimeMillis());
    }
  }

  @Override
  public void lastStep() throws YarnException {
    //LOG.info(MessageFormat.format("Application reaching to laststep {0}.", appId));
    super.lastStep();

    // clear data structures
    allMaps.clear();
    allReduces.clear();
    assignedMaps.clear();
    assignedReduces.clear();
    pendingFailedMaps.clear();
    pendingFailedReduces.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    scheduledMaps.clear();
    scheduledReduces.clear();
    responseQueue.clear();

  }

}
