/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;
import io.hops.metadata.yarn.entity.Load;
import io.hops.util.DBUtility;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplNotDist;

public class ResourceTrackerService extends AbstractService implements
    ResourceTracker {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final RMContainerTokenSecretManager containerTokenSecretManager;
  private final NMTokenSecretManagerInRM nmTokenSecretManager;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private long nextHeartBeatInterval;
  private Server server;
  private InetSocketAddress resourceTrackerAddress;
  private String minimumNodeManagerVersion;

  private int minAllocMb;
  private int minAllocVcores;
  private int minAllocGPUs;

  private boolean isDistributedNodeLabelsConf;
  private boolean isDelegatedCentralizedNodeLabelsConf;
  private DynamicResourceConfiguration drConf;
  private AtomicInteger load = new AtomicInteger(0);
  

  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmTokenSecretManager = nmTokenSecretManager;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    RackResolver.init(conf);
    nextHeartBeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (nextHeartBeatInterval <= 0) {
      throw new YarnRuntimeException("Invalid Configuration. "
          + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS
          + " should be larger than 0.");
    }

    minAllocMb = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    minAllocVcores = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    minAllocGPUs = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_GPUS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_GPUS);

    minimumNodeManagerVersion = conf.get(
        YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION);

    if (YarnConfiguration.areNodeLabelsEnabled(conf)) {
      isDistributedNodeLabelsConf =
          YarnConfiguration.isDistributedNodeLabelConfiguration(conf);
      isDelegatedCentralizedNodeLabelsConf =
          YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(conf);
    }

    loadDynamicResourceConfiguration(conf);
    super.serviceInit(conf);
  }

  /**
   * Load DynamicResourceConfiguration from dynamic-resources.xml.
   * @param conf
   * @throws IOException
   */
  public void loadDynamicResourceConfiguration(Configuration conf)
      throws IOException {
    try {
      // load dynamic-resources.xml
      InputStream drInputStream = this.rmContext.getConfigurationProvider()
          .getConfigurationInputStream(conf,
          YarnConfiguration.DR_CONFIGURATION_FILE);
      // write lock here on drConfig is unnecessary as here get called at
      // ResourceTrackerService get initiated and other read and write
      // operations haven't started yet.
      if (drInputStream != null) {
        this.drConf = new DynamicResourceConfiguration(conf, drInputStream);
      } else {
        this.drConf = new DynamicResourceConfiguration(conf);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Update DynamicResourceConfiguration with new configuration.
   * @param conf
   */
  public void updateDynamicResourceConfiguration(
      DynamicResourceConfiguration conf) {
    this.writeLock.lock();
    try {
      this.drConf = conf;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = rpc.getServer(
        ResourceTracker.class, this, resourceTrackerAddress, conf, null,
        conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        server.getListenerAddress());
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  /**
   * Helper method to handle received ContainerStatus. If this corresponds to
   * the completion of a master-container of a managed AM,
   * we call the handler for RMAppAttemptContainerFinishedEvent.
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  void handleNMContainerStatus(NMContainerStatus containerStatus, NodeId nodeId) {
    ApplicationAttemptId appAttemptId =
        containerStatus.getContainerId().getApplicationAttemptId();
    RMApp rmApp =
        rmContext.getRMApps().get(appAttemptId.getApplicationId());
    if (rmApp == null) {
      LOG.error("Received finished container : "
          + containerStatus.getContainerId()
          + " for unknown application " + appAttemptId.getApplicationId()
          + " Skipping.");
      return;
    }

    if (rmApp.getApplicationSubmissionContext().getUnmanagedAM()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring container completion status for unmanaged AM "
            + rmApp.getApplicationId());
      }
      return;
    }

    RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
    Container masterContainer = rmAppAttempt.getMasterContainer();
    if (masterContainer.getId().equals(containerStatus.getContainerId())
        && containerStatus.getContainerState() == ContainerState.COMPLETE) {
      ContainerStatus status =
          ContainerStatus.newInstance(containerStatus.getContainerId(),
            containerStatus.getContainerState(), containerStatus.getDiagnostics(),
            containerStatus.getContainerExitStatus());
      // sending master container finished event.
      RMAppAttemptContainerFinishedEvent evt =
          new RMAppAttemptContainerFinishedEvent(appAttemptId, status,
              nodeId);
      rmContext.getDispatcher().getEventHandler().handle(evt);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException,
      IOException {
    LOG.info("receive registration request");
    NodeId nodeId = request.getNodeId();
    String host = nodeId.getHost();
    int cmPort = nodeId.getPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();
    String nodeManagerVersion = request.getNMVersion();

    RegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(RegisterNodeManagerResponse.class);

    if (!minimumNodeManagerVersion.equals("NONE")) {
      if (minimumNodeManagerVersion.equals("EqualToRM")) {
        minimumNodeManagerVersion = YarnVersionInfo.getVersion();
      }

      if ((nodeManagerVersion == null) ||
          (VersionUtil.compareVersions(nodeManagerVersion,minimumNodeManagerVersion)) < 0) {
        String message =
            "Disallowed NodeManager Version " + nodeManagerVersion
                + ", is less than the minimum version "
                + minimumNodeManagerVersion + " sending SHUTDOWN signal to "
                + "NodeManager.";
        LOG.info(message);
        response.setDiagnosticsMessage(message);
        response.setNodeAction(NodeAction.SHUTDOWN);
        return response;
      }
    }

    // Check if this node is a 'valid' node
    if (!this.nodesListManager.isValidNode(host) &&
        !isNodeInDecommissioning(nodeId)) {
      String message =
          "Disallowed NodeManager from  " + host
              + ", Sending SHUTDOWN signal to the NodeManager.";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    // check if node's capacity is load from dynamic-resources.xml
    String nid = nodeId.toString();

    Resource dynamicLoadCapability = loadNodeResourceFromDRConfiguration(nid);
    if (dynamicLoadCapability != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resource for node: " + nid + " is adjusted from: " +
            capability + " to: " + dynamicLoadCapability +
            " due to settings in dynamic-resources.xml.");
      }
      capability = dynamicLoadCapability;
      // sync back with new resource.
      response.setResource(capability);
    }

    // Check if this node has minimum allocations
    if (capability.getMemorySize() < minAllocMb
        || capability.getVirtualCores() < minAllocVcores
        || capability.getGPUs() < minAllocGPUs) {
      String message =
          "NodeManager from  " + host
              + " doesn't satisfy minimum allocations, Sending SHUTDOWN"
              + " signal to the NodeManager.";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    response.setContainerTokenMasterKey(containerTokenSecretManager
        .getCurrentKey());
    response.setNMTokenMasterKey(nmTokenSecretManager
        .getCurrentKey());

    //TODO get the class to use from the config file
    RMNode rmNode;
    if (!rmContext.isDistributed()) {
      rmNode = new RMNodeImplNotDist(nodeId, rmContext, host, cmPort, httpPort,
              resolve(host), capability, nodeManagerVersion);
    } else {
      rmNode = new RMNodeImplDist(nodeId, rmContext, host, cmPort, httpPort,
              resolve(host), capability, nodeManagerVersion);
    }
    RMNode oldNode = this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode);
    Map<ApplicationId, UpdatedCryptoForApp> runningsAppsWithCryptoVersion = request.getRunningApplications();
    List<ApplicationId> runningApplications = new ArrayList<>(runningsAppsWithCryptoVersion.size());
    runningApplications.addAll(runningsAppsWithCryptoVersion.keySet());
    if (oldNode == null) {
      this.rmContext.getDispatcher().getEventHandler().handle(
              new RMNodeStartedEvent(nodeId, request.getNMContainerStatuses(), runningApplications));
      pushCryptoUpdatedEventsForRunningApps(runningsAppsWithCryptoVersion, rmNode);
    } else {
      LOG.info("Reconnect from the node at: " + host);
      this.nmLivelinessMonitor.unregister(nodeId);
      if (this.rmContext.isDistributed()) {
        load.decrementAndGet();
        DBUtility.updateLoad(new Load(rmContext.getGroupMembershipService().
                getRMId(), load.get()));
      }
      // Reset heartbeat ID since node just restarted.
      oldNode.resetLastNodeHeartBeatResponse();
      this.rmContext
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMNodeReconnectEvent(nodeId, rmNode, runningApplications, request.getNMContainerStatuses()));
      pushCryptoUpdatedEventsForRunningApps(runningsAppsWithCryptoVersion, oldNode);
    }
    // On every node manager register we will be clearing NMToken keys if
    // present for any running application.
    this.nmTokenSecretManager.removeNodeKey(nodeId);
    this.nmLivelinessMonitor.register(nodeId);
    if (this.rmContext.isDistributed()) {
      load.incrementAndGet();
      DBUtility.updateLoad(new Load(rmContext.getGroupMembershipService().
              getRMId(), load.get()));
    }
    // Handle received container status, this should be processed after new
    // RMNode inserted
    if (!rmContext.isWorkPreservingRecoveryEnabled()) {
      if (!request.getNMContainerStatuses().isEmpty()) {
        LOG.info("received container statuses on node manager register :"
            + request.getNMContainerStatuses());
        for (NMContainerStatus status : request.getNMContainerStatuses()) {
          handleNMContainerStatus(status, nodeId);
        }
      }
    }

    // Update node's labels to RM's NodeLabelManager.
    Set<String> nodeLabels = NodeLabelsUtils.convertToStringSet(
        request.getNodeLabels());
    if (isDistributedNodeLabelsConf && nodeLabels != null) {
      try {
        updateNodeLabelsFromNMReport(nodeLabels, nodeId);
        response.setAreNodeLabelsAcceptedByRM(true);
      } catch (IOException ex) {
        // Ensure the exception is captured in the response
        response.setDiagnosticsMessage(ex.getMessage());
        response.setAreNodeLabelsAcceptedByRM(false);
      }
    } else if (isDelegatedCentralizedNodeLabelsConf) {
      this.rmContext.getRMDelegatedNodeLabelsUpdater().updateNodeLabels(nodeId);
    }

    StringBuilder message = new StringBuilder();
    message.append("NodeManager from node ").append(host).append("(cmPort: ")
        .append(cmPort).append(" httpPort: ");
    message.append(httpPort).append(") ")
        .append("registered with capability: ").append(capability);
    message.append(", assigned nodeId ").append(nodeId);
    if (response.getAreNodeLabelsAcceptedByRM()) {
      message.append(", node labels { ").append(
          StringUtils.join(",", nodeLabels) + " } ");
    }

    LOG.info(message.toString());
    response.setNodeAction(NodeAction.NORMAL);
    response.setRMIdentifier(ResourceManager.getClusterTimeStamp());
    response.setRMVersion(YarnVersionInfo.getVersion());
    return response;
  }
  
  private void pushCryptoUpdatedEventsForRunningApps(Map<ApplicationId, UpdatedCryptoForApp> runningApps, RMNode rmNode) {
    if (!isHopsTLSEnabled() && !isJWTEnabled()) {
      return;
    }
    for (Map.Entry<ApplicationId, UpdatedCryptoForApp> entry : runningApps.entrySet()) {
      ApplicationId appId = entry.getKey();
      RMApp rmApp = rmContext.getRMApps().get(appId);
      if (rmApp != null) {
        if (isHopsTLSEnabled()) {
          Integer nmCryptoMaterialVersion = entry.getValue().getVersion();
          if (rmApp.getCryptoMaterialVersion() > nmCryptoMaterialVersion) {
            ByteBuffer keyStore = ByteBuffer.wrap(rmApp.getKeyStore());
            char[] keyStorePassword = rmApp.getKeyStorePassword();
            ByteBuffer trustStore = ByteBuffer.wrap(rmApp.getTrustStore());
            char[] trustStorePassword = rmApp.getTrustStorePassword();
            int cryptoVersion = rmApp.getCryptoMaterialVersion();
            UpdatedCryptoForApp updatedCrypto = recordFactory.newRecordInstance(UpdatedCryptoForApp.class);
            updatedCrypto.setKeyStore(keyStore);
            updatedCrypto.setKeyStorePassword(keyStorePassword);
            updatedCrypto.setTrustStore(trustStore);
            updatedCrypto.setTrustStorePassword(trustStorePassword);
            updatedCrypto.setVersion(cryptoVersion);
            rmNode.getAppX509ToUpdate().putIfAbsent(appId, updatedCrypto);
          }
        }
        
        if (isJWTEnabled()) {
          long nmJWTExpiration = entry.getValue().getJWTExpiration();
          if (rmApp.getJWTExpiration().isAfter(Instant.ofEpochMilli(nmJWTExpiration))) {
            UpdatedCryptoForApp updateJWT = recordFactory.newRecordInstance(UpdatedCryptoForApp.class);
            updateJWT.setJWT(rmApp.getJWT());
            updateJWT.setJWTExpiration(rmApp.getJWTExpiration().toEpochMilli());
            rmNode.getAppJWTToUpdate().putIfAbsent(appId, updateJWT);
          }
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException {

    NodeStatus remoteNodeStatus = request.getNodeStatus();
    /**
     * Here is the node heartbeat sequence...
     * 1. Check if it's a valid (i.e. not excluded) node
     * 2. Check if it's a registered node
     * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
     * 4. Send healthStatus to RMNode
     * 5. Update node's labels if distributed Node Labels configuration is enabled
     */

    NodeId nodeId = remoteNodeStatus.getNodeId();

    // 1. Check if it's a valid (i.e. not excluded) node, if not, see if it is
    // in decommissioning.
    if (!this.nodesListManager.isValidNode(nodeId.getHost())
        && !isNodeInDecommissioning(nodeId)) {
      String message =
          "Disallowed NodeManager nodeId: " + nodeId + " hostname: "
              + nodeId.getHost();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(
          NodeAction.SHUTDOWN, message);
    }

    // 2. Check if it's a registered node
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      /* node does not exist */
      String message = "Node not found resyncing " + remoteNodeStatus.getNodeId();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }

    // Send ping
    this.nmLivelinessMonitor.receivedPing(nodeId);
    
    if (isHopsTLSEnabled()) {
      Set<ApplicationId> updatedApps = request.getUpdatedApplicationsWithNewCryptoMaterial();
      if (updatedApps != null) {
        for (ApplicationId appId : updatedApps) {
          rmNode.getAppX509ToUpdate().remove(appId);
          RMApp rmApp = rmContext.getRMApps().get(appId);
          rmApp.rmNodeHasUpdatedCryptoMaterial(rmNode.getNodeID());
        }
      }
    }

    // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
    NodeHeartbeatResponse lastNodeHeartbeatResponse = rmNode.getLastNodeHeartBeatResponse();
    if (remoteNodeStatus.getResponseId() + 1 == lastNodeHeartbeatResponse
        .getResponseId()) {
      LOG.info("Received duplicate heartbeat from node "
          + rmNode.getNodeAddress()+ " responseId=" + remoteNodeStatus.getResponseId());
      return lastNodeHeartbeatResponse;
    } else if (remoteNodeStatus.getResponseId() + 1 < lastNodeHeartbeatResponse
        .getResponseId()) {
      String message =
          "Too far behind rm response id:"
              + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
              + remoteNodeStatus.getResponseId();
      LOG.info(message);
      // TODO: Just sending reboot is not enough. Think more.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }

    // Heartbeat response
    NodeHeartbeatResponse nodeHeartBeatResponse = YarnServerBuilderUtils
        .newNodeHeartbeatResponse(lastNodeHeartbeatResponse.
            getResponseId() + 1, NodeAction.NORMAL, null, null, null, null,
            nextHeartBeatInterval);
    rmNode.updateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);
    rmNode.updateNodeHeartbeatResponseForContainersDecreasing(
        nodeHeartBeatResponse);

    populateKeys(request, nodeHeartBeatResponse);
    if (isHopsTLSEnabled() || isJWTEnabled()) {
      Map<ApplicationId, UpdatedCryptoForApp> mergedUpdates = mergeNewSecurityMaterialForApps(rmNode);
      nodeHeartBeatResponse.setUpdatedCryptoForApps(mergedUpdates);
    }
    
    ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
        rmContext.getSystemCredentialsForApps();
    if (!systemCredentials.isEmpty()) {
      nodeHeartBeatResponse.setSystemCredentialsForApps(systemCredentials);
    }

    nodeHeartBeatResponse.setNextheartbeat(((RMNodeImpl) rmNode).
            getNextHeartbeat());
    // 4. Send status to RMNode, saving the latest response.
    RMNodeStatusEvent nodeStatusEvent =
        new RMNodeStatusEvent(nodeId, remoteNodeStatus, nodeHeartBeatResponse);
    if (request.getLogAggregationReportsForApps() != null
        && !request.getLogAggregationReportsForApps().isEmpty()) {
      nodeStatusEvent.setLogAggregationReportsForApps(request
        .getLogAggregationReportsForApps());
    }
    this.rmContext.getDispatcher().getEventHandler().handle(nodeStatusEvent);

    // 5. Update node's labels to RM's NodeLabelManager.
    if (isDistributedNodeLabelsConf && request.getNodeLabels() != null) {
      try {
        updateNodeLabelsFromNMReport(
            NodeLabelsUtils.convertToStringSet(request.getNodeLabels()),
            nodeId);
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(true);
      } catch (IOException ex) {
        //ensure the error message is captured and sent across in response
        nodeHeartBeatResponse.setDiagnosticsMessage(ex.getMessage());
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(false);
      }
    }

    // 6. check if node's capacity is load from dynamic-resources.xml
    // if so, send updated resource back to NM.
    String nid = nodeId.toString();
    Resource capability = loadNodeResourceFromDRConfiguration(nid);
    // sync back with new resource if not null.
    if (capability != null) {
      nodeHeartBeatResponse.setResource(capability);
    }

    return nodeHeartBeatResponse;
  }
  
  // TODO(Antonis): Replace with Stream.concat when we upgrade to Java 8 (HADOOP-11858)
  @InterfaceAudience.Private
  @VisibleForTesting
  protected Map<ApplicationId, UpdatedCryptoForApp> mergeNewSecurityMaterialForApps(RMNode rmNode) {
    Map<ApplicationId, UpdatedCryptoForApp> x509Updates = rmNode.getAppX509ToUpdate();
    final Map<ApplicationId, UpdatedCryptoForApp> jwtUpdates = rmNode.getAppJWTToUpdate();
    Map<ApplicationId, UpdatedCryptoForApp> mergedUpdates = new HashMap<>();
    List<ApplicationId> mergedJWTUpdates = new ArrayList<>();
    
    for (Map.Entry<ApplicationId, UpdatedCryptoForApp> x509Update : x509Updates.entrySet()) {
      ApplicationId appId = x509Update.getKey();
      UpdatedCryptoForApp update = x509Update.getValue();
      if (jwtUpdates.containsKey(appId)) {
        UpdatedCryptoForApp jwtUpdate = jwtUpdates.get(appId);
        update.setJWT(jwtUpdate.getJWT());
        update.setJWTExpiration(jwtUpdate.getJWTExpiration());
        mergedJWTUpdates.add(appId);
      }
      mergedUpdates.put(appId, update);
    }
    
    for (Map.Entry<ApplicationId, UpdatedCryptoForApp> jwtUpdate : jwtUpdates.entrySet()) {
      ApplicationId appId = jwtUpdate.getKey();
      UpdatedCryptoForApp update = jwtUpdate.getValue();
      if (!mergedUpdates.containsKey(appId)) {
        mergedUpdates.put(appId, update);
        mergedJWTUpdates.add(appId);
      }
    }
    
    // For JWT we don't wait for confirmation
    for (ApplicationId appId : mergedJWTUpdates) {
      jwtUpdates.remove(appId);
    }
    
    return mergedUpdates;
  }
  
  /**
   * Check if node in decommissioning state.
   * @param nodeId
   */
  private boolean isNodeInDecommissioning(NodeId nodeId) {
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode != null &&
        rmNode.getState().equals(NodeState.DECOMMISSIONING)) {
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(UnRegisterNodeManagerResponse.class);
    NodeId nodeId = request.getNodeId();
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      LOG.info("Node not found, ignoring the unregister from node id : "
          + nodeId);
      return response;
    }
    LOG.info("Node with node id : " + nodeId
        + " has shutdown, hence unregistering the node.");
    this.nmLivelinessMonitor.unregister(nodeId);
    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeEvent(nodeId, RMNodeEventType.SHUTDOWN));
    return response;
  }

  private void updateNodeLabelsFromNMReport(Set<String> nodeLabels,
      NodeId nodeId) throws IOException {
    try {
      Map<NodeId, Set<String>> labelsUpdate =
          new HashMap<NodeId, Set<String>>();
      labelsUpdate.put(nodeId, nodeLabels);
      this.rmContext.getNodeLabelManager().replaceLabelsOnNode(labelsUpdate);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node Labels {" + StringUtils.join(",", nodeLabels)
            + "} from Node " + nodeId + " were Accepted from RM");
      }
    } catch (IOException ex) {
      StringBuilder errorMessage = new StringBuilder();
      errorMessage.append("Node Labels {")
          .append(StringUtils.join(",", nodeLabels))
          .append("} reported from NM with ID ").append(nodeId)
          .append(" was rejected from RM with exception message as : ")
          .append(ex.getMessage());
      LOG.error(errorMessage, ex);
      throw new IOException(errorMessage.toString(), ex);
    }
  }

  private void populateKeys(NodeHeartbeatRequest request,
      NodeHeartbeatResponse nodeHeartBeatResponse) {

    // Check if node's masterKey needs to be updated and if the currentKey has
    // roller over, send it across

    // ContainerTokenMasterKey

    MasterKey nextMasterKeyForNode =
        this.containerTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownContainerTokenMasterKey().getKeyId()
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setContainerTokenMasterKey(nextMasterKeyForNode);
    }

    // NMTokenMasterKey

    nextMasterKeyForNode = this.nmTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownNMTokenMasterKey().getKeyId() 
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setNMTokenMasterKey(nextMasterKeyForNode);
    }
  }

  private Resource loadNodeResourceFromDRConfiguration(String nodeId) {
    // check if node's capacity is loaded from dynamic-resources.xml
    this.readLock.lock();
    try {
      String[] nodes = this.drConf.getNodes();
      if (nodes != null && Arrays.asList(nodes).contains(nodeId)) {
        return Resource.newInstance(this.drConf.getMemoryPerNode(nodeId),
            this.drConf.getVcoresPerNode(nodeId));
      } else {
        return null;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
  
  private boolean isHopsTLSEnabled() {
    return getConfig().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  }
  
  private boolean isJWTEnabled() {
    return getConfig().getBoolean(YarnConfiguration.RM_JWT_ENABLED,
        YarnConfiguration.DEFAULT_RM_JWT_ENABLED);
  }
}
