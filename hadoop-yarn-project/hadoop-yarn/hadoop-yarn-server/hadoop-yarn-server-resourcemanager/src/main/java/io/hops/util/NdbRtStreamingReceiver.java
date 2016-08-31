package io.hops.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by antonis on 8/22/16.
 */
public class NdbRtStreamingReceiver {

    private final static Log LOG = LogFactory.getLog(NdbRtStreamingReceiver.class);

    // TODO: For the moment currentPrice and currentPriceTick are skipped

    public static BlockingQueue<StreamingRTComps> receivedRTEvents =
            new ArrayBlockingQueue<StreamingRTComps>(100000, true);

    private Map<String, Set<ContainerId>> containersToClean = null;
    private Map<String, List<ApplicationId>> finishedAppsList = null;
    private String containerId = null;
    private String applicationId = null;
    private Set<String> nodeIds = null;
    private Map<String, Boolean> nextHeartbeatMap = null;
    private boolean nextHeartbeat = false;
    private String nextHeartbeatNodeId = null;
    private int finishedAppPendingId = 0;
    private int cidToCleanPendingId = 0;
    private int nextHeartbeatPendingId = 0;
    private String cidToCleanRMNodeId = null;
    private String finishedAppRMNodeId = null;
    private List<ContainerStatus> hopContainerStatusList = null;

    private MasterKey currentNMMasterKey = null;
    private MasterKey nextNMMasterKey = null;
    private MasterKey currentRMContainerMasterKey = null;
    private MasterKey nextRMContainerMasterKey = null;

    private String keyId = "";
    private byte[] keyBytes = null;

    public NdbRtStreamingReceiver() {
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public void setFinishedAppPendingId(int finishedAppPendingId) {
        this.finishedAppPendingId = finishedAppPendingId;
    }

    public void setCidToCleanPendingId(int cidToCleanPendingId) {
        this.cidToCleanPendingId = cidToCleanPendingId;
    }

    public void setNextHBPendingId(int nextHeartbeatPendingId) {
        this.nextHeartbeatPendingId = nextHeartbeatPendingId;
    }

    public void setCidToCleanForRMNodeId(String rmNodeId) {
        this.cidToCleanRMNodeId = rmNodeId;
        nodeIds.add(rmNodeId);
    }

    public void buildNodeIds() {
        nodeIds = new HashSet<>();
    }

    public void buildContainersToClean() {
        containersToClean = new HashMap<>();
    }

    public void addContainersToClean() {
        ContainerId addContainerId = ConverterUtils.toContainerId(containerId);
        Set<ContainerId> containerIds = containersToClean.get(cidToCleanRMNodeId);
        if (containerIds == null) {
            containerIds = new HashSet<>();
            containersToClean.put(cidToCleanRMNodeId, containerIds);
        }
        containerIds.add(addContainerId);
    }

    public void buildFinishedApplications() {
        finishedAppsList = new HashMap<>();
    }

    public void setFinishedAppRMNodeId(String rmNodeId) {
        this.finishedAppRMNodeId = rmNodeId;
        nodeIds.add(rmNodeId);
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public void addFinishedApps() {
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        List<ApplicationId> finishedApps = finishedAppsList.get(finishedAppRMNodeId);

        if (finishedApps == null) {
            finishedApps = new ArrayList<>();
            finishedAppsList.put(finishedAppRMNodeId, finishedApps);
        }
        finishedApps.add(appId);

        LOG.debug("finishedApplications appid: " + appId + " pendingId: "
                + finishedAppPendingId + " RMNodeId: " + finishedAppRMNodeId);
    }

    public void buildNextHeartbeatMap() {
        nextHeartbeatMap = new HashMap<>();
    }

    public void setNodeId(String nodeId) {
        LOG.debug("Set nextHeartbeatNodeId: " + nodeId);
        this.nextHeartbeatNodeId = nodeId;
        nodeIds.add(nodeId);
    }

    public void setNextHeartbeat(int nextHeartbeat) {
      if(nextHeartbeat==0){
        this.nextHeartbeat = false;
      }else{
        this.nextHeartbeat = true;
      }
    }

    public void addNextHeartbeat() {
        nextHeartbeatMap.put(nextHeartbeatNodeId, nextHeartbeat);
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public void setKeyBytes(byte[] keyBytes) {
        this.keyBytes = keyBytes;
    }

    public void setKey() {
        try {
            RMNodeImplDist.KeyType keyType = RMNodeImplDist.KeyType.valueOf(keyId);
            MasterKey key = new MasterKeyPBImpl(YarnServerCommonProtos
                    .MasterKeyProto.parseFrom(keyBytes));

            switch (keyType) {
                case CURRENTNMTOKENMASTERKEY:
                    currentNMMasterKey = key;
                    break;
                case NEXTNMTOKENMASTERKEY:
                    nextNMMasterKey = key;
                    break;
                case CURRENTCONTAINERTOKENMASTERKEY:
                    currentRMContainerMasterKey = key;
                    break;
                case NEXTCONTAINERTOKENMASTERKEY:
                    nextRMContainerMasterKey = key;
            }
        } catch (InvalidProtocolBufferException ex) {
            LOG.error(ex, ex);
        }
    }

    int numOfEvents = 0;
    long lastTimestamp = 0;

    public void onEventMethod() throws InterruptedException {
        StreamingRTComps streamingRTComps = new StreamingRTComps(
                containersToClean, finishedAppsList, nodeIds,
                nextHeartbeatMap, hopContainerStatusList,
                currentNMMasterKey, nextNMMasterKey,
                currentRMContainerMasterKey, nextRMContainerMasterKey);

        receivedRTEvents.put(streamingRTComps);
        numOfEvents++;

        if ((System.currentTimeMillis() - lastTimestamp) >= 1000) {
            LOG.error("*** <Profiler> Received " + numOfEvents + " per second");
            numOfEvents = 0;
            lastTimestamp = System.currentTimeMillis();
        }
    }

    // Build container status
    private String hopContainerStatusContainerId = "";
    private String hopContainerStatusState = "";
    private String hopContainerStatusDiagnostics = "";
    private int hopContainerStatusExitStatus = 0;
    private String hopContainerStatusRMNodeId = "";
    private int hopContainerStatusPendingId = 0;
    private int hopContainerStatusUciId = 0;

    public void setHopContainerStatusContainerId(
            String hopContainerStatusContainerId) {
        this.hopContainerStatusContainerId = hopContainerStatusContainerId;
    }

    public void setHopContainerStatusState(String hopContainerStatusState) {
        this.hopContainerStatusState = hopContainerStatusState;
    }

    public void setHopContainerStatusPendingId(int hopContainerStatusPendingId) {
        this.hopContainerStatusPendingId = hopContainerStatusPendingId;
    }

    public void setHopContainerStatusDiagnostics(String hopContainerStatusDiagnostics) {
        this.hopContainerStatusDiagnostics = hopContainerStatusDiagnostics;
    }

    public void setHopContainerStatusExitStatus(int hopContainerStatusExitStatus) {
        this.hopContainerStatusExitStatus = hopContainerStatusExitStatus;
    }

    public void setHopContainerStatusRMNodeId(String hopContainerStatusRMNodeId) {
        this.hopContainerStatusRMNodeId = hopContainerStatusRMNodeId;
    }

    public void setHopContainerStatusUciId(int hopContainerStatusUciId) {
        this.hopContainerStatusUciId = hopContainerStatusUciId;
    }

    public void buildHopContainerStatus() {
        hopContainerStatusList = new ArrayList<>();
    }

    public void addHopContainerStatus() {
        ContainerStatus hopContainerStatus = new ContainerStatus(
                hopContainerStatusContainerId, hopContainerStatusState,
                hopContainerStatusDiagnostics, hopContainerStatusExitStatus,
                hopContainerStatusRMNodeId, hopContainerStatusPendingId,
                ContainerStatus.Type.UCI,
                hopContainerStatusUciId);
        hopContainerStatusList.add(hopContainerStatus);
    }

    // These methods are used from the multi-threaded version of C++ library
    StreamingRTComps buildStreamingRTComps() {
        return new StreamingRTComps(containersToClean, finishedAppsList,
                nodeIds, nextHeartbeatMap, hopContainerStatusList, currentNMMasterKey,
                nextNMMasterKey, currentRMContainerMasterKey, nextRMContainerMasterKey);
    }

    public void onEventMethodMultiThread(StreamingRTComps streamingRTComps) throws InterruptedException {
        receivedRTEvents.put(streamingRTComps);
    }

    public void resetObjects() {
        containersToClean = null;
        finishedAppsList = null;
        nodeIds = null;
        nextHeartbeatMap = null;
        hopContainerStatusList = null;
        currentNMMasterKey = null;
        nextNMMasterKey = null;
        currentRMContainerMasterKey = null;
        nextRMContainerMasterKey = null;
    }
}
