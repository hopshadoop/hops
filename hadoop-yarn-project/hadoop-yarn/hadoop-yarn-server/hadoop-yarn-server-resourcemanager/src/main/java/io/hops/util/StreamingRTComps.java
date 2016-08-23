package io.hops.util;

import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by antonis on 8/22/16.
 */
public class StreamingRTComps {

    // TODO: For the moment currentPrice and currentPriceTick are skipped

    // <NodeId, [ContainersId]>
    private final Map<String, Set<ContainerId>> containersToClean;
    // <NodeId, [ApplicationIds]>
    private final Map<String, List<ApplicationId>> finishedApps;
    private final Set<String> nodeIds;
    private final Map<String, Boolean> nextHeartbeat;
    private final List<ContainerStatus> containerStatusList;
    private final MasterKey currentNMMasterKey;
    private final MasterKey nextNMMasterKey;
    private final MasterKey currentRMContainerMasterKey;
    private final MasterKey nextRMContainerMasterKey;

    public StreamingRTComps(
            Map<String, Set<ContainerId>> containersToClean,
            Map<String, List<ApplicationId>> finishedApps,
            Set<String> nodeIds,
            Map<String, Boolean> nextHeartbeat,
            List<ContainerStatus> containerStatusList,
            MasterKey currentNMMasterKey,
            MasterKey nextNMMasterKey,
            MasterKey currentRMContainerMasterKey,
            MasterKey nextRMContainerMasterKey) {
        this.containersToClean = containersToClean;
        this.finishedApps = finishedApps;
        this.nodeIds = nodeIds;
        this.nextHeartbeat = nextHeartbeat;
        this.containerStatusList = containerStatusList;
        this.currentNMMasterKey = currentNMMasterKey;
        this.nextNMMasterKey = nextNMMasterKey;
        this.currentRMContainerMasterKey = currentRMContainerMasterKey;
        this.nextRMContainerMasterKey = nextRMContainerMasterKey;
    }

    public Set<ContainerId> getContainersToCleanByNodeId(String nodeId) {
        return containersToClean.get(nodeId);
    }

    public List<ApplicationId> getFinishedAppsByNodeId(String nodeId) {
        return finishedApps.get(nodeId);
    }

    public Set<String> getNodeIds() {
        return nodeIds;
    }

    public Boolean isNextHeartbeatForNodeId(String nodeId) {
        return nextHeartbeat.get(nodeId);
    }

    public List<ContainerStatus> getHopContainerStatusList() {
        return containerStatusList;
    }

    public MasterKey getCurrentNMMasterKey() {
        return currentNMMasterKey;
    }

    public MasterKey getNextNMMasterKey() {
        return nextNMMasterKey;
    }

    public MasterKey getCurrentRMContainerMasterKey() {
        return currentRMContainerMasterKey;
    }

    public MasterKey getNextRMContainerMasterKey() {
        return nextRMContainerMasterKey;
    }
}
