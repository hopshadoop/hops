package io.hops.util;

import io.hops.metadata.yarn.entity.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by antonis on 8/23/16.
 */
public class NdbRmStreamingReceiver {

    private static final Log LOG = LogFactory.getLog(NdbRmStreamingReceiver.class);

    private static final int queueCapacity = 100000;
    public static final BlockingQueue<RMNodeComps> receivedEvents =
            new ArrayBlockingQueue<>(queueCapacity, true);

    public NdbRmStreamingReceiver() {
        LOG.info("HOP :: Created RM streaming receiver");
    }

    private PendingEvent hopPendingEvent = null;
    private RMNode hopRMNode = null;
    private NextHeartbeat hopNextHeartbeat = null;
    private NodeHBResponse hopNodeHBResponse = null;
    private Resource hopResource = null;

    private List<UpdatedContainerInfo> hopUpdatedContainerInfoList = null;
    private List<ContainerId> hopContainerIdsToCleanList = null;
    private List<FinishedApplications> hopFinishedApplicationsList = null;
    private List<ContainerStatus> hopContainerStatusList = null;

    // Build Hops RMNode
    private String hopRMNodeNodeId = "";
    private String hopRMNodeHostName = "";
    private int hopRMNodeCommandPort = 0;
    private int hopRMNodeHttpPort = 0;
    private String hopRMNodeNodeAddress = "";
    private String hopRMNodeHttpAddress = "";
    private String hopRMNodeHealthReport = "";
    private long hopRMNodeLastHealthReportTime = 0;
    private String hopRMNodeCurrentState = "";
    private String hopRMNodeNodeManagerVersion = "";
    private int hopRMNodeOvercommitTimeout = 0;
    private int hopRMNodePendingEventId = 0;

    public void setHopRMNodeNodeId(String hopRMNodeNodeId) {
        LOG.debug("setHopRMNodeNodeId - " + hopRMNodeNodeId);
        this.hopRMNodeNodeId = hopRMNodeNodeId;
    }

    public void setHopRMNodeHostName(String hopRMNodeHostName) {
        LOG.debug("setHopRMNodeHostName - " + hopRMNodeHostName);
        this.hopRMNodeHostName = hopRMNodeHostName;
    }

    public void setHopRMNodeCommandPort(int hopRMNodeCommandPort) {
        this.hopRMNodeCommandPort = hopRMNodeCommandPort;
    }

    public void setHopRMNodeHttpPort(int hopRMNodeHttpPort) {
        this.hopRMNodeHttpPort = hopRMNodeHttpPort;
    }

    public void setHopRMNodeNodeAddress(String hopRMNodeNodeAddress) {
        this.hopRMNodeNodeAddress = hopRMNodeNodeAddress;
    }

    public void setHopRMNodeHttpAddress(String hopRMNodeHttpAddress) {
        this.hopRMNodeHttpAddress = hopRMNodeHttpAddress;
    }

    public void setHopRMNodeHealthReport(String hopRMNodeHealthReport) {
        this.hopRMNodeHealthReport = hopRMNodeHealthReport;
    }

    public void setHopRMNodeLastHealthReportTime(long hopRMNodeLastHealthReportTime) {
        this.hopRMNodeLastHealthReportTime = hopRMNodeLastHealthReportTime;
    }

    public void setHopRMNodeCurrentState(String hopRMNodeCurrentState) {
        this.hopRMNodeCurrentState = hopRMNodeCurrentState;
    }

    public void setHopRMNodeNodeManagerVersion(String hopRMNodeNodeManagerVersion) {
        this.hopRMNodeNodeManagerVersion = hopRMNodeNodeManagerVersion;
    }

    public void setHopRMNodeOvercommitTimeout(int hopRMNodeOvercommitTimeout) {
        this.hopRMNodeOvercommitTimeout = hopRMNodeOvercommitTimeout;
    }

    public void setHopRMNodePendingEventId(int hopRMNodePendingEventId) {
        this.hopRMNodePendingEventId = hopRMNodePendingEventId;
    }

    public void buildHopRMNode() {
        LOG.debug("buildHopRMNode - " + hopRMNodeNodeId);
        hopRMNode = new RMNode(hopRMNodeNodeId, hopRMNodeHostName, hopRMNodeCommandPort,
                hopRMNodeHttpPort, hopRMNodeHealthReport, hopRMNodeLastHealthReportTime, hopRMNodeCurrentState,
                hopRMNodeNodeManagerVersion, hopRMNodePendingEventId);
    }

    // Build Hops Pending Event
    private String hopPendingEventRMNodeId = "";
    private PendingEvent.Type hopPendingEventType;
    private PendingEvent.Status hopPendingEventStatus;
    // Used to order the events when retrieved by the scheduler
    private int hopPendingEventId = 0;

    public void setHopPendingEventRMNodeId(String hopPendingEventRMNodeId) {
        this.hopPendingEventRMNodeId = hopPendingEventRMNodeId;
    }

    public void setHopPendingEventType(String hopPendingEventType) {
        this.hopPendingEventType = PendingEvent.Type.valueOf(hopPendingEventType);
    }

    public void setHopPendingEventStatus(String hopPendingEventStatus) {
        this.hopPendingEventStatus = PendingEvent.Status.valueOf(hopPendingEventStatus);
    }

    public void setHopPendingEventId(int hopPendingEventId) {
        this.hopPendingEventId = hopPendingEventId;
    }

    public void buildHopPendingEvent() {
        hopPendingEvent = new PendingEvent(hopPendingEventRMNodeId,
                hopPendingEventType, hopPendingEventStatus, hopPendingEventId);
    }

    // Build Hops Resource
    private String hopResourceId = "";
    private int hopResourceType = 0;
    private int hopResourceParent = 0;
    private int hopResourceMemory = 0;
    private int hopResourceVirtualCores = 0;
    private int hopResourcePendingEventId = 0;

    public void setHopResourceId(String hopResourceId) {
        this.hopResourceId = hopResourceId;
    }

    public void setHopResourceType(int hopResourceType) {
        this.hopResourceType = hopResourceType;
    }

    public void setHopResourceParent(int hopResourceParent) {
        this.hopResourceParent = hopResourceParent;
    }

    public void setHopResourceMemory(int hopResourceMemory) {
        this.hopResourceMemory = hopResourceMemory;
    }

    public void setHopResourceVirtualCores(int hopResourceVirtualCores) {
        this.hopResourceVirtualCores = hopResourceVirtualCores;
    }

    public void setHopResourcePendingEventId(int hopResourcePendingEventId) {
        this.hopResourcePendingEventId = hopResourcePendingEventId;
    }

    public void buildHopResource() {
        hopResource = new Resource(hopResourceId, hopResourceMemory,
                hopResourceVirtualCores, hopResourcePendingEventId);
    }

    // Build Hops Updated Container Info
    private String hopUpdatedContainerInfoRMNodeId = "";
    private String hopUpdatedContainerInfoContainerId = "";
    private int hopUpdatedContainerInfoUpdatetedContainerInfoId = 0;
    private int hopUpdatedContainerInfoPendingId = 0;

    public void buildHopUpdatedContainerInfo() {
        hopUpdatedContainerInfoList = new ArrayList<>();
    }

    public void setHopUpdatedContainerInfoRMNodeId(String hopUpdatedContainerInfoRMNodeId) {
        this.hopUpdatedContainerInfoRMNodeId = hopUpdatedContainerInfoRMNodeId;
    }

    public void setHopUpdatedContainerInfoContainerId(String hopUpdatedContainerInfoContainerId) {
        this. hopUpdatedContainerInfoContainerId = hopUpdatedContainerInfoContainerId;
    }

    public void setHopUpdatedContainerInfoUpdatedContainerInfoId(
            int hopUpdatedContainerInfoUpdatedContainerInfoId) {
        this.hopUpdatedContainerInfoUpdatetedContainerInfoId =
                hopUpdatedContainerInfoUpdatedContainerInfoId;
    }

    public void setHopUpdatedContainerInfoPendingId(int hopUpdatedContainerInfoPendingId) {
        this.hopUpdatedContainerInfoPendingId = hopUpdatedContainerInfoPendingId;
    }

    public void addHopUpdatedContainerInfo() {
        UpdatedContainerInfo hopUCI = new UpdatedContainerInfo(
                hopUpdatedContainerInfoRMNodeId, hopUpdatedContainerInfoContainerId,
                hopUpdatedContainerInfoUpdatetedContainerInfoId, hopUpdatedContainerInfoPendingId);
        hopUpdatedContainerInfoList.add(hopUCI);
    }

    // Build Hops Container Status
    private String hopContainerStatusContainerId = "";
    private String hopContainerStatusState = "";
    private String hopContainerStatusDiagnostics = "";
    private int hopContainerStatusExitStatus = 0;
    private String hopContainerStatusRMNodeId = "";
    private int hopContainerStatusPendingId = 0;
    private int hopContainerStatusUciId = 0;

    public void buildHopContainerStatus() {
        hopContainerStatusList = new ArrayList<>();
    }

    public void setHopContainerStatusContainerId(String hopContainerStatusContainerId) {
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

    public void addHopContainerStatus() {
        ContainerStatus containerStatus = new ContainerStatus(
                hopContainerStatusContainerId, hopContainerStatusState,
                hopContainerStatusDiagnostics, hopContainerStatusExitStatus,
                hopContainerStatusRMNodeId, hopContainerStatusPendingId,
                ContainerStatus.Type.UCI,
                hopContainerStatusUciId);
        hopContainerStatusList.add(containerStatus);
    }

    int numOfEvents = 0;
    long lastTimestamp = 0;

    // This will be called by the C++ library
    public void onEventMethod() throws InterruptedException {
        LOG.debug("HOP :: Received event from NDB");
        LOG.debug("Size of Received Events Queue is: " + receivedEvents.size());
        RMNodeComps hopRMNodeDBObj = new RMNodeComps(hopRMNode, hopNextHeartbeat,
                hopNodeHBResponse, hopResource, hopPendingEvent, hopUpdatedContainerInfoList,
                hopContainerIdsToCleanList, hopFinishedApplicationsList, hopContainerStatusList,
                hopPendingEvent.getId().getNodeId());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<Receiver> Put event in the queue: " + hopRMNodeDBObj.getPendingEvent()
                    .getId() + " : " + hopRMNodeDBObj.getPendingEvent().getId().getNodeId());
        }
        if ((System.currentTimeMillis() - lastTimestamp) >= 1000) {
            LOG.error("***<Profiler> Put " + numOfEvents + " per second");
            numOfEvents = 0;
            lastTimestamp = System.currentTimeMillis();
        }
        receivedEvents.put(hopRMNodeDBObj);
        numOfEvents++;
    }

    // These two methods will be used by the multi-threaded version of C++ library
    RMNodeComps buildCompositeClass() {
        return new RMNodeComps(hopRMNode, hopNextHeartbeat, 
                hopNodeHBResponse, hopResource, hopPendingEvent, hopUpdatedContainerInfoList,
                hopContainerIdsToCleanList, hopFinishedApplicationsList, hopContainerStatusList,
                hopPendingEvent.getId().getNodeId());
    }

    public void onEventMethodMultiThread(RMNodeComps hopCompObj) throws InterruptedException {
        receivedEvents.put(hopCompObj);
    }

    public void resetObjects() {
        hopRMNode = null;
        hopNextHeartbeat = null;
        hopNodeHBResponse = null;
        hopResource = null;
        hopPendingEvent = null;
        hopUpdatedContainerInfoList = null;
        hopContainerIdsToCleanList = null;
        hopFinishedApplicationsList = null;
        hopContainerStatusList = null;
    }
}
