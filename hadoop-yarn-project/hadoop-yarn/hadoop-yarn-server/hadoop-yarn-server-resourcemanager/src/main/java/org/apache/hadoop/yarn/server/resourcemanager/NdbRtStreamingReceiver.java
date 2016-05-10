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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;

public class NdbRtStreamingReceiver {

  //TODO make the queue size configurable
  public static BlockingQueue<StreamingRTComps> blockingRTQueue
          = new ArrayBlockingQueue<StreamingRTComps>(100000);

  private static final Log LOG = LogFactory.getLog(NdbRtStreamingReceiver.class);
  private Set<org.apache.hadoop.yarn.api.records.ContainerId> containersToCleanSet
          = null;
  private List<org.apache.hadoop.yarn.api.records.ApplicationId> finishedAppList
          = null;
  private String containerId = null;
  private String applicationId = null;
  private String nodeId = null;
  private boolean nextHeartbeat = false;
  private int finishedAppPendingId = 0;
  private int cidToCleanPendingId = 0;
  private int nextHBPendingId = 0;
  private String containerIdToCleanrmnodeid = null;
  private String finishedApplicationrmnodeid = null;
  private List<ContainerStatus> hopContainersStatusList = null;
  private float currentPrice = 0.0f;
  private long currentPriceTick = 0;

  public void setCurrentPriceTick(long CurrentPriceTick) {
    this.currentPriceTick = CurrentPriceTick;
  }

  public void setCurrentPrice(float CurrentPrice) {
    this.currentPrice = CurrentPrice;
  }

  public void setPriceName(String priceName){
    //not usefull so far
  }
  
  NdbRtStreamingReceiver() {
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

  public void setNextHBPendingId(int nextHBPendingId) {
    this.nextHBPendingId = nextHBPendingId;

  }

  public void setContainerIdToClenrmnodeid(String rmnodeid) {
    this.containerIdToCleanrmnodeid = rmnodeid;
  }

  public void buildContainersToClean() {
    containersToCleanSet
            = new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
  }

  public void AddContainersToClean() {
    org.apache.hadoop.yarn.api.records.ContainerId addContainerId
            = ConverterUtils.toContainerId(containerId);
    containersToCleanSet.add(addContainerId);
  }

  public void buildFinishedApplications() {
    finishedAppList = new ArrayList<ApplicationId>();
  }

  public void setApplicationIdrmnodeid(String rmnodeid) {
    this.finishedApplicationrmnodeid = rmnodeid;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public void AddFinishedApplications() {
    ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
    finishedAppList.add(appId);
    LOG.debug("finishedapplications appid : " + appId + " pending id : "
            + finishedAppPendingId + " rmnode node : "
            + finishedApplicationrmnodeid);

  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public void setNextHeartbeat(boolean nextHeartbeat) {
    this.nextHeartbeat = nextHeartbeat;
  }

  
  private MasterKey currentNMMasterKey = null;
  private MasterKey nextNMMasterKey = null;
  private MasterKey currentRMContainerMasterKey = null;
  private MasterKey nextRMContainerMasterKey = null;
  
  private String keyId = "";
  private byte[] keyBytes=null;
  
  public void setKeyId(String keyId){
    this.keyId = keyId;
  }
  
  public void setKeyBytes(byte[] keyBytes){
    this.keyBytes = keyBytes;
  }
  
  public void setKey(){
    try {
      RMStateStore.KeyType keyType = RMStateStore.KeyType.valueOf(keyId);
      MasterKey key = new MasterKeyPBImpl(
              YarnServerCommonProtos.MasterKeyProto
                      .parseFrom(keyBytes));
      switch(keyType){
        case CURRENTNMTOKENMASTERKEY:
          currentNMMasterKey=key;
          break;
        case NEXTNMTOKENMASTERKEY:
          nextNMMasterKey=key;
          break;
        case CURRENTCONTAINERTOKENMASTERKEY:
          currentRMContainerMasterKey = key;
          break;
        case NEXTCONTAINERTOKENMASTERKEY:
          nextRMContainerMasterKey=key;
      }
    } catch (InvalidProtocolBufferException ex) {
      LOG.error(ex, ex);
    }
  }

  //This will be called by c++ shared library, libhopsndbevent.so
  public void onEventMethod() throws InterruptedException {
    StreamingRTComps streamingRTComps = new StreamingRTComps(
            containersToCleanSet, finishedAppList, nodeId, nextHeartbeat,
            hopContainersStatusList, currentNMMasterKey, nextNMMasterKey,
            currentRMContainerMasterKey, nextRMContainerMasterKey,
            currentPrice, currentPriceTick);
    blockingRTQueue.put(streamingRTComps);
  }
  
  //// list building - build container status
  private String hopContainerStatusContainerid = "";
  private String hopContainerStatusState = "";
  private String hopContainerStatusDiagnostics = "";
  private int hopContainerStatusExitstatus = 0;
  private String hopContainerStatusRMNodeId = "";
  private int hopContainerStatusPendingId = 0;

  public void setHopContainerStatusContainerid(
          String hopContainerStatusContainerid) {
    this.hopContainerStatusContainerid = hopContainerStatusContainerid;
  }

  public void setHopContainerStatusState(String hopContainerStatusState) {
    this.hopContainerStatusState = hopContainerStatusState;
  }

  public void setHopContainerStatusPendingId(int hopContainerStatusPendingId) {
    this.hopContainerStatusPendingId = hopContainerStatusPendingId;
  }

  public void setHopContainerStatusDiagnostics(
          String hopContainerStatusDiagnostics) {
    this.hopContainerStatusDiagnostics = hopContainerStatusDiagnostics;
  }

  public void setHopContainerStatusExitstatus(int hopContainerStatusExitstatus) {
    this.hopContainerStatusExitstatus = hopContainerStatusExitstatus;
  }

  public void setHopContainerStatusRMNodeId(String hopContainerStatusRMNodeId) {
    this.hopContainerStatusRMNodeId = hopContainerStatusRMNodeId;
  }

  public void buildHopContainerStatus() {
    hopContainersStatusList = new ArrayList<ContainerStatus>();
  }

  public void AddHopContainerStatus() {
    ContainerStatus hopContainerStatus = new ContainerStatus(
            hopContainerStatusContainerid, hopContainerStatusState,
            hopContainerStatusDiagnostics, hopContainerStatusExitstatus,
            hopContainerStatusRMNodeId, hopContainerStatusPendingId,
            ContainerStatus.Type.UCI);
    hopContainersStatusList.add(hopContainerStatus);
  }
  

  // this two methods are using for multi-thread version from c++ library
  StreamingRTComps buildStreamingRTComps() {
    return new StreamingRTComps(
            containersToCleanSet, finishedAppList, nodeId, nextHeartbeat,
            hopContainersStatusList, currentNMMasterKey, nextNMMasterKey,
            currentRMContainerMasterKey, nextRMContainerMasterKey, currentPrice,
            currentPriceTick);
  }

  public void onEventMethodMultiThread(StreamingRTComps streamingRTComps) throws
          InterruptedException {
    blockingRTQueue.put(streamingRTComps);
  }
  
  public void resetObjects() {
    containersToCleanSet = null;
    finishedAppList = null;
    nodeId = null;
    nextHeartbeat = false;
    hopContainersStatusList = null;
    currentNMMasterKey = null;
    nextNMMasterKey = null;
    currentRMContainerMasterKey = null;
    nextRMContainerMasterKey = null;
    currentPrice = 0.0f;
    currentPriceTick = 0;
  }
}
