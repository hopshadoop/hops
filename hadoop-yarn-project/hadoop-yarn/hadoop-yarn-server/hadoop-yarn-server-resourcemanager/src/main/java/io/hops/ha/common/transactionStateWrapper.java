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
package io.hops.ha.common;

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.entity.RMNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import static org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.LOG;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;

public class transactionStateWrapper extends TransactionStateImpl {

  private final TransactionStateImpl ts;
  private final AtomicInteger rpcCounter = new AtomicInteger(0);
  int rpcId;
  long startTime = System.currentTimeMillis();
  String rpcType;
  Map<String, Queue<Long>> handleStarts = new ConcurrentHashMap<String, Queue<Long>>();
  Map<String, Queue<Long>> handleDurations = new ConcurrentHashMap<String, Queue<Long>>();
  Map<Integer, Long> timeInit = new ConcurrentHashMap<Integer, Long>();
  
  public transactionStateWrapper(TransactionStateImpl ts, 
          TransactionType type, int rpcId, String rpcType) {
    super(type);
    this.ts = ts;
    this.rpcId = rpcId;
    this.rpcType = rpcType;
  }

  public void addTime(int i){
    timeInit.put(i, System.currentTimeMillis()-startTime);
  }
  public void incCounter(Enum type) {
    String key = type.getDeclaringClass().getName()+ "." + type.name();

    Queue<Long> queue = handleStarts.get(key);
    if (queue == null) {
      synchronized (handleStarts) {
        queue = handleStarts.get(key);
        if (queue == null) {
          queue = new LinkedBlockingQueue<Long>();
          handleStarts.put(key, queue);
        }
      }
    }
    queue.add(System.currentTimeMillis());
    
    ts.incCounter(type);
    rpcCounter.incrementAndGet();
  }
  
  private String printDetailedDurations(){
    String durations = " (";
      for(String types: handleDurations.keySet()){
        String vals = "";
        for(long val: handleDurations.get(types)){
          vals = vals + val + ", ";
        }
        durations= durations + types + ": " + vals;
      }
      durations = durations + ") ";
      
      for(int i: timeInit.keySet()){
        durations= durations + i + " " + timeInit.get(i) + ", ";
      }
      return durations;
  }
  public void decCounter(Enum type) throws IOException {
    ts.decCounter(type);
    int val = rpcCounter.decrementAndGet();
    String key = type.getDeclaringClass().getName()+ "." + type.name();
    Queue<Long> queue = handleDurations.get(key);
    if(queue==null){
      queue = new LinkedBlockingQueue<Long>();
      queue.add(System.currentTimeMillis()-handleStarts.get(key).poll());
      handleDurations.put(key, queue);
    }else{
      queue.add(System.currentTimeMillis()-handleStarts.get(key).poll());
    }
    if (val == 0) {
      long duration = System.currentTimeMillis() - startTime;
      
      if (duration > 1000) {
        LOG.info("finishing rpc too long : " + rpcId + " "
                + rpcType + " after " + duration + printDetailedDurations());
      }
    }
  }

  public int getRPCCounter(){
    return rpcCounter.get();
  }
  
  public String getRPCType(){
    return rpcType;
  }
  
  public String getRunningEvents() {
    int nbRunning = 0;
    int nbFinished = 0;
    for (String key : handleStarts.keySet()) {
      for (long val : handleStarts.get(key)) {
        nbRunning++;
      }
    }
    for (String key : handleDurations.keySet()) {
      for (long val : handleDurations.get(key)) {
        nbFinished++;
      }
    }
    String result = "nb running events: " + nbRunning + " nb finished events "
            + nbFinished + " still running: ";
    for (String key : handleStarts.keySet()) {
      if (handleStarts.get(key).size() > 0) {
        result = result + ", " + key;
      }
    }
    result = result + " finished: ";
    for (String key : handleDurations.keySet()) {
      result = result + ", " + key + " (" + handleDurations.get(key).size()
              + ")";
    }
    return result;
  }
  public int getCounter() {
    return ts.getCounter();
  }

  public int getRPCID(){
    return rpcId;
  }
  
  public void addRPCId(int rpcId){
    this.rpcId = rpcId;
    ts.addRPCId(rpcId);
  }

  @Override
  public void commit(boolean first) throws IOException {
    ts.commit(first);
  }

  @Override
  public FairSchedulerNodeInfo getFairschedulerNodeInfo() {
    return ts.getFairschedulerNodeInfo();
  }
  
  @Override
  public SchedulerApplicationInfo getSchedulerApplicationInfos(
          ApplicationId appId) {
    return ts.getSchedulerApplicationInfos(appId);
  }

  @Override
  public void persist() throws IOException {
    ts.persist();
  }

  @Override
  public CSQueueInfo getCSQueueInfo() {
    return ts.getCSQueueInfo();
  }


  @Override
  public FiCaSchedulerNodeInfoToUpdate getFicaSchedulerNodeInfoToUpdate(
          NodeId nodeId) {
    return ts.getFicaSchedulerNodeInfoToUpdate(nodeId);
  }

  @Override
  public void addFicaSchedulerNodeInfoToAdd(String nodeId,
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {

    ts.addFicaSchedulerNodeInfoToAdd(nodeId, node);
  }

  @Override
  public void addFicaSchedulerNodeInfoToRemove(String nodeId,
          org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode node) {
    ts.addFicaSchedulerNodeInfoToRemove(nodeId, node);
  }

  @Override
  public void addApplicationToAdd(RMAppImpl app) {
    ts.addApplicationToAdd(app);
  }

  @Override
  public void addApplicationToRemove(ApplicationId appId) {
    ts.addApplicationToRemove(appId);
  }

  @Override
  public void addAppAttempt(RMAppAttempt appAttempt) {
    ts.addAppAttempt(appAttempt);
  }

  @Override
  public void removeAllocateResponse(ApplicationAttemptId id, int responseId) {
    ts.removeAllocateResponse(id, responseId);
  }

  @Override
  public void addRMContainerToUpdate(RMContainerImpl rmContainer) {
    ts.addRMContainerToUpdate(rmContainer);
  }

  @Override
  public RMContextInfo getRMContextInfo() {
    return ts.getRMContextInfo();
  }

  @Override
  public void toUpdateRMNode(
          org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmnodeToAdd) {
    ts.toUpdateRMNode(rmnodeToAdd);
  }

  @Override
  public RMNodeInfo getRMNodeInfo(NodeId rmNodeId) {
    return ts.getRMNodeInfo(rmNodeId);
  }

  /**
   * Remove pending event from DB. In this case, the event id is not needed,
   * hence set to MIN.
   * <p/>
   *
   * @param id
   * @param rmnodeId
   * @param type
   * @param status
   */
  @Override
  public void addPendingEventToRemove(int id, String rmnodeId, int type,
          int status) {
    ts.addPendingEventToRemove(id, rmnodeId, type, status);
  }
  
   public int getId(){
    return ts.getId();
  }
    public Set<ApplicationId> getAppIds(){
    return ts.getAppIds();
  }
    
  @Override
  public void addRMContainerToAdd(RMContainerImpl rmContainer) {
    ts.addRMContainerToAdd(rmContainer);
  }  

  @Override
  public void addRMContainerToRemove(RMContainer rmContainer){
      ts.addRMContainerToRemove(rmContainer);
  }
  
  @Override
  public void addAllocateResponse(ApplicationAttemptId id,
          ApplicationMasterService.AllocateResponseLock allocateResponse) {
    ts.addAllocateResponse(id, allocateResponse);
  }
    
  @Override
  public void addAllRanNodes(RMAppAttempt appAttempt) {
    ts.addAllRanNodes(appAttempt);
  }

  @Override
  public void addRanNode(NodeId nid, ApplicationAttemptId appAttemptId) {
   ts.addRanNode(nid, appAttemptId);
  }
    
  
  @Override
  public void addAllJustFinishedContainersToAdd(List<ContainerStatus> status,
          ApplicationAttemptId appAttemptId) {
    ts.addAllJustFinishedContainersToAdd(status, appAttemptId);
  }
  
  @Override
  public void addJustFinishedContainerToAdd(ContainerStatus status,
          ApplicationAttemptId appAttemptId) {
    ts.addJustFinishedContainerToAdd(status, appAttemptId);
  }
  
  @Override
  public void addAllJustFinishedContainersToRemove(List<ContainerStatus> status,
          ApplicationAttemptId appAttemptId) {
    ts.addAllJustFinishedContainersToRemove(status, appAttemptId);
  }
  
}
