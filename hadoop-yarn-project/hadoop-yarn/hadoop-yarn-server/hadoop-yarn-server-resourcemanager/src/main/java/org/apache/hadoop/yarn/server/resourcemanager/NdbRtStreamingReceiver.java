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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author sri
 */
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

  //This will be called by c++ shared library, libhopsndbevent.so
  public void onEventMethod() throws InterruptedException {
    StreamingRTComps streamingRTComps = new StreamingRTComps(
            containersToCleanSet, finishedAppList, nodeId, nextHeartbeat);
    blockingRTQueue.put(streamingRTComps);
  }

  // this two methods are using for multi-thread version from c++ library
  StreamingRTComps buildStreamingRTComps() {
    return new StreamingRTComps(containersToCleanSet, finishedAppList, nodeId,
            nextHeartbeat);
  }

  public void onEventMethodMultiThread(StreamingRTComps streamingRTComps) throws
          InterruptedException {
    blockingRTQueue.put(streamingRTComps);
  }
}
