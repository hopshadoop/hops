/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterMetrics</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster.  
 *   </li>
 *   <li>
 *   Number of blacklisted and decommissioned trackers.  
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running map & reduce tasks.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterMetrics</code>, via 
 * {@link Cluster#getClusterStatus()}.</p>
 * 
 * @see Cluster
 */
public class ClusterMetrics implements Writable {
  int runningMaps;
  int runningReduces;
  int mapSlots;
  int reduceSlots;
  int numTrackers;
  int numBlacklistedTrackers;
  int numDecommissionedTrackers;

  public ClusterMetrics() {
  }
  
  public ClusterMetrics(int runningMaps, int runningReduces, int mapSlots, 
    int reduceSlots, int numTrackers, int numBlacklistedTrackers,
    int numDecommisionedNodes) {
    this.runningMaps = runningMaps;
    this.runningReduces = runningReduces;
    this.mapSlots = mapSlots;
    this.reduceSlots = reduceSlots;
    this.numTrackers = numTrackers;
    this.numBlacklistedTrackers = numBlacklistedTrackers;
    this.numDecommissionedTrackers = numDecommisionedNodes;
  }
  
  public int getOccupiedMapSlots() { 
    return runningMaps;
  }
  
  public int getOccupiedReduceSlots() { 
    return runningReduces; 
  }
  
  public int getMapSlotCapacity() {
    return mapSlots;
  }
  
  public int getReduceSlotCapacity() {
    return reduceSlots;
  }
  
  public int getTaskTrackerCount() {
    return numTrackers;
  }
  
  public int getBlackListedTaskTrackerCount() {
    return numBlacklistedTrackers;
  }
  
  public int getDecommissionedTaskTrackerCount() {
    return numDecommissionedTrackers;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    runningMaps = in.readInt();
    runningReduces = in.readInt();
    mapSlots = in.readInt();
    reduceSlots = in.readInt();
    numTrackers = in.readInt();
    numBlacklistedTrackers = in.readInt();
    numDecommissionedTrackers = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(runningMaps);
    out.writeInt(runningReduces);
    out.writeInt(mapSlots);
    out.writeInt(reduceSlots);
    out.writeInt(numTrackers);
    out.writeInt(numBlacklistedTrackers);
    out.writeInt(numDecommissionedTrackers);
  }

}
