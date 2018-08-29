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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import java.io.IOException;
import java.util.*;

import io.hops.metadata.HdfsVariables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.BRLoadBalancingException;

public class BRTrackingService {

  private class Work {

    private final long startTime;
    private final long noOfBlks;
    private final long nnId;

    public Work(final long startTime, final long noOfBlks, final long nnId) {
      this.startTime = startTime;
      this.noOfBlks = noOfBlks;
      this.nnId = nnId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getNoOfBlks() {
      return noOfBlks;
    }

    public long getNNId() { return nnId; }
  }

  private List workHistory = new LinkedList<Work>();

  public static final Log LOG = LogFactory.getLog(BRTrackingService.class);
  private final long DB_VAR_UPDATE_THRESHOLD;
  private final long BR_LB_TIME_WINDOW_SIZE;
  private int rrIndex = 0; // for round robin allocation


  public BRTrackingService(final long DB_VAR_UPDATE_THRESHOLD, final long BR_LB_TIME_WINDOW_SIZE) {
    workHistory = new LinkedList<Work>();
    this.DB_VAR_UPDATE_THRESHOLD = DB_VAR_UPDATE_THRESHOLD;
    this.BR_LB_TIME_WINDOW_SIZE = BR_LB_TIME_WINDOW_SIZE;
  }

  private int getRRIndex(final SortedActiveNodeList nnList){
      if(rrIndex < 0 || rrIndex >= nnList.size()){
          rrIndex = 0;
      }
      return (rrIndex++)%nnList.size();
  }

  private boolean canProcessMoreBR(long noOfBlks) throws IOException {
    //first remove the old history
    long timePoint = (System.currentTimeMillis() - BR_LB_TIME_WINDOW_SIZE);

    if (workHistory.size() > 0) {
      for (int i = workHistory.size() - 1; i >= 0; i--) {
        Work work = (Work) workHistory.get(i);
        if (work.getStartTime() <=  timePoint) {
          workHistory.remove(i);
          LOG.debug("Removing ("+work.getNoOfBlks()+" blks) from history. It was assigned to NN: "+work.getNNId());
        }
      }
    }

    long ongoingWork = 0;
    if (workHistory.size() > 0) {
      for (int i = workHistory.size() - 1; i >= 0; i--) {
        Work work = (Work) workHistory.get(i);
        ongoingWork += work.getNoOfBlks();
      }
    }

    LOG.debug("Currently processing at "+ongoingWork+" blks /"+(BR_LB_TIME_WINDOW_SIZE/(double)1000)+" sec");
    if ((ongoingWork + noOfBlks) > getBrLbMaxBlkPerTW(DB_VAR_UPDATE_THRESHOLD)) {
      LOG.info("Work ("+noOfBlks+" blks) can not be assigned, ongoing work: " + ongoingWork);
      return false;
    } else {
      return true;
    }
  }

  private long lastChecked = 0;
  private long cachedBrLbMaxBlkPerTW = -1;
  private long getBrLbMaxBlkPerTW(long DB_VAR_UPDATE_THRESHOLD) throws IOException {
    if ((System.currentTimeMillis() - lastChecked) > DB_VAR_UPDATE_THRESHOLD) {
      long newValue = HdfsVariables.getBrLbMaxBlkPerTW();
      if(newValue != cachedBrLbMaxBlkPerTW){
        cachedBrLbMaxBlkPerTW = newValue;
        LOG.info("BRTrackingService. Processing "+cachedBrLbMaxBlkPerTW
                +" per time window");
      }
      lastChecked = System.currentTimeMillis();
    }
    return cachedBrLbMaxBlkPerTW;
  }

  public synchronized ActiveNode assignWork(final SortedActiveNodeList nnList, long noOfBlks) throws IOException {
    if(canProcessMoreBR(noOfBlks)){
      int index = getRRIndex(nnList);
      if(index >= 0 && index < nnList.size()){
        ActiveNode an = nnList.getSortedActiveNodes().get(index);
        Work work  = new Work(System.currentTimeMillis(),noOfBlks,an.getId());
        workHistory.add(work);
        LOG.info("Work ("+noOfBlks+" blks)  assigned to NN: "+an.getId());
        return an;
      }
    }
    throw new BRLoadBalancingException("Work ("+noOfBlks+" blks) could not be assigned. System is fully loaded now. At most "+getBrLbMaxBlkPerTW(
            DB_VAR_UPDATE_THRESHOLD )+" blocks can be processed per "+BR_LB_TIME_WINDOW_SIZE);
  }
}
