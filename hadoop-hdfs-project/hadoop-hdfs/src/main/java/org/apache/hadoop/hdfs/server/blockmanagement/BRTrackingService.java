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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.ActiveBlockReportsDataAccess;
import io.hops.metadata.hdfs.entity.ActiveBlockReport;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BRTrackingService {

  public static final Log LOG = LogFactory.getLog(BRTrackingService.class);
  private final long DB_VAR_UPDATE_THRESHOLD;
  private final long BR_MAX_PROCESSING_TIME;
  private int rrIndex = 0; // for round robin allocation


  public BRTrackingService(final long DB_VAR_UPDATE_THRESHOLD,
                           final long MAX_CONCURRENT_BRS,
                           final long BR_MAX_PROCESSING_TIME) {
    this.DB_VAR_UPDATE_THRESHOLD = DB_VAR_UPDATE_THRESHOLD;
    this.BR_MAX_PROCESSING_TIME = BR_MAX_PROCESSING_TIME;
    this.cachedMaxConcurrentBRPerNN = MAX_CONCURRENT_BRS;
  }

  private int getRRIndex(final SortedActiveNodeList nnList) {
    if (rrIndex < 0 || rrIndex >= nnList.size()) {
      rrIndex = 0;
    }
    return (rrIndex++) % nnList.size();
  }

  private boolean canProcessMoreBR(final SortedActiveNodeList nnList,
                                   List<ActiveBlockReport> retActiveBRs) throws IOException {

    List<ActiveBlockReport> allActiveBRs = getAllActiveBlockReports();

    //remove dead operations from the table
    Iterator<ActiveBlockReport> itr = allActiveBRs.iterator();
    while (itr.hasNext()) {
      ActiveBlockReport abr = itr.next();
      if ((System.currentTimeMillis() - abr.getStartTime()) > BR_MAX_PROCESSING_TIME) {
        //remove
        LOG.warn("block report timed out dn: " + abr.getDnAddress() + " on NN: " + abr.getNnId());
        removeActiveBlockReport(abr);
        itr.remove();
      }
    }

    retActiveBRs.addAll(allActiveBRs);
    if (allActiveBRs.size() < getBrLbMaxConcurrentBRs(nnList)) {
      return true;
    } else {
      return false;
    }
  }

  private ActiveNode getLeastLoadedNode(final SortedActiveNodeList nnList,
                                        List<ActiveBlockReport> retActiveBRs) throws IOException {
    class Tuple{
      ActiveNode an;
      Integer counter;
      Tuple(ActiveNode an, Integer count){
        this.an = an;
        this.counter = count;
      }
    }

    Map<String, Tuple> usage = new HashMap<>();
    for (ActiveNode an : nnList.getActiveNodes()) {
      usage.put(an.getHttpAddress(), new Tuple(an, new Integer(0)));
    }

    for(ActiveBlockReport abr : retActiveBRs) {
      Tuple entry = usage.get(abr.getNnAddress());
      entry.counter += 1;
    }

    ActiveNode leastLoaded = null;
    int count = Integer.MAX_VALUE;

    for (String key : usage.keySet()) {
      Tuple tuple = usage.get(key);
      if (tuple.counter < count) {
        leastLoaded = tuple.an;
        count = tuple.counter;
      }
    }

    if (count >= cachedMaxConcurrentBRPerNN) {
      return null;
    } else {
      return leastLoaded;
    }

  }

  private long lastChecked = 0;
  private long cachedMaxConcurrentBRPerNN = 0;
  private long getBrLbMaxConcurrentBRs(final SortedActiveNodeList nnList) throws IOException {
    if ((System.currentTimeMillis() - lastChecked) > DB_VAR_UPDATE_THRESHOLD) {
      long value = HdfsVariables.getMaxConcurrentBrs();
      if (value != cachedMaxConcurrentBRPerNN) {
        cachedMaxConcurrentBRPerNN = value;
        LOG.info("BRTrackingService param update. Processing " + cachedMaxConcurrentBRPerNN + " " +
                "concurrent block reports");
      }
      lastChecked = System.currentTimeMillis();
    }
    return cachedMaxConcurrentBRPerNN * nnList.size();
  }

  public synchronized ActiveNode assignWork(final SortedActiveNodeList nnList,
      final String dnAddress, final long noOfBlks) throws IOException {
    return (ActiveNode) new LightWeightRequestHandler(HDFSOperationType.BR_LB_GET_ALL) {
      @Override
      public Object performTask() throws IOException {
        boolean isActive = connector.isTransactionActive();
        if (!isActive) {
          connector.beginTransaction();
          connector.writeLock();
        }

        try {
          List<ActiveBlockReport> retActiveBRs = new ArrayList<>();
          if (canProcessMoreBR(nnList, retActiveBRs)) {
            ActiveNode an = getLeastLoadedNode(nnList, retActiveBRs);
            if( an != null) {
              ActiveBlockReport abr = new ActiveBlockReport(dnAddress, an.getId(),
                      an.getHttpAddress(), System.currentTimeMillis(),
                      noOfBlks);
              addActiveBlockReport(abr);
              LOG.info("Block report from " + dnAddress + " containing " + noOfBlks + " blocks "
                      + "is assigned to NN [ID: " + an.getId() + ", IP: " + an.getRpcServerIpAddress() + "]");
              return an;
            }
          }
          String msg = "Work (" + noOfBlks + " blks) could not be assigned. " + "System is fully loaded now. At most "
              + getBrLbMaxConcurrentBRs(nnList)
              + " concurrent block reports can be processed.";
          LOG.info(msg);
          throw new BRLoadBalancingOverloadException(msg);
        } finally {
          if (!isActive) {
            connector.commit();
          }
        }
      }
    }.handle();
  }


  public synchronized void blockReportCompleted( String dnAddress) throws IOException {
    ActiveBlockReport abr = new ActiveBlockReport(dnAddress, 0, "", 0, 0);
    LOG.info("Block report from "+dnAddress+" has completed");
    removeActiveBlockReport(abr);
  }

  private List<ActiveBlockReport> getAllActiveBlockReports() throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType
            .BR_LB_GET_ALL) {
      @Override
      public Object performTask() throws IOException {
        ActiveBlockReportsDataAccess da = (ActiveBlockReportsDataAccess) HdfsStorageFactory
                .getDataAccess(ActiveBlockReportsDataAccess.class);
        return da.getAll();
      }
    };
    return (List<ActiveBlockReport>) handler.handle();
  }
  
  private void addActiveBlockReport(final ActiveBlockReport abr) throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType
            .BR_LB_ADD) {
      @Override
      public Object performTask() throws IOException {
        boolean isActive = connector.isTransactionActive();
        if(!isActive){
          connector.beginTransaction();
          connector.writeLock();
        }
        ActiveBlockReportsDataAccess da = (ActiveBlockReportsDataAccess) HdfsStorageFactory
                .getDataAccess(ActiveBlockReportsDataAccess.class);
        da.addActiveReport(abr);
        if(!isActive){
          connector.commit();
        }
        return null;
      }
    };
    handler.handle();
  }

  private void removeActiveBlockReport(final ActiveBlockReport abr) throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType
            .BR_LB_REMOVE) {
      @Override
      public Object performTask() throws IOException {
        boolean isActive = connector.isTransactionActive();
        if(!isActive){
          connector.beginTransaction();
          connector.writeLock();
        }
        ActiveBlockReportsDataAccess da = (ActiveBlockReportsDataAccess) HdfsStorageFactory
                .getDataAccess(ActiveBlockReportsDataAccess.class);
        ActiveBlockReport inDB = da.getActiveBlockReport(abr);
        if(inDB!=null){
          da.removeActiveReport(inDB);
        }
        if(!isActive){
          connector.commit();
        }
        return null;
      }
    };
    handler.handle();
  }
}
