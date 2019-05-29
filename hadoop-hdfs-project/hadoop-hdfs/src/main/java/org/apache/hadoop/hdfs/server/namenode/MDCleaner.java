/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.leaderElection.LeaderElection;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.election.entity.LeDescriptor.FailedNodeLeDescriptor;
import org.apache.hadoop.util.Daemon;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Cleans metadata left behind by the failed namenodes
 */
public class MDCleaner {

  public static final Log LOG = LogFactory.getLog(MDCleaner.class);


  private boolean run = false;
  private LeaderElection leaderElection;
  private List<FailedNodeLeDescriptor> failedNodes;
  private Daemon mdCleaner;
  private long stoTableCleanDelay = 0;
  private FSNamesystem namesystem;

  //[S] Singleton does not work in unit tests as multiple NN run in same JVM
//  private static final MDCleaner instance = new MDCleaner();
//  private MDCleaner() {
//  }
//
  public static MDCleaner getInstance(){
   return new MDCleaner();
  }

  class Monitor implements Runnable {
    @Override
    public void run() {
      while (run) {
        try {
          if (leaderElection.isRunning()) {
            failedNodes.addAll(leaderElection.getDeadNodes());
          }

          clearLocks();

          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (IOException e){
          e.printStackTrace();
        }
      }
    }
  }

  private void clearLocks() throws IOException {
    LOG.debug("Cleaning STO Locks. Pending Locks: " + failedNodes.size());

    Iterator<FailedNodeLeDescriptor> iterator = failedNodes.iterator();
    while (iterator.hasNext()) {
      FailedNodeLeDescriptor descriptor = iterator.next();
      if ((System.currentTimeMillis() - descriptor.getFailTime()) > stoTableCleanDelay) {
        //find all locked paths;
        iterator.remove();
        Collection<SubTreeOperation> ops = getPaths(descriptor.getId());
        LOG.debug("Cleaning STO Lock for NN: " + descriptor +" No of stale locks: "+ops.size());
        for(SubTreeOperation op : ops){
          namesystem.unlockSubtree(op.getPath(), -1);
        }
      }
    }

    //Clean failed STO operations belonging to live NNs
    Collection<SubTreeOperation> ops = getPathsToRecoverAsync();
    for(SubTreeOperation op : ops){
      LOG.info("Cleaning STO Lock. OP = {"+op+"}");
      namesystem.unlockSubtree(op.getPath(), op.getInodeID());
    }
  }

  Collection<SubTreeOperation> getPaths(final long nnID) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.MDCLEANER) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allOpsByNN(nnID);
              }
            };
    return (Collection<SubTreeOperation>)subTreeLockChecker.handle();

  }

  Collection<SubTreeOperation> getPathsToRecoverAsync() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.MDCLEANER) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allOpsToRecoverAsync();
              }
            };
    return (Collection<SubTreeOperation>)subTreeLockChecker.handle();

  }

  void startMDCleanerMonitor(FSNamesystem namesystem, LeaderElection leaderElection, long stoTableCleanDelay) {
    this.leaderElection = leaderElection;
    this.failedNodes = new ArrayList<FailedNodeLeDescriptor>();
    this.stoTableCleanDelay = stoTableCleanDelay;
    this.namesystem = namesystem;
    run = true;

    mdCleaner = new Daemon(new Monitor());
    mdCleaner.start();
  }

  void stopMDCleanerMonitor() {
    if (mdCleaner != null) {
      run = false;
      LOG.debug("Shutting down metadata cleaner ");
      try {
        mdCleaner.interrupt();
        mdCleaner.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      mdCleaner = null;
    }
  }
}
