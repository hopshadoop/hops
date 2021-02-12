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
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.util.Collection;

/**
 * Cleans metadata left behind by the failed namenodes
 */
public class MDCleaner {

  public static final Log LOG = LogFactory.getLog(MDCleaner.class);


  private boolean run = false;
  private LeaderElection leaderElection;
  private Daemon mdCleaner;
  private long failedSTOCleanDelay = 0;
  private long slowSTOCleanDelay = 0;
  private FSNamesystem namesystem;

//  [S] Singleton does not work in unit tests as multiple NN run in same JVM
//  private static final MDCleaner instance = new MDCleaner();
//  private MDCleaner() {
//  }
//
  public static MDCleaner getInstance() {
    return new MDCleaner();
  }

  class Monitor implements Runnable {
    @Override
    public void run() {
      while (run) {
        try {
          if (leaderElection.isRunning() && leaderElection.isLeader()) {
            if(LOG.isTraceEnabled()) {
              LOG.trace("Cleaning dead locks. I am th leader ");
            }
            clearLocks();
          }
        } catch (IOException e) {
          LOG.info("Eror in metadata cleaner " + e);
        }

        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          LOG.warn("Metadata Cleaner Interrupted");
        }
      }
    }
  }

  private void clearLocks() throws IOException {
    SortedActiveNodeList activeNodeList = leaderElection.getActiveNamenodes();
    if (activeNodeList.size() == 0) {
      return;
    }

    long aliveNNs[] = new long[activeNodeList.size()];
    for (int i = 0; i < activeNodeList.getActiveNodes().size(); i++) {
      aliveNNs[i] = activeNodeList.getActiveNodes().get(i).getId();
    }

    // Clean failed ops
    Collection<SubTreeOperation> ops = getFailedPaths(aliveNNs, (System.currentTimeMillis() - failedSTOCleanDelay));
    for (SubTreeOperation op : ops) {
      LOG.info("Cleaning dead STO lock. OP = {" + op + "}");
      removeLock(op.getPath(), -1);
    }

    // Clean subtree operations that belong to alive NNs
    // Operations that take very long time are considered dead after
    // a predefined time
    ops = getSlowOpsPaths(aliveNNs, (System.currentTimeMillis() - slowSTOCleanDelay));
    for (SubTreeOperation op : ops) {
      LOG.warn("Cleaning slow STO lock. OP = {" + op + "}");
      removeLock(op.getPath(), -1);
    }

    //Clean failed STO operations belonging to live NNs
    ops = getPathsToRecoverAsync();
    for (SubTreeOperation op : ops) {
      LOG.warn("Cleaning async STO lock. OP = {" + op + "}");
      removeLock(op.getPath(), op.getInodeID());
    }
  }

  private void removeLock(String path, long ignoreInodeID) {
    try {
      namesystem.unlockSubtree(path, ignoreInodeID);
    } catch (IOException e) {
      LOG.info("Error while removing sub tree lock " + e);
    }
  }

  Collection<SubTreeOperation> getFailedPaths(final long[] nnIDs, long time) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.MDCLEANER_LIST_FAILED_OPS) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allDeadOperations(nnIDs, time);
              }
            };
    return (Collection<SubTreeOperation>) subTreeLockChecker.handle();
  }

  Collection<SubTreeOperation> getSlowOpsPaths(final long[] nnIDs, long time) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.MDCLEANER_LIST_SLOW_OPS) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allSlowActiveOperations(nnIDs, time);
              }
            };
    return (Collection<SubTreeOperation>) subTreeLockChecker.handle();

  }

  Collection<SubTreeOperation> getPathsToRecoverAsync() throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.MDCLEANER_LIST_ASYNC_OPS) {
              @Override
              public Object performTask() throws IOException {
                OngoingSubTreeOpsDataAccess da = (OngoingSubTreeOpsDataAccess) HdfsStorageFactory
                        .getDataAccess(OngoingSubTreeOpsDataAccess.class);
                return da.allOpsToRecoverAsync();
              }
            };
    return (Collection<SubTreeOperation>) subTreeLockChecker.handle();

  }

  void startMDCleanerMonitor(FSNamesystem namesystem, LeaderElection leaderElection,
                             long failedSTOCleanDelay, long slowSTOCleanDelay) {
    this.leaderElection = leaderElection;
    this.failedSTOCleanDelay = failedSTOCleanDelay;
    this.slowSTOCleanDelay = slowSTOCleanDelay;
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
