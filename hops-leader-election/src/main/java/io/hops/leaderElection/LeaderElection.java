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
package io.hops.leaderElection;

import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;
import io.hops.leaderElection.LeaderElectionRole.Role;
import io.hops.leaderElection.exception.LeaderElectionForceAbort;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.election.entity.LeDescriptor.FailedNodeLeDescriptor;
import io.hops.metadata.election.entity.LeDescriptorFactory;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LeaderElection extends Thread {

  private static final Logger LOG = Logger.getLogger(LeaderElection.class);
  public static final long DRIFT_CONSTANT = 100; //ms
  public static final long LEADER_INITIALIZATION_ID = -1;
  LEContext context;
  protected boolean running = true;
  protected boolean stopped = false;
  //for testing
  private long pause_time = 0; // pause the thread for some time 
  private boolean pause_started = false;
  private boolean forceContantTP = false;
  private long forcedTimePerid = 0;
  private long sucessfulTx = 0;
  private long failedtx = 0;
  private boolean relinquishCurrentId = false;
  private final LeDescriptorFactory leFactory;
  private List<FailedNodeLeDescriptor> deadNodes;
  
  public LeaderElection(final LeDescriptorFactory leFactory,
      final long time_period, final int max_missed_hb_threshold,
      final long time_period_increment, String http_address,
      String rpc_addresses) throws IOException{
    this(leFactory, time_period, max_missed_hb_threshold,
        time_period_increment, http_address, rpc_addresses,
        LeDescriptor.DEFAULT_LOCATION_DOMAIN_ID);
  }
  
  public LeaderElection(final LeDescriptorFactory leFactory,
      final long time_period, final int max_missed_hb_threshold,
      final long time_period_increment, String http_address,
      String rpc_addresses, byte locationDomainId)
      throws IOException {
    context = LEContext.initialContext();
    context.init_phase = true;
    context.time_period = time_period;
    context.max_missed_hb_threshold = max_missed_hb_threshold;
    context.rpc_addresses = rpc_addresses;
    context.http_address = http_address;
    context.time_period_increment = time_period_increment;
    context.locationDomainId = locationDomainId;
    this.leFactory = leFactory;
    this.deadNodes = new ArrayList<FailedNodeLeDescriptor>();
    initialize();
  }

  private void initialize() throws IOException {
    LETransaction transaction = new LETransaction();
    LEContext newContext =
        transaction.doTransaction(leFactory, context, false, this);
    swapContexts(newContext);
  }

  @Override
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    while (running) {
      long txStartTime = System.currentTimeMillis();
      long sleepDuration = -1;
      LEContext updatedContext = null;
      boolean txFailed = false;

      try {
        LOG.debug("LE Status: id " + context.id +
            " Leader Election Start Round. Time period " + context.time_period +
            " Time since last update " +
            (System.currentTimeMillis() - context.last_hb_time));

        LETransaction transaction = new LETransaction();
        updatedContext = transaction
            .doTransaction(leFactory, context, relinquishCurrentId, this);
        relinquishCurrentId = false;
        sucessfulTx++;
      } catch (TransientStorageException te) {
        LOG.error("LE Status: id " + context.id +
            " LeaderElection thread received TransientStorageException. " +
            "sucessfulTx " + sucessfulTx + " failedTx " + failedtx +
            " time period " + context.time_period + " " + te.getMessage(), te);
        // transaction failed
        sleepDuration = 0;
        txFailed = true;
        failedtx++;
      } catch (StorageException e) {
        LOG.error("LE Status: id " + context.id +
                " LeaderElection thread received StorageException. " +
                "sucessfulTx " + sucessfulTx + " failedTx " + failedtx +
                " time period " + context.time_period + " " + e.getMessage(), e);
        // transaction failed
        sleepDuration = 0;
        txFailed = true;
        failedtx++;
      } catch (LeaderElectionForceAbort fa) {
        LOG.error("LE Status: id " + context.id +
            " LeaderElection thread received Forced Abort Exception." +
            " sucessfulTx " + sucessfulTx + " failedTx " + failedtx +
            " time period " + context.time_period + " " + fa.getMessage(), fa);
        // transaction failed
        sleepDuration = 0;
        txFailed = true;
        failedtx++;
      } catch (Throwable t) {
        LOG.fatal("LE Status: id " + context.id +
            " LeaderElection thread received non recoverable exception. " +
            t.getMessage(), t);
        running = false;
        txFailed = true;
        throw new Error(t); // get the hell out of here
      }

      Long txTotalTime = System.currentTimeMillis() - txStartTime;

      if (!txFailed) {
        //swap contexts
        swapContexts(updatedContext);

        if (txTotalTime < context.time_period &&
            !context.nextTimeTakeStrongerLocks) {
          // retry immediately if strong locks are requested
          sleepDuration = context.time_period;
        } else { //retry immediately 
          sleepDuration = 0;
          if (txTotalTime > context.time_period) {
            LOG.error("LE Status: id " + context.id +
                " LeaderElection: Update Tx took very long time to update: " +
                txTotalTime + ", time_perid is " + context.time_period);
          }
        }
      }
      LOG.debug("LE Status: id " + context.id +
          " Leader Election End Round. Update time was " + txTotalTime + " ms");

      if (running) {
        pauseForTesting();
      }
      if (running) {
        if (sleepDuration > 0) {
          sleepFor(sleepDuration);
        }
      }
    } // main while loop
    stopped = true;
    return;
  }

  public synchronized boolean isLeader() {
    if (context.role == Role.LEADER) {
      long elapsed_time = System.currentTimeMillis() - context.last_hb_time;
      if (elapsed_time <
          (context.time_period * context.max_missed_hb_threshold -
              DRIFT_CONSTANT)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public synchronized boolean isSecond() {
    List<ActiveNode> activeNodes =context.memberShip.getSortedActiveNodes();
    if (activeNodes.size()<2 ||
            context.memberShip.getSortedActiveNodes().get(1).getId()==context.id) {
      long elapsed_time = System.currentTimeMillis() - context.last_hb_time;
      if (elapsed_time <
          (context.time_period * context.max_missed_hb_threshold -
              DRIFT_CONSTANT)) {
        return true;
      } 
    }
    return false;
  }
  
  public synchronized boolean isUpToDate() {
    if (context.memberShip == null || context.memberShip.size() <= 0) {
      return false;
    }
    long elapsed_time = System.currentTimeMillis() - context.last_hb_time;
    if (elapsed_time < (context.time_period * context.max_missed_hb_threshold - DRIFT_CONSTANT)) {
      return true;
    } else {
      return false;
    }

  }
  
  public void stopElectionThread() {
    running = false;
    this.interrupt();
  }
  
  public boolean isStopped() {
    return stopped;
  }

  //for testing only
  public void pauseFor(long pause) {
    LOG.debug(
        "LE Status: id " + context.id + " setting pause flag. Time " + pause);
    if (pause_time != 0) {
      throw new UnsupportedOperationException("LE Status: id " + context.id +
          " Application is alredy paused. Remaining pause time " + pause_time);
    }
    pause_time = pause;
  }

  //for testing only
  public void forceResume() {
    if (pause_started) {
      LOG.debug("LE Status: id " + context.id + " sending interrupt");
      this.interrupt();
    } else {
      pause_time = 0;
      pause_started = false;
    }
  }

  // only for testing 
  public boolean isPaused() {
    if (pause_time > 0 ||
        pause_started) { // two separate condition as i am not synchronizing access to these variables
      return true;
    } else {
      return false;
    }
  }

  //only for testing
  public void forceFixedTimePeriod(long tp) {
    forcedTimePerid = tp;
    forceContantTP = true;
  }

  private long pauseForTesting() {
    if (pause_time > 0) {
      LOG.debug(
          "LE Status: id " + context.id + " pausing the leader election for " +
              pause_time + " ms");
      pause_started = true;
      sleepFor(pause_time);
      pause_started = false;
      long retVal = pause_time;
      pause_time = 0;
      LOG.debug(
          "LE Status: id " + context.id + " resuming the leader election");
      return retVal;
    } else {
      return pause_time;
    }
  }

  public boolean isRunning() {
    return running;
  }

  private void sleepFor(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException ex) {
      LOG.warn("LE Status: id " + context.id + " got Interrupted " + ex);
    }
  }

  public synchronized long getCurrentId() {
    return context.id;
  }

  public synchronized long getCurrentTimePeriod() {
    return context.time_period;
  }

  public synchronized SortedActiveNodeList getActiveNamenodes() {
    return context.memberShip;
  }

  public synchronized String getRpcAddress() {
    return context.rpc_addresses;
  }

  public synchronized String getHttpAddress() {
    return context.http_address;
  }

  private synchronized void swapContexts(LEContext newContext) {
    assert newContext != null;
    context = newContext;

    // save failed nodes if any
    for (LeDescriptor descriptor : newContext.removedNodes){
      deadNodes.add(new FailedNodeLeDescriptor(descriptor));
    }

    //only for testing
    if (forceContantTP) {
      context.time_period = forcedTimePerid;
    }
  }
  
  public void relinquishCurrentIdInNextRound() throws InterruptedException {
    relinquishCurrentId = true;
    while (true) {
      Thread.sleep(50);
      if (relinquishCurrentId == false) {
        return;
      }
    }
  }

  public synchronized List<FailedNodeLeDescriptor> getDeadNodes(){
    List<FailedNodeLeDescriptor> ret = deadNodes;
    deadNodes = new ArrayList<FailedNodeLeDescriptor>();
    return ret;
  }
  
  public void waitActive() throws InterruptedException {
    long start = -1;
    while (true) {
      Thread.sleep(100);
      if (context.memberShip == null) {
        continue;
      }
      if (context.memberShip.size() >= 1) {
        if (start < 0) {
          start = Time.now();
        }
        // HOPS-626
        if (isLeader() ||
            Time.now() - start > context.time_period * (context.max_missed_hb_threshold + 1)) {
          return;
        }
      }
    }
  }
}
