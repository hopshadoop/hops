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
import io.hops.exception.TransactionContextException;
import io.hops.leaderElection.exception.LEWeakLocks;
import io.hops.leaderElection.exception.LeaderElectionForceAbort;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.election.entity.LeDescriptorFactory;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.LeaderOperationType;
import io.hops.transaction.handler.LeaderTransactionalRequestHandler;
import io.hops.transaction.lock.LeLockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class LETransaction {

  private final static Logger LOG = Logger.getLogger(LETransaction.class);
  private LEContext context;
  private TransactionLockTypes.LockType txLockType = null;
  private List<LeDescriptor> sortedList = null;
  private LeDescriptorFactory leFactory;

  private void LETransaction() {
  }

  protected LEContext doTransaction(final LeDescriptorFactory lef,
      final LEContext currentContext, final boolean relinquishCurrentId,
      final LeaderElection le) throws IOException {

    LeaderTransactionalRequestHandler leaderElectionHandler =
        new LeaderTransactionalRequestHandler(
            LeaderOperationType.LEADER_ELECTION) {
          @Override
          public void preTransactionSetup() throws IOException {
            sortedList = null;
            leFactory = lef;
            super.preTransactionSetup();
            context = new LEContext(currentContext, lef);
            context.removedNodes.clear(); //do not report dead nodes multiple times

            if (relinquishCurrentId) {
              context.id = LeaderElection.LEADER_INITIALIZATION_ID;
            }
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LeLockFactory lockFactory = LeLockFactory.getInstance();

            if (currentContext.id == LeaderElection.LEADER_INITIALIZATION_ID ||
                currentContext.role == LeaderElectionRole.Role.LEADER ||
                currentContext.nextTimeTakeStrongerLocks) {
              locks.add(lockFactory.getLeVarsLock(leFactory.getVarsFinder(),
                  TransactionLockTypes.LockType.WRITE)).add(lockFactory
                  .getLeDescriptorLock(leFactory,
                      TransactionLockTypes.LockType.READ_COMMITTED));
              txLockType = TransactionLockTypes.LockType.WRITE;

            } else {
              locks.add(lockFactory.getLeVarsLock(leFactory.getVarsFinder(),
                  TransactionLockTypes.LockType.READ)).add(lockFactory
                  .getLeDescriptorLock(leFactory,
                      TransactionLockTypes.LockType.READ_COMMITTED));
              txLockType = TransactionLockTypes.LockType.READ;
            }
            // taking lock on first register is enough to achieve serialization
          }

          @Override
          public Object performTask() throws IOException {

            try {
              if (context.nextTimeTakeStrongerLocks) {
                context.nextTimeTakeStrongerLocks = false;
              }

              if (VarsRegister.getTimePeriod(leFactory.getVarsFinder()) == 0) {
                VarsRegister.setTimePeriod(leFactory.getVarsFinder(),
                    context.time_period);
              } else {
                context.time_period =
                    VarsRegister.getTimePeriod(leFactory.getVarsFinder());
              }

              if (context.init_phase) {
                initPhase();
              } else {
                periodicUpdate();
              }

              if (!le.isRunning()) {
                throw new LeaderElectionForceAbort(
                    "Aborting the transaction because the parent thread has stopped");
              }

              return new Boolean(true);
            } finally {
            }
          }
        };

    Boolean retVal = (Boolean) leaderElectionHandler.handle(null);
    if (retVal != null && retVal.equals(true)) {
      return context;
    } else {
      return null;
    }
  }

  private void initPhase() throws IOException {
    LOG.debug("LE Status: id " + context.id +
        " Executing initial phase of the protocol. ");
    try {
      updateCounter();
      context.init_phase = false;
    } catch (LEWeakLocks wl) {
      context.nextTimeTakeStrongerLocks = true;
      LOG.info("LE Status: id " + context.id +
          " initPhase Stronger locks requested in next round");
    }
  }

  private void periodicUpdate() throws IOException {
    try {
      updateCounter();
      leaderCheck();
      increaseTimePeriod();
      membershipMgm();
    } catch (LEWeakLocks wl) {
      context.nextTimeTakeStrongerLocks = true;
      LOG.info("LE Status: id " + context.id +
          " periodic update. Stronger locks requested in next round");
    }
    appendHistory();
    context.last_hb_time = System.currentTimeMillis();
  }

  protected void updateCounter() throws IOException, LEWeakLocks {
    if (descriptorExists(context.id)) {
      incrementCounter();
    } else {      //case: join, reboot, evicted
      if (txLockType == TransactionLockTypes.LockType.READ) {
        String msg = "LE Status: id " + context.id +
            " Id not found. I have shared locks. Retry with stronger lock";
        LOG.info(msg);
        throw new LEWeakLocks(msg);
      } else if (txLockType == TransactionLockTypes.LockType.WRITE) {
        long oldId = context.id;
        context.id = getNewNamenondeID();
        LeDescriptor newDescriptor = leFactory
            .getNewDescriptor(context.id, 0/*counter*/, context.rpc_addresses,
                context.http_address, context.locationDomainId);
        EntityManager.add(newDescriptor);
        if (oldId != LeaderElection.LEADER_INITIALIZATION_ID) {
          LOG.warn( "LE Status: id " + context.id + " I was kicked out. Old Id was " + oldId);
          setEvictionFlag();
        }
      } else {
        String msg =
            "LE Status: id " + context.id + " lock type not supported. Got " + txLockType + " lock";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    }
  }

  private boolean descriptorExists(final long processId)
      throws TransactionContextException, StorageException {
    LeDescriptor descriptor = getDescriptor(processId);
    if (descriptor != null) {
      return true;
    } else {
      return false;
    }
  }

  private LeDescriptor getDescriptor(final long process_id)
      throws TransactionContextException, StorageException {
    LeDescriptor descriptor = EntityManager
        .find(leFactory.getByIdFinder(), process_id,
            LeDescriptor.DEFAULT_PARTITION_VALUE);
    return descriptor;
  }

  private long getNewNamenondeID()
      throws TransactionContextException, StorageException {
    long newId = VarsRegister.getMaxID(leFactory.getVarsFinder()) + 1;
    VarsRegister.setMaxID(leFactory.getVarsFinder(), newId);
    return newId;
  }

  private void incrementCounter() throws IOException {
    LeDescriptor descriptor = getDescriptor(context.id);
    descriptor.setCounter(descriptor.getCounter() + 1);
    EntityManager.add(descriptor);
  }

  List<LeDescriptor> getAllSortedDescriptors()
      throws TransactionContextException, StorageException {
    if (sortedList == null) {
      sortedList =
          (List<LeDescriptor>) EntityManager.findList(leFactory.getAllFinder());
      Collections.sort(sortedList);
    }
    return sortedList;
  }

  private void leaderCheck() throws IOException, LEWeakLocks {
    long smallestAliveProcess = getSmallestIdAliveProcess();
    //LOG.debug("LE Status: id " + context.id + " Smalles alive process is id "+smallestAliveProcess);
    if (smallestAliveProcess == context.id) {
      if (txLockType == TransactionLockTypes.LockType.WRITE) {
        if (context.role !=
            LeaderElectionRole.Role.LEADER) { //print the log messsage only if the status changes
          LOG.info("LE Status: id " + context.id + " I am the new LEADER. ");
        }
        context.role = LeaderElectionRole.Role.LEADER;
        removeDeadNameNodes();
      } else {
        String msg = "LE Status: id " + context.id +
            " I can be the leader but I have weak locks. Retry with stronger lock";
        LOG.info(msg);
        throw new LEWeakLocks(msg);
      }
    } else {
      if (context.role != LeaderElectionRole.Role.NON_LEADER ||
          context.last_hb_time ==
              0) {//print the log messsage only if the status changes
        LOG.info("LE Status: id " + context.id + " I am a NON_LEADER process ");
      }
      context.role = LeaderElectionRole.Role.NON_LEADER;
      context.leader = smallestAliveProcess;
    }
  }

  private long getSmallestIdAliveProcess() throws IOException {
    List<LeDescriptor> sortedList = getAllAliveProcesses();
    if (sortedList.size() > 0) {
      return sortedList.get(0).getId();
    } else {
      LOG.debug("LE Status: id " + context.id +
          " No namenodes in the system. The first process will be the leader");
      return LeaderElection.LEADER_INITIALIZATION_ID;
    }
  }

  private void removeDeadNameNodes() throws IOException {
    HashMap<Long, LeDescriptor> oldDescriptors = null;
    if (context.history.size() >= context.max_missed_hb_threshold) {
      oldDescriptors = context.history.get(0);
    }

    if (oldDescriptors == null) {
      return;
    }

    List<LeDescriptor> newDescriptors = getAllSortedDescriptors();

    for (LeDescriptor newDesc : newDescriptors) {
      LeDescriptor oldDesc = oldDescriptors.get(newDesc.getId());
      if (oldDesc != null && newDesc.getCounter() == oldDesc.getCounter()) {
        LOG.debug("LE Status: id " + context.id + " removing dead node " +
            oldDesc.getId());
        removeLeaderRow(oldDesc);
        context.removedNodes.add(oldDesc);
      }
    }
  }

  private void removeLeaderRow(LeDescriptor leader)
      throws StorageException, TransactionContextException {
    EntityManager.remove(leader);
  }

  private void membershipMgm() throws IOException {
    List<LeDescriptor> aliveList = getAllAliveProcesses();
    makeSortedActiveNodeList(aliveList);

  }

  private List<LeDescriptor> getAllAliveProcesses() throws IOException {
    List<LeDescriptor> aliveList = new ArrayList<LeDescriptor>();
    HashMap<Long, LeDescriptor> oldDescriptors = null;
    if (context.history.size() >= context.max_missed_hb_threshold) {
      oldDescriptors = context.history.get(0);
    }

    List<LeDescriptor> newDescriptors = getAllSortedDescriptors();

    if (oldDescriptors != null) {
      for (LeDescriptor newDesc : newDescriptors) {
        LeDescriptor oldDesc = oldDescriptors.get(newDesc.getId());
        if (oldDesc != null) {
          if (newDesc.getCounter() > oldDesc.getCounter()) {
            aliveList.add(newDesc);
          } else {
            // LOG.debug("LE Status: id " + context.id + " Suspecting id "+oldDesc.getId());
          }
        } else {
          aliveList.add(newDesc);
        }
      }
    } else {
      aliveList.addAll(newDescriptors);
    }
    return aliveList;
  }

  private void makeSortedActiveNodeList(List<LeDescriptor> nns) {
    List<ActiveNode> activeNameNodeList = new ArrayList<ActiveNode>();
    for (LeDescriptor l : nns) {
      // comma separated list. First address is RPC address and the
      // second address is Server IPC address
      String[] hostName =  {"", ""};
      int [] port = {0,0};
      String hostNameNPort = l.getRpcAddresses();
      StringTokenizer st = new StringTokenizer(hostNameNPort, ",");
      for(int i = 0; i < 2; i++){
        if(st.hasMoreTokens()) {
          String address = st.nextToken();
          StringTokenizer st2 = new StringTokenizer(address, ":");
          String intermediaryHostName = st2.nextToken();
          port[i] = Integer.parseInt(st2.nextToken());
          StringTokenizer st3 = new StringTokenizer(intermediaryHostName, "/");
          hostName[i] = st3.nextToken();
        }
      }
      String httpAddress = l.getHttpAddress();
      ActiveNode ann =
          new ActiveNodePBImpl(l.getId(), l.getRpcAddresses(), hostName[0], port[0],
              httpAddress, hostName[1], port[1], l.getLocationDomainId());
      activeNameNodeList.add(ann);
    }

    context.memberShip = new SortedActiveNodeListPBImpl(activeNameNodeList);
  }

  private void appendHistory() throws IOException {
    //rolling history
    //remove oldest to make room for new entries
    if (context.history.size() > 0 &&
        context.history.size() >= context.max_missed_hb_threshold) {
      context.history.remove(0);
    }
    List<LeDescriptor> list = getAllSortedDescriptors();
    HashMap<Long, LeDescriptor> descriptors = new HashMap<Long, LeDescriptor>();
    for (LeDescriptor desc : list) {
      descriptors.put(desc.getId(), desc);
    }
    context.history.add(descriptors);
  }

  private void increaseTimePeriod()
      throws TransactionContextException, StorageException {
    if (VarsRegister.isEvict(leFactory.getVarsFinder()) &&
        context.role == LeaderElectionRole.Role.LEADER &&
        txLockType == TransactionLockTypes.LockType.WRITE) {
      long oldTP = context.time_period;
      context.time_period = context.time_period + context.time_period_increment;
      VarsRegister
          .setTimePeriod(leFactory.getVarsFinder(), context.time_period);
      LOG.warn("LE Status: id " + context.id +
          " I am LEADER and I am updating the time period. Old Tp: " + oldTP +
          " new TP: " + context.time_period);
      VarsRegister.setEvictFlag(leFactory.getVarsFinder(), false);
    }
  }

  private void setEvictionFlag()
      throws TransactionContextException, StorageException {
    VarsRegister.setEvictFlag(leFactory.getVarsFinder(), true);
  }
}
