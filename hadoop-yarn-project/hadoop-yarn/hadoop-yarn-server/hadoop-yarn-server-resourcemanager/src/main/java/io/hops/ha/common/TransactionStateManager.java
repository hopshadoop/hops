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

import io.hops.metadata.util.RMUtilities;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TransactionStateManager implements Runnable{
  private static final Log LOG = LogFactory.getLog(TransactionStateManager.class);
  TransactionState currentTransactionState;
  Lock lock = new ReentrantLock(true);
  AtomicInteger acceptedRPC =new AtomicInteger();
  List<transactionStateWrapper> curentRPCs = new CopyOnWriteArrayList<transactionStateWrapper>();
  int batchMaxSize = 50;
  int batchMaxDuration = 100;
  AtomicBoolean blockNonHB = new AtomicBoolean(false);
  private boolean running = false;
  
  Thread excutingThread;
  
  private final ExecutorService executorService =
      Executors.newFixedThreadPool(32);
    
  public TransactionStateManager(Configuration conf){
    currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true, this);
    batchMaxSize = conf.getInt(YarnConfiguration.HOPS_BATCH_MAX_SIZE, YarnConfiguration.DEFAULT_HOPS_BATCH_MAX_SIZE);
    batchMaxDuration = conf.getInt(YarnConfiguration.HOPS_BATCH_MAX_DURATION, YarnConfiguration.DEFAULT_HOPS_BATCH_MAX_DURATION);
    RMUtilities.setCommitAndQueueLimits(conf);
  }
  
  @Override
  public void run() {
    lock.lock();
    long commitDuration = 0;
    long startTime = System.currentTimeMillis();
    double accumulatedCycleDuration=0;
    long nbCycles =0;
    List<Long> duration = new ArrayList<Long>();
    List<Integer> rpcCount = new ArrayList<Integer>();
    double accumulatedRPCCount = 0;
    long t1=0;
    long t2=0;
    long t3=0;
    long t4=0;
    while(running){
      try {
        //create new transactionState
        currentTransactionState = new TransactionStateImpl(
                TransactionState.TransactionType.RM, 0, true, this);
        curentRPCs = new CopyOnWriteArrayList<transactionStateWrapper>();
        acceptedRPC.set(0);
        //accept RPCs
        lock.unlock();
        commitDuration = System.currentTimeMillis()-startTime;
        t3= commitDuration;
//        Thread.sleep(Math.max(0, 10-commitDuration));
        waitForBatch(Math.max(0, batchMaxDuration-commitDuration));
        t4= System.currentTimeMillis() - startTime;
        //stop acception RPCs
        lock.lock();
        
        long cycleDuration = System.currentTimeMillis() - startTime;
        if (cycleDuration> batchMaxDuration + 10) {
          LOG.debug("Cycle too long: " + cycleDuration + "| " + t1 + ", " + t2
                  + ", " + t3 + ", " + t4);
        }
        nbCycles++;
        accumulatedCycleDuration+=cycleDuration;
        duration.add(cycleDuration);
        rpcCount.add(acceptedRPC.get());
        accumulatedRPCCount+=acceptedRPC.get();
        if(duration.size()>39){
          double avgCycleDuration=accumulatedCycleDuration/nbCycles;
          LOG.debug("cycle duration: " + avgCycleDuration + " " + duration.toString());
          double avgRPCCount = accumulatedRPCCount/nbCycles;
          LOG.debug("rpc count: " + avgRPCCount + " " + rpcCount.toString());
          duration = new ArrayList<Long>();
          rpcCount = new ArrayList<Integer>();
        }
        
        startTime = System.currentTimeMillis();
        //wait for all the accepted RPCs to finish
        int count = 0;
        long startWaiting = System.currentTimeMillis();
        while(currentTransactionState.getCounter() != 0 && running){
          if(System.currentTimeMillis()-startWaiting>1000){
            startWaiting = System.currentTimeMillis();
            count++;
            LOG.error("waiting too long " + count + " counter: " + currentTransactionState.getCounter());
            for(transactionStateWrapper w : curentRPCs){
              if(w.getRPCCounter()>0){
                LOG.error("rpc not finishing: " + w.getRPCID() + " type: " + w.getRPCType() + ", counter: " + w.getRPCCounter() + " running events: " + w.getRunningEvents());
              }
            }
          }
        }
        if(!running){
          break;
        }
        
        t1= System.currentTimeMillis() - startTime;
        //commit the transactionState
        currentTransactionState.commit(true);
        t2= System.currentTimeMillis() - startTime;
      } catch (IOException ex) {
        Logger.getLogger(TransactionStateManager.class.getName()).
                log(Level.SEVERE, null, ex);
      } catch (InterruptedException ex) {
        Logger.getLogger(TransactionStateManager.class.getName()).
                log(Level.SEVERE, null, ex);
      }
    }
    LOG.info("TransactionStateManager stoped");
  }
  
  public ExecutorService getExecutorService(){
    return executorService;
  }
  
  private void waitForBatch(long maxTime) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (running) {
      if (System.currentTimeMillis() - start > maxTime || acceptedRPC.get() >= batchMaxSize) {
        break;
      }
      Thread.sleep(1);
    }
  }
  
  public TransactionState getCurrentTransactionStateNonPriority(int rpcId,
          String callingFuncition) {
    synchronized(blockNonHB){
    while (blockNonHB.get()) {
      try {
        blockNonHB.wait();
      } catch (InterruptedException e) {
        LOG.warn(e, e);
      }
    }
    }
    return getCurrentTransactionState(rpcId, callingFuncition, false);

  }

  public TransactionState getCurrentTransactionStatePriority(int rpcId,
          String callingFuncition) {
    long start = System.currentTimeMillis();
    TransactionState ts = getCurrentTransactionState(rpcId, callingFuncition,
            true);
    long duration = System.currentTimeMillis() - start;
    if (duration > 400) {
      LOG.error("getCurrentTransactionStatePriority too long: " + duration);
    }
    return ts;
  }

  private TransactionState getCurrentTransactionState(int rpcId,
          String callingFuncition, boolean priority) {
    while (true) {
      int accepted = acceptedRPC.incrementAndGet();
      if (priority || accepted < batchMaxSize) {
        lock.lock();
        try {
          transactionStateWrapper wrapper = new transactionStateWrapper(
                  (TransactionStateImpl) currentTransactionState,
                  TransactionState.TransactionType.RM, rpcId, callingFuncition);
          wrapper.incCounter(TransactionState.TransactionType.INIT);
          if (rpcId >= 0) {
            wrapper.addRPCId(rpcId);
          }
          curentRPCs.add(wrapper);
          return wrapper;
        } finally {
          lock.unlock();
        }
      } else {
        acceptedRPC.decrementAndGet();
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          LOG.warn(e, e);
        }
      }
    }
  }
  
  public void start(){
    excutingThread = new Thread(this);
    excutingThread.setName("transactionStateManager Thread");
    running = true;
    excutingThread.start();
  }
  
  public void stop() throws InterruptedException{
    if (running) {
      running = false;
      excutingThread.join(10000);
      executorService.shutdown();
      executorService.
              awaitTermination(10, TimeUnit.SECONDS);
    }
  }
  
  public boolean blockNonHB(){
    synchronized(blockNonHB){
    return blockNonHB.compareAndSet(false, true);
    }
  }
  
  public void unblockNonHB(){
    synchronized(blockNonHB){
    if(blockNonHB.compareAndSet(true, false)){
      LOG.info("unblocking non priority");
    }
    blockNonHB.notify();
    }
  }
}
