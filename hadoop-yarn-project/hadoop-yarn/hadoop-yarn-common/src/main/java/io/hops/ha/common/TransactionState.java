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
package io.hops.ha.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

public abstract class TransactionState {

  //TODO: Should we persist this id when the RT crashes and the NM starts 
  //sending HBs to the new RT?
  protected final static AtomicInteger pendingEventId = new AtomicInteger(0);

  public enum TransactionType {

    RM,
    APP,
    NODE,
    INIT
  }

  private static final Log LOG = LogFactory.getLog(TransactionState.class);
  private AtomicInteger counter = new AtomicInteger(0);
  protected final Set<ApplicationId> appIds = new ConcurrentSkipListSet<ApplicationId>();
  protected final Set<NodeId> nodesIds = new ConcurrentSkipListSet<NodeId>();
  private Set<Integer> rpcIds = new ConcurrentSkipListSet<Integer>();
  private AtomicInteger id=new AtomicInteger(-1);
  private final boolean batch;

  public TransactionState(int initialCounter, boolean batch) {

    counter = new AtomicInteger(initialCounter);
    this.batch = batch;
  }

  public int getId(){
    return id.get();
  }
    public Set<ApplicationId> getAppIds(){
    return appIds;
  }

  public void incCounter(Enum type) {
    counter.incrementAndGet();
  }

  public void decCounter(Enum type) throws IOException {
    int value = counter.decrementAndGet();
    if(!batch && value==0){
      commit(true);
    }
  }

  public int getCounter(){
    return counter.get();
  }

  public void addRPCId(int rpcId){
    if(rpcId>=0){
      id.compareAndSet(-1, rpcId);
    }
    rpcIds.add(rpcId);
  }

  public Set<Integer> getRPCIds(){
    return rpcIds;
  }

  public abstract void commit(boolean first) throws IOException;
}
