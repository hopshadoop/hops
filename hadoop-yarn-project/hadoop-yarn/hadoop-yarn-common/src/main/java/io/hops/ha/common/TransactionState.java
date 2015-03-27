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
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TransactionState {

  
  //TODO: Should we persist this id when the RT crashes and the NM starts 
  //sending HBs to the new RT?
  protected static AtomicInteger pendingEventId = new AtomicInteger(0);

  public enum TransactionType {

    RM,
    APP,
    NODE
  }

  private static final Log LOG = LogFactory.getLog(TransactionState.class);
  private int counter = 1;
  protected int rpcID;

  public TransactionState(int rpcID) {
    this.rpcID = rpcID;
  }

  public synchronized void incCounter(Enum type) {
    counter++;
    LOG.debug(
        "counter inc for rpc: " + this.rpcID + " count " + counter + " type: " +
            type + " classe:" + type.getClass());
  }

  public synchronized void decCounter(Enum type) throws IOException {
    counter--;
    LOG.debug(
        "counter dec for rpc: " + this.rpcID + " count " + counter + " type: " +
            type + " classe:" + type.getClass());
    if (counter == 0) {
      commit();
    }
  }

  public synchronized void decCounter(String type) throws IOException {
    counter--;
    LOG.debug(
        "counter dec for rpc: " + this.rpcID + " count " + counter + " type: " +
            type);
    if (counter == 0) {
      commit();
    }
  }


  public int getId() {
    return rpcID;
  }

  abstract void commit() throws IOException;
}
