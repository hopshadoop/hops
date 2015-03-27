/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.event;

import io.hops.ha.common.TransactionState;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Parent class of all the events. All events extend this class.
 */
@Public
@Evolving
public abstract class AbstractEventTransaction<TYPE extends Enum<TYPE>>
    implements Event<TYPE> {

  private final TYPE type;
  private final long timestamp;
  protected TransactionState transactionState;

  // use this if you DON'T care about the timestamp
  public AbstractEventTransaction(TYPE type,
      TransactionState transactionState) {
    this.type = type;
    // We're not generating a real timestamp here.  It's too expensive.
    timestamp = -1L;
    this.transactionState = transactionState;
  }

  // use this if you care about the timestamp
  public AbstractEventTransaction(TYPE type, long timestamp,
      TransactionState transactionState) {
    this.type = type;
    this.timestamp = timestamp;
    this.transactionState = transactionState;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public TYPE getType() {
    return type;
  }

  @Override
  public String toString() {
    return "EventType: " + getType();
  }

  public TransactionState getTransactionState() {
    return transactionState;
  }
}
